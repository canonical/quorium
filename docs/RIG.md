# The emulated rig

`examples/rig` runs Quorium as five real processes in LXD containers on one host, grouped into three emulated regions. It runs Jepsen-style workloads against them while injecting faults.

## Layout

| Container | Region | One-way delay to region A / B / C |
|---|---|---|
| qr1, qr4 | region-a | 0 / 15 / 30 ms |
| qr2, qr5 | region-b | 15 / 0 / 25 ms |
| qr3 | region-c | 30 / 25 / 0 ms |

`tc netem` on each container's interface adds the delays, with 1 ms of jitter, and the packet-loss fault. Nodes talk over QUIC with mutual TLS, run 2 shards, checkpoint every 200 entries, and use a 150 ms heartbeat, about 2.5× the largest round trip between voters (60 ms).

## Workloads and checks

| Workload | Where it runs | Check |
|---|---|---|
| register | 8 clients on the host, through `quorium::Client` | Every key's history is linearizable (the simulator's Wing–Gong checker) |
| bank | Inside every node, on one `Bank` entity: random transfers plus balance reads | Every read sees the original total and no negative balance |
| counter | Inside every node, `try_take(1)` on an escrow counter with 2,000 units | Acknowledged sales never exceed stock |

## Faults

`kill` (SIGKILL, then restart), `pause` (SIGSTOP and SIGCONT), `partition` (iptables, random minority), `power` (`lxc stop --force`, then start), `loss` (5–25% packet loss on every link).

Container stops keep the host page cache, so they do not test fsync the way pulling the power would. Power-loss durability is covered by the simulator's lost-write disk model.

## Running it

```sh
cargo build --release -p rig
./target/release/rig up --dir target/rig
./target/release/rig run --dir target/rig --seconds 300 --seed 21
./target/release/rig probe --dir target/rig
./target/release/rig down
```

`run` wipes the nodes, pushes the current `rig-node` binary and checks its checksum in every container, starts the cluster, runs the workloads and faults, heals everything, then prints a JSON report. The command exits with status 1 if any check failed.

## Soak

`rig soak --windows 72 --seconds 3600` starts one cluster and never wipes it. Every window runs the workloads and faults for an hour on fresh register keys and is checked on its own. Each window's report records every node's memory use and data size, so leaks and unbounded growth show up across windows.

## Rolling upgrade

`rig upgrade --binary <old> --next-binary <new>` starts the cluster on the old build. It runs the workloads and faults, and a quarter of the way in replaces the nodes one at a time with the new build. The rig's `next` feature builds a `rig-node` that registers an extra entity type, `Ledger`, and writes to it continuously. The check passes only if:

- no write of the new type was accepted before the last node started on the new build (method-id gating);
- writes succeeded afterwards;
- no old node ever had to apply an entry it does not understand.

## Results (2026-10-20)

| Run | Faults | Acknowledged register writes | Result |
|---|---|---|---|
| 5 × 300 s, seeds 21–25, all faults | 194 | 51,209 | All linearizable; bank conserved in 7,658 reads; no oversell; write p50 80–106 ms, p99 0.9–1.4 s; recovery after healing 80–444 ms |
| Rolling upgrade, 240 s, seed 7 | 21 (pause, partition, loss) | 6,018 | Passed: 0 early writes of the new type, 1,431 after |
| Rolling upgrade, 300 s, seed 8 | 29 (including 10 kills) | 6,351 | Passed: 0 early, 2,096 after |
| 2 × 300 s, seeds 31–32, all faults, tmpfs storage, escrow copy peers and hedged client | 53 | 46,571 | All linearizable; bank conserved; counter sold exactly its 2,000 units; write p50 48–51 ms, p99 113–154 ms |
| 72-hour soak, first attempt | — | — | Void: the host disk filled after window 1 (see finding 5) |
| 72-hour soak, second attempt | — | — | Running since 2026-10-20 17:00 UTC |

## Failover

`rig failover --samples 30` kills the shard-0 leader with SIGKILL, then writes from the host until a write is acknowledged, 30 times. The gap is measured from the kill, timed inside the container, to the acknowledgement. It therefore includes detection, the election, the new leader's first commit and the client's write.

| Storage | Heartbeat | p50 | p99 | Max |
|---|---|---|---|---|
| tmpfs | 100 ms | 577 ms | 650 ms | 650 ms |
| tmpfs | 150 ms | 702 ms | 788 ms | 788 ms |
| Host network disk, 5 nodes sharing it | 150 ms | 1.8 s | 8.2 s | 8.2 s |

On tmpfs at a 100 ms heartbeat the time splits into:

- detection: about 300 ms, the Ladder's base of 3 heartbeats;
- election: about 60 ms, a pre-vote and a vote to the nearest majority;
- the new leader's first commit and the client's write: one round trip each.

The 500 ms target is not met on this topology. Detection alone needs about 3 heartbeats, and a heartbeat below 100 ms is under twice the 60 ms round trip between voters.

On the host's network disk, a synced 4 KiB write takes 12 ms when idle and 100–200 ms when five nodes sync at once. Votes, the leader's first entry and the client's write each wait for syncs. The Ladder also stretches its election deadline when heartbeats arrive late.

## What the rig found

The first runs passed every safety check but showed that the cluster could not keep a leader under load:

- terms climbing by dozens per minute with no faults;
- a write after healing taking 30–60 s.

These causes were found and fixed. None of them appears in the simulator, because simulated I/O never blocks a thread and simulated RPCs ignore deadlines.

1. **Disk syncs blocked the async runtime.**
   - The shared WAL held its lock during fsync.
   - The escrow journal fsynced every sale inline.
   - Votes, truncations and state checkpoints synced on async threads.

   With 2 runtime workers and a network block device (17–150 ms per fsync), heartbeats stopped. All syncs now run on blocking threads outside the locks, and the WAL lock is async-aware. Tests with a deliberately slow disk cover each path.
2. **openraft deadlines were shorter than a cross-region round trip.** Heartbeat RPCs time out after one heartbeat interval and votes after the election timeout. The network adapter now applies a 1 s floor where it can. A heartbeat above the largest round trip raises the Ladder's election base instead of being clamped, and the node warns when a measured round trip exceeds the heartbeat.
3. **Campaigns were shorter than a vote.** A vote costs a round trip plus the voter's fsync. The Ladder now measures both the round trip to the nearest majority and the local fsync latency, and scales campaign length, rank spacing and retries with them. It also stretches its election deadline to twice the worst recent heartbeat gap, capped at 8× the base.
4. **Leader balancing moved leadership to lagging followers.** openraft rejects such a transfer, and the attempt interrupts heartbeats. Balancing now waits until the shard has been idle for 2 s and the target has replicated the whole log.
5. **Container logs filled the host disk.** Each node's output went to both journald and rsyslog, about 1 GB per container in a day, and rsyslog kept the host load at 20–35. `rig up` now caps journald at 64 MB, stops forwarding to syslog and disables rsyslog.
6. **The client gave up on writes that were about to commit.** A write attempt that took longer than the client's per-attempt timeout was cancelled and retried, so when commits slowed down no attempt ever finished; one failover took 72 s. The client now keeps a slow attempt in flight and sends a copy to another node, up to 3 at a time. It sends the copy after twice its recent reply time, and never to a node it is already waiting on.
7. **Container reboots filled the disk with AppArmor debris.** Each boot of an Ubuntu container runs `apparmor.service`, which fails to load some profiles inside the container and leaves about 100 temporary cache files (9 MB) behind. Under the soak's power faults this grew by about 130 MB an hour. `rig up` now masks the service, which only loads profiles for processes inside the container, and deletes stale cache files.
8. **A killed node could join two event records on one line.** `rig-node` wrote each record and its newline in two writes, and a kill between them left two records on one line. The checker refused the line, so every later soak window ended without a verdict. Records are now written in a single append, and the checker reads any number of whole records per line.
