# Operating Quorium

## Sizing

| Resource | Guidance |
|---|---|
| Nodes | 3 or 5 voters (`replicas`). More service instances join as learners and replace failed voters |
| Shards | Fixed at cluster creation with `shards(n)`. Every shard is replicated on every voter. Leadership is balanced across voters and follows the datacenter that writes most |
| Memory | Entity state is served from memory and checkpointed to redb. Keep single entities under 1 MiB; larger ones are counted in `quorium_large_entity_writes_total` and logged as a warning |
| Disk | One write-ahead log per node (`wal`), compacted automatically after checkpoints past 64 MiB, plus one redb database (`state.redb`) holding every shard's checkpointed state |
| fsync | One fsync covers every write on the node issued while the previous fsync ran (group commit) |

Measured on the development machine (criterion, `cargo bench`):

| Operation | Time |
|---|---|
| Apply one key-value write to state | about 185 ns |
| Route a key to its shard | about 24 ns |
| Encode / decode a 16-entry append request | about 5.6 µs / 7.3 µs |
| Append 64 × 1 KiB to the WAL and fsync | about 114 µs |
| Checkpoint 10 changed rows of a 100,000-row shard to redb | about 3.3 ms |
| Write the same shard as one full snapshot file | about 59 ms |
| Rewrite every row in redb (after a snapshot is installed) | about 151 ms |
| Checkpoint 10 changed rows with the fjall backend | about 5.1 ms |
| Rewrite every row with the fjall backend | about 1.1 s |

## Timing

- `heartbeat(d)` should be about 2–3× the largest round trip between voters. openraft gives each heartbeat a deadline of one heartbeat interval, and a leader only accepts writes while a quorum has acknowledged its recent heartbeats (its lease). The default of 50 ms suits a single region; use 150 ms when regions are 60 ms apart. When a fresh leader's lease is not yet established, it holds incoming writes for up to 2 s instead of redirecting clients.
- The Ladder's election base is raised to at least 3× the heartbeat. Failover time grows with it.
- The node logs `the round trip to a peer exceeds the heartbeat interval` when this rule is broken.
- Disk fsync latency is measured continuously and lengthens election campaigns. A slow disk makes failover slower rather than causing repeated elections.

## Failure domains

- Set `datacenter(...)` on every node to the real failure domain (zone or region).
- The Ladder controller ranks election candidates by log, then by datacenter (the failed leader's datacenter first), so leadership stays local when possible.
- With 3 voters in 3 datacenters, the loss of any one datacenter keeps every shard available.
- Single-region failover measured in simulation: p99 170 ms. Three regions with 15–40 ms one-way latency: p99 486 ms.

## Membership

- New nodes are added as learners automatically and promoted while voters are fewer than `replicas`.
- A voter unreachable for `replacement_grace` (default 10 minutes) is replaced by a caught-up learner. Membership never shrinks below `replicas`; without a learner, a dead voter stays in place.
- A node keeps its identity in `data_dir/node_id`. Wiping a data directory creates a new node; the old identity is replaced, never reused.

## Reads

A linearizable read is answered by the shard leader at once while its lease is valid (it heard from a majority within the election timeout). Otherwise the leader confirms leadership with a majority first (ReadIndex). Leases assume clocks advance at roughly the same rate on every node. Skew between wall clocks does not matter. A `.stale()` handle reads the local replica without any check.

## Upgrades

1. Upgrade one node at a time.
2. A new write method becomes usable only when every voter of its shard advertises it; until then the handle returns `Error::Unavailable`, which is retryable.
3. A change to a stored type's encoding needs `version = N + 1` and `migrate = f`. Old state is migrated when it is next written or read.
4. Startup refuses to run if stored state contains a type that the binary does not register.
5. Stored-format fixtures in `crates/quorium-store/tests/fixtures` must keep decoding. They are protected paths.

## Backup and restore

```rust
node.export(Path::new("/backups/2026-10-19.qbak")).await?;
new_cluster_node.import(Path::new("/backups/2026-10-19.qbak")).await?;
```

- `export` reads every shard through a linearizable read and writes a checksummed archive atomically.
- `import` replicates the archive into a cluster whose shards are empty and whose shard count matches. Importing into a non-empty shard is refused.
- Each shard's state travels in one message, so a shard must stay below 16 MiB to be exported.

## Monitoring

Set `metrics_bind(addr)` to serve Prometheus text at `/metrics`, or call `metrics_text()`.

| Metric | Meaning |
|---|---|
| `quorium_shard_leader{shard}` | 1 when this node leads the shard |
| `quorium_shard_term{shard}` | Raft term; a fast-rising term means election churn |
| `quorium_shard_last_applied{shard}` | Applied log index; compare across nodes to see lag |
| `quorium_shard_quiesced{shard}` | 1 when an idle shard has stopped heartbeating |
| `quorium_shard_checkpoint_index{shard}` | Log index of the last durable state checkpoint |
| `quorium_large_entity_writes_total` | Writes that left an entity above 1 MiB |
| `quorium_shard_writes_per_second{shard}` | Writes this node led on the shard in the last full second |
| `quorium_appends_sent_total` | Replication traffic |
| `quorium_pings_received_total` | Host liveness traffic, constant per node pair |
| `quorium_wal_fsyncs_total`, `quorium_wal_bytes` | Disk activity and log size |
| `quorium_wal_sync_latency_seconds` | Smoothed fsync latency. It lengthens elections; above roughly a third of the heartbeat interval, expect slower failover and writes held while a new leader's lease is established |

A Grafana dashboard for these metrics is in [dashboards/quorium.json](dashboards/quorium.json).

## Checkpoints

State is stored in redb by default. With the `fjall` feature, `state_backend(StateBackend::Fjall)` stores it in an LSM tree under `state.fjall` instead, which suits write-heavy or very large datasets. Both sit behind the same `Storage` trait and migrate legacy snapshot files the same way. Startup refuses to switch backends on a data directory that already holds state in the other one.

Each shard checkpoints its state into `state.redb` when Raft takes a snapshot (by default every 5,000 log entries). A checkpoint writes only the rows changed since the previous one, in a single fsynced redb transaction, and the log is truncated behind it. Individual writes are made durable by the log, so state commits never fsync per write. `node.checkpoint().await` forces a checkpoint of every shard, for example before a planned shutdown.

## Rebalancing

`auto_rebalance(every, min_items)` moves half of the largest group's widest range to the smallest group when one group holds more than twice the average number of items. `split_by_load(n)` adds a load rule. When the busiest group leads more than `n` writes per second and more than twice the average, its range is split at the median position of recently written keys, and the upper half moves to the least loaded group. Both rules run on the meta-group leader every `every`.

## Replication messages

Each append sent to a follower carries at most 4 MiB of entries (always at least one). The rest follows in the next message, so a burst of large writes on one shard cannot hold up heartbeats and other shards behind a single huge frame.

## Quiescence

`quiesce_after(d)` stops heartbeats on shards with no writes for `d`. Idle traffic then stays flat as shards grow (measured: 58 messages/s with 1 shard, 59 with 64 on three nodes). A write or read wakes the shard first. A quiesced shard whose leader dies fails over after the host-liveness window (1 s) plus the Ladder delay.

## Data directory

| Path | Content |
|---|---|
| `node_id` | Persistent node identity |
| `wal` | Shared write-ahead log for all shards |
| `state.redb` | Checkpointed state of every shard: one row per key or entity, plus session, lease, escrow and routing metadata |
| `state/shard-N/snapshot` | Legacy snapshot files; read once and migrated into `state.redb` |
| `escrow.journal` | Local escrow grants, sales and pending settlements |
