# Builder options

`Quorium::builder()` returns a `Builder`. Every method takes and returns the builder by value. `start().await` returns `Result<Quorium, StartError>`.

## Required

| Option | Type | Meaning |
|---|---|---|
| `data_dir(dir)` | `impl AsRef<Path>` | Directory for this node's identity, log and state. Created if missing |
| `bind(addr)` | `SocketAddr` | Local address to listen on |
| `tls(config)` or `insecure_plaintext()` | `TlsConfig` | Mutual TLS for every connection, or an explicit opt-out for development |

## Cluster

| Option | Type | Default | Meaning |
|---|---|---|---|
| `advertise(addr)` | `SocketAddr` | the `bind` address | Address other nodes use to reach this one |
| `discovery(d)` | `Discovery` | `Discovery::None` | How to find the other nodes; see below |
| `replicas(n)` | `usize` | `3` | Number of voters: 3 or 5 |
| `datacenter(name)` | `&str` | `"default"` | This node's failure domain |
| `single_node_dev_mode()` | | off | Start a one-node cluster. `replicas` is then not checked |
| `replacement_grace(d)` | `Duration` | 10 minutes | How long a voter may be unreachable before a learner replaces it |

## Timing

| Option | Type | Default | Meaning |
|---|---|---|---|
| `heartbeat(d)` | `Duration` | 50 ms | Leader heartbeat interval. Set to 2–3× the largest round trip between voters. At least 2 ms |
| `leader_lease(d)` | `Duration` | 2 × heartbeat | How long a leader keeps accepting writes after a majority last acknowledged it. Followers refuse to vote for another candidate for the same time. At least 2 × heartbeat |
| `rpc_timeout(d)` | `Duration` | 1 s | Minimum deadline of a consensus request (heartbeat, append, vote, transfer) |
| `snapshot_timeout(d)` | `Duration` | 60 s | Deadline for sending a full state snapshot to a lagging follower |
| `write_wait(d)` | `Duration` | 2 s | How long a leader whose lease has lapsed holds a write before answering that there is no leader |
| `ladder(config)` | `LadderConfig` | derived | Election timing; see [Ladder settings](#ladder-settings) |
| `handoff_suspend(d)` | `Duration` | 30 s | After an election, how long to wait before moving leadership to its preferred datacenter |
| `quiesce_after(d)` | `Duration` | never | Stop heartbeats on shards idle for this long |

`node.timings()` returns the values in effect after derivation, and `node.failover_estimate()` estimates how long a failover takes; see [Timings](api.md#timings).

### Ladder settings

Without `ladder(...)`, the defaults below are used, with `base` raised to at least `leader_lease + heartbeat`. A `LadderConfig` passed to `ladder(...)` is used exactly as given, and `start` fails with `StartError::Timing` if it conflicts with the other settings.

| Field | Default | Meaning |
|---|---|---|
| `base` | 150 ms | Silence from the leader before the first-ranked follower stands. Must be at least `leader_lease + heartbeat` |
| `step` | 50 ms | Extra wait per rank. Raised to the measured consensus round trip |
| `campaign_timeout` | 60 ms | First campaign's time limit. Raised to 3 round trips. At most `max_campaign` |
| `max_campaign` | 300 ms | Longest a campaign may run. Raised to 6 round trips |
| `fast_retry`, `flat_retry` | 20 ms, 100 ms | Pause after a first and a later failed campaign |
| `liveness_window` | 1 s | How recently a peer must have been heard to count as live |
| `handoff_suspend` | 30 s | Same as `handoff_suspend(d)` |

## Data placement

| Option | Type | Default | Meaning |
|---|---|---|---|
| `shards(n)` | `u64` | `1` | Number of shards. Fixed when the cluster is created |
| `auto_rebalance(every, min_items)` | `Duration`, `usize` | off | Move ranges from a shard holding more than twice the average item count |
| `split_by_load(n)` | `u64` | off | Split the range of a shard leading more than `n` writes per second |

## Storage and transport

| Option | Type | Default | Meaning |
|---|---|---|---|
| `checkpoint_every(n)` | `u64` | 5,000 | Log entries between state checkpoints |
| `log_retention(n)` | `u64` | 1,000, or `checkpoint_every` if smaller | Log entries kept behind a checkpoint. A follower that falls further behind needs a full snapshot |
| `state_backend(b)` | `StateBackend` | `Redb` | `Redb`, or `Fjall` with the `fjall` feature |
| `transport(t)` | `Transport` | `Quic` (`Tcp` without the `quic` feature) | Protocol between nodes. All nodes must match |

## Other

| Option | Type | Meaning |
|---|---|---|
| `metrics_bind(addr)` | `SocketAddr` | Serve Prometheus metrics at `http://addr/metrics` |
| `register::<T>()` | state type | Register a state type explicitly, for platforms that drop link-time sections |

## Discovery

| Variant | Resolves to |
|---|---|
| `Discovery::None` | No peers. Only useful with `single_node_dev_mode()` |
| `Discovery::Static(addrs)` | The given addresses |
| `Discovery::Dns { name, port }` | Every address `name` resolves to, with `port` |
| `Discovery::Kubernetes { service, namespace, port }` | Every address of `service.namespace.svc.cluster.local`, with `port` |
| `Discovery::Mdns { service }` | Nodes announcing `_service._tcp.local.` on the local network. Needs the `mdns` feature |

Names are resolved again periodically, so new instances are found as they appear.

## Forming the first cluster

A node that finds no existing cluster waits until `replicas` seeds are reachable. Then only the seed with the lowest node ID initialises the cluster, so two clusters can never form. A node that finds an existing cluster joins it as a learner and never initialises a new one.

## `StartError`

| Variant | Cause |
|---|---|
| `MissingDataDir` | `data_dir` was not set |
| `MissingBind` | `bind` was not set |
| `TlsRequired` | Neither `tls` nor `insecure_plaintext` was set |
| `InvalidReplicas(n)` | `replicas` is not 3 or 5, outside single-node mode |
| `Io(e)` | The data directory or the listening socket failed |
| `Storage(e)` | Stored state could not be opened, or uses a different backend |
| `Raft(e)` | The consensus engine failed to start |
| `UnknownType(name)` | Stored state holds a type this binary does not register |
| `Timing(e)` | The timing settings conflict, for example a ladder `base` shorter than `leader_lease + heartbeat`. The message names both values |
