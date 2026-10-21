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
| `heartbeat(d)` | `Duration` | 50 ms | Leader heartbeat interval. Set to 2–3× the largest round trip between voters |
| `handoff_suspend(d)` | `Duration` | 30 s | After an election, how long to wait before moving leadership to its preferred datacenter |
| `quiesce_after(d)` | `Duration` | never | Stop heartbeats on shards idle for this long |

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
