# Testing

The `quorium::testing` module needs the `testing` feature. Enable it for tests only:

```toml
[dev-dependencies]
quorium = { version = "0.1.0-alpha.1", features = ["testing"] }
```

## `Cluster`

Real nodes in the test process, each with a temporary data directory and its own ports. Nodes run in plaintext with the default transport, spread over the datacenters `dc-a`, `dc-b` and `dc-c`.

| Method | Meaning |
|---|---|
| `Cluster::start(n).await` | Start `n` nodes: 1, 3 or 5. `n = 1` gives a single-node cluster |
| `wait_for_leader(limit).await` | Index of the leader of shard 0, or `None` after `limit` |
| `node(i)` | The `Quorium` of node `i` |
| `kill(i).await` | Stop node `i`. Its data directory is kept for `restart` |
| `restart(i).await` | Start node `i` again on its data directory |
| `running()` | Number of running nodes |
| `any_running()` | Index of some running node |
| `shutdown().await` | Stop every node |

`quorium::testing::free_addrs(n)` returns `n` local addresses that are free for both TCP and UDP.

## `SimCluster`

The same state types on the deterministic simulator: virtual time, a seeded scheduler, a simulated network and disk.

| Method | Meaning |
|---|---|
| `SimCluster::start(seed, n)` | Start `n` simulated nodes. The same seed replays the same history |
| `entity::<T>(key)`, `global::<T>()`, `escrow(name)` | Handles, as on a node |
| `nodes()` | IDs of every node |
| `leader()` | Current leader, if any |
| `now()` | Virtual time since start |
| `run_for(d)` | Advance virtual time by `d` |
| `crash(id)`, `restart(id)` | Stop a node abruptly; start it again on its stored state |
| `partition(left, right)` | Cut every link between the two sets |
| `isolate(id)` | Cut one node off from all others |
| `heal()` | Restore every link |
| `violations()` | Breaches of election safety or state-machine agreement seen so far. Must be empty |

Differences from real nodes:

- `Ctx::now()` is virtual time.
- Escrow sales go through consensus instead of local grants.
- `watch()` never fires.
