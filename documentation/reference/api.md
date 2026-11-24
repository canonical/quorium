# API

The main types of the `quorium` crate. Generated API documentation is on [docs.rs](https://docs.rs/quorium).

## `Quorium`

A running node. Cheap to clone; all clones share the node.

### State

| Method | Returns | Meaning |
|---|---|---|
| `entity::<T>(key)` | `T`'s handle | Handle to the entity of type `T` with this key |
| `global::<T>()` | `T`'s handle | Same as `entity::<T>(())`, for `key = ()` types |
| `escrow(name)` | `EscrowCounter` | Standalone escrow counter; see [Escrow](escrow.md) |
| `singleton(name, job)` | `Singleton` | Run `job` on at most one node; see [Singletons](#singletons) |
| `singleton_with(name, ttl, job)` | `Singleton` | Same, with a lease of `ttl` instead of 5 s |
| `write(key, bytes)` | `Result<Reply, ClientError>` | Write a raw key-value pair |
| `read(key)` | `Result<Option<Vec<u8>>, ClientError>` | Read a raw key-value pair |

### Cluster

| Method | Returns | Meaning |
|---|---|---|
| `status()` | `Status` | This node's ID, and the leader, term and leadership ages of shard 0 |
| `shard_status(n)` | `Status` | The same for shard `n` |
| `membership()` | `Membership` | Current voters and learners |
| `stats()` | `Stats` | Counters: traffic, elections, leader changes, failed requests, time without a leader, snapshots sent |
| `is_stable(window)` | `bool` | Every shard has had the same leader for `window`, no election started within `window`, and every voter was heard from recently |
| `leadership_events()` | `impl Stream<Item = LeadershipEvent>` | Elections, leader changes and peer reachability as they happen; see [Leadership events](#leadership-events) |
| `timings()` | `Timings` | Timing values in effect; see [Timings](#timings) |
| `failover_estimate()` | `FailoverEstimate` | Estimated failover time at the measured round trip |
| `metrics_text()` | `String` | Prometheus text for this node |
| `shards()` | `u64` | Shard count |
| `shard_of::<T>(&key)` | `u64` | Shard holding this entity |
| `routing().await` | `RoutingTable` | Current range-to-shard table |
| `group_sizes().await` | `Vec<usize>` | Item count per shard |
| `split_range(at).await`, `merge_ranges().await`, `move_range(lo, hi, to).await` | `RoutingTable` | Change placement by hand |
| `checkpoint().await` | `Result<(), ClientError>` | Checkpoint every shard now |
| `export(path).await` | `Result<(), Error<Infallible>>` | Write a backup archive |
| `import(path).await` | `Result<(), Error<Infallible>>` | Load a backup into empty shards |
| `step_down(options).await` | `Result<HandoffReport, HandoffError>` | Hand leadership of every shard to other voters; see [Handoff and shutdown](#handoff-and-shutdown) |
| `shutdown_gracefully(deadline).await` | `ShutdownReport` | Hand off, drain writes, then stop and sync storage |
| `shutdown().await` | | Stop at once, without handing off leadership |

### Status types

```{code-block} rust
pub struct Status {
    pub id: u64,
    pub leader: Option<u64>,
    pub term: u64,
    pub is_leader: bool,
    pub leader_age: Option<Duration>,
    pub quorum_contact_age: Option<Duration>,
}
pub struct Membership { pub voters: BTreeSet<u64>, pub learners: BTreeSet<u64> }
pub struct Stats {
    pub appends_sent: u64,
    pub pings_received: u64,
    pub quiesced_shards: usize,
    pub large_entity_writes: u64,
    pub elections_started: u64,
    pub leader_changes: u64,
    pub rpc_failures: u64,
    pub leaderless: Duration,
    pub snapshots_sent: BTreeMap<u64, u64>,
}
```

`Status` and `Stats` are `#[non_exhaustive]`.

- `leader_age`: how long the shard has had its current leader, as seen by this node.
- `quorum_contact_age`: on the leader only, how long ago a majority last acknowledged it. It approaches the leader lease just before the leader stops accepting writes.
- `elections_started` and `leader_changes`: counted by this node over all shards.
- `rpc_failures`: consensus requests to peers that failed or timed out.
- `leaderless`: total shard time this node spent without a known leader.
- `snapshots_sent`: full state snapshots this node sent, by peer. A count that keeps rising means a follower cannot catch up.

## Timings

```{code-block} rust
pub struct Timings {
    pub heartbeat: Duration,
    pub leader_lease: Duration,
    pub rpc_timeout: Duration,
    pub snapshot_timeout: Duration,
    pub ladder: LadderConfig,
    pub write_wait: Duration,
}
pub struct FailoverEstimate {
    pub detection: Duration,
    pub ranking: Duration,
    pub campaign: Duration,
    pub establish: Duration,
    pub total: Duration,
}
```

`timings.stall_tolerance()` is `leader_lease - heartbeat`: the longest a majority may stay silent without the leader losing its lease.

`failover_estimate()` adds up, at the consensus round trip measured now:

| Part | Value |
|---|---|
| `detection` | The ladder `base` |
| `ranking` | `step` for each voter that may stand ahead of the best surviving one |
| `campaign` | `max_campaign` |
| `establish` | One round trip for the new leader's lease |

It assumes the round trip stays as measured and one campaign succeeds. Nodes that stall, or campaigns that fail, take longer. The Ladder also stretches `base` up to 8 times when heartbeats arrive irregularly. Treat the estimate as a lower bound for sizing timeouts that depend on failover, not as a guarantee.

## Leadership events

```{code-block} rust
#[non_exhaustive]
pub enum LeadershipEvent {
    ElectionStarted { shard: u64, term: u64, pre_vote: bool },
    LeaderChanged { shard: u64, term: u64, leader: Option<u64> },
    PeerUnreachable { shard: u64, peer: u64 },
    PeerReachable { shard: u64, peer: u64 },
    Lagged { missed: u64 },
}
```

Events describe what this node observes. `ElectionStarted` means this node started a campaign for `term`. `LeaderChanged` with `leader: None` means the shard lost its leader. Reachability events are sent when consensus requests to a peer start or stop failing, not on every failure. A subscriber that falls more than 1,024 events behind receives `Lagged` and continues with newer events.

## Handoff and shutdown

```{code-block} rust
pub struct StepDown { pub deadline: Duration, pub stand_aside: Duration, pub prefer: Option<u64> }
pub struct HandoffReport { pub shards: Vec<ShardHandoff> }
pub struct ShardHandoff { pub shard: u64, pub new_leader: Option<u64>, pub took: Duration }
#[non_exhaustive]
pub enum HandoffError { StillLeading { shards: Vec<u64>, report: HandoffReport } }
#[non_exhaustive]
pub struct ShutdownReport {
    pub handoff: Result<HandoffReport, HandoffError>,
    pub writes_drained: bool,
    pub storage: std::io::Result<()>,
    pub took: Duration,
}
```

`step_down(options)`:

1. Stops this node from standing for election for `stand_aside` (default 60 s), and tells its peers, so they neither rank it as a candidate nor move leadership back to it.
2. For each shard this node leads, picks a live voter: `prefer` if it qualifies, otherwise the most caught-up one, then the one leading the fewest shards.
3. Holds new writes on the shard, waits for writes in progress, waits until the target has every log entry, transfers leadership and waits until the target leads. Held writes then go to the new leader. A write held longer than `write_wait` is answered as if there were no leader, and the caller retries it; none is dropped.
4. Returns once every shard has moved, or with `HandoffError::StillLeading` when `deadline` (default 10 s) expires or no voter qualifies.

A node that leads nothing returns an empty report at once.

`shutdown_gracefully(deadline)` stops leadership moves, steps down with three quarters of `deadline`, refuses new writes with `ClientError::Shutdown`, waits for writes in progress, stops consensus and the server, then syncs and closes the log and the state database. `report.is_clean()` is `true` when every step succeeded. A single-node cluster cannot hand off, so its report is never clean.

## Entity handles

`node.entity::<T>(key)` returns a handle generated for `T`. It has one `async` method per `pub fn` of the `#[quorium::state]` block, plus:

| Method | Returns | Meaning |
|---|---|---|
| `get()` | `Result<T, Error<Infallible>>` | The whole state. `T::default()` if the entity does not exist |
| `read(\|state\| ...)` | `Result<R, Error<Infallible>>` | Run a closure on the state |
| `read_result(\|state\| ...)` | `Result<R, Error<E>>` | Same, for a closure returning `Result<R, E>` |
| `exists()` | `Result<bool, Error<Infallible>>` | Whether the entity has been written |
| `delete()` | `Result<(), Error<Infallible>>` | Remove the entity |
| `watch()` | `Watch<T>` | Follow changes; see below |
| `stale()` | handle | A handle whose reads use local state without a check |

Handles are cheap to clone and can be kept and reused.

### Stale reads

Reads through `stale()` run on this node's replica at once. They may miss recent writes, but they keep working when the node cannot reach a majority. Writes through a stale handle are ordinary replicated writes.

### `Watch<T>`

`watch.changed().await` waits for the next change and returns `Result<T, Error<Infallible>>`. Changes arrive in commit order. A slow consumer skips to the latest state. A deleted entity is reported as `T::default()`. Returns `Err(Error::Shutdown)` when the node stops.

## Errors

### `Error<E>`

Returned by handle methods. `E` is the method's own error type, or `Infallible`. The enum is `#[non_exhaustive]`.

| Variant | Meaning | Retryable |
|---|---|---|
| `Rejected(E)` | The write method returned `Err(e)`. Nothing changed | No |
| `SoldOut` | An escrow sale exceeds the remaining units | No |
| `Unavailable` | No leader can be reached, the method is not yet enabled on every voter, or an escrow copy was not confirmed | Yes |
| `Timeout` | No answer in time. The write may or may not have been applied | Yes |
| `Shutdown` | This node is shutting down | No |
| `Internal(msg)` | An unexpected failure, such as undecodable state | No |

`error.is_retryable()` is `true` for `Unavailable` and `Timeout`. Retrying a write is always safe: each call carries a request ID and is applied at most once.

### `ClientError`

Returned by raw key-value calls and `Client`.

`#[non_exhaustive]`.

| Variant | Meaning | Retryable |
|---|---|---|
| `Unavailable(msg)` | No node could serve the request | Yes |
| `NoLeader { for_at_least }` | Every node asked reported that it knows no leader, or a leader without a majority, for at least this long | Yes |
| `Timeout` | The total timeout expired | Yes |
| `Shutdown` | This node is shutting down | No |

`NoLeader` usually clears once an election completes; retry after a short pause. Entity handles report it as `Error::Unavailable` and `Shutdown` as `Error::Shutdown`.

## Singletons

```{code-block} rust
pub fn singleton<F, Fut>(&self, name: &str, job: F) -> Singleton
where
    F: Fn(Lease) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static;
```

`job` is called each time this node acquires the lease named `name`, and its future is dropped when the lease is lost. `Lease { token: u64 }` carries a fencing token, strictly larger for every new holder. Dropping the returned `Singleton` stops the job on this node.

## Remote client

`quorium::Client` reaches a cluster from a process that is not a node.

| Method | Meaning |
|---|---|
| `Client::new(seeds, tls)` | `seeds: Vec<SocketAddr>`, `tls: Option<TlsConfig>` |
| `.with_transport(t)` | `Transport::Quic` (default) or `Transport::Tcp` |
| `.with_timeouts(total, attempt)` | Defaults: 10 s total, 1 s per attempt |
| `.write(key, bytes).await` | `Result<Reply, ClientError>` |
| `.read(key).await` | `Result<Option<Vec<u8>>, ClientError>` |
| `.read_escrow(name).await` | The stored state of a standalone escrow counter, or `None` |

`attempt` is how long the client waits for one node before also asking another, without cancelling the first. At most 3 attempts are in flight. After the first replies, the next attempt starts after twice the average reply time, clamped between 50 ms and `attempt`.

## TLS types

| Type | Meaning |
|---|---|
| `TlsConfig { ca_pem, cert_pem, key_pem, server_name }` | PEM strings for the cluster CA, this node's certificate and key, and the name peers verify |
| `TlsMaterial::generate_ca(name)` | Create a CA in memory |
| `ca.issue(name)` | Issue a certificate for `name` with a fresh key |
| `material.config(&ca)` | Build a `TlsConfig` |
