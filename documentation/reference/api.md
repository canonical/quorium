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
| `status()` | `Status` | This node's ID, and the leader and term of shard 0 |
| `shard_status(n)` | `Status` | The same for shard `n` |
| `membership()` | `Membership` | Current voters and learners |
| `stats()` | `Stats` | Counters: appends sent, pings received, quiesced shards, large entity writes |
| `metrics_text()` | `String` | Prometheus text for this node |
| `shards()` | `u64` | Shard count |
| `shard_of::<T>(&key)` | `u64` | Shard holding this entity |
| `routing().await` | `RoutingTable` | Current range-to-shard table |
| `group_sizes().await` | `Vec<usize>` | Item count per shard |
| `split_range(at).await`, `merge_ranges().await`, `move_range(lo, hi, to).await` | `RoutingTable` | Change placement by hand |
| `checkpoint().await` | `Result<(), ClientError>` | Checkpoint every shard now |
| `export(path).await` | `Result<(), Error<Infallible>>` | Write a backup archive |
| `import(path).await` | `Result<(), Error<Infallible>>` | Load a backup into empty shards |
| `shutdown().await` | | Stop the node cleanly |

### Status types

```{code-block} rust
pub struct Status { pub id: u64, pub leader: Option<u64>, pub term: u64, pub is_leader: bool }
pub struct Membership { pub voters: BTreeSet<u64>, pub learners: BTreeSet<u64> }
pub struct Stats { pub appends_sent: u64, pub pings_received: u64, pub quiesced_shards: usize, pub large_entity_writes: u64 }
```

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

| Variant | Meaning |
|---|---|
| `Unavailable(msg)` | No node could serve the request |
| `Timeout` | The total timeout expired |

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
