# Quorium user guide

Quorium is an embedded Rust library. Every instance of your service is a cluster node, your shared state is plain Rust types, and failover needs no human.

## Add the dependency

```toml
[dependencies]
quorium = "0.1.0-alpha.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

Enable the `testing` feature in `[dev-dependencies]` to get `quorium::testing::Cluster`.

## Define state

A state type is a plain struct with three derives and one attribute block.

```rust
use quorium::{Ctx, Error};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Cart {
    items: Vec<String>,
    checked_out: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CartError {
    CheckedOut,
}

#[quorium::state]
impl Cart {
    pub fn add(&mut self, item: String) -> Result<usize, CartError> {
        if self.checked_out {
            return Err(CartError::CheckedOut);
        }
        self.items.push(item);
        Ok(self.items.len())
    }

    pub fn checkout(&mut self, ctx: &Ctx) -> u64 {
        self.checked_out = true;
        ctx.index()
    }

    pub fn total(&self) -> usize {
        self.items.len()
    }
}
```

Rules, all checked at compile time:

| Rule | Why |
|---|---|
| `&mut self` methods are replicated writes | They run on every replica, in log order |
| Writes must be synchronous and non-generic | Every replica must reach the same result |
| Arguments are `Serialize + DeserializeOwned + Send` | They travel through the log |
| An optional first parameter `ctx: &Ctx` is supplied by Quorium | Deterministic `now()`, `random()` and `index()` |
| `ctx` is not available to reads | Reads run locally and have no log position |
| `&self` methods are reads | They run on local state after a linearizable barrier |
| Each write has a stable ID: a hash of its name, or `#[quorium(id = N)]` | Renaming a method must not change its wire identity |
| Duplicate IDs are rejected | Two methods cannot share a dispatch slot |

`#[state(...)]` options:

| Option | Default | Meaning |
|---|---|---|
| `key = Type` | `String` | Key type: `String`, integers, `bool`, `char`, `()`, tuples, `uuid::Uuid` (feature `uuid`) |
| `name = "..."` | the type name | Storage identity. Keep it stable forever |
| `version = N` | `1` | Bump when the encoding changes |
| `migrate = path::to_fn` | none | `fn(from: u32, bytes: &[u8]) -> Result<Self, MigrationError>` |

## Start a node

```rust
use quorium::{Discovery, Quorium};

let node = Quorium::builder()
    .data_dir("/var/lib/shop")
    .bind("0.0.0.0:7000".parse()?)
    .advertise("10.0.0.5:7000".parse()?)
    .datacenter("eu-west-1a")
    .discovery(Discovery::Kubernetes { service: "shop".into(), namespace: "prod".into(), port: 7000 })
    .replicas(3)
    .tls(tls_config)
    .start()
    .await?;
```

- `discovery`: `Static(addrs)`, `Dns { name, port }`, `Kubernetes { service, namespace, port }`, or `Mdns { service }` with the `mdns` feature. Names are re-resolved continuously. mDNS is for development on one network: each node announces `_<service>._tcp.local.` and browses for the others.
- `replicas`: 3 or 5. The first cluster forms only once that many seeds are reachable, and only the seed with the lowest node ID initialises it, so two clusters can never form. A node that sees an existing cluster never bootstraps a new one.
- New nodes join as learners. The leader promotes them while voters are below `replicas`, and replaces a voter that has been unreachable for `replacement_grace` (10 minutes by default) with a caught-up learner. Membership never shrinks below `replicas`.
- TLS is mutual and required. Every node and client presents a certificate signed by the cluster CA and carrying the cluster name (the `server_name` of `TlsConfig`, which peers verify). `ca.issue("shop-cluster")` issues one, with a fresh key each call. `insecure_plaintext()` opts out explicitly, for development only.
- Traffic runs over QUIC by default (feature `quic`, on by default). `transport(Transport::Tcp)` uses TCP instead, which costs about half the CPU per message but has a worse tail latency under packet loss. Every node of a cluster must use the same transport; QUIC needs the bind port open for UDP. See [benchmarks/transport.md](benchmarks/transport.md).
- `checkpoint_every(n)` sets how many log entries pass between state checkpoints (default 5,000).
- `single_node_dev_mode()` starts a one-node cluster. It must be requested explicitly.

## Use state

```rust
let cart = node.entity::<Cart>("alice");
cart.add("apple".into()).await?;
let n = cart.total().await?;
let index = cart.checkout().await?;

match cart.add("pear".into()).await {
    Err(Error::Rejected(CartError::CheckedOut)) => {}
    Err(e) if e.is_retryable() => {}
    other => panic!("{other:?}"),
}
```

Every handle also has `get()`, `read(|state| ...)`, `exists()`, `delete()` and `watch()`.

A type with `key = ()` has a single instance per cluster: `node.global::<Settings>()` is the same as `node.entity::<Settings>(())`.

`handle.stale()` returns a handle whose reads run on this node's local state at once, without asking the leader. They may lag behind recent writes, and they keep working when this node cannot reach a majority. Writes made through a stale handle are ordinary replicated writes.

Types register themselves at link time. If your platform drops link-time sections, register them explicitly with `Quorium::builder().register::<Cart>()`.

A method returning `Result<T, E>` becomes `Result<T, quorium::Error<E>>` on the handle; any other return type `T` becomes `Result<T, quorium::Error<Infallible>>`. `Error` is `#[non_exhaustive]`: `Rejected(E)`, `SoldOut`, `Unavailable`, `Timeout`, `Shutdown`, `Internal(String)`. Only `Unavailable` and `Timeout` are retryable, and retrying is safe: every write carries a client request ID and applies exactly once.

## Guarantees

| Operation | Consistency | On the minority side of a partition | Durable when acknowledged |
|---|---|---|---|
| Write | Linearizable per entity | Fails with `Unavailable` | On a majority of disks, fsynced |
| Read | Linearizable: served at once by the leader while its lease is valid, otherwise confirmed with a majority (ReadIndex) | Fails with `Unavailable` | Not applicable |
| Stale read (`.stale()`) | Local replica state, possibly behind | Served | Not applicable |

Writes to different entities are not atomic together. Compose them like `examples/shop/src/lib.rs` does: reserve, then compensate on failure.

## Remote clients

A process that does not run a node talks to the cluster through `quorium::Client`:

```rust
let client = quorium::Client::new(seed_addrs, Some(tls))
    .with_timeouts(Duration::from_secs(10), Duration::from_secs(1));
client.write("key", value).await?;
```

`with_timeouts(total, attempt)` sets two limits:

- **total** bounds the whole call;
- **attempt** is the longest the client waits for one node before also trying another.

The client follows leader redirects. It does not cancel a slow attempt: it keeps the attempt in flight and sends a copy elsewhere, up to 3 at a time. The first reply wins, and the request ID makes the copies apply once. A leader whose commits are slow therefore still answers, and a dead leader costs one attempt slot, not the whole call.

Once the client has seen replies, it sends the extra copy after twice its average reply time, between 50 ms and `attempt`. That is what keeps failover short.

## Test your service

```rust
use quorium::testing::Cluster;

#[tokio::test(flavor = "multi_thread")]
async fn checkout_survives_leader_loss() {
    let mut cluster = Cluster::start(3).await;
    let leader = cluster.wait_for_leader(std::time::Duration::from_secs(10)).await.unwrap();
    cluster.node(0).entity::<Cart>("alice").add("apple".into()).await.unwrap();
    cluster.kill(leader).await;
    let any = cluster.any_running();
    assert_eq!(cluster.node(any).entity::<Cart>("alice").total().await, Ok(1));
    cluster.shutdown().await;
}
```

`Cluster::start(1)` gives a single-node cluster for fast unit tests.

`SimCluster` runs the same state types on the deterministic simulator: virtual time, a seeded scheduler and injected faults. A test finishes in milliseconds, and the same seed always replays the same history.

```rust
use quorium::testing::SimCluster;

#[tokio::test]
async fn cart_survives_a_partition() {
    let sim = SimCluster::start(42, 3);
    let cart = sim.entity::<Cart>("alice");
    cart.add("apple".into()).await.unwrap();
    let leader = sim.leader().unwrap();
    let others: Vec<u64> = sim.nodes().into_iter().filter(|n| *n != leader).collect();
    sim.partition(&[leader], &others);
    sim.run_for(std::time::Duration::from_secs(2));
    assert_eq!(cart.add("pear".into()).await, Ok(2));
    sim.heal();
    assert!(sim.violations().is_empty());
}
```

`SimCluster` offers `crash`, `restart`, `partition`, `isolate`, `heal`, `run_for`, `leader`, `now` and `violations` (election safety and state-machine agreement). In simulation, `Ctx::now()` is virtual time. Escrow sales go straight through consensus rather than local grants, and `watch()` never fires.

## Operate

- Data lives in `data_dir`: `node_id`, the shared write-ahead log `wal`, and checkpointed state in the redb database `state.redb`.
- The log is fsynced in groups, so one fsync covers every concurrent write on the node. State is checkpointed into redb incrementally and the log is compacted behind each checkpoint.
- Startup refuses to run if the stored state holds a type this binary does not register, naming the type.
- `quorium-node` (in `crates/quorium/src/bin`) runs a bare node for experiments: `quorium-node --data-dir d --bind 127.0.0.1:7000 --seeds a,b,c --insecure-plaintext`.

## Shards and locality

```rust
let node = Quorium::builder()
    .shards(64)
    .quiesce_after(Duration::from_secs(5))
    // ...
    .start()
    .await?;
```

Each shard is its own Raft group; entities are placed by a hash of their type and key. `auto_rebalance(every, min_items)` and `split_by_load(writes_per_second)` move ranges between groups as data and load grow (see [OPERATIONS.md](OPERATIONS.md)). Add `locality = f` to place a region's entities together, so that their shard's leadership follows the region that writes them:

```rust
#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = (String, u64), locality = region_of)]
pub struct Profile { visits: u64 }

fn region_of(key: &(String, u64)) -> String { key.0.clone() }
```

## Watch changes

```rust
let mut watch = node.entity::<Cart>("alice").watch();
while let Ok(cart) = watch.changed().await {
    println!("{} items", cart.items.len());
}
```

Changes arrive in commit order on any replica. A slow consumer skips to the latest version. A deleted entity is reported as `Default::default()`.

## Singleton jobs

```rust
let _job = node.singleton("billing", |lease: quorium::Lease| async move {
    run_billing(lease.token).await;
});
```

At most one node runs the job. The job stops if its lease cannot be renewed, and the next runner gets a strictly higher `lease.token`. Pass the token to external systems and have them reject smaller tokens: inside Quorium "at most one" holds; outside, only the fencing token makes it hold.

## Escrow

An `Escrow<T>` field holds a stock of units inside an entity. Writes refill it and read its estimate; the handle gets one method per escrow field that sells from it.

```rust
use quorium::Escrow;

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Concert {
    title: String,
    seats: Escrow<u64>,
}

#[quorium::state]
impl Concert {
    pub fn open(&mut self, title: String, seats: u64) {
        self.title = title;
        self.seats.refill(seats);
    }

    pub fn capacity(&self) -> u64 {
        self.seats.estimate()
    }
}

let concert = node.entity::<Concert>("gig");
concert.open("Gig".into(), 500).await?;
concert.seats().try_take(2).await?;
```

Standalone counters work the same way through `node.escrow(name)`, which returns an `EscrowCounter`:

```rust
let tickets = node.escrow("concert-42");
tickets.refill(1_000).await?;
match tickets.try_take(2).await {
    Ok(()) => {}
    Err(Error::SoldOut) => {}
    Err(e) => return Err(e.into()),
}
```

Each node holds a grant of units and sells from it locally in microseconds. Grants shrink as stock runs low, unused units are released after 2 seconds idle, and when grants are exhausted `try_take` falls back to a consensus purchase. Total sales never exceed stock. `local_remaining()`, `estimate()` and `sold()` report the state.

Where a sale is recorded:

- Every sale is journaled on the local disk before `try_take` returns.
- A grant names one live node in the same datacenter as its copy peer. A sale is acknowledged only after that peer has durably recorded it. If the peer does not confirm, the sale is rolled back and `try_take` returns `Error::Unavailable`.
- A node with no live peer in its datacenter buys through consensus instead of holding a grant. The only exception is a cluster of one node.

When a node is removed from the cluster, its copy peer reclaims the node's grants. The peer first stops accepting copies from the removed node, so the removed node can no longer complete a sale even if it is still running. It then settles every sale it recorded and returns the rest of the grant to the stock. If the copy peer was removed too, the whole outstanding grant is counted as sold: units can be lost that way, but never sold twice.

## Backup, metrics, operations

See [OPERATIONS.md](OPERATIONS.md).
