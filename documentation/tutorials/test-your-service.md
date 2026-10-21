# Test your service against failures

In this tutorial you write two tests for the guestbook from [Your first cluster](first-cluster.md):

1. one that starts a real three-node cluster in the test process, kills the leader, and checks that no signature is lost;
2. one that runs the cluster on Quorium's deterministic simulator, cuts the network in two, and checks the cluster's safety properties.

## 1. Enable the testing feature

Add this to `Cargo.toml`:

```toml
[dev-dependencies]
quorium = { version = "0.1.0-alpha.1", features = ["testing"] }
```

The `testing` feature adds the `quorium::testing` module. It is only needed by tests.

## 2. Kill the leader

Create `tests/guestbook.rs`:

```rust
use std::time::Duration;

use guestbook::Guestbook;
use quorium::testing::{Cluster, SimCluster};

#[tokio::test(flavor = "multi_thread")]
async fn signatures_survive_the_loss_of_the_leader() {
    let mut cluster = Cluster::start(3).await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let book = cluster.node(leader).entity::<Guestbook>("lobby");
    book.sign("ada".into()).await.unwrap();

    cluster.kill(leader).await;

    let survivor = cluster.any_running();
    let book = cluster.node(survivor).entity::<Guestbook>("lobby");
    assert_eq!(book.sign("grace".into()).await, Ok(2));
    assert_eq!(book.names().await.unwrap(), vec!["ada".to_string(), "grace".to_string()]);
    cluster.shutdown().await;
}
```

Run it:

```sh
cargo test signatures_survive
```

```text
test signatures_survive_the_loss_of_the_leader ... ok
```

`Cluster::start(3)` started three real nodes, each with its own temporary data directory and ports. `kill` stopped the leader. The write to the survivor waited for a new leader to be elected, then succeeded, and the read shows both signatures.

## 3. Partition the network in simulation

Add a second test to the same file:

```rust
#[tokio::test]
async fn the_majority_side_of_a_partition_keeps_accepting_signatures() {
    let sim = SimCluster::start(7, 3);
    let book = sim.entity::<Guestbook>("lobby");
    book.sign("ada".into()).await.unwrap();

    let leader = sim.leader().unwrap();
    let others: Vec<u64> = sim.nodes().into_iter().filter(|n| *n != leader).collect();
    sim.partition(&[leader], &others);
    sim.run_for(Duration::from_secs(2));

    assert_eq!(book.sign("grace".into()).await, Ok(2));
    sim.heal();
    assert!(sim.violations().is_empty());
}
```

Run it:

```sh
cargo test majority_side
```

```text
test the_majority_side_of_a_partition_keeps_accepting_signatures ... ok
```

This test finished almost instantly, even though it simulated two seconds. `SimCluster` runs your state types on a simulated network and disk with virtual time:

- `7` is the seed. The same seed always produces the same history, so a failing test can be replayed exactly.
- `partition` cut the old leader off from the other two. The majority side elected a new leader, and the write went through it.
- `violations()` reports any breach of election safety (two leaders in one term) or state-machine agreement (two nodes applying different entries at the same position). It must be empty.

## 4. Try a different seed

Change `7` to another number and run the test again. Each seed schedules messages and timers differently. Running many seeds is how Quorium itself is tested; see [How Quorium is verified](../explanation/verification.md).

## What you learned

- `Cluster` runs real nodes inside a test. Use it to check behaviour end to end.
- `SimCluster` runs the same state types deterministically, in milliseconds, with injected faults. Use it to explore failure scenarios.

## Next

- [Testing reference](../reference/testing.md) lists every method of `Cluster` and `SimCluster`.
- [How-to guides](../how-to/index.md) cover the tasks you will meet in a real service.
