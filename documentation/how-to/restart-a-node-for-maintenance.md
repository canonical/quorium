# Restart a node for maintenance

Use this guide before you stop a voter on purpose: to upgrade it, reboot its host or move its disk. It moves leadership away first, so the remaining voters never need an election while the node is gone.

## 1. Wait until the cluster is stable

Stop a voter only when every shard has a settled leader and every voter is reachable. Otherwise the remaining voters may be one failure away from losing their majority.

```rust
while !node.is_stable(Duration::from_secs(10)) {
    tokio::time::sleep(Duration::from_millis(500)).await;
}
```

`is_stable(window)` is `true` when, as seen from this node, every shard has had the same leader for at least `window`, no election started within `window`, and every voter was heard from within the liveness window (1 s). Pick a `window` of several heartbeats; 10 s suits most clusters.

## 2. Shut down gracefully

```rust
let report = node.shutdown_gracefully(Duration::from_secs(20)).await;
if !report.is_clean() {
    eprintln!("shutdown was not clean: {report:?}");
}
```

This:

1. Hands leadership of every shard this node leads to another voter, holding new writes on each shard for the short time it takes the target to catch up. Held writes go to the new leader.
2. Marks the node as standing aside, so it does not win leadership back, and tells the other voters so they do not move leadership to it.
3. Refuses new writes on this node with `ClientError::Shutdown` and waits for writes in progress.
4. Stops consensus and the network server, then syncs and closes the log and the state database.

The report says which steps completed. A failed handoff is not fatal: the remaining voters then elect a new leader as they would after a crash.

## 3. Stop the process, do the work, start it again

Start the node on the same data directory. It rejoins as a follower and catches up from the log, or from a snapshot if it was away for longer than the log retention (see `log_retention` in the [builder reference](../reference/builder.md#storage-and-transport)).

Before moving to the next node, wait until the restarted one has caught up: its `quorium_shard_last_applied` matches the others, and `is_stable` is `true` again.

## Hand off leadership without stopping

To move leadership away and keep the node running, for example before a risky operation on its host:

```rust
let report = node.step_down(StepDown { deadline: Duration::from_secs(10), ..StepDown::default() }).await?;
for moved in &report.shards {
    println!("shard {} now led by {:?} after {:?}", moved.shard, moved.new_leader, moved.took);
}
```

The node stays a follower and stands aside from elections for `stand_aside` (60 s by default). Set `prefer` to choose the voter that should lead.

## Size timeouts that depend on the cluster

If your service stops itself when it cannot write for a while, for example to fence work it might no longer own, make that timeout longer than a failover:

```rust
let estimate = node.failover_estimate();
if estimate.total > Duration::from_secs(6) {
    return Err(format!("failover may take {:?}", estimate.total).into());
}
```

The estimate assumes the round trip measured now and one successful campaign; leave a margin. A planned restart with `shutdown_gracefully` needs no failover at all.

## Related

- [Upgrade a cluster](upgrade-a-cluster.md)
- [Monitor a cluster](monitor-a-cluster.md)
- [API reference](../reference/api.md#handoff-and-shutdown)
