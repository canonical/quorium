# Release notes

## 0.1.0-alpha.2

### Features

- `step_down` hands leadership of every shard to other voters while writes keep flowing: new writes are held briefly while the target catches up, then go to the new leader.
- `shutdown_gracefully` hands off leadership, drains writes, then syncs and closes the log and the state database, and reports which steps completed.
- A node can stand aside from elections; its peers skip it when ranking candidates and moving leadership. The TLA+ model covers it.
- New timing settings: `leader_lease`, `rpc_timeout`, `snapshot_timeout`, `write_wait` and `ladder`. Conflicting settings fail with `StartError::Timing` instead of being adjusted silently. `timings()` and `failover_estimate()` report the values in effect.
- `leadership_events()` streams elections, leader changes and peer reachability. `Status` reports leader and quorum-contact ages, `Stats` counts elections, leader changes, failed requests, time without a leader and snapshots sent, and `is_stable(window)` tells when it is safe to stop a voter.
- `log_retention` keeps more log behind each checkpoint, so a briefly absent follower can catch up without a snapshot.
- New metrics for elections, leader changes, failed requests, time without a leader, snapshots sent per peer, leader and quorum-contact ages, and the purged log index.

### Fixes

- Snapshot transfers had a 200 ms deadline, so a follower more than 200 ms of round trip away could never catch up by snapshot and the same snapshot was resent indefinitely. They now get `snapshot_timeout` (60 s by default).
- A write on a leader whose lease has lapsed now backs off exponentially and shares one heartbeat with other waiting writes, instead of triggering a heartbeat every 10 ms.
- Planned leadership transfers (rebalancing, following the writer) wait until the target has caught up and confirm that it took over.

### Breaking changes

- `ClientError`, `Status` and `Stats` are `#[non_exhaustive]`. `ClientError` gains `NoLeader { for_at_least }` and `Shutdown`.
- The default leader lease is exactly two heartbeats; it was two heartbeats plus 1 ms.
- Peers gained a `StandAside` message. Older nodes ignore it, so mixed-version clusters keep working, but an older node may still move leadership to a node that is standing aside.

## 0.1.0-alpha.1

The first release of Quorium.

### Features

- Replicated state types with linearizable, exactly-once writes and linearizable or stale reads.
- Automatic membership: discovery, learners, promotion and replacement of dead voters.
- The Ladder election controller, with leadership handed back to the preferred datacenter.
- Sharding with locality, range moves, automatic rebalancing and quiescence of idle shards.
- Escrow counters for selling limited stock without a consensus round trip per sale.
- Singleton jobs with fencing tokens.
- Watches on entities.
- QUIC and TCP transports with mutual TLS.
- Write-ahead log with group commit, and redb or fjall checkpoint storage.
- Versioned state types with migrations, and rolling upgrades.
- Backups with `export` and `import`.
- A remote client for processes that are not cluster members.
- Prometheus metrics.
- `quorium::testing`: real-process `Cluster` and deterministic `SimCluster` test harnesses.

### Known issues

See [Known limitations](../explanation/limitations.md).
