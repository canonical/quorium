# Monitor a cluster

## Expose metrics

Serve Prometheus metrics on a dedicated port:

```rust
.metrics_bind("0.0.0.0:9100".parse()?)
```

Prometheus scrapes `http://<node>:9100/metrics`. To serve them from your own HTTP server instead, return `node.metrics_text()` from your handler.

A Grafana dashboard for these metrics is in [`docs/dashboards/quorium.json`](https://github.com/canonical/quorium/blob/main/docs/dashboards/quorium.json). Import it into Grafana and choose your Prometheus data source.

## Alert on these conditions

| Condition | Query idea | Meaning |
|---|---|---|
| A shard has no leader | `max by (shard) (quorium_shard_leader) == 0` for 30 s | The shard cannot accept writes |
| Election churn | `rate(quorium_shard_term[5m]) > 0.1` | Leaders keep changing: check the network and the heartbeat setting |
| Replica lag | Spread of `quorium_shard_last_applied` across nodes keeps growing | A node cannot keep up: check its disk and CPU |
| Slow disk | `quorium_wal_sync_latency_seconds` above a third of the heartbeat | Writes and failover slow down |
| Large entities | `rate(quorium_large_entity_writes_total[5m]) > 0` | Some entities exceed 1 MiB: split them |
| Time without a leader | `rate(quorium_leaderless_seconds_total[5m]) > 0` | Shards spent time unable to write |
| Unreachable peers | `rate(quorium_rpc_failures_total[1m]) > 0` | Consensus requests time out: check the network and the peer's load |
| Leader about to lose its lease | `quorium_shard_quorum_contact_age_seconds` close to the leader lease | A majority is slow to answer; writes stop when the lease lapses |
| A follower cannot catch up | `rate(quorium_snapshots_sent_total[10m]) > 0` for long | The same peer keeps needing snapshots: raise `snapshot_timeout` or `log_retention` |

## Check health from code

```rust
let status = node.status();
println!("node {} sees leader {:?} in term {}", status.id, status.leader, status.term);
let stats = node.stats();
```

`node.shard_status(n)` gives the same for shard `n`. `status.leader_age` is how long the shard has had its leader, and `node.is_stable(window)` combines leader age, recent elections and peer liveness into one check.

## Follow leadership changes

```rust
let mut events = Box::pin(node.leadership_events());
while let Some(event) = events.next().await {
    match event {
        LeadershipEvent::LeaderChanged { shard, leader, .. } => println!("shard {shard}: leader {leader:?}"),
        LeadershipEvent::ElectionStarted { shard, term, .. } => println!("shard {shard}: election for term {term}"),
        other => println!("{other:?}"),
    }
}
```

The stream needs `futures::StreamExt`. Events describe what this node observes; see the [API reference](../reference/api.md#leadership-events).

## Read the logs

Quorium logs through [`tracing`](https://crates.io/crates/tracing). Install a subscriber in your service to see them. Warnings worth acting on:

- `the round trip to a peer exceeds the heartbeat interval`: raise `heartbeat`; see [Run across regions](run-across-regions.md).
- Large entity warnings: an entity grew above 1 MiB.
- `leadership handoff did not complete`: a `step_down` target did not take over in time; the cluster elects a leader instead.

## Related

- [Metrics reference](../reference/metrics.md)
