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

## Check health from code

```rust
let status = node.status();
println!("node {} sees leader {:?} in term {}", status.id, status.leader, status.term);
let stats = node.stats();
```

`node.shard_status(n)` gives the same for shard `n`.

## Read the logs

Quorium logs through [`tracing`](https://crates.io/crates/tracing). Install a subscriber in your service to see them. Warnings worth acting on:

- `the round trip to a peer exceeds the heartbeat interval`: raise `heartbeat`; see [Run across regions](run-across-regions.md).
- Large entity warnings: an entity grew above 1 MiB.

## Related

- [Metrics reference](../reference/metrics.md)
