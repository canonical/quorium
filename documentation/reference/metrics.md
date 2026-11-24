# Metrics

Served in Prometheus text format at `/metrics` on the `metrics_bind` address, and returned by `node.metrics_text()`.

## Node

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `quorium_node_info` | gauge | `datacenter`, `id` | Always 1. Static facts about the node |
| `quorium_appends_sent_total` | counter | | Replication messages sent |
| `quorium_pings_received_total` | counter | | Host liveness pings received; constant per node pair |
| `quorium_large_entity_writes_total` | counter | | Writes that left an entity above 1 MiB |
| `quorium_elections_started_total` | counter | | Election campaigns this node started |
| `quorium_leader_changes_total` | counter | | Leader changes this node observed, over all shards |
| `quorium_rpc_failures_total` | counter | | Consensus requests to peers that failed or timed out |
| `quorium_leaderless_seconds_total` | counter | | Shard time spent without a known leader |
| `quorium_snapshots_sent_total` | counter | `peer` | Full state snapshots sent to a peer. A steady rise means the peer cannot catch up |

## Shards

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `quorium_shard_leader` | gauge | `shard` | 1 when this node leads the shard |
| `quorium_shard_term` | gauge | `shard` | Raft term. A fast-rising term means election churn |
| `quorium_shard_last_applied` | gauge | `shard` | Index of the last applied log entry. Compare across nodes to see lag |
| `quorium_shard_quiesced` | gauge | `shard` | 1 when the idle shard has stopped heartbeating |
| `quorium_shard_checkpoint_index` | gauge | `shard` | Log index of the last durable state checkpoint |
| `quorium_shard_writes_per_second` | gauge | `shard` | Writes this node led on the shard in the last full second |
| `quorium_shard_purged_index` | gauge | `shard` | Log index up to which the log was discarded. A follower behind it needs a snapshot |
| `quorium_shard_leader_age_seconds` | gauge | `shard` | How long the shard has had its current leader |
| `quorium_shard_quorum_contact_age_seconds` | gauge | `shard` | On the leader, time since a majority last acknowledged it. Close to the leader lease means writes are about to stop |

## Write-ahead log

| Metric | Type | Meaning |
|---|---|---|
| `quorium_wal_fsyncs_total` | counter | fsyncs of the log |
| `quorium_wal_bytes` | gauge | Size of the log on disk |
| `quorium_wal_sync_latency_seconds` | gauge | Smoothed fsync latency. Above about a third of the heartbeat interval, expect slower failover |

A Grafana dashboard for these metrics is in [`docs/dashboards/quorium.json`](https://github.com/canonical/quorium/blob/main/docs/dashboards/quorium.json).
