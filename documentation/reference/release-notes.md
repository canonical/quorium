# Release notes

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
