# Run across regions

Use this guide when your voters are in different regions, with tens of milliseconds between them.

## 1. Label every node with its failure domain

```rust
.datacenter("eu-west-1a")
```

Use the real failure domain: a zone, or a region if each region holds one voter. Quorium uses it to keep leadership near the failed leader's datacenter, and to choose escrow copy peers.

## 2. Set the heartbeat from the round trip

Set `heartbeat` to about 2–3 times the largest round trip between voters:

| Largest round trip between voters | `heartbeat` |
|---|---|
| Under 20 ms (one region) | 50 ms (the default) |
| 60 ms | 150 ms |
| 100 ms | 250 ms |

```rust
.heartbeat(Duration::from_millis(150))
```

A leader only accepts writes while a majority has acknowledged its recent heartbeats. If a round trip exceeds the heartbeat, leaders lose their lease and writes stall. The node logs `the round trip to a peer exceeds the heartbeat interval` when this happens.

Failover time grows with the heartbeat: a failure is detected after about 3 heartbeats.

## 3. Give the leader lease room for stalls

By default the leader lease is 2 heartbeats. A leader whose majority stays silent for longer than `leader_lease - heartbeat`, for example because a disk or a host stalls for a second, stops accepting writes until it hears from them again. If such stalls are expected, lengthen the lease and the consensus request deadline:

```rust
.heartbeat(Duration::from_millis(500))
.leader_lease(Duration::from_secs(3))
.rpc_timeout(Duration::from_secs(2))
```

A longer lease also delays failover: followers refuse to elect a new leader until it has expired, and the ladder `base` is raised to at least `leader_lease + heartbeat`. Check the result with `node.timings()` and `node.failover_estimate()`. Conflicting settings make `start` fail with `StartError::Timing` instead of being adjusted silently.

## 4. Place voters for availability

With 3 voters in 3 datacenters, losing any one datacenter keeps every shard available. With 3 voters in 2 datacenters, losing the datacenter that holds 2 of them stops writes.

## 5. Keep data near its writers

If entities belong to a region, give the type a locality function, and use several shards:

```rust
#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = (String, u64), locality = region_of)]
pub struct Profile {
    visits: u64,
}

fn region_of(key: &(String, u64)) -> String {
    key.0.clone()
}
```

Entities with the same locality are placed together, and their shard's leadership moves to the datacenter that writes them most. Writes from that region then commit without a cross-region hop to the leader. See [Spread load with shards](spread-load-with-shards.md).

## 6. Use fast disks

Each write is fsynced on a majority before it is acknowledged, and fsync latency also lengthens elections. Network block storage with 100 ms fsyncs turns failover from under a second into several seconds. Watch `quorium_wal_sync_latency_seconds`; above about a third of the heartbeat, expect slower failover.

## Related

- [Leader elections and the Ladder](../explanation/ladder.md)
- [Monitor a cluster](monitor-a-cluster.md)
