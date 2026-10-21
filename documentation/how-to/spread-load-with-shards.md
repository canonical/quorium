# Spread load with shards

By default a cluster has one shard: one Raft group, one leader, every write through it. Use several shards when one leader's throughput or one log's size becomes the limit, or when different regions should lead different data.

## Choose the shard count at creation

```rust
.shards(16)
```

The shard count is fixed when the cluster is created. Every shard is replicated on every voter, and leadership is spread across voters. Pick a count that leaves room to grow: 8–64 is typical.

Entities are placed by a hash of their type name and key. Entities of different types with the same key may land in different shards.

## Let idle shards go quiet

```rust
.quiesce_after(Duration::from_secs(5))
```

A shard with no writes for 5 seconds stops heartbeating. Idle traffic then stays flat however many shards you have. The first read or write wakes the shard. A quiesced shard whose leader dies fails over after the host-liveness window (1 second) plus the usual election delay.

## Rebalance automatically

Two rules move key ranges between shards at run time. Both run on one node every `every`:

```rust
.auto_rebalance(Duration::from_secs(60), 1_000)
.split_by_load(2_000)
```

- `auto_rebalance(every, min_items)`: when one shard holds more than twice the average number of items, half of its widest range moves to the smallest shard.
- `split_by_load(writes_per_second)`: when the busiest shard leads more than this many writes per second, and more than twice the average, its range is split at the median of recently written keys and the upper half moves to the least loaded shard.

## Group a region's entities

Add a locality function so that a region's entities share a shard, whose leadership then follows that region:

```rust
#[state(key = (String, u64), locality = region_of)]
pub struct Profile {
    visits: u64,
}

fn region_of(key: &(String, u64)) -> String {
    key.0.clone()
}
```

## Inspect placement

```rust
let shard = node.shard_of::<Profile>(&("eu".to_string(), 7));
let sizes = node.group_sizes().await;
let table = node.routing().await?;
```

## Related

- [Sharding and placement](../explanation/sharding.md)
- [Builder options](../reference/builder.md)
