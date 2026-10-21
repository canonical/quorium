# Sharding and placement

## Why shards

A Raft group has one leader, and every write goes through it. That leader's CPU, network and disk bound the group's throughput. Splitting the data into several groups, each with its own leader, spreads the load across voters.

In Quorium, every voter holds a replica of every shard. Sharding spreads leadership, not storage: each node still stores all the data. The shared write-ahead log keeps the disk cost of many shards close to that of one.

## Placing entities

An entity's position is a hash of its type name and key. The routing table, kept in a small separate Raft group, maps ranges of that hash space to shards. Every node caches the table and routes each call to the right shard.

A locality function changes the hash: the top 16 bits of an entity's position come from its locality, so entities with the same locality fall in the same narrow slice of the hash space and normally share a shard. Leadership of that shard then follows the datacenter that writes it most, which keeps writes from that region local.

## Moving ranges

The shard count is fixed, but which ranges a shard owns is not. A range move:

1. records in the routing table that the range is moving,
2. extracts the range's state from the source shard,
3. ingests it into the target shard,
4. switches the routing table,
5. releases the range in the source shard.

A call that reaches a shard that no longer owns its key is answered `WrongShard`; the node refreshes its routing table and sends the call to the new owner. `auto_rebalance` moves ranges by item count; `split_by_load` splits and moves ranges by write rate.

## Quiescence

With many shards, idle heartbeats add up: each shard's leader heartbeats each follower several times a second. With `quiesce_after`, a shard idle for that long stops heartbeating. Liveness is then tracked by one ping per node pair, whatever the shard count. On three nodes, idle traffic measured 58 messages per second with 1 shard and 59 with 64 shards.

The cost is failover time for idle shards: a quiesced shard notices its leader is gone only through the host-liveness window (1 second) before the usual election delay.
