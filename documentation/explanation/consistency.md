# Consistency guarantees

## The short version

| Operation | Consistency | On the minority side of a partition | Durable when acknowledged |
|---|---|---|---|
| Write | Linearizable per entity | Fails with `Unavailable` | Yes: fsynced on a majority |
| Read | Linearizable | Fails with `Unavailable` | Not applicable |
| Stale read (`.stale()`) | This replica's state, possibly behind | Served | Not applicable |

## Linearizable writes

A write is acknowledged only once a majority of voters have written it to their logs and fsynced them. From then on every later read, on any node, sees it. Writes to one entity happen in one order that every replica agrees on.

Writes to different entities are independent. There is no transaction across entities; see [Write to several entities consistently](../how-to/write-across-entities.md).

## Exactly-once writes

A write that times out may or may not have been applied: the leader may have committed it just before the connection failed. Retrying would then apply it twice, unless the retry is recognised.

Every node and client therefore has a session, and every write carries a sequence number in that session. Replicas remember which sequence numbers have been applied and return the stored reply for a duplicate instead of applying it again. Retrying any write is safe.

## Linearizable reads

A read must not return state older than a write that was already acknowledged. On the leader, Quorium answers immediately while its **lease** is valid: a majority acknowledged its heartbeats within the election timeout, so no other leader can have been elected. Otherwise the leader confirms its leadership with a majority round trip first (ReadIndex).

Leases depend on clocks advancing at about the same rate on every node. They do not depend on clocks showing the same time. The simulator checks reads with nodes' clocks running up to 30% apart.

## Stale reads

A stale read skips the check and reads this node's replica at once. Use it where slightly old data is acceptable and availability matters more: dashboards, caches, the minority side of a partition.

## What "on the minority side" means

During a network partition, only the side holding a majority of voters can elect a leader and commit writes. Nodes on the other side return `Unavailable` for writes and linearizable reads, and keep serving stale reads. This is the price of linearizability: no system can offer linearizable writes on both sides of a partition.
