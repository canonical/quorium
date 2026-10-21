# Glossary

Copy peer
: Another live node in the same datacenter that records every escrow sale a node makes from its grant, so that the sale survives the selling node's crash. See [How escrow works](../explanation/escrow.md).

`Ctx`
: The context a write method can take as an argument. It gives deterministic time, randomness and the log index, identical on every replica.

Datacenter
: The location label a node is given with `.datacenter(...)`. Elections, leadership hand-back, copy peers and locality use it.

Discovery
: How a node finds its peers: a static list, DNS, Kubernetes or mDNS. See [Builder options](builder.md#discovery).

Entity
: One instance of a state type, identified by its key. Writes to one entity are linearizable; there are no transactions across entities.

Escrow
: Selling from a limited stock without a consensus round trip per sale, by moving units from a shared pool into per-node grants. See [How escrow works](../explanation/escrow.md).

Fencing token
: A number given to each holder of a singleton lease, strictly larger than every previous holder's. External systems use it to reject requests from an earlier holder.

Grant
: Units of an escrow counter moved out of the pool, through consensus, into one node's hands. The node sells from its grant locally.

Group commit
: Syncing the write-ahead log once for every write that arrived while the previous fsync was running.

Ladder
: Quorium's election order: followers stand for election one after the other, ranked by log completeness, datacenter and node ID, instead of after random timeouts. See [Leader elections and the Ladder](../explanation/ladder.md).

Lease
: A time-limited right. A leader's read lease lets it answer linearizable reads without a round trip; a singleton lease lets one node run a job.

Learner
: A node that receives the replicated log but does not vote. New nodes join as learners and are promoted to voters when needed.

Locality
: A function of an entity's key that fixes the top 16 bits of its hash, so that related entities share a shard. See [Sharding and placement](../explanation/sharding.md).

Meta group
: The small Raft group that holds the routing table.

Pool
: The units of an escrow counter not granted to any node.

Pre-vote
: A round in which a candidate asks whether others would vote for it before raising its term, so that a node returning from a partition cannot disrupt a healthy leader.

Quiescence
: An idle shard stopping its heartbeats after `quiesce_after`. Liveness is then tracked by one ping per pair of nodes.

Routing table
: The map from ranges of the hash space to shards, kept in the meta group and cached on every node.

Settlement
: A node reporting its escrow sales back to the cluster in the background.

Shard
: An independent Raft group with its own leader and log, owning ranges of the hash space. Every voter holds a replica of every shard.

Singleton
: A job that runs on at most one node of the cluster at a time, guarded by a lease.

State type
: A Rust type marked with `#[derive(quorium::State)]` whose instances are replicated. Its write methods change it through the log; its read methods do not.

Voter
: A node that votes in elections and counts toward the majority that commits a write. The cluster keeps `replicas` voters.

Write-ahead log
: The file every node appends log entries to and fsyncs before acknowledging them, shared by all shards on the node.
