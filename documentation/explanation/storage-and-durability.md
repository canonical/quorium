# Storage and durability

## When a write is safe

A write is acknowledged after a majority of voters have appended it to their write-ahead logs and fsynced them. It then survives the loss of any minority of nodes, including power loss: a node that comes back replays its log.

## One log for every shard

A node with many shards could keep one log per shard, each with its own fsync. Quorium shares one write-ahead log between all shards on a node instead, and syncs it with **group commit**: every write that arrives while an fsync is in progress waits for the next one, which then covers all of them. One fsync serves every concurrent write on the node, whatever the shard.

Each log record is framed with its length and a CRC32 checksum. A record torn by a crash fails its checksum, and the log ends at the last whole record.

## Checkpoints

The log cannot grow forever. Every `checkpoint_every` entries (5,000 by default), each shard writes the state that changed since its last checkpoint into a state database, in one fsynced transaction. Log entries before the checkpoint are then no longer needed, and the log is compacted once it passes 64 MiB.

Checkpoints are incremental: they write only changed rows, so their cost depends on how much changed, not on how large the state is. Checkpointing 10 changed rows of a 100,000-row shard takes about 3 ms.

State is stored in [redb](https://crates.io/crates/redb) by default, or in [fjall](https://crates.io/crates/fjall), an LSM tree, with the `fjall` feature.

## Memory

Entity state is served from memory. The state database is for recovery and for catching up other nodes, not for serving reads. Size your nodes to hold the whole dataset in memory, and keep single entities under 1 MiB: every write to an entity replicates its arguments, but every read and checkpoint handles the whole entity.

## Catching up

A node that falls far behind, or a new learner, receives a snapshot of a shard's state instead of the full log, then the log entries after it. Snapshots use the same versioned format as backups.

## Formats

Everything Quorium writes is versioned: log records, snapshots, state rows, the escrow journal and network messages. Each version has a fixture file written by an earlier build that later builds must decode. Newer builds read older formats; older builds do not read newer ones.
