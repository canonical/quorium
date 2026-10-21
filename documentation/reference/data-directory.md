# Data directory and formats

## Files

| Path | Content |
|---|---|
| `node_id` | The node's identity, a decimal number. Created on first start |
| `wal` | Write-ahead log shared by every shard. Compacted after checkpoints, once past 64 MiB |
| `state.redb` | Checkpointed state of every shard: one row per key or entity, plus session, lease, escrow and routing metadata |
| `state.fjall` | Used instead of `state.redb` with `StateBackend::Fjall` |
| `escrow.journal` | This node's escrow grants, sales and pending settlements |
| `state/shard-N/snapshot` | Legacy snapshot files from early builds. Migrated into `state.redb` on start |

Back up a cluster with `node.export`, not by copying these files: a copy of a running node's files is not consistent. See [Back up and restore](../how-to/back-up-and-restore.md).

## Stored formats

Every stored and wire format is versioned. Each version has a fixture written by an earlier build that today's code must still decode.

| Format | Current version | Older versions read |
|---|---|---|
| Write-ahead log records | 1 | |
| Full state snapshot (Raft snapshots, backups, range moves) | 4 | 3, 2 |
| State rows | 2 | 1 |
| redb database file | redb 2.x | |
| Escrow journal | 1 | |
| Messages between nodes and clients | envelope 1 | |
| Legacy snapshot files | 1 | Migrated on start |

Your own state types are versioned separately, with `#[state(version = N, migrate = f)]`.

## Limits

| Limit | Value |
|---|---|
| Recommended entity size | Under 1 MiB. Larger writes are counted and logged |
| Entries per replication message | Up to 4 MiB, at least one entry |
| Shard size for `export` | Under 16 MiB |
