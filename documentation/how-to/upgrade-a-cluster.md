# Upgrade a cluster

Upgrade by replacing the binary on one node at a time. The cluster stays available throughout.

## Steps

1. Check that every node is healthy: each shard has a leader, and `quorium_shard_last_applied` is close across nodes.
2. Optionally call `node.checkpoint().await` on the node you are about to stop. Restart is faster when little log needs replaying.
3. Stop the node and start it with the new binary, on the same data directory.
4. Wait until it has caught up: its `quorium_shard_last_applied` matches the others.
5. Repeat for the next node.

Never stop a majority of voters at once. With 3 voters, upgrade one at a time; with 5, at most two.

## What happens to new methods

A write method that only the new binary knows is disabled until every voter of its shard advertises it. Calls to it return `Error::Unavailable` until then, so callers that retry on `Unavailable` start succeeding once the upgrade completes.

## What happens to changed types

Types with a new `version` are migrated when each entity is next read or written. See [Change a state type safely](change-a-state-type.md).

## Roll back

Rolling back is the same procedure with the old binary. It works only if no new state version has been written yet: an old binary cannot decode an entity written at a newer version, and returns `Error::Internal` for it. Ship type changes in a release of their own, and roll back before any traffic uses them.

## Related

- [Change a state type safely](change-a-state-type.md)
- [Stored formats](../reference/data-directory.md#stored-formats)
