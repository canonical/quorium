# Replace a node

Use this guide when a machine is lost for good, or when you want to move a node to new hardware.

## Let Quorium replace a dead voter

Run at least one more instance than `replicas`. The extra instances join as learners: they receive every write but do not vote.

When a voter has been unreachable for `replacement_grace` (10 minutes by default), the leader promotes a caught-up learner in its place and removes the dead voter. You do not need to do anything.

```rust
.replacement_grace(Duration::from_secs(300))
```

A shorter grace replaces voters sooner, but also replaces a voter that was only briefly away, for example during a slow restart.

Without a learner, a dead voter stays in place. The membership never shrinks below `replicas`.

## Move a node to new hardware

1. Start a new instance on the new machine with an empty data directory. It joins as a learner and catches up.
2. Stop the old instance for good.
3. After `replacement_grace`, the learner replaces it.

## Bring back a node with a wiped disk

A node keeps its identity in `data_dir/node_id`. If its data directory is wiped, it starts as a new node with a new ID and joins as a learner. The old identity is replaced after `replacement_grace`; it is never reused.

## Escrow grants of a removed node

When a voter is removed, the node that copied its escrow sales settles them and returns the rest of its grant to the stock. See [How escrow works](../explanation/escrow.md#when-a-node-is-removed).

## Check the result

```rust
let membership = node.membership();
println!("voters {:?}, learners {:?}", membership.voters, membership.learners);
```

## Related

- [Cluster membership](../explanation/architecture.md#membership)
