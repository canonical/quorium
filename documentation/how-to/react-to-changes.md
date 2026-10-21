# React to changes

Use `watch()` to run code whenever an entity changes: refresh a cache, push updates to connected clients, trigger follow-up work.

## Watch an entity

```rust
let mut watch = node.entity::<Cart>("alice").watch();
while let Ok(cart) = watch.changed().await {
    println!("{} items", cart.items.len());
}
```

`changed()` waits for the next change and returns the new state. It returns `Err(Error::Shutdown)` when the node shuts down, which ends the loop.

## Know what you will see

- Changes arrive in commit order, on every node, not only on the leader.
- A slow consumer skips to the latest version. Do not use `watch` to count changes; it reports states, not events.
- A deleted entity is reported as `Default::default()`.
- The watch observes this node's replica. Right after a failover, this replica may briefly lag behind the leader; the next change arrives once it catches up.

## Related

- [API reference](../reference/api.md#entity-handles)
