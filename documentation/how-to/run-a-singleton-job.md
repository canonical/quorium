# Run a job on exactly one node

Use a singleton for work that one instance of your service must do at a time: billing runs, schedulers, consumers of an external queue.

## Start the job on every node

Call `singleton` on every node, with the same name. At most one node runs the job at a time; the others wait to take over.

```rust
use quorium::Lease;

let _job = node.singleton("billing", |lease: Lease| async move {
    run_billing(lease.token).await;
});
```

Keep the returned `Singleton` alive. Dropping it stops the job on this node.

The job function is called each time this node acquires the lease. Its future is stopped when the lease cannot be renewed, for example when this node is cut off from the majority.

## Fence external side effects

Inside Quorium, "at most one runner" holds. Outside it, a runner that has just lost its lease may still be finishing a request to an external system. To make "at most one" hold there too, use the fencing token:

1. Pass `lease.token` with every request the job makes to an external system.
2. Have the external system remember the largest token it has seen, and reject requests with a smaller one.

Each new holder gets a strictly larger token than every previous holder.

## Change the lease length

The default lease lasts 5 seconds. A shorter lease means faster takeover after a crash, and more renewal traffic:

```{code-block} rust
let _job = node.singleton_with("report", Duration::from_secs(10), |lease| async move {
    // ...
});
```

## Related

- [API reference](../reference/api.md#singletons)
