# Write to several entities consistently

A write to one entity is atomic and linearizable. Writes to different entities are not atomic together: there are no multi-entity transactions. This guide shows how to keep several entities consistent anyway.

## Put what must change together in one entity

If two values must always change together, they belong in the same state type. A single write method can update any number of fields atomically:

```{code-block} rust
#[quorium::state]
impl Account {
    pub fn transfer_between_pockets(&mut self, from: Pocket, to: Pocket, amount: u64) -> Result<(), BankError> {
        // check, then update both fields in one write
    }
}
```

Prefer this whenever the data model allows it.

## Reserve, then compensate

When a change spans entities, write in an order that can be undone, and undo the earlier steps if a later one fails. This is the checkout from the [`shop` example](https://github.com/canonical/quorium/blob/main/examples/shop/src/lib.rs):

1. Read the cart's lines.
2. For each line, call `reserve` on that product's `Inventory`. Each call either takes the units or is rejected with `SoldOut`.
3. If any reservation fails, call `release` on the inventories already reserved, and stop.
4. Close the cart. If that fails, release every reservation.
5. Place the order, with an ID taken from the cart's `close` write (`ctx.random()`), so that the order ID is the same on every replica.

```{code-block} rust
pub async fn checkout(node: &Quorium, user: &str) -> Result<u64, ShopError> {
    let cart = node.entity::<Cart>(user.to_string());
    let lines = cart.lines().await?;
    let mut reserved = Vec::new();
    for (sku, quantity) in &lines {
        match node.entity::<Inventory>(sku.clone()).reserve(*quantity).await {
            Ok(_) => reserved.push((sku, *quantity)),
            Err(e) => {
                for (done, units) in reserved {
                    node.entity::<Inventory>(done.clone()).release(units).await?;
                }
                return Err(/* map e */);
            }
        }
    }
    // close the cart, then place the order
}
```

## Make retries safe

A write that returns `Unavailable` or `Timeout` may or may not have been applied. Retrying the same handle call is safe, because every call carries a request ID and is applied at most once, even if retried.

What is not automatically safe is the sequence: if your process dies between step 2 and step 3, reservations stay taken. Make the sequence resumable, for example by recording progress in the cart entity, or by having a periodic job release reservations of carts that never closed.

## Related

- [Consistency guarantees](../explanation/consistency.md)
- [Sell limited stock with escrow](sell-limited-stock.md), for counters written by many nodes at once
