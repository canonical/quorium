# Sell limited stock with escrow

Use escrow for a counter that many nodes decrement at high rate and that must never go below zero: tickets, seats, quotas, rate budgets. A plain entity works too, but every sale is then a consensus round trip to one leader. With escrow, each node sells from a local grant in microseconds, and total sales still never exceed the stock.

## Use a standalone counter

```{code-block} rust
use quorium::Error;

let tickets = node.escrow("concert-42");
tickets.refill(1_000).await?;

match tickets.try_take(2).await {
    Ok(()) => { /* sold two */ }
    Err(Error::SoldOut) => { /* not enough units left */ }
    Err(e) if e.is_retryable() => { /* nothing was sold; try again */ }
    Err(e) => return Err(e.into()),
}
```

- `refill(n)` adds `n` units to the stock.
- `try_take(n)` sells `n` units or fails. It never sells part of a request.
- `estimate()` returns the units not yet recorded as sold: the shared pool plus the grants nodes hold. Nodes report their sales in the background, so it can run slightly high for a moment.
- `sold()` returns the units recorded as sold so far, with the same short delay.
- `local_remaining()` returns what this node can still sell without asking the cluster.

## Put the stock inside an entity

When the stock belongs to an entity, declare an `Escrow` field. The handle gets one method per escrow field, named after it:

```rust
use quorium::Escrow;

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Concert {
    title: String,
    seats: Escrow<u64>,
}

#[quorium::state]
impl Concert {
    pub fn open(&mut self, title: String, seats: u64) {
        self.title = title;
        self.seats.refill(seats);
    }

    pub fn capacity(&self) -> u64 {
        self.seats.estimate()
    }
}

let concert = node.entity::<Concert>("gig");
concert.open("Gig".into(), 500).await?;
concert.seats().try_take(2).await?;
```

Write methods refill the field with `refill`; read methods can call `estimate` and `sold`. Selling goes through the handle's field method, which returns the same counter type as `node.escrow`.

## Deploy with a copy peer in every datacenter

A sale is acknowledged only after a second node in the same datacenter has recorded it durably. A node with no other live node in its datacenter does not hold a grant: each of its sales goes through consensus, which is slower but still correct. Run at least two nodes per datacenter where sales are frequent.

## Handle `Unavailable`

`try_take` returns `Error::Unavailable` when the copy peer does not confirm a sale. Nothing was sold in that case, so retrying is safe.

## Known issue in 0.1.0-alpha.1

A node that crashes while asking the cluster for a grant can strand that grant's units. Sales never exceed stock, but the counter can stop short of selling all of it. See [Known limitations](../explanation/limitations.md).

## Related

- [How escrow works](../explanation/escrow.md)
- [Escrow reference](../reference/escrow.md)
