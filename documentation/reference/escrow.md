# Escrow

## `EscrowCounter`

Returned by `node.escrow(name)` for a standalone counter, and by the field method of an entity handle for an `Escrow<T>` field.

| Method | Returns | Meaning |
|---|---|---|
| `try_take(units).await` | `Result<(), Error<Infallible>>` | Sell `units`, all or nothing |
| `refill(units).await` | `Result<u64, Error<Infallible>>` | Add `units` to the stock |
| `estimate().await` | `Result<u64, Error<Infallible>>` | Units not yet recorded as sold: the pool plus the grants held by nodes |
| `sold().await` | `Result<u64, Error<Infallible>>` | Units recorded as sold |
| `local_remaining()` | `u64` | Units this node can sell from its grant without a network round trip |

`try_take` errors:

| Error | Meaning |
|---|---|
| `Error::SoldOut` | Not enough units remain anywhere |
| `Error::Unavailable` | The sale could not be confirmed by the copy peer or by consensus. Nothing was sold; retrying is safe |
| `Error::Timeout` | No answer in time |

## `Escrow<T>`

A field type for stock held inside an entity. `T` is `u8`, `u16`, `u32` or `u64` (default `u64`).

| Method | Available in | Meaning |
|---|---|---|
| `refill(units: T)` | Write methods | Add units to the stock |
| `estimate() -> T` | Reads and writes | Same as `EscrowCounter::estimate` |
| `sold() -> T` | Reads and writes | Same as `EscrowCounter::sold` |

For each `Escrow` field named `f`, the entity's handle has a method `f()` that returns an `EscrowCounter` for selling from it.

`Escrow<T>` is stored inside the entity, so its format is part of the entity's format.

## Behaviour

| Aspect | Value |
|---|---|
| Grant size | Shrinks as stock runs low |
| Idle grant | Released back to the stock after 2 seconds without sales |
| Durability of a sale | Journaled on the selling node and on a copy peer in the same datacenter before `try_take` returns |
| Nodes without a peer in their datacenter | Sell through consensus, without holding a grant |
| In `SimCluster` | Every sale goes through consensus |

See [How escrow works](../explanation/escrow.md) for the reasoning.
