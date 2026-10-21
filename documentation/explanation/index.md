# Explanation

Background reading: how Quorium works, the trade-offs it makes, and why. None of these pages are needed to use Quorium, but they help you predict how it behaves.

- [Architecture](architecture.md): the parts of a node and how they fit together.
- [Consistency guarantees](consistency.md): what a read and a write promise, and what they do not.
- [Leader elections and the Ladder](ladder.md): how Quorium fails over quickly without split votes.
- [How escrow works](escrow.md): selling from a shared stock without a round trip per sale.
- [Storage and durability](storage-and-durability.md): what is on disk, and when a write is safe.
- [Sharding and placement](sharding.md): how data is spread across Raft groups.
- [How Quorium is verified](verification.md): the testing behind the guarantees.
- [Known limitations](limitations.md): what does not work yet.

```{toctree}
:hidden:

architecture
consistency
ladder
escrow
storage-and-durability
sharding
verification
limitations
```
