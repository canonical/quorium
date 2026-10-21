# Choose a transport and a storage backend

The defaults suit most services: QUIC between nodes and redb for checkpoints. Change them only for the cases below.

## Transport

| Choose | When |
|---|---|
| QUIC (default) | Networks with any packet loss, including most cloud and cross-region links. Its p99 latency stays stable under loss |
| TCP | CPU-bound services on a reliable network. TCP uses about half the CPU per message, but its tail latency grows under packet loss |

```rust
use quorium::Transport;

.transport(Transport::Tcp)
```

Every node of a cluster, and every remote client, must use the same transport. QUIC needs the port open for UDP; TCP needs it open for TCP.

To build without QUIC at all, disable default features: `quorium = { version = "0.1.0-alpha.1", default-features = false }`. TCP is then the default.

## Storage backend

| Choose | When |
|---|---|
| redb (default) | Most workloads. Checkpointing a few changed rows takes milliseconds |
| fjall | Write-heavy workloads or very large datasets, where an LSM tree's write pattern helps |

```toml
quorium = { version = "0.1.0-alpha.1", features = ["fjall"] }
```

```rust
use quorium::StateBackend;

.state_backend(StateBackend::Fjall)
```

The backend is fixed for a data directory. A node refuses to start with a different backend from the one its data was written with. To switch, replace nodes one by one with fresh data directories; see [Replace a node](replace-a-node.md).

## Related

- [Storage and durability](../explanation/storage-and-durability.md)
- [Cargo features](../reference/features.md)
