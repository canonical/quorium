# Cargo features

Features of the `quorium` crate.

| Feature | Default | Adds |
|---|---|---|
| `quic` | yes | The QUIC transport (`Transport::Quic`), which becomes the default. Without it, TCP is the only transport |
| `mdns` | no | `Discovery::Mdns` and `quorium::discovery::announce_mdns`, for development on one local network |
| `uuid` | no | `uuid::Uuid` as a state key type |
| `fjall` | no | `StateBackend::Fjall`, an LSM-tree checkpoint store |
| `testing` | no | The `quorium::testing` module: `Cluster`, `SimCluster` and `free_addrs` |

Enable `testing` in `[dev-dependencies]` only. It pulls in the simulator and `tempfile`.

The other crates of the workspace (`quorium-core`, `quorium-store`, `quorium-net`, `quorium-macros`, `quorium-sim`) are internal. Depend on `quorium` only.
