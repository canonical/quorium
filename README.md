# Quorium

An embedded Rust library: every service instance is a cluster node, shared state is plain Rust types, and failover needs no human. Consensus safety comes from [openraft](https://github.com/databendlabs/openraft) (pinned to `=0.10.0-alpha.37`); Quorium adds the Ladder controller, which decides when nodes stand for election.

- User guide: [docs/GUIDE.md](docs/GUIDE.md)
- Operations: [docs/OPERATIONS.md](docs/OPERATIONS.md)
- Spec conformance: [docs/CONFORMANCE.md](docs/CONFORMANCE.md)
- Emulated multi-region rig: [docs/RIG.md](docs/RIG.md)
- Releasing to crates.io: [docs/RELEASING.md](docs/RELEASING.md)
- Example service: [examples/shop](examples/shop)

## Crates

| Crate | Responsibility |
|---|---|
| `quorium` | Public API, builder, runtime shell, routing, discovery, membership, test harness |
| `quorium-macros` | `#[derive(quorium::State)]` and `#[quorium::state]` |
| `quorium-core` | openraft type config, Ladder controller and driver, entity registry, key-value state |
| `quorium-store` | Shared write-ahead log behind openraft's log storage, group commit, compaction; state checkpoints in redb behind the `Storage` trait |
| `quorium-net` | Versioned envelope codec, QUIC (default) and framed TCP transports with mutual TLS, openraft network adapter |
| `quorium-sim` | Deterministic runtime for openraft, simulated network and disk, fault scripts, checkers, TLA+ trace conformance |
| `xtask` | Gate reports, protected-path enforcement, seed sweeps, mutation score |

## Build and test

```sh
cargo test --workspace --release
cargo xtask gate --thresholds gates/phase1.toml --seeds 10000
cargo xtask sweep --from 0 --seeds 100000
```

The TLA+ models in `spec/` run with TLC (`-deadlock`, configs `MCSafety.cfg` and `MCLiveness.cfg`). Simulator traces are checked against the same spec with `QUORIUM_TLA_JAR=... cargo test --release -p quorium-sim --test conformance`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
