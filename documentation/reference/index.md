# Reference

Precise descriptions of Quorium's interfaces, for looking things up.

| Page | Covers |
|---|---|
| [Builder options](builder.md) | Every `Quorium::builder()` option, its default and its errors |
| [State macros](state-macros.md) | `#[derive(quorium::State)]`, `#[state(...)]`, `#[quorium::state]`, `Ctx` |
| [API](api.md) | `Quorium`, entity handles, `Watch`, errors, singletons, the remote client, status types |
| [Escrow](escrow.md) | `EscrowCounter` and the `Escrow<T>` field |
| [Testing](testing.md) | `quorium::testing::Cluster` and `SimCluster` |
| [Cargo features](features.md) | Optional features of the `quorium` crate |
| [Metrics](metrics.md) | Every Prometheus metric |
| [Data directory and formats](data-directory.md) | Files a node writes, and their format versions |
| [`quorium-node`](quorium-node.md) | The bare node binary |
| [Glossary](glossary.md) | The terms these pages use |
| [Release notes](release-notes.md) | What changed in each release |

The API documentation generated from the code is on [docs.rs](https://docs.rs/quorium).

```{toctree}
:hidden:

builder
state-macros
api
escrow
testing
features
metrics
data-directory
quorium-node
glossary
release-notes
```
