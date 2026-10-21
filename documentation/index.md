# Quorium documentation

Quorium is an embedded Rust library for replicated state. Every instance of your service is a node of a Raft cluster, your shared state is plain Rust types, and failover needs no human.

This documentation follows the [Diátaxis](https://diataxis.fr/) framework. Pick the section that matches what you need right now.

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} Tutorials
:link: tutorials/index
:link-type: doc

Learn Quorium by building something, step by step. Start with [Your first cluster](tutorials/first-cluster.md).
:::

:::{grid-item-card} How-to guides
:link: how-to/index
:link-type: doc

Get a specific task done in your own service, such as [securing a cluster with TLS](how-to/secure-a-cluster-with-tls.md).
:::

:::{grid-item-card} Reference
:link: reference/index
:link-type: doc

Look up an option, a method, an error or a metric. Start with the [builder options](reference/builder.md).
:::

:::{grid-item-card} Explanation
:link: explanation/index
:link-type: doc

Understand how Quorium works and why. Start with the [architecture](explanation/architecture.md).
:::

::::

## Status

Quorium is alpha software (`0.1.0-alpha.1`). The API will change before 1.0. Read [Known limitations](explanation/limitations.md) before you depend on it.

## Project

- Source code, issues and pull requests: [github.com/canonical/quorium](https://github.com/canonical/quorium)
- Crate and generated API documentation: [crates.io](https://crates.io/crates/quorium) and [docs.rs](https://docs.rs/quorium)
- [Contribute to Quorium](contributing.md)

```{toctree}
:hidden:

Tutorials <tutorials/index>
How-to guides <how-to/index>
Reference <reference/index>
Explanation <explanation/index>
contributing
```
