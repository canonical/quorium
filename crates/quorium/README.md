# Quorium

An embedded Rust library: every instance of your service is a cluster node, your shared state is plain Rust types, and failover needs no human. Consensus safety comes from [openraft](https://github.com/databendlabs/openraft); Quorium adds the Ladder controller, which decides when nodes stand for election.

## Status: alpha

This is an early release. The API will change before 1.0, and the on-disk and wire formats are versioned but not yet frozen.

Known issues in this release:

- A node that crashes while asking for an escrow grant can strand the units of that grant. Escrow counters never oversell, but they may stop short of the full stock. A fix is in progress.
- Leader failover takes about 650 ms at p99 on local disks, and several seconds on slow network disks.
- Quorium depends on a pre-release of openraft, pinned to `=0.10.0-alpha.37`.

## Example

```rust,ignore
use quorium::{Discovery, Quorium};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Cart {
    items: Vec<String>,
}

#[quorium::state]
impl Cart {
    pub fn add(&mut self, item: String) -> usize {
        self.items.push(item);
        self.items.len()
    }

    pub fn total(&self) -> usize {
        self.items.len()
    }
}

let node = Quorium::builder()
    .data_dir("/var/lib/shop")
    .bind("0.0.0.0:7000".parse()?)
    .advertise("10.0.0.5:7000".parse()?)
    .discovery(Discovery::Dns { name: "shop".into(), port: 7000 })
    .replicas(3)
    .tls(tls_config)
    .start()
    .await?;

let cart = node.entity::<Cart>("alice");
cart.add("apple".into()).await?;
assert_eq!(cart.total().await?, 1);
```

## Features

| Feature | Default | Meaning |
|---|---|---|
| `quic` | yes | QUIC transport; TCP is always available |
| `mdns` | no | `Discovery::Mdns` for development on one network |
| `uuid` | no | `uuid::Uuid` as a state key |
| `fjall` | no | fjall as an alternative checkpoint store |
| `testing` | no | `quorium::testing::Cluster` and simulation helpers for tests |

## Documentation

- [User guide](https://github.com/canonical/quorium/blob/main/docs/GUIDE.md)
- [Operations](https://github.com/canonical/quorium/blob/main/docs/OPERATIONS.md)

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
