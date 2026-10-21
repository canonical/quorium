# Your first cluster

In this tutorial you build a guestbook that three processes share. Each process signs the book every two seconds. You will stop one process, see the other two carry on, then bring it back and see it catch up.

By the end you will have defined a replicated state type, started a three-node cluster on your machine, and seen a failover happen.

## 1. Create the project

```sh
cargo new guestbook
cd guestbook
```

Replace the `[dependencies]` section of `Cargo.toml` with:

```toml
[dependencies]
quorium = "0.1.0-alpha.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

## 2. Define the shared state

Create `src/lib.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Guestbook {
    names: Vec<String>,
}

#[quorium::state]
impl Guestbook {
    pub fn sign(&mut self, name: String) -> usize {
        self.names.push(name);
        self.names.len()
    }

    pub fn names(&self) -> Vec<String> {
        self.names.clone()
    }
}
```

`Guestbook` is an ordinary struct. The two attributes turn it into replicated state:

- `sign` takes `&mut self`, so it is a **write**. Quorium runs it on every node, in the same order.
- `names` takes `&self`, so it is a **read**.
- `key = String` means there can be many guestbooks, each named by a string.

## 3. Start a node

Replace `src/main.rs` with:

```rust
use std::net::SocketAddr;
use std::time::Duration;

use guestbook::Guestbook;
use quorium::{Discovery, Quorium};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let name = &args[1];
    let port: u16 = args[2].parse()?;
    let seeds: Vec<SocketAddr> =
        args[3..].iter().map(|p| format!("127.0.0.1:{p}").parse()).collect::<Result<_, _>>()?;
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;

    let node = Quorium::builder()
        .data_dir(format!("data/{name}"))
        .bind(addr)
        .advertise(addr)
        .discovery(Discovery::Static(seeds))
        .replicas(3)
        .insecure_plaintext()
        .start()
        .await?;
    println!("{name} started as node {}", node.status().id);

    let book = node.entity::<Guestbook>("lobby");
    loop {
        match book.sign(name.clone()).await {
            Ok(count) => println!("{name} signed; the book has {count} entries"),
            Err(e) => println!("{name} could not sign: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
```

The program takes a name, its own port, and the ports of all three nodes. `insecure_plaintext()` turns off TLS, which is fine on your own machine. Never use it in production; see [Secure a cluster with TLS](../how-to/secure-a-cluster-with-tls.md).

Build it:

```sh
cargo build
```

## 4. Start three nodes

Open three terminals in the `guestbook` directory and start one node in each:

```sh
./target/debug/guestbook ada 7101 7101 7102 7103
```

```sh
./target/debug/guestbook grace 7102 7101 7102 7103
```

```sh
./target/debug/guestbook linus 7103 7101 7102 7103
```

Once all three are running, they form a cluster and start signing. Every terminal prints a growing count, and the counts interleave because all three write to the same book:

```text
ada started as node 3359184315401692589
ada signed; the book has 2 entries
ada signed; the book has 4 entries
```

```text
grace signed; the book has 1 entries
grace signed; the book has 5 entries
```

## 5. Stop a node

Press `Ctrl+C` in the `linus` terminal.

Watch the other two. They keep signing, and the count keeps growing. Two out of three nodes are a majority, so the cluster still accepts writes. If `linus` was the leader, the other two elected a new one before you could notice.

## 6. Bring it back

Start `linus` again with the same command:

```sh
./target/debug/guestbook linus 7103 7101 7102 7103
```

It prints the same node ID as before, because its identity is stored in `data/linus/node_id`. Its first signature already counts every entry written while it was away: it caught up from the others before writing.

```text
linus started as node 8292480990652603809
linus signed; the book has 17 entries
```

Stop all three nodes when you are done.

## What you built

- A state type whose writes are replicated to every node in the same order.
- A cluster that forms by itself once three seeds can reach each other.
- A service that keeps working when one node out of three is gone, and heals when it returns.

Look inside `data/ada`: `wal` is the write-ahead log, `state.redb` holds checkpointed state, and `node_id` is the node's identity.

The finished project, with the tests from the next tutorial, is in [`examples/guestbook`](https://github.com/canonical/quorium/tree/main/examples/guestbook).

## Next

[Test your service against failures](test-your-service.md) shows how to check this behaviour automatically, without opening three terminals.
