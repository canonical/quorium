# Secure a cluster with TLS

Quorium requires mutual TLS between nodes and clients. A node refuses to start without it, unless you call `insecure_plaintext()`.

Every node and every client presents a certificate that:

- is signed by your cluster CA, and
- names the cluster: the certificate's subject alternative names include the `server_name` that peers verify.

## Load certificates from files

If your platform already issues certificates (cert-manager, Vault, your own PKI), load the PEM files into a `TlsConfig`:

```rust
use quorium::TlsConfig;

fn tls() -> std::io::Result<TlsConfig> {
    Ok(TlsConfig {
        ca_pem: std::fs::read_to_string("/etc/shop/tls/ca.pem")?,
        cert_pem: std::fs::read_to_string("/etc/shop/tls/node.pem")?,
        key_pem: std::fs::read_to_string("/etc/shop/tls/node.key")?,
        server_name: "shop-cluster".into(),
    })
}
```

Then pass it to the builder:

```{code-block} rust
let node = Quorium::builder()
    // ...
    .tls(tls()?)
    .start()
    .await?;
```

The certificate in `node.pem` must carry `shop-cluster` as a DNS subject alternative name.

## Generate certificates with Quorium

For test environments, `TlsMaterial` creates a CA and issues certificates:

```rust
use quorium::TlsMaterial;

let ca = TlsMaterial::generate_ca("shop-ca")?;
let node = ca.issue("shop-cluster")?;
std::fs::write("ca.pem", &ca.cert_pem)?;
std::fs::write("node.pem", &node.cert_pem)?;
std::fs::write("node.key", &node.key_pem)?;
let config = node.config(&ca);
```

Each call to `issue` creates a fresh key. Issue one certificate per node, and keep the CA key out of your service. `TlsMaterial` holds the CA key in memory only, so for anything long-lived, use a real CA.

## Use TLS from a remote client

`quorium::Client` takes the same `TlsConfig`:

```rust
let client = quorium::Client::new(seeds, Some(tls()?));
```

## Check that it works

A node or client whose certificate comes from another CA is rejected during the handshake. Its requests fail with `ClientError::Unavailable`, and the node never joins the cluster.

## Related

- [Builder options](../reference/builder.md) for `tls` and `insecure_plaintext`
- [Architecture](../explanation/architecture.md) for how nodes talk to each other
