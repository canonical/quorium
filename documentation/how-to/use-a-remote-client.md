# Use the cluster from another process

Most services embed a node in every instance. Use `quorium::Client` when a process must talk to the cluster without being a node: an admin tool, a batch job, a service in another language's sidecar.

The client reads and writes the cluster's key-value space: byte values under string keys. It does not call entity methods.

## Connect

```rust
use std::time::Duration;

let client = quorium::Client::new(seeds, Some(tls)).with_timeouts(Duration::from_secs(10), Duration::from_secs(1));
```

- `seeds` is a list of node addresses. One reachable node is enough; the client follows leader redirects.
- `tls` is a `TlsConfig` signed by the cluster CA. Pass `None` only for a cluster started with `insecure_plaintext()`.
- If the cluster uses TCP, add `.with_transport(quorium::Transport::Tcp)`.

## Read and write

```rust
client.write("greeting", b"hello".to_vec()).await?;
let value: Option<Vec<u8>> = client.read("greeting").await?;
```

Reads are linearizable. Both calls return `ClientError::Timeout` or `ClientError::Unavailable` on failure. Retrying a write is safe: it is applied at most once.

## Tune the timeouts

`with_timeouts(total, attempt)`:

- `total` bounds the whole call.
- `attempt` is the longest the client waits for one node before also sending the request to another node. The slow attempt is not cancelled: the first reply wins, at most 3 attempts run at once, and the request ID makes the copies apply once.

Once the client has seen replies, it sends the extra copy sooner: after twice its average reply time, but at least 50 ms and at most `attempt`. Keep `attempt` near your failover time, so that a dead leader costs one attempt rather than the whole call.

## Related

- [Client reference](../reference/api.md#remote-client)
