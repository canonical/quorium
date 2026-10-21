# `quorium-node`

A bare node with no state types of its own, for experiments with the key-value space and for checking a deployment. It is built from `crates/quorium/src/bin/quorium-node.rs`.

```sh
cargo install quorium --version 0.1.0-alpha.1 --bin quorium-node
quorium-node --data-dir d1 --bind 127.0.0.1:7001 --seeds 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 --insecure-plaintext
```

| Flag | Default | Meaning |
|---|---|---|
| `--data-dir DIR` | required | Data directory |
| `--bind ADDR` | required | Listen and advertise address |
| `--seeds A,B,C` | none | Static discovery addresses |
| `--replicas N` | the number of seeds, at least 3 | Number of voters |
| `--dc NAME` | `default` | Datacenter |
| `--insecure-plaintext` | off | Disable TLS. `quorium-node` has no TLS options, so this flag is required |
| `--checkpoint-every N` | 5,000 | Log entries between checkpoints |
| `--single-node` | off | Start a one-node cluster |

The node prints `ready <id>` once started, and runs until killed. Use `quorium::Client` to read and write its key-value space.
