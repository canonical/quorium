# TCP versus QUIC (Phase 2 benchmark)

Run on 2026-10-19 on the development machine (loopback, one process holding both ends, 2 Tokio worker threads), with `crates/quorium-net/examples/transport_compare.rs`:

```sh
cargo build --release -p quorium-net --features quic --example transport_compare
sudo tc qdisc add dev lo root netem delay 5ms loss 1%
./target/release/examples/transport_compare --messages 20000 --concurrency 32 --size 256
sudo tc qdisc del dev lo root
```

Both transports use mutual TLS 1.3 with the same certificates. TCP is Quorium's transport: one connection, request multiplexing and write coalescing. QUIC is quinn 0.11 with one bidirectional stream per request. `netem` applies to each direction, so `delay 5ms` is a 10 ms round trip.

| Network | Transport | Messages/s | CPU µs/message | p50 ms | p99 ms |
|---|---|---|---|---|---|
| clean (100k messages) | TCP | 143,164 | 13.2 | 0.22 | 0.41 |
| clean (100k messages) | QUIC | 88,150 | 19.9 | 0.34 | 0.70 |
| 10 ms RTT | TCP | 3,141 | 84 | 10.1 | 10.4 |
| 10 ms RTT | QUIC | 3,142 | 115 | 10.2 | 10.6 |
| 10 ms RTT, 1% loss (3 runs) | TCP | 1,959–2,132 | 48–50 | 10.5–14.6 | 30.3 / 30.5 / 36.4 |
| 10 ms RTT, 1% loss (3 runs) | QUIC | 2,523–2,567 | 84–108 | 10.4 | 29.0 / 29.6 / 30.0 |
| 10 ms RTT, 5% loss (3 runs) | TCP | 1,150–1,349 | 35–40 | 20.3 | 51 / 82 / 245 |
| 10 ms RTT, 5% loss (3 runs) | QUIC | 1,689–1,696 | 87–89 | 19.9–20.0 | 59 / 60 / 63 |

## Reading

- CPU per message: TCP wins everywhere, by 1.5× clean and about 2× under loss.
- p99 under loss: equal at 1% loss within noise (one TCP run was worse). At 5% loss TCP's p99 swings with head-of-line blocking and the 200 ms minimum retransmission timeout (51 to 245 ms), while QUIC stays at about 60 ms.
- Throughput under loss: QUIC is 20–45% higher.

The plan's rule is that TCP stays the default only if it matches or beats QUIC on both CPU per message and p99 under loss. On this loopback emulation it does not hold for p99 at 5% loss. Following that rule, QUIC is the default transport (decided 2026-10-20). `transport(Transport::Tcp)` keeps TCP available for CPU-bound deployments on reliable networks. The numbers are to be re-measured on the emulated 3-region rig.
