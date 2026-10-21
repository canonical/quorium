use std::sync::Arc;
use std::time::{Duration, Instant};

use quorium_net::quic::{QuicClient, QuicServer};
use quorium_net::tls::TlsMaterial;
use quorium_net::transport::{Client, Server};

fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields: Vec<&str> = stat.rsplit(") ").next().unwrap_or_default().split(' ').collect();
    let ticks: f64 = fields.get(11).and_then(|u| u.parse().ok()).unwrap_or(0.0)
        + fields.get(12).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    ticks / 100.0
}

fn arg(name: &str, default: usize) -> usize {
    let args: Vec<String> = std::env::args().collect();
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn drive<F, Fut>(messages: usize, concurrency: usize, call: F) -> (Vec<Duration>, usize)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = bool> + Send,
{
    let call = Arc::new(call);
    let mut workers = Vec::new();
    for w in 0..concurrency {
        let call = call.clone();
        let share = messages / concurrency + usize::from(w < messages % concurrency);
        workers.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(share);
            let mut failures = 0;
            for _ in 0..share {
                let start = Instant::now();
                if call().await {
                    latencies.push(start.elapsed());
                } else {
                    failures += 1;
                }
            }
            (latencies, failures)
        }));
    }
    let mut all = Vec::new();
    let mut failures = 0;
    for w in workers {
        let (l, f) = w.await.unwrap();
        all.extend(l);
        failures += f;
    }
    all.sort();
    (all, failures)
}

fn report(name: &str, wall: Duration, cpu: f64, latencies: &[Duration], failures: usize) {
    let n = latencies.len().max(1);
    let pct = |p: f64| latencies.get(((n as f64 * p) as usize).min(n - 1)).copied().unwrap_or_default();
    println!(
        "{{\"transport\":\"{name}\",\"messages\":{},\"failures\":{failures},\"per_second\":{:.0},\"cpu_us_per_message\":{:.1},\"p50_ms\":{:.3},\"p99_ms\":{:.3},\"max_ms\":{:.3}}}",
        latencies.len(),
        latencies.len() as f64 / wall.as_secs_f64(),
        cpu * 1e6 / n as f64,
        pct(0.50).as_secs_f64() * 1e3,
        pct(0.99).as_secs_f64() * 1e3,
        latencies.last().copied().unwrap_or_default().as_secs_f64() * 1e3,
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let messages = arg("--messages", 20_000);
    let concurrency = arg("--concurrency", 32);
    let size = arg("--size", 256);
    let ca = TlsMaterial::generate_ca("bench").unwrap();
    let server_tls = ca.issue("node-1").unwrap().config(&ca);
    let client_tls = ca.issue("node-2").unwrap().config(&ca).with_server_name("node-1");
    let echo = |body: Vec<u8>| async move { Ok::<_, String>(body) };
    let any = "127.0.0.1:0".parse().unwrap();
    let body = vec![7u8; size];

    let tcp_server = Server::bind(any, Some(server_tls.clone()), echo).await.unwrap();
    let tcp = Arc::new(Client::new(tcp_server.local_addr(), Some(client_tls.clone())));
    tcp.call(body.clone(), Duration::from_secs(5)).await.unwrap();
    let (cpu, wall) = (cpu_seconds(), Instant::now());
    let (c, b) = (tcp.clone(), body.clone());
    let (lat, failed) = drive(messages, concurrency, move || {
        let (c, b) = (c.clone(), b.clone());
        async move { c.call(b, Duration::from_secs(10)).await.is_ok() }
    })
    .await;
    report("tcp+tls", wall.elapsed(), cpu_seconds() - cpu, &lat, failed);
    tcp_server.shutdown().await;

    let quic_server = QuicServer::bind(any, Some(server_tls), echo).await.unwrap();
    let quic = Arc::new(QuicClient::new(quic_server.local_addr(), Some(client_tls)));
    quic.call(body.clone(), Duration::from_secs(5)).await.unwrap();
    let (cpu, wall) = (cpu_seconds(), Instant::now());
    let (c, b) = (quic.clone(), body.clone());
    let (lat, failed) = drive(messages, concurrency, move || {
        let (c, b) = (c.clone(), b.clone());
        async move { c.call(b, Duration::from_secs(10)).await.is_ok() }
    })
    .await;
    report("quic", wall.elapsed(), cpu_seconds() - cpu, &lat, failed);
    quic_server.shutdown().await;
}
