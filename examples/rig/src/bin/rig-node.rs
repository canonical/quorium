use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use quorium::{Discovery, Quorium, TlsConfig};
use quorium_sim::rng::SimRng;
use rig::{Bank, Event};

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn tls(dir: &Path) -> TlsConfig {
    let read = |f: &str| std::fs::read_to_string(dir.join(f)).unwrap_or_else(|e| panic!("{f}: {e}"));
    TlsConfig {
        ca_pem: read("ca.pem"),
        cert_pem: read("node.pem"),
        key_pem: read("node.key"),
        server_name: "rig".into(),
    }
}

fn record(log: &Path, event: &Event) {
    let mut line = serde_json::to_string(event).expect("events serialize");
    line.push('\n');
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(log) {
        let _ = f.write_all(line.as_bytes());
    }
}

async fn bank_loop(node: Quorium, log: PathBuf, seed: u64) {
    let mut rng = SimRng::new(seed);
    let bank = node.entity::<Bank>("main");
    let mut n = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(rng.range(5, 25))).await;
        n += 1;
        if n.is_multiple_of(5) {
            if let Ok(balances) = bank.balances().await
                && !balances.is_empty()
            {
                let total = balances.values().sum();
                let min = balances.values().copied().min().unwrap_or(0);
                record(&log, &Event::BankRead { total, min });
            }
            continue;
        }
        let from = rng.range(0, 10) as u32;
        let to = rng.range(0, 10) as u32;
        let _ = bank.transfer(from, to, rng.range(1, 50) as i64).await;
    }
}

async fn sales_loop(node: Quorium, log: PathBuf, seed: u64) {
    let mut rng = SimRng::new(seed);
    let stock = node.escrow("rig-stock");
    loop {
        tokio::time::sleep(Duration::from_millis(rng.range(2, 15))).await;
        if stock.try_take(1).await.is_ok() {
            record(&log, &Event::Sale { units: 1 });
        }
    }
}

#[cfg(feature = "next")]
async fn ledger_loop(node: Quorium, log: PathBuf) {
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let ok = node.entity::<rig::Ledger>("main").record().await.is_ok();
        let at_ms =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64);
        record(&log, &Event::Ledger { ok, at_ms });
    }
}

async fn refill_once(node: &Quorium, data_dir: &Path, log: &Path, units: u64) {
    let marker = data_dir.join("refilled");
    if marker.exists() {
        return;
    }
    record(log, &Event::Refill { units });
    loop {
        if node.escrow("rig-stock").refill(units).await.is_ok() {
            let _ = std::fs::write(&marker, b"done");
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn open_bank(node: &Quorium) {
    loop {
        if node.entity::<Bank>("main").open(10, 100).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("quorium=info,quorium_core=info")),
        )
        .with_ansi(false)
        .init();
    let args: Vec<String> = std::env::args().collect();
    let data_dir = PathBuf::from(arg(&args, "--data-dir").expect("--data-dir is required"));
    let bind: SocketAddr = arg(&args, "--bind").expect("--bind is required").parse().expect("valid --bind");
    let seeds: Vec<SocketAddr> = arg(&args, "--seeds")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().expect("valid seed address"))
        .collect();
    let log_dir = PathBuf::from(arg(&args, "--log-dir").unwrap_or_else(|| "/var/log/rig".into()));
    std::fs::create_dir_all(&log_dir).expect("log directory");
    let seed: u64 = arg(&args, "--seed").and_then(|s| s.parse().ok()).unwrap_or(1);
    let mut builder = Quorium::builder()
        .data_dir(&data_dir)
        .bind(bind)
        .advertise(bind)
        .datacenter(&arg(&args, "--dc").unwrap_or_else(|| "default".into()))
        .discovery(Discovery::Static(seeds.clone()))
        .replicas(arg(&args, "--replicas").and_then(|r| r.parse().ok()).unwrap_or(seeds.len()))
        .shards(arg(&args, "--shards").and_then(|r| r.parse().ok()).unwrap_or(1))
        .checkpoint_every(arg(&args, "--checkpoint-every").and_then(|r| r.parse().ok()).unwrap_or(500));
    if let Some(ms) = arg(&args, "--heartbeat-ms").and_then(|v| v.parse().ok()) {
        builder = builder.heartbeat(Duration::from_millis(ms));
    }
    builder = builder.metrics_bind("0.0.0.0:9100".parse().expect("valid metrics address"));
    if arg(&args, "--transport").as_deref() == Some("tcp") {
        builder = builder.transport(quorium::Transport::Tcp);
    }
    builder = match arg(&args, "--tls-dir") {
        Some(dir) => builder.tls(tls(Path::new(&dir))),
        None => builder.insecure_plaintext(),
    };
    let node = match builder.start().await {
        Ok(node) => node,
        Err(e) => {
            eprintln!("failed to start: {e}");
            std::process::exit(1);
        }
    };
    println!("ready {}", node.status().id);
    if flag(&args, "--open-bank") {
        open_bank(&node).await;
    }
    if let Some(units) = arg(&args, "--refill").and_then(|u| u.parse().ok()) {
        refill_once(&node, &data_dir, &log_dir.join("sales.jsonl"), units).await;
    }
    if flag(&args, "--bank") {
        tokio::spawn(bank_loop(node.clone(), log_dir.join("bank.jsonl"), seed));
    }
    #[cfg(feature = "next")]
    if flag(&args, "--ledger") {
        tokio::spawn(ledger_loop(node.clone(), log_dir.join("ledger.jsonl")));
    }
    if flag(&args, "--sales") {
        tokio::spawn(sales_loop(node.clone(), log_dir.join("sales.jsonl"), seed ^ 0x5a1e));
    }
    std::future::pending::<()>().await;
}
