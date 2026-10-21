use std::net::SocketAddr;

use quorium::{Discovery, Quorium};

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = arg(&args, "--data-dir").expect("--data-dir is required");
    let bind: SocketAddr = arg(&args, "--bind").expect("--bind is required").parse().expect("valid --bind");
    let seeds: Vec<SocketAddr> = arg(&args, "--seeds")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().expect("valid seed address"))
        .collect();
    let replicas: usize = arg(&args, "--replicas").and_then(|r| r.parse().ok()).unwrap_or(seeds.len().max(3));
    let mut builder = Quorium::builder()
        .data_dir(&data_dir)
        .bind(bind)
        .advertise(bind)
        .datacenter(&arg(&args, "--dc").unwrap_or_else(|| "default".into()))
        .discovery(Discovery::Static(seeds))
        .replicas(replicas);
    if args.iter().any(|a| a == "--insecure-plaintext") {
        builder = builder.insecure_plaintext();
    }
    if let Some(n) = arg(&args, "--checkpoint-every").and_then(|n| n.parse().ok()) {
        builder = builder.checkpoint_every(n);
    }
    if args.iter().any(|a| a == "--single-node") {
        builder = builder.single_node_dev_mode();
    }
    let node = match builder.start().await {
        Ok(node) => node,
        Err(e) => {
            eprintln!("failed to start: {e}");
            std::process::exit(1);
        }
    };
    println!("ready {}", node.status().id);
    std::future::pending::<()>().await;
}
