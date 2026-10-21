use std::time::Instant;

use quorium_sim::scenario::{FaultMode, ScenarioConfig, run_seed};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned();
    let from: u64 = arg("--from").and_then(|v| v.parse().ok()).unwrap_or(0);
    let seeds: u64 = arg("--seeds").and_then(|v| v.parse().ok()).unwrap_or(100);
    let bug = match arg("--bug").as_deref() {
        Some("restore") => Some(quorium_sim::cluster::Bug::LeaderRestore),
        Some("vote") => Some(quorium_sim::cluster::Bug::ForgetVote),
        Some("fsync") => Some(quorium_sim::cluster::Bug::LostFsync),
        Some("apply") => Some(quorium_sim::cluster::Bug::NondeterministicApply),
        Some("stale-read") => Some(quorium_sim::cluster::Bug::StaleRead),
        _ => None,
    };
    let mode = match arg("--mode").as_deref() {
        Some("vote") => ScenarioConfig::vote_hunting(),
        Some("power") => ScenarioConfig::power_loss(),
        _ => ScenarioConfig { faults: FaultMode::Swarm, ..ScenarioConfig::default() },
    };
    let drift: u64 = arg("--drift-ppm").and_then(|v| v.parse().ok()).unwrap_or(1_000);
    let mode =
        ScenarioConfig { bug, max_drift_ppm: drift, lease_reads: !args.iter().any(|a| a == "--read-index"), ..mode };
    let wall = Instant::now();
    let mut failed = Vec::new();
    let mut acked = 0usize;
    for seed in from..from + seeds {
        let report = run_seed(seed, &mode);
        acked += report.acked_writes;
        if !report.violations.is_empty() {
            eprintln!("seed {seed}: {:?}", report.violations);
            failed.push(seed);
        }
    }
    println!(
        "{{\"seeds\":{seeds},\"from\":{from},\"failed\":{},\"failing_seeds\":{:?},\"acked_writes\":{acked},\"wall_ms\":{}}}",
        failed.len(),
        failed,
        wall.elapsed().as_millis()
    );
    if !failed.is_empty() {
        std::process::exit(1);
    }
}
