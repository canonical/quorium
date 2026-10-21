use std::process::{Command, ExitCode};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use quorium_sim::cluster::Bug;
use quorium_sim::scenario::{FailoverConfig, FaultMode, ScenarioConfig, failover_samples, percentile, run_seed};
use xtask::gate::{Measured, Thresholds, evaluate};
use xtask::mutation::{Score, evaluate_score};
use xtask::protected::{Approvals, check_change};

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn protected_paths(args: &[String]) -> ExitCode {
    let base = arg(args, "--base").unwrap_or_else(|| "origin/main".into());
    let out = Command::new("git").args(["diff", "--name-only", &format!("{base}...HEAD")]).output();
    let Ok(out) = out else {
        eprintln!("git diff failed");
        return ExitCode::FAILURE;
    };
    let paths: Vec<String> = String::from_utf8_lossy(&out.stdout).lines().map(str::to_string).collect();
    let approvals = Approvals {
        reviewer: args.iter().any(|a| a == "--reviewer-approved"),
        owner: args.iter().any(|a| a == "--owner-approved"),
    };
    match check_change(&paths, approvals) {
        Ok(()) => ExitCode::SUCCESS,
        Err(errors) => {
            eprintln!("protected path rules violated: {errors:?}");
            ExitCode::FAILURE
        }
    }
}

fn sweep(seeds: u64, cfg: &ScenarioConfig, threads: u64) -> u64 {
    sweep_from(0, seeds, cfg, threads)
}

fn sweep_from(from: u64, seeds: u64, cfg: &ScenarioConfig, threads: u64) -> u64 {
    let next = Arc::new(AtomicU64::new(0));
    let violations = Arc::new(AtomicU64::new(0));
    let failing = Arc::new(Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let next = next.clone();
            let violations = violations.clone();
            let failing = failing.clone();
            scope.spawn(move || {
                loop {
                    let offset = next.fetch_add(1, Ordering::Relaxed);
                    if offset >= seeds {
                        return;
                    }
                    let seed = from + offset;
                    let report = run_seed(seed, cfg);
                    if !report.violations.is_empty() {
                        violations.fetch_add(1, Ordering::Relaxed);
                        failing.lock().unwrap().push((seed, report.violations));
                    }
                }
            });
        }
    });
    for (seed, v) in failing.lock().unwrap().iter() {
        eprintln!("seed {seed}: {v:?}");
    }
    violations.load(Ordering::Relaxed)
}

fn caught(name: &str, bug: Bug, cfg: ScenarioConfig, seeds: u64) -> Option<String> {
    let cfg = ScenarioConfig { bug: Some(bug), ..cfg };
    (0..seeds).any(|s| !run_seed(s, &cfg).violations.is_empty()).then(|| name.to_string())
}

fn gate(args: &[String]) -> ExitCode {
    let file = arg(args, "--thresholds").unwrap_or_else(|| "gates/phase1.toml".into());
    let Ok(text) = std::fs::read_to_string(&file) else {
        eprintln!("cannot read {file}");
        return ExitCode::FAILURE;
    };
    let thresholds: Thresholds = match toml::from_str(&text) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("invalid thresholds: {e}");
            return ExitCode::FAILURE;
        }
    };
    let seeds: u64 = arg(args, "--seeds").and_then(|s| s.parse().ok()).unwrap_or(thresholds.min_seeds);
    let threads = threads(args);
    let swarm = ScenarioConfig { faults: FaultMode::Swarm, ..ScenarioConfig::default() };
    let mut violations = sweep(seeds, &swarm, threads);
    violations += sweep(seeds / 5, &ScenarioConfig::vote_hunting(), threads);
    violations += sweep(seeds / 5, &ScenarioConfig::power_loss(), threads);
    let single = failover_samples(&FailoverConfig::single_region(), 0..400);
    let three = failover_samples(&FailoverConfig::three_regions(), 0..400);
    let planted_caught = [
        caught("apply", Bug::NondeterministicApply, ScenarioConfig::default(), 10),
        caught("vote", Bug::ForgetVote, ScenarioConfig::vote_hunting(), 200),
        caught("fsync", Bug::LostFsync, ScenarioConfig::power_loss(), 40),
        caught("restore", Bug::LeaderRestore, ScenarioConfig::vote_hunting(), 40),
        caught("escrow", Bug::OversellEscrow, ScenarioConfig::default(), 20),
        caught("stale-read", Bug::StaleRead, ScenarioConfig::default(), 40),
    ]
    .into_iter()
    .flatten()
    .collect();
    let measured = Measured {
        seeds,
        violations,
        failover_p99_single: percentile(&single, 0.99),
        failover_p99_three: percentile(&three, 0.99),
        planted_caught,
    };
    let report = evaluate(&thresholds, &measured);
    let json = report.to_json();
    println!("{json}");
    if let Some(out) = arg(args, "--out")
        && std::fs::write(&out, &json).is_err()
    {
        eprintln!("cannot write {out}");
        return ExitCode::FAILURE;
    }
    if report.passed() { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}

fn threads(args: &[String]) -> u64 {
    arg(args, "--threads")
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get() as u64).min(8))
}

fn sweep_command(args: &[String]) -> ExitCode {
    let from: u64 = arg(args, "--from").and_then(|s| s.parse().ok()).unwrap_or(0);
    let seeds: u64 = arg(args, "--seeds").and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let cfg = match arg(args, "--mode").as_deref() {
        Some("vote") => ScenarioConfig::vote_hunting(),
        Some("power") => ScenarioConfig::power_loss(),
        _ => ScenarioConfig::default(),
    };
    let violations = sweep_from(from, seeds, &cfg, threads(args));
    println!("{{\"from\":{from},\"seeds\":{seeds},\"failed\":{violations}}}");
    if violations == 0 { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}

fn mutation_score(args: &[String]) -> ExitCode {
    let outcomes = arg(args, "--outcomes").unwrap_or_else(|| "mutants.out/outcomes.json".into());
    let thresholds = arg(args, "--thresholds").unwrap_or_else(|| "gates/mutation.toml".into());
    let floor = std::fs::read_to_string(&thresholds)
        .ok()
        .and_then(|t| toml::from_str::<toml::Table>(&t).ok())
        .and_then(|t| t.get("min_killed_ratio").and_then(toml::Value::as_float));
    let Some(floor) = floor else {
        eprintln!("cannot read min_killed_ratio from {thresholds}");
        return ExitCode::FAILURE;
    };
    let score = outcomes
        .split(',')
        .map(|path| {
            std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}")).and_then(|j| Score::from_outcomes(&j))
        })
        .collect::<Result<Vec<Score>, String>>()
        .map(|scores| scores.into_iter().sum::<Score>());
    match score.and_then(|s| evaluate_score(s, floor).map(|_| s)) {
        Ok(s) => {
            println!(
                "{{\"killed\":{},\"missed\":{},\"ratio\":{:.4},\"floor\":{floor}}}",
                s.killed,
                s.missed,
                s.ratio()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn perf_compare(args: &[String]) -> ExitCode {
    let dir = arg(args, "--criterion").unwrap_or_else(|| "target/criterion".into());
    let thresholds = arg(args, "--thresholds").unwrap_or_else(|| "gates/perf.toml".into());
    let limit = std::fs::read_to_string(&thresholds)
        .ok()
        .and_then(|t| toml::from_str::<toml::Table>(&t).ok())
        .and_then(|t| t.get("max_regression").and_then(toml::Value::as_float));
    let Some(limit) = limit else {
        eprintln!("cannot read max_regression from {thresholds}");
        return ExitCode::FAILURE;
    };
    match xtask::perf::compare(std::path::Path::new(&dir), limit) {
        Ok(regressions) if regressions.is_empty() => {
            println!("{{\"regressions\":0,\"limit\":{limit}}}");
            ExitCode::SUCCESS
        }
        Ok(regressions) => {
            for r in &regressions {
                eprintln!("{} regressed by {:.1}%", r.bench, r.change * 100.0);
            }
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn docs_check(args: &[String]) -> ExitCode {
    let docs = arg(args, "--docs").unwrap_or_else(|| "documentation".into());
    let sources = ["examples/guestbook", "examples/doc-snippets"].map(std::path::PathBuf::from);
    match xtask::docs::check(std::path::Path::new(&docs), &sources) {
        Ok(problems) if problems.is_empty() => ExitCode::SUCCESS,
        Ok(problems) => {
            for problem in &problems {
                eprintln!("{problem}");
            }
            eprintln!("{} documentation lines are not in the compiled examples", problems.len());
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("docs check failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("protected-paths") => protected_paths(&args),
        Some("gate") => gate(&args),
        Some("sweep") => sweep_command(&args),
        Some("mutation-score") => mutation_score(&args),
        Some("perf-compare") => perf_compare(&args),
        Some("docs-check") => docs_check(&args),
        _ => {
            eprintln!(
                "usage: cargo xtask <protected-paths|gate|sweep|mutation-score|perf-compare|docs-check> [options]"
            );
            ExitCode::FAILURE
        }
    }
}
