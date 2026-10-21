use std::time::Duration;

use crate::checkers::Violation;
use crate::cluster::{Bug, ClusterConfig, SimCluster};
use crate::faults::{Fault, FaultKind, FaultScript};
use crate::network::{LinkConfig, NodeId};
use crate::rng::SimRng;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    Swarm,
    VoteHunting,
    PowerLoss,
}

#[derive(Debug, Clone)]
pub struct ScenarioConfig {
    pub nodes: Vec<(NodeId, String)>,
    pub warmup: Duration,
    pub fault_window: Duration,
    pub liveness_bound: Duration,
    pub settle: Duration,
    pub clients: usize,
    pub keys: u64,
    pub op_interval: Duration,
    pub faults: FaultMode,
    pub snapshots: bool,
    pub bug: Option<Bug>,
    pub lease_reads: bool,
    pub max_drift_ppm: u64,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            nodes: [(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b")]
                .iter()
                .map(|(id, dc)| (*id, dc.to_string()))
                .collect(),
            warmup: Duration::from_millis(500),
            fault_window: Duration::from_secs(8),
            liveness_bound: Duration::from_secs(10),
            settle: Duration::from_secs(2),
            clients: 5,
            keys: 3,
            op_interval: Duration::from_millis(40),
            faults: FaultMode::Swarm,
            snapshots: true,
            bug: None,
            lease_reads: true,
            max_drift_ppm: 1_000,
        }
    }
}

impl ScenarioConfig {
    pub fn vote_hunting() -> Self {
        Self {
            nodes: [(1, "a"), (2, "b"), (3, "c")].iter().map(|(id, dc)| (*id, dc.to_string())).collect(),
            faults: FaultMode::VoteHunting,
            clients: 2,
            ..Self::default()
        }
    }

    pub fn power_loss() -> Self {
        Self { faults: FaultMode::PowerLoss, ..Self::default() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedReport {
    pub seed: u64,
    pub violations: Vec<Violation>,
    pub acked_writes: usize,
    pub lease_grants: usize,
    pub escrow_sales: u64,
    pub end: Duration,
    pub final_leader: Option<NodeId>,
}

pub fn script(mode: FaultMode, rng: &mut SimRng, nodes: &[NodeId], window: Duration) -> FaultScript {
    match mode {
        FaultMode::Swarm => FaultScript::swarm(rng, nodes, window),
        FaultMode::VoteHunting => {
            let mut faults = Vec::new();
            let mut t = Duration::ZERO;
            while t < window {
                t += Duration::from_millis(rng.range(30, 200));
                let node = *rng.pick(nodes).unwrap_or(&1);
                faults.push(Fault { at: t, kind: FaultKind::Crash(node) });
                let back = t + Duration::from_millis(rng.range(5, 150));
                faults.push(Fault { at: back, kind: FaultKind::Restart(node) });
                if rng.chance(0.3) {
                    faults.push(Fault { at: t, kind: FaultKind::Isolate(*rng.pick(nodes).unwrap_or(&1)) });
                    faults.push(Fault { at: back, kind: FaultKind::Heal });
                }
            }
            faults.extend(nodes.iter().map(|n| Fault { at: window, kind: FaultKind::Restart(*n) }));
            faults.push(Fault { at: window, kind: FaultKind::Heal });
            FaultScript::new(faults)
        }
        FaultMode::PowerLoss => {
            let at = Duration::from_millis(rng.range(1_000, window.as_millis() as u64 / 2));
            let mut faults: Vec<Fault> = nodes.iter().map(|n| Fault { at, kind: FaultKind::Crash(*n) }).collect();
            let mut order = nodes.to_vec();
            rng.shuffle(&mut order);
            let mut back = at;
            for n in order {
                back += Duration::from_millis(rng.range(10, 400));
                faults.push(Fault { at: back, kind: FaultKind::Restart(n) });
            }
            FaultScript::new(faults)
        }
    }
}

pub fn guard(seed: u64, f: impl FnOnce() -> SeedReport) -> SeedReport {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(report) => report,
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            SeedReport {
                seed,
                violations: vec![Violation::Panic { message }],
                acked_writes: 0,
                lease_grants: 0,
                escrow_sales: 0,
                end: Duration::ZERO,
                final_leader: None,
            }
        }
    }
}

pub fn describe_after_heal(seed: u64, cfg: &ScenarioConfig) -> Vec<String> {
    let mut out = Vec::new();
    let _ = run_seed_inner(seed, cfg, Some(&mut out));
    out
}

pub fn run_seed(seed: u64, cfg: &ScenarioConfig) -> SeedReport {
    guard(seed, || run_seed_inner(seed, cfg, None))
}

fn run_seed_inner(seed: u64, cfg: &ScenarioConfig, mut trace: Option<&mut Vec<String>>) -> SeedReport {
    let mut rng = SimRng::new(seed ^ 0x51_6d_75_72_69_75_6d);
    let nodes: Vec<(NodeId, &str)> = cfg.nodes.iter().map(|(id, dc)| (*id, dc.as_str())).collect();
    let ids: Vec<NodeId> = nodes.iter().map(|(id, _)| *id).collect();
    let mut cluster_cfg = ClusterConfig::new(seed, &nodes);
    cluster_cfg.bug = cfg.bug;
    cluster_cfg.lease_reads = cfg.lease_reads;
    cluster_cfg.max_drift_ppm = cfg.max_drift_ppm;
    if cfg.snapshots && rng.chance(0.5) {
        cluster_cfg.snapshot_every = Some(rng.range(10, 120));
        cluster_cfg.compaction_threshold = Some(rng.range(1024, 16 * 1024));
    }
    let mut cluster = SimCluster::new(cluster_cfg);
    cluster.run_for(cfg.warmup);
    let start = cluster.now();
    let stop = start + cfg.fault_window;
    let stocked = cluster.refill_escrow(60);
    cluster.spawn_clients(cfg.clients, cfg.keys, cfg.op_interval, stop, rng.next_u64());
    let script = script(cfg.faults, &mut rng, &ids, cfg.fault_window);
    for fault in script.faults() {
        let at = start + fault.at;
        if at > cluster.now() {
            cluster.run_for(at - cluster.now());
        }
        cluster.apply_fault(&fault.kind);
    }
    if stop > cluster.now() {
        cluster.run_for(stop - cluster.now());
    }
    cluster.heal();
    cluster.apply_fault(&FaultKind::Loss { permille: 0 });
    for id in &ids {
        cluster.restart(*id);
    }
    let healed = cluster.now();
    if let Some(trace) = trace.as_mut() {
        cluster.run_for(cfg.liveness_bound / 2);
        trace.push(format!("{:?} leaders {:?} rpc {:?}", cluster.now(), cluster.leaders(), cluster.rpc_counts()));
        trace.extend(cluster.describe_nodes());
    }
    let mut violations = Vec::new();
    let live = cluster.write_within("__liveness", seed, cfg.liveness_bound).is_ok();
    if !live {
        violations.push(Violation::NoProgress { healed_at: healed, bound: cfg.liveness_bound });
    }
    cluster.run_for(cfg.settle);
    for _ in 0..10 {
        let applied = cluster.applied_indexes();
        if applied.windows(2).all(|w| w[0] == w[1]) {
            break;
        }
        cluster.run_for(cfg.settle);
    }
    let hashes = cluster.state_hashes();
    if let Some(w) = hashes.windows(2).find(|w| live && w[0] != w[1]) {
        violations.push(Violation::Divergence { index: u64::MAX, node: 0, expected: w[0], actual: w[1] });
    }
    if !stocked {
        violations.push(Violation::NoProgress { healed_at: start, bound: Duration::from_secs(5) });
    }
    violations.extend(cluster.violations());
    violations.extend(cluster.check_durability());
    SeedReport {
        seed,
        violations,
        acked_writes: cluster.acked_writes(),
        lease_grants: cluster.lease_grants(),
        escrow_sales: cluster.escrow_sales(),
        end: cluster.now(),
        final_leader: cluster.leader(),
    }
}

#[derive(Debug, Clone)]
pub struct FailoverConfig {
    pub nodes: Vec<(NodeId, String)>,
    pub inter_dc: Option<LinkConfig>,
    pub warmup: Duration,
}

impl FailoverConfig {
    pub fn single_region() -> Self {
        Self {
            nodes: [(1, "az1"), (2, "az2"), (3, "az3"), (4, "az1"), (5, "az2")]
                .iter()
                .map(|(id, dc)| (*id, dc.to_string()))
                .collect(),
            inter_dc: None,
            warmup: Duration::from_millis(1_000),
        }
    }

    pub fn three_regions() -> Self {
        Self {
            nodes: [(1, "us-east"), (2, "us-east"), (3, "us-west"), (4, "us-west"), (5, "eu-west")]
                .iter()
                .map(|(id, dc)| (*id, dc.to_string()))
                .collect(),
            inter_dc: Some(LinkConfig {
                min_latency: Duration::from_millis(15),
                max_latency: Duration::from_millis(40),
                loss: 0.0,
                duplicate: 0.0,
            }),
            warmup: Duration::from_millis(1_500),
        }
    }
}

pub fn failover_sample(seed: u64, cfg: &FailoverConfig) -> Option<Duration> {
    let nodes: Vec<(NodeId, &str)> = cfg.nodes.iter().map(|(id, dc)| (*id, dc.as_str())).collect();
    let mut cluster_cfg = ClusterConfig::new(seed, &nodes);
    cluster_cfg.inter_dc = cfg.inter_dc.clone();
    let mut cluster = SimCluster::new(cluster_cfg);
    cluster.run_for(cfg.warmup);
    cluster.write("before", 1).ok()?;
    let mut rng = SimRng::new(seed);
    cluster.run_for(Duration::from_micros(rng.range(0, 50_000)));
    let leader = cluster.leader()?;
    cluster.crash(leader);
    let start = cluster.now();
    cluster.write_within("after", 2, Duration::from_secs(10)).ok()?;
    Some(cluster.now() - start)
}

pub fn failover_samples(cfg: &FailoverConfig, seeds: std::ops::Range<u64>) -> Vec<Duration> {
    seeds.filter_map(|seed| failover_sample(seed, cfg)).collect()
}

pub fn percentile(samples: &[Duration], p: f64) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort();
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}
