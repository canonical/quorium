use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use crate::cluster::{ClusterConfig, SimCluster, StoreKind};
use crate::faults::FaultKind;
use crate::network::NodeId;
use crate::rng::SimRng;
use crate::scenario::{ScenarioConfig, script};

const LADDER: &str = include_str!("../spec/Ladder.tla");
const TRACE_CHECK: &str = include_str!("../spec/TraceCheck.tla");
const TRACE_CFG: &str = include_str!("../spec/TraceCheck.cfg");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Role {
    fn tla(self) -> &'static str {
        match self {
            Role::Follower => "\"F\"",
            Role::Candidate => "\"C\"",
            Role::Leader => "\"L\"",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceState {
    pub up: Vec<bool>,
    pub term: Vec<u64>,
    pub role: Vec<Role>,
    pub voted: Vec<usize>,
    pub log: Vec<Vec<u64>>,
    pub committed: BTreeSet<(u64, u64, u64)>,
}

fn tuple<T>(items: &[T], f: impl Fn(&T) -> String) -> String {
    format!("<<{}>>", items.iter().map(f).collect::<Vec<_>>().join(", "))
}

pub fn to_tla(trace: &[TraceState]) -> String {
    let nodes = trace.first().map_or(0, |s| s.term.len());
    let mut out = String::from("---- MODULE TraceData ----\nEXTENDS Naturals, Sequences\n\n");
    let _ = writeln!(out, "TraceNodes == 1..{nodes}\n");
    out.push_str("Trace == <<\n");
    for (k, s) in trace.iter().enumerate() {
        let committed: Vec<String> = s.committed.iter().map(|(i, t, o)| format!("<<{i}, {t}, {o}>>")).collect();
        let _ = write!(
            out,
            "  [up |-> {}, term |-> {}, role |-> {}, voted |-> {}, log |-> {}, committed |-> {{{}}}]",
            tuple(&s.up, |u| if *u { "TRUE".into() } else { "FALSE".into() }),
            tuple(&s.term, u64::to_string),
            tuple(&s.role, |r| r.tla().to_string()),
            tuple(&s.voted, usize::to_string),
            tuple(&s.log, |l| tuple(l, u64::to_string)),
            committed.join(", "),
        );
        out.push_str(if k + 1 < trace.len() { ",\n" } else { "\n" });
    }
    out.push_str(">>\n\n====\n");
    out
}

struct Recorder {
    ids: Vec<NodeId>,
    last: Vec<Option<crate::cluster::NodeView>>,
    first_seen: BTreeMap<(u64, u64), u64>,
    states: Vec<TraceState>,
}

impl Recorder {
    fn new(ids: Vec<NodeId>) -> Self {
        let last = vec![None; ids.len()];
        Self { ids, last, first_seen: BTreeMap::new(), states: Vec::new() }
    }

    fn position(&self, id: NodeId) -> usize {
        self.ids.iter().position(|n| *n == id).map_or(0, |p| p + 1)
    }

    fn sample(&mut self, cluster: &SimCluster) {
        let mut up = Vec::new();
        let mut term = Vec::new();
        let mut role = Vec::new();
        let mut voted = Vec::new();
        let mut log = Vec::new();
        let mut applied = Vec::new();
        for (k, id) in self.ids.clone().into_iter().enumerate() {
            let view = cluster.node_view(id);
            up.push(view.is_some());
            if let Some(v) = view {
                self.last[k] = Some(v);
            }
            let v = self.last[k].clone();
            let is_up = up[k];
            term.push(v.as_ref().map_or(0, |v| v.term));
            role.push(match v.as_ref() {
                Some(v) if is_up && v.leader => Role::Leader,
                Some(v) if is_up && v.candidate => Role::Candidate,
                _ => Role::Follower,
            });
            let previous = self.states.last().map(|s| (s.term[k], s.voted[k]));
            voted.push(match v.as_ref() {
                None => 0,
                Some(v) if !v.vote_committed || v.voted == id => self.position(v.voted),
                Some(v) => previous.filter(|(t, _)| *t == v.term).map_or(0, |(_, p)| p),
            });
            log.push(v.as_ref().map(|v| v.log.clone()).unwrap_or_default());
            applied.push(if is_up { v.as_ref().and_then(|v| v.applied) } else { None });
        }
        let observed = term.iter().copied().max().unwrap_or(0);
        for (l, a) in log.iter().zip(&applied) {
            let Some(a) = a else { continue };
            for (index, t) in l.iter().enumerate().take(*a as usize + 1) {
                self.first_seen.entry((index as u64 + 1, *t)).or_insert(observed);
            }
        }
        let committed = self.first_seen.iter().map(|((i, t), o)| (*i, *t, *o)).collect();
        let state = TraceState { up, term, role, voted, log, committed };
        if self.states.last() != Some(&state) {
            self.states.push(state);
        }
    }

    fn run(&mut self, cluster: &mut SimCluster, d: Duration) {
        let step = Duration::from_millis(1);
        let end = cluster.now() + d;
        while cluster.now() < end {
            cluster.run_for(step.min(end - cluster.now()));
            self.sample(cluster);
        }
    }

    fn run_until(&mut self, cluster: &mut SimCluster, at: Duration) {
        if at > cluster.now() {
            self.run(cluster, at - cluster.now());
        }
    }
}

pub fn record(seed: u64, cfg: &ScenarioConfig) -> Vec<TraceState> {
    let mut rng = SimRng::new(seed ^ 0x51_6d_75_72_69_75_6d);
    let nodes: Vec<(NodeId, &str)> = cfg.nodes.iter().map(|(id, dc)| (*id, dc.as_str())).collect();
    let mut ids: Vec<NodeId> = nodes.iter().map(|(id, _)| *id).collect();
    ids.sort();
    let mut config = ClusterConfig::new(seed, &nodes);
    config.store = StoreKind::Mem;
    config.bug = cfg.bug;
    let mut cluster = SimCluster::new(config);
    let mut rec = Recorder::new(ids.clone());
    rec.sample(&cluster);
    rec.run(&mut cluster, cfg.warmup);
    let start = cluster.now();
    let stop = start + cfg.fault_window;
    cluster.spawn_clients(cfg.clients, cfg.keys, cfg.op_interval, stop, rng.next_u64());
    for fault in script(cfg.faults, &mut rng, &ids, cfg.fault_window).faults() {
        rec.run_until(&mut cluster, start + fault.at);
        cluster.apply_fault(&fault.kind);
        rec.sample(&cluster);
    }
    rec.run_until(&mut cluster, stop);
    cluster.heal();
    cluster.apply_fault(&FaultKind::Loss { permille: 0 });
    for id in &ids {
        cluster.restart(*id);
    }
    rec.run(&mut cluster, cfg.settle);
    rec.states
}

pub fn check_with_tlc(trace: &[TraceState], jar: &Path) -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let files = [
        ("Ladder.tla", LADDER.to_string()),
        ("TraceCheck.tla", TRACE_CHECK.to_string()),
        ("TraceCheck.cfg", TRACE_CFG.to_string()),
        ("TraceData.tla", to_tla(trace)),
    ];
    for (name, text) in files {
        std::fs::write(dir.path().join(name), text).map_err(|e| e.to_string())?;
    }
    let output = Command::new("java")
        .current_dir(dir.path())
        .arg(format!("-Djava.io.tmpdir={}", dir.path().display()))
        .args(["-Xmx2g", "-XX:+UseParallelGC", "-cp"])
        .arg(jar)
        .args(["tlc2.TLC", "-deadlock", "-workers", "1", "-config", "TraceCheck.cfg", "TraceCheck.tla"])
        .output()
        .map_err(|e| format!("could not run java: {e}"))?;
    let text = String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr);
    if text.contains("No error has been found") {
        return Ok(());
    }
    let reason = text.lines().filter(|l| l.contains("Error") || l.contains("violated")).collect::<Vec<_>>().join("\n");
    Err(if reason.is_empty() { text } else { reason })
}
