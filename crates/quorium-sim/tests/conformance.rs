use std::collections::BTreeSet;
use std::path::PathBuf;

use quorium_sim::scenario::ScenarioConfig;
use quorium_sim::trace::{Role, TraceState, check_with_tlc, record, to_tla};

fn jar() -> Option<PathBuf> {
    std::env::var_os("QUORIUM_TLA_JAR").map(PathBuf::from).filter(|p| p.exists())
}

fn short() -> ScenarioConfig {
    ScenarioConfig { fault_window: std::time::Duration::from_secs(2), ..ScenarioConfig::vote_hunting() }
}

fn state(term: [u64; 3], role: [Role; 3], voted: [usize; 3], log: [&[u64]; 3]) -> TraceState {
    TraceState {
        up: vec![true; 3],
        term: term.to_vec(),
        role: role.to_vec(),
        voted: voted.to_vec(),
        log: log.iter().map(|l| l.to_vec()).collect(),
        committed: BTreeSet::new(),
    }
}

#[test]
fn traces_render_as_a_tla_module() {
    let s = state([1, 1, 0], [Role::Leader, Role::Follower, Role::Follower], [1, 1, 0], [&[1], &[], &[]]);
    let text = to_tla(&[s]);
    assert!(text.starts_with("---- MODULE TraceData ----"));
    assert!(text.contains("TraceNodes == 1..3"));
    assert!(text.contains("term |-> <<1, 1, 0>>"));
    assert!(text.contains("role |-> <<\"L\", \"F\", \"F\">>"));
    assert!(text.contains("log |-> <<<<1>>, <<>>, <<>>>>"));
    assert!(text.contains("committed |-> {}"));
    assert!(text.trim_end().ends_with("===="));
}

#[test]
fn recorded_traces_cover_elections_and_commits() {
    let trace = record(7, &short());
    assert!(trace.len() > 10, "{} states", trace.len());
    assert!(trace.iter().all(|s| s.term.len() == 3 && s.log.len() == 3));
    assert!(trace.iter().any(|s| s.role.contains(&Role::Leader)));
    assert!(!trace.last().unwrap().committed.is_empty());
    assert!(trace.windows(2).all(|w| w[0] != w[1]));
}

#[test]
fn real_traces_conform_to_the_spec() {
    let Some(jar) = jar() else {
        eprintln!("QUORIUM_TLA_JAR is not set; skipping TLC");
        return;
    };
    let seeds: u64 = std::env::var("QUORIUM_CONFORMANCE_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(3);
    let from: u64 = std::env::var("QUORIUM_CONFORMANCE_FROM").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let swarm = ScenarioConfig { fault_window: std::time::Duration::from_secs(2), ..ScenarioConfig::default() };
    for seed in from..from + seeds {
        for cfg in [&short(), &swarm] {
            let trace = record(seed, cfg);
            check_with_tlc(&trace, &jar).unwrap_or_else(|e| panic!("seed {seed} {:?}: {e}", cfg.faults));
        }
    }
}

#[test]
fn tlc_rejects_two_leaders_in_one_term() {
    let Some(jar) = jar() else { return };
    let a = state([0, 0, 0], [Role::Follower; 3], [0, 0, 0], [&[], &[], &[]]);
    let b = state([1, 1, 1], [Role::Leader, Role::Leader, Role::Follower], [1, 2, 1], [&[1], &[1], &[]]);
    let err = check_with_tlc(&[a, b], &jar).unwrap_err();
    assert!(err.contains("ElectionSafety"), "{err}");
}

#[test]
fn tlc_rejects_a_changed_vote_within_a_term() {
    let Some(jar) = jar() else { return };
    let a = state([1, 1, 1], [Role::Candidate, Role::Follower, Role::Follower], [1, 1, 0], [&[], &[], &[]]);
    let b = state([1, 1, 1], [Role::Candidate, Role::Follower, Role::Follower], [1, 3, 0], [&[], &[], &[]]);
    let err = check_with_tlc(&[a, b], &jar).unwrap_err();
    assert!(err.contains("VotesAreStable"), "{err}");
}

#[test]
fn tlc_rejects_entries_that_no_leader_wrote() {
    let Some(jar) = jar() else { return };
    let a = state([1, 1, 1], [Role::Leader, Role::Follower, Role::Follower], [1, 1, 1], [&[1], &[], &[]]);
    let b = state([1, 1, 1], [Role::Leader, Role::Follower, Role::Follower], [1, 1, 1], [&[1], &[1, 1], &[]]);
    let err = check_with_tlc(&[a, b], &jar).unwrap_err();
    assert!(err.contains("EntriesComeFromLeaders"), "{err}");
}

#[test]
#[ignore]
fn dump_trace() {
    let seed = std::env::var("TRACE_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let trace = record(seed, &short());
    let dir = std::env::var("TRACE_DIR").unwrap();
    std::fs::write(format!("{dir}/TraceData.tla"), to_tla(&trace)).unwrap();
}

#[test]
fn conformance_catches_the_planted_forgotten_vote() {
    let Some(jar) = jar() else { return };
    let cfg = ScenarioConfig { bug: Some(quorium_sim::cluster::Bug::ForgetVote), ..short() };
    let caught = (1..=20).find_map(|seed| check_with_tlc(&record(seed, &cfg), &jar).err().map(|e| (seed, e)));
    let (seed, err) = caught.expect("no seed exposed the forgotten vote");
    assert!(err.contains("violated"), "seed {seed}: {err}");
    eprintln!("seed {seed}: {err}");
}
