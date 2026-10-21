use quorium_sim::cluster::Bug;
use quorium_sim::scenario::{ScenarioConfig, run_seed};

fn seeds() -> u64 {
    std::env::var("QUORIUM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(24)
}

#[test]
fn swarm_seeds_have_no_violations() {
    let cfg = ScenarioConfig::default();
    let mut acked = 0;
    for seed in 0..seeds() {
        let report = run_seed(seed, &cfg);
        assert!(report.violations.is_empty(), "seed {seed}: {:?}", report.violations);
        acked += report.acked_writes;
    }
    assert!(acked > 0);
}

#[test]
fn seed_reports_are_reproducible() {
    let cfg = ScenarioConfig::default();
    assert_eq!(run_seed(17, &cfg), run_seed(17, &cfg));
}

#[test]
fn planted_nondeterministic_apply_is_caught() {
    let cfg = ScenarioConfig { bug: Some(Bug::NondeterministicApply), ..ScenarioConfig::default() };
    assert!((0..5).any(|seed| !run_seed(seed, &cfg).violations.is_empty()));
}

#[test]
fn planted_forgotten_vote_is_caught() {
    let cfg = ScenarioConfig { bug: Some(Bug::ForgetVote), ..ScenarioConfig::vote_hunting() };
    assert!((0..200).any(|seed| !run_seed(seed, &cfg).violations.is_empty()));
}

#[test]
fn planted_lost_fsync_is_caught() {
    let cfg = ScenarioConfig { bug: Some(Bug::LostFsync), ..ScenarioConfig::power_loss() };
    assert!((0..20).any(|seed| !run_seed(seed, &cfg).violations.is_empty()));
}

#[test]
fn panics_inside_a_seed_become_violations() {
    let report = quorium_sim::scenario::guard(3, || panic!("planted"));
    assert!(
        matches!(&report.violations[..], [quorium_sim::checkers::Violation::Panic { message }] if message.contains("planted"))
    );
}

#[test]
fn planted_leader_restore_is_caught() {
    let cfg = ScenarioConfig { bug: Some(Bug::LeaderRestore), ..ScenarioConfig::vote_hunting() };
    assert!((0..20).any(|seed| !run_seed(seed, &cfg).violations.is_empty()));
}

#[test]
fn lease_grants_are_exercised_by_the_workload() {
    let report = run_seed(3, &ScenarioConfig::default());
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert!(report.lease_grants > 0);
}

#[test]
fn escrow_sales_are_exercised_and_checked() {
    let report = run_seed(5, &ScenarioConfig::default());
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert!(report.escrow_sales > 10, "{}", report.escrow_sales);
}

#[test]
fn planted_escrow_oversell_is_caught() {
    let cfg = ScenarioConfig { bug: Some(Bug::OversellEscrow), ..ScenarioConfig::default() };
    assert!((0..10).any(|seed| run_seed(seed, &cfg).violations.iter().any(|v| matches!(
        v,
        quorium_sim::checkers::Violation::OverGrant { .. } | quorium_sim::checkers::Violation::OverSold { .. }
    ))));
}

#[test]
fn planted_stale_read_is_caught() {
    let cfg = ScenarioConfig { bug: Some(Bug::StaleRead), ..ScenarioConfig::default() };
    assert!((0..40).any(|seed| {
        run_seed(seed, &cfg)
            .violations
            .iter()
            .any(|v| matches!(v, quorium_sim::checkers::Violation::NotLinearizable { .. }))
    }));
}

#[test]
fn read_index_reads_stay_linearizable() {
    let cfg = ScenarioConfig { lease_reads: false, ..ScenarioConfig::default() };
    for seed in 0..seeds() {
        let report = run_seed(seed, &cfg);
        assert!(report.violations.is_empty(), "seed {seed}: {:?}", report.violations);
    }
}
