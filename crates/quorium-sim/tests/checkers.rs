use std::time::Duration;

use quorium_sim::checkers::Violation;
use quorium_sim::checkers::determinism::check_determinism;
use quorium_sim::checkers::durability::DurabilityChecker;
use quorium_sim::checkers::election::ElectionSafety;
use quorium_sim::checkers::escrow::EscrowChecker;
use quorium_sim::checkers::linearizability::{History, Register, RegisterOp, RegisterRet, check};
use quorium_sim::checkers::liveness::LivenessChecker;
use quorium_sim::checkers::singleton::SingletonChecker;
use quorium_sim::checkers::state_machine::StateMachineSafety;

#[test]
fn election_safety_accepts_one_leader_per_term() {
    let mut c = ElectionSafety::default();
    c.observe_leader(1, 1);
    c.observe_leader(1, 1);
    c.observe_leader(2, 3);
    assert!(c.violations().is_empty());
}

#[test]
fn election_safety_catches_two_leaders_in_a_term() {
    let mut c = ElectionSafety::default();
    c.observe_leader(4, 1);
    c.observe_leader(4, 2);
    assert_eq!(c.violations(), &[Violation::TwoLeaders { term: 4, first: 1, second: 2 }]);
}

#[test]
fn state_machine_safety_accepts_matching_hashes() {
    let mut c = StateMachineSafety::default();
    c.observe_apply(1, 5, 0xAB);
    c.observe_apply(2, 5, 0xAB);
    c.observe_apply(3, 6, 0xCD);
    assert!(c.violations().is_empty());
}

#[test]
fn state_machine_safety_catches_divergence() {
    let mut c = StateMachineSafety::default();
    c.observe_apply(1, 5, 0xAB);
    c.observe_apply(2, 5, 0xAC);
    assert_eq!(c.violations(), &[Violation::Divergence { index: 5, node: 2, expected: 0xAB, actual: 0xAC }]);
}

fn w(h: &mut History<Register>, client: u64, v: u64, call: u64, ret: Option<u64>) {
    h.record(client, RegisterOp::Write(v), call, ret.map(|r| (r, RegisterRet::Ok)));
}

fn r(h: &mut History<Register>, client: u64, v: Option<u64>, call: u64, ret: u64) {
    h.record(client, RegisterOp::Read, call, Some((ret, RegisterRet::Value(v))));
}

#[test]
fn linearizable_sequential_history() {
    let mut h = History::new();
    w(&mut h, 1, 1, 0, Some(1));
    r(&mut h, 2, Some(1), 2, 3);
    w(&mut h, 1, 2, 4, Some(5));
    r(&mut h, 2, Some(2), 6, 7);
    assert!(check(&h).is_ok());
}

#[test]
fn concurrent_ops_may_reorder() {
    let mut h = History::new();
    w(&mut h, 1, 1, 0, Some(10));
    r(&mut h, 2, None, 1, 2);
    r(&mut h, 3, Some(1), 3, 4);
    assert!(check(&h).is_ok());
}

#[test]
fn stale_read_is_caught() {
    let mut h = History::new();
    w(&mut h, 1, 1, 0, Some(1));
    w(&mut h, 1, 2, 2, Some(3));
    r(&mut h, 2, Some(1), 4, 5);
    assert!(check(&h).is_err());
}

#[test]
fn read_going_backwards_is_caught() {
    let mut h = History::new();
    w(&mut h, 1, 1, 0, Some(100));
    w(&mut h, 2, 2, 0, Some(100));
    r(&mut h, 3, Some(2), 10, 11);
    r(&mut h, 3, Some(1), 12, 13);
    r(&mut h, 3, Some(2), 14, 15);
    assert!(check(&h).is_err());
}

#[test]
fn indeterminate_write_may_or_may_not_apply() {
    let mut h = History::new();
    w(&mut h, 1, 7, 0, None);
    r(&mut h, 2, None, 1, 2);
    r(&mut h, 2, Some(7), 3, 4);
    assert!(check(&h).is_ok());
    let mut h = History::new();
    w(&mut h, 1, 7, 0, None);
    r(&mut h, 2, None, 1, 2);
    assert!(check(&h).is_ok());
}

#[test]
fn cas_semantics() {
    let mut h = History::new();
    w(&mut h, 1, 1, 0, Some(1));
    h.record(2, RegisterOp::Cas(1, 5), 2, Some((3, RegisterRet::Swapped(true))));
    h.record(3, RegisterOp::Cas(1, 6), 4, Some((5, RegisterRet::Swapped(true))));
    assert!(check(&h).is_err());
}

#[test]
fn durability_catches_lost_ack() {
    let mut c = DurabilityChecker::default();
    c.acknowledged(1);
    c.acknowledged(2);
    c.acknowledged(3);
    assert!(c.check([1, 2, 3, 4]).is_empty());
    assert_eq!(c.check([1, 3]), vec![Violation::LostWrite { id: 2 }]);
}

#[test]
fn escrow_accepts_sales_within_grants_and_stock() {
    let mut c = EscrowChecker::new(10);
    c.grant(1, 5);
    c.grant(2, 5);
    c.sell(1, 5);
    c.sell(2, 3);
    assert!(c.violations().is_empty());
}

#[test]
fn escrow_catches_overselling_a_grant() {
    let mut c = EscrowChecker::new(10);
    c.grant(1, 2);
    c.sell(1, 3);
    assert_eq!(c.violations(), vec![Violation::OverGrant { node: 1, granted: 2, sold: 3 }]);
}

#[test]
fn escrow_catches_overselling_stock() {
    let mut c = EscrowChecker::new(4);
    c.grant(1, 3);
    c.grant(2, 3);
    c.sell(1, 3);
    c.sell(2, 3);
    assert!(c.violations().contains(&Violation::OverSold { stock: 4, sold: 6 }));
}

#[test]
fn escrow_released_units_return_to_pool() {
    let mut c = EscrowChecker::new(4);
    c.grant(1, 4);
    c.release(1, 2);
    c.grant(2, 2);
    c.sell(2, 2);
    c.sell(1, 2);
    assert!(c.violations().is_empty());
    c.refill(3);
    c.grant(1, 3);
    c.sell(1, 3);
    assert!(c.violations().is_empty());
}

#[test]
fn singleton_accepts_increasing_tokens() {
    let mut c = SingletonChecker::default();
    c.acquired(1, 1);
    c.acquired(2, 2);
    c.acquired(1, 5);
    assert!(c.violations().is_empty());
}

#[test]
fn singleton_catches_reused_and_decreasing_tokens() {
    let mut c = SingletonChecker::default();
    c.acquired(1, 3);
    c.acquired(2, 3);
    c.acquired(3, 2);
    assert_eq!(
        c.violations(),
        &[
            Violation::DuplicateToken { token: 3, first: 1, second: 2 },
            Violation::TokenRegressed { previous: 3, token: 2 },
        ]
    );
}

#[test]
fn determinism_compares_double_apply() {
    assert!(check_determinism(5, |s: &u64| s * 2).is_none());
    let mut counter = 0u64;
    let flaky = |s: &u64| {
        counter += 1;
        s + counter
    };
    assert_eq!(check_determinism(5, flaky), Some(Violation::NonDeterministic { first: 6, second: 7 }));
}

#[test]
fn liveness_requires_commit_within_bound_after_heal() {
    let bound = Duration::from_secs(5);
    let mut c = LivenessChecker::new(bound);
    c.healed(Duration::from_secs(10));
    c.committed(Duration::from_secs(9));
    assert_eq!(c.check(Duration::from_secs(14)), None);
    assert_eq!(
        c.check(Duration::from_secs(16)),
        Some(Violation::NoProgress { healed_at: Duration::from_secs(10), bound })
    );
    c.committed(Duration::from_secs(12));
    assert_eq!(c.check(Duration::from_secs(100)), None);
}

#[test]
fn many_unobserved_indeterminate_writes_check_quickly() {
    let mut h = History::new();
    for i in 0..60 {
        w(&mut h, 100 + i, 1000 + i, i * 2, None);
    }
    for i in 0..60 {
        w(&mut h, 1, i, 200 + i * 10, Some(201 + i * 10));
        r(&mut h, 2, Some(i), 203 + i * 10, 204 + i * 10);
    }
    let started = std::time::Instant::now();
    assert!(check(&h).is_ok());
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[test]
fn pruning_keeps_indeterminate_writes_that_were_observed() {
    let mut h = History::new();
    w(&mut h, 1, 5, 0, Some(1));
    w(&mut h, 2, 9, 2, None);
    r(&mut h, 3, Some(9), 10, 11);
    r(&mut h, 3, Some(5), 12, 13);
    assert!(check(&h).is_err());
}

#[test]
fn exhausted_budget_is_reported_not_passed() {
    use quorium_sim::checkers::linearizability::check_with_budget;
    let mut h = History::new();
    for i in 0..30 {
        w(&mut h, i, i, 0, Some(1000));
    }
    for i in 0..30 {
        r(&mut h, 100 + i, Some(i), 2000 + i * 2, 2001 + i * 2);
    }
    assert!(matches!(check_with_budget("k", &h, 50), Err(Violation::CheckBudgetExceeded { .. })));
}

#[test]
fn long_histories_check_without_exhausting_the_stack() {
    let worker = std::thread::Builder::new().stack_size(2 * 1024 * 1024).spawn(|| {
        let mut h = History::new();
        let mut t = 0;
        for i in 0..20_000u64 {
            w(&mut h, 1, i, t, Some(t + 1));
            r(&mut h, 2, Some(i), t + 2, t + 3);
            t += 4;
        }
        let started = std::time::Instant::now();
        let ok = check(&h).is_ok();
        (ok, started.elapsed())
    });
    let (ok, elapsed) = worker.unwrap().join().unwrap();
    assert!(ok);
    assert!(elapsed < std::time::Duration::from_secs(10), "{elapsed:?}");
}

#[test]
fn long_non_linearizable_histories_are_still_caught() {
    let worker = std::thread::Builder::new().stack_size(2 * 1024 * 1024).spawn(|| {
        let mut h = History::new();
        let mut t = 0;
        for i in 0..5_000u64 {
            w(&mut h, 1, i, t, Some(t + 1));
            t += 2;
        }
        r(&mut h, 2, Some(3), t, t + 1);
        check(&h).is_err()
    });
    assert!(worker.unwrap().join().unwrap());
}
