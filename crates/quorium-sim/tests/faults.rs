use std::time::Duration;

use quorium_sim::faults::{Fault, FaultKind, FaultScript};
use quorium_sim::rng::SimRng;

#[test]
fn script_yields_due_faults_in_order() {
    let mut s = FaultScript::new(vec![
        Fault { at: Duration::from_millis(20), kind: FaultKind::Heal },
        Fault { at: Duration::from_millis(10), kind: FaultKind::Crash(1) },
    ]);
    assert!(s.due(Duration::from_millis(5)).is_empty());
    assert_eq!(s.due(Duration::from_millis(15)), vec![FaultKind::Crash(1)]);
    assert_eq!(s.due(Duration::from_millis(30)), vec![FaultKind::Heal]);
    assert!(s.is_done());
}

#[test]
fn swarm_scripts_are_deterministic_per_seed() {
    let nodes = [1, 2, 3, 4, 5];
    let a = FaultScript::swarm(&mut SimRng::new(77), &nodes, Duration::from_secs(10));
    let b = FaultScript::swarm(&mut SimRng::new(77), &nodes, Duration::from_secs(10));
    assert_eq!(a, b);
}

#[test]
fn swarm_scripts_end_healed_and_restarted() {
    let nodes = [1, 2, 3];
    for seed in 0..200 {
        let s = FaultScript::swarm(&mut SimRng::new(seed), &nodes, Duration::from_secs(10));
        let faults = s.faults();
        let tail: Vec<_> = faults.iter().filter(|f| f.at >= Duration::from_secs(10)).collect();
        assert!(tail.iter().any(|f| f.kind == FaultKind::Heal));
        for n in nodes {
            assert!(tail.iter().any(|f| f.kind == FaultKind::Restart(n)));
        }
        assert!(faults.windows(2).all(|w| w[0].at <= w[1].at));
    }
}

#[test]
fn swarm_varies_fault_mix_across_seeds() {
    let nodes = [1, 2, 3];
    let mixes: std::collections::BTreeSet<_> = (0..100)
        .map(|seed| {
            let s = FaultScript::swarm(&mut SimRng::new(seed), &nodes, Duration::from_secs(10));
            s.faults()
                .iter()
                .map(|f| std::mem::discriminant(&f.kind))
                .map(|d| format!("{d:?}"))
                .collect::<std::collections::BTreeSet<_>>()
        })
        .collect();
    assert!(mixes.len() > 5);
}
