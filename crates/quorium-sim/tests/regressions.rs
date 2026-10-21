use quorium_sim::scenario::{ScenarioConfig, run_seed};

const CORPUS: &[(u64, &str)] = &[
    (27, "linearizability search exploded on unobserved indeterminate writes"),
    (75, "linearizability search exhausted memory"),
    (2331, "restored leader reused log id 1.91 for a different entry"),
    (2358, "restored leader reused log ids after losing unflushed entries"),
    (3490, "restored leader reused a log id"),
    (4568, "restored leader reused a log id"),
    (4966, "restored leader reused a log id"),
    (6842, "two leaders in one term under advanced leader ids"),
    (7465, "two leaders in one term under advanced leader ids"),
    (8935, "two leaders in one term under advanced leader ids"),
    (9176, "persisted committed index ahead of the durable log"),
    (9350, "persisted committed index ahead of the durable log"),
    (14, "stepped-down leader stayed in the ladder's leading phase"),
    (806, "split vote left candidates campaigning forever"),
    (970, "split vote left candidates campaigning forever"),
    (1757, "split vote left candidates campaigning forever"),
    (4584, "split vote left candidates campaigning forever"),
    (7495, "split vote left candidates campaigning forever"),
    (9679, "split vote left candidates campaigning forever"),
];

#[test]
fn failing_seed_corpus_stays_clean() {
    let cfg = ScenarioConfig::default();
    for (seed, reason) in CORPUS {
        let report = run_seed(*seed, &cfg);
        assert!(report.violations.is_empty(), "seed {seed} ({reason}): {:?}", report.violations);
    }
}
