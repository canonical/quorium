use std::time::Duration;

use xtask::gate::{Measured, Thresholds, evaluate};

fn thresholds() -> Thresholds {
    toml::from_str(
        r#"
        min_seeds = 10000
        max_violations = 0
        failover_p99_ms = 500
        planted_controls = ["apply", "vote", "fsync", "restore"]
        "#,
    )
    .unwrap()
}

fn passing() -> Measured {
    Measured {
        seeds: 10_000,
        violations: 0,
        failover_p99_single: Duration::from_millis(170),
        failover_p99_three: Duration::from_millis(480),
        planted_caught: vec!["apply".into(), "vote".into(), "fsync".into(), "restore".into()],
    }
}

#[test]
fn all_numbers_within_thresholds_pass() {
    let report = evaluate(&thresholds(), &passing());
    assert!(report.passed(), "{report:?}");
    assert_eq!(report.checks.len(), 5);
}

#[test]
fn any_miss_fails_the_gate_with_the_measured_value() {
    let mut m = passing();
    m.violations = 1;
    m.failover_p99_three = Duration::from_millis(501);
    m.planted_caught.retain(|c| c != "restore");
    m.seeds = 9_999;
    let report = evaluate(&thresholds(), &m);
    assert!(!report.passed());
    let failed: Vec<&str> = report.checks.iter().filter(|c| !c.pass).map(|c| c.name.as_str()).collect();
    assert_eq!(failed, vec!["seeds", "violations", "failover_p99_three_regions", "planted_controls"]);
    assert!(report.to_json().contains("\"passed\":false"));
}

#[test]
fn json_escapes_quotes_and_backslashes() {
    let report = evaluate(&thresholds(), &passing());
    let json = report.to_json();
    assert!(json.contains(r#"caught [\"apply\", \"vote\", \"fsync\", \"restore\"]"#), "{json}");
    assert!(!json.contains(r#"caught [""#));
}
