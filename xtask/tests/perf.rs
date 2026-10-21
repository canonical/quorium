use xtask::perf::{Regression, compare};

fn write(dir: &std::path::Path, bench: &str, lower: f64, point: f64, upper: f64) {
    let path = dir.join(bench).join("change");
    std::fs::create_dir_all(&path).unwrap();
    let json = format!(
        r#"{{"mean":{{"confidence_interval":{{"confidence_level":0.95,"lower_bound":{lower},"upper_bound":{upper}}},"point_estimate":{point},"standard_error":0.01}}}}"#
    );
    std::fs::write(path.join("estimates.json"), json).unwrap();
}

#[test]
fn only_statistically_clear_regressions_are_reported() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "wal_append", 0.00, 0.02, 0.04);
    write(dir.path(), "kv_apply", 0.06, 0.08, 0.10);
    write(dir.path(), "noisy_encode", 0.01, 0.059, 0.11);
    write(dir.path(), "codec_decode", -0.35, -0.30, -0.25);
    let found = compare(dir.path(), 0.05).unwrap();
    assert_eq!(found, vec![Regression { bench: "kv_apply".into(), change: 0.08 }]);
}

#[test]
fn nested_benchmark_groups_are_found() {
    let dir = tempfile::tempdir().unwrap();
    write(&dir.path().join("wal"), "append", 0.2, 0.3, 0.4);
    let found = compare(dir.path(), 0.05).unwrap();
    assert_eq!(found, vec![Regression { bench: "wal/append".into(), change: 0.3 }]);
}

#[test]
fn missing_comparisons_are_an_error() {
    let dir = tempfile::tempdir().unwrap();
    assert!(compare(dir.path(), 0.05).is_err());
}
