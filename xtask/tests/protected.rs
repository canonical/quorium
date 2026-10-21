use xtask::protected::{Approvals, PathError, check_change, is_protected};

#[test]
fn protected_paths_are_recognised() {
    for p in [
        "crates/quorium-sim/src/checkers.rs",
        "crates/quorium-sim/src/checkers/linearizability.rs",
        "crates/quorium-sim/tests/checkers.rs",
        "crates/quorium-sim/tests/swarm.rs",
        "crates/quorium-sim/tests/regressions.rs",
        "spec/Ladder.tla",
        "gates/phase1.toml",
        ".github/workflows/ci.yml",
        "xtask/src/protected.rs",
        "crates/quorium-store/tests/fixtures/wal-v1.bin",
        "crates/quorium-store/tests/fixtures.rs",
    ] {
        assert!(is_protected(p), "{p}");
    }
    for p in ["crates/quorium-core/src/ladder.rs", "crates/quorium-sim/src/cluster.rs", "README.md"] {
        assert!(!is_protected(p), "{p}");
    }
}

#[test]
fn ordinary_change_needs_no_approval() {
    assert_eq!(check_change(&["crates/quorium-core/src/ladder.rs"], Approvals::default()), Ok(()));
}

#[test]
fn protected_change_must_touch_nothing_else() {
    let err = check_change(
        &["crates/quorium-sim/src/checkers.rs", "crates/quorium-core/src/ladder.rs"],
        Approvals { reviewer: true, owner: true },
    );
    assert_eq!(err, Err(vec![PathError::Mixed { unprotected: vec!["crates/quorium-core/src/ladder.rs".into()] }]));
}

#[test]
fn protected_change_needs_reviewer_and_owner() {
    let paths = ["gates/phase1.toml"];
    assert_eq!(
        check_change(&paths, Approvals::default()),
        Err(vec![PathError::MissingReviewer, PathError::MissingOwner])
    );
    assert_eq!(check_change(&paths, Approvals { reviewer: true, owner: false }), Err(vec![PathError::MissingOwner]));
    assert_eq!(check_change(&paths, Approvals { reviewer: true, owner: true }), Ok(()));
}
