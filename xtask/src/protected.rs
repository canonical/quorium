pub const PROTECTED: &[&str] = &[
    "crates/quorium-sim/src/checkers.rs",
    "crates/quorium-sim/src/checkers/",
    "crates/quorium-sim/tests/checkers.rs",
    "crates/quorium-sim/tests/swarm.rs",
    "crates/quorium-sim/tests/regressions.rs",
    "crates/quorium-store/tests/fixtures/",
    "crates/quorium-store/tests/fixtures.rs",
    "crates/quorium-net/tests/fixtures/",
    "crates/quorium-net/tests/wire_fixtures.rs",
    "crates/quorium/tests/fixtures/",
    "crates/quorium/tests/journal_fixture.rs",
    "crates/quorium-sim/tests/conformance.rs",
    "spec/",
    "gates/",
    ".github/",
    "xtask/",
];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Approvals {
    pub reviewer: bool,
    pub owner: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    Mixed { unprotected: Vec<String> },
    MissingReviewer,
    MissingOwner,
}

pub fn is_protected(path: &str) -> bool {
    PROTECTED.iter().any(|p| if p.ends_with('/') { path.starts_with(p) } else { path == *p })
}

pub fn check_change<S: AsRef<str>>(paths: &[S], approvals: Approvals) -> Result<(), Vec<PathError>> {
    let (protected, unprotected): (Vec<&str>, Vec<&str>) =
        paths.iter().map(AsRef::as_ref).partition(|p| is_protected(p));
    if protected.is_empty() {
        return Ok(());
    }
    let mut errors = Vec::new();
    if !unprotected.is_empty() {
        errors.push(PathError::Mixed { unprotected: unprotected.iter().map(|s| s.to_string()).collect() });
    }
    if !approvals.reviewer {
        errors.push(PathError::MissingReviewer);
    }
    if !approvals.owner {
        errors.push(PathError::MissingOwner);
    }
    if errors.is_empty() { Ok(()) } else { Err(errors) }
}
