use super::Violation;

pub fn check_determinism<S>(state: S, mut apply: impl FnMut(&S) -> u64) -> Option<Violation> {
    let first = apply(&state);
    let second = apply(&state);
    (first != second).then_some(Violation::NonDeterministic { first, second })
}
