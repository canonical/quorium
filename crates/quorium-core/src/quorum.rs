use std::collections::BTreeSet;

pub fn majority(voters: usize) -> usize {
    voters / 2 + 1
}

pub fn has_quorum<T: Ord>(voters: &[T], acks: &[T]) -> bool {
    let acked: BTreeSet<&T> = acks.iter().filter(|a| voters.contains(a)).collect();
    let distinct: BTreeSet<&T> = voters.iter().collect();
    acked.len() >= majority(distinct.len())
}
