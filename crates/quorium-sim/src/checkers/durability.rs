use std::collections::BTreeSet;

use super::Violation;

#[derive(Debug, Clone, Default)]
pub struct DurabilityChecker {
    acked: BTreeSet<u64>,
}

impl DurabilityChecker {
    pub fn acknowledged(&mut self, id: u64) {
        self.acked.insert(id);
    }

    pub fn acknowledged_count(&self) -> usize {
        self.acked.len()
    }

    pub fn check(&self, present: impl IntoIterator<Item = u64>) -> Vec<Violation> {
        let present: BTreeSet<u64> = present.into_iter().collect();
        self.acked.difference(&present).map(|&id| Violation::LostWrite { id }).collect()
    }
}
