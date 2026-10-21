use std::collections::BTreeMap;

use super::{NodeId, Violation};

#[derive(Debug, Clone, Default)]
pub struct StateMachineSafety {
    hashes: BTreeMap<u64, u64>,
    violations: Vec<Violation>,
}

impl StateMachineSafety {
    pub fn observe_apply(&mut self, node: NodeId, index: u64, hash: u64) {
        match self.hashes.get(&index) {
            None => {
                self.hashes.insert(index, hash);
            }
            Some(&expected) if expected != hash => {
                self.violations.push(Violation::Divergence { index, node, expected, actual: hash });
            }
            Some(_) => {}
        }
    }

    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }
}
