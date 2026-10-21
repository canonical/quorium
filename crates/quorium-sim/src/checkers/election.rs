use std::collections::BTreeMap;

use super::{NodeId, Violation};

#[derive(Debug, Clone, Default)]
pub struct ElectionSafety {
    leaders: BTreeMap<u64, NodeId>,
    violations: Vec<Violation>,
}

impl ElectionSafety {
    pub fn observe_leader(&mut self, term: u64, node: NodeId) {
        match self.leaders.get(&term) {
            None => {
                self.leaders.insert(term, node);
            }
            Some(&first) if first != node => {
                self.violations.push(Violation::TwoLeaders { term, first, second: node });
            }
            Some(_) => {}
        }
    }

    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }
}
