use std::collections::BTreeMap;

use super::{NodeId, Violation};

#[derive(Debug, Clone, Default)]
pub struct SingletonChecker {
    holders: BTreeMap<u64, NodeId>,
    highest: Option<u64>,
    violations: Vec<Violation>,
}

impl SingletonChecker {
    pub fn acquired(&mut self, runner: NodeId, token: u64) {
        if let Some(&first) = self.holders.get(&token) {
            if first != runner {
                self.violations.push(Violation::DuplicateToken { token, first, second: runner });
            }
            return;
        }
        if let Some(previous) = self.highest.filter(|&h| token < h) {
            self.violations.push(Violation::TokenRegressed { previous, token });
        }
        self.holders.insert(token, runner);
        self.highest = Some(self.highest.map_or(token, |h| h.max(token)));
    }

    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }
}
