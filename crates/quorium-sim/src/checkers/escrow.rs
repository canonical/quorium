use std::collections::BTreeMap;

use super::{NodeId, Violation};

#[derive(Debug, Clone, Default)]
pub struct EscrowChecker {
    stock: u64,
    sold: u64,
    granted: BTreeMap<NodeId, u64>,
    sold_by: BTreeMap<NodeId, u64>,
    violations: Vec<Violation>,
}

impl EscrowChecker {
    pub fn new(stock: u64) -> Self {
        Self { stock, ..Self::default() }
    }

    pub fn refill(&mut self, units: u64) {
        self.stock += units;
    }

    pub fn grant(&mut self, node: NodeId, units: u64) {
        *self.granted.entry(node).or_default() += units;
    }

    pub fn release(&mut self, node: NodeId, units: u64) {
        let g = self.granted.entry(node).or_default();
        *g = g.saturating_sub(units);
    }

    pub fn sell(&mut self, node: NodeId, units: u64) {
        let sold = self.sold_by.entry(node).or_default();
        *sold += units;
        let sold_here = *sold;
        let granted = self.granted.get(&node).copied().unwrap_or_default();
        if sold_here > granted {
            self.violations.push(Violation::OverGrant { node, granted, sold: sold_here });
        }
        self.sold += units;
        if self.sold > self.stock {
            self.violations.push(Violation::OverSold { stock: self.stock, sold: self.sold });
        }
    }

    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }
}
