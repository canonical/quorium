use std::time::Duration;

pub mod determinism;
pub mod durability;
pub mod election;
pub mod escrow;
pub mod linearizability;
pub mod liveness;
pub mod singleton;
pub mod state_machine;

pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    TwoLeaders { term: u64, first: NodeId, second: NodeId },
    Divergence { index: u64, node: NodeId, expected: u64, actual: u64 },
    NotLinearizable { key: String, operations: usize },
    CheckBudgetExceeded { key: String, operations: usize },
    LostWrite { id: u64 },
    OverGrant { node: NodeId, granted: u64, sold: u64 },
    OverSold { stock: u64, sold: u64 },
    DuplicateToken { token: u64, first: NodeId, second: NodeId },
    TokenRegressed { previous: u64, token: u64 },
    NonDeterministic { first: u64, second: u64 },
    NoProgress { healed_at: Duration, bound: Duration },
    NodeFailure { node: NodeId, message: String },
    Panic { message: String },
}
