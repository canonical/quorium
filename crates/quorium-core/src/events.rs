use std::sync::Arc;
use std::time::Duration;

use crate::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LeadershipEvent {
    ElectionStarted { shard: u64, term: u64, pre_vote: bool },
    LeaderChanged { shard: u64, term: u64, leader: Option<NodeId> },
    PeerUnreachable { shard: u64, peer: NodeId },
    PeerReachable { shard: u64, peer: NodeId },
    Lagged { missed: u64 },
}

pub type EventSink = Arc<dyn Fn(LeadershipEvent) + Send + Sync>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LeadershipStats {
    pub leader: Option<NodeId>,
    pub term: u64,
    pub leader_since: Option<Duration>,
    pub last_election: Option<Duration>,
    pub elections_started: u64,
    pub leader_changes: u64,
    pub leaderless: Duration,
}

impl LeadershipStats {
    pub fn observe(&mut self, leader: Option<NodeId>, term: u64, now: Duration, elapsed: Duration) -> bool {
        if self.leader.is_none() {
            self.leaderless += elapsed;
        }
        if (leader, term) == (self.leader, self.term) {
            return false;
        }
        let changed = leader.is_some() || self.leader.is_some();
        self.leader = leader;
        self.term = term;
        if changed {
            self.leader_since = leader.map(|_| now);
            self.leader_changes += 1;
        }
        changed
    }

    pub fn election_started(&mut self, now: Duration) {
        self.elections_started += 1;
        self.last_election = Some(now);
    }

    pub fn stable_for(&self, window: Duration, now: Duration) -> bool {
        let led_long_enough = self.leader_since.is_some_and(|since| now.saturating_sub(since) >= window);
        let no_recent_election = self.last_election.is_none_or(|at| now.saturating_sub(at) >= window);
        led_long_enough && no_recent_election
    }
}
