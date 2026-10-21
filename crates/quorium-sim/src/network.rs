use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::rng::SimRng;

pub type NodeId = u64;
pub type Delivery = Vec<Duration>;

#[derive(Debug, Clone, PartialEq)]
pub struct LinkConfig {
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub loss: f64,
    pub duplicate: f64,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            min_latency: Duration::from_micros(200),
            max_latency: Duration::from_millis(2),
            loss: 0.0,
            duplicate: 0.0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SimNetwork {
    config: LinkConfig,
    links: BTreeMap<(NodeId, NodeId), LinkConfig>,
    blocked: BTreeSet<(NodeId, NodeId)>,
    spikes: BTreeMap<(NodeId, NodeId), Duration>,
}

impl SimNetwork {
    pub fn new(config: LinkConfig) -> Self {
        Self { config, ..Self::default() }
    }

    pub fn set_config(&mut self, config: LinkConfig) {
        self.config = config;
    }

    pub fn config(&self) -> &LinkConfig {
        &self.config
    }

    pub fn set_link(&mut self, from: NodeId, to: NodeId, config: LinkConfig) {
        self.links.insert((from, to), config);
    }

    pub fn partition(&mut self, left: &[NodeId], right: &[NodeId]) {
        for &a in left {
            for &b in right {
                self.blocked.insert((a, b));
                self.blocked.insert((b, a));
            }
        }
    }

    pub fn isolate(&mut self, node: NodeId, all: &[NodeId]) {
        self.partition(&[node], all);
    }

    pub fn cut(&mut self, from: NodeId, to: NodeId) {
        self.blocked.insert((from, to));
    }

    pub fn spike(&mut self, from: NodeId, to: NodeId, extra: Duration) {
        self.spikes.insert((from, to), extra);
    }

    pub fn heal(&mut self) {
        self.blocked.clear();
        self.spikes.clear();
    }

    pub fn reachable(&self, from: NodeId, to: NodeId) -> bool {
        from == to || !self.blocked.contains(&(from, to))
    }

    pub fn send(&mut self, from: NodeId, to: NodeId, rng: &mut SimRng) -> Delivery {
        if from == to {
            return vec![Duration::ZERO];
        }
        if !self.reachable(from, to) {
            return Vec::new();
        }
        let config = self.links.get(&(from, to)).unwrap_or(&self.config).clone();
        if rng.chance(config.loss) {
            return Vec::new();
        }
        let extra = self.spikes.get(&(from, to)).copied().unwrap_or_default();
        let copies = if rng.chance(config.duplicate) { 2 } else { 1 };
        (0..copies)
            .map(|_| {
                let lo = config.min_latency.as_nanos() as u64;
                let hi = config.max_latency.as_nanos() as u64;
                Duration::from_nanos(rng.range(lo, hi + 1)) + extra
            })
            .collect()
    }
}
