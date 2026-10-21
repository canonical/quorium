use std::time::Duration;

use crate::network::NodeId;
use crate::rng::SimRng;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FaultKind {
    Crash(NodeId),
    Restart(NodeId),
    Pause { node: NodeId, duration: Duration },
    Partition { left: Vec<NodeId>, right: Vec<NodeId> },
    Isolate(NodeId),
    Cut { from: NodeId, to: NodeId },
    Spike { from: NodeId, to: NodeId, extra: Duration },
    Loss { permille: u32 },
    Duplicate { permille: u32 },
    ClockSkew { node: NodeId, drift_ppm: i32 },
    SlowDisk { node: NodeId, extra: Duration },
    Heal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fault {
    pub at: Duration,
    pub kind: FaultKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FaultScript {
    faults: Vec<Fault>,
    cursor: usize,
}

const SWARM_KINDS: usize = 11;

impl FaultScript {
    pub fn new(mut faults: Vec<Fault>) -> Self {
        faults.sort_by_key(|f| f.at);
        Self { faults, cursor: 0 }
    }

    pub fn faults(&self) -> &[Fault] {
        &self.faults
    }

    pub fn is_done(&self) -> bool {
        self.cursor >= self.faults.len()
    }

    pub fn next_at(&self) -> Option<Duration> {
        self.faults.get(self.cursor).map(|f| f.at)
    }

    pub fn due(&mut self, now: Duration) -> Vec<FaultKind> {
        let mut out = Vec::new();
        while let Some(f) = self.faults.get(self.cursor) {
            if f.at > now {
                break;
            }
            out.push(f.kind.clone());
            self.cursor += 1;
        }
        out
    }

    pub fn swarm(rng: &mut SimRng, nodes: &[NodeId], duration: Duration) -> Self {
        let enabled: Vec<usize> = (0..SWARM_KINDS).filter(|_| rng.chance(0.4)).collect();
        let mut faults = Vec::new();
        if !enabled.is_empty() {
            let count = rng.range(1, 20);
            for _ in 0..count {
                let at = Duration::from_millis(rng.range(0, duration.as_millis() as u64));
                let kind = *rng.pick(&enabled).unwrap_or(&0);
                faults.push(Fault { at, kind: Self::random_kind(rng, kind, nodes) });
            }
        }
        faults.push(Fault { at: duration, kind: FaultKind::Heal });
        faults.push(Fault { at: duration, kind: FaultKind::Loss { permille: 0 } });
        faults.push(Fault { at: duration, kind: FaultKind::Duplicate { permille: 0 } });
        for &n in nodes {
            faults.push(Fault { at: duration, kind: FaultKind::Restart(n) });
        }
        Self::new(faults)
    }

    fn random_kind(rng: &mut SimRng, kind: usize, nodes: &[NodeId]) -> FaultKind {
        let node = *rng.pick(nodes).unwrap_or(&0);
        let other = *rng.pick(nodes).unwrap_or(&0);
        match kind {
            0 => FaultKind::Crash(node),
            1 => FaultKind::Restart(node),
            2 => FaultKind::Pause { node, duration: Duration::from_millis(rng.range(100, 3000)) },
            3 => {
                let mut shuffled = nodes.to_vec();
                rng.shuffle(&mut shuffled);
                let split = rng.range(1, shuffled.len().max(2) as u64) as usize;
                let right = shuffled.split_off(split.min(shuffled.len()));
                FaultKind::Partition { left: shuffled, right }
            }
            4 => FaultKind::Isolate(node),
            5 => FaultKind::Cut { from: node, to: other },
            6 => FaultKind::Spike { from: node, to: other, extra: Duration::from_millis(rng.range(10, 1000)) },
            7 => FaultKind::Loss { permille: rng.range(1, 300) as u32 },
            8 => FaultKind::Duplicate { permille: rng.range(1, 300) as u32 },
            9 => FaultKind::ClockSkew { node, drift_ppm: rng.range(0, 2001) as i32 - 1000 },
            _ => FaultKind::SlowDisk { node, extra: Duration::from_millis(rng.range(1, 200)) },
        }
    }
}
