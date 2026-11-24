use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use openraft::async_runtime::watch::WatchReceiver;
use quorium_core::types::NodeId;
use quorium_net::raft::{ProdTypes, Request, TcpNetwork};

use crate::service::{PeerBook, Shard};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepDown {
    pub deadline: Duration,
    pub stand_aside: Duration,
    pub prefer: Option<NodeId>,
}

impl Default for StepDown {
    fn default() -> Self {
        Self { deadline: Duration::from_secs(10), stand_aside: Duration::from_secs(60), prefer: None }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HandoffReport {
    pub shards: Vec<ShardHandoff>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardHandoff {
    pub shard: u64,
    pub new_leader: Option<NodeId>,
    pub took: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandoffError {
    StillLeading { shards: Vec<u64>, report: HandoffReport },
}

impl fmt::Display for HandoffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandoffError::StillLeading { shards, .. } => write!(f, "this node still leads shards {shards:?}"),
        }
    }
}

impl std::error::Error for HandoffError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferFailed {
    Busy,
    TimedOut,
    Stopped,
}

type Metrics = openraft::RaftMetrics<ProdTypes>;

fn matched(m: &Metrics, target: NodeId) -> Option<u64> {
    m.replication.as_ref().and_then(|r| r.get(&target).cloned().flatten()).map(|l| l.index)
}

pub(crate) fn target_caught_up(m: &Metrics, target: NodeId) -> bool {
    quorium_core::ladder::target_caught_up(matched(m, target), m.last_log_index)
}

pub(crate) async fn transfer(shard: &Shard, target: NodeId, budget: Duration) -> Result<Duration, TransferFailed> {
    if !shard.gate.hold() {
        return Err(TransferFailed::Busy);
    }
    let started = Instant::now();
    let moved = tokio::time::timeout(budget, async {
        shard.gate.drained().await;
        let raft = &shard.raft;
        raft.wait(None)
            .metrics(|m| target_caught_up(m, target), "transfer target caught up")
            .await
            .map_err(|_| TransferFailed::Stopped)?;
        raft.trigger().transfer_leader(target).await.map_err(|_| TransferFailed::Stopped)?;
        raft.wait(None)
            .metrics(|m| m.current_leader == Some(target), "transfer confirmed")
            .await
            .map_err(|_| TransferFailed::Stopped)?;
        Ok(())
    })
    .await
    .unwrap_or(Err(TransferFailed::TimedOut));
    shard.gate.release();
    moved.map(|()| started.elapsed())
}

const PEER_FRESH: Duration = Duration::from_secs(2);

const MAX_CONCURRENT_HANDOFFS: usize = 8;

pub(crate) fn live_peers(peers: &PeerBook) -> BTreeMap<NodeId, String> {
    peers
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, (_, at))| at.elapsed() < PEER_FRESH)
        .map(|(id, (addr, _))| (*id, addr.clone()))
        .collect()
}

fn choose_target(
    m: &Metrics,
    shard: &Shard,
    me: NodeId,
    live: &BTreeMap<NodeId, String>,
    counts: &BTreeMap<NodeId, usize>,
    prefer: Option<NodeId>,
) -> Option<NodeId> {
    let eligible: Vec<NodeId> = m
        .membership_config
        .membership()
        .voter_ids()
        .filter(|v| *v != me && live.contains_key(v) && shard.ladder.is_candidate(*v))
        .collect();
    if let Some(p) = prefer.filter(|p| eligible.contains(p)) {
        return Some(p);
    }
    eligible.into_iter().min_by_key(|v| (Reverse(matched(m, *v)), counts.get(v).copied().unwrap_or(0), *v))
}

pub(crate) fn leader_counts(shards: &[Arc<Shard>]) -> BTreeMap<NodeId, usize> {
    let mut counts = BTreeMap::new();
    for s in shards {
        if let Some(l) = s.raft.metrics().borrow_watched().current_leader {
            *counts.entry(l).or_default() += 1;
        }
    }
    counts
}

async fn announce_stand_aside(network: &TcpNetwork, live: &BTreeMap<NodeId, String>, duration: Duration) {
    let for_ms = duration.as_millis() as u64;
    let notices = live.values().map(|addr| {
        let network = network.clone();
        async move { network.request_addr(addr, Request::StandAside { for_ms }, PEER_FRESH / 2).await }
    });
    futures::future::join_all(notices).await;
}

pub(crate) async fn step_down(
    shards: &[Arc<Shard>],
    me: NodeId,
    peers: &PeerBook,
    network: &TcpNetwork,
    opts: StepDown,
) -> Result<HandoffReport, HandoffError> {
    let started = Instant::now();
    let deadline = started + opts.deadline;
    let live = live_peers(peers);
    if !opts.stand_aside.is_zero() {
        for s in shards {
            s.ladder.stand_aside(opts.stand_aside);
        }
        announce_stand_aside(network, &live, opts.stand_aside).await;
    }
    let mut counts = leader_counts(shards);
    let mut plan = Vec::new();
    for s in shards.iter().filter(|s| s.is_leader(me)) {
        let m = s.raft.metrics().borrow_watched().clone();
        let target = choose_target(&m, s, me, &live, &counts, opts.prefer);
        if let Some(t) = target {
            *counts.entry(t).or_default() += 1;
        }
        plan.push((s.clone(), target));
    }
    let shards_moved: Vec<ShardHandoff> = futures::stream::iter(plan)
        .map(|(s, target)| async move {
            let shard_started = Instant::now();
            if let Some(target) = target {
                let budget = deadline.saturating_duration_since(Instant::now());
                if let Err(e) = transfer(&s, target, budget).await {
                    tracing::warn!(shard = s.id, to = target, error = ?e, "leadership handoff did not complete");
                }
            }
            let leader = s.raft.metrics().borrow_watched().current_leader;
            ShardHandoff { shard: s.id, new_leader: leader.filter(|l| *l != me), took: shard_started.elapsed() }
        })
        .buffer_unordered(MAX_CONCURRENT_HANDOFFS)
        .collect()
        .await;
    let mut report = HandoffReport { shards: shards_moved };
    report.shards.sort_by_key(|h| h.shard);
    let still: Vec<u64> = shards.iter().filter(|s| s.is_leader(me)).map(|s| s.id).collect();
    if still.is_empty() {
        tracing::info!(node = me, shards = report.shards.len(), took = ?started.elapsed(), "stepped down");
        Ok(report)
    } else {
        Err(HandoffError::StillLeading { shards: still, report })
    }
}
