use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::alias::{InstantOf, VoteOf};
use openraft::async_runtime::watch::WatchReceiver;
use openraft::raft::VoteRequest;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use openraft::{AsyncRuntime, Instant, Raft, ServerState};

use crate::ladder::{Action, Controller, LadderConfig, LogPosition, Member};
use crate::types::{LogId, NodeId, Types};

pub fn position(log_id: Option<&LogId>) -> LogPosition {
    log_id.map_or(LogPosition::default(), |l| LogPosition { term: l.committed_leader_id().term, index: l.index() })
}

struct Shared {
    controller: Controller,
    pending: Option<Action>,
    last_contact: Option<(NodeId, Duration)>,
}

pub struct Ladder<RT: AsyncRuntime> {
    shared: Arc<Mutex<Shared>>,
    datacenters: Arc<Mutex<BTreeMap<NodeId, String>>>,
    epoch: InstantOf<Types<RT>>,
    tick: Duration,
    shard: u64,
}

impl<RT: AsyncRuntime> Clone for Ladder<RT> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            datacenters: self.datacenters.clone(),
            epoch: self.epoch,
            tick: self.tick,
            shard: self.shard,
        }
    }
}

impl<RT: AsyncRuntime> Ladder<RT> {
    pub fn new(config: LadderConfig, me: NodeId, datacenters: BTreeMap<NodeId, String>, tick: Duration) -> Self {
        let members = datacenters.iter().map(|(id, dc)| Member::new(*id, dc.clone())).collect();
        let controller = Controller::new(config, me, members);
        Self {
            shared: Arc::new(Mutex::new(Shared { controller, pending: None, last_contact: None })),
            datacenters: Arc::new(Mutex::new(datacenters)),
            epoch: InstantOf::<Types<RT>>::now(),
            tick,
            shard: 0,
        }
    }

    pub fn for_shard(
        config: LadderConfig,
        me: NodeId,
        datacenters: BTreeMap<NodeId, String>,
        tick: Duration,
        shard: u64,
    ) -> Self {
        let mut ladder = Self::new(config, me, datacenters, tick);
        ladder.shard = shard;
        {
            let mut shared = ladder.shared.lock().unwrap();
            let controller =
                std::mem::replace(&mut shared.controller, Controller::new(LadderConfig::default(), me, Vec::new()));
            shared.controller = controller.with_rotation(shard);
        }
        ladder
    }

    pub fn datacenter_of(&self, id: NodeId) -> Option<String> {
        self.datacenters.lock().unwrap().get(&id).cloned()
    }

    pub fn set_datacenter(&self, id: NodeId, datacenter: &str) {
        self.datacenters.lock().unwrap().insert(id, datacenter.to_string());
    }

    pub fn now(&self) -> Duration {
        InstantOf::<Types<RT>>::now() - self.epoch
    }

    pub fn describe(&self) -> String {
        format!("{:?}", self.shared.lock().unwrap().controller)
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        self.shared.lock().unwrap().controller.next_deadline()
    }

    pub fn rank(&self) -> usize {
        self.shared.lock().unwrap().controller.rank()
    }

    pub fn observe_peer(&self, peer: NodeId) {
        let now = self.now();
        self.shared.lock().unwrap().controller.on_peer_seen(peer, now);
    }

    pub fn observe_ping(&self, peer: NodeId, log: LogPosition) {
        let now = self.now();
        self.shared.lock().unwrap().controller.on_ping(peer, log, now);
    }

    pub fn ping(&self) -> LogPosition {
        self.shared.lock().unwrap().controller.local_log()
    }

    pub fn observe_append(&self, vote: &VoteOf<Types<RT>>) {
        if !vote.is_committed() {
            return;
        }
        let leader = *vote.leader_id().node_id();
        let now = self.now();
        let mut shared = self.shared.lock().unwrap();
        if let Some((previous, at)) = shared.last_contact
            && previous == leader
            && now.saturating_sub(at) > shared.controller.election_base()
        {
            tracing::warn!(
                shard = self.shard,
                leader,
                gap_ms = now.saturating_sub(at).as_millis() as u64,
                "leader contact arrived late"
            );
        }
        shared.last_contact = Some((leader, now));
        shared.controller.set_quiesced(false);
        shared.controller.on_leader_contact(leader, now);
    }

    pub fn observe_pre_vote(&self, req: &VoteRequest<Types<RT>>) {
        let candidate = *req.vote.leader_id().node_id();
        let log = position(req.last_log_id.as_ref());
        let now = self.now();
        let mut shared = self.shared.lock().unwrap();
        if let Some(action) = shared.controller.on_pre_vote(candidate, log, now) {
            shared.pending = Some(action);
        }
    }

    pub fn observe_round_trip(&self, peer: NodeId, rtt: Duration) {
        self.shared.lock().unwrap().controller.on_round_trip(peer, rtt);
    }

    pub fn slowest_round_trip(&self) -> Option<(NodeId, Duration)> {
        self.shared.lock().unwrap().controller.slowest_round_trip()
    }

    pub fn observe_sync_latency(&self, latency: Duration) {
        self.shared.lock().unwrap().controller.on_sync_latency(latency);
    }

    pub fn set_quiesced(&self, quiesced: bool) {
        self.shared.lock().unwrap().controller.set_quiesced(quiesced);
    }

    pub fn may_hand_off(&self) -> bool {
        let now = self.now();
        self.shared.lock().unwrap().controller.may_hand_off(now)
    }

    pub async fn run<SM>(self, raft: Raft<Types<RT>, SM>)
    where
        SM: RaftStateMachine<Types<RT>>,
    {
        let me = *raft.node_id();
        let mut was_leader = false;
        let mut known: Vec<Member> = Vec::new();
        loop {
            let metrics = raft.metrics().borrow_watched().clone();
            if metrics.running_state.is_err() {
                return;
            }
            let now = self.now();
            let action = {
                let mut shared = self.shared.lock().unwrap();
                let voters: Vec<NodeId> = metrics.membership_config.membership().voter_ids().collect();
                let members: Vec<Member> = {
                    let dcs = self.datacenters.lock().unwrap();
                    voters.iter().map(|id| Member::new(*id, dcs.get(id).cloned().unwrap_or_default())).collect()
                };
                if members != known {
                    known = members.clone();
                    shared.controller.set_members(members);
                }
                let c = &mut shared.controller;
                c.on_local_log(position(metrics.log_id_list.last()));
                let leading = metrics.state == ServerState::Leader;
                if metrics.state == ServerState::Candidate {
                    c.on_campaigning(now);
                }
                if leading && !was_leader {
                    c.on_became_leader(now);
                }
                if !leading && was_leader {
                    c.on_stepped_down(now);
                }
                was_leader = leading;
                let pending = shared.pending.take();
                let polled = shared.controller.poll(now);
                if known.iter().any(|m| m.id == me) && !leading { pending.or(polled) } else { None }
            };
            if let Some(Action::Elect { pre_vote }) = action {
                tracing::info!(
                    node = me,
                    shard = self.shard,
                    term = metrics.current_term,
                    leader = ?metrics.current_leader,
                    pre_vote,
                    "ladder starts an election"
                );
                if raft.trigger().elect(pre_vote).await.is_err() {
                    return;
                }
            }
            RT::sleep(self.tick).await;
        }
    }
}
