use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::time::Duration;

pub type NodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, serde::Serialize, serde::Deserialize)]
pub struct LogPosition {
    pub term: u64,
    pub index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub id: NodeId,
    pub datacenter: String,
}

impl Member {
    pub fn new(id: NodeId, datacenter: impl Into<String>) -> Self {
        Self { id, datacenter: datacenter.into() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LadderConfig {
    pub base: Duration,
    pub step: Duration,
    pub fast_retry: Duration,
    pub campaign_timeout: Duration,
    pub max_campaign: Duration,
    pub flat_retry: Duration,
    pub liveness_window: Duration,
    pub handoff_suspend: Duration,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(150),
            step: Duration::from_millis(50),
            fast_retry: Duration::from_millis(20),
            campaign_timeout: Duration::from_millis(60),
            max_campaign: Duration::from_millis(300),
            flat_retry: Duration::from_millis(100),
            liveness_window: Duration::from_millis(1000),
            handoff_suspend: Duration::from_secs(30),
        }
    }
}

pub const TRANSFER_IDLE: Duration = Duration::from_secs(2);

pub fn transfer_ready(since_last_write: Duration, target_matched: Option<u64>, leader_last: Option<u64>) -> bool {
    since_last_write >= TRANSFER_IDLE && target_matched == leader_last
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Elect { pre_vote: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Following { quiet_since: Duration },
    Campaigning { started: Duration, until: Duration },
    Retrying { failed_at: Duration, delay: Duration },
    Leading,
}

#[derive(Debug, Clone, Default)]
struct PeerView {
    log: LogPosition,
    seen: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct Controller {
    config: LadderConfig,
    me: NodeId,
    members: Vec<Member>,
    peers: BTreeMap<NodeId, PeerView>,
    local_log: LogPosition,
    leader: Option<NodeId>,
    phase: Phase,
    failures: u32,
    last_election: Option<Duration>,
    quiesced: bool,
    rotation: Option<u64>,
    now: Duration,
    round_trips: BTreeMap<NodeId, Duration>,
    gaps: std::collections::VecDeque<Duration>,
    last_contact: Option<(NodeId, Duration)>,
    sync_latency: Duration,
}

const GAP_SAMPLES: usize = 16;
const BASE_STRETCH_CAP: u32 = 8;

fn mix(x: u64) -> u64 {
    let z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Controller {
    pub fn new(config: LadderConfig, me: NodeId, members: Vec<Member>) -> Self {
        Self {
            config,
            me,
            members,
            peers: BTreeMap::new(),
            local_log: LogPosition::default(),
            leader: None,
            phase: Phase::Following { quiet_since: Duration::ZERO },
            failures: 0,
            last_election: None,
            quiesced: false,
            rotation: None,
            now: Duration::ZERO,
            round_trips: BTreeMap::new(),
            gaps: std::collections::VecDeque::new(),
            last_contact: None,
            sync_latency: Duration::ZERO,
        }
    }

    pub fn with_rotation(mut self, rotation: u64) -> Self {
        self.rotation = Some(rotation);
        self
    }

    pub fn me(&self) -> NodeId {
        self.me
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn set_members(&mut self, members: Vec<Member>) {
        self.peers.retain(|id, _| members.iter().any(|m| m.id == *id));
        self.members = members;
    }

    pub fn set_quiesced(&mut self, quiesced: bool) {
        self.quiesced = quiesced;
    }

    pub fn on_local_log(&mut self, log: LogPosition) {
        self.local_log = log;
    }

    pub fn on_peer_log(&mut self, peer: NodeId, log: LogPosition) {
        let view = self.peers.entry(peer).or_default();
        view.log = view.log.max(log);
    }

    pub fn on_peer_seen(&mut self, peer: NodeId, now: Duration) {
        self.advance(now);
        let view = self.peers.entry(peer).or_default();
        view.seen = Some(view.seen.map_or(now, |s| s.max(now)));
    }

    pub fn on_ping(&mut self, peer: NodeId, log: LogPosition, now: Duration) {
        self.on_peer_log(peer, log);
        self.on_peer_seen(peer, now);
    }

    pub fn on_round_trip(&mut self, peer: NodeId, rtt: Duration) {
        let smoothed = match self.round_trips.get(&peer) {
            Some(old) => (*old * 7 + rtt * 3) / 10,
            None => rtt,
        };
        self.round_trips.insert(peer, smoothed);
    }

    pub fn round_trip(&self, peer: NodeId) -> Option<Duration> {
        self.round_trips.get(&peer).copied()
    }

    pub fn slowest_round_trip(&self) -> Option<(NodeId, Duration)> {
        self.round_trips.iter().map(|(id, rtt)| (*id, *rtt)).max_by_key(|(_, rtt)| *rtt)
    }

    pub fn quorum_round_trip(&self) -> Duration {
        let mut known: Vec<Duration> =
            self.members.iter().filter(|m| m.id != self.me).filter_map(|m| self.round_trip(m.id)).collect();
        known.sort();
        let need = self.members.len() / 2;
        need.checked_sub(1).and_then(|i| known.get(i)).copied().unwrap_or_default()
    }

    pub fn on_sync_latency(&mut self, latency: Duration) {
        self.sync_latency = latency;
    }

    pub fn consensus_round_trip(&self) -> Duration {
        self.quorum_round_trip() + self.sync_latency
    }

    fn step(&self) -> Duration {
        self.config.step.max(self.consensus_round_trip())
    }

    pub fn campaign_ends(&self) -> Option<Duration> {
        match self.phase {
            Phase::Campaigning { until, .. } => Some(until),
            _ => None,
        }
    }

    fn campaign_timeout(&self) -> Duration {
        self.config.campaign_timeout.max(self.consensus_round_trip() * 3)
    }

    fn max_campaign(&self) -> Duration {
        self.config.max_campaign.max(self.consensus_round_trip() * 6)
    }

    pub fn election_base(&self) -> Duration {
        self.base()
    }

    fn base(&self) -> Duration {
        let worst = self.gaps.iter().copied().max().unwrap_or_default();
        self.config.base.max(worst * 2).min(self.config.base * BASE_STRETCH_CAP)
    }

    pub fn local_log(&self) -> LogPosition {
        self.local_log
    }

    pub fn on_leader_contact(&mut self, leader: NodeId, now: Duration) {
        self.advance(now);
        if leader != self.me {
            if let Some((previous, at)) = self.last_contact
                && previous == leader
                && now > at
            {
                if self.gaps.len() == GAP_SAMPLES {
                    self.gaps.pop_front();
                }
                self.gaps.push_back(now - at);
            }
            self.last_contact = Some((leader, now));
        }
        if self.leader != Some(leader) {
            self.last_election = Some(now);
        }
        self.leader = Some(leader);
        self.failures = 0;
        self.phase = if leader == self.me { Phase::Leading } else { Phase::Following { quiet_since: now } };
        self.on_peer_seen(leader, now);
    }

    pub fn on_became_leader(&mut self, now: Duration) {
        self.on_leader_contact(self.me, now);
    }

    pub fn on_stepped_down(&mut self, now: Duration) {
        self.advance(now);
        if self.phase == Phase::Leading {
            self.leader = None;
            self.phase = Phase::Following { quiet_since: now };
        }
    }

    pub fn on_campaigning(&mut self, now: Duration) {
        self.advance(now);
        if let Phase::Campaigning { started, until } = self.phase {
            let cap = started + self.max_campaign();
            self.phase = Phase::Campaigning { started, until: until.max(now + self.campaign_timeout()).min(cap) };
        }
    }

    pub fn on_election_failed(&mut self, now: Duration) {
        self.advance(now);
        if self.phase == Phase::Leading {
            return;
        }
        self.schedule_retry(now);
    }

    pub fn on_pre_vote(&mut self, candidate: NodeId, log: LogPosition, now: Duration) -> Option<Action> {
        self.advance(now);
        self.on_peer_log(candidate, log);
        self.on_peer_seen(candidate, now);
        match self.phase {
            Phase::Leading | Phase::Campaigning { .. } => None,
            _ if self.local_log > log && self.leader_quiet(now) => {
                self.begin_campaign(now);
                Some(Action::Elect { pre_vote: true })
            }
            _ if self.local_log > log => None,
            Phase::Following { quiet_since } => {
                self.phase = Phase::Following { quiet_since: quiet_since.max(now) };
                None
            }
            Phase::Retrying { .. } => {
                self.phase = Phase::Following { quiet_since: now };
                None
            }
        }
    }

    pub fn may_hand_off(&self, now: Duration) -> bool {
        self.last_election.is_none_or(|at| now >= at + self.config.handoff_suspend)
    }

    pub fn rank(&self) -> usize {
        let leader_dc = self.leader.and_then(|l| self.datacenter(l));
        let rotation = self.rotation;
        let key = |id: NodeId, log: LogPosition| {
            let local = leader_dc.is_some() && self.datacenter(id) == leader_dc;
            let order = rotation.map_or(id, |r| mix(id ^ mix(r)));
            (Reverse(log), !local, order, id)
        };
        let mine = key(self.me, self.local_log);
        self.members
            .iter()
            .filter(|m| m.id != self.me && Some(m.id) != self.leader && self.is_live(m.id))
            .filter(|m| key(m.id, self.peer_log(m.id)) < mine)
            .count()
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        let spacing = self.step() * self.rank() as u32;
        match self.phase {
            Phase::Leading => None,
            Phase::Following { quiet_since } => {
                let mut since = quiet_since;
                let mut grace = Duration::ZERO;
                if self.quiesced {
                    if let Some(seen) = self.leader.and_then(|l| self.peers.get(&l)).and_then(|v| v.seen) {
                        since = since.max(seen);
                    }
                    grace = self.config.liveness_window;
                }
                Some(since + grace + self.base() + spacing)
            }
            Phase::Campaigning { until, .. } => Some(until),
            Phase::Retrying { failed_at, delay } => Some(failed_at + delay + spacing),
        }
    }

    pub fn poll(&mut self, now: Duration) -> Option<Action> {
        self.advance(now);
        let deadline = self.next_deadline()?;
        if now < deadline {
            return None;
        }
        match self.phase {
            Phase::Campaigning { .. } => {
                self.schedule_retry(now);
                None
            }
            _ => {
                self.begin_campaign(now);
                Some(Action::Elect { pre_vote: true })
            }
        }
    }

    fn begin_campaign(&mut self, now: Duration) {
        self.phase = Phase::Campaigning { started: now, until: now + self.campaign_timeout() };
    }

    fn schedule_retry(&mut self, now: Duration) {
        let rtt = self.consensus_round_trip();
        let delay =
            if self.failures == 0 { self.config.fast_retry.max(rtt / 2) } else { self.config.flat_retry.max(rtt * 2) };
        self.failures = self.failures.saturating_add(1);
        self.phase = Phase::Retrying { failed_at: now, delay };
    }

    fn leader_quiet(&self, now: Duration) -> bool {
        match self.phase {
            Phase::Following { quiet_since } => now >= quiet_since + self.base(),
            _ => true,
        }
    }

    fn advance(&mut self, now: Duration) {
        self.now = self.now.max(now);
    }

    fn is_live(&self, id: NodeId) -> bool {
        self.peers
            .get(&id)
            .and_then(|v| v.seen)
            .is_some_and(|seen| self.now.saturating_sub(seen) <= self.config.liveness_window)
    }

    fn peer_log(&self, id: NodeId) -> LogPosition {
        self.peers.get(&id).map(|v| v.log).unwrap_or_default()
    }

    fn datacenter(&self, id: NodeId) -> Option<&str> {
        self.members.iter().find(|m| m.id == id).map(|m| m.datacenter.as_str())
    }
}
