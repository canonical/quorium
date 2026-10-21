use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::alias::{SnapshotOf, VoteOf};
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::impls::BasicNode;
use openraft::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest, TransferLeaderResponse,
    VoteRequest, VoteResponse,
};
use openraft::{AsyncRuntime, Config, Raft, ReadPolicy, ServerState};
use quorium_core::driver::Ladder;
use quorium_core::kv::{Command, KvOp, Reply};
use quorium_core::ladder::LadderConfig;
use quorium_core::types::{NodeId, Types};
use quorium_store::mem::{KvStateMachine, MemLog, MemSnapshotStore};

use crate::checkers::Violation;
use crate::checkers::durability::DurabilityChecker;
use crate::checkers::election::ElectionSafety;
use crate::checkers::escrow::EscrowChecker;
use crate::checkers::linearizability::{History, Register, RegisterOp, RegisterRet, check_key};
use crate::checkers::singleton::SingletonChecker;
use crate::checkers::state_machine::StateMachineSafety;
use crate::disk::{DiskFaults, SimDisk};
use crate::faults::FaultKind;
use crate::network::{LinkConfig, SimNetwork};
use crate::rng::SimRng;
use crate::runtime::SimRuntime;
use crate::sim_disk::SimStorage;
use quorium_core::kv::{EscrowAction, EscrowOp};
use quorium_store::wal_log::SharedWal;

pub type C = Types<SimRuntime>;
pub type SimRaft = Raft<C, KvStateMachine>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bug {
    NondeterministicApply,
    ForgetVote,
    LostFsync,
    LeaderRestore,
    OversellEscrow,
    StaleRead,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub seed: u64,
    pub nodes: Vec<(NodeId, String)>,
    pub link: LinkConfig,
    pub fsync: Duration,
    pub ladder: LadderConfig,
    pub tick: Duration,
    pub heartbeat: Duration,
    pub ping: Duration,
    pub ladder_enabled: bool,
    pub fsync_overrides: Vec<(NodeId, Duration)>,
    pub inter_dc: Option<LinkConfig>,
    pub store: StoreKind,
    pub snapshot_every: Option<u64>,
    pub compaction_threshold: Option<u64>,
    pub bug: Option<Bug>,
    pub lease_reads: bool,
    pub max_drift_ppm: u64,
}

impl ClusterConfig {
    pub fn new(seed: u64, nodes: &[(NodeId, &str)]) -> Self {
        Self {
            seed,
            nodes: nodes.iter().map(|(id, dc)| (*id, dc.to_string())).collect(),
            link: LinkConfig::default(),
            fsync: Duration::from_millis(2),
            ladder: LadderConfig::default(),
            tick: Duration::from_millis(5),
            heartbeat: Duration::from_millis(50),
            ping: Duration::from_millis(100),
            ladder_enabled: true,
            fsync_overrides: Vec::new(),
            inter_dc: None,
            store: StoreKind::Wal,
            snapshot_every: None,
            compaction_threshold: None,
            bug: None,
            lease_reads: true,
            max_drift_ppm: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    NoLeader,
    Rejected(String),
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    pub term: u64,
    pub leader: bool,
    pub candidate: bool,
    pub voted: NodeId,
    pub vote_committed: bool,
    pub log: Vec<u64>,
    pub applied: Option<u64>,
}

struct NodeEntry {
    raft: SimRaft,
    ladder: Ladder<SimRuntime>,
}

#[derive(Default)]
struct Checks {
    election: ElectionSafety,
    state_machine: StateMachineSafety,
    histories: BTreeMap<String, History<Register>>,
    acked: Vec<(u64, u64)>,
    leases: Vec<(u64, u64)>,
    escrow: EscrowChecker,
    local_escrow: BTreeMap<NodeId, LocalEscrow>,
    escrow_sales: u64,
    extra: Vec<Violation>,
}

#[derive(Default)]
struct LocalEscrow {
    remaining: u64,
    unsettled: u64,
    pending: Option<(Command, u64, u64)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RpcCounts {
    pub append: u64,
    pub vote: u64,
    pub pre_vote: u64,
}

struct RouterInner {
    counts: RpcCounts,
    net: SimNetwork,
    rng: SimRng,
    nodes: BTreeMap<NodeId, NodeEntry>,
    paused: BTreeMap<NodeId, Duration>,
}

#[derive(Clone)]
struct Router {
    inner: Arc<Mutex<RouterInner>>,
}

fn unreachable(msg: &str) -> Unreachable<C> {
    Unreachable::new(&io::Error::other(msg.to_string()))
}

const DROP_PENALTY: Duration = Duration::from_millis(50);

impl Router {
    fn count(&self, f: impl FnOnce(&mut RpcCounts)) {
        f(&mut self.inner.lock().unwrap().counts);
    }

    fn ping(&self, from: NodeId, peers: &[NodeId], log: quorium_core::ladder::LogPosition) {
        for &to in peers {
            let Some(delay) = self.hop(from, to) else { continue };
            let router = self.clone();
            drop(SimRuntime::spawn(async move {
                SimRuntime::sleep(delay).await;
                if let Some((_, ladder)) = router.target(to) {
                    ladder.observe_ping(from, log);
                }
            }));
        }
    }

    fn hop(&self, from: NodeId, to: NodeId) -> Option<Duration> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.nodes.contains_key(&to) || !inner.nodes.contains_key(&from) {
            return None;
        }
        let now = now_micros_duration();
        if [from, to].iter().any(|n| inner.paused.get(n).is_some_and(|until| now < *until)) {
            return None;
        }
        let RouterInner { net, rng, .. } = &mut *inner;
        net.send(from, to, rng).into_iter().min()
    }

    fn target(&self, to: NodeId) -> Option<(SimRaft, Ladder<SimRuntime>)> {
        let inner = self.inner.lock().unwrap();
        inner.nodes.get(&to).map(|n| (n.raft.clone(), n.ladder.clone()))
    }

    async fn call<T, F, Fut>(&self, from: NodeId, to: NodeId, f: F) -> Result<T, Unreachable<C>>
    where
        F: FnOnce(SimRaft, Ladder<SimRuntime>) -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        let Some(delay) = self.hop(from, to) else {
            SimRuntime::sleep(DROP_PENALTY).await;
            return Err(unreachable("request dropped"));
        };
        SimRuntime::sleep(delay).await;
        let Some((raft, ladder)) = self.target(to) else {
            return Err(unreachable("target down"));
        };
        ladder.observe_peer(from);
        let Some(resp) = f(raft, ladder).await else {
            return Err(unreachable("target failed"));
        };
        let Some(back) = self.hop(to, from) else {
            SimRuntime::sleep(DROP_PENALTY).await;
            return Err(unreachable("response dropped"));
        };
        SimRuntime::sleep(back).await;
        if self.target(from).is_none() {
            return Err(unreachable("caller down"));
        }
        Ok(resp)
    }
}

struct NetFactory {
    from: NodeId,
    router: Router,
}

struct Conn {
    from: NodeId,
    to: NodeId,
    router: Router,
}

impl RaftNetworkFactory<C> for NetFactory {
    type Network = Conn;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Conn {
        Conn { from: self.from, to: target, router: self.router.clone() }
    }
}

impl RaftNetworkV2<C> for Conn {
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        self.router.count(|c| c.append += 1);
        self.router
            .call(self.from, self.to, |raft, ladder| async move {
                ladder.observe_append(&rpc.vote);
                raft.append_entries(rpc).await.ok()
            })
            .await
            .map_err(RPCError::Unreachable)
    }

    async fn vote(&mut self, rpc: VoteRequest<C>, _option: RPCOption) -> Result<VoteResponse<C>, RPCError<C>> {
        self.router.count(|c| c.vote += 1);
        self.router
            .call(self.from, self.to, |raft, ladder| async move {
                ladder.observe_pre_vote(&rpc);
                raft.vote(rpc).await.ok()
            })
            .await
            .map_err(RPCError::Unreachable)
    }

    async fn pre_vote(&mut self, rpc: VoteRequest<C>, _option: RPCOption) -> Result<VoteResponse<C>, RPCError<C>> {
        self.router.count(|c| c.pre_vote += 1);
        self.router
            .call(self.from, self.to, |raft, ladder| async move {
                ladder.observe_pre_vote(&rpc);
                raft.pre_vote(rpc).await.ok()
            })
            .await
            .map_err(RPCError::Unreachable)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Vec<u8>>,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        self.router
            .call(self.from, self.to, |raft, _| async move { raft.install_full_snapshot(vote, snapshot).await.ok() })
            .await
            .map_err(StreamingError::Unreachable)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<C>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<C>, RPCError<C>> {
        self.router
            .call(self.from, self.to, |raft, _| async move { raft.handle_transfer_leader(req).await.ok() })
            .await
            .map_err(RPCError::Unreachable)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    Mem,
    Wal,
}

enum NodeStore {
    Mem(MemLog<C>),
    Wal(SharedWal<SimRuntime, SimStorage>),
    Lost,
}

pub struct SimCluster {
    rt: SimRuntime,
    config: ClusterConfig,
    router: Router,
    logs: BTreeMap<NodeId, NodeStore>,
    checks: Arc<Mutex<Checks>>,
    raft_config: Arc<Config>,
    next_client: u64,
    quiesced: bool,
    snapshots: BTreeMap<NodeId, MemSnapshotStore>,
}

fn encode(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn decode(v: &[u8]) -> Option<u64> {
    v.try_into().ok().map(u64::from_be_bytes)
}

impl SimCluster {
    pub fn new(config: ClusterConfig) -> Self {
        let mut rt = SimRuntime::with_seed(config.seed);
        let mut rng = SimRng::new(config.seed);
        let router = Router {
            inner: Arc::new(Mutex::new(RouterInner {
                counts: RpcCounts::default(),
                net: SimNetwork::new(config.link.clone()),
                rng: rng.fork(),
                nodes: BTreeMap::new(),
                paused: BTreeMap::new(),
            })),
        };
        let mut raft_config = quorium_core::config::raft_config(config.heartbeat, &config.ladder);
        if let Some(n) = config.snapshot_every {
            raft_config.snapshot_policy = openraft::SnapshotPolicy::LogsSinceLast(n);
            raft_config.max_in_snapshot_log_to_keep = n / 2;
        }
        if config.bug == Some(Bug::LeaderRestore) {
            raft_config.enable_leader_restore = Some(true);
        }
        let raft_config = Arc::new(raft_config);
        let lose = config.bug == Some(Bug::LostFsync);
        let store = if config.bug == Some(Bug::ForgetVote) { StoreKind::Mem } else { config.store };
        let logs = config
            .nodes
            .iter()
            .map(|(id, _)| {
                let fsync = config.fsync_overrides.iter().find(|(n, _)| n == id).map_or(config.fsync, |(_, d)| *d);
                let node_store = match store {
                    StoreKind::Mem => NodeStore::Mem(MemLog::with_flush_delay(fsync).with_lost_fsync(lose)),
                    StoreKind::Wal => {
                        let faults = DiskFaults {
                            torn_write: 0.5,
                            lost_fsync: if lose { 1.0 } else { 0.0 },
                            ..DiskFaults::none()
                        };
                        let disk = SimStorage::new(SimDisk::new(faults), rng.fork());
                        let wal = SharedWal::open(disk).expect("empty simulated disk opens");
                        wal.set_flush_delay(fsync);
                        if let Some(t) = config.compaction_threshold {
                            wal.set_compaction_threshold(t);
                        }
                        NodeStore::Wal(wal)
                    }
                };
                (*id, node_store)
            })
            .collect();
        let mut cluster = Self {
            rt: SimRuntime::with_seed(0),
            config,
            router,
            logs,
            checks: Arc::new(Mutex::new(Checks::default())),
            raft_config,
            next_client: 1,
            quiesced: false,
            snapshots: BTreeMap::new(),
        };
        std::mem::swap(&mut cluster.rt, &mut rt);
        if let Some(inter) = cluster.config.inter_dc.clone() {
            let nodes = cluster.config.nodes.clone();
            let mut inner = cluster.router.inner.lock().unwrap();
            for (a, da) in &nodes {
                for (b, db) in &nodes {
                    if da != db {
                        inner.net.set_link(*a, *b, inter.clone());
                    }
                }
            }
        }
        let ids: Vec<NodeId> = cluster.config.nodes.iter().map(|(id, _)| *id).collect();
        for id in &ids {
            cluster.start_node(*id);
        }
        let members: BTreeMap<NodeId, BasicNode> = ids.iter().map(|id| (*id, BasicNode::new(id.to_string()))).collect();
        let first = cluster.raft(ids[0]).expect("first node is up");
        cluster.rt.block_on(async move { first.initialize(members).await.expect("cluster initializes") });
        cluster
    }

    fn datacenters(&self) -> BTreeMap<NodeId, String> {
        self.config.nodes.iter().cloned().collect()
    }

    pub fn datacenter(&self, id: NodeId) -> String {
        self.datacenters().get(&id).cloned().unwrap_or_default()
    }

    pub fn drift_ppm(&self, id: NodeId) -> i64 {
        let max = self.config.max_drift_ppm as i64;
        if max == 0 {
            return 0;
        }
        let mut rng = SimRng::new(self.config.seed ^ id.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        rng.range(0, 2 * max as u64 + 1) as i64 - max
    }

    fn start_node(&mut self, id: NodeId) {
        let previous = self.rt.set_clock_rate(self.drift_ppm(id));
        self.start_node_on_its_clock(id);
        self.rt.set_clock_rate(previous);
    }

    fn start_node_on_its_clock(&mut self, id: NodeId) {
        let checks = self.checks.clone();
        let bug = self.config.bug;
        let counter = Arc::new(Mutex::new(0u64));
        let store = self.snapshots.entry(id).or_default().clone();
        let sm = match KvStateMachine::with_store(Arc::new(store)) {
            Ok(sm) => sm,
            Err(e) => {
                self.record_violation(Violation::NodeFailure { node: id, message: e.to_string() });
                return;
            }
        };
        sm.set_observer(Arc::new(move |index, hash| {
            let hash = if bug == Some(Bug::NondeterministicApply) && id == 1 {
                let mut c = counter.lock().unwrap();
                *c += 1;
                hash ^ *c
            } else {
                hash
            };
            checks.lock().unwrap().state_machine.observe_apply(id, index, hash);
        }));
        let factory = NetFactory { from: id, router: self.router.clone() };
        let config = self.raft_config.clone();
        let ladder = Ladder::<SimRuntime>::new(self.config.ladder.clone(), id, self.datacenters(), self.config.tick);
        let started = match &self.logs[&id] {
            NodeStore::Mem(log) => {
                let log = log.clone();
                self.rt.block_on(async move { Raft::new(id, config, factory, log, sm).await })
            }
            NodeStore::Wal(wal) => {
                let log = wal.shard(0);
                self.rt.block_on(async move { Raft::new(id, config, factory, log, sm).await })
            }
            NodeStore::Lost => return,
        };
        let raft = match started {
            Ok(raft) => raft,
            Err(e) => {
                self.record_violation(Violation::NodeFailure { node: id, message: e.to_string() });
                return;
            }
        };
        self.router.inner.lock().unwrap().nodes.insert(id, NodeEntry { raft: raft.clone(), ladder: ladder.clone() });
        if self.config.ladder_enabled {
            self.rt.spawn_detached(ladder.clone().run(raft.clone()));
        }
        if self.quiesced {
            raft.runtime_config().heartbeat(false);
            ladder.set_quiesced(true);
        }
        let router = self.router.clone();
        let peers: Vec<NodeId> = self.node_ids().into_iter().filter(|p| *p != id).collect();
        let interval = self.config.ping;
        let own = raft.clone();
        self.rt.spawn_detached(async move {
            loop {
                SimRuntime::sleep(interval).await;
                if own.metrics().borrow_watched().running_state.is_err() {
                    return;
                }
                router.ping(id, &peers, ladder.ping());
            }
        });
        let checks = self.checks.clone();
        let mut metrics = raft.metrics();
        self.rt.spawn_detached(async move {
            loop {
                let (state, term) = {
                    let m = metrics.borrow_watched();
                    (m.state, m.current_term)
                };
                if state == ServerState::Leader {
                    checks.lock().unwrap().election.observe_leader(term, id);
                }
                if metrics.changed().await.is_err() {
                    return;
                }
            }
        });
    }

    pub fn node_view(&self, id: NodeId) -> Option<NodeView> {
        let raft = self.raft(id)?;
        let m = raft.metrics().borrow_watched().clone();
        let Some(NodeStore::Mem(store)) = self.logs.get(&id) else { return None };
        let (term, voted, vote_committed) = store
            .stored_vote()
            .map_or((0, 0, false), |v| (v.leader_id().term, v.leader_id().voted_for, v.is_committed()));
        let settled = term == m.current_term;
        Some(NodeView {
            term,
            leader: settled && m.state == ServerState::Leader,
            candidate: settled && m.state == ServerState::Candidate,
            voted,
            vote_committed,
            log: store.log_ids().iter().map(|l| l.leader_id.term).collect(),
            applied: m.last_applied.map(|l| l.index),
        })
    }

    pub fn now(&self) -> Duration {
        self.rt.now()
    }

    pub fn run_for(&mut self, d: Duration) {
        self.rt.run_for(d);
    }

    fn raft(&self, id: NodeId) -> Option<SimRaft> {
        self.router.target(id).map(|(r, _)| r)
    }

    pub fn is_up(&self, id: NodeId) -> bool {
        self.raft(id).is_some()
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.config.nodes.iter().map(|(id, _)| *id).collect()
    }

    pub fn leaders(&self) -> Vec<(NodeId, u64)> {
        let mut out = Vec::new();
        for id in self.node_ids() {
            if let Some(raft) = self.raft(id) {
                let m = raft.metrics().borrow_watched().clone();
                if m.state == ServerState::Leader {
                    out.push((id, m.current_term));
                }
            }
        }
        out
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leaders().into_iter().max_by_key(|(_, t)| *t).map(|(id, _)| id)
    }

    pub fn run_until_leader_other_than(&mut self, old: NodeId, limit: Duration) -> Option<NodeId> {
        let deadline = self.now() + limit;
        let step = Duration::from_millis(1);
        while self.now() < deadline {
            self.run_for(step);
            if let Some(l) = self.leader().filter(|l| *l != old) {
                return Some(l);
            }
        }
        None
    }

    pub fn crash(&mut self, id: NodeId) {
        let Some(entry) = self.router.inner.lock().unwrap().nodes.remove(&id) else {
            return;
        };
        self.checks.lock().unwrap().local_escrow.remove(&id);
        let raft = entry.raft;
        self.rt.spawn_detached(async move {
            let _ = raft.shutdown().await;
        });
        let recovered = match self.logs.remove(&id) {
            Some(NodeStore::Mem(log)) => {
                let log = log.crash();
                NodeStore::Mem(if self.config.bug == Some(Bug::ForgetVote) { log.forget_vote() } else { log })
            }
            Some(NodeStore::Wal(wal)) => {
                let delay = wal.flush_delay();
                match wal.close().map(SimStorage::crash).map(SharedWal::open) {
                    Some(Ok(wal)) => {
                        wal.set_flush_delay(delay);
                        if let Some(t) = self.config.compaction_threshold {
                            wal.set_compaction_threshold(t);
                        }
                        NodeStore::Wal(wal)
                    }
                    Some(Err(e)) => {
                        self.record_violation(Violation::NodeFailure { node: id, message: e.to_string() });
                        NodeStore::Lost
                    }
                    None => NodeStore::Lost,
                }
            }
            _ => NodeStore::Lost,
        };
        self.logs.insert(id, recovered);
    }

    pub fn restart(&mut self, id: NodeId) {
        if self.is_up(id) {
            return;
        }
        self.start_node(id);
    }

    pub fn partition(&mut self, left: &[NodeId], right: &[NodeId]) {
        self.router.inner.lock().unwrap().net.partition(left, right);
    }

    pub fn isolate(&mut self, id: NodeId) {
        let all = self.node_ids();
        self.router.inner.lock().unwrap().net.isolate(id, &all);
    }

    pub fn heal(&mut self) {
        let mut inner = self.router.inner.lock().unwrap();
        inner.net.heal();
        inner.paused.clear();
    }

    pub fn describe_nodes(&self) -> Vec<String> {
        self.node_ids()
            .into_iter()
            .filter_map(|id| {
                self.router.target(id).map(|(r, l)| {
                    let m = r.metrics().borrow_watched().clone();
                    format!(
                        "n{id} {:?} vote={} last={:?} ladder={}",
                        m.state,
                        m.vote,
                        m.log_id_list.last(),
                        l.describe()
                    )
                })
            })
            .collect()
    }

    pub fn snapshots_built(&self) -> u64 {
        self.snapshots.values().map(MemSnapshotStore::saves).sum()
    }

    pub fn rpc_counts(&self) -> RpcCounts {
        self.router.inner.lock().unwrap().counts
    }

    pub fn term(&self) -> u64 {
        self.node_ids()
            .into_iter()
            .filter_map(|id| self.raft(id))
            .map(|r| r.metrics().borrow_watched().current_term)
            .max()
            .unwrap_or(0)
    }

    pub fn transfer_leader(&mut self, to: NodeId) {
        if let Some(raft) = self.leader().and_then(|l| self.raft(l)) {
            self.rt.block_on(async move {
                let _ = raft.trigger().transfer_leader(to).await;
            });
        }
    }

    pub fn quiesce(&mut self, on: bool) {
        self.quiesced = on;
        let entries: Vec<(SimRaft, Ladder<SimRuntime>)> =
            self.node_ids().into_iter().filter_map(|id| self.router.target(id)).collect();
        for (raft, ladder) in entries {
            raft.runtime_config().heartbeat(!on);
            ladder.set_quiesced(on);
        }
    }

    pub fn pause(&mut self, id: NodeId, duration: Duration) {
        let until = self.now() + duration;
        self.router.inner.lock().unwrap().paused.insert(id, until);
    }

    pub fn apply_fault(&mut self, kind: &FaultKind) {
        let mut link = self.router.inner.lock().unwrap().net.config().clone();
        match kind {
            FaultKind::Crash(n) => self.crash(*n),
            FaultKind::Restart(n) => self.restart(*n),
            FaultKind::Pause { node, duration } => self.pause(*node, *duration),
            FaultKind::Partition { left, right } => self.partition(left, right),
            FaultKind::Isolate(n) => self.isolate(*n),
            FaultKind::Cut { from, to } => self.router.inner.lock().unwrap().net.cut(*from, *to),
            FaultKind::Spike { from, to, extra } => self.router.inner.lock().unwrap().net.spike(*from, *to, *extra),
            FaultKind::Loss { permille } => {
                link.loss = f64::from(*permille) / 1000.0;
                self.set_link(link);
            }
            FaultKind::Duplicate { permille } => {
                link.duplicate = f64::from(*permille) / 1000.0;
                self.set_link(link);
            }
            FaultKind::SlowDisk { node, extra } => {
                if let Some(NodeStore::Wal(wal)) = self.logs.get(node) {
                    wal.set_flush_delay(self.config.fsync + *extra);
                }
            }
            FaultKind::ClockSkew { .. } => {}
            FaultKind::Heal => self.heal(),
        }
    }

    pub fn spawn_clients(&mut self, count: usize, keys: u64, interval: Duration, stop_at: Duration, seed: u64) {
        let ids = self.node_ids();
        let mut rng = SimRng::new(seed);
        for i in 0..count {
            let client = self.client_id();
            let home = ids[i % ids.len()];
            let router = self.router.clone();
            let checks = self.checks.clone();
            let rng = rng.fork();
            let mode = (self.config.bug, self.config.lease_reads);
            self.rt.spawn_detached(client_loop(router, checks, client, home, keys, interval, stop_at, rng, mode));
        }
    }

    pub fn refill_escrow(&mut self, units: u64) -> bool {
        let client = self.client_id();
        let deadline = self.now() + Duration::from_secs(5);
        while self.now() < deadline {
            if let Some(raft) = self.leader().and_then(|l| self.raft(l)) {
                let op = EscrowOp { name: ESCROW.into(), node: 0, action: EscrowAction::Refill { units } };
                let cmd = Command::new(client, 1, KvOp::Escrow(op));
                let handle = self.rt.spawn_in(async move { raft.client_write(cmd).await.is_ok() });
                if self.drive(handle) {
                    self.checks.lock().unwrap().escrow.refill(units);
                    return true;
                }
            }
            self.run_for(Duration::from_millis(10));
        }
        false
    }

    pub fn escrow_sales(&self) -> u64 {
        self.checks.lock().unwrap().escrow_sales
    }

    pub fn lease_grants(&self) -> usize {
        self.checks.lock().unwrap().leases.len()
    }

    pub fn acked_writes(&self) -> usize {
        self.checks.lock().unwrap().acked.len()
    }

    pub fn check_durability(&mut self) -> Vec<Violation> {
        let Some(leader) = self.leader() else {
            return Vec::new();
        };
        let Some(raft) = self.raft(leader) else {
            return Vec::new();
        };
        let acked = self.checks.lock().unwrap().acked.clone();
        let sessions = self.rt.block_on(async move {
            raft.with_state_machine(move |sm: &mut KvStateMachine| {
                let seen: Vec<bool> = sm.read(|s| acked.iter().map(|(c, q)| s.applied(*c, *q)).collect());
                Box::pin(async move { seen })
            })
            .await
            .unwrap_or_default()
        });
        let acked = self.checks.lock().unwrap().acked.clone();
        let mut checker = DurabilityChecker::default();
        let mut present = Vec::new();
        for (i, (client, seq)) in acked.iter().enumerate() {
            let id = (client << 32) | seq;
            checker.acknowledged(id);
            if sessions.get(i).copied().unwrap_or(false) {
                present.push(id);
            }
        }
        checker.check(present)
    }

    pub fn write_within(&mut self, key: &str, value: u64, limit: Duration) -> Result<Reply, ClientError> {
        let client = self.client_id();
        let deadline = self.now() + limit;
        loop {
            let result = match self.leader() {
                Some(leader) => self.attempt_write(leader, client, key, value),
                None => Err(ClientError::NoLeader),
            };
            if result.is_ok() || self.now() >= deadline {
                return result;
            }
            self.run_for(Duration::from_millis(2));
        }
    }

    pub fn submit(&mut self, client: u64, seq: u64, op: KvOp, limit: Duration) -> Result<Reply, ClientError> {
        let cmd = Command::new(client, seq, op);
        let deadline = self.now() + limit;
        loop {
            let result = match self.leader().and_then(|l| self.raft(l)) {
                Some(raft) => {
                    let cmd = cmd.clone();
                    let handle = self.rt.spawn_in(async move {
                        match SimRuntime::timeout(Duration::from_secs(2), raft.client_write(cmd)).await {
                            Ok(Ok(resp)) => Ok(resp.data),
                            Ok(Err(e)) => Err(ClientError::Rejected(e.to_string())),
                            Err(_) => Err(ClientError::Timeout),
                        }
                    });
                    self.drive(handle)
                }
                None => Err(ClientError::NoLeader),
            };
            if result.is_ok() || self.now() >= deadline {
                return result;
            }
            self.run_for(Duration::from_millis(2));
        }
    }

    pub fn read_state<T: Send + 'static>(
        &mut self,
        f: impl Fn(&quorium_core::kv::KvState) -> T + Send + Sync + 'static,
        limit: Duration,
    ) -> Result<T, ClientError> {
        let f = Arc::new(f);
        let deadline = self.now() + limit;
        loop {
            let result = match self.leader().and_then(|l| self.raft(l)) {
                Some(raft) => {
                    let f = f.clone();
                    let handle = self.rt.spawn_in(async move {
                        match SimRuntime::timeout(
                            Duration::from_secs(2),
                            raft.ensure_linearizable(ReadPolicy::ReadIndex),
                        )
                        .await
                        {
                            Ok(Ok(_)) => raft
                                .with_state_machine(move |sm: &mut KvStateMachine| {
                                    let v = sm.read(|s| f(s));
                                    Box::pin(async move { v })
                                })
                                .await
                                .map_err(|e| ClientError::Rejected(e.to_string())),
                            Ok(Err(e)) => Err(ClientError::Rejected(e.to_string())),
                            Err(_) => Err(ClientError::Timeout),
                        }
                    });
                    self.drive(handle)
                }
                None => Err(ClientError::NoLeader),
            };
            if result.is_ok() || self.now() >= deadline {
                return result;
            }
            self.run_for(Duration::from_millis(2));
        }
    }

    pub fn set_link(&mut self, link: LinkConfig) {
        self.router.inner.lock().unwrap().net.set_config(link);
    }

    fn client_id(&mut self) -> u64 {
        let c = self.next_client;
        self.next_client += 1;
        c
    }

    pub fn write(&mut self, key: &str, value: u64) -> Result<Reply, ClientError> {
        self.write_within(key, value, Duration::from_secs(5))
    }

    pub fn write_via(&mut self, node: NodeId, key: &str, value: u64) -> Result<Reply, ClientError> {
        let client = self.client_id();
        self.attempt_write(node, client, key, value)
    }

    fn attempt_write(&mut self, node: NodeId, client: u64, key: &str, value: u64) -> Result<Reply, ClientError> {
        let raft = self.raft(node).ok_or(ClientError::NoLeader)?;
        let cmd = Command::new(client, 1, KvOp::Put { key: key.into(), value: encode(value) });
        let checks = self.checks.clone();
        let key = key.to_string();
        let handle = self.rt.spawn_in(async move {
            let call = now_micros();
            let res = SimRuntime::timeout(Duration::from_secs(2), raft.client_write(cmd)).await;
            let ret = now_micros();
            let (outcome, record) = match res {
                Ok(Ok(resp)) => (Ok(resp.data), Some(Some((ret, RegisterRet::Ok)))),
                Ok(Err(e)) if e.forward_to_leader().is_some() => (Err(ClientError::Rejected(e.to_string())), None),
                Ok(Err(e)) => (Err(ClientError::Rejected(e.to_string())), Some(None)),
                Err(_) => (Err(ClientError::Timeout), Some(None)),
            };
            if let Some(completed) = record {
                checks.lock().unwrap().histories.entry(key).or_default().record(
                    client,
                    RegisterOp::Write(value),
                    call,
                    completed,
                );
            }
            outcome
        });
        self.drive(handle)
    }

    pub fn read(&mut self, key: &str) -> Result<Option<u64>, ClientError> {
        let leader = self.leader().ok_or(ClientError::NoLeader)?;
        let raft = self.raft(leader).ok_or(ClientError::NoLeader)?;
        let client = self.client_id();
        let checks = self.checks.clone();
        let key = key.to_string();
        let handle = self.rt.spawn_in(async move {
            let call = now_micros();
            let res =
                SimRuntime::timeout(Duration::from_secs(2), raft.ensure_linearizable(ReadPolicy::ReadIndex)).await;
            match res {
                Ok(Ok(_)) => {
                    let lookup = key.clone();
                    let value = raft
                        .with_state_machine(move |sm: &mut KvStateMachine| {
                            let v = sm.read(|s| s.get(&lookup).and_then(|v| decode(v)));
                            Box::pin(async move { v })
                        })
                        .await
                        .map_err(|e| ClientError::Rejected(e.to_string()))?;
                    let ret = now_micros();
                    checks.lock().unwrap().histories.entry(key).or_default().record(
                        client,
                        RegisterOp::Read,
                        call,
                        Some((ret, RegisterRet::Value(value))),
                    );
                    Ok(value)
                }
                Ok(Err(e)) => Err(ClientError::Rejected(e.to_string())),
                Err(_) => Err(ClientError::Timeout),
            }
        });
        self.drive(handle)
    }

    fn drive<T: Send + 'static>(&mut self, handle: crate::runtime::JoinHandle<T>) -> T {
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let out = slot.clone();
        self.rt.spawn_detached(async move {
            if let Ok(v) = handle.await {
                *out.lock().unwrap() = Some(v);
            }
        });
        loop {
            if let Some(v) = slot.lock().unwrap().take() {
                return v;
            }
            self.rt.run_for(Duration::from_millis(1));
        }
    }

    pub fn applied_indexes(&self) -> Vec<Option<u64>> {
        self.node_ids()
            .into_iter()
            .filter_map(|id| self.raft(id))
            .map(|r| r.metrics().borrow_watched().last_applied.map(|l| l.index))
            .collect()
    }

    pub fn state_hashes(&mut self) -> Vec<u64> {
        let mut out = Vec::new();
        for id in self.node_ids() {
            if let Some(raft) = self.raft(id) {
                let h = self.rt.block_on(async move {
                    raft.with_state_machine(|sm: &mut KvStateMachine| {
                        let h = sm.state_hash();
                        Box::pin(async move { h })
                    })
                    .await
                    .ok()
                });
                out.extend(h);
            }
        }
        out
    }

    pub fn violations(&self) -> Vec<Violation> {
        let checks = self.checks.lock().unwrap();
        let mut out: Vec<Violation> = checks.election.violations().to_vec();
        out.extend(checks.state_machine.violations().iter().cloned());
        for (key, history) in &checks.histories {
            if let Err(v) = check_key(key, history) {
                out.push(v);
            }
        }
        let mut grants = checks.leases.clone();
        grants.sort_by_key(|(holder, token)| (*token, *holder));
        grants.dedup();
        let mut singleton = SingletonChecker::default();
        for (holder, token) in grants {
            singleton.acquired(holder, token);
        }
        out.extend(singleton.violations().iter().cloned());
        out.extend(checks.escrow.violations().iter().cloned());
        out.extend(checks.extra.iter().cloned());
        out
    }

    pub fn record_violation(&mut self, v: Violation) {
        self.checks.lock().unwrap().extra.push(v);
    }
}

fn now_micros_duration() -> Duration {
    use openraft::Instant;
    crate::runtime::SimInstant::now().as_duration()
}

fn now_micros() -> u64 {
    now_micros_duration().as_micros() as u64
}

const CLIENT_TIMEOUT: Duration = Duration::from_secs(1);

enum WriteOutcome {
    Acked,
    Failed,
    Unknown,
}

async fn submit_write(router: &Router, home: NodeId, cmd: Command) -> WriteOutcome {
    let Some((raft, _)) = router.target(home) else {
        return WriteOutcome::Failed;
    };
    match SimRuntime::timeout(CLIENT_TIMEOUT, raft.client_write(cmd.clone())).await {
        Ok(Ok(_)) => WriteOutcome::Acked,
        Ok(Err(e)) => match e.forward_to_leader() {
            Some(fwd) => match fwd.leader_id {
                Some(leader) if leader != home => {
                    let forwarded = router
                        .call(home, leader, move |raft, _| async move {
                            SimRuntime::timeout(CLIENT_TIMEOUT, raft.client_write(cmd)).await.ok()?.ok()
                        })
                        .await;
                    if forwarded.is_ok() { WriteOutcome::Acked } else { WriteOutcome::Unknown }
                }
                _ => WriteOutcome::Failed,
            },
            None => WriteOutcome::Unknown,
        },
        Err(_) => WriteOutcome::Unknown,
    }
}

async fn linearizable_read(
    router: &Router,
    home: NodeId,
    key: String,
    lease: bool,
    bug: Option<Bug>,
) -> Option<Option<u64>> {
    let (raft, _) = router.target(home)?;
    let target = raft.metrics().borrow_watched().current_leader.unwrap_or(home);
    router
        .call(home, target, move |raft, _| async move {
            if bug != Some(Bug::StaleRead) && !(lease && raft.ensure_linearizable(ReadPolicy::LeaseRead).await.is_ok())
            {
                SimRuntime::timeout(CLIENT_TIMEOUT, raft.ensure_linearizable(ReadPolicy::ReadIndex))
                    .await
                    .ok()?
                    .ok()?;
            }
            raft.with_state_machine(move |sm: &mut KvStateMachine| {
                let v = sm.read(|s| s.get(&key).and_then(|v| decode(v)));
                Box::pin(async move { v })
            })
            .await
            .ok()
        })
        .await
        .ok()
}

#[allow(clippy::too_many_arguments)]
async fn client_loop(
    router: Router,
    checks: Arc<Mutex<Checks>>,
    client: u64,
    home: NodeId,
    keys: u64,
    interval: Duration,
    stop_at: Duration,
    mut rng: SimRng,
    (bug, lease_reads): (Option<Bug>, bool),
) {
    let mut seq = 0u64;
    loop {
        let jitter = Duration::from_micros(rng.range(0, interval.as_micros() as u64 + 1));
        SimRuntime::sleep(interval + jitter).await;
        if now_micros_duration() >= stop_at {
            return;
        }
        let key = format!("k{}", rng.range(0, keys.max(1)));
        if rng.chance(0.15) {
            seq += 1;
            escrow_step(&router, &checks, client, &mut seq, home, &mut rng, bug).await;
        } else if rng.chance(0.1) {
            seq += 1;
            let op = quorium_core::kv::LeaseOp {
                name: "job".into(),
                holder: home,
                ttl_ms: 500,
                now_ms: now_micros() / 1000,
                action: quorium_core::kv::LeaseAction::Acquire,
            };
            let cmd = Command::new(client, seq, KvOp::Lease(op));
            if let Some((raft, _)) = router.target(home)
                && let Ok(Ok(resp)) = SimRuntime::timeout(CLIENT_TIMEOUT, raft.client_write(cmd)).await
                && let Reply::Lease { held: true, token, holder, .. } = resp.data
            {
                checks.lock().unwrap().leases.push((holder, token));
            }
        } else if rng.chance(0.5) {
            seq += 1;
            let value = (client << 32) | seq;
            let cmd = Command::new(client, seq, KvOp::Put { key: key.clone(), value: encode(value) });
            let call = now_micros();
            let outcome = submit_write(&router, home, cmd).await;
            let ret = now_micros();
            let mut c = checks.lock().unwrap();
            match outcome {
                WriteOutcome::Acked => {
                    c.acked.push((client, seq));
                    c.histories.entry(key).or_default().record(
                        client,
                        RegisterOp::Write(value),
                        call,
                        Some((ret, RegisterRet::Ok)),
                    );
                }
                WriteOutcome::Unknown => {
                    c.histories.entry(key).or_default().record(client, RegisterOp::Write(value), call, None);
                }
                WriteOutcome::Failed => {}
            }
        } else {
            let call = now_micros();
            if let Some(value) = linearizable_read(&router, home, key.clone(), lease_reads, bug).await {
                let ret = now_micros();
                checks.lock().unwrap().histories.entry(key).or_default().record(
                    client,
                    RegisterOp::Read,
                    call,
                    Some((ret, RegisterRet::Value(value))),
                );
            }
        }
    }
}

const ESCROW: &str = "sku";

async fn escrow_write(router: &Router, home: NodeId, cmd: Command) -> Option<Reply> {
    let (raft, _) = router.target(home)?;
    match SimRuntime::timeout(CLIENT_TIMEOUT, raft.client_write(cmd.clone())).await {
        Ok(Ok(resp)) => Some(resp.data),
        Ok(Err(e)) => match e.forward_to_leader().and_then(|f| f.leader_id) {
            Some(leader) if leader != home => router
                .call(home, leader, move |raft, _| async move {
                    SimRuntime::timeout(CLIENT_TIMEOUT, raft.client_write(cmd)).await.ok()?.ok()
                })
                .await
                .ok()
                .map(|r| r.data),
            _ => None,
        },
        Err(_) => None,
    }
}

async fn escrow_step(
    router: &Router,
    checks: &Arc<Mutex<Checks>>,
    client: u64,
    seq: &mut u64,
    home: NodeId,
    rng: &mut SimRng,
    bug: Option<Bug>,
) {
    if router.target(home).is_none() {
        return;
    }
    let pending = {
        let mut c = checks.lock().unwrap();
        let local = c.local_escrow.entry(home).or_default();
        if local.pending.is_none() && local.unsettled + local.remaining > 0 && rng.chance(0.2) {
            let release = if rng.chance(0.3) { local.remaining } else { 0 };
            let sold = local.unsettled;
            local.remaining -= release;
            local.unsettled = 0;
            let op = EscrowOp { name: ESCROW.into(), node: home, action: EscrowAction::Settle { sold, release } };
            local.pending = Some((Command::new(client, *seq, KvOp::Escrow(op)), sold, release));
        }
        local.pending.clone()
    };
    if let Some((cmd, _sold, release)) = pending {
        if let Some(Reply::Escrow { .. }) = escrow_write(router, home, cmd).await {
            let mut c = checks.lock().unwrap();
            c.escrow.release(home, release);
            c.local_escrow.entry(home).or_default().pending = None;
        }
        return;
    }
    let sold_locally = {
        let mut c = checks.lock().unwrap();
        let oversell = bug == Some(Bug::OversellEscrow);
        let local = c.local_escrow.entry(home).or_default();
        if local.remaining > 0 || (oversell && local.unsettled > 0) {
            local.remaining = local.remaining.saturating_sub(1);
            local.unsettled += 1;
            c.escrow.sell(home, 1);
            c.escrow_sales += 1;
            true
        } else {
            false
        }
    };
    if sold_locally {
        return;
    }
    let grant = EscrowOp { name: ESCROW.into(), node: home, action: EscrowAction::Grant { want: 4 } };
    match escrow_write(router, home, Command::new(client, *seq, KvOp::Escrow(grant))).await {
        Some(Reply::Escrow { granted, .. }) if granted > 0 => {
            let mut c = checks.lock().unwrap();
            c.escrow.grant(home, granted);
            if router.target(home).is_some() {
                c.local_escrow.entry(home).or_default().remaining += granted;
            }
        }
        Some(Reply::Escrow { .. }) => {
            *seq += 1;
            let buy = EscrowOp { name: ESCROW.into(), node: home, action: EscrowAction::Buy { units: 1 } };
            if let Some(Reply::Escrow { granted: 1, .. }) =
                escrow_write(router, home, Command::new(client, *seq, KvOp::Escrow(buy))).await
            {
                let mut c = checks.lock().unwrap();
                c.escrow.grant(home, 1);
                c.escrow.sell(home, 1);
                c.escrow_sales += 1;
            }
        }
        _ => {}
    }
}
