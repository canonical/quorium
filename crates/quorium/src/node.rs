use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use openraft::Raft;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::impls::BasicNode;
use openraft_rt_tokio::TokioRuntime;
use quorium_core::config::raft_config;
use quorium_core::driver::Ladder;
use quorium_core::kv::{Command, KvOp, Reply};
use quorium_core::kv::{RouteOp, RoutingTable};
use quorium_core::shard::{entity_position, key_hash};
use quorium_core::types::NodeId;
use quorium_net::raft::{Request, Response, TcpNetwork, serve_with};
use quorium_net::transport::Listener;
use quorium_store::disk::FileDisk;
use quorium_store::mem::KvStateMachine;
use quorium_store::redb_state::StateDb;
use quorium_store::wal_log::SharedWal;
use tokio::task::JoinHandle;

use crate::builder::{Builder, StartError, node_id};
use crate::discovery::Discovery;
use crate::entity::{Key, State};
use crate::service::{
    NodeService, PeerBook, Shard, group_for, is_initialized, local_read, local_read_entity, local_write,
    observe_peer_ping, quiesced_shards,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    Unavailable(String),
    Timeout,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Unavailable(e) => write!(f, "cluster unavailable: {e}"),
            ClientError::Timeout => f.write_str("request timed out"),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Membership {
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub id: NodeId,
    pub leader: Option<NodeId>,
    pub term: u64,
    pub is_leader: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Stats {
    pub appends_sent: u64,
    pub pings_received: u64,
    pub quiesced_shards: usize,
    pub large_entity_writes: u64,
}

pub const LARGE_ENTITY_BYTES: usize = 1 << 20;

pub(crate) type Watches =
    Arc<Mutex<std::collections::HashMap<(String, Vec<u8>), tokio::sync::watch::Sender<Option<(u32, Vec<u8>)>>>>>;

pub(crate) struct Inner {
    routing: crate::service::RoutingCache,
    methods: crate::service::MethodBook,
    escrow: Arc<crate::escrow::EscrowBook>,
    peers: PeerBook,
    watches: Watches,
    large_entity_writes: Arc<AtomicU64>,
    announcement: Mutex<Option<Box<dyn std::any::Any + Send>>>,
    id: NodeId,
    datacenter: String,
    shards: Arc<Vec<Arc<Shard>>>,
    service: Arc<NodeService>,
    network: TcpNetwork,
    wal: SharedWal<TokioRuntime, FileDisk>,
    state_db: StateStores,
    server: Mutex<Option<Listener>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    client: u64,
    sequencer: quorium_core::session::Sequencer,
    timeout: Duration,
}

#[derive(Clone)]
pub struct Quorium {
    inner: Arc<Inner>,
}

const RPC_TIMEOUT: Duration = Duration::from_secs(2);

impl Quorium {
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub(crate) async fn start(b: Builder) -> Result<Quorium, StartError> {
        let (dir, bind) = b.validate()?;
        std::fs::create_dir_all(&dir)?;
        let id = node_id(&dir)?;
        let advertise = b.advertise.unwrap_or(bind);
        let disk = FileDisk::open(&dir.join("wal"))?;
        let wal = SharedWal::<TokioRuntime, FileDisk>::open(disk).map_err(|e| StartError::Storage(e.to_string()))?;
        let network = TcpNetwork::with_protocol(id, b.tls.clone(), b.transport);
        let mut ladder_config = quorium_core::config::ladder_for(b.heartbeat, &b.ladder);
        ladder_config.handoff_suspend = b.handoff_suspend;
        let mut config = raft_config(b.heartbeat, &ladder_config);
        if let Some(n) = b.checkpoint_every {
            config.snapshot_policy = openraft::SnapshotPolicy::LogsSinceLast(n);
            config.max_in_snapshot_log_to_keep = config.max_in_snapshot_log_to_keep.min(n);
        }
        let config = Arc::new(config);
        let watches: Watches = Arc::default();
        let large_entity_writes = Arc::new(AtomicU64::new(0));
        let state_db = StateStores::open(b.state_backend, &dir)?;
        let mut shards = Vec::new();
        for s in 0..b.shards {
            let legacy = dir.join("state").join(format!("shard-{s}"));
            let sm = KvStateMachine::with_store(state_db.shard(s, &legacy))?;
            let registry = watches.clone();
            let large = large_entity_writes.clone();
            sm.set_change_observer(Arc::new(move |type_name: &str, key: &[u8], value: Option<(u32, &[u8])>| {
                if let Some((_, bytes)) = value.filter(|(_, b)| b.len() >= LARGE_ENTITY_BYTES) {
                    large.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        type_name,
                        size = bytes.len(),
                        "entity state exceeds 1 MiB; split it into smaller entities"
                    );
                }
                let mut watches = registry.lock().unwrap();
                let slot = (type_name.to_string(), key.to_vec());
                if let Some(sender) = watches.get(&slot) {
                    if sender.receiver_count() == 0 {
                        watches.remove(&slot);
                    } else {
                        sender.send_replace(value.map(|(v, b)| (v, b.to_vec())));
                    }
                }
            }));
            if let Some(unknown) = sm.unknown_types().into_iter().next() {
                return Err(StartError::UnknownType(unknown));
            }
            let raft = Raft::new(id, config.clone(), network.for_shard(s), wal.shard(s), sm)
                .await
                .map_err(|e| StartError::Raft(e.to_string()))?;
            let dcs: BTreeMap<NodeId, String> = [(id, b.datacenter.clone())].into();
            let ladder =
                Ladder::<TokioRuntime>::for_shard(ladder_config.clone(), id, dcs, Duration::from_millis(10), s);
            shards.push(Arc::new(Shard::new(s, raft, ladder)));
        }
        let shards = Arc::new(shards);
        let peers: PeerBook = Arc::default();
        let escrow = Arc::new(crate::escrow::EscrowBook::open(&dir)?);
        let methods: crate::service::MethodBook = Arc::default();
        methods
            .lock()
            .unwrap()
            .insert(id, b.advertised_methods.clone().unwrap_or_else(quorium_core::entity::local_methods));
        let routing: crate::service::RoutingCache = Arc::new(std::sync::RwLock::new(RoutingTable::uniform(b.shards)));
        let service = Arc::new(NodeService {
            routing: routing.clone(),
            methods: methods.clone(),
            escrow: escrow.clone(),
            id,
            peers: peers.clone(),
            shards: shards.clone(),
            datacenter: b.datacenter.clone(),
            pings: AtomicU64::new(0),
        });
        let server = serve_with(b.transport, bind, b.tls.clone(), service.clone()).await?;
        let announcement = announce(&b.discovery, id, advertise)?;
        let mut tasks: Vec<JoinHandle<()>> =
            shards.iter().map(|s| tokio::spawn(s.ladder.clone().run(s.raft.clone()))).collect();
        let seen: Seen = Arc::default();
        tasks.push(tokio::spawn(ping_loop(
            shards.clone(),
            id,
            network.clone(),
            b.discovery.clone(),
            advertise,
            b.datacenter.clone(),
            b.ping,
            seen.clone(),
            peers.clone(),
            methods.clone(),
        )));
        tasks.push(tokio::spawn(manage_membership(shards.clone(), id, b.replicas, b.replacement_grace, peers.clone())));
        tasks.push(tokio::spawn(bootstrap(
            shards.clone(),
            id,
            advertise,
            b.single_node,
            b.replicas,
            b.discovery.clone(),
            seen,
        )));
        tasks.push(tokio::spawn(housekeeping(
            shards.clone(),
            id,
            b.datacenter.clone(),
            b.quiesce_after,
            peers.clone(),
        )));
        let node = Quorium {
            inner: Arc::new(Inner {
                routing,
                methods: methods.clone(),
                escrow,
                peers: peers.clone(),
                watches,
                large_entity_writes,
                announcement: Mutex::new(announcement),
                id,
                datacenter: b.datacenter.clone(),
                shards,
                service,
                network,
                wal,
                state_db,
                server: Mutex::new(Some(server)),
                tasks: Mutex::new(tasks),
                client: rand::random::<u64>() >> 1,
                sequencer: quorium_core::session::Sequencer::default(),
                timeout: Duration::from_secs(5),
            }),
        };
        let weak = Arc::downgrade(&node.inner);
        let settle = tokio::spawn(async move {
            let mut ticks = 0u64;
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let Some(inner) = weak.upgrade() else { return };
                let node = Quorium { inner };
                node.settle_escrows().await;
                ticks += 1;
                if ticks.is_multiple_of(5) {
                    node.reclaim_escrows().await;
                }
            }
        });
        node.inner.tasks.lock().unwrap().push(settle);
        let weak = Arc::downgrade(&node.inner);
        let heartbeat = b.heartbeat;
        let disks = tokio::spawn(async move {
            let mut warned: Option<Instant> = None;
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let Some(inner) = weak.upgrade() else { return };
                let latency = inner.wal.sync_latency();
                for s in inner.shards.iter() {
                    s.ladder.observe_sync_latency(latency);
                }
                let slowest = inner.shards.first().and_then(|s| s.ladder.slowest_round_trip());
                if let Some((peer, rtt)) = slowest.filter(|(_, rtt)| *rtt > heartbeat)
                    && warned.is_none_or(|w| w.elapsed() > Duration::from_secs(60))
                {
                    tracing::warn!(
                        peer,
                        round_trip_ms = rtt.as_millis() as u64,
                        heartbeat_ms = heartbeat.as_millis() as u64,
                        "the round trip to a peer exceeds the heartbeat interval; raise heartbeat() above it"
                    );
                    warned = Some(Instant::now());
                }
            }
        });
        node.inner.tasks.lock().unwrap().push(disks);
        if let Some((every, min_items)) = b.rebalance {
            let weak = Arc::downgrade(&node.inner);
            let load_split = b.load_split;
            let task = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    let Some(inner) = weak.upgrade() else { return };
                    Quorium { inner }.rebalance_once(min_items, load_split).await;
                }
            });
            node.inner.tasks.lock().unwrap().push(task);
        }
        if let Some(addr) = b.metrics_bind {
            crate::metrics::serve_metrics(addr, Arc::downgrade(&node.inner)).await?;
        }
        Ok(node)
    }

    pub(crate) async fn method_enabled(&self, shard: u64, type_name: &str, method: u32) -> bool {
        use quorium_core::entity::{MethodSupport, method_support};
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let voters: Vec<NodeId> = self.shard_metrics(shard).membership_config.membership().voter_ids().collect();
            let support = method_support(type_name, method, &voters, &self.inner.methods.lock().unwrap());
            match support {
                MethodSupport::Enabled => return true,
                MethodSupport::Missing => return false,
                MethodSupport::Unknown if Instant::now() >= deadline => return false,
                MethodSupport::Unknown => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    }

    pub async fn group_sizes(&self) -> Vec<usize> {
        let mut sizes = Vec::new();
        for s in self.inner.shards.iter() {
            let size = s
                .raft
                .with_state_machine(|sm: &mut KvStateMachine| {
                    let n = sm.read(|state| state.item_count());
                    Box::pin(async move { n })
                })
                .await
                .unwrap_or(0);
            sizes.push(size);
        }
        sizes
    }

    async fn rebalance_once(&self, min_items: usize, load_split: Option<u64>) {
        if !self.shard_status(0).is_leader {
            return;
        }
        let sizes = self.group_sizes().await;
        let total: usize = sizes.iter().sum();
        let average = total / sizes.len().max(1);
        let Some((hot, &largest)) = sizes.iter().enumerate().max_by_key(|(_, s)| **s) else { return };
        let Some((cold, _)) = sizes.iter().enumerate().min_by_key(|(_, s)| **s) else { return };
        let Ok(table) = self.initialised_routing().await else { return };
        if table.moving.is_some() {
            if let Some((lo, hi, _, to)) = table.moving {
                let _ = self.move_range(lo, hi, to).await;
            }
            return;
        }
        if largest < min_items || largest <= 2 * average.max(1) || hot == cold {
            if let Some(min_rate) = load_split
                && self.split_by_load(&table, min_rate).await
            {
                return;
            }
            let _ = self.merge_ranges().await;
            return;
        }
        let Some(&(lo, hi, _)) =
            table.ranges.iter().filter(|(_, _, g)| *g == hot as u64).max_by_key(|(lo, hi, _)| hi - lo)
        else {
            return;
        };
        if hi - lo < 2 {
            return;
        }
        let at = lo + (hi - lo) / 2 + 1;
        if self.split_range(at).await.is_ok() {
            let _ = self.move_range(at, hi, cold as u64).await;
        }
    }

    async fn split_by_load(&self, table: &RoutingTable, min_rate: u64) -> bool {
        let mut loads = Vec::new();
        for g in 0..self.inner.shards.len() as u64 {
            match self.route_to(g, || Request::ReadLoad).await {
                Ok(Response::Load { per_second, samples }) => loads.push((per_second, samples)),
                _ => return false,
            }
        }
        let average = loads.iter().map(|(r, _)| *r).sum::<u64>() / loads.len().max(1) as u64;
        let Some((hot, (rate, samples))) = loads.iter().enumerate().max_by_key(|(_, (r, _))| *r) else { return false };
        let Some((cold, _)) = loads.iter().enumerate().min_by_key(|(_, (r, _))| *r) else { return false };
        if *rate < min_rate || *rate <= 2 * average.max(1) || hot == cold {
            return false;
        }
        let inside = |lo: u64, hi: u64| samples.iter().filter(move |p| lo <= **p && **p <= hi);
        let Some((lo, hi)) = table.ranges_of(hot as u64).into_iter().max_by_key(|(lo, hi)| inside(*lo, *hi).count())
        else {
            return false;
        };
        let mut points: Vec<u64> = inside(lo, hi).copied().collect();
        points.sort_unstable();
        let Some(&median) = points.get(points.len() / 2) else { return false };
        let at = median.max(lo.saturating_add(1));
        if points.len() < 2 || at > hi {
            return false;
        }
        self.split_range(at).await.is_ok() && self.move_range(at, hi, cold as u64).await.is_ok()
    }

    pub(crate) fn from_inner(inner: Arc<Inner>) -> Quorium {
        Quorium { inner }
    }

    pub(crate) fn datacenter(&self) -> &str {
        &self.inner.datacenter
    }

    pub(crate) fn shard_metrics(&self, s: u64) -> openraft::RaftMetrics<quorium_net::raft::ProdTypes> {
        self.shard(s).raft.metrics().borrow_watched().clone()
    }

    pub(crate) fn shard_write_rate(&self, s: u64) -> u64 {
        self.shard(s).load().0
    }

    pub(crate) fn shard_quiesced(&self, s: u64) -> bool {
        self.shard(s).activity.lock().unwrap().quiesced
    }

    pub(crate) fn inner_wal_sync_latency(&self) -> Duration {
        self.inner.wal.sync_latency()
    }

    pub(crate) fn wal_stats(&self) -> (u64, u64) {
        (self.inner.wal.fsyncs(), self.inner.wal.disk_len())
    }

    pub(crate) fn escrow_book(&self) -> Arc<crate::escrow::EscrowBook> {
        self.inner.escrow.clone()
    }

    fn same_datacenter_peers(&self) -> Vec<String> {
        let ladder = &self.shard(0).ladder;
        let book = self.inner.peers.lock().unwrap();
        book.iter()
            .filter(|(id, (_, at))| {
                **id != self.inner.id
                    && at.elapsed() < Duration::from_secs(2)
                    && ladder.datacenter_of(**id).as_deref() == Some(self.inner.datacenter.as_str())
            })
            .map(|(_, (addr, _))| addr.clone())
            .collect()
    }

    pub(crate) async fn tell_peer(&self, request: Request) {
        if let Some(addr) = self.same_datacenter_peers().into_iter().next() {
            let _ = self.inner.network.request_addr(&addr, request, Duration::from_millis(300)).await;
        }
    }

    pub(crate) fn shard_members(&self, shard: usize) -> std::collections::BTreeSet<NodeId> {
        let metrics = self.inner.shards[shard].raft.metrics().borrow_watched().clone();
        let membership = metrics.membership_config.membership();
        membership.voter_ids().chain(membership.learner_ids()).collect()
    }

    pub(crate) fn shard_count(&self) -> usize {
        self.inner.shards.len()
    }

    pub(crate) async fn escrow_holders(
        &self,
        shard: usize,
    ) -> Vec<(quorium_core::kv::EscrowKey, NodeId, quorium_core::kv::Holder)> {
        self.inner.shards[shard]
            .raft
            .with_state_machine(|sm: &mut KvStateMachine| {
                let v = sm.read(|s| s.escrow_holders().map(|(k, n, h)| (k.clone(), n, *h)).collect::<Vec<_>>());
                Box::pin(async move { v })
            })
            .await
            .unwrap_or_default()
    }

    pub(crate) fn escrow_copy(&self) -> crate::escrow::Copy {
        let members = self.shard_members(0);
        if !members.contains(&self.inner.id) {
            return crate::escrow::Copy::Unavailable;
        }
        if members.len() == 1 {
            return crate::escrow::Copy::Alone;
        }
        self.escrow_copy_peer(&members).map_or(crate::escrow::Copy::Unavailable, |(id, addr)| {
            crate::escrow::Copy::Peer(id, addr)
        })
    }

    fn escrow_copy_peer(&self, members: &std::collections::BTreeSet<NodeId>) -> Option<(NodeId, String)> {
        let ladder = &self.shard(0).ladder;
        let book = self.inner.peers.lock().unwrap();
        book.iter()
            .filter(|(id, (_, at))| {
                **id != self.inner.id
                    && members.contains(id)
                    && at.elapsed() < Duration::from_secs(2)
                    && ladder.datacenter_of(**id).as_deref() == Some(self.inner.datacenter.as_str())
            })
            .min_by_key(|(id, _)| **id)
            .map(|(id, (addr, _))| (*id, addr.clone()))
    }

    pub(crate) async fn copy_sale(&self, addr: &str, sale: Request) -> Option<bool> {
        match self.inner.network.request_addr(addr, sale, Duration::from_millis(300)).await {
            Ok(Response::Recorded) => Some(true),
            Ok(Response::Failed(e)) if e == crate::escrow::FENCED => Some(false),
            _ => None,
        }
    }

    fn shard(&self, s: u64) -> &Arc<Shard> {
        &self.inner.shards[s as usize]
    }

    pub(crate) fn subscribe(
        &self,
        type_name: &str,
        key: &[u8],
    ) -> tokio::sync::watch::Receiver<Option<(u32, Vec<u8>)>> {
        let mut watches = self.inner.watches.lock().unwrap();
        watches
            .entry((type_name.to_string(), key.to_vec()))
            .or_insert_with(|| tokio::sync::watch::channel(None).0)
            .subscribe()
    }

    pub fn shards(&self) -> u64 {
        self.inner.shards.len() as u64
    }

    pub fn shard_status(&self, s: u64) -> Status {
        let m = self.shard(s).raft.metrics().borrow_watched().clone();
        Status {
            id: self.inner.id,
            leader: m.current_leader,
            term: m.current_term,
            is_leader: m.current_leader == Some(self.inner.id),
        }
    }

    pub fn status(&self) -> Status {
        self.shard_status(0)
    }

    pub fn stats(&self) -> Stats {
        Stats {
            appends_sent: self.inner.network.appends_sent(),
            pings_received: self.inner.service.pings.load(Ordering::Relaxed),
            quiesced_shards: quiesced_shards(&self.inner.shards, self.inner.id).len(),
            large_entity_writes: self.inner.large_entity_writes.load(Ordering::Relaxed),
        }
    }

    pub fn shard_of<T: State>(&self, key: &T::Key) -> u64 {
        self.group_of_position(entity_position(T::NAME, &key.to_key_bytes(), T::locality(key).as_deref()))
    }

    pub(crate) fn group_of_position(&self, position: u64) -> u64 {
        self.inner.routing.read().unwrap().group_of(position)
    }

    pub fn membership(&self) -> Membership {
        let m = self.shard(0).raft.metrics().borrow_watched().clone();
        let membership = m.membership_config.membership();
        Membership { voters: membership.voter_ids().collect(), learners: membership.learner_ids().collect() }
    }

    fn leader_addr(&self, s: u64, leader: NodeId) -> Option<String> {
        let m = self.shard(s).raft.metrics().borrow_watched().clone();
        m.membership_config.membership().get_node(&leader).map(|n| n.addr.clone())
    }

    async fn send_to_group(&self, s: u64, request: Request) -> Response {
        let Some(shard) = self.inner.shards.get(s as usize).cloned() else {
            return Response::Failed(format!("no group {s}"));
        };
        match self.shard_status(s).leader {
            Some(l) if l == self.inner.id => match request {
                Request::Write(cmd) => local_write(&shard, cmd, Some(self.inner.datacenter.clone())).await,
                Request::Read { key } => {
                    shard.wake().await;
                    local_read(&shard.raft, key).await
                }
                Request::ReadEntity { type_name, key, locality } => {
                    shard.wake().await;
                    local_read_entity(&shard.raft, type_name, key, locality).await
                }
                Request::ReadEscrow { name } => {
                    shard.wake().await;
                    crate::service::local_read_escrow(&shard.raft, name).await
                }
                Request::ExportShard { .. } => {
                    shard.wake().await;
                    crate::service::local_export(&shard.raft).await
                }
                Request::ReadRouting => crate::service::local_read_routing(&shard.raft).await,
                Request::ReadLoad => {
                    let (per_second, samples) = shard.load();
                    Response::Load { per_second, samples }
                }
                other => Response::Failed(format!("not routable: {other:?}")),
            },
            Some(l) => match self.leader_addr(s, l) {
                Some(addr) => self
                    .inner
                    .network
                    .request_addr_shard(&addr, s, request, RPC_TIMEOUT)
                    .await
                    .unwrap_or_else(|e| Response::Failed(e.to_string())),
                None => Response::NotLeader { leader: None, addr: None },
            },
            None => Response::NotLeader { leader: None, addr: None },
        }
    }

    async fn route_with(
        &self,
        target: impl Fn(&Request) -> u64,
        make: impl Fn() -> Request,
    ) -> Result<Response, ClientError> {
        let deadline = Instant::now() + self.inner.timeout;
        let mut last = String::from("no leader");
        while Instant::now() < deadline {
            let request = make();
            let s = target(&request);
            match self.send_to_group(s, request).await {
                Response::NotLeader { .. } => {}
                Response::WrongShard => {
                    last = String::from("routing changed");
                    self.refresh_routing().await;
                }
                Response::Failed(e) => last = e,
                done => return Ok(done),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err(ClientError::Unavailable(last))
    }

    pub(crate) async fn route(&self, make: impl Fn() -> Request) -> Result<Response, ClientError> {
        let groups = self.shards();
        let routing = self.inner.routing.clone();
        self.route_with(move |r| group_for(r, &routing, groups).unwrap_or(0), make).await
    }

    pub(crate) async fn route_to(&self, group: u64, make: impl Fn() -> Request) -> Result<Response, ClientError> {
        self.route_with(move |_| group, make).await
    }

    async fn refresh_routing(&self) {
        for _ in 0..25 {
            match self.send_to_group(0, Request::ReadRouting).await {
                Response::Routing(Some(table)) => {
                    let mut cache = self.inner.routing.write().unwrap();
                    if table.generation >= cache.generation {
                        *cache = table;
                    }
                    return;
                }
                Response::Routing(None) => return,
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    }

    pub async fn routing(&self) -> Result<RoutingTable, crate::Error<std::convert::Infallible>> {
        match self.route_to(0, || Request::ReadRouting).await? {
            Response::Routing(Some(table)) => {
                *self.inner.routing.write().unwrap() = table.clone();
                Ok(table)
            }
            Response::Routing(None) => Ok(RoutingTable::uniform(self.shards())),
            other => Err(crate::Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    async fn admin(&self, group: u64, op: KvOp) -> Result<Reply, crate::Error<std::convert::Infallible>> {
        let (cmd, _in_flight) = self.next_command(op);
        match self.route_to(group, || Request::Write(cmd.clone())).await? {
            Response::Written(Reply::ApplyFailed(e)) => Err(crate::Error::Internal(e)),
            Response::Written(Reply::Stale) => Err(crate::Error::Internal("command refused as stale".into())),
            Response::Written(reply) => Ok(reply),
            other => Err(crate::Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    async fn route_op(&self, op: RouteOp) -> Result<RoutingTable, crate::Error<std::convert::Infallible>> {
        match self.admin(0, KvOp::Route(op)).await? {
            Reply::Routing(table) => {
                let mut cache = self.inner.routing.write().unwrap();
                if table.generation >= cache.generation {
                    *cache = table.clone();
                }
                Ok(table)
            }
            other => Err(crate::Error::Internal(format!("unexpected reply {other:?}"))),
        }
    }

    async fn initialised_routing(&self) -> Result<RoutingTable, crate::Error<std::convert::Infallible>> {
        self.route_op(RouteOp::Init { table: RoutingTable::uniform(self.shards()) }).await
    }

    pub async fn split_range(&self, at: u64) -> Result<RoutingTable, crate::Error<std::convert::Infallible>> {
        let table = self.initialised_routing().await?;
        self.route_op(RouteOp::Split { at, generation: table.generation }).await
    }

    pub async fn merge_ranges(&self) -> Result<RoutingTable, crate::Error<std::convert::Infallible>> {
        let table = self.initialised_routing().await?;
        self.route_op(RouteOp::Merge { generation: table.generation }).await
    }

    pub async fn __test_begin_move(
        &self,
        lo: u64,
        hi: u64,
        to: u64,
    ) -> Result<RoutingTable, crate::Error<std::convert::Infallible>> {
        let table = self.initialised_routing().await?;
        self.route_op(RouteOp::BeginMove { lo, hi, to, generation: table.generation }).await
    }

    pub async fn move_range(
        &self,
        lo: u64,
        hi: u64,
        to: u64,
    ) -> Result<RoutingTable, crate::Error<std::convert::Infallible>> {
        let table = self.initialised_routing().await?;
        if table.moving.is_none() && table.ranges.contains(&(lo, hi, to)) {
            return Ok(table);
        }
        let table = self.route_op(RouteOp::BeginMove { lo, hi, to, generation: table.generation }).await?;
        let Some((_, _, from, _)) = table.moving else {
            return Err(crate::Error::Internal("move was not recorded".into()));
        };
        self.admin(from, KvOp::OwnIfUnset { ranges: table.ranges_of(from) }).await?;
        let target_ranges: Vec<(u64, u64)> = table.ranges_of(to);
        self.admin(to, KvOp::OwnIfUnset { ranges: target_ranges }).await?;
        let state = match self.admin(from, KvOp::Extract { lo, hi }).await? {
            Reply::Extracted(bytes) => bytes,
            other => return Err(crate::Error::Internal(format!("unexpected reply {other:?}"))),
        };
        self.admin(to, KvOp::Ingest { lo, hi, state }).await?;
        let table = self.route_op(RouteOp::FinishMove { lo, hi, to }).await?;
        self.admin(from, KvOp::Release { lo, hi }).await?;
        Ok(table)
    }

    pub(crate) fn next_command(&self, op: KvOp) -> (Command, quorium_core::session::Ticket) {
        let ticket = self.inner.sequencer.issue();
        let cmd = Command::new(self.inner.client, ticket.seq(), op).acknowledging_below(ticket.ack_below());
        (cmd, ticket)
    }

    pub fn global<T: State<Key = ()>>(&self) -> T::Handle {
        self.entity::<T>(())
    }

    pub(crate) async fn local_entity(
        &self,
        type_name: &str,
        key: &[u8],
        locality: Option<&str>,
    ) -> Option<Option<(u32, Vec<u8>)>> {
        let position = entity_position(type_name, key, locality);
        let shard = self.inner.shards.get(self.group_of_position(position) as usize)?.clone();
        let (type_name, key) = (type_name.to_string(), key.to_vec());
        shard
            .raft
            .with_state_machine(move |sm: &mut KvStateMachine| {
                let v = sm.read(|s| s.owns(position).then(|| s.entity(&type_name, &key).map(|(v, b)| (v, b.to_vec()))));
                Box::pin(async move { v })
            })
            .await
            .ok()
            .flatten()
    }

    pub fn entity<T: State>(&self, key: impl Into<T::Key>) -> T::Handle {
        <T::Handle as crate::entity::Handle>::from_entity(crate::entity::Entity::new(
            crate::entity::Backend::Node(self.clone()),
            &key.into(),
        ))
    }

    pub async fn write(&self, key: &str, value: Vec<u8>) -> Result<Reply, ClientError> {
        let (cmd, _in_flight) = self.next_command(KvOp::Put { key: key.into(), value });
        match self.route(|| Request::Write(cmd.clone())).await? {
            Response::Written(Reply::Stale) => Err(ClientError::Unavailable("the write was refused as stale".into())),
            Response::Written(reply) => Ok(reply),
            other => Err(ClientError::Unavailable(format!("{other:?}"))),
        }
    }

    pub async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        match self.route(|| Request::Read { key: key.into() }).await? {
            Response::Value(v) => Ok(v),
            other => Err(ClientError::Unavailable(format!("{other:?}"))),
        }
    }

    pub async fn checkpoint(&self) -> Result<(), ClientError> {
        for s in self.inner.shards.iter() {
            let applied = s.raft.metrics().borrow_watched().last_applied;
            if applied.is_none() {
                continue;
            }
            s.raft.trigger().snapshot().await.map_err(|e| ClientError::Unavailable(e.to_string()))?;
            s.raft
                .wait(Some(RPC_TIMEOUT * 5))
                .metrics(|m| m.snapshot >= applied, "checkpoint")
                .await
                .map_err(|_| ClientError::Timeout)?;
        }
        Ok(())
    }

    pub fn shard_of_key(&self, key: &str) -> u64 {
        self.group_of_position(key_hash(key.as_bytes()))
    }

    pub async fn shutdown(self) {
        for t in self.inner.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
        for s in self.inner.shards.iter() {
            let _ = s.raft.shutdown().await;
        }
        let server = self.inner.server.lock().unwrap().take();
        if let Some(server) = server {
            server.shutdown().await;
        }
        self.inner.announcement.lock().unwrap().take();
        self.inner.wal.close();
        self.inner.state_db.close();
    }
}

#[cfg(feature = "mdns")]
fn announce(
    discovery: &Discovery,
    id: NodeId,
    advertise: SocketAddr,
) -> std::io::Result<Option<Box<dyn std::any::Any + Send>>> {
    match discovery {
        Discovery::Mdns { service } => Ok(Some(
            Box::new(crate::discovery::announce_mdns(service, id, advertise)?) as Box<dyn std::any::Any + Send>
        )),
        _ => Ok(None),
    }
}

#[cfg(not(feature = "mdns"))]
fn announce(
    _discovery: &Discovery,
    _id: NodeId,
    _advertise: SocketAddr,
) -> std::io::Result<Option<Box<dyn std::any::Any + Send>>> {
    Ok(None)
}

#[derive(Clone)]
enum StateStores {
    Redb(StateDb),
    #[cfg(feature = "fjall")]
    Fjall(quorium_store::fjall_state::FjallDb),
}

impl StateStores {
    fn open(backend: crate::builder::StateBackend, dir: &std::path::Path) -> std::io::Result<Self> {
        let (wanted, other) = match backend {
            crate::builder::StateBackend::Redb => ("state.redb", "state.fjall"),
            #[cfg(feature = "fjall")]
            crate::builder::StateBackend::Fjall => ("state.fjall", "state.redb"),
        };
        if !dir.join(wanted).exists() && dir.join(other).exists() {
            return Err(std::io::Error::other(format!(
                "{} holds state in {other}; switching state backends is not supported",
                dir.display()
            )));
        }
        Ok(match backend {
            crate::builder::StateBackend::Redb => StateStores::Redb(StateDb::open(&dir.join("state.redb"))?),
            #[cfg(feature = "fjall")]
            crate::builder::StateBackend::Fjall => {
                StateStores::Fjall(quorium_store::fjall_state::FjallDb::open(&dir.join("state.fjall"))?)
            }
        })
    }

    fn shard(&self, shard: u64, legacy: &std::path::Path) -> Arc<dyn quorium_store::mem::Storage> {
        match self {
            StateStores::Redb(db) => Arc::new(db.shard(shard).migrating_from(legacy)),
            #[cfg(feature = "fjall")]
            StateStores::Fjall(db) => Arc::new(db.shard(shard).migrating_from(legacy)),
        }
    }

    fn close(&self) {
        match self {
            StateStores::Redb(db) => db.close(),
            #[cfg(feature = "fjall")]
            StateStores::Fjall(db) => db.close(),
        }
    }
}

type Seen = Arc<Mutex<BTreeMap<SocketAddr, (NodeId, Instant, bool)>>>;

#[allow(clippy::too_many_arguments)]
async fn ping_loop(
    shards: Arc<Vec<Arc<Shard>>>,
    id: NodeId,
    network: TcpNetwork,
    discovery: Discovery,
    advertise: SocketAddr,
    datacenter: String,
    interval: Duration,
    seen: Seen,
    peers: PeerBook,
    methods: crate::service::MethodBook,
) {
    loop {
        tokio::time::sleep(interval).await;
        let own: Vec<(String, u32)> =
            methods.lock().unwrap().get(&id).map(|m| m.iter().cloned().collect()).unwrap_or_default();
        let seeds = discovery.resolve().await;
        let mut members: BTreeSet<String> = BTreeSet::new();
        for s in shards.iter() {
            let m = s.raft.metrics().borrow_watched().clone();
            members.extend(m.membership_config.membership().nodes().map(|(_, n)| n.addr.clone()));
        }
        let targets: BTreeSet<String> =
            seeds.iter().map(SocketAddr::to_string).chain(members).filter(|a| *a != advertise.to_string()).collect();
        let logs: Vec<(u64, quorium_core::ladder::LogPosition)> =
            shards.iter().map(|s| (s.id, s.ladder.ping())).collect();
        let quiesced = quiesced_shards(&shards, id);
        let initialized = shards.first().is_some_and(|s| is_initialized(&s.raft));
        let pings = targets.into_iter().map(|addr| {
            let network = network.clone();
            let request = Request::Ping {
                logs: logs.clone(),
                quiesced: quiesced.clone(),
                datacenter: datacenter.clone(),
                addr: advertise.to_string(),
                initialized,
                methods: own.clone(),
            };
            async move {
                let sent = Instant::now();
                let result = network.request_addr(&addr, request, interval * 2).await;
                (addr.clone(), result, sent.elapsed())
            }
        });
        for (addr, result, rtt) in futures::future::join_all(pings).await {
            if let Ok(Response::Pong { id: peer, logs, quiesced, datacenter, initialized, methods: theirs }) = result {
                for s in shards.iter() {
                    s.ladder.observe_round_trip(peer, rtt);
                }
                methods.lock().unwrap().insert(peer, theirs.into_iter().collect());
                observe_peer_ping(&shards, peer, &datacenter, &logs, &quiesced);
                peers.lock().unwrap().insert(peer, (addr.clone(), Instant::now()));
                if let Ok(sock) = addr.parse() {
                    seen.lock().unwrap().insert(sock, (peer, Instant::now(), initialized));
                }
            }
        }
    }
}

async fn bootstrap(
    shards: Arc<Vec<Arc<Shard>>>,
    id: NodeId,
    advertise: SocketAddr,
    single_node: bool,
    replicas: usize,
    discovery: Discovery,
    seen: Seen,
) {
    let mut pending: Vec<Arc<Shard>> = shards.iter().cloned().collect();
    loop {
        let mut still = Vec::new();
        for s in pending {
            if !s.raft.is_initialized().await.unwrap_or(true) {
                still.push(s);
            }
        }
        pending = still;
        if pending.is_empty() {
            return;
        }
        let seeds = discovery.resolve().await;
        let members: Option<BTreeMap<NodeId, BasicNode>> = if single_node {
            Some([(id, BasicNode::new(advertise.to_string()))].into())
        } else {
            let fresh = Duration::from_secs(1);
            let seen = seen.lock().unwrap().clone();
            let recent: Vec<_> =
                seen.iter().filter(|(addr, (_, at, _))| seeds.contains(addr) && at.elapsed() < fresh).collect();
            let existing_cluster = seen.values().any(|(_, _, initialized)| *initialized);
            let mut live: BTreeMap<NodeId, BasicNode> =
                recent.iter().map(|(addr, (nid, _, _))| (*nid, BasicNode::new(addr.to_string()))).collect();
            live.insert(id, BasicNode::new(advertise.to_string()));
            let lowest = live.keys().next().copied() == Some(id);
            (!existing_cluster && live.len() >= replicas && lowest).then_some(live)
        };
        if let Some(members) = members {
            for s in &pending {
                let _ = s.raft.initialize(members.clone()).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

const CAUGHT_UP_SLACK: u64 = 16;

async fn manage_membership(
    shards: Arc<Vec<Arc<Shard>>>,
    id: NodeId,
    replicas: usize,
    grace: Duration,
    peers: PeerBook,
) {
    let started = Instant::now();
    let mut leading_since: BTreeMap<u64, Instant> = BTreeMap::new();
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let book = peers.lock().unwrap().clone();
        for s in shards.iter() {
            let raft = &s.raft;
            let m = raft.metrics().borrow_watched().clone();
            if m.current_leader != Some(id) {
                leading_since.remove(&s.id);
                continue;
            }
            let since = *leading_since.entry(s.id).or_insert_with(Instant::now);
            let membership = m.membership_config.membership().clone();
            let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
            let learners: BTreeSet<NodeId> = membership.learner_ids().collect();
            let joining = book.iter().find(|(nid, (_, at))| {
                **nid != 0 && !voters.contains(nid) && !learners.contains(nid) && at.elapsed() < Duration::from_secs(2)
            });
            if let Some((nid, (addr, _))) = joining {
                let _ = raft.add_learner(*nid, BasicNode::new(addr.clone()), false).await;
                continue;
            }
            let last = m.last_log_index.unwrap_or(0);
            let caught_up = |nid: &NodeId| {
                m.replication
                    .as_ref()
                    .and_then(|r| r.get(nid).cloned().flatten())
                    .is_some_and(|matched| matched.index + CAUGHT_UP_SLACK >= last)
            };
            let Some(learner) = learners.iter().copied().find(|l| caught_up(l)) else { continue };
            if voters.len() < replicas {
                let mut next = voters.clone();
                next.insert(learner);
                let _ = raft.change_membership(next, true).await;
                continue;
            }
            let baseline = since.max(started);
            let dead = voters
                .iter()
                .copied()
                .find(|v| *v != id && book.get(v).map_or(baseline, |(_, at)| (*at).max(baseline)).elapsed() > grace);
            if let Some(dead) = dead {
                let mut next = voters.clone();
                next.remove(&dead);
                next.insert(learner);
                let _ = raft.change_membership(next, false).await;
            }
        }
    }
}

const FOLLOW_WINDOW: Duration = Duration::from_secs(2);
const FOLLOW_MIN_WRITES: u64 = 20;
const FOLLOW_SHARE: f64 = 0.6;

fn leader_counts(shards: &[Arc<Shard>]) -> BTreeMap<NodeId, usize> {
    let mut counts = BTreeMap::new();
    for s in shards {
        if let Some(l) = s.raft.metrics().borrow_watched().current_leader {
            *counts.entry(l).or_default() += 1;
        }
    }
    counts
}

const MAX_TRANSFERS_PER_TICK: usize = 4;

fn caught_up(
    m: &openraft::RaftMetrics<quorium_net::raft::ProdTypes>,
    target: NodeId,
    since_last_write: Duration,
) -> bool {
    let matched = m.replication.as_ref().and_then(|r| r.get(&target).cloned().flatten()).map(|l| l.index);
    let last = m.last_log_index;
    quorium_core::ladder::transfer_ready(since_last_write, matched, last)
}

async fn rebalance(shards: &[Arc<Shard>], id: NodeId, peers: &PeerBook) {
    let mut counts = leader_counts(shards);
    let mut transfers = 0;
    for s in shards {
        if transfers >= MAX_TRANSFERS_PER_TICK {
            return;
        }
        let mine = counts.get(&id).copied().unwrap_or(0);
        if !s.is_leader(id) || !s.ladder.may_hand_off() {
            continue;
        }
        let since_last_write = s.activity.lock().unwrap().last_write.elapsed();
        if since_last_write < quorium_core::ladder::TRANSFER_IDLE {
            continue;
        }
        let m = s.raft.metrics().borrow_watched().clone();
        let voters: Vec<NodeId> = m.membership_config.membership().voter_ids().collect();
        let fair = shards.len().div_ceil(voters.len().max(1));
        if mine <= fair {
            return;
        }
        let book = peers.lock().unwrap().clone();
        let live = |v: &NodeId| book.get(v).is_some_and(|(_, at)| at.elapsed() < Duration::from_secs(2));
        let target = voters
            .iter()
            .copied()
            .filter(|v| *v != id && live(v))
            .min_by_key(|v| (counts.get(v).copied().unwrap_or(0), *v));
        if let Some(target) = target
            && counts.get(&target).copied().unwrap_or(0) + 1 < mine
            && caught_up(&m, target, since_last_write)
        {
            s.wake().await;
            tracing::info!(shard = s.id, from = id, to = target, "balancing leadership");
            let _ = s.raft.trigger().transfer_leader(target).await;
            *counts.entry(target).or_default() += 1;
            *counts.entry(id).or_default() -= 1;
            transfers += 1;
        }
    }
}

async fn housekeeping(
    shards: Arc<Vec<Arc<Shard>>>,
    id: NodeId,
    datacenter: String,
    quiesce_after: Option<Duration>,
    peers: PeerBook,
) {
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        rebalance(&shards, id, &peers).await;
        for s in shards.iter() {
            if !s.is_leader(id) {
                let mut a = s.activity.lock().unwrap();
                a.writes.clear();
                a.window_start = Instant::now();
                continue;
            }
            let (quiesce, transfer) = {
                let mut a = s.activity.lock().unwrap();
                let quiesce = !a.quiesced && quiesce_after.is_some_and(|q| a.last_write.elapsed() > q);
                if quiesce {
                    a.quiesced = true;
                }
                let mut transfer = None;
                if a.window_start.elapsed() > FOLLOW_WINDOW {
                    let total: u64 = a.writes.values().sum();
                    if let Some((dc, n)) = a.writes.iter().max_by_key(|(_, n)| **n)
                        && total >= FOLLOW_MIN_WRITES
                        && *dc != datacenter
                        && (*n as f64) >= FOLLOW_SHARE * total as f64
                    {
                        transfer = Some(dc.clone());
                    }
                    a.writes.clear();
                    a.window_start = Instant::now();
                }
                if !a.quiesced {
                    s.raft.runtime_config().heartbeat(true);
                }
                (quiesce, transfer)
            };
            if quiesce {
                s.raft.runtime_config().heartbeat(false);
                s.ladder.set_quiesced(true);
            }
            if let Some(dc) = transfer
                && s.ladder.may_hand_off()
            {
                let m = s.raft.metrics().borrow_watched().clone();
                let target = m
                    .membership_config
                    .membership()
                    .voter_ids()
                    .find(|v| *v != id && s.ladder.datacenter_of(*v).as_deref() == Some(dc.as_str()));
                let since_last_write = s.activity.lock().unwrap().last_write.elapsed();
                if let Some(target) =
                    target.filter(|t| caught_up(&m, *t, since_last_write.max(quorium_core::ladder::TRANSFER_IDLE)))
                {
                    tracing::info!(shard = s.id, from = id, to = target, datacenter = %dc, "following the writer");
                    let _ = s.raft.trigger().transfer_leader(target).await;
                }
            }
        }
    }
}
