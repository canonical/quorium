use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use openraft::ReadPolicy;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{ClientWriteError, RaftError};
use openraft::storage::Snapshot;
use openraft_rt_tokio::TokioRuntime;
use quorium_core::driver::Ladder;
use quorium_core::kv::Command;
use quorium_core::types::NodeId;
use quorium_net::raft::{ProdTypes, RaftService, Request, Response, ServiceFuture};
use quorium_store::mem::KvStateMachine;

pub(crate) type NodeRaft = openraft::Raft<ProdTypes, KvStateMachine>;

pub(crate) type PeerBook = Arc<Mutex<BTreeMap<NodeId, (String, Instant)>>>;

pub(crate) struct Activity {
    pub(crate) last_write: Instant,
    pub(crate) quiesced: bool,
    pub(crate) writes: BTreeMap<String, u64>,
    pub(crate) window_start: Instant,
    load_started: Instant,
    load_writes: u64,
    load_rate: u64,
    samples: std::collections::VecDeque<u64>,
}

const LOAD_SAMPLES: usize = 512;

impl Activity {
    fn roll_load(&mut self) {
        let elapsed = self.load_started.elapsed();
        if elapsed >= std::time::Duration::from_secs(1) {
            self.load_rate = self.load_writes * 1000 / (elapsed.as_millis() as u64).max(1);
            self.load_writes = 0;
            self.load_started = Instant::now();
        }
    }
}

pub(crate) struct Shard {
    pub(crate) id: u64,
    pub(crate) raft: NodeRaft,
    pub(crate) ladder: Ladder<TokioRuntime>,
    pub(crate) activity: Mutex<Activity>,
}

impl Shard {
    pub(crate) fn new(id: u64, raft: NodeRaft, ladder: Ladder<TokioRuntime>) -> Shard {
        let now = Instant::now();
        Shard {
            id,
            raft,
            ladder,
            activity: Mutex::new(Activity {
                last_write: now,
                quiesced: false,
                writes: BTreeMap::new(),
                window_start: now,
                load_started: now,
                load_writes: 0,
                load_rate: 0,
                samples: std::collections::VecDeque::with_capacity(LOAD_SAMPLES),
            }),
        }
    }

    pub(crate) async fn wake(&self) {
        let was_quiesced = {
            let mut a = self.activity.lock().unwrap();
            a.last_write = Instant::now();
            std::mem::replace(&mut a.quiesced, false)
        };
        if was_quiesced {
            self.raft.runtime_config().heartbeat(true);
            self.ladder.set_quiesced(false);
            let _ = self.raft.trigger().heartbeat().await;
        }
    }

    pub(crate) fn record_write(&self, origin: Option<String>, position: Option<u64>) {
        let mut a = self.activity.lock().unwrap();
        a.last_write = Instant::now();
        if let Some(dc) = origin {
            *a.writes.entry(dc).or_default() += 1;
        }
        a.roll_load();
        a.load_writes += 1;
        if let Some(p) = position {
            if a.samples.len() == LOAD_SAMPLES {
                a.samples.pop_front();
            }
            a.samples.push_back(p);
        }
    }

    pub(crate) fn load(&self) -> (u64, Vec<u64>) {
        let mut a = self.activity.lock().unwrap();
        a.roll_load();
        (a.load_rate, a.samples.iter().copied().collect())
    }

    pub(crate) fn is_leader(&self, me: NodeId) -> bool {
        self.raft.metrics().borrow_watched().current_leader == Some(me)
    }
}

pub(crate) fn is_initialized(raft: &NodeRaft) -> bool {
    raft.metrics().borrow_watched().membership_config.membership().voter_ids().next().is_some()
}

pub(crate) type MethodBook = Arc<Mutex<BTreeMap<NodeId, std::collections::BTreeSet<(String, u32)>>>>;

pub(crate) type RoutingCache = Arc<std::sync::RwLock<quorium_core::kv::RoutingTable>>;

pub(crate) struct NodeService {
    pub(crate) routing: RoutingCache,
    pub(crate) methods: MethodBook,
    pub(crate) escrow: Arc<crate::escrow::EscrowBook>,
    pub(crate) id: NodeId,
    pub(crate) peers: PeerBook,
    pub(crate) shards: Arc<Vec<Arc<Shard>>>,
    pub(crate) datacenter: String,
    pub(crate) pings: AtomicU64,
}

fn failed(e: impl std::fmt::Display) -> Response {
    Response::Failed(e.to_string())
}

const LEASE_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

pub(crate) async fn local_write(shard: &Shard, cmd: Command, origin: Option<String>) -> Response {
    shard.wake().await;
    let position = cmd.position();
    let deadline = Instant::now() + LEASE_WAIT;
    loop {
        match shard.raft.client_write(cmd.clone()).await {
            Ok(resp) if resp.data == quorium_core::kv::Reply::WrongShard => return Response::WrongShard,
            Ok(resp) => {
                shard.record_write(origin, position);
                return Response::Written(resp.data);
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f)))
                if f.reason == openraft::error::ForwardReason::LeaseExpired && Instant::now() < deadline =>
            {
                let _ = shard.raft.trigger().heartbeat().await;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                return Response::NotLeader { leader: f.leader_id, addr: f.leader_node.map(|n| n.addr) };
            }
            Err(e) => return failed(e),
        }
    }
}

async fn linearizable(raft: &NodeRaft) -> Option<Response> {
    if raft.ensure_linearizable(ReadPolicy::LeaseRead).await.is_ok() {
        return None;
    }
    match raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
        Ok(_) => None,
        Err(e) => Some(match e.forward_to_leader() {
            Some(f) => Response::NotLeader { leader: f.leader_id, addr: f.leader_node.clone().map(|n| n.addr) },
            None => failed(e),
        }),
    }
}

pub(crate) async fn owned_read<T: Send + 'static>(
    raft: &NodeRaft,
    position: Option<u64>,
    read: impl FnOnce(&quorium_core::kv::KvState) -> T + Send + 'static,
) -> Result<T, Response> {
    if let Some(refused) = linearizable(raft).await {
        return Err(refused);
    }
    let outcome = raft
        .with_state_machine(move |sm: &mut KvStateMachine| {
            let v = sm.read(|s| if position.is_some_and(|p| !s.owns(p)) { None } else { Some(read(s)) });
            Box::pin(async move { v })
        })
        .await;
    match outcome {
        Ok(Some(v)) => Ok(v),
        Ok(None) => Err(Response::WrongShard),
        Err(e) => Err(failed(e)),
    }
}

pub(crate) async fn local_read_entity(
    raft: &NodeRaft,
    type_name: String,
    key: Vec<u8>,
    locality: Option<String>,
) -> Response {
    let position = quorium_core::shard::entity_position(&type_name, &key, locality.as_deref());
    owned_read(raft, Some(position), move |s| s.entity(&type_name, &key).map(|(ver, b)| (ver, b.to_vec())))
        .await
        .map_or_else(|r| r, Response::Entity)
}

pub(crate) async fn local_read_routing(raft: &NodeRaft) -> Response {
    owned_read(raft, None, |s| s.routing().cloned()).await.map_or_else(|r| r, Response::Routing)
}

pub(crate) async fn local_read_escrow(raft: &NodeRaft, name: String) -> Response {
    let position = quorium_core::shard::key_hash(name.as_bytes());
    owned_read(raft, Some(position), move |s| s.escrow(&name).cloned()).await.map_or_else(|r| r, Response::Escrow)
}

pub(crate) async fn local_export(raft: &NodeRaft) -> Response {
    if let Some(refused) = linearizable(raft).await {
        return refused;
    }
    match raft
        .with_state_machine(|sm: &mut KvStateMachine| {
            let v = sm.read(|s| s.to_bytes());
            Box::pin(async move { v })
        })
        .await
    {
        Ok(v) => Response::ShardState(v),
        Err(e) => failed(e),
    }
}

pub(crate) async fn local_read(raft: &NodeRaft, key: String) -> Response {
    let position = quorium_core::shard::key_hash(key.as_bytes());
    owned_read(raft, Some(position), move |s| s.get(&key).cloned()).await.map_or_else(|r| r, Response::Value)
}

pub(crate) fn position_of(request: &Request) -> Option<u64> {
    match request {
        Request::Write(cmd) => cmd.position(),
        Request::Read { key } => Some(quorium_core::shard::key_hash(key.as_bytes())),
        Request::ReadEntity { type_name, key, locality } => {
            Some(quorium_core::shard::entity_position(type_name, key, locality.as_deref()))
        }
        Request::ReadEscrow { name } => Some(quorium_core::shard::key_hash(name.as_bytes())),
        _ => None,
    }
}

pub(crate) fn group_for(request: &Request, routing: &RoutingCache, groups: u64) -> Option<u64> {
    if let Some(position) = position_of(request) {
        return Some(routing.read().unwrap().group_of(position));
    }
    match request {
        Request::Write(cmd) => Some(cmd.shard(groups)),
        Request::ExportShard { shard } => Some(*shard),
        Request::ReadRouting => Some(0),
        _ => None,
    }
}

impl NodeService {
    pub(crate) fn pong(&self) -> Response {
        Response::Pong {
            id: self.id,
            logs: self.shards.iter().map(|s| (s.id, s.ladder.ping())).collect(),
            quiesced: quiesced_shards(&self.shards, self.id),
            datacenter: self.datacenter.clone(),
            initialized: self.shards.first().is_some_and(|s| is_initialized(&s.raft)),
            methods: self
                .methods
                .lock()
                .unwrap()
                .get(&self.id)
                .map(|m| m.iter().cloned().collect())
                .unwrap_or_default(),
        }
    }
}

pub(crate) fn quiesced_shards(shards: &[Arc<Shard>], me: NodeId) -> Vec<u64> {
    shards.iter().filter(|s| s.activity.lock().unwrap().quiesced && s.is_leader(me)).map(|s| s.id).collect()
}

pub(crate) fn observe_peer_ping(
    shards: &[Arc<Shard>],
    from: NodeId,
    datacenter: &str,
    logs: &[(u64, quorium_core::ladder::LogPosition)],
    quiesced: &[u64],
) {
    for s in shards {
        s.ladder.set_datacenter(from, datacenter);
    }
    for (shard, log) in logs {
        if let Some(s) = shards.get(*shard as usize) {
            s.ladder.observe_ping(from, *log);
        }
    }
    for shard in quiesced {
        if let Some(s) = shards.get(*shard as usize)
            && s.raft.metrics().borrow_watched().current_leader == Some(from)
        {
            s.ladder.set_quiesced(true);
        }
    }
}

impl RaftService for NodeService {
    fn handle(&self, from: NodeId, shard: u64, request: Request) -> ServiceFuture<'_> {
        Box::pin(async move {
            let count = self.shards.len() as u64;
            let shard = if from == 0 { group_for(&request, &self.routing, count).unwrap_or(shard) } else { shard };
            let Some(s) = self.shards.get(shard as usize).cloned() else {
                return failed(format!("no shard {shard} on this node"));
            };
            s.ladder.observe_peer(from);
            match request {
                Request::Append(r) => {
                    s.ladder.observe_append(&r.vote);
                    s.raft.append_entries(r).await.map_or_else(failed, Response::Append)
                }
                Request::Vote(r) => {
                    s.ladder.observe_pre_vote(&r);
                    s.raft.vote(r).await.map_or_else(failed, Response::Vote)
                }
                Request::PreVote(r) => {
                    s.ladder.observe_pre_vote(&r);
                    s.raft.pre_vote(r).await.map_or_else(failed, Response::Vote)
                }
                Request::Snapshot { vote, meta, data } => s
                    .raft
                    .install_full_snapshot(vote, Snapshot { meta, snapshot: data })
                    .await
                    .map_or_else(failed, Response::Snapshot),
                Request::TransferLeader(r) => {
                    s.raft.handle_transfer_leader(r).await.map_or_else(failed, Response::TransferLeader)
                }
                Request::Ping { logs, quiesced, datacenter, addr, methods, .. } => {
                    self.methods.lock().unwrap().insert(from, methods.into_iter().collect());
                    self.pings.fetch_add(1, Ordering::Relaxed);
                    observe_peer_ping(&self.shards, from, &datacenter, &logs, &quiesced);
                    self.peers.lock().unwrap().insert(from, (addr, Instant::now()));
                    self.pong()
                }
                Request::Write(cmd) => {
                    let origin = if from == 0 { None } else { s.ladder.datacenter_of(from) };
                    local_write(&s, cmd, origin).await
                }
                Request::Read { key } => {
                    s.wake().await;
                    local_read(&s.raft, key).await
                }
                Request::ReadEntity { type_name, key, locality } => {
                    s.wake().await;
                    local_read_entity(&s.raft, type_name, key, locality).await
                }
                Request::ReadRouting => local_read_routing(&s.raft).await,
                Request::ReadLoad if s.is_leader(self.id) => {
                    let (per_second, samples) = s.load();
                    Response::Load { per_second, samples }
                }
                Request::ReadLoad => {
                    Response::NotLeader { leader: s.raft.metrics().borrow_watched().current_leader, addr: None }
                }
                Request::ReadEscrow { name } => {
                    s.wake().await;
                    local_read_escrow(&s.raft, name).await
                }
                Request::ExportShard { .. } => local_export(&s.raft).await,
                Request::EscrowSale { owner, name, units } => {
                    self.escrow.hold_for_peer(owner, name, units, false);
                    Response::Recorded
                }
                Request::EscrowSettled { owner, name, units } => {
                    self.escrow.hold_for_peer(owner, name, units, true);
                    Response::Recorded
                }
                Request::EscrowSold { owner, name, units, through } => {
                    match self.escrow.record_copy(owner, name, units, through).await {
                        Ok(true) => Response::Recorded,
                        Ok(false) => failed(crate::escrow::FENCED),
                        Err(e) => failed(e),
                    }
                }
            }
        })
    }
}
