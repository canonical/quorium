use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::alias::SnapshotOf;
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::impls::BasicNode;
use openraft::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest, TransferLeaderResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft_rt_tokio::TokioRuntime;
use quorium_core::kv::{Command, Reply};
use quorium_core::ladder::LogPosition;
use quorium_core::types::{CommittedLeaderId, KvVote, NodeId, Types};
use serde::{Deserialize, Serialize};

use crate::codec::{CodecError, Envelope, FORMAT_VERSION};
use crate::tls::TlsConfig;
use crate::transport::{Listener, Peer, Protocol, RpcError};

pub type ProdTypes = Types<TokioRuntime>;
pub type KvSnapshotMeta = openraft::SnapshotMeta<CommittedLeaderId, NodeId, BasicNode>;

const RAFT_TYPE: &str = "quorium.raft";

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Append(AppendEntriesRequest<ProdTypes>),
    Vote(VoteRequest<ProdTypes>),
    PreVote(VoteRequest<ProdTypes>),
    Snapshot {
        vote: KvVote,
        meta: KvSnapshotMeta,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    TransferLeader(TransferLeaderRequest<ProdTypes>),
    Ping {
        logs: Vec<(u64, LogPosition)>,
        quiesced: Vec<u64>,
        datacenter: String,
        addr: String,
        initialized: bool,
        methods: Vec<(String, u32)>,
    },
    Write(Command),
    Read {
        key: String,
    },
    ReadEntity {
        type_name: String,
        key: Vec<u8>,
        locality: Option<String>,
    },
    ReadEscrow {
        name: String,
    },
    EscrowSale {
        owner: NodeId,
        name: String,
        units: u64,
    },
    EscrowSettled {
        owner: NodeId,
        name: String,
        units: u64,
    },
    ExportShard {
        shard: u64,
    },
    ReadRouting,
    ReadLoad,
    EscrowSold {
        owner: NodeId,
        name: String,
        units: u64,
        through: u64,
    },
}

impl Request {
    fn method(&self) -> u32 {
        match self {
            Request::Append(_) => 1,
            Request::Vote(_) => 2,
            Request::PreVote(_) => 3,
            Request::Snapshot { .. } => 4,
            Request::TransferLeader(_) => 5,
            Request::Ping { .. } => 6,
            Request::Write(_) => 7,
            Request::Read { .. } => 8,
            Request::ReadEntity { .. } => 9,
            Request::ReadEscrow { .. } => 10,
            Request::EscrowSale { .. } => 11,
            Request::EscrowSettled { .. } => 12,
            Request::ExportShard { .. } => 13,
            Request::ReadRouting => 14,
            Request::ReadLoad => 15,
            Request::EscrowSold { .. } => 16,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Append(AppendEntriesResponse<ProdTypes>),
    Vote(VoteResponse<ProdTypes>),
    Snapshot(SnapshotResponse<ProdTypes>),
    TransferLeader(TransferLeaderResponse<ProdTypes>),
    Pong {
        id: NodeId,
        logs: Vec<(u64, LogPosition)>,
        quiesced: Vec<u64>,
        datacenter: String,
        initialized: bool,
        methods: Vec<(String, u32)>,
    },
    Written(Reply),
    Value(#[serde(with = "serde_bytes")] Option<Vec<u8>>),
    Entity(Option<(u32, Vec<u8>)>),
    Escrow(Option<quorium_core::kv::EscrowView>),
    Recorded,
    ShardState(#[serde(with = "serde_bytes")] Vec<u8>),
    Routing(Option<quorium_core::kv::RoutingTable>),
    WrongShard,
    NotLeader {
        leader: Option<NodeId>,
        addr: Option<String>,
    },
    Failed(String),
    Load {
        per_second: u64,
        samples: Vec<u64>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub from: NodeId,
    pub shard: u64,
    pub request: Request,
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        Envelope {
            format: FORMAT_VERSION,
            type_name: RAFT_TYPE.into(),
            method: self.request.method(),
            state_version: 0,
            payload: postcard::to_allocvec(self).expect("raft messages always serialize"),
        }
        .encode()
    }

    pub fn decode(bytes: &[u8]) -> Result<Message, CodecError> {
        let envelope = Envelope::decode(bytes)?;
        if envelope.type_name != RAFT_TYPE {
            return Err(CodecError::Malformed);
        }
        let message: Message = postcard::from_bytes(&envelope.payload).map_err(|_| CodecError::Malformed)?;
        if message.request.method() != envelope.method {
            return Err(CodecError::Malformed);
        }
        Ok(message)
    }
}

fn encode_response(r: &Response) -> Vec<u8> {
    postcard::to_allocvec(r).expect("raft responses always serialize")
}

fn decode_response(bytes: &[u8]) -> Result<Response, CodecError> {
    postcard::from_bytes(bytes).map_err(|_| CodecError::Malformed)
}

pub type ServiceFuture<'a> = Pin<Box<dyn Future<Output = Response> + Send + 'a>>;

pub trait RaftService: Send + Sync + 'static {
    fn handle(&self, from: NodeId, shard: u64, request: Request) -> ServiceFuture<'_>;
}

pub async fn serve<S: RaftService>(addr: SocketAddr, tls: Option<TlsConfig>, service: Arc<S>) -> io::Result<Listener> {
    serve_with(Protocol::Tcp, addr, tls, service).await
}

pub async fn serve_with<S: RaftService>(
    protocol: Protocol,
    addr: SocketAddr,
    tls: Option<TlsConfig>,
    service: Arc<S>,
) -> io::Result<Listener> {
    Listener::bind(protocol, addr, tls, move |body: Vec<u8>| {
        let service = service.clone();
        async move {
            let message = Message::decode(&body).map_err(|e| e.to_string())?;
            Ok(encode_response(&service.handle(message.from, message.shard, message.request).await))
        }
    })
    .await
}

pub struct PeerClients {
    pub consensus: Arc<Peer>,
    pub snapshots: Arc<Peer>,
}

type PeerTable = Arc<Mutex<HashMap<NodeId, (String, Arc<PeerClients>)>>>;

#[derive(Clone)]
pub struct TcpNetwork {
    me: NodeId,
    shard: u64,
    tls: Option<TlsConfig>,
    protocol: Protocol,
    peers: PeerTable,
    by_addr: Arc<Mutex<HashMap<String, Arc<Peer>>>>,
    appends: Arc<std::sync::atomic::AtomicU64>,
}

impl TcpNetwork {
    pub fn new(me: NodeId, tls: Option<TlsConfig>) -> Self {
        Self::with_protocol(me, tls, Protocol::Tcp)
    }

    pub fn with_protocol(me: NodeId, tls: Option<TlsConfig>, protocol: Protocol) -> Self {
        Self { me, shard: 0, tls, protocol, peers: Arc::default(), by_addr: Arc::default(), appends: Arc::default() }
    }

    pub fn for_shard(&self, shard: u64) -> TcpNetwork {
        TcpNetwork { shard, ..self.clone() }
    }

    pub fn appends_sent(&self) -> u64 {
        self.appends.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn peer_at(&self, addr: &str) -> Option<Arc<Peer>> {
        self.by_addr.lock().unwrap().get(addr).cloned()
    }

    pub fn peer(&self, id: NodeId) -> Option<Arc<PeerClients>> {
        self.peers.lock().unwrap().get(&id).map(|(_, p)| p.clone())
    }

    fn clients(&self, id: NodeId, addr: &str) -> Result<Arc<PeerClients>, RpcError> {
        let mut peers = self.peers.lock().unwrap();
        if let Some((known, p)) = peers.get(&id)
            && known == addr
        {
            return Ok(p.clone());
        }
        let sock: SocketAddr = addr.parse().map_err(|e: std::net::AddrParseError| RpcError::Connect(e.to_string()))?;
        let tls = self.tls.clone();
        let consensus = self
            .by_addr
            .lock()
            .unwrap()
            .entry(addr.to_string())
            .or_insert_with(|| Arc::new(Peer::new(self.protocol, sock, tls.clone())))
            .clone();
        let p = Arc::new(PeerClients { consensus, snapshots: Arc::new(Peer::new(self.protocol, sock, tls)) });
        peers.insert(id, (addr.to_string(), p.clone()));
        Ok(p)
    }

    pub async fn request_addr(&self, addr: &str, request: Request, timeout: Duration) -> Result<Response, RpcError> {
        self.request_addr_shard(addr, self.shard, request, timeout).await
    }

    pub async fn request_addr_shard(
        &self,
        addr: &str,
        shard: u64,
        request: Request,
        timeout: Duration,
    ) -> Result<Response, RpcError> {
        let client = {
            let mut by_addr = self.by_addr.lock().unwrap();
            match by_addr.get(addr) {
                Some(c) => c.clone(),
                None => {
                    let sock: SocketAddr =
                        addr.parse().map_err(|e: std::net::AddrParseError| RpcError::Connect(e.to_string()))?;
                    let c = Arc::new(Peer::new(self.protocol, sock, self.tls.clone()));
                    by_addr.insert(addr.to_string(), c.clone());
                    c
                }
            }
        };
        call(&client, self.me, shard, request, timeout).await
    }

    pub async fn request(
        &self,
        to: NodeId,
        addr: &str,
        request: Request,
        timeout: Duration,
    ) -> Result<Response, RpcError> {
        let client = self.clients(to, addr)?.consensus.clone();
        call(&client, self.me, self.shard, request, timeout).await
    }
}

async fn call(
    client: &Peer,
    me: NodeId,
    shard: u64,
    request: Request,
    timeout: Duration,
) -> Result<Response, RpcError> {
    let body = Message { from: me, shard, request }.encode();
    let bytes = client.call(body, timeout).await?;
    decode_response(&bytes).map_err(RpcError::Codec)
}

pub struct PeerConn {
    me: NodeId,
    shard: u64,
    clients: Result<Arc<PeerClients>, RpcError>,
    appends: Arc<std::sync::atomic::AtomicU64>,
}

fn rpc_error(e: RpcError) -> RPCError<ProdTypes> {
    match e {
        RpcError::Connect(_) | RpcError::Closed => RPCError::Unreachable(Unreachable::new(&e)),
        other => RPCError::Network(NetworkError::new(&other)),
    }
}

fn unexpected(r: Response) -> RPCError<ProdTypes> {
    let e = io::Error::other(format!("unexpected response {r:?}"));
    RPCError::Network(NetworkError::new(&e))
}

impl PeerConn {
    async fn consensus(&self, request: Request, option: &RPCOption) -> Result<Response, RPCError<ProdTypes>> {
        let clients = self.clients.as_ref().map_err(|e| rpc_error(e.clone()))?;
        call(&clients.consensus, self.me, self.shard, request, option.hard_ttl().max(MIN_RPC_TTL))
            .await
            .map_err(rpc_error)
    }
}

impl RaftNetworkFactory<ProdTypes> for TcpNetwork {
    type Network = PeerConn;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> PeerConn {
        PeerConn {
            me: self.me,
            shard: self.shard,
            clients: self.clients(target, &node.addr),
            appends: self.appends.clone(),
        }
    }
}

pub const APPEND_BYTE_BUDGET: usize = 4 << 20;

pub const MIN_RPC_TTL: Duration = Duration::from_secs(1);

pub fn limit_append(
    mut rpc: AppendEntriesRequest<ProdTypes>,
    budget: usize,
) -> (AppendEntriesRequest<ProdTypes>, Option<quorium_core::types::LogId>) {
    let mut used = 0usize;
    let mut keep = 0usize;
    for entry in &rpc.entries {
        let size = postcard::serialize_with_flavor::<_, postcard::ser_flavors::Size, usize>(entry, Default::default())
            .unwrap_or(usize::MAX);
        if keep > 0 && used.saturating_add(size) > budget {
            break;
        }
        used = used.saturating_add(size);
        keep += 1;
    }
    if keep == rpc.entries.len() {
        return (rpc, None);
    }
    rpc.entries.truncate(keep);
    let last = rpc.entries.last().map(|e| e.log_id);
    (rpc, last)
}

impl RaftNetworkV2<ProdTypes> for PeerConn {
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ProdTypes>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<ProdTypes>, RPCError<ProdTypes>> {
        self.appends.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (rpc, cut) = limit_append(rpc, APPEND_BYTE_BUDGET);
        match (self.consensus(Request::Append(rpc), &option).await?, cut) {
            (Response::Append(AppendEntriesResponse::Success), Some(last)) => {
                Ok(AppendEntriesResponse::PartialSuccess(Some(last)))
            }
            (Response::Append(r), _) => Ok(r),
            (other, _) => Err(unexpected(other)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ProdTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ProdTypes>, RPCError<ProdTypes>> {
        match self.consensus(Request::Vote(rpc), &option).await? {
            Response::Vote(r) => Ok(r),
            other => Err(unexpected(other)),
        }
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<ProdTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ProdTypes>, RPCError<ProdTypes>> {
        match self.consensus(Request::PreVote(rpc), &option).await? {
            Response::Vote(r) => Ok(r),
            other => Err(unexpected(other)),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: KvVote,
        snapshot: SnapshotOf<ProdTypes, Vec<u8>>,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ProdTypes>, StreamingError<ProdTypes>> {
        let clients = self.clients.as_ref().map_err(|e| StreamingError::Unreachable(Unreachable::new(e)))?;
        let Snapshot { meta, snapshot } = snapshot;
        let request = Request::Snapshot { vote, meta, data: snapshot };
        match call(&clients.snapshots, self.me, self.shard, request, option.hard_ttl()).await {
            Ok(Response::Snapshot(r)) => Ok(r),
            Ok(other) => Err(StreamingError::Network(NetworkError::new(&io::Error::other(format!("{other:?}"))))),
            Err(e) => Err(StreamingError::Unreachable(Unreachable::new(&e))),
        }
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<ProdTypes>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<ProdTypes>, RPCError<ProdTypes>> {
        match self.consensus(Request::TransferLeader(req), &option).await? {
            Response::TransferLeader(r) => Ok(r),
            other => Err(unexpected(other)),
        }
    }
}
