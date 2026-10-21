use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use quorium_core::kv::{Command, KvOp, Reply};
use quorium_net::raft::{Request, Response, TcpNetwork};
use quorium_net::tls::TlsConfig;

use crate::node::ClientError;

const MAX_HEDGES: usize = 3;
const HEDGE_LIFETIME: u32 = 8;
const RETRY_PAUSE: Duration = Duration::from_millis(20);
const MIN_HEDGE: Duration = Duration::from_millis(50);

pub struct Client {
    seeds: Vec<SocketAddr>,
    network: TcpNetwork,
    tls: Option<TlsConfig>,
    leader: Mutex<Option<String>>,
    next_seed: AtomicUsize,
    id: u64,
    sequencer: quorium_core::session::Sequencer,
    timeout: Duration,
    attempt: Duration,
    latency: Mutex<Option<Duration>>,
}

impl Client {
    pub fn new(seeds: Vec<SocketAddr>, tls: Option<TlsConfig>) -> Client {
        Client {
            seeds,
            network: TcpNetwork::with_protocol(0, tls.clone(), crate::Transport::default()),
            tls,
            leader: Mutex::new(None),
            next_seed: AtomicUsize::new(0),
            id: rand::random::<u64>() >> 1,
            sequencer: quorium_core::session::Sequencer::default(),
            timeout: Duration::from_secs(10),
            attempt: Duration::from_secs(1),
            latency: Mutex::new(None),
        }
    }

    pub fn with_transport(mut self, transport: crate::Transport) -> Client {
        self.network = TcpNetwork::with_protocol(0, self.tls.clone(), transport);
        self
    }

    pub fn with_timeouts(mut self, total: Duration, attempt: Duration) -> Client {
        self.timeout = total;
        self.attempt = attempt;
        self
    }

    fn target(&self) -> Option<String> {
        if let Some(l) = self.leader.lock().unwrap().clone() {
            return Some(l);
        }
        if self.seeds.is_empty() {
            return None;
        }
        let i = self.next_seed.fetch_add(1, Ordering::Relaxed) % self.seeds.len();
        Some(self.seeds[i].to_string())
    }

    fn hedge_delay(&self) -> Duration {
        match *self.latency.lock().unwrap() {
            Some(l) => (l * 2).clamp(MIN_HEDGE, self.attempt.max(MIN_HEDGE)),
            None => self.attempt,
        }
    }

    fn observe_latency(&self, sample: Duration) {
        let mut latency = self.latency.lock().unwrap();
        *latency = Some(match *latency {
            Some(l) => (l * 7 + sample) / 8,
            None => sample,
        });
    }

    fn untried_target(&self, waiting: &std::collections::HashSet<String>) -> Option<String> {
        for _ in 0..=self.seeds.len() {
            let addr = self.target()?;
            if !waiting.contains(&addr) {
                return Some(addr);
            }
            *self.leader.lock().unwrap() = None;
        }
        None
    }

    async fn route(&self, make: impl Fn() -> Request) -> Result<Response, ClientError> {
        let deadline = Instant::now() + self.timeout;
        let mut last = String::from("no reachable node");
        let mut inflight = tokio::task::JoinSet::new();
        let mut waiting = std::collections::HashSet::new();
        let mut next_attempt = Instant::now();
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(ClientError::Unavailable(last));
            }
            if now >= next_attempt && inflight.len() < MAX_HEDGES {
                if self.seeds.is_empty() {
                    return Err(ClientError::Unavailable("no seeds".into()));
                }
                if let Some(addr) = self.untried_target(&waiting) {
                    waiting.insert(addr.clone());
                    let network = self.network.clone();
                    let request = make();
                    let lifetime = (self.attempt * HEDGE_LIFETIME).min(deadline - now);
                    inflight.spawn(async move {
                        let sent = Instant::now();
                        let outcome = network.request_addr(&addr, request, lifetime).await;
                        (addr, sent.elapsed(), outcome)
                    });
                }
                next_attempt = now + self.hedge_delay();
            }
            let wait = next_attempt.max(now).min(deadline);
            let finished = tokio::select! {
                done = inflight.join_next(), if !inflight.is_empty() => done,
                _ = tokio::time::sleep_until(wait.into()) => {
                    *self.leader.lock().unwrap() = None;
                    continue;
                }
            };
            let Some(Ok((addr, took, outcome))) = finished else { continue };
            waiting.remove(&addr);
            tracing::debug!(%addr, outcome = ?outcome.as_ref().map(|r| format!("{r:?}").chars().take(80).collect::<String>()), "client attempt");
            match outcome {
                Ok(Response::NotLeader { addr: Some(leader), .. }) => {
                    *self.leader.lock().unwrap() = Some(leader);
                    next_attempt = Instant::now();
                }
                Ok(Response::NotLeader { .. }) => {
                    *self.leader.lock().unwrap() = None;
                    next_attempt = Instant::now() + RETRY_PAUSE;
                }
                Ok(Response::Failed(e)) => {
                    *self.leader.lock().unwrap() = None;
                    last = e;
                    next_attempt = Instant::now() + RETRY_PAUSE;
                }
                Ok(done) => {
                    self.observe_latency(took);
                    *self.leader.lock().unwrap() = Some(addr);
                    return Ok(done);
                }
                Err(e) => {
                    *self.leader.lock().unwrap() = None;
                    last = e.to_string();
                    next_attempt = next_attempt.min(Instant::now() + RETRY_PAUSE);
                }
            }
        }
    }

    pub async fn write(&self, key: &str, value: Vec<u8>) -> Result<Reply, ClientError> {
        let ticket = self.sequencer.issue();
        let cmd = Command::new(self.id, ticket.seq(), KvOp::Put { key: key.into(), value })
            .acknowledging_below(ticket.ack_below());
        match self.route(|| Request::Write(cmd.clone())).await? {
            Response::Written(Reply::Stale) => Err(ClientError::Unavailable("the write was refused as stale".into())),
            Response::Written(reply) => Ok(reply),
            other => Err(ClientError::Unavailable(format!("{other:?}"))),
        }
    }

    pub async fn read_escrow(&self, name: &str) -> Result<Option<quorium_core::kv::EscrowView>, ClientError> {
        match self.route(|| Request::ReadEscrow { name: name.into() }).await? {
            Response::Escrow(view) => Ok(view),
            other => Err(ClientError::Unavailable(format!("{other:?}"))),
        }
    }

    pub async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        match self.route(|| Request::Read { key: key.into() }).await? {
            Response::Value(v) => Ok(v),
            other => Err(ClientError::Unavailable(format!("{other:?}"))),
        }
    }
}
