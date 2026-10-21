use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};

use quorium_core::kv::{EscrowView, KvOp, Reply};
use quorium_sim::cluster::{ClientError as SimError, ClusterConfig};
use tempfile::TempDir;

use crate::entity::{Backend, Entity, Handle, State};
use crate::{Discovery, Error, EscrowCounter, Quorium};

pub struct Cluster {
    dirs: Vec<TempDir>,
    addrs: Vec<SocketAddr>,
    nodes: Vec<Option<Quorium>>,
}

const PORT_FLOOR: u16 = 10_000;
const PORT_CEILING: u16 = 32_768;

static NEXT_PORT: std::sync::Mutex<Option<u16>> = std::sync::Mutex::new(None);

fn usable(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    TcpListener::bind(addr).is_ok() && std::net::UdpSocket::bind(addr).is_ok()
}

pub fn free_addrs(n: usize) -> Vec<SocketAddr> {
    let span = PORT_CEILING - PORT_FLOOR;
    let mut next = NEXT_PORT.lock().unwrap();
    let mut port = next.unwrap_or_else(|| PORT_FLOOR + (std::process::id() % 64) as u16 * (span / 64));
    let mut out = Vec::with_capacity(n);
    for _ in 0..span {
        if out.len() == n {
            break;
        }
        if usable(port) {
            out.push(SocketAddr::from(([127, 0, 0, 1], port)));
        }
        port = if port + 1 >= PORT_CEILING { PORT_FLOOR } else { port + 1 };
    }
    *next = Some(port);
    assert_eq!(out.len(), n, "no free local ports below the ephemeral range");
    out
}

const DATACENTERS: [&str; 5] = ["dc-a", "dc-b", "dc-c", "dc-a", "dc-b"];

impl Cluster {
    pub async fn start(size: usize) -> Cluster {
        assert!(matches!(size, 1 | 3 | 5), "test clusters have 1, 3 or 5 nodes");
        let dirs: Vec<TempDir> = (0..size).map(|_| tempfile::tempdir().expect("temporary directory")).collect();
        let addrs = free_addrs(size);
        let mut cluster = Cluster { dirs, addrs, nodes: (0..size).map(|_| None).collect() };
        for i in 0..size {
            cluster.restart(i).await;
        }
        cluster
    }

    pub async fn restart(&mut self, i: usize) {
        if self.nodes[i].is_some() {
            return;
        }
        let mut builder = Quorium::builder()
            .data_dir(self.dirs[i].path())
            .bind(self.addrs[i])
            .advertise(self.addrs[i])
            .datacenter(DATACENTERS[i % DATACENTERS.len()])
            .insecure_plaintext();
        builder = if self.addrs.len() == 1 {
            builder.single_node_dev_mode()
        } else {
            builder.discovery(Discovery::Static(self.addrs.clone())).replicas(self.addrs.len())
        };
        self.nodes[i] = Some(builder.start().await.expect("test node starts"));
    }

    pub async fn kill(&mut self, i: usize) {
        if let Some(node) = self.nodes[i].take() {
            node.shutdown().await;
        }
    }

    pub fn node(&self, i: usize) -> &Quorium {
        self.nodes[i].as_ref().expect("node is running")
    }

    pub fn running(&self) -> usize {
        self.nodes.iter().flatten().count()
    }

    pub fn any_running(&self) -> usize {
        self.nodes.iter().position(Option::is_some).expect("at least one node is running")
    }

    pub async fn wait_for_leader(&self, limit: Duration) -> Option<usize> {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            let leaders: Vec<Option<u64>> = self.nodes.iter().flatten().map(|n| n.status().leader).collect();
            if let Some(Some(id)) = leaders.first()
                && leaders.iter().all(|l| *l == Some(*id))
                && let Some(i) = self.nodes.iter().position(|n| n.as_ref().is_some_and(|n| n.status().id == *id))
            {
                return Some(i);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }

    pub async fn shutdown(mut self) {
        for i in 0..self.nodes.len() {
            self.kill(i).await;
        }
    }
}

const SIM_LIMIT: Duration = Duration::from_secs(10);

struct SimInner {
    cluster: quorium_sim::cluster::SimCluster,
    client: u64,
    seq: u64,
}

type Job = Box<dyn FnOnce(&mut SimInner) + Send>;

#[derive(Clone)]
pub struct SimCluster {
    jobs: std::sync::mpsc::Sender<Job>,
}

fn sim_error(e: SimError) -> Error<Infallible> {
    match e {
        SimError::Timeout => Error::Timeout,
        SimError::NoLeader | SimError::Rejected(_) => Error::Unavailable,
    }
}

impl SimCluster {
    pub fn start(seed: u64, size: usize) -> SimCluster {
        assert!(size > 0, "a simulated cluster needs at least one node");
        let (jobs, inbox) = std::sync::mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name(format!("quorium-sim-{seed}"))
            .spawn(move || {
                let nodes: Vec<(u64, &str)> =
                    (1..=size).map(|i| (i as u64, DATACENTERS[(i - 1) % DATACENTERS.len()])).collect();
                let mut cluster = quorium_sim::cluster::SimCluster::new(ClusterConfig::new(seed, &nodes));
                let deadline = cluster.now() + SIM_LIMIT;
                while cluster.leader().is_none() && cluster.now() < deadline {
                    cluster.run_for(Duration::from_millis(10));
                }
                let mut inner = SimInner { cluster, client: seed | 1 << 62, seq: 0 };
                while let Ok(job) = inbox.recv() {
                    job(&mut inner);
                }
            })
            .expect("simulator thread starts");
        SimCluster { jobs }
    }

    fn with<T: Send + 'static>(&self, f: impl FnOnce(&mut SimInner) -> T + Send + 'static) -> T {
        let (tx, rx) = std::sync::mpsc::channel();
        self.jobs
            .send(Box::new(move |s: &mut SimInner| {
                let _ = tx.send(f(s));
            }))
            .expect("simulator thread is running");
        rx.recv().expect("simulator thread answered")
    }

    pub fn entity<T: State>(&self, key: impl Into<T::Key>) -> T::Handle {
        T::Handle::from_entity(Entity::new(Backend::Sim(self.clone()), &key.into()))
    }

    pub fn global<T: State<Key = ()>>(&self) -> T::Handle {
        self.entity::<T>(())
    }

    pub fn escrow(&self, name: &str) -> EscrowCounter {
        EscrowCounter::named(Backend::Sim(self.clone()), name)
    }

    pub fn nodes(&self) -> Vec<u64> {
        self.with(|s| s.cluster.node_ids())
    }

    pub fn leader(&self) -> Option<u64> {
        self.with(|s| s.cluster.leader())
    }

    pub fn now(&self) -> Duration {
        self.with(|s| s.cluster.now())
    }

    pub fn run_for(&self, d: Duration) {
        self.with(move |s| s.cluster.run_for(d));
    }

    pub fn crash(&self, node: u64) {
        self.with(move |s| s.cluster.crash(node));
    }

    pub fn restart(&self, node: u64) {
        self.with(move |s| s.cluster.restart(node));
    }

    pub fn partition(&self, left: &[u64], right: &[u64]) {
        let (left, right) = (left.to_vec(), right.to_vec());
        self.with(move |s| s.cluster.partition(&left, &right));
    }

    pub fn isolate(&self, node: u64) {
        self.with(move |s| s.cluster.isolate(node));
    }

    pub fn heal(&self) {
        self.with(|s| s.cluster.heal());
    }

    pub fn violations(&self) -> Vec<String> {
        self.with(|s| s.cluster.violations().iter().map(|v| format!("{v:?}")).collect())
    }

    pub(crate) fn submit(&self, op: KvOp) -> Result<Reply, Error<Infallible>> {
        self.with(move |s| {
            s.seq += 1;
            let (client, seq) = (s.client, s.seq);
            s.cluster.submit(client, seq, op, SIM_LIMIT).map_err(sim_error)
        })
    }

    pub(crate) fn entity_state(
        &self,
        type_name: &str,
        key: &[u8],
    ) -> Result<Option<(u32, Vec<u8>)>, Error<Infallible>> {
        let (type_name, key) = (type_name.to_string(), key.to_vec());
        self.with(move |s| {
            s.cluster
                .read_state(move |state| state.entity(&type_name, &key).map(|(v, b)| (v, b.to_vec())), SIM_LIMIT)
                .map_err(sim_error)
        })
    }

    pub(crate) fn escrow_view(&self, name: &str) -> Result<EscrowView, Error<Infallible>> {
        let name = name.to_string();
        self.with(move |s| {
            s.cluster
                .read_state(move |state| state.escrow(&name).cloned().unwrap_or_default(), SIM_LIMIT)
                .map_err(sim_error)
        })
    }
}
