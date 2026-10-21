use std::fmt;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use quorium_core::ladder::LadderConfig;
use quorium_net::tls::TlsConfig;

use crate::discovery::Discovery;
use crate::node::Quorium;

#[derive(Debug)]
pub enum StartError {
    MissingDataDir,
    MissingBind,
    TlsRequired,
    InvalidReplicas(usize),
    Io(io::Error),
    Storage(String),
    Raft(String),
    UnknownType(String),
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StartError::MissingDataDir => f.write_str("data_dir is required"),
            StartError::MissingBind => f.write_str("bind address is required"),
            StartError::TlsRequired => f.write_str("tls is required; call insecure_plaintext() to opt out explicitly"),
            StartError::InvalidReplicas(n) => write!(f, "replicas must be 3 or 5, got {n}"),
            StartError::Io(e) => write!(f, "io error: {e}"),
            StartError::Storage(e) => write!(f, "storage error: {e}"),
            StartError::Raft(e) => write!(f, "consensus error: {e}"),
            StartError::UnknownType(t) => write!(f, "stored state type {t} is not registered in this binary"),
        }
    }
}

impl std::error::Error for StartError {}

impl From<io::Error> for StartError {
    fn from(e: io::Error) -> Self {
        StartError::Io(e)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StateBackend {
    #[default]
    Redb,
    #[cfg(feature = "fjall")]
    Fjall,
}

#[derive(Debug, Clone)]
pub struct Builder {
    pub(crate) data_dir: Option<PathBuf>,
    pub(crate) bind: Option<SocketAddr>,
    pub(crate) advertise: Option<SocketAddr>,
    pub(crate) datacenter: String,
    pub(crate) discovery: Discovery,
    pub(crate) replicas: usize,
    pub(crate) tls: Option<TlsConfig>,
    pub(crate) plaintext: bool,
    pub(crate) single_node: bool,
    pub(crate) heartbeat: Duration,
    pub(crate) checkpoint_every: Option<u64>,
    pub(crate) transport: crate::Transport,
    pub(crate) state_backend: StateBackend,
    pub(crate) ping: Duration,
    pub(crate) ladder: LadderConfig,
    pub(crate) replacement_grace: Duration,
    pub(crate) shards: u64,
    pub(crate) quiesce_after: Option<Duration>,
    pub(crate) handoff_suspend: Duration,
    pub(crate) metrics_bind: Option<SocketAddr>,
    pub(crate) advertised_methods: Option<std::collections::BTreeSet<(String, u32)>>,
    pub(crate) rebalance: Option<(Duration, usize)>,
    pub(crate) load_split: Option<u64>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            data_dir: None,
            bind: None,
            advertise: None,
            datacenter: "default".into(),
            discovery: Discovery::None,
            replicas: 3,
            tls: None,
            plaintext: false,
            single_node: false,
            heartbeat: Duration::from_millis(50),
            checkpoint_every: None,
            transport: crate::Transport::default(),
            state_backend: StateBackend::default(),
            ping: Duration::from_millis(100),
            ladder: LadderConfig::default(),
            replacement_grace: Duration::from_secs(600),
            shards: 1,
            quiesce_after: None,
            handoff_suspend: Duration::from_secs(30),
            metrics_bind: None,
            advertised_methods: None,
            rebalance: None,
            load_split: None,
        }
    }
}

impl Builder {
    pub fn data_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.data_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.bind = Some(addr);
        self
    }

    pub fn advertise(mut self, addr: SocketAddr) -> Self {
        self.advertise = Some(addr);
        self
    }

    pub fn datacenter(mut self, dc: &str) -> Self {
        self.datacenter = dc.to_string();
        self
    }

    pub fn discovery(mut self, discovery: Discovery) -> Self {
        self.discovery = discovery;
        self
    }

    pub fn replicas(mut self, n: usize) -> Self {
        self.replicas = n;
        self
    }

    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn insecure_plaintext(mut self) -> Self {
        self.plaintext = true;
        self
    }

    pub fn single_node_dev_mode(mut self) -> Self {
        self.single_node = true;
        self
    }

    pub fn __test_advertise_methods(mut self, methods: std::collections::BTreeSet<(String, u32)>) -> Self {
        self.advertised_methods = Some(methods);
        self
    }

    pub fn split_by_load(mut self, min_writes_per_second: u64) -> Self {
        self.load_split = Some(min_writes_per_second);
        self
    }

    pub fn auto_rebalance(mut self, every: Duration, min_items: usize) -> Self {
        self.rebalance = Some((every, min_items));
        self
    }

    pub fn metrics_bind(mut self, addr: SocketAddr) -> Self {
        self.metrics_bind = Some(addr);
        self
    }

    pub fn shards(mut self, shards: u64) -> Self {
        self.shards = shards.max(1);
        self
    }

    pub fn quiesce_after(mut self, idle: Duration) -> Self {
        self.quiesce_after = Some(idle);
        self
    }

    pub fn handoff_suspend(mut self, suspend: Duration) -> Self {
        self.handoff_suspend = suspend;
        self
    }

    pub fn replacement_grace(mut self, grace: Duration) -> Self {
        self.replacement_grace = grace;
        self
    }

    pub fn state_backend(mut self, backend: StateBackend) -> Self {
        self.state_backend = backend;
        self
    }

    pub fn register<T: crate::private::Registered>(self) -> Self {
        quorium_core::entity::register(T::registration());
        self
    }

    pub fn transport(mut self, transport: crate::Transport) -> Self {
        self.transport = transport;
        self
    }

    pub fn checkpoint_every(mut self, entries: u64) -> Self {
        self.checkpoint_every = Some(entries.max(1));
        self
    }

    pub fn heartbeat(mut self, interval: Duration) -> Self {
        self.heartbeat = interval;
        self
    }

    pub(crate) fn validate(&self) -> Result<(PathBuf, SocketAddr), StartError> {
        let dir = self.data_dir.clone().ok_or(StartError::MissingDataDir)?;
        let bind = self.bind.ok_or(StartError::MissingBind)?;
        if self.tls.is_none() && !self.plaintext {
            return Err(StartError::TlsRequired);
        }
        if !self.single_node && self.replicas != 3 && self.replicas != 5 {
            return Err(StartError::InvalidReplicas(self.replicas));
        }
        Ok((dir, bind))
    }

    pub async fn start(self) -> Result<Quorium, StartError> {
        Quorium::start(self).await
    }
}

pub(crate) fn node_id(dir: &Path) -> io::Result<u64> {
    let path = dir.join("node_id");
    match std::fs::File::open(&path) {
        Ok(mut f) => {
            let mut s = String::new();
            f.read_to_string(&mut s)?;
            s.trim().parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "node_id file is damaged"))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let id = loop {
                let candidate: u64 = rand::random::<u64>() >> 1;
                if candidate != 0 {
                    break candidate;
                }
            };
            let tmp = dir.join("node_id.tmp");
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(id.to_string().as_bytes())?;
            f.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            std::fs::File::open(dir)?.sync_all()?;
            Ok(id)
        }
        Err(e) => Err(e),
    }
}
