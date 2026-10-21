use std::cell::Cell;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub use linkme;

pub type ApplyFn = fn(Option<&[u8]>, u32, &[u8], &Ctx) -> Result<(Vec<u8>, Vec<u8>), ApplyError>;
pub type MigrateFn = fn(u32, &[u8]) -> Result<Vec<u8>, ApplyError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyError {
    UnknownType(String),
    UnknownMethod(u32),
    UnsupportedVersion { type_name: String, stored: u32, known: u32 },
    Decode,
    Migration(String),
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplyError::UnknownType(t) => write!(f, "state type {t} is not registered in this binary"),
            ApplyError::UnknownMethod(m) => write!(f, "write method {m:#x} is not known to this binary"),
            ApplyError::UnsupportedVersion { type_name, stored, known } => {
                write!(f, "{type_name} is stored at version {stored} but this binary knows version {known}")
            }
            ApplyError::Decode => f.write_str("arguments could not be decoded"),
            ApplyError::Migration(e) => write!(f, "migration failed: {e}"),
        }
    }
}

impl std::error::Error for ApplyError {}

pub struct Registration {
    pub name: &'static str,
    pub version: u32,
    pub methods: &'static [u32],
    pub apply: ApplyFn,
    pub migrate: Option<MigrateFn>,
}

impl Registration {
    pub fn accepts(&self, method: u32) -> bool {
        self.methods.contains(&method)
    }
}

#[linkme::distributed_slice]
pub static REGISTRY: [Registration];

static EXPLICIT: std::sync::RwLock<Vec<&'static Registration>> = std::sync::RwLock::new(Vec::new());

pub fn register(registration: &'static Registration) {
    if lookup(registration.name).is_some() {
        return;
    }
    let mut explicit = EXPLICIT.write().unwrap_or_else(|e| e.into_inner());
    if !explicit.iter().any(|r| r.name == registration.name) {
        explicit.push(registration);
    }
}

fn all() -> Vec<&'static Registration> {
    let explicit = EXPLICIT.read().unwrap_or_else(|e| e.into_inner());
    REGISTRY.iter().chain(explicit.iter().copied()).collect()
}

pub fn lookup(name: &str) -> Option<&'static Registration> {
    REGISTRY.iter().find(|r| r.name == name).or_else(|| {
        let explicit = EXPLICIT.read().unwrap_or_else(|e| e.into_inner());
        explicit.iter().copied().find(|r| r.name == name)
    })
}

pub fn local_methods() -> std::collections::BTreeSet<(String, u32)> {
    all().into_iter().flat_map(|r| r.methods.iter().map(move |m| (r.name.to_string(), *m))).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MethodSupport {
    Enabled,
    Missing,
    Unknown,
}

pub fn method_support(
    type_name: &str,
    method: u32,
    voters: &[u64],
    advertised: &std::collections::BTreeMap<u64, std::collections::BTreeSet<(String, u32)>>,
) -> MethodSupport {
    let wanted = (type_name.to_string(), method);
    let mut unknown = false;
    for v in voters {
        match advertised.get(v) {
            None => unknown = true,
            Some(m) if !m.contains(&wanted) => return MethodSupport::Missing,
            Some(_) => {}
        }
    }
    if unknown { MethodSupport::Unknown } else { MethodSupport::Enabled }
}

pub fn method_enabled(
    type_name: &str,
    method: u32,
    voters: &[u64],
    advertised: &std::collections::BTreeMap<u64, std::collections::BTreeSet<(String, u32)>>,
) -> bool {
    let wanted = (type_name.to_string(), method);
    voters.iter().all(|v| advertised.get(v).is_some_and(|m| m.contains(&wanted)))
}

pub fn registered_names() -> Vec<&'static str> {
    all().into_iter().map(|r| r.name).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityCall {
    pub type_name: String,
    pub version: u32,
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    pub method: u32,
    #[serde(with = "serde_bytes")]
    pub args: Vec<u8>,
    pub now_ms: u64,
    #[serde(default)]
    pub locality: Option<String>,
}

#[derive(Debug)]
pub struct Ctx {
    index: u64,
    now_ms: u64,
    state: Cell<u64>,
}

fn splitmix(x: u64) -> u64 {
    let z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Ctx {
    pub fn new(index: u64, now_ms: u64, seed: u64) -> Ctx {
        Ctx { index, now_ms, state: Cell::new(splitmix(seed ^ splitmix(index))) }
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.now_ms)
    }

    pub fn random(&self) -> u64 {
        let next = self.state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        self.state.set(next);
        splitmix(next)
    }
}
