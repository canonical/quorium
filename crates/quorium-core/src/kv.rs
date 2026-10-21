use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::entity::{ApplyError, Ctx, EntityCall, lookup};
use crate::hash::fnv1a;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvOp {
    Put {
        key: String,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
    Cas {
        key: String,
        #[serde(with = "serde_bytes")]
        expected: Option<Vec<u8>>,
        #[serde(with = "serde_bytes")]
        new: Vec<u8>,
    },
    Apply(EntityCall),
    DeleteEntity {
        type_name: String,
        key: Vec<u8>,
        #[serde(default)]
        locality: Option<String>,
    },
    Lease(LeaseOp),
    Escrow(EscrowOp),
    Restore {
        #[serde(with = "serde_bytes")]
        state: Vec<u8>,
        shard: u64,
    },
    Own {
        ranges: Vec<(u64, u64)>,
    },
    Extract {
        lo: u64,
        hi: u64,
    },
    Ingest {
        lo: u64,
        hi: u64,
        #[serde(with = "serde_bytes")]
        state: Vec<u8>,
    },
    Release {
        lo: u64,
        hi: u64,
    },
    OwnIfUnset {
        ranges: Vec<(u64, u64)>,
    },
    Route(RouteOp),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseAction {
    Acquire,
    Renew { token: u64 },
    Release { token: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseOp {
    pub name: String,
    pub holder: u64,
    pub ttl_ms: u64,
    pub now_ms: u64,
    pub action: LeaseAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct LeaseState {
    holder: u64,
    token: u64,
    expires_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EscrowAction {
    Refill { units: u64 },
    Grant { want: u64 },
    Settle { sold: u64, release: u64 },
    Buy { units: u64 },
    SettleThrough { through: u64, release: u64 },
    Reclaim { through: u64 },
    Close { sold: u64 },
    GrantVia { want: u64, peer: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EscrowKey {
    Named(String),
    Field { type_name: String, key: Vec<u8>, locality: Option<String>, field: String },
}

impl EscrowKey {
    pub fn position(&self) -> u64 {
        match self {
            EscrowKey::Named(name) => crate::shard::key_hash(name.as_bytes()),
            EscrowKey::Field { type_name, key, locality, .. } => {
                crate::shard::entity_position(type_name, key, locality.as_deref())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Holder {
    pub settled_through: u64,
    pub held: u64,
    pub copy_peer: Option<u64>,
    pub reclaimed: bool,
}

impl Holder {
    pub fn translate(holder: Option<&Holder>, action: EscrowAction) -> Result<EscrowAction, String> {
        let h = holder.copied().unwrap_or_default();
        Ok(match action {
            EscrowAction::Grant { .. } | EscrowAction::GrantVia { .. } if h.reclaimed => EscrowAction::Grant { want: 0 },
            EscrowAction::GrantVia { want, peer } => {
                let switching = h.held > 0 && h.copy_peer != Some(peer);
                EscrowAction::Grant { want: if switching { 0 } else { want } }
            }
            EscrowAction::Grant { want } => {
                EscrowAction::Grant { want: if h.held > 0 && h.copy_peer.is_some() { 0 } else { want } }
            }
            EscrowAction::SettleThrough { .. } | EscrowAction::Reclaim { .. } if h.reclaimed => {
                return Err(crate::escrow::RECLAIMED.into());
            }
            EscrowAction::SettleThrough { through, release } => {
                let Some(sold) = through.checked_sub(h.settled_through) else {
                    return Err(format!("settlement through {through} is behind {}", h.settled_through));
                };
                EscrowAction::Settle { sold, release }
            }
            EscrowAction::Reclaim { through } => {
                EscrowAction::Close { sold: through.saturating_sub(h.settled_through) }
            }
            other => other,
        })
    }

    pub fn record(holder: Option<Holder>, action: EscrowAction, granted: u64) -> Option<Holder> {
        let mut h = holder.unwrap_or_default();
        match action {
            EscrowAction::Grant { .. } if granted > 0 => {
                h.held += granted;
                h.copy_peer = None;
            }
            EscrowAction::GrantVia { peer, .. } if granted > 0 => {
                h.held += granted;
                h.copy_peer = Some(peer);
            }
            EscrowAction::SettleThrough { through, release } => {
                let sold = through - h.settled_through;
                h.held = h.held.saturating_sub(sold + release);
                h.settled_through = through;
            }
            EscrowAction::Settle { sold, release } if holder.is_some() => {
                h.held = h.held.saturating_sub(sold + release);
            }
            EscrowAction::Reclaim { through } => {
                h.settled_through = h.settled_through.max(through);
                h.held = 0;
                h.reclaimed = true;
            }
            _ => return holder,
        }
        Some(h)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EscrowOp {
    pub name: String,
    pub node: u64,
    pub action: EscrowAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EscrowView {
    pub pool: u64,
    pub sold: u64,
    pub outstanding: BTreeMap<u64, u64>,
}

impl EscrowView {
    pub fn estimate(&self) -> u64 {
        self.pool + self.outstanding.values().sum::<u64>()
    }

    pub fn apply(&mut self, node: u64, action: EscrowAction) -> Result<u64, String> {
        Ok(match action {
            EscrowAction::Refill { units } => {
                self.pool += units;
                0
            }
            EscrowAction::Grant { want } => {
                let size = if self.pool == 0 { 0 } else { want.min((self.pool / 4).max(1)) };
                self.pool -= size;
                if size > 0 {
                    *self.outstanding.entry(node).or_default() += size;
                }
                size
            }
            EscrowAction::Settle { sold, release } => {
                let held = self.outstanding.get(&node).copied().unwrap_or(0);
                let Some(left) = held.checked_sub(sold).and_then(|l| l.checked_sub(release)) else {
                    return Err(format!("node {node} settled {sold}+{release} of a {held} grant"));
                };
                self.sold += sold;
                self.pool += release;
                if left == 0 {
                    self.outstanding.remove(&node);
                } else {
                    self.outstanding.insert(node, left);
                }
                0
            }
            EscrowAction::Buy { units } => {
                if self.pool >= units {
                    self.pool -= units;
                    self.sold += units;
                    units
                } else {
                    0
                }
            }
            EscrowAction::Close { sold } => {
                let held = self.outstanding.remove(&node).unwrap_or(0);
                let sold = sold.min(held);
                self.sold += sold;
                self.pool += held - sold;
                0
            }
            EscrowAction::SettleThrough { .. } | EscrowAction::Reclaim { .. } | EscrowAction::GrantVia { .. } => {
                return Err("escrow holders are applied by the shard".into());
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub client: u64,
    pub seq: u64,
    pub op: KvOp,
    #[serde(default)]
    pub ack_below: u64,
}

impl Command {
    pub fn new(client: u64, seq: u64, op: KvOp) -> Self {
        Self { client, seq, op, ack_below: 0 }
    }

    pub fn shard(&self, shards: u64) -> crate::shard::ShardId {
        match &self.op {
            KvOp::Apply(call) => {
                crate::shard::entity_shard_with_locality(&call.type_name, &call.key, call.locality.as_deref(), shards)
            }
            KvOp::DeleteEntity { type_name, key, locality } => {
                crate::shard::entity_shard_with_locality(type_name, key, locality.as_deref(), shards)
            }
            KvOp::Lease(op) => crate::shard::shard_of(op.name.as_bytes(), shards),
            KvOp::Escrow(op) => crate::shard::shard_of(op.name.as_bytes(), shards),
            KvOp::Restore { shard, .. } => *shard,
            KvOp::Own { .. }
            | KvOp::Extract { .. }
            | KvOp::Ingest { .. }
            | KvOp::Release { .. }
            | KvOp::OwnIfUnset { .. }
            | KvOp::Route(_) => 0,
            _ => crate::shard::shard_of(self.key().as_bytes(), shards),
        }
    }

    pub fn position(&self) -> Option<u64> {
        use crate::shard::{entity_position, key_hash};
        match &self.op {
            KvOp::Put { key, .. } | KvOp::Get { key } | KvOp::Delete { key } | KvOp::Cas { key, .. } => {
                Some(key_hash(key.as_bytes()))
            }
            KvOp::Apply(call) => Some(entity_position(&call.type_name, &call.key, call.locality.as_deref())),
            KvOp::DeleteEntity { type_name, key, locality } => {
                Some(entity_position(type_name, key, locality.as_deref()))
            }
            KvOp::Lease(op) => Some(key_hash(op.name.as_bytes())),
            KvOp::Escrow(op) => Some(key_hash(op.name.as_bytes())),
            KvOp::Restore { .. }
            | KvOp::Own { .. }
            | KvOp::Extract { .. }
            | KvOp::Ingest { .. }
            | KvOp::Release { .. }
            | KvOp::OwnIfUnset { .. }
            | KvOp::Route(_) => None,
        }
    }

    pub fn acknowledging_below(mut self, seq: u64) -> Self {
        self.ack_below = seq;
        self
    }

    pub fn key(&self) -> &str {
        match &self.op {
            KvOp::Put { key, .. } | KvOp::Get { key } | KvOp::Delete { key } | KvOp::Cas { key, .. } => key,
            KvOp::Apply(call) => &call.type_name,
            KvOp::DeleteEntity { type_name, .. } => type_name,
            KvOp::Lease(op) => &op.name,
            KvOp::Escrow(op) => &op.name,
            KvOp::Restore { .. }
            | KvOp::Own { .. }
            | KvOp::Extract { .. }
            | KvOp::Ingest { .. }
            | KvOp::Release { .. }
            | KvOp::OwnIfUnset { .. }
            | KvOp::Route(_) => "",
        }
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}:{:?}", self.client, self.seq, self.op)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Reply {
    #[default]
    Done,
    Value(#[serde(with = "serde_bytes")] Option<Vec<u8>>),
    Swapped(bool),
    Stale,
    Applied(#[serde(with = "serde_bytes")] Vec<u8>),
    ApplyFailed(String),
    Lease {
        held: bool,
        token: u64,
        holder: u64,
        expires_ms: u64,
    },
    Escrow {
        granted: u64,
        pool: u64,
    },
    WrongShard,
    Extracted(#[serde(with = "serde_bytes")] Vec<u8>),
    Routing(RoutingTable),
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct Session {
    floor: u64,
    highest: u64,
    replies: BTreeMap<u64, Reply>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RoutingTable {
    pub generation: u64,
    pub ranges: Vec<(u64, u64, u64)>,
    pub moving: Option<(u64, u64, u64, u64)>,
}

impl RoutingTable {
    pub fn uniform(groups: u64) -> RoutingTable {
        let groups = groups.max(1);
        let ranges = (0..groups)
            .map(|g| {
                let (lo, hi) = crate::shard::shard_range(g, groups);
                (lo, hi, g)
            })
            .collect();
        RoutingTable { generation: 0, ranges, moving: None }
    }

    pub fn group_of(&self, position: u64) -> u64 {
        self.ranges.iter().find(|(lo, hi, _)| within(position, *lo, *hi)).map_or(0, |(_, _, g)| *g)
    }

    pub fn ranges_of(&self, group: u64) -> Vec<(u64, u64)> {
        self.ranges.iter().filter(|(_, _, g)| *g == group).map(|(lo, hi, _)| (*lo, *hi)).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouteOp {
    Init { table: RoutingTable },
    Split { at: u64, generation: u64 },
    BeginMove { lo: u64, hi: u64, to: u64, generation: u64 },
    FinishMove { lo: u64, hi: u64, to: u64 },
    Merge { generation: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct EntitySlot {
    version: u32,
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
    position: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct KvState {
    data: BTreeMap<String, Vec<u8>>,
    sessions: BTreeMap<u64, Session>,
    entities: BTreeMap<(String, Vec<u8>), EntitySlot>,
    leases: BTreeMap<String, LeaseState>,
    escrows: BTreeMap<String, EscrowView>,
    owned: Option<Vec<(u64, u64)>>,
    frozen: BTreeMap<(u64, u64), Vec<u8>>,
    routing: Option<RoutingTable>,
    holders: BTreeMap<(EscrowKey, u64), Holder>,
}

#[derive(Deserialize)]
struct KvStateV3 {
    data: BTreeMap<String, Vec<u8>>,
    sessions: BTreeMap<u64, Session>,
    entities: BTreeMap<(String, Vec<u8>), EntitySlot>,
    leases: BTreeMap<String, LeaseState>,
    escrows: BTreeMap<String, EscrowView>,
    owned: Option<Vec<(u64, u64)>>,
    frozen: BTreeMap<(u64, u64), Vec<u8>>,
    routing: Option<RoutingTable>,
}

impl From<KvStateV3> for KvState {
    fn from(v3: KvStateV3) -> Self {
        KvState {
            data: v3.data,
            sessions: v3.sessions,
            entities: v3.entities,
            leases: v3.leases,
            escrows: v3.escrows,
            owned: v3.owned,
            frozen: v3.frozen,
            routing: v3.routing,
            holders: BTreeMap::new(),
        }
    }
}

#[derive(Deserialize)]
struct KvStateV2 {
    data: BTreeMap<String, Vec<u8>>,
    sessions: BTreeMap<u64, Session>,
    entities: BTreeMap<(String, Vec<u8>), (u32, Vec<u8>)>,
    leases: BTreeMap<String, LeaseState>,
    escrows: BTreeMap<String, EscrowView>,
}

impl From<KvStateV2> for KvState {
    fn from(v2: KvStateV2) -> Self {
        let entities = v2
            .entities
            .into_iter()
            .map(|((t, k), (version, bytes))| {
                let position = crate::shard::entity_position(&t, &k, None);
                ((t, k), EntitySlot { version, bytes, position })
            })
            .collect();
        KvState {
            data: v2.data,
            sessions: v2.sessions,
            entities,
            leases: v2.leases,
            escrows: v2.escrows,
            owned: None,
            frozen: BTreeMap::new(),
            routing: None,
            holders: BTreeMap::new(),
        }
    }
}

fn within(position: u64, lo: u64, hi: u64) -> bool {
    lo <= position && position <= hi
}

fn subtract(ranges: &[(u64, u64)], lo: u64, hi: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    for &(a, b) in ranges {
        if b < lo || a > hi {
            out.push((a, b));
            continue;
        }
        if a < lo {
            out.push((a, lo - 1));
        }
        if b > hi {
            out.push((hi + 1, b));
        }
    }
    out
}

fn normalize(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.sort();
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (a, b) in ranges {
        match out.last_mut() {
            Some(last) if last.1 != u64::MAX && a <= last.1 + 1 => last.1 = last.1.max(b),
            _ => out.push((a, b)),
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowKey {
    Meta,
    Data(String),
    Entity(String, Vec<u8>),
}

impl RowKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RowKey::Meta => vec![0],
            RowKey::Data(key) => [&[1u8][..], key.as_bytes()].concat(),
            RowKey::Entity(t, k) => {
                let mut out = vec![2];
                out.extend(postcard::to_allocvec(&(t, k)).expect("row keys always serialize"));
                out
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes.split_first() {
            Some((0, [])) => Ok(RowKey::Meta),
            Some((1, rest)) => String::from_utf8(rest.to_vec()).map(RowKey::Data).map_err(|_| DecodeError),
            Some((2, rest)) => match postcard::take_from_bytes::<(String, Vec<u8>)>(rest) {
                Ok(((t, k), [])) => Ok(RowKey::Entity(t, k)),
                _ => Err(DecodeError),
            },
            _ => Err(DecodeError),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Touch {
    Meta,
    Row(RowKey),
    All,
}

impl KvOp {
    pub fn touches(&self) -> Touch {
        match self {
            KvOp::Put { key, .. } | KvOp::Delete { key } | KvOp::Cas { key, .. } => {
                Touch::Row(RowKey::Data(key.clone()))
            }
            KvOp::Apply(call) => Touch::Row(RowKey::Entity(call.type_name.clone(), call.key.clone())),
            KvOp::DeleteEntity { type_name, key, .. } => Touch::Row(RowKey::Entity(type_name.clone(), key.clone())),
            KvOp::Restore { .. } | KvOp::Ingest { .. } | KvOp::Release { .. } => Touch::All,
            KvOp::Get { .. }
            | KvOp::Lease(_)
            | KvOp::Escrow(_)
            | KvOp::Own { .. }
            | KvOp::Extract { .. }
            | KvOp::OwnIfUnset { .. }
            | KvOp::Route(_) => Touch::Meta,
        }
    }
}

#[derive(Serialize)]
struct MetaRef<'a> {
    sessions: &'a BTreeMap<u64, Session>,
    leases: &'a BTreeMap<String, LeaseState>,
    escrows: &'a BTreeMap<String, EscrowView>,
    owned: &'a Option<Vec<(u64, u64)>>,
    frozen: &'a BTreeMap<(u64, u64), Vec<u8>>,
    routing: &'a Option<RoutingTable>,
    holders: &'a BTreeMap<(EscrowKey, u64), Holder>,
}

#[derive(Deserialize)]
struct MetaRow {
    sessions: BTreeMap<u64, Session>,
    leases: BTreeMap<String, LeaseState>,
    escrows: BTreeMap<String, EscrowView>,
    owned: Option<Vec<(u64, u64)>>,
    frozen: BTreeMap<(u64, u64), Vec<u8>>,
    routing: Option<RoutingTable>,
    holders: BTreeMap<(EscrowKey, u64), Holder>,
}

#[derive(Deserialize)]
struct MetaRowV1 {
    sessions: BTreeMap<u64, Session>,
    leases: BTreeMap<String, LeaseState>,
    escrows: BTreeMap<String, EscrowView>,
    owned: Option<Vec<(u64, u64)>>,
    frozen: BTreeMap<(u64, u64), Vec<u8>>,
    routing: Option<RoutingTable>,
}

impl From<MetaRowV1> for MetaRow {
    fn from(v1: MetaRowV1) -> Self {
        MetaRow {
            sessions: v1.sessions,
            leases: v1.leases,
            escrows: v1.escrows,
            owned: v1.owned,
            frozen: v1.frozen,
            routing: v1.routing,
            holders: BTreeMap::new(),
        }
    }
}

const ROW_FORMAT: u8 = 2;
const ROW_FORMAT_V1: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError;

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid key-value snapshot")
    }
}

impl std::error::Error for DecodeError {}

const SNAPSHOT_FORMAT: u8 = 4;
const SNAPSHOT_FORMAT_V3: u8 = 3;
const SNAPSHOT_FORMAT_V2: u8 = 2;

impl KvState {
    pub fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn session(&self, client: u64) -> Option<u64> {
        self.sessions.get(&client).map(|s| s.highest)
    }

    pub fn applied(&self, client: u64, seq: u64) -> bool {
        self.sessions.get(&client).is_some_and(|s| s.replies.contains_key(&seq))
    }

    pub fn cached_replies(&self, client: u64) -> usize {
        self.sessions.get(&client).map_or(0, |s| s.replies.len())
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn apply(&mut self, cmd: &Command) -> Reply {
        self.apply_at(cmd, 0).unwrap_or_else(|e| Reply::ApplyFailed(e.to_string()))
    }

    pub fn owns(&self, position: u64) -> bool {
        self.owned.as_ref().is_none_or(|ranges| ranges.iter().any(|(lo, hi)| within(position, *lo, *hi)))
    }

    pub fn owned_ranges(&self) -> Option<Vec<(u64, u64)>> {
        self.owned.clone()
    }

    fn range_state(&self, lo: u64, hi: u64) -> KvState {
        use crate::shard::key_hash;
        KvState {
            data: self
                .data
                .iter()
                .filter(|(k, _)| within(key_hash(k.as_bytes()), lo, hi))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            sessions: self.sessions.clone(),
            entities: self
                .entities
                .iter()
                .filter(|(_, e)| within(e.position, lo, hi))
                .map(|(k, e)| (k.clone(), e.clone()))
                .collect(),
            leases: self
                .leases
                .iter()
                .filter(|(n, _)| within(key_hash(n.as_bytes()), lo, hi))
                .map(|(n, l)| (n.clone(), l.clone()))
                .collect(),
            escrows: self
                .escrows
                .iter()
                .filter(|(n, _)| within(key_hash(n.as_bytes()), lo, hi))
                .map(|(n, e)| (n.clone(), e.clone()))
                .collect(),
            owned: None,
            frozen: BTreeMap::new(),
            routing: None,
            holders: self
                .holders
                .iter()
                .filter(|((k, _), _)| within(k.position(), lo, hi))
                .map(|(k, h)| (k.clone(), *h))
                .collect(),
        }
    }

    pub fn item_count(&self) -> usize {
        use crate::shard::key_hash;
        self.data.keys().filter(|k| self.owns(key_hash(k.as_bytes()))).count()
            + self.entities.values().filter(|e| self.owns(e.position)).count()
            + self.leases.keys().filter(|n| self.owns(key_hash(n.as_bytes()))).count()
            + self.escrows.keys().filter(|n| self.owns(key_hash(n.as_bytes()))).count()
    }

    pub fn routing(&self) -> Option<&RoutingTable> {
        self.routing.as_ref()
    }

    fn route(&mut self, op: &RouteOp) -> Reply {
        if let RouteOp::Init { table } = op {
            if self.routing.is_none() {
                self.routing = Some(table.clone());
            }
            return Reply::Routing(self.routing.clone().unwrap_or_default());
        }
        let Some(t) = self.routing.as_mut() else {
            return Reply::ApplyFailed("routing table is not initialised".into());
        };
        let stale = |generation: u64, t: &RoutingTable| {
            Reply::ApplyFailed(format!("routing generation {generation} is stale; current is {}", t.generation))
        };
        match op {
            RouteOp::Init { .. } => {}
            RouteOp::Split { at, generation } => {
                if *generation != t.generation {
                    return stale(*generation, t);
                }
                let Some(i) = t.ranges.iter().position(|(lo, hi, _)| *lo < *at && *at <= *hi) else {
                    return Reply::ApplyFailed(format!("no range can be split at {at}"));
                };
                if t.moving.is_some_and(|(lo, hi, _, _)| within(*at, lo, hi)) {
                    return Reply::ApplyFailed("cannot split a range that is moving".into());
                }
                let (lo, hi, g) = t.ranges[i];
                t.ranges.splice(i..=i, [(lo, at - 1, g), (*at, hi, g)]);
                t.generation += 1;
            }
            RouteOp::BeginMove { lo, hi, to, generation } => {
                if t.moving == Some((*lo, *hi, t.group_of(*lo), *to)) {
                    return Reply::Routing(t.clone());
                }
                if *generation != t.generation {
                    return stale(*generation, t);
                }
                if t.moving.is_some() {
                    return Reply::ApplyFailed("another range is already moving".into());
                }
                let Some(&(_, _, from)) = t.ranges.iter().find(|(a, b, _)| a == lo && b == hi) else {
                    return Reply::ApplyFailed(format!("no range is exactly {lo}..={hi}; split first"));
                };
                if from == *to {
                    return Reply::ApplyFailed("the range is already on that group".into());
                }
                t.moving = Some((*lo, *hi, from, *to));
                t.generation += 1;
            }
            RouteOp::FinishMove { lo, hi, to } => match t.moving {
                Some((a, b, _, dest)) if a == *lo && b == *hi && dest == *to => {
                    if let Some(entry) = t.ranges.iter_mut().find(|(a, b, _)| a == lo && b == hi) {
                        entry.2 = *to;
                    }
                    t.moving = None;
                    t.generation += 1;
                }
                _ if t.ranges.contains(&(*lo, *hi, *to)) => {}
                _ => return Reply::ApplyFailed("no such move is in progress".into()),
            },
            RouteOp::Merge { generation } => {
                if *generation != t.generation {
                    return stale(*generation, t);
                }
                let moving = t.moving;
                let mut merged: Vec<(u64, u64, u64)> = Vec::new();
                for &(lo, hi, g) in &t.ranges {
                    let pinned = |a: u64, b: u64| moving.is_some_and(|(ml, mh, _, _)| ml == a && mh == b);
                    match merged.last_mut() {
                        Some(last)
                            if last.2 == g
                                && last.1.checked_add(1) == Some(lo)
                                && !pinned(lo, hi)
                                && !pinned(last.0, last.1) =>
                        {
                            last.1 = hi
                        }
                        _ => merged.push((lo, hi, g)),
                    }
                }
                if merged != t.ranges {
                    t.ranges = merged;
                    t.generation += 1;
                }
            }
        }
        Reply::Routing(t.clone())
    }

    fn extract(&mut self, lo: u64, hi: u64) -> Reply {
        if let Some(bytes) = self.frozen.get(&(lo, hi)) {
            return Reply::Extracted(bytes.clone());
        }
        let bytes = self.range_state(lo, hi).to_bytes();
        let owned = self.owned.take().unwrap_or_else(|| vec![(0, u64::MAX)]);
        self.owned = Some(subtract(&owned, lo, hi));
        self.frozen.insert((lo, hi), bytes.clone());
        Reply::Extracted(bytes)
    }

    fn ingest(&mut self, lo: u64, hi: u64, bytes: &[u8]) -> Reply {
        if self.owned.as_ref().is_some_and(|o| o.iter().any(|(a, b)| *a <= lo && hi <= *b)) {
            return Reply::Done;
        }
        let incoming = match KvState::from_bytes(bytes) {
            Ok(s) => s,
            Err(e) => return Reply::ApplyFailed(e.to_string()),
        };
        self.data.extend(incoming.data);
        self.entities.extend(incoming.entities);
        self.leases.extend(incoming.leases);
        self.escrows.extend(incoming.escrows);
        self.holders.extend(incoming.holders);
        for (client, theirs) in incoming.sessions {
            let mine = self.sessions.entry(client).or_default();
            mine.floor = mine.floor.max(theirs.floor);
            mine.highest = mine.highest.max(theirs.highest);
            for (seq, reply) in theirs.replies {
                mine.replies.entry(seq).or_insert(reply);
            }
            let floor = mine.floor;
            mine.replies = mine.replies.split_off(&floor);
        }
        let mut owned = self.owned.take().unwrap_or_default();
        owned.push((lo, hi));
        self.owned = Some(normalize(owned));
        Reply::Done
    }

    fn release(&mut self, lo: u64, hi: u64) -> Reply {
        use crate::shard::key_hash;
        let owned = self.owned.clone();
        let keep =
            |p: u64| !within(p, lo, hi) || owned.as_ref().is_none_or(|o| o.iter().any(|(a, b)| within(p, *a, *b)));
        self.data.retain(|k, _| keep(key_hash(k.as_bytes())));
        self.entities.retain(|_, e| keep(e.position));
        self.leases.retain(|n, _| keep(key_hash(n.as_bytes())));
        self.escrows.retain(|n, _| keep(key_hash(n.as_bytes())));
        self.holders.retain(|(k, _), _| keep(k.position()));
        self.frozen.remove(&(lo, hi));
        Reply::Done
    }

    pub fn apply_at(&mut self, cmd: &Command, index: u64) -> Result<Reply, ApplyError> {
        if cmd.position().is_some_and(|p| !self.owns(p)) {
            return Ok(Reply::WrongShard);
        }
        {
            let session = self.sessions.entry(cmd.client).or_default();
            if cmd.ack_below > session.floor {
                session.floor = cmd.ack_below;
                session.replies = session.replies.split_off(&session.floor);
            }
            if cmd.seq < session.floor {
                return Ok(Reply::Stale);
            }
            if let Some(reply) = session.replies.get(&cmd.seq) {
                return Ok(reply.clone());
            }
        }
        let seed = fnv1a(&[cmd.client.to_le_bytes(), cmd.seq.to_le_bytes()].concat());
        let reply = self.execute(&cmd.op, index, seed)?;
        let session = self.sessions.entry(cmd.client).or_default();
        session.replies.insert(cmd.seq, reply.clone());
        session.highest = session.highest.max(cmd.seq);
        Ok(reply)
    }

    pub fn entity(&self, type_name: &str, key: &[u8]) -> Option<(u32, &[u8])> {
        self.entities.get(&(type_name.to_string(), key.to_vec())).map(|e| (e.version, e.bytes.as_slice()))
    }

    pub fn unknown_types(&self) -> Vec<String> {
        let mut out: Vec<String> =
            self.entities.keys().map(|(t, _)| t.clone()).filter(|t| lookup(t).is_none()).collect();
        out.dedup();
        out
    }

    fn apply_field_escrow(&mut self, call: &EntityCall, index: u64, seed: u64) -> Result<Reply, ApplyError> {
        let Ok((field, node, action)) = postcard::from_bytes::<(String, u64, EscrowAction)>(&call.args) else {
            return self.apply_entity(call, index, seed);
        };
        let key = EscrowKey::Field {
            type_name: call.type_name.clone(),
            key: call.key.clone(),
            locality: call.locality.clone(),
            field: field.clone(),
        };
        let slot = (key, node);
        let holder = self.holders.get(&slot).copied();
        let translated = match Holder::translate(holder.as_ref(), action) {
            Ok(a) => a,
            Err(reason) => {
                let refused: Result<(u64, u64), String> = Err(reason);
                return Ok(Reply::Applied(postcard::to_allocvec(&refused).expect("escrow outcomes always serialize")));
            }
        };
        let mut translated_call = call.clone();
        translated_call.args =
            postcard::to_allocvec(&(field, node, translated)).expect("escrow arguments always serialize");
        let reply = self.apply_entity(&translated_call, index, seed)?;
        if let Reply::Applied(bytes) = &reply
            && let Ok(Ok((granted, _))) = postcard::from_bytes::<Result<(u64, u64), String>>(bytes)
            && let Some(h) = Holder::record(holder, action, granted)
        {
            self.holders.insert(slot, h);
        }
        Ok(reply)
    }

    fn apply_entity(&mut self, call: &EntityCall, index: u64, seed: u64) -> Result<Reply, ApplyError> {
        let reg = lookup(&call.type_name).ok_or_else(|| ApplyError::UnknownType(call.type_name.clone()))?;
        if !reg.accepts(call.method) {
            return Err(ApplyError::UnknownMethod(call.method));
        }
        if call.version > reg.version {
            return Err(ApplyError::UnsupportedVersion {
                type_name: call.type_name.clone(),
                stored: call.version,
                known: reg.version,
            });
        }
        let slot = (call.type_name.clone(), call.key.clone());
        let current = match self.entities.get(&slot).map(|e| (&e.version, &e.bytes)) {
            None => None,
            Some((v, bytes)) if *v == reg.version => Some(bytes.clone()),
            Some((v, bytes)) if *v < reg.version => match reg.migrate {
                Some(migrate) => match migrate(*v, bytes) {
                    Ok(b) => Some(b),
                    Err(e) => return Ok(Reply::ApplyFailed(e.to_string())),
                },
                None => return Ok(Reply::ApplyFailed(format!("no migration from version {v}"))),
            },
            Some((v, _)) => {
                return Err(ApplyError::UnsupportedVersion {
                    type_name: call.type_name.clone(),
                    stored: *v,
                    known: reg.version,
                });
            }
        };
        let ctx = Ctx::new(index, call.now_ms, seed);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (reg.apply)(current.as_deref(), call.method, &call.args, &ctx)
        }));
        match outcome {
            Ok(Ok((state, reply))) => {
                let position = crate::shard::entity_position(&call.type_name, &call.key, call.locality.as_deref());
                self.entities.insert(slot, EntitySlot { version: reg.version, bytes: state, position });
                Ok(Reply::Applied(reply))
            }
            Ok(Err(ApplyError::UnknownMethod(m))) => Err(ApplyError::UnknownMethod(m)),
            Ok(Err(e)) => Ok(Reply::ApplyFailed(e.to_string())),
            Err(panic) => {
                let message = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                Ok(Reply::ApplyFailed(format!("write panicked: {message}")))
            }
        }
    }

    pub fn is_pristine(&self) -> bool {
        self.data.is_empty() && self.entities.is_empty() && self.leases.is_empty() && self.escrows.is_empty()
    }

    fn restore(&mut self, bytes: &[u8]) -> Reply {
        if !self.is_pristine() {
            return Reply::ApplyFailed("restore requires an empty shard".into());
        }
        match KvState::from_bytes(bytes) {
            Ok(restored) => {
                let sessions = std::mem::take(&mut self.sessions);
                *self = restored;
                for (client, session) in sessions {
                    self.sessions.entry(client).or_insert(session);
                }
                Reply::Done
            }
            Err(e) => Reply::ApplyFailed(e.to_string()),
        }
    }

    pub fn escrow(&self, name: &str) -> Option<&EscrowView> {
        self.escrows.get(name)
    }

    pub fn escrow_holder(&self, key: &EscrowKey, node: u64) -> Option<&Holder> {
        self.holders.get(&(key.clone(), node))
    }

    pub fn escrow_holders(&self) -> impl Iterator<Item = (&EscrowKey, u64, &Holder)> {
        self.holders.iter().map(|((k, node), h)| (k, *node, h))
    }

    fn escrow_op(&mut self, op: &EscrowOp) -> Reply {
        let slot = (EscrowKey::Named(op.name.clone()), op.node);
        let holder = self.holders.get(&slot).copied();
        let action = match Holder::translate(holder.as_ref(), op.action) {
            Ok(a) => a,
            Err(reason) => return Reply::ApplyFailed(reason),
        };
        let e = self.escrows.entry(op.name.clone()).or_default();
        match e.apply(op.node, action) {
            Ok(granted) => {
                let pool = e.pool;
                if let Some(h) = Holder::record(holder, op.action, granted) {
                    self.holders.insert(slot, h);
                }
                Reply::Escrow { granted, pool }
            }
            Err(reason) => Reply::ApplyFailed(reason),
        }
    }

    fn lease(&mut self, op: &LeaseOp) -> Reply {
        let l = self.leases.entry(op.name.clone()).or_default();
        let live = op.now_ms < l.expires_ms;
        let held = match op.action {
            LeaseAction::Acquire if live && l.holder == op.holder => true,
            LeaseAction::Acquire if live => false,
            LeaseAction::Acquire => {
                l.token += 1;
                l.holder = op.holder;
                l.expires_ms = op.now_ms + op.ttl_ms;
                true
            }
            LeaseAction::Renew { token } if live && l.holder == op.holder && l.token == token => {
                l.expires_ms = l.expires_ms.max(op.now_ms + op.ttl_ms);
                true
            }
            LeaseAction::Renew { .. } => false,
            LeaseAction::Release { token } => {
                if l.holder == op.holder && l.token == token {
                    l.expires_ms = 0;
                }
                false
            }
        };
        Reply::Lease { held, token: l.token, holder: l.holder, expires_ms: l.expires_ms }
    }

    fn execute(&mut self, op: &KvOp, index: u64, seed: u64) -> Result<Reply, ApplyError> {
        Ok(match op {
            KvOp::Lease(lease) => self.lease(lease),
            KvOp::Escrow(op) => self.escrow_op(op),
            KvOp::Restore { state, .. } => self.restore(state),
            KvOp::Own { ranges } => {
                self.owned = Some(normalize(ranges.clone()));
                Reply::Done
            }
            KvOp::Extract { lo, hi } => self.extract(*lo, *hi),
            KvOp::Ingest { lo, hi, state } => self.ingest(*lo, *hi, state),
            KvOp::Release { lo, hi } => self.release(*lo, *hi),
            KvOp::OwnIfUnset { ranges } => {
                if self.owned.is_none() {
                    self.owned = Some(normalize(ranges.clone()));
                }
                Reply::Done
            }
            KvOp::Route(op) => self.route(op),
            KvOp::Apply(call) if call.method == crate::escrow::ESCROW_METHOD => {
                return self.apply_field_escrow(call, index, seed);
            }
            KvOp::Apply(call) => return self.apply_entity(call, index, seed),
            KvOp::DeleteEntity { type_name, key, .. } => {
                self.entities.remove(&(type_name.clone(), key.clone()));
                Reply::Done
            }
            KvOp::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                Reply::Done
            }
            KvOp::Get { key } => Reply::Value(self.data.get(key).cloned()),
            KvOp::Delete { key } => {
                self.data.remove(key);
                Reply::Done
            }
            KvOp::Cas { key, expected, new } => {
                if self.data.get(key) == expected.as_ref() {
                    self.data.insert(key.clone(), new.clone());
                    Reply::Swapped(true)
                } else {
                    Reply::Swapped(false)
                }
            }
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![SNAPSHOT_FORMAT];
        out.extend(postcard::to_allocvec(self).expect("key-value state always serializes"));
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes.split_first() {
            Some((&SNAPSHOT_FORMAT, rest)) => match postcard::take_from_bytes(rest) {
                Ok((state, [])) => Ok(state),
                _ => Err(DecodeError),
            },
            Some((&SNAPSHOT_FORMAT_V3, rest)) => match postcard::take_from_bytes::<KvStateV3>(rest) {
                Ok((state, [])) => Ok(KvState::from(state).with_current_frozen()),
                _ => Err(DecodeError),
            },
            Some((&SNAPSHOT_FORMAT_V2, rest)) => match postcard::take_from_bytes::<KvStateV2>(rest) {
                Ok((state, [])) => Ok(KvState::from(state).with_current_frozen()),
                _ => Err(DecodeError),
            },
            _ => Err(DecodeError),
        }
    }

    fn with_current_frozen(mut self) -> Self {
        let current = |bytes: &mut Vec<u8>| {
            if let Ok(state) = KvState::from_bytes(bytes) {
                *bytes = state.to_bytes();
            }
        };
        self.frozen.values_mut().for_each(current);
        for session in self.sessions.values_mut() {
            for reply in session.replies.values_mut() {
                if let Reply::Extracted(bytes) = reply {
                    current(bytes);
                }
            }
        }
        self
    }

    fn meta_row(&self) -> Vec<u8> {
        let meta = MetaRef {
            sessions: &self.sessions,
            leases: &self.leases,
            escrows: &self.escrows,
            owned: &self.owned,
            frozen: &self.frozen,
            routing: &self.routing,
            holders: &self.holders,
        };
        let mut out = vec![ROW_FORMAT];
        out.extend(postcard::to_allocvec(&meta).expect("key-value state always serializes"));
        out
    }

    pub fn row(&self, key: &RowKey) -> Option<Vec<u8>> {
        match key {
            RowKey::Meta => Some(self.meta_row()),
            RowKey::Data(k) => self.data.get(k).cloned(),
            RowKey::Entity(t, k) => self
                .entities
                .get(&(t.clone(), k.clone()))
                .map(|e| postcard::to_allocvec(e).expect("entities always serialize")),
        }
    }

    pub fn rows(&self) -> Vec<(RowKey, Vec<u8>)> {
        let mut out = vec![(RowKey::Meta, self.meta_row())];
        out.extend(self.data.iter().map(|(k, v)| (RowKey::Data(k.clone()), v.clone())));
        out.extend(self.entities.iter().map(|((t, k), e)| {
            (RowKey::Entity(t.clone(), k.clone()), postcard::to_allocvec(e).expect("entities always serialize"))
        }));
        out
    }

    pub fn from_rows(rows: impl IntoIterator<Item = (RowKey, Vec<u8>)>) -> Result<Self, DecodeError> {
        let mut state = KvState::default();
        let mut meta = None;
        let mut upgraded = false;
        for (key, value) in rows {
            match key {
                RowKey::Meta => match value.split_first() {
                    Some((&ROW_FORMAT, rest)) => match postcard::take_from_bytes::<MetaRow>(rest) {
                        Ok((m, [])) => meta = Some(m),
                        _ => return Err(DecodeError),
                    },
                    Some((&ROW_FORMAT_V1, rest)) => match postcard::take_from_bytes::<MetaRowV1>(rest) {
                        Ok((m, [])) => {
                            meta = Some(m.into());
                            upgraded = true;
                        }
                        _ => return Err(DecodeError),
                    },
                    _ => return Err(DecodeError),
                },
                RowKey::Data(k) => {
                    state.data.insert(k, value);
                }
                RowKey::Entity(t, k) => match postcard::take_from_bytes::<EntitySlot>(&value) {
                    Ok((e, [])) => {
                        state.entities.insert((t, k), e);
                    }
                    _ => return Err(DecodeError),
                },
            }
        }
        let meta = meta.ok_or(DecodeError)?;
        state.sessions = meta.sessions;
        state.leases = meta.leases;
        state.escrows = meta.escrows;
        state.owned = meta.owned;
        state.frozen = meta.frozen;
        state.routing = meta.routing;
        state.holders = meta.holders;
        Ok(if upgraded { state.with_current_frozen() } else { state })
    }

    pub fn state_hash(&self) -> u64 {
        fnv1a(&self.to_bytes())
    }
}
