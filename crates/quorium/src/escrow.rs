use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use quorium_core::escrow::{Holding, RECLAIMED};
use quorium_core::kv::{Command, EscrowAction, EscrowKey, EscrowOp, EscrowView, KvOp, Reply};
use quorium_core::types::NodeId;
use quorium_net::raft::{Request, Response};
use quorium_store::disk::FileDisk;
use quorium_store::wal::Wal;
use serde::{Deserialize, Serialize};

use crate::entity::Error;
use crate::node::Quorium;

const ESCROW_CLIENT_SALT: u64 = 0x4553_4352_4f57_0000;
const IDLE_RELEASE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize)]
enum JournalRecord {
    Granted { name: String, units: u64 },
    Sold { name: String, units: u64 },
    SettleSent { name: String, seq: u64, sold: u64, release: u64 },
    SettleDone { name: String, seq: u64 },
    Bought { name: String, units: u64 },
    Reserved { upto: u64 },
    Unsold { name: String, units: u64 },
    Field { name: String, type_name: String, version: u32, key: Vec<u8>, locality: Option<String>, field: String },
    Copied { name: String, units: u64 },
    GrantedVia { name: String, units: u64, peer: NodeId },
    PeerSold { owner: NodeId, name: String, through: u64 },
    Fenced { owner: NodeId, name: String },
    Reclaimed { name: String },
}

pub(crate) const FENCED: &str = "the escrow holder is fenced";

pub(crate) enum Copy {
    Peer(NodeId, String),
    Alone,
    Unavailable,
}

enum Taken {
    Yes(u64),
    Exhausted,
    OtherPeer,
}

#[derive(Debug, Default, Clone, Copy)]
struct PeerCopy {
    through: u64,
    fenced: bool,
}

const RESERVE_BLOCK: u64 = 1_000;

#[derive(Debug, Default, Clone)]
pub(crate) struct LocalGrant {
    holding: Holding,
    last_take: Option<Instant>,
    copy_peer: Option<NodeId>,
}

struct Sequencer {
    next: u64,
    reserved: u64,
    durable: u64,
    in_flight: std::collections::BTreeSet<u64>,
}

pub(crate) struct EscrowBook {
    targets: Mutex<HashMap<String, Target>>,
    grants: Mutex<HashMap<String, LocalGrant>>,
    journal: std::sync::Arc<Mutex<Wal<FileDisk>>>,
    seq: Mutex<Sequencer>,
    held_for_peers: Mutex<HashMap<(NodeId, String), u64>>,
    copies: Mutex<HashMap<(NodeId, String), PeerCopy>>,
}

pub(crate) struct Ticket<'a> {
    book: &'a EscrowBook,
    pub(crate) seq: u64,
    pub(crate) ack_below: u64,
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        self.book.seq.lock().unwrap().in_flight.remove(&self.seq);
    }
}

impl EscrowBook {
    pub(crate) fn open(dir: &Path) -> std::io::Result<EscrowBook> {
        let mut journal = Wal::open(FileDisk::open(&dir.join("escrow.journal"))?)?;
        let mut grants: HashMap<String, LocalGrant> = HashMap::new();
        let mut seq = 0u64;
        let mut reserved = 0u64;
        let mut fields: HashMap<String, Target> = HashMap::new();
        let mut copies: HashMap<(NodeId, String), PeerCopy> = HashMap::new();
        for bytes in journal.take_records() {
            let Ok(record) = postcard::from_bytes::<JournalRecord>(&bytes) else { continue };
            match record {
                JournalRecord::Granted { name, units } => grants.entry(name).or_default().holding.grant(units),
                JournalRecord::Sold { name, units } => grants.entry(name).or_default().holding.replay_sold(units),
                JournalRecord::Copied { name, units } => grants.entry(name).or_default().holding.confirm(units),
                JournalRecord::SettleSent { name, seq: s, sold, release } => {
                    grants.entry(name).or_default().holding.replay_settle(s, sold, release);
                    seq = seq.max(s);
                }
                JournalRecord::SettleDone { name, seq: s } => grants.entry(name).or_default().holding.settled(s),
                JournalRecord::Bought { .. } => {}
                JournalRecord::Reserved { upto } => reserved = reserved.max(upto),
                JournalRecord::Field { name, type_name, version, key, locality, field } => {
                    fields.insert(name, Target::Field { type_name, version, key, locality, field, reader: None });
                }
                JournalRecord::Unsold { name, units } => grants.entry(name).or_default().holding.unsell(units),
                JournalRecord::GrantedVia { name, units, peer } => {
                    let g = grants.entry(name).or_default();
                    g.holding.grant(units);
                    g.copy_peer = Some(peer);
                }
                JournalRecord::PeerSold { owner, name, through } => {
                    let c = copies.entry((owner, name)).or_default();
                    c.through = c.through.max(through);
                }
                JournalRecord::Fenced { owner, name } => copies.entry((owner, name)).or_default().fenced = true,
                JournalRecord::Reclaimed { name } => {
                    grants.insert(name, LocalGrant::default());
                }
            }
        }
        let mut in_flight = std::collections::BTreeSet::new();
        for g in grants.values_mut() {
            g.holding.finish_replay();
            if let Some((p, _, _)) = g.holding.pending {
                in_flight.insert(p);
            }
        }
        let targets = grants
            .keys()
            .map(|name| (name.clone(), fields.remove(name).unwrap_or_else(|| Target::Named(name.clone()))))
            .collect();
        Ok(EscrowBook {
            targets: Mutex::new(targets),
            grants: Mutex::new(grants),
            journal: std::sync::Arc::new(Mutex::new(journal)),
            seq: Mutex::new(Sequencer { next: reserved.max(seq), reserved, durable: reserved, in_flight }),
            held_for_peers: Mutex::default(),
            copies: Mutex::new(copies),
        })
    }

    fn append(&self, record: &JournalRecord) -> std::io::Result<()> {
        let bytes = postcard::to_allocvec(record).map_err(|e| std::io::Error::other(e.to_string()))?;
        self.journal.lock().unwrap().append(&bytes)?;
        Ok(())
    }

    async fn durable(&self) -> std::io::Result<()> {
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || {
            let begun = journal.lock().unwrap().begin_sync()?;
            if let Some((ticket, sync)) = begun {
                sync()?;
                journal.lock().unwrap().finish_sync(ticket);
            }
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?
    }

    async fn record(&self, record: &JournalRecord) -> std::io::Result<()> {
        self.append(record)?;
        self.durable().await
    }

    async fn ticket(&self) -> std::io::Result<Ticket<'_>> {
        let (ticket, reserved, wait) = {
            let mut seq = self.seq.lock().unwrap();
            seq.next += 1;
            if seq.next > seq.reserved {
                let upto = seq.next + RESERVE_BLOCK;
                self.append(&JournalRecord::Reserved { upto })?;
                seq.reserved = upto;
            }
            let n = seq.next;
            seq.in_flight.insert(n);
            let ack_below = seq.in_flight.first().copied().unwrap_or(n);
            (Ticket { book: self, seq: n, ack_below }, seq.reserved, n > seq.durable)
        };
        if wait {
            self.durable().await?;
            let mut seq = self.seq.lock().unwrap();
            seq.durable = seq.durable.max(reserved);
        }
        Ok(ticket)
    }

    fn resume(&self, seq: u64) -> Ticket<'_> {
        let mut s = self.seq.lock().unwrap();
        s.in_flight.insert(seq);
        let ack_below = s.in_flight.first().copied().unwrap_or(seq);
        Ticket { book: self, seq, ack_below }
    }

    pub(crate) fn held_for(&self, owner: NodeId, name: &str) -> u64 {
        self.held_for_peers.lock().unwrap().get(&(owner, name.to_string())).copied().unwrap_or(0)
    }

    async fn unsell(&self, name: &str, units: u64) -> std::io::Result<()> {
        self.record(&JournalRecord::Unsold { name: name.to_string(), units }).await?;
        self.grants.lock().unwrap().entry(name.to_string()).or_default().holding.unsell(units);
        Ok(())
    }

    fn copied(&self, name: &str, units: u64) {
        let _ = self.append(&JournalRecord::Copied { name: name.to_string(), units });
        self.grants.lock().unwrap().entry(name.to_string()).or_default().holding.confirm(units);
    }

    pub(crate) async fn record_copy(&self, owner: NodeId, name: String, units: u64, through: u64) -> std::io::Result<bool> {
        {
            let mut copies = self.copies.lock().unwrap();
            let c = copies.entry((owner, name.clone())).or_default();
            if c.fenced {
                return Ok(false);
            }
            self.append(&JournalRecord::PeerSold { owner, name: name.clone(), through })?;
            c.through = c.through.max(through);
        }
        self.durable().await?;
        self.hold_for_peer(owner, name, units, false);
        Ok(true)
    }

    pub(crate) async fn fence(&self, owner: NodeId, name: &str) -> std::io::Result<u64> {
        let through = {
            let mut copies = self.copies.lock().unwrap();
            let c = copies.entry((owner, name.to_string())).or_default();
            if !c.fenced {
                self.append(&JournalRecord::Fenced { owner, name: name.to_string() })?;
                c.fenced = true;
            }
            c.through
        };
        self.durable().await?;
        Ok(through)
    }

    async fn drop_grant(&self, name: &str) -> std::io::Result<()> {
        self.record(&JournalRecord::Reclaimed { name: name.to_string() }).await?;
        self.grants.lock().unwrap().insert(name.to_string(), LocalGrant::default());
        Ok(())
    }

    pub(crate) fn hold_for_peer(&self, owner: NodeId, name: String, units: u64, settled: bool) {
        let mut held = self.held_for_peers.lock().unwrap();
        let slot = held.entry((owner, name)).or_default();
        if settled {
            *slot = slot.saturating_sub(units);
        } else {
            *slot += units;
        }
    }
}

type FieldReader = fn(Option<(u32, Vec<u8>)>, &str) -> Option<EscrowView>;

#[derive(Clone)]
enum Target {
    Named(String),
    Field {
        type_name: String,
        version: u32,
        key: Vec<u8>,
        locality: Option<String>,
        field: String,
        reader: Option<FieldReader>,
    },
}

pub struct EscrowCounter {
    backend: crate::entity::Backend,
    name: String,
    target: Target,
}

pub use quorium_core::escrow::ESCROW_METHOD;

impl EscrowCounter {
    pub(crate) fn for_field<T: crate::entity::State + crate::private::EscrowFields>(
        entity: &crate::entity::Entity<T>,
        field: &str,
    ) -> EscrowCounter {
        let (backend, key, locality) = entity.parts();
        let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        EscrowCounter {
            backend,
            name: format!("{}:{hex}:{field}", T::NAME),
            target: Target::Field {
                type_name: T::NAME.to_string(),
                version: T::VERSION,
                key,
                locality,
                field: field.to_string(),
                reader: Some(crate::private::read_escrow_field::<T>),
            },
        }
    }

    pub(crate) fn named(backend: crate::entity::Backend, name: &str) -> EscrowCounter {
        EscrowCounter { backend, name: name.to_string(), target: Target::Named(name.to_string()) }
    }

    fn node(&self) -> Option<&Quorium> {
        match &self.backend {
            crate::entity::Backend::Node(node) => Some(node),
            crate::entity::Backend::Sim(_) => None,
        }
    }

    fn owner(&self) -> NodeId {
        self.node().map_or(0, |n| n.status().id)
    }

    fn op(&self, action: EscrowAction) -> KvOp {
        match &self.target {
            Target::Named(name) => KvOp::Escrow(EscrowOp { name: name.clone(), node: self.owner(), action }),
            Target::Field { type_name, version, key, locality, field, .. } => {
                let args = postcard::to_allocvec(&(field.clone(), self.owner(), action))
                    .expect("escrow arguments always serialize");
                KvOp::Apply(quorium_core::entity::EntityCall {
                    type_name: type_name.clone(),
                    version: *version,
                    key: key.clone(),
                    method: ESCROW_METHOD,
                    args,
                    now_ms: 0,
                    locality: locality.clone(),
                })
            }
        }
    }

    fn outcome(reply: &Reply) -> Option<Result<(u64, u64), String>> {
        match reply {
            Reply::Escrow { granted, pool } => Some(Ok((*granted, *pool))),
            Reply::ApplyFailed(e) => Some(Err(e.clone())),
            Reply::Applied(bytes) => postcard::from_bytes::<Result<(u64, u64), String>>(bytes).ok(),
            _ => None,
        }
    }
}

impl Quorium {
    pub fn escrow_held_for(&self, owner: NodeId, name: &str) -> u64 {
        self.escrow_book().held_for(owner, name)
    }

    pub fn escrow(&self, name: &str) -> EscrowCounter {
        EscrowCounter::named(crate::entity::Backend::Node(self.clone()), name)
    }

    fn escrow_command(&self, op: KvOp, seq: u64) -> Command {
        Command::new(self.status().id ^ ESCROW_CLIENT_SALT, seq, op)
    }

    async fn escrow_write(&self, cmd: Command) -> Result<Reply, Error<Infallible>> {
        match self.route(|| Request::Write(cmd.clone())).await? {
            Response::Written(reply) => Ok(reply),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    pub(crate) async fn register_escrow(&self, counter: &EscrowCounter) -> std::io::Result<()> {
        let book = self.escrow_book();
        let known = book.targets.lock().unwrap().contains_key(&counter.name);
        if !known && let Target::Field { type_name, version, key, locality, field, .. } = &counter.target {
            book.record(&JournalRecord::Field {
                name: counter.name.clone(),
                type_name: type_name.clone(),
                version: *version,
                key: key.clone(),
                locality: locality.clone(),
                field: field.clone(),
            })
            .await?;
        }
        book.targets.lock().unwrap().insert(counter.name.clone(), counter.target.clone());
        Ok(())
    }

    pub(crate) async fn settle_escrows(&self) {
        let book = self.escrow_book();
        let names: Vec<String> = book.grants.lock().unwrap().keys().cloned().collect();
        for name in names {
            let Some(target) = book.targets.lock().unwrap().get(&name).cloned() else { continue };
            let counter =
                EscrowCounter { backend: crate::entity::Backend::Node(self.clone()), name: name.clone(), target };
            let due = {
                let grants = book.grants.lock().unwrap();
                grants.get(&name).is_some_and(|g| {
                    let h = &g.holding;
                    h.pending.is_some()
                        || h.unsettled > 0
                        || (h.remaining > 0 && g.last_take.is_none_or(|t| t.elapsed() > IDLE_RELEASE))
                })
            };
            if !due {
                continue;
            }
            let Ok(ticket) = book.ticket().await else { continue };
            let (pending, sent) = {
                let mut grants = book.grants.lock().unwrap();
                let g = grants.entry(name.clone()).or_default();
                let mut sent = false;
                let idle = g.last_take.is_none_or(|t| t.elapsed() > IDLE_RELEASE);
                let mut next = g.holding.clone();
                if let Some((sold, release)) = next.begin_settle(ticket.seq, idle) {
                    let record = JournalRecord::SettleSent { name: name.clone(), seq: ticket.seq, sold, release };
                    if book.append(&record).is_err() {
                        continue;
                    }
                    g.holding = next;
                    sent = true;
                }
                (g.holding.pending.map(|(seq, sold, release)| (seq, sold, release, g.holding.pending_through)), sent)
            };
            if sent {
                std::mem::forget(ticket);
                if book.durable().await.is_err() {
                    continue;
                }
            } else {
                drop(ticket);
            }
            let Some((seq, sold, release, through)) = pending else { continue };
            let ticket = book.resume(seq);
            let cmd = self
                .escrow_command(counter.op(EscrowAction::SettleThrough { through, release }), seq)
                .acknowledging_below(ticket.ack_below);
            let outcome = self.escrow_write(cmd).await;
            let settled = outcome.as_ref().ok().and_then(EscrowCounter::outcome);
            let Some(settled) = settled else {
                std::mem::forget(ticket);
                continue;
            };
            drop(ticket);
            if settled.as_ref().is_err_and(|e| e == RECLAIMED) {
                let _ = book.drop_grant(&name).await;
                continue;
            }
            {
                if book.record(&JournalRecord::SettleDone { name: name.clone(), seq }).await.is_ok()
                    && let Some(g) = book.grants.lock().unwrap().get_mut(&name)
                {
                    g.holding.settled(seq);
                }
                self.tell_peer(Request::EscrowSettled { owner: self.status().id, name: name.clone(), units: sold })
                    .await;
            }
        }
    }
}

fn counter_name(key: &EscrowKey) -> String {
    match key {
        EscrowKey::Named(name) => name.clone(),
        EscrowKey::Field { type_name, key, field, .. } => {
            let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
            format!("{type_name}:{hex}:{field}")
        }
    }
}

fn reclaim_op(key: &EscrowKey, node: NodeId, through: u64) -> Option<KvOp> {
    let action = EscrowAction::Reclaim { through };
    Some(match key {
        EscrowKey::Named(name) => KvOp::Escrow(EscrowOp { name: name.clone(), node, action }),
        EscrowKey::Field { type_name, key, locality, field } => {
            let version = quorium_core::entity::lookup(type_name)?.version;
            KvOp::Apply(quorium_core::entity::EntityCall {
                type_name: type_name.clone(),
                version,
                key: key.clone(),
                method: ESCROW_METHOD,
                args: postcard::to_allocvec(&(field.clone(), node, action)).ok()?,
                now_ms: 0,
                locality: locality.clone(),
            })
        }
    })
}

impl Quorium {
    pub(crate) async fn reclaim_escrows(&self) {
        let me = self.status().id;
        for shard in 0..self.shard_count() {
            let members = self.shard_members(shard);
            if !members.contains(&me) {
                continue;
            }
            for (key, node, holder) in self.escrow_holders(shard).await {
                if holder.reclaimed || holder.held == 0 || members.contains(&node) {
                    continue;
                }
                let name = counter_name(&key);
                let through = match holder.copy_peer {
                    Some(peer) if peer == me => match self.escrow_book().fence(node, &name).await {
                        Ok(through) => through,
                        Err(_) => continue,
                    },
                    Some(peer) if members.contains(&peer) => continue,
                    _ if members.first() == Some(&me) => u64::MAX,
                    _ => continue,
                };
                let Some(op) = reclaim_op(&key, node, through) else { continue };
                let book = self.escrow_book();
                let Ok(ticket) = book.ticket().await else { continue };
                let cmd = self.escrow_command(op, ticket.seq).acknowledging_below(ticket.ack_below);
                let _ = self.escrow_write(cmd).await;
            }
        }
    }
}

impl EscrowCounter {
    pub fn local_remaining(&self) -> u64 {
        self.node().map_or(0, |n| n.escrow_book().grants.lock().unwrap().get(&self.name).map_or(0, |g| g.holding.remaining))
    }

    async fn take_locally(&self, node: &Quorium, units: u64, peer: Option<NodeId>) -> Result<Taken, Error<Infallible>> {
        let book = node.escrow_book();
        let through = {
            let mut grants = book.grants.lock().unwrap();
            let g = grants.entry(self.name.clone()).or_default();
            if g.holding.remaining > 0 && g.copy_peer != peer {
                g.last_take = None;
                return Ok(Taken::OtherPeer);
            }
            if g.holding.remaining < units {
                return Ok(Taken::Exhausted);
            }
            book.append(&JournalRecord::Sold { name: self.name.clone(), units })
                .map_err(|e| Error::Internal(e.to_string()))?;
            g.holding.take(units);
            g.last_take = Some(Instant::now());
            g.holding.taken_total()
        };
        book.durable().await.map_err(|e| Error::Internal(e.to_string()))?;
        Ok(Taken::Yes(through))
    }

    async fn submit(&self, op: KvOp) -> Result<Reply, Error<Infallible>> {
        match &self.backend {
            crate::entity::Backend::Node(node) => {
                let (cmd, _in_flight) = node.next_command(op);
                node.escrow_write(cmd).await
            }
            crate::entity::Backend::Sim(sim) => sim.submit(op),
        }
    }

    async fn sell_locally(&self, node: &Quorium, units: u64) -> Result<Option<()>, Error<Infallible>> {
        let (peer, addr) = match node.escrow_copy() {
            Copy::Peer(id, addr) => (Some(id), Some(addr)),
            Copy::Alone => (None, None),
            Copy::Unavailable => return Ok(None),
        };
        let book = node.escrow_book();
        let internal = |e: std::io::Error| Error::Internal(e.to_string());
        for _ in 0..3 {
            match self.take_locally(node, units, peer).await? {
                Taken::Yes(_) if addr.is_none() => {
                    book.copied(&self.name, units);
                    return Ok(Some(()));
                }
                Taken::Yes(through) => {
                    let addr = addr.as_deref().unwrap_or_default();
                    let sale = Request::EscrowSold { owner: node.status().id, name: self.name.clone(), units, through };
                    return match node.copy_sale(addr, sale).await {
                        Some(true) => {
                            book.copied(&self.name, units);
                            Ok(Some(()))
                        }
                        Some(false) => {
                            book.unsell(&self.name, units).await.map_err(internal)?;
                            book.drop_grant(&self.name).await.map_err(internal)?;
                            Ok(None)
                        }
                        None => {
                            book.unsell(&self.name, units).await.map_err(internal)?;
                            Err(Error::Unavailable)
                        }
                    };
                }
                Taken::OtherPeer => return Ok(None),
                Taken::Exhausted => {}
            }
            let ticket = book.ticket().await.map_err(internal)?;
            let want = units.max(8);
            let action = match peer {
                Some(peer) => EscrowAction::GrantVia { want, peer },
                None => EscrowAction::Grant { want },
            };
            let cmd = node.escrow_command(self.op(action), ticket.seq).acknowledging_below(ticket.ack_below);
            let reply = node.escrow_write(cmd).await;
            drop(ticket);
            match reply.map(|r| (EscrowCounter::outcome(&r), r))? {
                (Some(Ok((granted, _))), _) if granted > 0 => {
                    let record = match peer {
                        Some(peer) => JournalRecord::GrantedVia { name: self.name.clone(), units: granted, peer },
                        None => JournalRecord::Granted { name: self.name.clone(), units: granted },
                    };
                    book.record(&record).await.map_err(internal)?;
                    let mut grants = book.grants.lock().unwrap();
                    let g = grants.entry(self.name.clone()).or_default();
                    g.holding.grant(granted);
                    g.copy_peer = peer;
                }
                (Some(Ok(_)), _) => return Ok(None),
                (Some(Err(e)), _) => return Err(Error::Internal(e)),
                (None, other) => return Err(Error::Internal(format!("unexpected reply {other:?}"))),
            }
        }
        Ok(None)
    }

    pub async fn try_take(&self, units: u64) -> Result<(), Error<Infallible>> {
        let Some(node) = self.node() else {
            let reply = self.submit(self.op(EscrowAction::Buy { units })).await?;
            return match EscrowCounter::outcome(&reply) {
                Some(Ok((granted, _))) if granted == units => Ok(()),
                Some(Ok(_)) => Err(Error::SoldOut),
                Some(Err(e)) => Err(Error::Internal(e)),
                None => Err(Error::Internal(format!("unexpected reply {reply:?}"))),
            };
        };
        node.register_escrow(self).await.map_err(|e| Error::Internal(e.to_string()))?;
        if self.sell_locally(node, units).await?.is_some() {
            return Ok(());
        }
        let book = node.escrow_book();
        let ticket = book.ticket().await.map_err(|e| Error::Internal(e.to_string()))?;
        let cmd =
            node.escrow_command(self.op(EscrowAction::Buy { units }), ticket.seq).acknowledging_below(ticket.ack_below);
        let reply = node.escrow_write(cmd).await;
        drop(ticket);
        match reply.map(|r| (EscrowCounter::outcome(&r), r))? {
            (Some(Ok((granted, _))), _) if granted == units => {
                let _ = book.record(&JournalRecord::Bought { name: self.name.clone(), units }).await;
                Ok(())
            }
            (Some(Ok(_)), _) => Err(Error::SoldOut),
            (Some(Err(e)), _) => Err(Error::Internal(e)),
            (None, other) => Err(Error::Internal(format!("unexpected reply {other:?}"))),
        }
    }

    pub async fn refill(&self, units: u64) -> Result<u64, Error<Infallible>> {
        let reply = self.submit(self.op(EscrowAction::Refill { units })).await?;
        match EscrowCounter::outcome(&reply) {
            Some(Ok((_, pool))) => Ok(pool),
            Some(Err(e)) => Err(Error::Internal(e)),
            None => Err(Error::Internal(format!("unexpected reply {reply:?}"))),
        }
    }

    async fn view(&self) -> Result<EscrowView, Error<Infallible>> {
        let node = match &self.backend {
            crate::entity::Backend::Node(node) => node,
            crate::entity::Backend::Sim(sim) => {
                return match &self.target {
                    Target::Named(name) => sim.escrow_view(name),
                    Target::Field { type_name, key, field, reader, .. } => {
                        let stored = sim.entity_state(type_name, key)?;
                        Ok(reader.and_then(|read| read(stored, field)).unwrap_or_default())
                    }
                };
            }
        };
        match &self.target {
            Target::Named(name) => match node.route(|| Request::ReadEscrow { name: name.clone() }).await? {
                Response::Escrow(v) => Ok(v.unwrap_or_default()),
                other => Err(Error::Internal(format!("unexpected response {other:?}"))),
            },
            Target::Field { type_name, key, locality, field, reader, .. } => {
                let request = || Request::ReadEntity {
                    type_name: type_name.clone(),
                    key: key.clone(),
                    locality: locality.clone(),
                };
                match node.route(request).await? {
                    Response::Entity(stored) => match reader {
                        Some(read) => Ok(read(stored, field).unwrap_or_default()),
                        None => Err(Error::Internal("escrow field reader is unavailable".into())),
                    },
                    other => Err(Error::Internal(format!("unexpected response {other:?}"))),
                }
            }
        }
    }

    pub async fn estimate(&self) -> Result<u64, Error<Infallible>> {
        Ok(self.view().await?.estimate())
    }

    pub async fn sold(&self) -> Result<u64, Error<Infallible>> {
        Ok(self.view().await?.sold)
    }
}
