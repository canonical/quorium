use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{self, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use openraft::alias::{LogIdOf, SnapshotOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::impls::EntryPayload;
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine, Snapshot,
};
use openraft::{AsyncRuntime, OptionalSend, RaftTypeConfig, StoredMembership};
use quorium_core::kv::{KvState, Reply, RowKey, Touch};
use quorium_core::types::Types;

struct LogData<C: RaftTypeConfig> {
    vote: Option<VoteOf<C>>,
    committed: Option<LogIdOf<C>>,
    purged: Option<LogIdOf<C>>,
    entries: BTreeMap<u64, C::Entry>,
}

impl<C: RaftTypeConfig<Entry: Clone>> Clone for LogData<C> {
    fn clone(&self) -> Self {
        Self {
            vote: self.vote.clone(),
            committed: self.committed.clone(),
            purged: self.purged.clone(),
            entries: self.entries.clone(),
        }
    }
}

impl<C: RaftTypeConfig<Entry: Clone>> Default for LogData<C> {
    fn default() -> Self {
        Self { vote: None, committed: None, purged: None, entries: BTreeMap::new() }
    }
}

impl<C: RaftTypeConfig<Entry: Clone>> LogData<C> {
    fn truncate_after(&mut self, last: Option<&LogIdOf<C>>) {
        let from = last.map_or(0, |l| l.index() + 1);
        self.entries.split_off(&from);
    }

    fn purge(&mut self, upto: &LogIdOf<C>) {
        let keep = self.entries.split_off(&(upto.index() + 1));
        self.entries = keep;
        if self.purged.as_ref().is_none_or(|p| p < upto) {
            self.purged = Some(upto.clone());
        }
    }
}

type Pending<C> = (Vec<<C as RaftTypeConfig>::Entry>, IOFlushed<C>);

struct Shared<C: RaftTypeConfig> {
    epoch: u64,
    live: LogData<C>,
    durable: LogData<C>,
    pending: Vec<Pending<C>>,
    flushing: bool,
    fsyncs: u64,
    lose_fsyncs: bool,
}

impl<C: RaftTypeConfig<Entry: Clone>> Shared<C> {
    fn persist(&mut self, entries: &[C::Entry]) {
        if self.lose_fsyncs {
            return;
        }
        for e in entries {
            let id = e.log_id();
            if self.live.entries.get(&id.index()).map(|l| l.log_id()) == Some(id.clone()) {
                self.durable.entries.insert(id.index(), e.clone());
            }
        }
    }
}

pub struct MemLog<C: RaftTypeConfig> {
    shared: Arc<Mutex<Shared<C>>>,
    epoch: u64,
    flush_delay: Duration,
}

impl<C: RaftTypeConfig<Entry: Clone>> Clone for MemLog<C> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone(), epoch: self.epoch, flush_delay: self.flush_delay }
    }
}

impl<C: RaftTypeConfig<Entry: Clone>> Default for MemLog<C> {
    fn default() -> Self {
        Self::with_flush_delay(Duration::ZERO)
    }
}

fn fenced() -> io::Error {
    io::Error::other("log handle was fenced by a crash")
}

impl<C: RaftTypeConfig<Entry: Clone>> MemLog<C> {
    pub fn with_flush_delay(flush_delay: Duration) -> Self {
        let shared = Shared {
            epoch: 0,
            live: LogData::default(),
            durable: LogData::default(),
            pending: Vec::new(),
            flushing: false,
            fsyncs: 0,
            lose_fsyncs: false,
        };
        Self { shared: Arc::new(Mutex::new(shared)), epoch: 0, flush_delay }
    }

    pub fn crash(&self) -> Self {
        let mut s = self.shared.lock().unwrap();
        s.epoch += 1;
        let covered = s.durable.committed.as_ref().is_none_or(|c| {
            s.durable.entries.get(&c.index()).is_some_and(|e| e.log_id() == *c)
                || s.durable.purged.as_ref().is_some_and(|p| p.index() >= c.index())
        });
        if !covered {
            s.durable.committed = None;
        }
        s.live = s.durable.clone();
        s.pending.clear();
        s.flushing = false;
        Self { shared: self.shared.clone(), epoch: s.epoch, flush_delay: self.flush_delay }
    }

    pub fn with_lost_fsync(self, lose: bool) -> Self {
        self.shared.lock().unwrap().lose_fsyncs = lose;
        self
    }

    pub fn forget_vote(self) -> Self {
        {
            let mut s = self.shared.lock().unwrap();
            s.live.vote = None;
            s.durable.vote = None;
        }
        self
    }

    pub fn fsync_count(&self) -> u64 {
        self.shared.lock().unwrap().fsyncs
    }

    pub fn stored_vote(&self) -> Option<VoteOf<C>> {
        self.shared.lock().unwrap().live.vote.clone()
    }

    pub fn log_ids(&self) -> Vec<LogIdOf<C>> {
        let s = self.shared.lock().unwrap();
        s.live.entries.values().map(|e| e.log_id()).collect()
    }

    pub fn durable_last_index(&self) -> Option<u64> {
        let s = self.shared.lock().unwrap();
        s.durable.entries.keys().next_back().copied()
    }

    fn with<T>(&self, f: impl FnOnce(&mut Shared<C>) -> T) -> Result<T, io::Error> {
        let mut s = self.shared.lock().unwrap();
        if s.epoch != self.epoch {
            return Err(fenced());
        }
        Ok(f(&mut s))
    }

    fn spawn_flusher(&self) {
        let shared = self.shared.clone();
        let epoch = self.epoch;
        let delay = self.flush_delay;
        drop(C::AsyncRuntime::spawn(async move {
            loop {
                let batch = {
                    let mut s = shared.lock().unwrap();
                    if s.epoch != epoch || s.pending.is_empty() {
                        s.flushing = false;
                        return;
                    }
                    std::mem::take(&mut s.pending)
                };
                C::AsyncRuntime::sleep(delay).await;
                let callbacks = {
                    let mut s = shared.lock().unwrap();
                    if s.epoch != epoch {
                        return;
                    }
                    s.fsyncs += 1;
                    batch
                        .into_iter()
                        .map(|(entries, cb)| {
                            s.persist(&entries);
                            cb
                        })
                        .collect::<Vec<_>>()
                };
                for cb in callbacks {
                    cb.io_completed(Ok(()));
                }
            }
        }));
    }
}

impl<C: RaftTypeConfig<Entry: Clone>> RaftLogReader<C> for MemLog<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        self.with(|s| s.live.entries.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        self.with(|s| s.live.vote.clone())
    }
}

impl<C: RaftTypeConfig<Entry: Clone>> RaftLogStorage<C> for MemLog<C> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        self.with(|s| {
            let last = s.live.entries.values().next_back().map(|e| e.log_id()).or_else(|| s.live.purged.clone());
            LogState { last_purged_log_id: s.live.purged.clone(), last_log_id: last }
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        self.with(|s| {
            s.live.vote = Some(vote.clone());
            s.durable.vote = Some(vote.clone());
        })
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        self.with(|s| {
            s.live.committed = committed.clone();
            s.durable.committed = committed;
        })
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
        self.with(|s| s.live.committed.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<C::Entry> = entries.into_iter().collect();
        if self.flush_delay.is_zero() {
            self.with(|s| {
                for e in &entries {
                    s.live.entries.insert(e.log_id().index(), e.clone());
                }
                s.fsyncs += 1;
                s.persist(&entries);
            })?;
            callback.io_completed(Ok(()));
            return Ok(());
        }
        let start = self.with(|s| {
            for e in &entries {
                s.live.entries.insert(e.log_id().index(), e.clone());
            }
            s.pending.push((entries, callback));
            !std::mem::replace(&mut s.flushing, true)
        })?;
        if start {
            self.spawn_flusher();
        }
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        self.with(|s| {
            s.live.truncate_after(last_log_id.as_ref());
            s.durable.truncate_after(last_log_id.as_ref());
        })
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        self.with(|s| {
            s.live.purge(&log_id);
            s.durable.purge(&log_id);
        })
    }
}

pub type ApplyObserver = Arc<dyn Fn(u64, u64) + Send + Sync>;
pub type ChangeObserver = Arc<dyn Fn(&str, &[u8], Option<(u32, &[u8])>) + Send + Sync>;

type KvLogId = quorium_core::types::LogId;
type KvMembership = StoredMembership<quorium_core::types::CommittedLeaderId, u64, openraft::impls::BasicNode>;
pub type KvSnapshotMeta =
    openraft::SnapshotMeta<quorium_core::types::CommittedLeaderId, u64, openraft::impls::BasicNode>;

#[derive(Default)]
struct SmInner {
    state: KvState,
    last_applied: Option<KvLogId>,
    membership: KvMembership,
    snapshot: Option<(KvSnapshotMeta, Vec<u8>)>,
    observer: Option<ApplyObserver>,
    changes: Option<ChangeObserver>,
    store: Option<Arc<dyn Storage>>,
    dirty: Changes,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changes {
    pub all: bool,
    pub rows: std::collections::BTreeSet<RowKey>,
}

impl Changes {
    pub fn record(&mut self, touch: Touch) {
        match touch {
            Touch::All => self.all = true,
            Touch::Row(key) if !self.all => {
                self.rows.insert(key);
            }
            Touch::Row(_) | Touch::Meta => {}
        }
    }

    pub fn everything() -> Self {
        Self { all: true, rows: Default::default() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rows {
    All(Vec<(RowKey, Vec<u8>)>),
    Changed(Vec<(RowKey, Option<Vec<u8>>)>),
}

impl Rows {
    pub fn capture(state: &KvState, changes: &Changes) -> Rows {
        if changes.all {
            return Rows::All(state.rows());
        }
        Rows::Changed(std::iter::once(&RowKey::Meta).chain(&changes.rows).map(|k| (k.clone(), state.row(k))).collect())
    }
}

pub struct Checkpoint<'a> {
    pub meta: &'a KvSnapshotMeta,
    pub rows: &'a Rows,
    pub bytes: &'a [u8],
}

pub trait Storage: Send + Sync + 'static {
    fn save(&self, checkpoint: Checkpoint<'_>) -> io::Result<()>;
    fn load(&self) -> io::Result<Option<(KvSnapshotMeta, KvState)>>;

    fn wants_rows(&self) -> bool {
        false
    }

    fn needs_full_rewrite(&self) -> bool {
        false
    }
}

fn decode_state(data: &[u8]) -> io::Result<KvState> {
    KvState::from_bytes(data).map_err(|_| invalid("snapshot state is undecodable"))
}

pub struct FileSnapshotStore {
    dir: PathBuf,
}

impl FileSnapshotStore {
    pub fn new(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self { dir: dir.to_path_buf() })
    }
}

impl Storage for FileSnapshotStore {
    fn save(&self, checkpoint: Checkpoint<'_>) -> io::Result<()> {
        write_snapshot(&self.dir, checkpoint.meta, checkpoint.bytes)
    }

    fn load(&self) -> io::Result<Option<(KvSnapshotMeta, KvState)>> {
        read_snapshot(&self.dir)?.map(|f| Ok((f.meta, decode_state(&f.data)?))).transpose()
    }
}

type SnapshotSlot = Arc<Mutex<Option<(KvSnapshotMeta, Vec<u8>)>>>;

#[derive(Clone, Default)]
pub struct MemSnapshotStore {
    slot: SnapshotSlot,
    saves: Arc<std::sync::atomic::AtomicU64>,
}

impl MemSnapshotStore {
    pub fn saves(&self) -> u64 {
        self.saves.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Storage for MemSnapshotStore {
    fn save(&self, checkpoint: Checkpoint<'_>) -> io::Result<()> {
        *self.slot.lock().unwrap() = Some((checkpoint.meta.clone(), checkpoint.bytes.to_vec()));
        self.saves.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn load(&self) -> io::Result<Option<(KvSnapshotMeta, KvState)>> {
        let slot = self.slot.lock().unwrap().clone();
        slot.map(|(meta, data)| Ok((meta, decode_state(&data)?))).transpose()
    }
}

const SNAPSHOT_MAGIC: &[u8; 4] = b"QSNP";

#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotFile {
    meta: KvSnapshotMeta,
    data: Vec<u8>,
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

fn write_snapshot(dir: &Path, meta: &KvSnapshotMeta, data: &[u8]) -> io::Result<()> {
    let body = postcard::to_allocvec(&SnapshotFile { meta: meta.clone(), data: data.to_vec() })
        .map_err(|e| io::Error::other(e.to_string()))?;
    let mut bytes = SNAPSHOT_MAGIC.to_vec();
    bytes.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    bytes.extend_from_slice(&body);
    let tmp = dir.join("snapshot.tmp");
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp, dir.join("snapshot"))?;
    std::fs::File::open(dir)?.sync_all()
}

fn read_snapshot(dir: &Path) -> io::Result<Option<SnapshotFile>> {
    let bytes = match std::fs::read(dir.join("snapshot")) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if bytes.len() < 8 || &bytes[..4] != SNAPSHOT_MAGIC {
        return Err(invalid("snapshot header is damaged"));
    }
    let crc = u32::from_le_bytes(bytes[4..8].try_into().expect("four bytes"));
    if crc32fast::hash(&bytes[8..]) != crc {
        return Err(invalid("snapshot checksum mismatch"));
    }
    postcard::from_bytes(&bytes[8..]).map(Some).map_err(|_| invalid("snapshot body is undecodable"))
}

#[derive(Clone, Default)]
pub struct KvStateMachine {
    inner: Arc<Mutex<SmInner>>,
}

impl KvStateMachine {
    pub fn with_observer(observer: ApplyObserver) -> Self {
        let sm = Self::default();
        sm.inner.lock().unwrap().observer = Some(observer);
        sm
    }

    pub fn persistent(dir: &Path) -> io::Result<Self> {
        Self::with_store(Arc::new(FileSnapshotStore::new(dir)?))
    }

    pub fn with_store(store: Arc<dyn Storage>) -> io::Result<Self> {
        let sm = Self::default();
        {
            let mut inner = sm.inner.lock().unwrap();
            if let Some((meta, state)) = store.load()? {
                let data = state.to_bytes();
                inner.state = state;
                inner.last_applied = meta.last_log_id;
                inner.membership = meta.last_membership.clone();
                inner.snapshot = Some((meta, data));
            }
            if store.needs_full_rewrite() {
                inner.dirty = Changes::everything();
            }
            inner.store = Some(store);
        }
        Ok(sm)
    }

    pub fn set_change_observer(&self, observer: ChangeObserver) {
        self.inner.lock().unwrap().changes = Some(observer);
    }

    pub fn set_observer(&self, observer: ApplyObserver) {
        self.inner.lock().unwrap().observer = Some(observer);
    }

    pub fn read<T>(&self, f: impl FnOnce(&KvState) -> T) -> T {
        f(&self.inner.lock().unwrap().state)
    }

    pub fn unknown_types(&self) -> Vec<String> {
        self.read(KvState::unknown_types)
    }

    pub fn last_applied_index(&self) -> Option<u64> {
        self.inner.lock().unwrap().last_applied.as_ref().map(|l| l.index)
    }

    pub fn state_hash(&self) -> u64 {
        self.read(KvState::state_hash)
    }

    fn snapshot_now(&self) -> io::Result<(KvSnapshotMeta, Vec<u8>)> {
        let (meta, bytes, pending) = {
            let mut inner = self.inner.lock().unwrap();
            let meta = KvSnapshotMeta { last_log_id: inner.last_applied, last_membership: inner.membership.clone() };
            let bytes = inner.state.to_bytes();
            let pending = inner.store.clone().map(|store| {
                let rows = if store.wants_rows() {
                    Rows::capture(&inner.state, &inner.dirty)
                } else {
                    Rows::Changed(Vec::new())
                };
                (store, rows, std::mem::take(&mut inner.dirty))
            });
            (meta, bytes, pending)
        };
        if let Some((store, rows, dirty)) = pending
            && let Err(e) = store.save(Checkpoint { meta: &meta, rows: &rows, bytes: &bytes })
        {
            let mut inner = self.inner.lock().unwrap();
            inner.dirty.all |= dirty.all;
            inner.dirty.rows.extend(dirty.rows);
            return Err(e);
        }
        self.inner.lock().unwrap().snapshot = Some((meta.clone(), bytes.clone()));
        Ok((meta, bytes))
    }
}

impl<RT: AsyncRuntime> RaftSnapshotBuilder<Types<RT>> for KvStateMachine {
    type SnapshotData = Vec<u8>;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<Types<RT>, Vec<u8>>, io::Error> {
        let sm = self.clone();
        let (meta, snapshot) = RT::spawn_blocking(move || sm.snapshot_now())
            .await
            .unwrap_or_else(|e| Err(io::Error::other(e.to_string())))?;
        Ok(Snapshot { meta, snapshot })
    }
}

impl<RT: AsyncRuntime> RaftStateMachine<Types<RT>> for KvStateMachine {
    type SnapshotData = Vec<u8>;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<KvLogId>, KvMembership), io::Error> {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<Types<RT>>, io::Error>> + Unpin + OptionalSend,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let (reply, observed) = {
                let mut inner = self.inner.lock().unwrap();
                let reply = match &entry.payload {
                    EntryPayload::Blank => Reply::Done,
                    EntryPayload::Normal(cmd) => {
                        inner.dirty.record(cmd.op.touches());
                        inner.state.apply_at(cmd, entry.log_id.index).map_err(|e| io::Error::other(e.to_string()))?
                    }
                    EntryPayload::Membership(m) => {
                        inner.membership = StoredMembership::new(Some(entry.log_id), m.clone());
                        Reply::Done
                    }
                };
                inner.last_applied = Some(entry.log_id);
                if let (Some(changes), EntryPayload::Normal(cmd)) = (inner.changes.clone(), &entry.payload) {
                    let touched = match &cmd.op {
                        quorium_core::kv::KvOp::Apply(call) => Some((call.type_name.as_str(), call.key.as_slice())),
                        quorium_core::kv::KvOp::DeleteEntity { type_name, key, .. } => {
                            Some((type_name.as_str(), key.as_slice()))
                        }
                        _ => None,
                    };
                    if let Some((type_name, key)) = touched {
                        changes(type_name, key, inner.state.entity(type_name, key));
                    }
                }
                let observed = inner.observer.clone().map(|o| (o, inner.state.state_hash()));
                (reply, observed)
            };
            if let Some((observer, hash)) = observed {
                observer(entry.log_id.index, hash);
            }
            if let Some(responder) = responder {
                responder.send(reply);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(&mut self, meta: &KvSnapshotMeta, snapshot: Vec<u8>) -> Result<(), io::Error> {
        let state = KvState::from_bytes(&snapshot).map_err(io::Error::other)?;
        let store = self.inner.lock().unwrap().store.clone();
        if let Some(store) = store {
            let rows = if store.wants_rows() { Rows::All(state.rows()) } else { Rows::Changed(Vec::new()) };
            let (meta, bytes) = (meta.clone(), snapshot.clone());
            RT::spawn_blocking(move || store.save(Checkpoint { meta: &meta, rows: &rows, bytes: &bytes }))
                .await
                .unwrap_or_else(|e| Err(io::Error::other(e.to_string())))?;
        }
        let mut inner = self.inner.lock().unwrap();
        inner.dirty = Changes::default();
        inner.state = state;
        inner.last_applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some((meta.clone(), snapshot));
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<Types<RT>, Vec<u8>>>, io::Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.snapshot.clone().map(|(meta, snapshot)| Snapshot { meta, snapshot }))
    }
}
