use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{AsyncRuntime, OptionalSend};
use quorium_core::types::{KvEntry, KvVote, LogId, Types};
use serde::{Deserialize, Serialize};

use crate::disk::{Disk, MemDisk};
use crate::wal::{Wal, WalError};

const RECORD_FORMAT: u8 = 1;
const DEFAULT_COMPACTION_THRESHOLD: u64 = 64 * 1024 * 1024;

type Lock<T> = tokio::sync::Mutex<T>;

#[derive(Default)]
struct Knobs {
    fsyncs: AtomicU64,
    sync_latency_us: AtomicU64,
    disk_len: AtomicU64,
    flush_delay_us: AtomicU64,
    compaction_threshold: AtomicU64,
}

#[derive(Debug, Serialize, Deserialize)]
enum Record {
    Entries { shard: u64, entries: Vec<KvEntry> },
    Vote { shard: u64, vote: KvVote },
    Committed { shard: u64, log_id: Option<LogId> },
    Truncate { shard: u64, after: Option<LogId> },
    Purge { shard: u64, upto: LogId },
}

impl Record {
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![RECORD_FORMAT];
        out.extend(postcard::to_allocvec(self).expect("wal records always serialize"));
        out
    }

    fn decode(bytes: &[u8]) -> Option<Record> {
        match bytes.split_first() {
            Some((&RECORD_FORMAT, rest)) => postcard::from_bytes(rest).ok(),
            _ => None,
        }
    }
}

#[derive(Default)]
struct ShardState {
    vote: Option<KvVote>,
    committed: Option<LogId>,
    purged: Option<LogId>,
    entries: BTreeMap<u64, KvEntry>,
}

impl ShardState {
    fn apply(&mut self, record: Record) {
        match record {
            Record::Entries { entries, .. } => {
                for e in entries {
                    self.entries.insert(e.log_id.index, e);
                }
            }
            Record::Vote { vote, .. } => self.vote = Some(vote),
            Record::Committed { log_id, .. } => self.committed = log_id,
            Record::Truncate { after, .. } => {
                self.entries.split_off(&after.map_or(0, |l| l.index + 1));
            }
            Record::Purge { upto, .. } => {
                self.entries = self.entries.split_off(&(upto.index + 1));
                if self.purged.is_none_or(|p| p < upto) {
                    self.purged = Some(upto);
                }
            }
        }
    }
}

fn shard_of(record: &Record) -> u64 {
    match record {
        Record::Entries { shard, .. }
        | Record::Vote { shard, .. }
        | Record::Committed { shard, .. }
        | Record::Truncate { shard, .. }
        | Record::Purge { shard, .. } => *shard,
    }
}

struct Inner<RT: AsyncRuntime, D: Disk> {
    wal: Option<Wal<D>>,
    shards: BTreeMap<u64, ShardState>,
    pending: Vec<IOFlushed<Types<RT>>>,
    flushing: bool,
    paused: bool,
    failed: Option<String>,
    knobs: Arc<Knobs>,
}

impl<RT: AsyncRuntime, D: Disk> Inner<RT, D> {
    fn wal(&mut self) -> io::Result<&mut Wal<D>> {
        if let Some(reason) = &self.failed {
            return Err(io::Error::other(format!("wal failed earlier: {reason}")));
        }
        self.wal.as_mut().ok_or_else(|| io::Error::other("wal is closed"))
    }

    fn write(&mut self, record: Record) -> io::Result<()> {
        let bytes = record.encode();
        let wal = self.wal()?;
        wal.append(&bytes)?;
        let len = wal.disk_len();
        self.knobs.disk_len.store(len, Ordering::Relaxed);
        let shard = shard_of(&record);
        self.shards.entry(shard).or_default().apply(record);
        Ok(())
    }

    fn compact(&mut self) -> io::Result<()> {
        let mut records = Vec::new();
        for (shard, state) in &self.shards {
            let shard = *shard;
            if let Some(vote) = state.vote {
                records.push(Record::Vote { shard, vote }.encode());
            }
            if let Some(upto) = state.purged {
                records.push(Record::Purge { shard, upto }.encode());
            }
            if !state.entries.is_empty() {
                let entries = state.entries.values().cloned().collect();
                records.push(Record::Entries { shard, entries }.encode());
            }
            if state.committed.is_some() {
                records.push(Record::Committed { shard, log_id: state.committed }.encode());
            }
        }
        let wal = self.wal()?;
        wal.replace(&records)?;
        let len = wal.disk_len();
        self.knobs.disk_len.store(len, Ordering::Relaxed);
        Ok(())
    }

    fn begin_sync(&mut self) -> io::Result<Option<(crate::wal::SyncTicket, crate::disk::Syncer)>> {
        self.wal()?.begin_sync().map_err(Into::into)
    }

    fn finish_sync(
        &mut self,
        ticket: crate::wal::SyncTicket,
        result: io::Result<()>,
        took: std::time::Duration,
    ) -> io::Result<()> {
        match result {
            Ok(()) => {
                self.wal()?.finish_sync(ticket);
                let count = self.knobs.fsyncs.fetch_add(1, Ordering::Relaxed);
                let took = took.as_micros() as u64;
                let old = self.knobs.sync_latency_us.load(Ordering::Relaxed);
                let smoothed = if count == 0 { took } else { (old * 7 + took * 3) / 10 };
                self.knobs.sync_latency_us.store(smoothed, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.failed = Some(e.to_string());
                Err(e)
            }
        }
    }
}

fn sync_unlocked<RT: AsyncRuntime, D: Disk>(inner: &Lock<Inner<RT, D>>) -> io::Result<()> {
    let begun = inner.blocking_lock().begin_sync()?;
    match begun {
        None => Ok(()),
        Some((ticket, sync)) => {
            let started = std::time::Instant::now();
            let result = sync();
            inner.blocking_lock().finish_sync(ticket, result, started.elapsed())
        }
    }
}

pub struct SharedWal<RT: AsyncRuntime, D: Disk> {
    inner: Arc<Lock<Inner<RT, D>>>,
    knobs: Arc<Knobs>,
}

impl<RT: AsyncRuntime, D: Disk> Clone for SharedWal<RT, D> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), knobs: self.knobs.clone() }
    }
}

impl<RT: AsyncRuntime, D: Disk> SharedWal<RT, D> {
    pub fn open(disk: D) -> Result<Self, WalError> {
        let mut wal = Wal::open(disk)?;
        let mut shards: BTreeMap<u64, ShardState> = BTreeMap::new();
        for (i, bytes) in wal.take_records().into_iter().enumerate() {
            let record =
                Record::decode(&bytes).ok_or(WalError::Corrupt { offset: i as u64, reason: "undecodable record" })?;
            shards.entry(shard_of(&record)).or_default().apply(record);
        }
        let knobs = Arc::new(Knobs::default());
        knobs.compaction_threshold.store(DEFAULT_COMPACTION_THRESHOLD, Ordering::Relaxed);
        knobs.disk_len.store(wal.disk_len(), Ordering::Relaxed);
        Ok(Self {
            inner: Arc::new(Lock::new(Inner {
                wal: Some(wal),
                shards,
                pending: Vec::new(),
                flushing: false,
                paused: false,
                failed: None,
                knobs: knobs.clone(),
            })),
            knobs,
        })
    }

    pub fn shard(&self, id: u64) -> WalLog<RT, D> {
        WalLog { shard: id, shared: self.clone(), _rt: PhantomData }
    }

    fn locked(&self) -> tokio::sync::MutexGuard<'_, Inner<RT, D>> {
        loop {
            if let Ok(guard) = self.inner.try_lock() {
                return guard;
            }
            std::thread::yield_now();
        }
    }

    pub fn shard_ids(&self) -> Vec<u64> {
        self.locked().shards.keys().copied().collect()
    }

    pub fn compact(&self) -> io::Result<()> {
        self.locked().compact()
    }

    pub fn disk_len(&self) -> u64 {
        self.knobs.disk_len.load(Ordering::Relaxed)
    }

    pub fn set_compaction_threshold(&self, bytes: u64) {
        self.knobs.compaction_threshold.store(bytes, Ordering::Relaxed);
    }

    pub fn set_flush_delay(&self, delay: Duration) {
        self.knobs.flush_delay_us.store(delay.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn flush_delay(&self) -> Duration {
        Duration::from_micros(self.knobs.flush_delay_us.load(Ordering::Relaxed))
    }

    pub fn sync_latency(&self) -> Duration {
        Duration::from_micros(self.knobs.sync_latency_us.load(Ordering::Relaxed))
    }

    pub fn fsyncs(&self) -> u64 {
        self.knobs.fsyncs.load(Ordering::Relaxed)
    }

    pub fn pause_flushing(&self, paused: bool) {
        let start = {
            let mut inner = self.locked();
            inner.paused = paused;
            !paused && !inner.pending.is_empty() && !std::mem::replace(&mut inner.flushing, true)
        };
        if start {
            self.spawn_flusher();
        }
    }

    pub fn close(&self) -> Option<D> {
        self.locked().wal.take().map(Wal::into_disk)
    }

    fn spawn_flusher(&self) {
        let inner = self.inner.clone();
        drop(RT::spawn(async move {
            loop {
                let (batch, delay) = {
                    let mut g = inner.lock().await;
                    if g.paused || g.pending.is_empty() {
                        g.flushing = false;
                        return;
                    }
                    let delay = Duration::from_micros(g.knobs.flush_delay_us.load(Ordering::Relaxed));
                    (std::mem::take(&mut g.pending), delay)
                };
                if !delay.is_zero() {
                    RT::sleep(delay).await;
                }
                let syncer = inner.clone();
                let result = RT::spawn_blocking(move || sync_unlocked(&syncer))
                    .await
                    .unwrap_or_else(|e| Err(io::Error::other(e.to_string())));
                for cb in batch {
                    cb.io_completed(result.as_ref().map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string())));
                }
            }
        }));
    }
}

impl<RT: AsyncRuntime> SharedWal<RT, MemDisk> {
    pub fn crash(&self) -> MemDisk {
        self.close().unwrap_or_default().crash()
    }
}

pub struct WalLog<RT: AsyncRuntime, D: Disk> {
    shard: u64,
    shared: SharedWal<RT, D>,
    _rt: PhantomData<fn() -> RT>,
}

impl<RT: AsyncRuntime, D: Disk> Clone for WalLog<RT, D> {
    fn clone(&self) -> Self {
        Self { shard: self.shard, shared: self.shared.clone(), _rt: PhantomData }
    }
}

impl<RT: AsyncRuntime, D: Disk> WalLog<RT, D> {
    async fn read<T>(&self, f: impl FnOnce(&ShardState) -> T) -> io::Result<T> {
        let mut inner = self.shared.inner.lock().await;
        inner.wal()?;
        let empty = ShardState::default();
        Ok(f(inner.shards.get(&self.shard).unwrap_or(&empty)))
    }

    async fn write(&self, record: Record) -> io::Result<()> {
        self.shared.inner.lock().await.write(record)
    }

    async fn write_durable(&self, record: Record) -> io::Result<()> {
        self.write(record).await?;
        let inner = self.shared.inner.clone();
        RT::spawn_blocking(move || sync_unlocked(&inner)).await.unwrap_or_else(|e| Err(io::Error::other(e.to_string())))
    }
}

impl<RT: AsyncRuntime, D: Disk> RaftLogReader<Types<RT>> for WalLog<RT, D> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<KvEntry>, io::Error> {
        self.read(|s| s.entries.range(range).map(|(_, e)| e.clone()).collect()).await
    }

    async fn read_vote(&mut self) -> Result<Option<KvVote>, io::Error> {
        self.read(|s| s.vote).await
    }
}

impl<RT: AsyncRuntime, D: Disk> RaftLogStorage<Types<RT>> for WalLog<RT, D> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<Types<RT>>, io::Error> {
        self.read(|s| {
            let last = s.entries.values().next_back().map(|e| e.log_id()).or(s.purged);
            LogState { last_purged_log_id: s.purged, last_log_id: last }
        })
        .await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &KvVote) -> Result<(), io::Error> {
        self.write_durable(Record::Vote { shard: self.shard, vote: *vote }).await
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        self.write(Record::Committed { shard: self.shard, log_id: committed }).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        self.read(|s| s.committed).await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<Types<RT>>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = KvEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<KvEntry> = entries.into_iter().collect();
        let start = {
            let mut inner = self.shared.inner.lock().await;
            inner.write(Record::Entries { shard: self.shard, entries })?;
            inner.pending.push(callback);
            !inner.paused && !std::mem::replace(&mut inner.flushing, true)
        };
        if start {
            self.shared.spawn_flusher();
        }
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        self.write_durable(Record::Truncate { shard: self.shard, after: last_log_id }).await
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let oversized = {
            let mut inner = self.shared.inner.lock().await;
            inner.write(Record::Purge { shard: self.shard, upto: log_id })?;
            inner.wal()?.disk_len() > self.shared.knobs.compaction_threshold.load(Ordering::Relaxed)
        };
        if oversized {
            let inner = self.shared.inner.clone();
            RT::spawn_blocking(move || inner.blocking_lock().compact())
                .await
                .unwrap_or_else(|e| Err(io::Error::other(e.to_string())))?;
        }
        Ok(())
    }
}
