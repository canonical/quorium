use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use openraft::async_runtime::Oneshot;
use openraft::entry::RaftEntry;
use openraft::impls::{Entry, Vote};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::{AsyncRuntime, LogId, RaftTypeConfig};
use openraft_rt_tokio::TokioRuntime;
use quorium_core::types::{CommittedLeaderId, Types};
use quorium_store::disk::{Disk, MemDisk, Syncer};
use quorium_store::wal_log::SharedWal;

type C = Types<TokioRuntime>;

#[derive(Clone)]
struct SlowDisk {
    inner: Arc<Mutex<MemDisk>>,
    delay: Duration,
}

impl Disk for SlowDisk {
    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        self.inner.lock().unwrap().read_all()
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.lock().unwrap().append(bytes)
    }

    fn sync(&mut self) -> io::Result<()> {
        std::thread::sleep(self.delay);
        self.inner.lock().unwrap().sync()
    }

    fn syncer(&self) -> io::Result<Syncer> {
        let delay = self.delay;
        Ok(Box::new(move || {
            std::thread::sleep(delay);
            Ok(())
        }))
    }

    fn mark_synced(&mut self, upto: u64) {
        self.inner.lock().unwrap().mark_synced(upto);
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.inner.lock().unwrap().truncate(len)
    }

    fn replace(&mut self, bytes: &[u8]) -> io::Result<()> {
        std::thread::sleep(self.delay);
        self.inner.lock().unwrap().replace(bytes)
    }

    fn len(&self) -> u64 {
        self.inner.lock().unwrap().len()
    }
}

fn blank(index: u64) -> <C as RaftTypeConfig>::Entry {
    Entry::new_blank(LogId::new(CommittedLeaderId::new(1), index))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap()
}

#[test]
fn log_work_proceeds_while_an_fsync_is_in_flight() {
    runtime().block_on(async {
        let disk = SlowDisk { inner: Arc::default(), delay: Duration::from_millis(400) };
        let wal = SharedWal::<TokioRuntime, SlowDisk>::open(disk.clone()).unwrap();
        let mut log = wal.shard(0);
        let (tx, rx) = <TokioRuntime as AsyncRuntime>::Oneshot::channel();
        log.append(vec![blank(1)], IOFlushed::<C>::signal(tx)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = Instant::now();
        log.save_committed(Some(LogId::new(CommittedLeaderId::new(1), 1))).await.unwrap();
        let mut reader = log.get_log_reader().await;
        assert_eq!(reader.try_get_log_entries(1..2).await.unwrap().len(), 1);
        let (tx2, rx2) = <TokioRuntime as AsyncRuntime>::Oneshot::channel();
        log.append(vec![blank(2)], IOFlushed::<C>::signal(tx2)).await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(150), "blocked for {:?}", started.elapsed());
        rx.await.unwrap().unwrap();
        rx2.await.unwrap().unwrap();
        assert!(wal.sync_latency() >= Duration::from_millis(300), "{:?}", wal.sync_latency());
        let state = disk.inner.lock().unwrap();
        assert_eq!(state.durable_len(), state.len());
    });
}

#[test]
fn vote_saves_do_not_block_other_shards() {
    runtime().block_on(async {
        let disk = SlowDisk { inner: Arc::default(), delay: Duration::from_millis(400) };
        let wal = SharedWal::<TokioRuntime, SlowDisk>::open(disk).unwrap();
        let mut voter = wal.shard(0);
        let reader_log = wal.shard(1);
        let vote = tokio::spawn(async move { voter.save_vote(&Vote::new(3, 1)).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = Instant::now();
        let mut reader = reader_log.clone();
        reader.get_log_state().await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(150), "blocked for {:?}", started.elapsed());
        vote.await.unwrap().unwrap();
    });
}

#[test]
fn a_sync_covers_only_bytes_written_before_it_began() {
    let mut disk = MemDisk::default();
    disk.append(b"abc").unwrap();
    let sync = disk.syncer().unwrap();
    let upto = disk.len();
    disk.append(b"def").unwrap();
    sync().unwrap();
    disk.mark_synced(upto);
    assert_eq!(disk.durable_len(), 3);
    assert_eq!(disk.clone().crash().len(), 3);
    disk.mark_synced(1);
    assert_eq!(disk.durable_len(), 3);
}

struct SlowStorage {
    delay: Duration,
    saves: Arc<Mutex<u32>>,
}

impl quorium_store::mem::Storage for SlowStorage {
    fn save(&self, _checkpoint: quorium_store::mem::Checkpoint<'_>) -> io::Result<()> {
        std::thread::sleep(self.delay);
        *self.saves.lock().unwrap() += 1;
        Ok(())
    }

    fn load(&self) -> io::Result<Option<(quorium_store::mem::KvSnapshotMeta, quorium_core::kv::KvState)>> {
        Ok(None)
    }
}

#[test]
fn applies_and_reads_proceed_during_a_slow_checkpoint() {
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use quorium_core::kv::{Command, KvOp};
    runtime().block_on(async {
        let saves = Arc::new(Mutex::new(0));
        let storage = SlowStorage { delay: Duration::from_millis(400), saves: saves.clone() };
        let mut sm = quorium_store::mem::KvStateMachine::with_store(Arc::new(storage)).unwrap();
        let put = |i: u64| {
            let cmd = Command::new(1, i, KvOp::Put { key: format!("k{i}"), value: vec![1] });
            Ok((Entry::new_normal(LogId::new(CommittedLeaderId::new(1), i), cmd), None))
        };
        RaftStateMachine::<C>::apply(&mut sm, futures::stream::iter(vec![put(1)])).await.unwrap();
        let mut builder = sm.clone();
        let build =
            tokio::spawn(async move { RaftSnapshotBuilder::<C>::build_snapshot(&mut builder).await.map(|_| ()) });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = Instant::now();
        RaftStateMachine::<C>::apply(&mut sm, futures::stream::iter(vec![put(2)])).await.unwrap();
        assert_eq!(sm.read(|s| s.get("k2").cloned()), Some(vec![1]));
        assert!(started.elapsed() < Duration::from_millis(150), "blocked for {:?}", started.elapsed());
        build.await.unwrap().unwrap();
        assert_eq!(*saves.lock().unwrap(), 1);
    });
}

#[test]
fn compaction_does_not_stall_the_runtime() {
    tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap().block_on(async {
        let disk = SlowDisk { inner: Arc::default(), delay: Duration::from_millis(300) };
        let wal = SharedWal::<TokioRuntime, SlowDisk>::open(disk).unwrap();
        wal.set_compaction_threshold(1);
        let mut log = wal.shard(0);
        let (tx, rx) = <TokioRuntime as AsyncRuntime>::Oneshot::channel();
        log.append(vec![blank(1), blank(2)], IOFlushed::<C>::signal(tx)).await.unwrap();
        rx.await.unwrap().unwrap();
        let ticker = tokio::spawn(async {
            let mut worst = Duration::ZERO;
            for _ in 0..30 {
                let before = Instant::now();
                tokio::time::sleep(Duration::from_millis(10)).await;
                worst = worst.max(before.elapsed());
            }
            worst
        });
        log.purge(LogId::new(CommittedLeaderId::new(1), 1)).await.unwrap();
        let worst = ticker.await.unwrap();
        assert!(worst < Duration::from_millis(100), "the runtime stalled for {worst:?}");
        assert!(wal.disk_len() > 0);
    });
}
