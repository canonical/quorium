use openraft::async_runtime::Oneshot;
use openraft::entry::RaftEntry;
use openraft::impls::{Entry, Vote};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::testing::log::{StoreBuilder, Suite};
use openraft::{AsyncRuntime, LogId, RaftTypeConfig, StorageError};
use openraft_rt_tokio::TokioRuntime;
use quorium_core::types::{CommittedLeaderId, Types};
use quorium_store::disk::MemDisk;
use quorium_store::mem::KvStateMachine;
use quorium_store::wal_log::SharedWal;

type C = Types<TokioRuntime>;

fn blank(term: u64, index: u64) -> <C as RaftTypeConfig>::Entry {
    Entry::new_blank(LogId::new(CommittedLeaderId::new(term), index))
}

struct Builder;

impl StoreBuilder<C, quorium_store::wal_log::WalLog<TokioRuntime, MemDisk>, KvStateMachine, ()> for Builder {
    async fn build(
        &self,
    ) -> Result<((), quorium_store::wal_log::WalLog<TokioRuntime, MemDisk>, KvStateMachine), StorageError<C>> {
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
        Ok(((), wal.shard(0), KvStateMachine::default()))
    }
}

#[test]
fn passes_openraft_storage_suite() {
    TokioRuntime::run(async {
        Suite::test_all(Builder).await.unwrap();
    });
}

async fn flushed(
    log: &mut quorium_store::wal_log::WalLog<TokioRuntime, MemDisk>,
    entries: Vec<<C as RaftTypeConfig>::Entry>,
) {
    let (tx, rx) = <TokioRuntime as AsyncRuntime>::Oneshot::channel();
    let cb = IOFlushed::<C>::signal(tx);
    log.append(entries, cb).await.unwrap();
    TokioRuntime::timeout(std::time::Duration::from_secs(2), rx).await.expect("flushed").unwrap().unwrap();
}

#[test]
fn shards_recover_independently_from_one_wal() {
    TokioRuntime::run(async {
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
        let mut a = wal.shard(1);
        let mut b = wal.shard(2);
        a.save_vote(&Vote::new(4, 1)).await.unwrap();
        flushed(&mut a, vec![blank(1, 1), blank(1, 2)]).await;
        flushed(&mut b, vec![blank(3, 1)]).await;
        a.save_committed(Some(LogId::new(CommittedLeaderId::new(1), 2))).await.unwrap();
        a.truncate_after(Some(LogId::new(CommittedLeaderId::new(1), 1))).await.unwrap();
        let disk = wal.crash();
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(disk).unwrap();
        let mut a = wal.shard(1);
        let mut b = wal.shard(2);
        assert_eq!(a.get_log_reader().await.read_vote().await.unwrap(), Some(Vote::new(4, 1)));
        assert_eq!(a.get_log_state().await.unwrap().last_log_id.map(|l| l.index), Some(1));
        assert_eq!(b.get_log_state().await.unwrap().last_log_id.map(|l| l.index), Some(1));
        assert_eq!(b.get_log_reader().await.read_vote().await.unwrap(), None);
        assert_eq!(wal.shard_ids(), vec![1, 2]);
    });
}

#[test]
fn group_commit_shares_one_fsync_across_shards() {
    TokioRuntime::run(async {
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
        let mut logs: Vec<_> = (0..8).map(|s| wal.shard(s)).collect();
        let before = wal.fsyncs();
        let mut receivers = Vec::new();
        wal.pause_flushing(true);
        for (i, log) in logs.iter_mut().enumerate() {
            let (tx, rx) = <TokioRuntime as AsyncRuntime>::Oneshot::channel();
            log.append(vec![blank(1, i as u64 + 1)], IOFlushed::<C>::signal(tx)).await.unwrap();
            receivers.push(rx);
        }
        wal.pause_flushing(false);
        for rx in receivers {
            TokioRuntime::timeout(std::time::Duration::from_secs(2), rx).await.expect("flushed").unwrap().unwrap();
        }
        assert!(wal.fsyncs() - before <= 2, "fsyncs {}", wal.fsyncs() - before);
    });
}

#[test]
fn unflushed_appends_do_not_survive_a_crash() {
    TokioRuntime::run(async {
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
        wal.pause_flushing(true);
        let mut a = wal.shard(1);
        a.append(vec![blank(1, 1)], IOFlushed::noop()).await.unwrap();
        a.save_committed(Some(LogId::new(CommittedLeaderId::new(1), 1))).await.unwrap();
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(wal.crash()).unwrap();
        let mut a = wal.shard(1);
        assert!(a.get_log_state().await.unwrap().last_log_id.is_none());
        assert_eq!(a.read_committed().await.unwrap(), None);
    });
}

#[test]
fn corrupt_wal_refuses_to_open() {
    let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
    TokioRuntime::run(async {
        let mut a = wal.shard(1);
        a.save_vote(&Vote::new(1, 1)).await.unwrap();
        a.save_vote(&Vote::new(2, 1)).await.unwrap();
    });
    let mut disk = wal.crash();
    disk.flip_bit(9 * 8);
    assert!(SharedWal::<TokioRuntime, MemDisk>::open(disk).is_err());
}

#[test]
fn flush_delay_postpones_acknowledgement() {
    TokioRuntime::run(async {
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
        wal.set_flush_delay(std::time::Duration::from_millis(50));
        let mut a = wal.shard(1);
        let (tx, rx) = <TokioRuntime as AsyncRuntime>::Oneshot::channel();
        let start = std::time::Instant::now();
        a.append(vec![blank(1, 1)], IOFlushed::<C>::signal(tx)).await.unwrap();
        rx.await.unwrap().unwrap();
        assert!(start.elapsed() >= std::time::Duration::from_millis(50));
    });
}

#[test]
fn compaction_preserves_state_and_shrinks_the_wal() {
    TokioRuntime::run(async {
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
        let mut a = wal.shard(1);
        let mut b = wal.shard(2);
        a.save_vote(&Vote::new(5, 1)).await.unwrap();
        for i in 1..=200 {
            flushed(&mut a, vec![blank(1, i)]).await;
        }
        flushed(&mut b, vec![blank(2, 1)]).await;
        a.save_committed(Some(LogId::new(CommittedLeaderId::new(1), 190))).await.unwrap();
        a.purge(LogId::new(CommittedLeaderId::new(1), 180)).await.unwrap();
        let before = wal.disk_len();
        wal.compact().unwrap();
        assert!(wal.disk_len() < before / 4, "{} -> {}", before, wal.disk_len());
        flushed(&mut a, vec![blank(1, 201)]).await;
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(wal.crash()).unwrap();
        let mut a = wal.shard(1);
        let mut b = wal.shard(2);
        let state = a.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(180));
        assert_eq!(state.last_log_id.map(|l| l.index), Some(201));
        assert_eq!(a.try_get_log_entries(181..202).await.unwrap().len(), 21);
        assert_eq!(a.read_committed().await.unwrap().map(|l| l.index), Some(190));
        assert_eq!(a.get_log_reader().await.read_vote().await.unwrap(), Some(Vote::new(5, 1)));
        assert_eq!(b.get_log_state().await.unwrap().last_log_id.map(|l| l.index), Some(1));
    });
}

#[test]
fn purge_compacts_automatically_past_the_threshold() {
    TokioRuntime::run(async {
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
        wal.set_compaction_threshold(4 * 1024);
        let mut a = wal.shard(1);
        for i in 1..=300 {
            flushed(&mut a, vec![blank(1, i)]).await;
        }
        let before = wal.disk_len();
        a.purge(LogId::new(CommittedLeaderId::new(1), 290)).await.unwrap();
        assert!(wal.disk_len() < before / 4);
    });
}

#[test]
fn file_disk_replace_is_atomic_and_durable() {
    use quorium_store::disk::{Disk, FileDisk};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let mut d = FileDisk::open(&path).unwrap();
    d.append(b"old-contents").unwrap();
    d.sync().unwrap();
    d.replace(b"new").unwrap();
    d.append(b"+more").unwrap();
    d.sync().unwrap();
    drop(d);
    assert_eq!(std::fs::read(&path).unwrap(), b"new+more");
    assert!(!dir.path().join("wal.compact").exists());
}
