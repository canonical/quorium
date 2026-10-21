#![cfg(feature = "fjall")]

use std::sync::Arc;

use openraft::entry::RaftEntry;
use openraft::impls::Entry;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{AsyncRuntime, LogId, RaftTypeConfig};
use openraft_rt_tokio::TokioRuntime;
use quorium_core::kv::{Command, KvOp};
use quorium_core::types::{CommittedLeaderId, Types};
use quorium_store::fjall_state::FjallDb as StateDb;
use quorium_store::mem::{FileSnapshotStore, KvStateMachine, Storage};

type C = Types<TokioRuntime>;

fn op(index: u64, op: KvOp) -> <C as RaftTypeConfig>::Entry {
    Entry::new_normal(LogId::new(CommittedLeaderId::new(1), index), Command::new(1, index, op))
}

fn put(index: u64, key: &str) -> <C as RaftTypeConfig>::Entry {
    op(index, KvOp::Put { key: key.into(), value: vec![index as u8] })
}

async fn apply(sm: &mut KvStateMachine, entries: Vec<<C as RaftTypeConfig>::Entry>) {
    let stream = futures::stream::iter(entries.into_iter().map(|e| Ok((e, None))));
    RaftStateMachine::<C>::apply(sm, stream).await.unwrap();
}

async fn checkpoint(sm: &mut KvStateMachine) {
    RaftSnapshotBuilder::<C>::build_snapshot(sm).await.unwrap();
}

#[test]
fn checkpointed_state_survives_reopen_and_later_writes_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.fjall");
    TokioRuntime::run(async {
        let db = StateDb::open(&path).unwrap();
        let mut sm = KvStateMachine::with_store(Arc::new(db.shard(0))).unwrap();
        apply(&mut sm, vec![put(1, "a"), put(2, "b")]).await;
        checkpoint(&mut sm).await;
        apply(&mut sm, vec![put(3, "c")]).await;
    });
    TokioRuntime::run(async {
        let db = StateDb::open(&path).unwrap();
        let mut sm = KvStateMachine::with_store(Arc::new(db.shard(0))).unwrap();
        assert_eq!(sm.last_applied_index(), Some(2));
        assert_eq!(sm.read(|s| s.get("b").cloned()), Some(vec![2]));
        assert_eq!(sm.read(|s| s.get("c").cloned()), None);
        let current = RaftStateMachine::<C>::get_current_snapshot(&mut sm).await.unwrap().unwrap();
        assert_eq!(current.meta.last_log_id.map(|l| l.index), Some(2));
    });
}

#[test]
fn deletes_reach_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = StateDb::open(&dir.path().join("state.fjall")).unwrap();
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::with_store(Arc::new(db.shard(0))).unwrap();
        apply(&mut sm, vec![put(1, "a"), put(2, "b")]).await;
        checkpoint(&mut sm).await;
        apply(&mut sm, vec![op(3, KvOp::Delete { key: "a".into() })]).await;
        checkpoint(&mut sm).await;
    });
    let (meta, state) = db.shard(0).load().unwrap().unwrap();
    assert_eq!(meta.last_log_id.map(|l| l.index), Some(3));
    assert_eq!(state.get("a"), None);
    assert_eq!(state.get("b"), Some(&vec![2]));
}

#[test]
fn checkpoints_write_only_changed_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = StateDb::open(&dir.path().join("state.fjall")).unwrap();
    let storage = Arc::new(db.shard(0));
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::with_store(storage.clone()).unwrap();
        apply(&mut sm, (1..=100).map(|i| put(i, &format!("k{i}"))).collect()).await;
        checkpoint(&mut sm).await;
        let before = storage.rows_written();
        apply(&mut sm, vec![put(101, "k5")]).await;
        checkpoint(&mut sm).await;
        assert_eq!(storage.rows_written() - before, 2);
    });
}

#[test]
fn shards_are_isolated_within_one_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = StateDb::open(&dir.path().join("state.fjall")).unwrap();
    TokioRuntime::run(async {
        let mut zero = KvStateMachine::with_store(Arc::new(db.shard(0))).unwrap();
        apply(&mut zero, vec![put(1, "only-zero")]).await;
        checkpoint(&mut zero).await;
    });
    assert!(db.shard(1).load().unwrap().is_none());
    assert!(db.shard(0).load().unwrap().is_some());
}

#[test]
fn installed_snapshot_replaces_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = StateDb::open(&dir.path().join("state.fjall")).unwrap();
    TokioRuntime::run(async {
        let mut leader = KvStateMachine::default();
        apply(&mut leader, vec![put(1, "x")]).await;
        let snap = RaftSnapshotBuilder::<C>::build_snapshot(&mut leader).await.unwrap();
        let mut follower = KvStateMachine::with_store(Arc::new(db.shard(0))).unwrap();
        apply(&mut follower, vec![put(1, "stale")]).await;
        checkpoint(&mut follower).await;
        RaftStateMachine::<C>::install_snapshot(&mut follower, &snap.meta, snap.snapshot).await.unwrap();
    });
    let (_, state) = db.shard(0).load().unwrap().unwrap();
    assert_eq!(state.get("x"), Some(&vec![1]));
    assert_eq!(state.get("stale"), None);
}

#[test]
fn legacy_snapshot_files_are_migrated() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("shard-0");
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::with_store(Arc::new(FileSnapshotStore::new(&legacy).unwrap())).unwrap();
        apply(&mut sm, vec![put(1, "old")]).await;
        checkpoint(&mut sm).await;
    });
    let db = StateDb::open(&dir.path().join("state.fjall")).unwrap();
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::with_store(Arc::new(db.shard(0).migrating_from(&legacy))).unwrap();
        assert_eq!(sm.read(|s| s.get("old").cloned()), Some(vec![1]));
        apply(&mut sm, vec![put(2, "new")]).await;
        checkpoint(&mut sm).await;
    });
    let (_, state) = db.shard(0).load().unwrap().unwrap();
    assert_eq!(state.get("old"), Some(&vec![1]));
    assert_eq!(state.get("new"), Some(&vec![2]));
}

#[test]
fn damaged_rows_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = StateDb::open(&dir.path().join("state.fjall")).unwrap();
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::with_store(Arc::new(db.shard(0))).unwrap();
        apply(&mut sm, vec![put(1, "a")]).await;
        checkpoint(&mut sm).await;
    });
    db.shard(0).__corrupt_meta_row().unwrap();
    assert!(KvStateMachine::with_store(Arc::new(db.shard(0))).is_err());
}
