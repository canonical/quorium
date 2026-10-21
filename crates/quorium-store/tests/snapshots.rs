use openraft::entry::RaftEntry;
use openraft::impls::Entry;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{AsyncRuntime, LogId, RaftTypeConfig};
use openraft_rt_tokio::TokioRuntime;
use quorium_core::kv::{Command, KvOp};
use quorium_core::types::{CommittedLeaderId, Types};
use quorium_store::mem::KvStateMachine;

type C = Types<TokioRuntime>;

fn put(index: u64, key: &str) -> <C as RaftTypeConfig>::Entry {
    let cmd = Command::new(1, index, KvOp::Put { key: key.into(), value: vec![index as u8] });
    Entry::new_normal(LogId::new(CommittedLeaderId::new(1), index), cmd)
}

async fn apply(sm: &mut KvStateMachine, entries: Vec<<C as RaftTypeConfig>::Entry>) {
    let stream = futures::stream::iter(entries.into_iter().map(|e| Ok((e, None))));
    RaftStateMachine::<C>::apply(sm, stream).await.unwrap();
}

#[test]
fn built_snapshot_is_restored_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::persistent(dir.path()).unwrap();
        apply(&mut sm, vec![put(1, "a"), put(2, "b")]).await;
        RaftSnapshotBuilder::<C>::build_snapshot(&mut sm).await.unwrap();
        apply(&mut sm, vec![put(3, "c")]).await;
    });
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::persistent(dir.path()).unwrap();
        assert_eq!(sm.last_applied_index(), Some(2));
        assert_eq!(sm.read(|s| s.get("b").cloned()), Some(vec![2]));
        assert_eq!(sm.read(|s| s.get("c").cloned()), None);
        let current = RaftStateMachine::<C>::get_current_snapshot(&mut sm).await.unwrap().unwrap();
        assert_eq!(current.meta.last_log_id.map(|l| l.index), Some(2));
    });
}

#[test]
fn installed_snapshot_is_durable() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    TokioRuntime::run(async {
        let mut leader = KvStateMachine::persistent(leader_dir.path()).unwrap();
        apply(&mut leader, vec![put(1, "x")]).await;
        let snap = RaftSnapshotBuilder::<C>::build_snapshot(&mut leader).await.unwrap();
        let mut follower = KvStateMachine::persistent(follower_dir.path()).unwrap();
        RaftStateMachine::<C>::install_snapshot(&mut follower, &snap.meta, snap.snapshot).await.unwrap();
    });
    let sm = KvStateMachine::persistent(follower_dir.path()).unwrap();
    assert_eq!(sm.read(|s| s.get("x").cloned()), Some(vec![1]));
}

#[test]
fn leftover_temporary_file_never_replaces_a_good_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::persistent(dir.path()).unwrap();
        apply(&mut sm, vec![put(1, "a")]).await;
        RaftSnapshotBuilder::<C>::build_snapshot(&mut sm).await.unwrap();
    });
    std::fs::write(dir.path().join("snapshot.tmp"), b"half-written").unwrap();
    let sm = KvStateMachine::persistent(dir.path()).unwrap();
    assert_eq!(sm.last_applied_index(), Some(1));
}

#[test]
fn corrupt_snapshot_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::persistent(dir.path()).unwrap();
        apply(&mut sm, vec![put(1, "a")]).await;
        RaftSnapshotBuilder::<C>::build_snapshot(&mut sm).await.unwrap();
    });
    let path = dir.path().join("snapshot");
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
    assert!(KvStateMachine::persistent(dir.path()).is_err());
}

#[test]
fn shared_memory_snapshot_store_survives_state_machine_rebuild() {
    use quorium_store::mem::MemSnapshotStore;
    let store = MemSnapshotStore::default();
    TokioRuntime::run(async {
        let mut sm = KvStateMachine::with_store(std::sync::Arc::new(store.clone())).unwrap();
        apply(&mut sm, vec![put(1, "a"), put(2, "b")]).await;
        RaftSnapshotBuilder::<C>::build_snapshot(&mut sm).await.unwrap();
    });
    let sm = KvStateMachine::with_store(std::sync::Arc::new(store)).unwrap();
    assert_eq!(sm.last_applied_index(), Some(2));
    assert_eq!(sm.read(|s| s.get("a").cloned()), Some(vec![1]));
}
