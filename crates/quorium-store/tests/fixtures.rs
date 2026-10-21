use std::path::PathBuf;

use openraft::async_runtime::Oneshot;
use openraft::entry::RaftEntry;
use openraft::impls::{Entry, Vote};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::{AsyncRuntime, LogId, RaftTypeConfig};
use openraft_rt_tokio::TokioRuntime;
use quorium_core::entity::EntityCall;
use quorium_core::kv::{Command, EscrowAction, EscrowOp, KvOp, KvState, LeaseAction, LeaseOp};
use quorium_core::types::{CommittedLeaderId, Types};
use quorium_store::disk::{Disk, MemDisk};
use quorium_store::wal_log::SharedWal;

type C = Types<TokioRuntime>;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn writing() -> bool {
    std::env::var("QUORIUM_WRITE_FIXTURES").is_ok()
}

fn commands() -> Vec<Command> {
    vec![
        Command::new(1, 1, KvOp::Put { key: "k".into(), value: b"v".to_vec() }).acknowledging_below(1),
        Command::new(
            2,
            1,
            KvOp::Lease(LeaseOp {
                name: "job".into(),
                holder: 2,
                ttl_ms: 500,
                now_ms: 1_000,
                action: LeaseAction::Acquire,
            }),
        ),
        Command::new(
            3,
            1,
            KvOp::Escrow(EscrowOp { name: "sku".into(), node: 3, action: EscrowAction::Refill { units: 9 } }),
        ),
        Command::new(
            4,
            1,
            KvOp::DeleteEntity { type_name: "fixture.Gone".into(), key: vec![1, 2], locality: Some("eu".into()) },
        ),
    ]
}

fn entry(index: u64, cmd: Command) -> <C as RaftTypeConfig>::Entry {
    Entry::new_normal(LogId::new(CommittedLeaderId::new(2), index), cmd)
}

#[test]
fn wal_format_is_stable() {
    let path = fixture("wal-v1.bin");
    if writing() {
        TokioRuntime::run(async {
            let wal = SharedWal::<TokioRuntime, MemDisk>::open(MemDisk::default()).unwrap();
            let mut log = wal.shard(3);
            log.save_vote(&Vote::new_committed(2, 7)).await.unwrap();
            let entries: Vec<_> = commands().into_iter().enumerate().map(|(i, c)| entry(i as u64 + 1, c)).collect();
            let (tx, rx) = <TokioRuntime as AsyncRuntime>::Oneshot::channel();
            log.append(entries, IOFlushed::<C>::signal(tx)).await.unwrap();
            rx.await.unwrap().unwrap();
            log.save_committed(Some(LogId::new(CommittedLeaderId::new(2), 3))).await.unwrap();
            log.save_vote(&Vote::new_committed(2, 7)).await.unwrap();
            let mut disk = wal.crash();
            std::fs::write(&path, disk.read_all().unwrap()).unwrap();
        });
    }
    let bytes = std::fs::read(&path).expect("fixture exists; regenerate with QUORIUM_WRITE_FIXTURES=1");
    let mut disk = MemDisk::default();
    disk.append(&bytes).unwrap();
    disk.sync().unwrap();
    TokioRuntime::run(async {
        let wal = SharedWal::<TokioRuntime, MemDisk>::open(disk).expect("today's code reads the stored WAL");
        let mut log = wal.shard(3);
        assert_eq!(log.get_log_reader().await.read_vote().await.unwrap(), Some(Vote::new_committed(2, 7)));
        assert_eq!(log.read_committed().await.unwrap().map(|l| l.index), Some(3));
        let stored = log.try_get_log_entries(1..5).await.unwrap();
        let expected: Vec<Command> = commands();
        assert_eq!(stored.len(), expected.len());
        for (e, c) in stored.iter().zip(expected) {
            match &e.payload {
                openraft::impls::EntryPayload::Normal(cmd) => assert_eq!(*cmd, c),
                other => panic!("{other:?}"),
            }
        }
    });
}

fn state() -> KvState {
    let mut s = KvState::default();
    for (i, c) in commands().into_iter().enumerate() {
        s.apply_at(&c, i as u64 + 1).unwrap();
    }
    let call = EntityCall {
        type_name: "fixture.Unknown".into(),
        version: 1,
        key: vec![9],
        method: 1,
        args: vec![],
        now_ms: 0,
        locality: None,
    };
    let _ = s.apply_at(&Command::new(5, 1, KvOp::Apply(call)), 9);
    s
}

#[test]
fn version_two_snapshots_still_decode() {
    let bytes = std::fs::read(fixture("state-v2.bin")).expect("the v2 fixture is kept forever");
    let decoded = KvState::from_bytes(&bytes).expect("today's code reads v2 snapshots");
    assert_eq!(decoded, state());
    assert_eq!(decoded.escrow("sku").map(|e| e.pool), Some(9));
    assert_eq!(decoded.get("k"), Some(&b"v".to_vec()));
}

#[test]
fn version_three_snapshots_still_decode() {
    let bytes = std::fs::read(fixture("state-v3.bin")).expect("the v3 fixture is kept forever");
    let decoded = KvState::from_bytes(&bytes).expect("today's code reads v3 snapshots");
    assert_eq!(decoded, state());
}

fn with_holders(mut s: KvState) -> KvState {
    let escrow = |node: u64, action: EscrowAction| KvOp::Escrow(EscrowOp { name: "sku".into(), node, action });
    let ops = vec![
        (3, escrow(3, EscrowAction::GrantVia { want: 2, peer: 5 })),
        (3, escrow(3, EscrowAction::SettleThrough { through: 1, release: 0 })),
        (8, escrow(8, EscrowAction::Grant { want: 2 })),
        (9, escrow(8, EscrowAction::Reclaim { through: 1 })),
    ];
    for (i, (client, op)) in ops.into_iter().enumerate() {
        s.apply_at(&Command::new(client, 100 + i as u64, op), 40 + i as u64).unwrap();
    }
    s
}

#[test]
fn snapshot_format_is_stable() {
    let path = fixture("state-v4.bin");
    let expected = with_holders(state());
    assert_eq!(expected.escrow_holders().count(), 2);
    if writing() {
        std::fs::write(&path, expected.to_bytes()).unwrap();
    }
    let bytes = std::fs::read(&path).expect("fixture exists; regenerate with QUORIUM_WRITE_FIXTURES=1");
    let decoded = KvState::from_bytes(&bytes).expect("today's code reads the stored snapshot");
    assert_eq!(decoded, expected);
}

fn rich_state() -> KvState {
    let mut s = state();
    let ops = vec![
        KvOp::Route(quorium_core::kv::RouteOp::Init { table: quorium_core::kv::RoutingTable::uniform(2) }),
        KvOp::Own { ranges: vec![(0, u64::MAX / 2)] },
        KvOp::Extract { lo: 10, hi: 20 },
    ];
    for (i, op) in ops.into_iter().enumerate() {
        s.apply_at(&Command::new(6, i as u64 + 1, op), 20 + i as u64).unwrap();
    }
    let mut rows = s.rows();
    let slot = postcard::to_allocvec(&(2u32, b"entity".to_vec(), 77u64)).unwrap();
    rows.push((quorium_core::kv::RowKey::Entity("fixture.Kept".into(), vec![4]), slot));
    KvState::from_rows(rows).unwrap()
}

fn stored_rows(name: &str) -> KvState {
    use quorium_core::kv::RowKey;
    let bytes = std::fs::read(fixture(name)).expect("fixture exists; regenerate with QUORIUM_WRITE_FIXTURES=1");
    let stored: Vec<(Vec<u8>, Vec<u8>)> = postcard::from_bytes(&bytes).unwrap();
    let rows = stored.into_iter().map(|(k, v)| (RowKey::from_bytes(&k).expect("row keys decode"), v));
    KvState::from_rows(rows).expect("today's code reads stored rows")
}

#[test]
fn version_one_rows_still_decode() {
    let decoded = stored_rows("rows-v1.bin");
    assert_eq!(decoded, rich_state());
    assert_eq!(decoded.entity("fixture.Kept", &[4]), Some((2, b"entity".as_slice())));
    assert!(decoded.routing().is_some());
}

#[test]
fn row_format_is_stable() {
    let expected = with_holders(rich_state());
    let encode =
        |s: &KvState| -> Vec<(Vec<u8>, Vec<u8>)> { s.rows().into_iter().map(|(k, v)| (k.to_bytes(), v)).collect() };
    if writing() {
        std::fs::write(fixture("rows-v2.bin"), postcard::to_allocvec(&encode(&expected)).unwrap()).unwrap();
    }
    let decoded = stored_rows("rows-v2.bin");
    assert_eq!(decoded, expected);
    assert_eq!(decoded.escrow_holders().count(), 2);
}

#[test]
fn redb_files_written_today_stay_readable() {
    use quorium_store::mem::{Checkpoint, KvSnapshotMeta, Rows, Storage};
    use quorium_store::redb_state::StateDb;
    let path = fixture("state-v1.redb");
    if writing() {
        let _ = std::fs::remove_file(&path);
        let db = StateDb::open(&path).unwrap();
        let state = rich_state();
        let rows = Rows::All(state.rows());
        db.shard(1).save(Checkpoint { meta: &KvSnapshotMeta::default(), rows: &rows, bytes: &[] }).unwrap();
        db.close();
        let mut raw = redb::Database::create(&path).unwrap();
        while raw.compact().unwrap() {}
    }
    let dir = tempfile::tempdir().unwrap();
    let copy = dir.path().join("state.redb");
    std::fs::copy(&path, &copy).expect("fixture exists; regenerate with QUORIUM_WRITE_FIXTURES=1");
    let db = StateDb::open(&copy).expect("today's redb opens the stored file");
    let (_, state) = db.shard(1).load().unwrap().expect("the stored shard is present");
    assert_eq!(state, rich_state());
    assert!(db.shard(0).load().unwrap().is_none());
}
