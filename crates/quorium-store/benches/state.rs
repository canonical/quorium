use criterion::{Criterion, criterion_group, criterion_main};
use quorium_core::kv::{Command, KvOp, KvState};
use quorium_store::mem::{Changes, Checkpoint, FileSnapshotStore, KvSnapshotMeta, Rows, Storage};
use quorium_store::redb_state::StateDb;

const ROWS: u64 = 100_000;

fn populated() -> KvState {
    let mut state = KvState::default();
    for i in 0..ROWS {
        state.apply(&Command::new(1, i + 1, KvOp::Put { key: format!("key-{i}"), value: vec![7; 100] }));
    }
    state
}

fn checkpoint(c: &mut Criterion) {
    let mut state = populated();
    let meta = KvSnapshotMeta::default();
    let dir = tempfile::tempdir().unwrap();
    let db = StateDb::open(&dir.path().join("state.redb")).unwrap();
    let redb = db.shard(0);
    let file = FileSnapshotStore::new(&dir.path().join("file")).unwrap();
    let all = Changes::everything();
    let bytes = state.to_bytes();
    redb.save(Checkpoint { meta: &meta, rows: &Rows::capture(&state, &all), bytes: &bytes }).unwrap();
    let mut group = c.benchmark_group("checkpoint_100k_rows");
    group.sample_size(10);
    let mut seq = ROWS + 1;
    group.bench_function("redb_10_changed_rows", |b| {
        b.iter(|| {
            let mut changes = Changes::default();
            for i in 0..10 {
                let key = format!("key-{}", (seq * 7919 + i) % ROWS);
                let op = KvOp::Put { key, value: vec![9; 100] };
                changes.record(op.touches());
                state.apply(&Command::new(1, seq, op));
                seq += 1;
            }
            let bytes = Vec::new();
            redb.save(Checkpoint { meta: &meta, rows: &Rows::capture(&state, &changes), bytes: &bytes }).unwrap();
        });
    });
    #[cfg(feature = "fjall")]
    {
        let fjall = quorium_store::fjall_state::FjallDb::open(&dir.path().join("state.fjall")).unwrap().shard(0);
        let bytes = state.to_bytes();
        fjall.save(Checkpoint { meta: &meta, rows: &Rows::capture(&state, &all), bytes: &bytes }).unwrap();
        group.bench_function("fjall_10_changed_rows", |b| {
            b.iter(|| {
                let mut changes = Changes::default();
                for i in 0..10 {
                    let key = format!("key-{}", (seq * 7919 + i) % ROWS);
                    let op = KvOp::Put { key, value: vec![9; 100] };
                    changes.record(op.touches());
                    state.apply(&Command::new(1, seq, op));
                    seq += 1;
                }
                let bytes = Vec::new();
                fjall.save(Checkpoint { meta: &meta, rows: &Rows::capture(&state, &changes), bytes: &bytes }).unwrap();
            });
        });
        let bytes = state.to_bytes();
        group.bench_function("fjall_full_rewrite", |b| {
            b.iter(|| {
                fjall.save(Checkpoint { meta: &meta, rows: &Rows::capture(&state, &all), bytes: &bytes }).unwrap();
            });
        });
    }
    group.bench_function("file_full_snapshot", |b| {
        b.iter(|| {
            let bytes = state.to_bytes();
            file.save(Checkpoint { meta: &meta, rows: &Rows::capture(&state, &all), bytes: &bytes }).unwrap();
        });
    });
    group.bench_function("redb_full_rewrite", |b| {
        b.iter(|| {
            redb.save(Checkpoint { meta: &meta, rows: &Rows::capture(&state, &all), bytes: &bytes }).unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, checkpoint);
criterion_main!(benches);
