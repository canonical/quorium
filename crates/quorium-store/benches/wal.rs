use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use quorium_store::disk::{FileDisk, MemDisk};
use quorium_store::wal::Wal;

fn wal(c: &mut Criterion) {
    let record = vec![7u8; 1024];
    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Bytes(64 * 1024));
    group.bench_function("append_64x1k_mem", |b| {
        let mut wal = Wal::open(MemDisk::default()).unwrap();
        b.iter(|| {
            for _ in 0..64 {
                wal.append(&record).unwrap();
            }
            wal.sync().unwrap();
        });
    });
    let dir = tempfile::tempdir().unwrap();
    group.sample_size(20);
    group.bench_function("append_64x1k_fsync_file", |b| {
        let mut wal = Wal::open(FileDisk::open(&dir.path().join("wal")).unwrap()).unwrap();
        b.iter(|| {
            for _ in 0..64 {
                wal.append(&record).unwrap();
            }
            wal.sync().unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, wal);
criterion_main!(benches);
