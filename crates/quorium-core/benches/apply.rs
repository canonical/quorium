use criterion::{Criterion, criterion_group, criterion_main};
use quorium_core::kv::{Command, KvOp, KvState};
use quorium_core::shard::entity_shard;

fn apply(c: &mut Criterion) {
    c.bench_function("kv_apply_put", |b| {
        let mut state = KvState::default();
        let mut seq = 0u64;
        b.iter(|| {
            seq += 1;
            let cmd = Command::new(1, seq, KvOp::Put { key: format!("k{}", seq % 1024), value: vec![1; 64] })
                .acknowledging_below(seq);
            state.apply_at(&cmd, seq).unwrap()
        });
    });
    c.bench_function("entity_shard_routing", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k += 1;
            entity_shard("shop.Cart", &k.to_le_bytes(), 256)
        });
    });
}

criterion_group!(benches, apply);
criterion_main!(benches);
