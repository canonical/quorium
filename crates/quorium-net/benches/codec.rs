use criterion::{Criterion, criterion_group, criterion_main};
use openraft::entry::RaftEntry;
use openraft::impls::{Entry, Vote};
use openraft::raft::AppendEntriesRequest;
use openraft::{LogId, RaftTypeConfig};
use quorium_core::kv::{Command, KvOp};
use quorium_core::types::CommittedLeaderId;
use quorium_net::raft::{Message, ProdTypes, Request};

fn append_request() -> Message {
    let entries: Vec<<ProdTypes as RaftTypeConfig>::Entry> = (0..16u64)
        .map(|i| {
            let cmd = Command::new(1, i, KvOp::Put { key: format!("key-{i}"), value: vec![3; 128] });
            Entry::new_normal(LogId::new(CommittedLeaderId::new(4), i + 1), cmd)
        })
        .collect();
    Message {
        from: 1,
        shard: 3,
        request: Request::Append(AppendEntriesRequest {
            vote: Vote::new_committed(4, 1),
            prev_log_id: None,
            entries,
            leader_commit: None,
        }),
    }
}

fn codec(c: &mut Criterion) {
    let message = append_request();
    let bytes = message.encode();
    c.bench_function("append_16_encode", |b| b.iter(|| message.encode()));
    c.bench_function("append_16_decode", |b| b.iter(|| Message::decode(&bytes).unwrap()));
}

criterion_group!(benches, codec);
criterion_main!(benches);
