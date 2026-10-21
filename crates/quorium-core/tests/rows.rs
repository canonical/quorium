use quorium_core::kv::{Command, KvOp, KvState, LeaseAction, LeaseOp, RowKey, Touch};

fn put(seq: u64, key: &str, value: &[u8]) -> Command {
    Command::new(1, seq, KvOp::Put { key: key.into(), value: value.to_vec() })
}

fn sample() -> KvState {
    let mut s = KvState::default();
    s.apply(&put(1, "a", b"1"));
    s.apply(&put(2, "b", b"2"));
    s.apply(&Command::new(
        2,
        1,
        KvOp::Lease(LeaseOp { name: "job".into(), holder: 3, action: LeaseAction::Acquire, now_ms: 5, ttl_ms: 10 }),
    ));
    s
}

#[test]
fn state_round_trips_through_rows() {
    let s = sample();
    let rows = s.rows();
    assert!(rows.iter().any(|(k, _)| *k == RowKey::Meta));
    assert!(rows.iter().any(|(k, v)| *k == RowKey::Data("a".into()) && v == b"1"));
    assert_eq!(KvState::from_rows(rows).unwrap(), s);
}

#[test]
fn row_reads_one_value_or_reports_absence() {
    let s = sample();
    assert_eq!(s.row(&RowKey::Data("b".into())), Some(b"2".to_vec()));
    assert_eq!(s.row(&RowKey::Data("zz".into())), None);
    assert_eq!(s.row(&RowKey::Entity("T".into(), vec![1])), None);
    assert!(s.row(&RowKey::Meta).is_some());
}

#[test]
fn row_keys_encode_reversibly_and_distinctly() {
    let keys = [
        RowKey::Meta,
        RowKey::Data("a".into()),
        RowKey::Data(String::new()),
        RowKey::Entity("a".into(), vec![]),
        RowKey::Entity("T".into(), vec![0, 1, 2]),
    ];
    let encoded: Vec<Vec<u8>> = keys.iter().map(RowKey::to_bytes).collect();
    for (k, e) in keys.iter().zip(&encoded) {
        assert_eq!(&RowKey::from_bytes(e).unwrap(), k);
    }
    let mut unique = encoded.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), keys.len());
    assert!(RowKey::from_bytes(&[]).is_err());
    assert!(RowKey::from_bytes(&[9]).is_err());
}

#[test]
fn rows_without_meta_or_with_garbage_are_refused() {
    let mut rows = sample().rows();
    rows.retain(|(k, _)| *k != RowKey::Meta);
    assert!(KvState::from_rows(rows).is_err());
    let rows = vec![(RowKey::Meta, vec![0xff, 0xff])];
    assert!(KvState::from_rows(rows).is_err());
}

#[test]
fn operations_report_the_rows_they_touch() {
    assert_eq!(KvOp::Put { key: "k".into(), value: vec![] }.touches(), Touch::Row(RowKey::Data("k".into())));
    assert_eq!(KvOp::Delete { key: "k".into() }.touches(), Touch::Row(RowKey::Data("k".into())));
    assert_eq!(
        KvOp::Cas { key: "k".into(), expected: None, new: vec![] }.touches(),
        Touch::Row(RowKey::Data("k".into()))
    );
    assert_eq!(KvOp::Get { key: "k".into() }.touches(), Touch::Meta);
    assert_eq!(
        KvOp::DeleteEntity { type_name: "T".into(), key: vec![1], locality: None }.touches(),
        Touch::Row(RowKey::Entity("T".into(), vec![1]))
    );
    assert_eq!(KvOp::Restore { state: vec![], shard: 0 }.touches(), Touch::All);
    assert_eq!(KvOp::Ingest { lo: 0, hi: 1, state: vec![] }.touches(), Touch::All);
    assert_eq!(KvOp::Release { lo: 0, hi: 1 }.touches(), Touch::All);
    assert_eq!(KvOp::Extract { lo: 0, hi: 1 }.touches(), Touch::Meta);
    assert_eq!(KvOp::Own { ranges: vec![] }.touches(), Touch::Meta);
}
