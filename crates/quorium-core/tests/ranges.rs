use quorium_core::entity::EntityCall;
use quorium_core::kv::{Command, KvOp, KvState, Reply};
use quorium_core::shard::{entity_position, entity_shard_with_locality, key_hash, position_shard, shard_of};

const HALF: u64 = u64::MAX / 2;

fn put(client: u64, seq: u64, key: &str) -> Command {
    Command::new(client, seq, KvOp::Put { key: key.into(), value: key.as_bytes().to_vec() })
}

fn key_in(lo: u64, hi: u64, n: usize) -> Vec<String> {
    (0..10_000).map(|i| format!("key-{i}")).filter(|k| (lo..=hi).contains(&key_hash(k.as_bytes()))).take(n).collect()
}

#[test]
fn positions_agree_with_shard_placement() {
    for i in 0..200u32 {
        let key = format!("k{i}");
        assert_eq!(shard_of(key.as_bytes(), 16), position_shard(key_hash(key.as_bytes()), 16));
        let pos = entity_position("T", &i.to_le_bytes(), Some("eu"));
        assert_eq!(entity_shard_with_locality("T", &i.to_le_bytes(), Some("eu"), 16), position_shard(pos, 16));
    }
}

#[test]
fn writes_outside_owned_ranges_are_refused_without_effect() {
    let mut s = KvState::default();
    s.apply(&Command::new(9, 1, KvOp::Own { ranges: vec![(0, HALF)] }));
    let inside = &key_in(0, HALF, 1)[0];
    let outside = &key_in(HALF + 1, u64::MAX, 1)[0];
    assert_eq!(s.apply(&put(1, 1, inside)), Reply::Done);
    assert_eq!(s.apply(&put(1, 2, outside)), Reply::WrongShard);
    assert_eq!(s.get(outside), None);
    assert!(!s.applied(1, 2));
    assert!(s.owns(key_hash(inside.as_bytes())));
    assert!(!s.owns(key_hash(outside.as_bytes())));
}

#[test]
fn unowned_states_accept_everything() {
    let mut s = KvState::default();
    assert_eq!(s.apply(&put(1, 1, "anything")), Reply::Done);
    assert!(s.owns(12345));
}

#[test]
fn extract_freezes_then_ingest_and_release_complete_the_move() {
    let mut source = KvState::default();
    source.apply(&Command::new(9, 1, KvOp::Own { ranges: vec![(0, u64::MAX)] }));
    let moving = key_in(HALF + 1, u64::MAX, 5);
    let staying = key_in(0, HALF, 5);
    for (i, k) in moving.iter().chain(staying.iter()).enumerate() {
        source.apply(&put(1, i as u64 + 1, k));
    }
    let call = EntityCall {
        type_name: "absent.Type".into(),
        version: 1,
        key: vec![1],
        method: 1,
        args: vec![],
        now_ms: 0,
        locality: None,
    };
    let _ = call;
    let extracted = match source.apply(&Command::new(9, 2, KvOp::Extract { lo: HALF + 1, hi: u64::MAX })) {
        Reply::Extracted(bytes) => bytes,
        other => panic!("{other:?}"),
    };
    assert_eq!(source.apply(&put(2, 1, &moving[0])), Reply::WrongShard);
    assert_eq!(source.apply(&put(2, 2, &staying[0])), Reply::Done);
    assert_eq!(
        source.apply(&Command::new(9, 3, KvOp::Extract { lo: HALF + 1, hi: u64::MAX })),
        Reply::Extracted(extracted.clone())
    );
    let mut target = KvState::default();
    target.apply(&Command::new(9, 1, KvOp::Own { ranges: vec![] }));
    assert_eq!(
        target.apply(&Command::new(9, 2, KvOp::Ingest { lo: HALF + 1, hi: u64::MAX, state: extracted })),
        Reply::Done
    );
    for k in &moving {
        assert_eq!(target.get(k), Some(&k.as_bytes().to_vec()));
    }
    assert_eq!(target.get(&staying[0]), None);
    assert_eq!(target.apply(&put(3, 1, &moving[1])), Reply::Done);
    assert_eq!(source.apply(&Command::new(9, 4, KvOp::Release { lo: HALF + 1, hi: u64::MAX })), Reply::Done);
    for k in &moving {
        assert_eq!(source.get(k), None);
    }
    assert_eq!(source.get(&staying[1]), Some(&staying[1].as_bytes().to_vec()));
}

#[test]
fn a_retry_after_a_move_is_not_applied_twice() {
    let mut source = KvState::default();
    source.apply(&Command::new(9, 1, KvOp::Own { ranges: vec![(0, u64::MAX)] }));
    let key = &key_in(HALF + 1, u64::MAX, 1)[0];
    let cas = Command::new(4, 7, KvOp::Cas { key: key.clone(), expected: None, new: b"first".to_vec() });
    assert_eq!(source.apply(&cas), Reply::Swapped(true));
    let Reply::Extracted(bytes) = source.apply(&Command::new(9, 2, KvOp::Extract { lo: HALF + 1, hi: u64::MAX }))
    else {
        panic!()
    };
    let mut target = KvState::default();
    target.apply(&Command::new(9, 1, KvOp::Own { ranges: vec![] }));
    target.apply(&Command::new(9, 2, KvOp::Ingest { lo: HALF + 1, hi: u64::MAX, state: bytes }));
    target.apply(&Command::new(5, 1, KvOp::Put { key: key.clone(), value: b"later".to_vec() }));
    assert_eq!(target.apply(&cas), Reply::Swapped(true));
    assert_eq!(target.get(key), Some(&b"later".to_vec()));
}

#[test]
fn ingest_is_idempotent() {
    let mut source = KvState::default();
    source.apply(&put(1, 1, "a"));
    let Reply::Extracted(bytes) = source.apply(&Command::new(9, 1, KvOp::Extract { lo: 0, hi: u64::MAX })) else {
        panic!()
    };
    let mut target = KvState::default();
    target.apply(&Command::new(9, 1, KvOp::Own { ranges: vec![] }));
    let ingest = Command::new(9, 2, KvOp::Ingest { lo: 0, hi: u64::MAX, state: bytes.clone() });
    assert_eq!(target.apply(&ingest), Reply::Done);
    assert_eq!(target.apply(&Command::new(9, 3, KvOp::Ingest { lo: 0, hi: u64::MAX, state: bytes })), Reply::Done);
    assert_eq!(target.get("a"), Some(&b"a".to_vec()));
}

fn route(s: &mut KvState, seq: u64, op: quorium_core::kv::RouteOp) -> Reply {
    s.apply(&Command::new(7, seq, KvOp::Route(op)))
}

fn table(r: Reply) -> quorium_core::kv::RoutingTable {
    match r {
        Reply::Routing(t) => t,
        other => panic!("{other:?}"),
    }
}

#[test]
fn routing_tables_split_move_and_merge() {
    use quorium_core::kv::{RouteOp, RoutingTable};
    let mut meta = KvState::default();
    let uniform = RoutingTable::uniform(2);
    assert_eq!(uniform.ranges, vec![(0, HALF, 0), (HALF + 1, u64::MAX, 1)]);
    let t = table(route(&mut meta, 1, RouteOp::Init { table: uniform.clone() }));
    assert_eq!(t, uniform);
    assert_eq!(table(route(&mut meta, 2, RouteOp::Init { table: RoutingTable::uniform(9) })), uniform);
    let quarter = HALF / 2;
    let t = table(route(&mut meta, 3, RouteOp::Split { at: quarter, generation: t.generation }));
    assert_eq!(t.ranges, vec![(0, quarter - 1, 0), (quarter, HALF, 0), (HALF + 1, u64::MAX, 1)]);
    assert!(matches!(route(&mut meta, 4, RouteOp::Split { at: 5, generation: 0 }), Reply::ApplyFailed(_)));
    let t = table(route(&mut meta, 5, RouteOp::BeginMove { lo: quarter, hi: HALF, to: 1, generation: t.generation }));
    assert_eq!(t.moving, Some((quarter, HALF, 0, 1)));
    assert_eq!(t.group_of(quarter + 1), 0);
    let again = table(route(&mut meta, 6, RouteOp::BeginMove { lo: quarter, hi: HALF, to: 1, generation: 0 }));
    assert_eq!(again.moving, t.moving);
    let t = table(route(&mut meta, 7, RouteOp::FinishMove { lo: quarter, hi: HALF, to: 1 }));
    assert_eq!(t.moving, None);
    assert_eq!(t.group_of(quarter + 1), 1);
    let t = table(route(&mut meta, 8, RouteOp::Merge { generation: t.generation }));
    assert_eq!(t.ranges, vec![(0, quarter - 1, 0), (quarter, u64::MAX, 1)]);
}

#[test]
fn own_if_unset_only_applies_once() {
    let mut s = KvState::default();
    s.apply(&Command::new(9, 1, KvOp::OwnIfUnset { ranges: vec![(0, 10)] }));
    s.apply(&Command::new(9, 2, KvOp::OwnIfUnset { ranges: vec![(0, u64::MAX)] }));
    assert_eq!(s.owned_ranges(), Some(vec![(0, 10)]));
}
