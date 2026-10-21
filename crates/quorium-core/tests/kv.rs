use quorium_core::kv::{Command, KvOp, KvState, Reply};

fn put(client: u64, seq: u64, key: &str, value: &str) -> Command {
    Command::new(client, seq, KvOp::Put { key: key.into(), value: value.as_bytes().to_vec() })
}

fn get(client: u64, seq: u64, key: &str) -> Command {
    Command::new(client, seq, KvOp::Get { key: key.into() })
}

#[test]
fn put_then_get() {
    let mut s = KvState::default();
    assert_eq!(s.apply(&put(1, 1, "a", "x")), Reply::Done);
    assert_eq!(s.apply(&get(1, 2, "a")), Reply::Value(Some(b"x".to_vec())));
    assert_eq!(s.apply(&get(1, 3, "b")), Reply::Value(None));
}

#[test]
fn retried_request_applies_exactly_once() {
    let mut s = KvState::default();
    let cas = Command::new(1, 1, KvOp::Cas { key: "k".into(), expected: None, new: b"1".to_vec() });
    assert_eq!(s.apply(&cas), Reply::Swapped(true));
    assert_eq!(s.apply(&cas), Reply::Swapped(true));
    assert_eq!(s.get("k"), Some(&b"1".to_vec()));
    let cas2 = Command::new(2, 1, KvOp::Cas { key: "k".into(), expected: None, new: b"2".to_vec() });
    assert_eq!(s.apply(&cas2), Reply::Swapped(false));
}

#[test]
fn acknowledged_sequences_are_stale_and_have_no_effect() {
    let mut s = KvState::default();
    s.apply(&put(1, 5, "a", "new").acknowledging_below(5));
    assert_eq!(s.apply(&put(1, 4, "a", "old")), Reply::Stale);
    assert_eq!(s.get("a"), Some(&b"new".to_vec()));
}

#[test]
fn concurrent_requests_from_one_client_may_arrive_out_of_order() {
    let mut s = KvState::default();
    assert_eq!(s.apply(&put(1, 2, "b", "two")), Reply::Done);
    assert_eq!(s.apply(&put(1, 1, "a", "one")), Reply::Done);
    assert_eq!(s.get("a"), Some(&b"one".to_vec()));
    assert_eq!(s.get("b"), Some(&b"two".to_vec()));
}

#[test]
fn retries_of_out_of_order_requests_return_the_cached_reply() {
    let mut s = KvState::default();
    let cas = Command::new(1, 1, KvOp::Cas { key: "k".into(), expected: None, new: b"x".to_vec() });
    s.apply(&put(1, 2, "other", "v"));
    assert_eq!(s.apply(&cas), Reply::Swapped(true));
    s.apply(&put(1, 3, "k", "changed"));
    assert_eq!(s.apply(&cas), Reply::Swapped(true));
    assert_eq!(s.get("k"), Some(&b"changed".to_vec()));
}

#[test]
fn acknowledgements_bound_the_reply_cache() {
    let mut s = KvState::default();
    for seq in 1..=100 {
        s.apply(&put(1, seq, "k", "v").acknowledging_below(seq));
    }
    assert!(s.cached_replies(1) <= 2, "{}", s.cached_replies(1));
}

#[test]
fn delete_removes_key() {
    let mut s = KvState::default();
    s.apply(&put(1, 1, "a", "x"));
    assert_eq!(s.apply(&Command::new(1, 2, KvOp::Delete { key: "a".into() })), Reply::Done);
    assert_eq!(s.get("a"), None);
}

#[test]
fn hash_is_equal_for_equal_states_and_differs_otherwise() {
    let mut a = KvState::default();
    let mut b = KvState::default();
    for s in [&mut a, &mut b] {
        s.apply(&put(1, 1, "a", "x"));
        s.apply(&put(2, 1, "b", "y"));
    }
    assert_eq!(a.state_hash(), b.state_hash());
    b.apply(&put(2, 2, "b", "z"));
    assert_ne!(a.state_hash(), b.state_hash());
}

#[test]
fn snapshot_round_trips() {
    let mut a = KvState::default();
    a.apply(&put(1, 1, "a", "x"));
    a.apply(&put(3, 9, "c", "z"));
    let bytes = a.to_bytes();
    let b = KvState::from_bytes(&bytes).unwrap();
    assert_eq!(a.state_hash(), b.state_hash());
    assert!(KvState::from_bytes(b"garbage").is_err());
}

#[test]
fn session_reports_last_applied_sequence() {
    let mut s = KvState::default();
    assert_eq!(s.session(1), None);
    s.apply(&put(1, 3, "a", "x"));
    s.apply(&put(1, 7, "a", "y"));
    assert_eq!(s.session(1), Some(7));
    s.apply(&put(1, 5, "a", "z"));
    assert_eq!(s.session(1), Some(7));
}

#[test]
fn command_reports_its_key_for_every_op() {
    assert_eq!(put(1, 1, "a", "x").key(), "a");
    assert_eq!(get(1, 1, "b").key(), "b");
    assert_eq!(Command::new(1, 1, KvOp::Delete { key: "c".into() }).key(), "c");
    assert_eq!(Command::new(1, 1, KvOp::Cas { key: "d".into(), expected: None, new: vec![] }).key(), "d");
}

#[test]
fn command_display_names_client_and_sequence() {
    assert!(put(4, 9, "a", "x").to_string().starts_with("4#9:Put"));
}

#[test]
fn len_and_emptiness_track_keys() {
    let mut s = KvState::default();
    assert!(s.is_empty());
    assert_eq!(s.len(), 0);
    s.apply(&put(1, 1, "a", "x"));
    s.apply(&put(1, 2, "b", "y"));
    assert!(!s.is_empty());
    assert_eq!(s.len(), 2);
}

#[test]
fn decode_error_explains_itself() {
    let err = KvState::from_bytes(b"").unwrap_err();
    assert_eq!(err.to_string(), "invalid key-value snapshot");
}

#[test]
fn commands_route_to_the_shard_of_their_key() {
    use quorium_core::entity::EntityCall;
    use quorium_core::shard::{entity_shard, shard_of};
    let kv = put(1, 1, "alpha", "x");
    assert_eq!(kv.shard(16), shard_of(b"alpha", 16));
    let call = EntityCall {
        type_name: "T".into(),
        version: 1,
        key: vec![7],
        method: 1,
        args: vec![],
        now_ms: 0,
        locality: None,
    };
    let apply = Command::new(1, 2, KvOp::Apply(call));
    assert_eq!(apply.shard(16), entity_shard("T", &[7], 16));
    let del = Command::new(1, 3, KvOp::DeleteEntity { type_name: "T".into(), key: vec![7], locality: None });
    assert_eq!(del.shard(16), apply.shard(16));
}

#[test]
fn restore_replaces_an_empty_state_only() {
    let mut source = KvState::default();
    source.apply(&put(1, 1, "a", "x"));
    let bytes = source.to_bytes();
    let mut empty = KvState::default();
    assert_eq!(empty.apply(&Command::new(9, 1, KvOp::Restore { state: bytes.clone(), shard: 0 })), Reply::Done);
    assert_eq!(empty.get("a"), Some(&b"x".to_vec()));
    let mut busy = KvState::default();
    busy.apply(&put(2, 1, "b", "y"));
    assert!(matches!(busy.apply(&Command::new(9, 1, KvOp::Restore { state: bytes, shard: 0 })), Reply::ApplyFailed(_)));
    assert_eq!(busy.get("a"), None);
    let mut fresh = KvState::default();
    assert!(matches!(
        fresh.apply(&Command::new(9, 1, KvOp::Restore { state: b"junk".to_vec(), shard: 0 })),
        Reply::ApplyFailed(_)
    ));
    assert!(fresh.is_pristine());
    assert_eq!(Command::new(9, 2, KvOp::Restore { state: vec![], shard: 5 }).shard(8), 5);
}

#[test]
fn byte_fields_keep_the_plain_sequence_encoding() {
    #[derive(serde::Serialize)]
    enum PlainOp {
        Put { key: String, value: Vec<u8> },
    }
    let value: Vec<u8> = (0..=255).collect();
    let ours = postcard::to_allocvec(&KvOp::Put { key: "k".into(), value: value.clone() }).unwrap();
    let plain = postcard::to_allocvec(&PlainOp::Put { key: "k".into(), value }).unwrap();
    assert_eq!(ours, plain);
    let reply = postcard::to_allocvec(&Reply::Value(Some(vec![1, 2, 3]))).unwrap();
    assert_eq!(postcard::from_bytes::<Reply>(&reply).unwrap(), Reply::Value(Some(vec![1, 2, 3])));
}
