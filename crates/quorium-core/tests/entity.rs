use linkme::distributed_slice;
use quorium_core::entity::{ApplyError, Ctx, EntityCall, REGISTRY, Registration, lookup};
use quorium_core::kv::{Command, KvOp, KvState, Reply};

fn counter_apply(state: Option<&[u8]>, method: u32, args: &[u8], ctx: &Ctx) -> Result<(Vec<u8>, Vec<u8>), ApplyError> {
    let current: u64 = state.map(|s| postcard::from_bytes(s).unwrap()).unwrap_or(0);
    let by: u64 = postcard::from_bytes(args).map_err(|_| ApplyError::Decode)?;
    match method {
        1 => {
            let next = current + by;
            Ok((postcard::to_allocvec(&next).unwrap(), postcard::to_allocvec(&next).unwrap()))
        }
        2 => Ok((postcard::to_allocvec(&current).unwrap(), postcard::to_allocvec(&ctx.index()).unwrap())),
        3 => panic!("user code panicked"),
        _ => Err(ApplyError::UnknownMethod(method)),
    }
}

#[distributed_slice(REGISTRY)]
static COUNTER: Registration =
    Registration { name: "test.Counter", version: 1, methods: &[1, 2, 3], apply: counter_apply, migrate: None };

fn call(method: u32, by: u64) -> EntityCall {
    EntityCall {
        type_name: "test.Counter".into(),
        version: 1,
        key: b"c1".to_vec(),
        method,
        args: postcard::to_allocvec(&by).unwrap(),
        now_ms: 1_700_000_000_000,
        locality: None,
    }
}

#[test]
fn registry_finds_link_time_registrations() {
    assert!(lookup("test.Counter").is_some());
    assert!(lookup("missing").is_none());
}

#[test]
fn entity_writes_apply_through_the_registry() {
    let mut s = KvState::default();
    let r = s.apply_at(&Command::new(1, 1, KvOp::Apply(call(1, 5))), 10).unwrap();
    assert_eq!(r, Reply::Applied(postcard::to_allocvec(&5u64).unwrap()));
    let r = s.apply_at(&Command::new(1, 2, KvOp::Apply(call(1, 2))), 11).unwrap();
    assert_eq!(r, Reply::Applied(postcard::to_allocvec(&7u64).unwrap()));
    assert_eq!(
        s.entity("test.Counter", b"c1").map(|(v, b)| (v, b.to_vec())),
        Some((1, postcard::to_allocvec(&7u64).unwrap()))
    );
}

#[test]
fn ctx_exposes_the_log_index() {
    let mut s = KvState::default();
    let r = s.apply_at(&Command::new(1, 1, KvOp::Apply(call(2, 0))), 42).unwrap();
    assert_eq!(r, Reply::Applied(postcard::to_allocvec(&42u64).unwrap()));
}

#[test]
fn ctx_randomness_and_time_are_deterministic() {
    let a = Ctx::new(7, 1_000, 99);
    let b = Ctx::new(7, 1_000, 99);
    assert_eq!((a.random(), a.random()), (b.random(), b.random()));
    assert_ne!(Ctx::new(7, 1_000, 99).random(), Ctx::new(8, 1_000, 99).random());
    assert_eq!(a.now(), std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_000));
}

#[test]
fn user_panics_become_failed_replies_without_state_change() {
    let mut s = KvState::default();
    s.apply_at(&Command::new(1, 1, KvOp::Apply(call(1, 3))), 1).unwrap();
    let before = s.state_hash();
    let r = s.apply_at(&Command::new(1, 2, KvOp::Apply(call(3, 0))), 2).unwrap();
    assert!(matches!(r, Reply::ApplyFailed(_)));
    let mut expected = KvState::default();
    expected.apply_at(&Command::new(1, 1, KvOp::Apply(call(1, 3))), 1).unwrap();
    expected.apply_at(&Command::new(1, 2, KvOp::Get { key: "x".into() }), 2).unwrap();
    assert_eq!(s.entity("test.Counter", b"c1"), expected.entity("test.Counter", b"c1"));
    assert_ne!(before, 0);
}

#[test]
fn unknown_methods_stop_the_replica_and_are_flagged_before_proposal() {
    let mut s = KvState::default();
    assert_eq!(s.apply_at(&Command::new(1, 1, KvOp::Apply(call(9, 0))), 1), Err(ApplyError::UnknownMethod(9)));
    assert!(lookup("test.Counter").unwrap().accepts(1));
    assert!(!lookup("test.Counter").unwrap().accepts(9));
}

#[test]
fn unknown_types_stop_the_replica() {
    let mut s = KvState::default();
    let mut c = call(1, 1);
    c.type_name = "nobody.Knows".into();
    assert_eq!(s.apply_at(&Command::new(1, 1, KvOp::Apply(c)), 1), Err(ApplyError::UnknownType("nobody.Knows".into())));
}

#[test]
fn stored_types_are_checked_against_the_registry() {
    let mut s = KvState::default();
    s.apply_at(&Command::new(1, 1, KvOp::Apply(call(1, 1))), 1).unwrap();
    assert_eq!(s.unknown_types(), Vec::<String>::new());
}

#[test]
fn entities_can_be_deleted() {
    let mut s = KvState::default();
    s.apply_at(&Command::new(1, 1, KvOp::Apply(call(1, 1))), 1).unwrap();
    let del = KvOp::DeleteEntity { type_name: "test.Counter".into(), key: b"c1".to_vec(), locality: None };
    assert_eq!(s.apply_at(&Command::new(1, 2, del), 2).unwrap(), Reply::Done);
    assert!(s.entity("test.Counter", b"c1").is_none());
}

#[test]
fn registry_fingerprint_lists_every_type_and_method() {
    use quorium_core::entity::local_methods;
    let methods = local_methods();
    assert!(methods.contains(&("test.Counter".to_string(), 1)));
    assert!(methods.contains(&("test.Counter".to_string(), 3)));
}

#[test]
fn a_method_is_enabled_only_when_every_voter_knows_it() {
    use quorium_core::entity::method_enabled;
    use std::collections::{BTreeMap, BTreeSet};
    let old: BTreeSet<(String, u32)> = [("T".to_string(), 1)].into();
    let new: BTreeSet<(String, u32)> = [("T".to_string(), 1), ("T".to_string(), 2)].into();
    let mut cluster: BTreeMap<u64, BTreeSet<(String, u32)>> = BTreeMap::new();
    cluster.insert(1, new.clone());
    cluster.insert(2, old.clone());
    let voters = [1u64, 2];
    assert!(method_enabled("T", 1, &voters, &cluster));
    assert!(!method_enabled("T", 2, &voters, &cluster));
    cluster.insert(2, new);
    assert!(method_enabled("T", 2, &voters, &cluster));
    assert!(!method_enabled("T", 2, &[1, 2, 3], &cluster));
}

#[test]
fn silent_voters_are_unknown_rather_than_old() {
    use quorium_core::entity::{MethodSupport, method_support};
    use std::collections::{BTreeMap, BTreeSet};
    let new: BTreeSet<(String, u32)> = [("T".to_string(), 2)].into();
    let old: BTreeSet<(String, u32)> = [("T".to_string(), 1)].into();
    let mut cluster: BTreeMap<u64, BTreeSet<(String, u32)>> = BTreeMap::new();
    cluster.insert(1, new.clone());
    assert_eq!(method_support("T", 2, &[1, 2], &cluster), MethodSupport::Unknown);
    cluster.insert(2, old);
    assert_eq!(method_support("T", 2, &[1, 2], &cluster), MethodSupport::Missing);
    cluster.insert(2, new);
    assert_eq!(method_support("T", 2, &[1, 2], &cluster), MethodSupport::Enabled);
}

fn explicit_apply(_: Option<&[u8]>, _: u32, _: &[u8], _: &Ctx) -> Result<(Vec<u8>, Vec<u8>), ApplyError> {
    Ok((vec![9], vec![]))
}

static EXPLICIT: Registration =
    Registration { name: "test.Explicit", version: 1, methods: &[5], apply: explicit_apply, migrate: None };

#[test]
fn explicit_registration_complements_link_time_registration() {
    assert!(lookup("test.Explicit").is_none());
    quorium_core::entity::register(&EXPLICIT);
    quorium_core::entity::register(&EXPLICIT);
    assert_eq!(lookup("test.Explicit").map(|r| r.version), Some(1));
    assert_eq!(quorium_core::entity::registered_names().iter().filter(|n| **n == "test.Explicit").count(), 1);
    assert!(quorium_core::entity::local_methods().contains(&("test.Explicit".to_string(), 5)));
    quorium_core::entity::register(lookup("test.Counter").unwrap());
    assert_eq!(quorium_core::entity::registered_names().iter().filter(|n| **n == "test.Counter").count(), 1);
}
