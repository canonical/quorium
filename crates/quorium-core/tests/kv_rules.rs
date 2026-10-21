use std::collections::BTreeMap;

use linkme::distributed_slice;
use quorium_core::entity::{ApplyError, Ctx, EntityCall, REGISTRY, Registration};
use quorium_core::kv::{
    Command, EscrowAction, EscrowView, KvOp, KvState, LeaseAction, LeaseOp, Reply, RouteOp, RoutingTable, RowKey,
};
use quorium_core::shard::{key_hash, shard_of};

const MAX: u64 = u64::MAX;

struct Client {
    seq: u64,
}

impl Client {
    fn new() -> Self {
        Client { seq: 0 }
    }

    fn run(&mut self, s: &mut KvState, op: KvOp) -> Reply {
        self.seq += 1;
        s.apply(&Command::new(1, self.seq, op))
    }
}

fn put(key: &str) -> KvOp {
    KvOp::Put { key: key.into(), value: vec![1] }
}

#[test]
fn owned_ranges_are_sorted_and_merged_without_overflow() {
    let mut s = KvState::default();
    let mut c = Client::new();
    c.run(&mut s, KvOp::Own { ranges: vec![(21, 30), (10, 20), (50, 60), (32, 40)] });
    assert_eq!(s.owned_ranges(), Some(vec![(10, 30), (32, 40), (50, 60)]));
    c.run(&mut s, KvOp::Own { ranges: vec![(0, MAX), (3, 7)] });
    assert_eq!(s.owned_ranges(), Some(vec![(0, MAX), (3, 7)]));
    c.run(&mut s, KvOp::Own { ranges: vec![(5, MAX), (0, 4)] });
    assert_eq!(s.owned_ranges(), Some(vec![(0, MAX)]));
    c.run(&mut s, KvOp::OwnIfUnset { ranges: vec![(1, 2)] });
    assert_eq!(s.owned_ranges(), Some(vec![(0, MAX)]));
    let mut fresh = KvState::default();
    c.run(&mut fresh, KvOp::OwnIfUnset { ranges: vec![(1, 2)] });
    assert_eq!(fresh.owned_ranges(), Some(vec![(1, 2)]));
}

#[test]
fn extracting_cuts_holes_in_ownership() {
    let mut s = KvState::default();
    let mut c = Client::new();
    c.run(&mut s, KvOp::Extract { lo: 100, hi: 200 });
    assert_eq!(s.owned_ranges(), Some(vec![(0, 99), (201, MAX)]));
    c.run(&mut s, KvOp::Extract { lo: 0, hi: 50 });
    assert_eq!(s.owned_ranges(), Some(vec![(51, 99), (201, MAX)]));
    c.run(&mut s, KvOp::Extract { lo: 300, hi: MAX });
    assert_eq!(s.owned_ranges(), Some(vec![(51, 99), (201, 299)]));
    c.run(&mut s, KvOp::Extract { lo: 60, hi: 250 });
    assert_eq!(s.owned_ranges(), Some(vec![(51, 59), (251, 299)]));
    c.run(&mut s, KvOp::Extract { lo: 1000, hi: 2000 });
    assert_eq!(s.owned_ranges(), Some(vec![(51, 59), (251, 299)]));
    assert!(s.owns(55) && !s.owns(60) && !s.owns(250) && s.owns(299) && !s.owns(300));
}

#[test]
fn ingest_skips_ranges_already_owned_and_merges_new_ones() {
    let mut donor = KvState::default();
    let mut c = Client::new();
    c.run(&mut donor, put("moved"));
    let p = key_hash(b"moved");
    let Reply::Extracted(bytes) = c.run(&mut donor, KvOp::Extract { lo: p, hi: p }) else { panic!() };
    let mut covered = KvState::default();
    c.run(&mut covered, KvOp::Own { ranges: vec![(0, MAX)] });
    assert_eq!(c.run(&mut covered, KvOp::Ingest { lo: p, hi: p, state: bytes.clone() }), Reply::Done);
    assert_eq!(covered.get("moved"), None);
    let mut partial = KvState::default();
    let below = p.saturating_sub(10);
    c.run(&mut partial, KvOp::Own { ranges: vec![(0, below)] });
    c.run(&mut partial, KvOp::Ingest { lo: p, hi: p, state: bytes.clone() });
    assert_eq!(partial.get("moved"), Some(&vec![1]));
    assert!(partial.owns(p));
    let mut adjacent = KvState::default();
    c.run(&mut adjacent, KvOp::Own { ranges: vec![(p, p)] });
    c.run(&mut adjacent, KvOp::Ingest { lo: p.saturating_sub(5), hi: p, state: bytes });
    assert_eq!(adjacent.get("moved"), Some(&vec![1]));
}

#[test]
fn release_drops_only_what_left_the_shard() {
    let mut s = KvState::default();
    let mut c = Client::new();
    c.run(&mut s, put("gone"));
    c.run(&mut s, put("stays"));
    let p = key_hash(b"gone");
    c.run(&mut s, KvOp::Extract { lo: p, hi: p });
    c.run(&mut s, KvOp::Release { lo: p, hi: p });
    assert_eq!(s.get("gone"), None);
    assert_eq!(s.get("stays"), Some(&vec![1]));
    let q = key_hash(b"stays");
    c.run(&mut s, KvOp::Release { lo: q, hi: q });
    assert_eq!(s.get("stays"), Some(&vec![1]));
}

#[test]
fn item_count_adds_every_kind_of_owned_item() {
    let mut s = KvState::default();
    let mut c = Client::new();
    c.run(&mut s, put("a"));
    c.run(&mut s, put("b"));
    c.run(&mut s, put("c"));
    c.run(
        &mut s,
        KvOp::Lease(LeaseOp { name: "job".into(), holder: 1, ttl_ms: 10, now_ms: 0, action: LeaseAction::Acquire }),
    );
    c.run(
        &mut s,
        KvOp::Escrow(quorium_core::kv::EscrowOp {
            name: "e".into(),
            node: 1,
            action: EscrowAction::Refill { units: 1 },
        }),
    );
    assert_eq!(s.item_count(), 5);
    let p = key_hash(b"a");
    c.run(&mut s, KvOp::Extract { lo: p, hi: p });
    assert_eq!(s.item_count(), 4);
}

#[test]
fn sessions_report_applied_sequences_and_cached_replies() {
    let mut s = KvState::default();
    s.apply(&Command::new(9, 1, put("x")));
    s.apply(&Command::new(9, 2, put("y")));
    assert!(s.applied(9, 1) && s.applied(9, 2));
    assert!(!s.applied(9, 3) && !s.applied(8, 1));
    assert_eq!(s.cached_replies(9), 2);
    assert_eq!(s.cached_replies(8), 0);
}

fn table() -> RoutingTable {
    RoutingTable { generation: 0, ranges: vec![(0, 99, 0), (100, MAX, 1)], moving: None }
}

fn routing(r: Reply) -> RoutingTable {
    match r {
        Reply::Routing(t) => t,
        other => panic!("{other:?}"),
    }
}

fn failed(r: Reply) -> bool {
    matches!(r, Reply::ApplyFailed(_))
}

#[test]
fn routing_splits_moves_and_merges_with_generations() {
    let mut s = KvState::default();
    let mut c = Client::new();
    assert!(s.routing().is_none());
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::Merge { generation: 0 }))));
    routing(c.run(&mut s, KvOp::Route(RouteOp::Init { table: table() })));
    assert_eq!(s.routing(), Some(&table()));
    assert_eq!(routing(c.run(&mut s, KvOp::Route(RouteOp::Init { table: RoutingTable::uniform(5) }))), table());
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::Split { at: 100, generation: 0 }))));
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::Split { at: 0, generation: 0 }))));
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::Split { at: 50, generation: 7 }))));
    let t = routing(c.run(&mut s, KvOp::Route(RouteOp::Split { at: 50, generation: 0 })));
    assert_eq!(t.ranges, vec![(0, 49, 0), (50, 99, 0), (100, MAX, 1)]);
    assert_eq!(t.generation, 1);
    assert_eq!(t.ranges_of(0), vec![(0, 49), (50, 99)]);
    assert_eq!(t.ranges_of(1), vec![(100, MAX)]);
    assert_eq!(t.ranges_of(2), vec![]);
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::BeginMove { lo: 50, hi: 99, to: 1, generation: 0 }))));
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::BeginMove { lo: 50, hi: 98, to: 1, generation: 1 }))));
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::BeginMove { lo: 50, hi: 99, to: 0, generation: 1 }))));
    let t = routing(c.run(&mut s, KvOp::Route(RouteOp::BeginMove { lo: 50, hi: 99, to: 1, generation: 1 })));
    assert_eq!((t.moving, t.generation), (Some((50, 99, 0, 1)), 2));
    let again = routing(c.run(&mut s, KvOp::Route(RouteOp::BeginMove { lo: 50, hi: 99, to: 1, generation: 0 })));
    assert_eq!(again, t);
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::BeginMove { lo: 0, hi: 49, to: 1, generation: 2 }))));
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::Split { at: 70, generation: 2 }))));
    let pinned = routing(c.run(&mut s, KvOp::Route(RouteOp::Merge { generation: 2 })));
    assert_eq!(pinned.ranges, vec![(0, 49, 0), (50, 99, 0), (100, MAX, 1)]);
    assert_eq!(pinned.generation, 2);
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::FinishMove { lo: 50, hi: 99, to: 2 }))));
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::FinishMove { lo: 50, hi: 98, to: 1 }))));
    let t = routing(c.run(&mut s, KvOp::Route(RouteOp::FinishMove { lo: 50, hi: 99, to: 1 })));
    assert_eq!((t.ranges.clone(), t.moving, t.generation), (vec![(0, 49, 0), (50, 99, 1), (100, MAX, 1)], None, 3));
    assert_eq!(routing(c.run(&mut s, KvOp::Route(RouteOp::FinishMove { lo: 50, hi: 99, to: 1 }))), t);
    assert!(failed(c.run(&mut s, KvOp::Route(RouteOp::Merge { generation: 2 }))));
    let merged = routing(c.run(&mut s, KvOp::Route(RouteOp::Merge { generation: 3 })));
    assert_eq!((merged.ranges.clone(), merged.generation), (vec![(0, 49, 0), (50, MAX, 1)], 4));
    let unchanged = routing(c.run(&mut s, KvOp::Route(RouteOp::Merge { generation: 4 })));
    assert_eq!(unchanged, merged);
    let split = routing(c.run(&mut s, KvOp::Route(RouteOp::Split { at: 49, generation: 4 })));
    assert_eq!(split.ranges, vec![(0, 48, 0), (49, 49, 0), (50, MAX, 1)]);
}

#[test]
fn merging_skips_gaps_and_the_moving_range() {
    let mut s = KvState::default();
    let mut c = Client::new();
    let gapped = RoutingTable { generation: 0, ranges: vec![(0, 9, 0), (11, 20, 0), (21, 30, 0)], moving: None };
    c.run(&mut s, KvOp::Route(RouteOp::Init { table: gapped }));
    let t = routing(c.run(&mut s, KvOp::Route(RouteOp::Merge { generation: 0 })));
    assert_eq!(t.ranges, vec![(0, 9, 0), (11, 30, 0)]);
    let mut m = KvState::default();
    let three = RoutingTable { generation: 0, ranges: vec![(0, 9, 0), (10, 19, 0), (20, 29, 1)], moving: None };
    c.run(&mut m, KvOp::Route(RouteOp::Init { table: three }));
    c.run(&mut m, KvOp::Route(RouteOp::BeginMove { lo: 0, hi: 9, to: 1, generation: 0 }));
    let t = routing(c.run(&mut m, KvOp::Route(RouteOp::Merge { generation: 1 })));
    assert_eq!(t.ranges, vec![(0, 9, 0), (10, 19, 0), (20, 29, 1)]);
}

#[test]
fn admin_operations_go_to_the_meta_group() {
    let shards = 1 << 20;
    for op in [
        KvOp::Own { ranges: vec![] },
        KvOp::Extract { lo: 0, hi: 1 },
        KvOp::Ingest { lo: 0, hi: 1, state: vec![] },
        KvOp::Release { lo: 0, hi: 1 },
        KvOp::OwnIfUnset { ranges: vec![] },
        KvOp::Route(RouteOp::Merge { generation: 0 }),
    ] {
        assert_eq!(Command::new(1, 1, op).shard(shards), 0);
    }
    assert_ne!(shard_of(b"", shards), 0);
}

#[test]
fn leases_follow_holder_and_token_rules() {
    let mut s = KvState::default();
    let mut c = Client::new();
    let lease = |holder: u64, now_ms: u64, action: LeaseAction| {
        KvOp::Lease(LeaseOp { name: "job".into(), holder, ttl_ms: 100, now_ms, action })
    };
    let held = |r: Reply| match r {
        Reply::Lease { held, token, holder, expires_ms } => (held, token, holder, expires_ms),
        other => panic!("{other:?}"),
    };
    assert_eq!(held(c.run(&mut s, lease(1, 0, LeaseAction::Acquire))), (true, 1, 1, 100));
    assert_eq!(held(c.run(&mut s, lease(1, 10, LeaseAction::Acquire))), (true, 1, 1, 100));
    assert_eq!(held(c.run(&mut s, lease(2, 20, LeaseAction::Acquire))), (false, 1, 1, 100));
    assert_eq!(held(c.run(&mut s, lease(1, 50, LeaseAction::Renew { token: 2 }))), (false, 1, 1, 100));
    assert_eq!(held(c.run(&mut s, lease(2, 50, LeaseAction::Renew { token: 1 }))), (false, 1, 1, 100));
    assert_eq!(held(c.run(&mut s, lease(1, 50, LeaseAction::Renew { token: 1 }))), (true, 1, 1, 150));
    assert_eq!(held(c.run(&mut s, lease(2, 60, LeaseAction::Release { token: 1 }))), (false, 1, 1, 150));
    assert_eq!(held(c.run(&mut s, lease(1, 60, LeaseAction::Release { token: 9 }))), (false, 1, 1, 150));
    assert_eq!(held(c.run(&mut s, lease(1, 60, LeaseAction::Release { token: 1 }))), (false, 1, 1, 0));
    assert_eq!(held(c.run(&mut s, lease(2, 70, LeaseAction::Acquire))), (true, 2, 2, 170));
    assert_eq!(held(c.run(&mut s, lease(1, 300, LeaseAction::Renew { token: 2 }))), (false, 2, 2, 170));
}

#[test]
fn escrow_grants_and_buys_have_exact_arithmetic() {
    let mut v = EscrowView::default();
    assert_eq!(v.apply(1, EscrowAction::Grant { want: 5 }), Ok(0));
    assert!(v.outstanding.is_empty());
    v.apply(0, EscrowAction::Refill { units: 100 }).unwrap();
    assert_eq!(v.apply(1, EscrowAction::Grant { want: 50 }), Ok(25));
    assert_eq!((v.pool, v.outstanding.get(&1).copied()), (75, Some(25)));
    assert_eq!(v.apply(2, EscrowAction::Buy { units: 3 }), Ok(3));
    assert_eq!((v.pool, v.sold), (72, 3));
    assert_eq!(v.apply(2, EscrowAction::Buy { units: 73 }), Ok(0));
    assert_eq!((v.pool, v.sold), (72, 3));
    let mut small = EscrowView::default();
    small.apply(0, EscrowAction::Refill { units: 2 }).unwrap();
    assert_eq!(small.apply(1, EscrowAction::Grant { want: 9 }), Ok(1));
}

fn versioned_apply(stored: Option<&[u8]>, method: u32, _: &[u8], _: &Ctx) -> Result<(Vec<u8>, Vec<u8>), ApplyError> {
    let current = stored.unwrap_or_default().to_vec();
    let mut next = current.clone();
    next.push(method as u8);
    Ok((next, current))
}

fn versioned_migrate(from: u32, bytes: &[u8]) -> Result<Vec<u8>, ApplyError> {
    if from == 0 {
        return Err(ApplyError::Migration("too old".into()));
    }
    Ok([b"m".as_slice(), bytes].concat())
}

#[distributed_slice(REGISTRY)]
static VERSIONED: Registration = Registration {
    name: "rules.Versioned",
    version: 2,
    methods: &[1],
    apply: versioned_apply,
    migrate: Some(versioned_migrate),
};

#[distributed_slice(REGISTRY)]
static RIGID: Registration =
    Registration { name: "rules.Rigid", version: 2, methods: &[1], apply: versioned_apply, migrate: None };

fn stored_at(type_name: &str, version: u32, bytes: &[u8]) -> KvState {
    let slot = postcard::to_allocvec(&(version, bytes.to_vec(), 0u64)).unwrap();
    let meta = KvState::default().row(&RowKey::Meta).unwrap();
    KvState::from_rows([(RowKey::Meta, meta), (RowKey::Entity(type_name.into(), b"k".to_vec()), slot)]).unwrap()
}

fn call(type_name: &str, version: u32) -> Command {
    Command::new(
        1,
        1,
        KvOp::Apply(EntityCall {
            type_name: type_name.into(),
            version,
            key: b"k".to_vec(),
            method: 1,
            args: vec![],
            now_ms: 0,
            locality: None,
        }),
    )
}

#[test]
fn stored_versions_are_migrated_kept_or_refused() {
    let mut current = stored_at("rules.Versioned", 2, b"x");
    assert_eq!(current.apply_at(&call("rules.Versioned", 2), 1), Ok(Reply::Applied(b"x".to_vec())));
    let mut old = stored_at("rules.Versioned", 1, b"x");
    assert_eq!(old.apply_at(&call("rules.Versioned", 2), 1), Ok(Reply::Applied(b"mx".to_vec())));
    assert_eq!(old.entity("rules.Versioned", b"k"), Some((2, b"mx\x01".as_slice())));
    let mut ancient = stored_at("rules.Versioned", 0, b"x");
    assert!(matches!(ancient.apply_at(&call("rules.Versioned", 2), 1), Ok(Reply::ApplyFailed(_))));
    let mut rigid = stored_at("rules.Rigid", 1, b"x");
    assert!(
        matches!(rigid.apply_at(&call("rules.Rigid", 2), 1), Ok(Reply::ApplyFailed(e)) if e.contains("no migration"))
    );
    let mut newer = stored_at("rules.Versioned", 3, b"x");
    assert!(matches!(
        newer.apply_at(&call("rules.Versioned", 2), 1),
        Err(ApplyError::UnsupportedVersion { stored: 3, .. })
    ));
    let mut fresh = KvState::default();
    assert!(matches!(
        fresh.apply_at(&call("rules.Versioned", 3), 1),
        Err(ApplyError::UnsupportedVersion { stored: 3, .. })
    ));
    assert_eq!(fresh.apply_at(&call("rules.Versioned", 1), 1), Ok(Reply::Applied(vec![])));
}

#[test]
fn unknown_types_are_listed_once() {
    let mut s = stored_at("rules.Nope", 1, b"x");
    let meta = s.row(&RowKey::Meta).unwrap();
    let slot = postcard::to_allocvec(&(1u32, b"y".to_vec(), 0u64)).unwrap();
    s = KvState::from_rows([
        (RowKey::Meta, meta),
        (RowKey::Entity("rules.Nope".into(), b"a".to_vec()), slot.clone()),
        (RowKey::Entity("rules.Nope".into(), b"b".to_vec()), slot.clone()),
        (RowKey::Entity("rules.Versioned".into(), b"c".to_vec()), slot),
    ])
    .unwrap();
    assert_eq!(s.unknown_types(), vec!["rules.Nope".to_string()]);
}

#[test]
fn decoding_rejects_trailing_bytes_and_reads_format_two() {
    let mut s = KvState::default();
    Client::new().run(&mut s, put("k"));
    let mut bytes = s.to_bytes();
    assert_eq!(KvState::from_bytes(&bytes).unwrap(), s);
    bytes.push(0);
    assert!(KvState::from_bytes(&bytes).is_err());
    let data: BTreeMap<String, Vec<u8>> = [("k".to_string(), vec![5u8])].into();
    let empty: BTreeMap<u64, u64> = BTreeMap::new();
    let mut v2 = vec![2u8];
    v2.extend(postcard::to_allocvec(&(data, &empty, &empty, &empty, &empty)).unwrap());
    let decoded = KvState::from_bytes(&v2).unwrap();
    assert_eq!(decoded.get("k"), Some(&vec![5]));
    v2.push(0);
    assert!(KvState::from_bytes(&v2).is_err());
    let meta = KvState::default().row(&RowKey::Meta).unwrap();
    let mut slot = postcard::to_allocvec(&(1u32, b"y".to_vec(), 0u64)).unwrap();
    slot.push(0);
    assert!(KvState::from_rows([(RowKey::Meta, meta), (RowKey::Entity("T".into(), b"a".to_vec()), slot)]).is_err());
}

#[test]
fn ctx_random_is_stable_across_releases() {
    let ctx = Ctx::new(5, 0, 42);
    assert_eq!(
        [ctx.random(), ctx.random(), ctx.random()],
        [3294996511516835082, 1199235950618391259, 11155751481834583800]
    );
}

#[test]
fn apply_errors_explain_themselves() {
    assert_eq!(ApplyError::UnknownType("T".into()).to_string(), "state type T is not registered in this binary");
    assert_eq!(ApplyError::UnknownMethod(16).to_string(), "write method 0x10 is not known to this binary");
    assert_eq!(
        ApplyError::UnsupportedVersion { type_name: "T".into(), stored: 3, known: 2 }.to_string(),
        "T is stored at version 3 but this binary knows version 2"
    );
    assert_eq!(ApplyError::Decode.to_string(), "arguments could not be decoded");
    assert_eq!(ApplyError::Migration("x".into()).to_string(), "migration failed: x");
}
