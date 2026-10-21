use linkme::distributed_slice;
use quorium_core::entity::{ApplyError, Ctx, EntityCall, REGISTRY, Registration};
use quorium_core::escrow::ESCROW_METHOD;
use quorium_core::kv::{Command, EscrowAction, EscrowKey, EscrowView, KvOp, KvState, Reply};

fn shop_apply(state: Option<&[u8]>, method: u32, args: &[u8], _ctx: &Ctx) -> Result<(Vec<u8>, Vec<u8>), ApplyError> {
    let mut view: EscrowView = state.map(|s| postcard::from_bytes(s).unwrap()).unwrap_or_default();
    if method != ESCROW_METHOD {
        return Err(ApplyError::UnknownMethod(method));
    }
    let (field, node, action): (String, u64, EscrowAction) =
        postcard::from_bytes(args).map_err(|_| ApplyError::Decode)?;
    let outcome: Result<(u64, u64), String> = if field == "stock" {
        view.apply(node, action).map(|granted| (granted, view.pool))
    } else {
        Err(format!("no escrow field named {field}"))
    };
    Ok((postcard::to_allocvec(&view).unwrap(), postcard::to_allocvec(&outcome).unwrap()))
}

#[distributed_slice(REGISTRY)]
static SHOP: Registration =
    Registration { name: "test.Shop", version: 1, methods: &[ESCROW_METHOD], apply: shop_apply, migrate: None };

fn escrow(s: &mut KvState, client: u64, seq: u64, holder: u64, action: EscrowAction) -> Result<(u64, u64), String> {
    let call = EntityCall {
        type_name: "test.Shop".into(),
        version: 1,
        key: b"s1".to_vec(),
        method: ESCROW_METHOD,
        args: postcard::to_allocvec(&("stock".to_string(), holder, action)).unwrap(),
        now_ms: 0,
        locality: Some("eu".into()),
    };
    match s.apply(&Command::new(client, seq, KvOp::Apply(call))) {
        Reply::Applied(bytes) => postcard::from_bytes(&bytes).unwrap(),
        Reply::ApplyFailed(e) => Err(e),
        other => panic!("{other:?}"),
    }
}

fn view(s: &KvState) -> EscrowView {
    postcard::from_bytes(s.entity("test.Shop", b"s1").unwrap().1).unwrap()
}

fn key() -> EscrowKey {
    EscrowKey::Field {
        type_name: "test.Shop".into(),
        key: b"s1".to_vec(),
        locality: Some("eu".into()),
        field: "stock".into(),
    }
}

#[test]
fn field_escrows_settle_by_running_total_and_can_be_reclaimed() {
    let mut s = KvState::default();
    escrow(&mut s, 9, 1, 9, EscrowAction::Refill { units: 100 }).unwrap();
    let (g, _) = escrow(&mut s, 1, 1, 1, EscrowAction::Grant { want: 20 }).unwrap();
    escrow(&mut s, 1, 2, 1, EscrowAction::SettleThrough { through: 3, release: 0 }).unwrap();
    assert_eq!(s.escrow_holder(&key(), 1).unwrap().settled_through, 3);
    escrow(&mut s, 50, 1, 1, EscrowAction::Reclaim { through: 5 }).unwrap();
    let v = view(&s);
    assert_eq!((v.sold, v.pool, v.outstanding.get(&1)), (5, 95, None));
    assert!(g >= 5);
    assert!(s.escrow_holder(&key(), 1).unwrap().reclaimed);
    assert!(escrow(&mut s, 1, 3, 1, EscrowAction::SettleThrough { through: 5, release: 0 }).is_err());
    assert_eq!(escrow(&mut s, 1, 4, 1, EscrowAction::Grant { want: 5 }).unwrap().0, 0);
}

#[test]
fn a_failed_field_escrow_call_leaves_no_holder_record() {
    let mut s = KvState::default();
    escrow(&mut s, 9, 1, 9, EscrowAction::Refill { units: 10 }).unwrap();
    escrow(&mut s, 1, 1, 1, EscrowAction::Grant { want: 2 }).unwrap();
    assert!(escrow(&mut s, 1, 2, 1, EscrowAction::SettleThrough { through: 50, release: 0 }).is_err());
    assert_eq!(s.escrow_holder(&key(), 1).unwrap().settled_through, 0);
}
