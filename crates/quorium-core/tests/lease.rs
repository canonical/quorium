use quorium_core::kv::{Command, KvOp, KvState, LeaseAction, LeaseOp, Reply};

fn lease(state: &mut KvState, seq: u64, holder: u64, action: LeaseAction, now_ms: u64) -> Reply {
    let op = KvOp::Lease(LeaseOp { name: "billing".into(), holder, ttl_ms: 1_000, now_ms, action });
    state.apply(&Command::new(holder, seq, op))
}

#[test]
fn first_acquirer_holds_with_token_one() {
    let mut s = KvState::default();
    assert_eq!(
        lease(&mut s, 1, 7, LeaseAction::Acquire, 0),
        Reply::Lease { held: true, token: 1, holder: 7, expires_ms: 1_000 }
    );
}

#[test]
fn second_acquirer_is_refused_while_the_lease_is_live() {
    let mut s = KvState::default();
    lease(&mut s, 1, 7, LeaseAction::Acquire, 0);
    assert_eq!(
        lease(&mut s, 1, 8, LeaseAction::Acquire, 500),
        Reply::Lease { held: false, token: 1, holder: 7, expires_ms: 1_000 }
    );
}

#[test]
fn renewal_extends_and_keeps_the_token() {
    let mut s = KvState::default();
    lease(&mut s, 1, 7, LeaseAction::Acquire, 0);
    assert_eq!(
        lease(&mut s, 2, 7, LeaseAction::Renew { token: 1 }, 600),
        Reply::Lease { held: true, token: 1, holder: 7, expires_ms: 1_600 }
    );
    assert_eq!(
        lease(&mut s, 2, 7, LeaseAction::Acquire, 900),
        Reply::Lease { held: true, token: 1, holder: 7, expires_ms: 1_600 }
    );
}

#[test]
fn expired_lease_moves_to_a_new_holder_with_a_higher_token() {
    let mut s = KvState::default();
    lease(&mut s, 1, 7, LeaseAction::Acquire, 0);
    assert_eq!(
        lease(&mut s, 1, 8, LeaseAction::Acquire, 1_000),
        Reply::Lease { held: true, token: 2, holder: 8, expires_ms: 2_000 }
    );
    assert_eq!(
        lease(&mut s, 2, 7, LeaseAction::Renew { token: 1 }, 1_100),
        Reply::Lease { held: false, token: 2, holder: 8, expires_ms: 2_000 }
    );
}

#[test]
fn release_frees_the_lease_but_tokens_keep_rising() {
    let mut s = KvState::default();
    lease(&mut s, 1, 7, LeaseAction::Acquire, 0);
    assert_eq!(
        lease(&mut s, 2, 7, LeaseAction::Release { token: 1 }, 100),
        Reply::Lease { held: false, token: 1, holder: 7, expires_ms: 0 }
    );
    assert_eq!(
        lease(&mut s, 3, 7, LeaseAction::Acquire, 200),
        Reply::Lease { held: true, token: 2, holder: 7, expires_ms: 1_200 }
    );
}

#[test]
fn stale_release_has_no_effect() {
    let mut s = KvState::default();
    lease(&mut s, 1, 7, LeaseAction::Acquire, 0);
    lease(&mut s, 1, 8, LeaseAction::Acquire, 1_500);
    assert_eq!(
        lease(&mut s, 2, 7, LeaseAction::Release { token: 1 }, 1_600),
        Reply::Lease { held: false, token: 2, holder: 8, expires_ms: 2_500 }
    );
}
