use std::collections::BTreeMap;
use std::time::Duration;

use openraft::LogId;
use openraft::impls::Vote;
use openraft::raft::VoteRequest;
use openraft_rt_tokio::TokioRuntime;
use quorium_core::driver::{Ladder, position};
use quorium_core::ladder::{LadderConfig, LogPosition};
use quorium_core::types::CommittedLeaderId;

fn ladder(me: u64) -> Ladder<TokioRuntime> {
    let dcs: BTreeMap<u64, String> =
        [(1, "a"), (2, "a"), (3, "b")].iter().map(|(id, dc)| (*id, dc.to_string())).collect();
    Ladder::new(LadderConfig::default(), me, dcs, Duration::from_millis(5))
}

#[test]
fn position_maps_log_ids() {
    assert_eq!(position(None), LogPosition::default());
    let id = LogId::new(CommittedLeaderId::new(3), 7);
    assert_eq!(position(Some(&id)), LogPosition { term: 3, index: 7 });
}

#[test]
fn pings_mark_peers_live_and_carry_their_log() {
    let l = ladder(3);
    assert_eq!(l.rank(), 0);
    l.observe_ping(1, LogPosition { term: 1, index: 5 });
    assert_eq!(l.rank(), 1);
    l.observe_peer(2);
    assert_eq!(l.rank(), 2);
}

#[test]
fn ping_reports_the_local_log() {
    assert_eq!(ladder(1).ping(), LogPosition::default());
}

#[test]
fn committed_append_names_the_leader() {
    let l = ladder(3);
    l.observe_append(&Vote::new(2, 1));
    assert!(!l.describe().contains("leader: Some(1)"));
    l.observe_append(&Vote::new_committed(2, 1));
    assert!(l.describe().contains("leader: Some(1)"));
    assert!(!l.may_hand_off());
}

#[test]
fn deadline_is_pushed_by_a_pre_vote() {
    let l = ladder(3);
    l.observe_append(&Vote::new_committed(1, 1));
    let before = l.next_deadline().unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let req = VoteRequest::new(Vote::new(2, 2), Some(LogId::new(CommittedLeaderId::new(1), 0)));
    l.observe_pre_vote(&req);
    assert!(l.next_deadline().unwrap() > before);
    assert!(l.now() >= Duration::from_millis(20));
}

#[test]
fn quiescing_extends_the_deadline_by_the_liveness_window() {
    let l = ladder(3);
    l.observe_append(&Vote::new_committed(1, 1));
    let before = l.next_deadline().unwrap();
    l.set_quiesced(true);
    assert_eq!(l.next_deadline().unwrap(), before + LadderConfig::default().liveness_window);
}

#[test]
fn datacenters_can_be_learned_later() {
    let l = ladder(3);
    l.set_datacenter(4, "c");
    l.set_datacenter(3, "b");
    assert_eq!(l.rank(), 0);
}

#[test]
fn appends_wake_a_quiesced_follower() {
    let l = ladder(3);
    l.observe_append(&Vote::new_committed(1, 1));
    let normal = l.next_deadline().unwrap();
    l.set_quiesced(true);
    assert!(l.next_deadline().unwrap() > normal);
    l.observe_append(&Vote::new_committed(1, 1));
    assert!(l.next_deadline().unwrap() < normal + LadderConfig::default().liveness_window);
}

#[test]
fn shard_ladders_remember_datacenters() {
    let dcs: BTreeMap<u64, String> = [(1, "a".to_string())].into();
    let l = Ladder::<TokioRuntime>::for_shard(LadderConfig::default(), 1, dcs, Duration::from_millis(5), 9);
    assert_eq!(l.datacenter_of(1).as_deref(), Some("a"));
    l.set_datacenter(2, "b");
    assert_eq!(l.datacenter_of(2).as_deref(), Some("b"));
    assert!(l.describe().contains("rotation: Some(9)"));
}

#[test]
fn hand_offs_are_allowed_before_any_election() {
    assert!(ladder(1).may_hand_off());
}
