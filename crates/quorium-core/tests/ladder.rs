use std::time::Duration;

use quorium_core::ladder::{Action, Controller, LadderConfig, LogPosition, Member};

fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

fn cfg() -> LadderConfig {
    LadderConfig {
        base: ms(150),
        step: ms(50),
        fast_retry: ms(20),
        campaign_timeout: ms(60),
        max_campaign: ms(200),
        flat_retry: ms(100),
        liveness_window: ms(1000),
        handoff_suspend: ms(30_000),
    }
}

fn members() -> Vec<Member> {
    vec![
        Member::new(1, "us-east"),
        Member::new(2, "us-east"),
        Member::new(3, "eu-west"),
        Member::new(4, "eu-west"),
        Member::new(5, "ap-south"),
    ]
}

fn pos(term: u64, index: u64) -> LogPosition {
    LogPosition { term, index }
}

fn controller(me: u64) -> Controller {
    let mut c = Controller::new(cfg(), me, members());
    for m in members() {
        c.on_peer_seen(m.id, ms(0));
        c.on_peer_log(m.id, pos(1, 10));
    }
    c.on_local_log(pos(1, 10));
    c
}

#[test]
fn log_position_orders_by_term_then_index() {
    assert!(pos(2, 1) > pos(1, 100));
    assert!(pos(1, 5) > pos(1, 4));
    assert_eq!(pos(3, 3), pos(3, 3));
}

#[test]
fn no_election_while_leader_is_heard() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.poll(ms(100)), None);
    c.on_leader_contact(1, ms(100));
    assert_eq!(c.poll(ms(240)), None);
}

#[test]
fn same_datacenter_as_leader_ranks_first() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 0);
    let mut c = controller(5);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 3);
}

#[test]
fn better_log_ranks_ahead_of_datacenter() {
    let mut c = controller(2);
    c.on_peer_log(5, pos(1, 11));
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 1);
}

#[test]
fn ties_break_by_node_id() {
    let mut c = controller(4);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 2);
    let mut c = controller(3);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 1);
}

#[test]
fn unreachable_peers_do_not_hold_a_rank() {
    let mut c = Controller::new(cfg(), 5, members());
    c.on_local_log(pos(1, 10));
    for id in [1, 2, 3, 4] {
        c.on_peer_log(id, pos(1, 10));
    }
    c.on_peer_seen(3, ms(5_000));
    c.on_leader_contact(1, ms(5_000));
    assert_eq!(c.rank(), 1);
}

#[test]
fn rank_zero_stands_after_base_timeout() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.poll(ms(149)), None);
    assert_eq!(c.poll(ms(150)), Some(Action::Elect { pre_vote: true }));
}

#[test]
fn higher_rank_waits_one_step_per_rank() {
    let mut c = controller(5);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.next_deadline(), Some(ms(150 + 3 * 50)));
    assert_eq!(c.poll(ms(299)), None);
    assert_eq!(c.poll(ms(300)), Some(Action::Elect { pre_vote: true }));
}

#[test]
fn poll_fires_once_per_deadline() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert!(c.poll(ms(150)).is_some());
    assert_eq!(c.poll(ms(151)), None);
}

#[test]
fn leader_itself_never_elects() {
    let mut c = controller(1);
    c.on_became_leader(ms(0));
    assert_eq!(c.poll(ms(10_000)), None);
    assert_eq!(c.next_deadline(), None);
}

#[test]
fn defers_to_candidate_with_at_least_as_good_a_log() {
    let mut c = controller(3);
    c.on_leader_contact(1, ms(0));
    let deadline = c.next_deadline().unwrap();
    assert_eq!(c.on_pre_vote(2, pos(1, 10), ms(160)), None);
    assert!(c.next_deadline().unwrap() > deadline);
    assert_eq!(c.poll(deadline), None);
}

#[test]
fn better_log_stands_at_once_against_worse_candidate() {
    let mut c = controller(3);
    c.on_local_log(pos(1, 12));
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.on_pre_vote(5, pos(1, 10), ms(160)), Some(Action::Elect { pre_vote: true }));
}

#[test]
fn pre_vote_from_worse_log_is_ignored_while_leader_is_live() {
    let mut c = controller(3);
    c.on_local_log(pos(1, 12));
    c.on_leader_contact(1, ms(100));
    assert_eq!(c.on_pre_vote(5, pos(1, 10), ms(120)), None);
}

#[test]
fn failed_election_retries_fast_then_flat() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert!(c.poll(ms(150)).is_some());
    c.on_election_failed(ms(160));
    assert_eq!(c.next_deadline(), Some(ms(180)));
    assert!(c.poll(ms(180)).is_some());
    c.on_election_failed(ms(190));
    assert_eq!(c.next_deadline(), Some(ms(290)));
    assert!(c.poll(ms(290)).is_some());
    c.on_election_failed(ms(300));
    assert_eq!(c.next_deadline(), Some(ms(400)));
}

#[test]
fn flat_retry_keeps_rank_spacing() {
    let mut c = controller(5);
    c.on_leader_contact(1, ms(0));
    assert!(c.poll(ms(300)).is_some());
    c.on_election_failed(ms(300));
    assert!(c.poll(ms(320 + 150)).is_some());
    c.on_election_failed(ms(500));
    assert_eq!(c.next_deadline(), Some(ms(500 + 100 + 3 * 50)));
}

#[test]
fn new_leader_contact_resets_retry_ladder() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert!(c.poll(ms(150)).is_some());
    c.on_election_failed(ms(160));
    c.on_leader_contact(3, ms(170));
    assert_eq!(c.leader(), Some(3));
    assert_eq!(c.next_deadline(), Some(ms(170 + 150 + 2 * 50)));
}

#[test]
fn handoffs_suspended_after_an_election() {
    let mut c = controller(1);
    assert!(c.may_hand_off(ms(0)));
    c.on_became_leader(ms(1_000));
    assert!(!c.may_hand_off(ms(30_999)));
    assert!(c.may_hand_off(ms(31_000)));
    c.on_leader_contact(2, ms(40_000));
    assert!(!c.may_hand_off(ms(50_000)));
}

#[test]
fn quiesced_leader_counts_as_live_while_host_pings() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    c.set_quiesced(true);
    c.on_peer_seen(1, ms(140));
    assert_eq!(c.poll(ms(200)), None);
    c.on_peer_seen(1, ms(400));
    assert_eq!(c.poll(ms(1_300)), None);
    assert_eq!(c.poll(ms(1_400 + 150)), Some(Action::Elect { pre_vote: true }));
}

#[test]
fn membership_change_drops_removed_peers() {
    let mut c = controller(5);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 3);
    c.set_members(vec![Member::new(1, "us-east"), Member::new(3, "eu-west"), Member::new(5, "ap-south")]);
    assert_eq!(c.rank(), 1);
}

#[test]
fn unanswered_campaign_counts_as_failure_after_campaign_timeout() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert!(c.poll(ms(150)).is_some());
    assert_eq!(c.next_deadline(), Some(ms(210)));
    assert_eq!(c.poll(ms(209)), None);
    assert_eq!(c.poll(ms(210)), None);
    assert_eq!(c.next_deadline(), Some(ms(230)));
    assert_eq!(c.poll(ms(230)), Some(Action::Elect { pre_vote: true }));
}

#[test]
fn ping_marks_peer_live_and_records_its_log() {
    let mut c = Controller::new(cfg(), 5, members());
    c.on_local_log(pos(1, 10));
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 0);
    c.on_ping(3, pos(1, 10), ms(10));
    assert_eq!(c.rank(), 1);
    c.on_ping(4, pos(1, 9), ms(10));
    assert_eq!(c.rank(), 1);
}

#[test]
fn defers_to_equal_candidate_even_while_leader_still_looks_live() {
    let mut c = controller(3);
    c.on_leader_contact(1, ms(100));
    assert_eq!(c.next_deadline(), Some(ms(100 + 150 + 50)));
    assert_eq!(c.on_pre_vote(2, pos(1, 10), ms(120)), None);
    assert_eq!(c.next_deadline(), Some(ms(120 + 150 + 50)));
    assert_eq!(c.poll(ms(300)), None);
}

#[test]
fn ongoing_campaign_extends_its_timeout() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert!(c.poll(ms(150)).is_some());
    c.on_campaigning(ms(200));
    assert_eq!(c.next_deadline(), Some(ms(260)));
    assert_eq!(c.poll(ms(230)), None);
    c.on_campaigning(ms(240));
    assert_eq!(c.next_deadline(), Some(ms(300)));
}

#[test]
fn campaigning_signal_is_ignored_outside_a_campaign() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    c.on_campaigning(ms(100));
    assert_eq!(c.next_deadline(), Some(ms(150)));
}

#[test]
fn stepped_down_leader_rejoins_the_ladder() {
    let mut c = controller(1);
    c.on_became_leader(ms(0));
    assert_eq!(c.next_deadline(), None);
    c.on_stepped_down(ms(1_000));
    assert_eq!(c.leader(), None);
    assert_eq!(c.next_deadline(), Some(ms(1_150)));
    assert_eq!(c.poll(ms(1_150)), Some(Action::Elect { pre_vote: true }));
}

#[test]
fn step_down_is_ignored_when_not_leading() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    c.on_stepped_down(ms(50));
    assert_eq!(c.leader(), Some(1));
    assert_eq!(c.next_deadline(), Some(ms(150)));
}

#[test]
fn campaign_extension_is_capped() {
    let mut c = controller(2);
    c.on_leader_contact(1, ms(0));
    assert!(c.poll(ms(150)).is_some());
    c.on_campaigning(ms(300));
    assert_eq!(c.next_deadline(), Some(ms(350)));
    c.on_campaigning(ms(340));
    assert_eq!(c.next_deadline(), Some(ms(350)));
    assert_eq!(c.poll(ms(350)), None);
    assert_eq!(c.next_deadline(), Some(ms(370)));
}

#[test]
fn exposes_identity_and_local_log() {
    let c = controller(4);
    assert_eq!(c.me(), 4);
    assert_eq!(c.local_log(), pos(1, 10));
}

#[test]
fn better_log_does_not_defer_to_worse_candidate_while_leader_is_live() {
    let mut c = controller(3);
    c.on_local_log(pos(1, 12));
    c.on_leader_contact(1, ms(100));
    let deadline = c.next_deadline();
    assert_eq!(c.on_pre_vote(5, pos(1, 10), ms(120)), None);
    assert_eq!(c.next_deadline(), deadline);
}

#[test]
fn readded_member_starts_without_liveness() {
    let mut c = controller(5);
    c.on_leader_contact(1, ms(0));
    c.set_members(vec![Member::new(1, "us-east"), Member::new(5, "ap-south")]);
    assert_eq!(c.rank(), 0);
    c.set_members(members());
    assert_eq!(c.rank(), 0);
}

#[test]
fn liveness_expires_as_time_advances_through_poll() {
    let mut c = controller(5);
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 3);
    assert!(c.poll(ms(1_001)).is_some());
    assert_eq!(c.rank(), 0);
}

#[test]
fn rotation_spreads_rank_zero_across_shards_among_equal_candidates() {
    let mut firsts = std::collections::BTreeSet::new();
    for shard in 0..32u64 {
        let mut winner = None;
        for me in [1u64, 2, 3, 4, 5] {
            let mut c = Controller::new(cfg(), me, members()).with_rotation(shard);
            for m in members() {
                c.on_peer_seen(m.id, ms(0));
                c.on_peer_log(m.id, pos(1, 10));
            }
            c.on_local_log(pos(1, 10));
            if c.rank() == 0 {
                assert!(winner.is_none(), "two rank-zero nodes on shard {shard}");
                winner = Some(me);
            }
        }
        firsts.insert(winner.expect("one rank-zero node"));
    }
    assert_eq!(firsts.len(), 5, "{firsts:?}");
}

#[test]
fn rotation_never_outranks_a_better_log_or_the_leaders_datacenter() {
    let mut c = Controller::new(cfg(), 2, members()).with_rotation(7);
    for m in members() {
        c.on_peer_seen(m.id, ms(0));
        c.on_peer_log(m.id, pos(1, 10));
    }
    c.on_local_log(pos(1, 10));
    c.on_leader_contact(1, ms(0));
    assert_eq!(c.rank(), 0);
    c.on_peer_log(5, pos(1, 11));
    assert_eq!(c.rank(), 1);
}

#[test]
fn shard_of_partitions_the_hash_space_into_ranges() {
    use quorium_core::shard::{shard_of, shard_range};
    assert_eq!(shard_of(b"anything", 1), 0);
    let mut counts = [0u32; 8];
    for i in 0..8000u32 {
        counts[shard_of(&i.to_le_bytes(), 8) as usize] += 1;
    }
    assert!(counts.iter().all(|c| *c > 700), "{counts:?}");
    let (lo, hi) = shard_range(3, 8);
    assert_eq!((lo, hi), (3u64 << 61, (4u64 << 61) - 1));
    assert_eq!(shard_range(7, 8).1, u64::MAX);
}

#[test]
fn keys_differing_only_in_their_last_byte_still_spread() {
    use quorium_core::shard::entity_shard;
    let mut used = std::collections::BTreeSet::new();
    for k in 0..64u8 {
        used.insert(entity_shard("shards.Counter", &[k], 16));
    }
    assert!(used.len() >= 14, "{used:?}");
}

#[test]
fn locality_prefix_groups_a_regions_entities_together() {
    use quorium_core::shard::entity_shard_with_locality;
    let eu: std::collections::BTreeSet<u64> =
        (0..200u32).map(|k| entity_shard_with_locality("T", &k.to_le_bytes(), Some("eu"), 64)).collect();
    let us: std::collections::BTreeSet<u64> =
        (0..200u32).map(|k| entity_shard_with_locality("T", &k.to_le_bytes(), Some("us"), 64)).collect();
    assert_eq!(eu.len(), 1, "{eu:?}");
    assert_eq!(us.len(), 1, "{us:?}");
    assert_ne!(eu, us);
    assert_eq!(entity_shard_with_locality("T", b"k", None, 64), quorium_core::shard::entity_shard("T", b"k", 64));
}

#[test]
fn campaigns_last_long_enough_for_a_quorum_round_trip() {
    let mut fast = Controller::new(cfg(), 1, members());
    assert_eq!(fast.poll(ms(10_000)), Some(Action::Elect { pre_vote: true }));
    assert_eq!(fast.campaign_ends(), Some(ms(10_060)));
    fast.on_campaigning(ms(10_500));
    assert_eq!(fast.campaign_ends(), Some(ms(10_200)));
    let mut slow = Controller::new(cfg(), 1, members());
    for (peer, rtt) in [(2, 40), (3, 90), (4, 100), (5, 120)] {
        slow.on_round_trip(peer, ms(rtt));
    }
    assert_eq!(slow.quorum_round_trip(), ms(90));
    assert_eq!(slow.poll(ms(10_000)), Some(Action::Elect { pre_vote: true }));
    assert_eq!(slow.campaign_ends(), Some(ms(10_270)));
    slow.on_campaigning(ms(10_500));
    assert_eq!(slow.campaign_ends(), Some(ms(10_540)));
}

#[test]
fn round_trips_are_smoothed() {
    let mut c = Controller::new(cfg(), 1, members());
    c.on_round_trip(2, ms(100));
    c.on_round_trip(2, ms(200));
    let smoothed = c.round_trip(2).unwrap();
    assert!(smoothed > ms(100) && smoothed < ms(200), "{smoothed:?}");
    assert_eq!(c.round_trip(3), None);
}

#[test]
fn late_heartbeats_stretch_the_election_deadline() {
    let mut c = Controller::new(cfg(), 3, members());
    let mut t = 0;
    for _ in 0..10 {
        t += 50;
        c.on_leader_contact(1, ms(t));
    }
    let tight = c.next_deadline().unwrap() - ms(t);
    for _ in 0..5 {
        t += 400;
        c.on_leader_contact(1, ms(t));
    }
    let stretched = c.next_deadline().unwrap() - ms(t);
    assert!(stretched >= tight + ms(600), "deadline only {stretched:?} after contact");
    assert!(stretched <= ms(150 * 8 + 50 * 4 + 1), "deadline {stretched:?} beyond the cap");
}

#[test]
fn slow_disks_lengthen_campaigns_and_rank_spacing() {
    let mut c = Controller::new(cfg(), 1, members());
    for (peer, rtt) in [(2, 40), (3, 90), (4, 100), (5, 120)] {
        c.on_round_trip(peer, ms(rtt));
    }
    c.on_sync_latency(ms(110));
    assert_eq!(c.consensus_round_trip(), ms(200));
    assert_eq!(c.poll(ms(10_000)), Some(Action::Elect { pre_vote: true }));
    assert_eq!(c.campaign_ends(), Some(ms(10_600)));
    let mut ranked = Controller::new(cfg(), 5, members());
    ranked.on_leader_contact(1, ms(0));
    let unscaled = ranked.next_deadline().unwrap();
    ranked.on_sync_latency(ms(300));
    let scaled = ranked.next_deadline().unwrap();
    if ranked.rank() > 0 {
        assert!(scaled > unscaled, "{scaled:?} vs {unscaled:?}");
    }
}

#[test]
fn leadership_moves_only_to_caught_up_targets_of_idle_shards() {
    use quorium_core::ladder::transfer_ready;
    assert!(transfer_ready(ms(3_000), Some(10), Some(10)));
    assert!(!transfer_ready(ms(500), Some(10), Some(10)));
    assert!(!transfer_ready(ms(3_000), Some(9), Some(10)));
    assert!(!transfer_ready(ms(3_000), None, Some(10)));
    assert!(transfer_ready(ms(3_000), None, None));
}

#[test]
fn the_slowest_peer_is_reported() {
    let mut c = Controller::new(cfg(), 1, members());
    assert_eq!(c.slowest_round_trip(), None);
    c.on_round_trip(2, ms(10));
    c.on_round_trip(3, ms(80));
    assert_eq!(c.slowest_round_trip(), Some((3, ms(80))));
}

#[test]
fn the_election_base_is_exposed_with_its_stretch() {
    let mut c = Controller::new(cfg(), 3, members());
    assert_eq!(c.election_base(), ms(150));
    c.on_leader_contact(1, ms(0));
    c.on_leader_contact(1, ms(100));
    assert_eq!(c.election_base(), ms(200));
}
