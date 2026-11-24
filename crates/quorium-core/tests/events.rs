use std::time::Duration;

use quorium_core::events::LeadershipStats;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

#[test]
fn leaderless_time_accumulates_until_a_leader_appears() {
    let mut s = LeadershipStats::default();
    assert!(!s.observe(None, 0, ms(10), ms(10)));
    assert!(!s.observe(None, 1, ms(20), ms(10)));
    assert!(s.observe(Some(1), 1, ms(30), ms(10)));
    assert!(!s.observe(Some(1), 1, ms(40), ms(10)));
    assert_eq!(s.leaderless, ms(30));
    assert_eq!(s.leader_changes, 1);
    assert_eq!(s.leader_since, Some(ms(30)));
}

#[test]
fn a_new_term_with_the_same_leader_is_a_change() {
    let mut s = LeadershipStats::default();
    s.observe(Some(1), 1, ms(10), ms(10));
    assert!(s.observe(Some(1), 2, ms(50), ms(40)));
    assert_eq!(s.leader_changes, 2);
    assert_eq!(s.leader_since, Some(ms(50)));
}

#[test]
fn losing_the_leader_is_a_change() {
    let mut s = LeadershipStats::default();
    s.observe(Some(1), 1, ms(10), ms(10));
    assert!(s.observe(None, 2, ms(20), ms(10)));
    assert_eq!(s.leader_since, None);
    assert_eq!(s.leader, None);
}

#[test]
fn stability_needs_an_old_leader_and_no_recent_election() {
    let mut s = LeadershipStats::default();
    assert!(!s.stable_for(ms(100), ms(1000)));
    s.observe(Some(1), 1, ms(0), ms(0));
    assert!(!s.stable_for(ms(100), ms(50)));
    assert!(s.stable_for(ms(100), ms(100)));
    s.election_started(ms(150));
    assert_eq!(s.elections_started, 1);
    assert!(!s.stable_for(ms(100), ms(200)));
    assert!(s.stable_for(ms(100), ms(250)));
}
