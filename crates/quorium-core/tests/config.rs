use std::time::Duration;

use quorium_core::config::raft_config;
use quorium_core::ladder::LadderConfig;

#[test]
fn ladder_owns_elections() {
    let c = raft_config(Duration::from_millis(50), &LadderConfig::default());
    assert!(!c.enable_elect);
    assert_eq!(c.enable_pre_vote, Some(true));
    assert!(c.enable_heartbeat);
}

#[test]
fn restarted_leader_never_restores_itself() {
    let c = raft_config(Duration::from_millis(50), &LadderConfig::default());
    assert_eq!(c.enable_leader_restore, Some(false));
}

#[test]
fn follower_lease_expires_before_rank_zero_stands() {
    let ladder = LadderConfig::default();
    let c = raft_config(Duration::from_millis(50), &ladder);
    assert!(Duration::from_millis(c.election_timeout_max) < ladder.base);
    assert!(c.election_timeout_min > c.heartbeat_interval);
}

#[test]
fn heartbeat_and_lease_follow_the_inputs() {
    let c = raft_config(Duration::from_millis(50), &LadderConfig::default());
    assert_eq!(c.cluster_name, "quorium");
    assert_eq!(c.heartbeat_interval, 50);
    assert_eq!(c.election_timeout_min, 100);
    assert_eq!(c.election_timeout_max, 101);
}

#[test]
fn lease_is_twice_an_unclamped_heartbeat() {
    let c = raft_config(Duration::from_millis(40), &LadderConfig::default());
    assert_eq!(c.heartbeat_interval, 40);
    assert_eq!(c.election_timeout_min, 80);
    assert_eq!(c.election_timeout_max, 81);
}

#[test]
fn heartbeat_is_clamped_so_the_lease_stays_below_the_base() {
    let ladder = LadderConfig::default();
    let c = raft_config(Duration::from_millis(400), &ladder);
    assert_eq!(c.heartbeat_interval, 50);
    assert_eq!(c.election_timeout_min, 100);
    assert!(Duration::from_millis(c.election_timeout_max) < ladder.base);
}

#[test]
fn the_lease_always_outlasts_one_heartbeat() {
    let tiny = LadderConfig { base: Duration::from_millis(3), ..LadderConfig::default() };
    let c = raft_config(Duration::from_millis(50), &tiny);
    assert_eq!(c.heartbeat_interval, 1);
    assert_eq!(c.election_timeout_min, 2);
    assert!(c.enable_heartbeat);
}

#[test]
fn a_longer_heartbeat_widens_the_ladder_instead_of_being_clamped() {
    let ladder = quorium_core::config::ladder_for(Duration::from_millis(100), &LadderConfig::default());
    assert_eq!(ladder.base, Duration::from_millis(300));
    let c = raft_config(Duration::from_millis(100), &ladder);
    assert_eq!(c.heartbeat_interval, 100);
    assert_eq!(c.election_timeout_min, 200);
    let unchanged = quorium_core::config::ladder_for(Duration::from_millis(50), &LadderConfig::default());
    assert_eq!(unchanged.base, LadderConfig::default().base);
}
