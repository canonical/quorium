use std::time::Duration;

use quorium_core::config::{TimingConfig, TimingError, Timings};
use quorium_core::ladder::LadderConfig;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn with_heartbeat(heartbeat: Duration) -> TimingConfig {
    TimingConfig { heartbeat, ..TimingConfig::default() }
}

fn defaults() -> Timings {
    TimingConfig::default().derive().unwrap()
}

#[test]
fn ladder_owns_elections() {
    let c = defaults().raft_config();
    assert!(!c.enable_elect);
    assert_eq!(c.enable_pre_vote, Some(true));
    assert!(c.enable_heartbeat);
}

#[test]
fn restarted_leader_never_restores_itself() {
    assert_eq!(defaults().raft_config().enable_leader_restore, Some(false));
}

#[test]
fn defaults_derive_the_documented_values() {
    let t = defaults();
    assert_eq!(t.heartbeat, ms(50));
    assert_eq!(t.leader_lease, ms(100));
    assert_eq!(t.rpc_timeout, ms(1000));
    assert_eq!(t.write_wait, ms(2000));
    assert_eq!(t.snapshot_timeout, ms(60_000));
    assert_eq!(t.ladder, LadderConfig::default());
    let c = t.raft_config();
    assert_eq!(c.cluster_name, "quorium");
    assert_eq!(c.heartbeat_interval, 50);
    assert_eq!(c.election_timeout_min, 99);
    assert_eq!(c.election_timeout_max, 100);
    assert_eq!(c.install_snapshot_timeout, 60_000);
}

#[test]
fn openraft_lease_is_the_reported_lease() {
    for h in [2, 7, 50, 333, 2000] {
        let t = with_heartbeat(ms(h)).derive().unwrap();
        assert_eq!(Duration::from_millis(t.raft_config().election_timeout_max), t.leader_lease);
    }
}

#[test]
fn follower_lease_expires_before_rank_zero_stands() {
    for h in 2..2_000 {
        let t = with_heartbeat(ms(h)).derive().unwrap();
        assert!(t.leader_lease + t.heartbeat <= t.ladder.base, "heartbeat {h} ms");
    }
}

#[test]
fn default_derivation_keeps_the_previous_formula() {
    for h in 2..2_000 {
        let t = with_heartbeat(ms(h)).derive().unwrap();
        assert_eq!(t.heartbeat, ms(h));
        assert_eq!(t.leader_lease, ms(2 * h));
        assert_eq!(t.ladder.base, LadderConfig::default().base.max(ms(3 * h)));
        assert_eq!(t.raft_config().heartbeat_interval, h);
    }
}

#[test]
fn a_longer_heartbeat_widens_the_ladder_when_none_is_given() {
    let t = with_heartbeat(ms(500)).derive().unwrap();
    assert_eq!(t.leader_lease, ms(1000));
    assert_eq!(t.ladder.base, ms(1500));
    assert_eq!(t.ladder.step, LadderConfig::default().step);
}

#[test]
fn an_explicit_lease_widens_the_derived_base() {
    let t = TimingConfig { leader_lease: Some(ms(3000)), ..with_heartbeat(ms(500)) }.derive().unwrap();
    assert_eq!(t.leader_lease, ms(3000));
    assert_eq!(t.ladder.base, ms(3500));
    assert_eq!(t.stall_tolerance(), ms(2500));
}

#[test]
fn an_explicit_ladder_is_used_as_given() {
    let ladder = LadderConfig { base: ms(4000), step: ms(200), ..LadderConfig::default() };
    let t = TimingConfig { ladder: Some(ladder.clone()), ..with_heartbeat(ms(500)) }.derive().unwrap();
    assert_eq!(t.ladder, ladder);
}

#[test]
fn a_heartbeat_below_the_minimum_is_refused() {
    assert_eq!(
        with_heartbeat(ms(1)).derive(),
        Err(TimingError::HeartbeatTooShort { heartbeat: ms(1), minimum: ms(2) })
    );
}

#[test]
fn a_lease_shorter_than_two_heartbeats_is_refused() {
    let config = TimingConfig { leader_lease: Some(ms(150)), ..with_heartbeat(ms(100)) };
    assert_eq!(config.derive(), Err(TimingError::LeaseTooShort { lease: ms(150), minimum: ms(200) }));
}

#[test]
fn an_explicit_base_inside_the_lease_is_refused_instead_of_clamped() {
    let config = TimingConfig { ladder: Some(LadderConfig::default()), ..with_heartbeat(ms(500)) };
    assert_eq!(config.derive(), Err(TimingError::BaseTooShort { base: ms(150), minimum: ms(1500) }));
}

#[test]
fn a_campaign_timeout_above_its_maximum_is_refused() {
    let ladder = LadderConfig { campaign_timeout: ms(400), max_campaign: ms(300), ..LadderConfig::default() };
    let config = TimingConfig { ladder: Some(ladder), ..TimingConfig::default() };
    assert_eq!(
        config.derive(),
        Err(TimingError::CampaignTimeoutAboveMax { campaign_timeout: ms(400), max_campaign: ms(300) })
    );
}

#[test]
fn zero_waits_are_refused() {
    let rpc = TimingConfig { rpc_timeout: Duration::ZERO, ..TimingConfig::default() };
    assert_eq!(rpc.derive(), Err(TimingError::ZeroRpcTimeout));
    let snapshot = TimingConfig { snapshot_timeout: Duration::from_micros(10), ..TimingConfig::default() };
    assert_eq!(snapshot.derive(), Err(TimingError::ZeroSnapshotTimeout));
    let write = TimingConfig { write_wait: Duration::ZERO, ..TimingConfig::default() };
    assert_eq!(write.derive(), Err(TimingError::ZeroWriteWait));
}

#[test]
fn errors_explain_the_conflict() {
    let config = TimingConfig { ladder: Some(LadderConfig::default()), ..with_heartbeat(ms(500)) };
    let text = config.derive().unwrap_err().to_string();
    assert!(text.contains("1.5s"), "{text}");
}

#[test]
fn consensus_deadline_has_a_floor() {
    let t = TimingConfig { rpc_timeout: ms(3000), ..TimingConfig::default() }.derive().unwrap();
    assert_eq!(t.consensus_deadline(ms(50)), ms(3000));
    assert_eq!(t.consensus_deadline(ms(5000)), ms(5000));
}

#[test]
fn failover_estimate_adds_up_its_parts() {
    let t = defaults();
    let idle = t.failover_estimate(3, Duration::ZERO);
    assert_eq!(idle.detection, ms(150));
    assert_eq!(idle.ranking, ms(50));
    assert_eq!(idle.campaign, ms(300));
    assert_eq!(idle.establish, Duration::ZERO);
    assert_eq!(idle.total, ms(500));
    let slow = t.failover_estimate(5, ms(100));
    assert_eq!(slow.ranking, ms(300));
    assert_eq!(slow.campaign, ms(600));
    assert_eq!(slow.establish, ms(100));
    assert_eq!(slow.total, ms(150 + 300 + 600 + 100));
}
