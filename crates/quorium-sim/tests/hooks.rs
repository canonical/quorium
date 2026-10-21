use std::time::Duration;

use quorium_sim::cluster::{ClusterConfig, SimCluster};

fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

fn three() -> ClusterConfig {
    ClusterConfig::new(11, &[(1, "a"), (2, "b"), (3, "c")])
}

#[test]
fn elections_happen_only_through_the_external_trigger() {
    let mut cfg = three();
    cfg.ladder_enabled = false;
    let mut c = SimCluster::new(cfg);
    c.run_for(ms(1_000));
    let bootstrap = c.leader().expect("initialize elects a bootstrap leader");
    c.crash(bootstrap);
    c.run_for(ms(5_000));
    assert!(c.leaders().is_empty());
    let mut c = SimCluster::new(three());
    c.run_for(ms(1_000));
    let bootstrap = c.leader().unwrap();
    c.crash(bootstrap);
    c.run_for(ms(5_000));
    assert_eq!(c.leaders().len(), 1);
}

#[test]
fn transfer_leader_hands_off_to_the_target() {
    let mut c = SimCluster::new(three());
    c.run_for(ms(1_000));
    let from = c.leader().unwrap();
    let to = if from == 3 { 2 } else { 3 };
    c.transfer_leader(to);
    c.run_for(ms(1_000));
    assert_eq!(c.leader(), Some(to));
    c.write("t", 1).unwrap();
    assert!(c.violations().is_empty(), "{:?}", c.violations());
}

#[test]
fn quiesced_cluster_sends_no_appends_and_stays_stable() {
    let mut c = SimCluster::new(three());
    c.run_for(ms(1_000));
    let term = c.term();
    c.quiesce(true);
    c.run_for(ms(200));
    let before = c.rpc_counts().append;
    c.run_for(ms(5_000));
    assert_eq!(c.rpc_counts().append, before);
    assert_eq!(c.term(), term);
    assert_eq!(c.leaders().len(), 1);
}

#[test]
fn quiesced_cluster_still_fails_over_through_host_liveness() {
    let mut c = SimCluster::new(three());
    c.run_for(ms(1_000));
    c.quiesce(true);
    c.run_for(ms(500));
    let old = c.leader().unwrap();
    c.crash(old);
    assert!(c.run_until_leader_other_than(old, ms(5_000)).is_some());
}

#[test]
fn pre_vote_keeps_an_isolated_node_from_inflating_the_term() {
    let mut c = SimCluster::new(three());
    c.run_for(ms(1_000));
    let leader = c.leader().unwrap();
    let term = c.term();
    let victim = if leader == 1 { 2 } else { 1 };
    c.isolate(victim);
    c.run_for(ms(5_000));
    c.heal();
    c.run_for(ms(1_000));
    assert!(c.rpc_counts().pre_vote > 0);
    assert_eq!(c.term(), term);
    assert_eq!(c.leader(), Some(leader));
}

#[test]
fn leader_overlaps_replication_with_its_own_fsync() {
    let mut cfg = three();
    cfg.fsync = ms(1);
    let mut c = SimCluster::new(cfg.clone());
    c.run_for(ms(1_000));
    let leader = c.leader().unwrap();
    let mut slow = cfg;
    slow.fsync_overrides = vec![(leader, ms(80))];
    let mut c = SimCluster::new(slow);
    c.run_for(ms(1_000));
    assert_eq!(c.leader(), Some(leader));
    let start = c.now();
    c.write("o", 1).unwrap();
    let latency = c.now() - start;
    assert!(latency < ms(40), "commit waited for the leader's fsync: {latency:?}");
}
