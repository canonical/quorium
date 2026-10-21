use std::time::Duration;

use quorium_sim::scenario::{FailoverConfig, failover_samples, percentile};

#[test]
fn single_region_failover_p99_under_500ms() {
    let samples = failover_samples(&FailoverConfig::single_region(), 0..200);
    assert_eq!(samples.len(), 200);
    let p99 = percentile(&samples, 0.99);
    assert!(p99 < Duration::from_millis(500), "p99 {p99:?}");
}

#[test]
fn three_region_failover_p99_under_500ms() {
    let samples = failover_samples(&FailoverConfig::three_regions(), 0..200);
    assert_eq!(samples.len(), 200);
    let p99 = percentile(&samples, 0.99);
    assert!(p99 < Duration::from_millis(500), "p99 {p99:?}");
}

#[test]
fn percentile_picks_ranked_sample() {
    let v: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
    assert_eq!(percentile(&v, 0.5), Duration::from_millis(50));
    assert_eq!(percentile(&v, 0.99), Duration::from_millis(99));
    assert_eq!(percentile(&v, 1.0), Duration::from_millis(100));
}

#[test]
fn three_regions_stay_stable_without_faults() {
    let cfg = quorium_sim::scenario::FailoverConfig::three_regions();
    let nodes: Vec<(u64, &str)> = cfg.nodes.iter().map(|(id, dc)| (*id, dc.as_str())).collect();
    let mut config = quorium_sim::cluster::ClusterConfig::new(5, &nodes);
    config.inter_dc = cfg.inter_dc.clone();
    let mut cluster = quorium_sim::cluster::SimCluster::new(config);
    cluster.run_for(std::time::Duration::from_secs(3));
    let before = cluster.term();
    cluster.spawn_clients(3, 5, std::time::Duration::from_millis(20), std::time::Duration::from_secs(60), 1);
    cluster.run_for(std::time::Duration::from_secs(20));
    assert!(cluster.term() <= before + 1, "terms rose from {before} to {}", cluster.term());
}

#[test]
fn a_leadership_transfer_settles_quickly_across_regions() {
    let cfg = quorium_sim::scenario::FailoverConfig::three_regions();
    let nodes: Vec<(u64, &str)> = cfg.nodes.iter().map(|(id, dc)| (*id, dc.as_str())).collect();
    for seed in 0..20 {
        let mut config = quorium_sim::cluster::ClusterConfig::new(seed, &nodes);
        config.inter_dc = cfg.inter_dc.clone();
        let mut cluster = quorium_sim::cluster::SimCluster::new(config);
        cluster.run_for(std::time::Duration::from_secs(3));
        let leader = cluster.leader().expect("a leader");
        let before = cluster.term();
        cluster.spawn_clients(3, 5, std::time::Duration::from_millis(20), std::time::Duration::from_secs(60), seed);
        let target = nodes.iter().map(|(id, _)| *id).find(|id| *id != leader).unwrap();
        cluster.transfer_leader(target);
        cluster.run_for(std::time::Duration::from_secs(3));
        assert!(cluster.leader().is_some(), "seed {seed}: no leader after the transfer");
        assert!(cluster.term() <= before + 3, "seed {seed}: terms rose from {before} to {}", cluster.term());
    }
}
