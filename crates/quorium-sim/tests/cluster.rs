use std::time::Duration;

use quorium_sim::cluster::{ClusterConfig, SimCluster};

fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    let mut c = SimCluster::new(ClusterConfig::new(1, &[(1, "a"), (2, "b"), (3, "c")]));
    c.run_for(ms(2_000));
    let leaders = c.leaders();
    assert_eq!(leaders.len(), 1, "{leaders:?}");
    assert!(c.violations().is_empty(), "{:?}", c.violations());
}

#[test]
fn writes_replicate_to_every_node() {
    let mut c = SimCluster::new(ClusterConfig::new(2, &[(1, "a"), (2, "b"), (3, "c")]));
    c.run_for(ms(1_000));
    for i in 0..20u64 {
        c.write(&format!("k{i}"), i).expect("write commits");
    }
    c.run_for(ms(500));
    let hashes = c.state_hashes();
    assert_eq!(hashes.len(), 3);
    assert!(hashes.windows(2).all(|w| w[0] == w[1]), "{hashes:?}");
    assert_eq!(c.read("k7").unwrap(), Some(7));
    assert!(c.violations().is_empty(), "{:?}", c.violations());
}

#[test]
fn leader_crash_fails_over_and_writes_resume() {
    let mut c = SimCluster::new(ClusterConfig::new(3, &[(1, "a"), (2, "b"), (3, "c")]));
    c.run_for(ms(1_000));
    c.write("x", 1).unwrap();
    let old = c.leader().unwrap();
    c.crash(old);
    let started = c.now();
    c.run_until_leader_other_than(old, ms(5_000)).expect("new leader elected");
    assert!(c.now() - started < ms(1_000), "failover took {:?}", c.now() - started);
    c.write("x", 2).unwrap();
    c.restart(old);
    c.run_for(ms(1_000));
    assert_eq!(c.read("x").unwrap(), Some(2));
    assert!(c.violations().is_empty(), "{:?}", c.violations());
}

#[test]
fn ladder_hands_leadership_to_same_datacenter_peer() {
    let nodes = [(1, "a"), (2, "a"), (3, "b"), (4, "b"), (5, "c")];
    for seed in 0..5 {
        let mut c = SimCluster::new(ClusterConfig::new(seed, &nodes));
        c.run_for(ms(1_000));
        let first = c.leader().unwrap();
        c.crash(first);
        let next = c.run_until_leader_other_than(first, ms(5_000)).unwrap();
        assert_eq!(c.datacenter(next), c.datacenter(first), "seed {seed}: {first} -> {next}");
    }
}

#[test]
fn minority_partition_cannot_commit() {
    let mut c = SimCluster::new(ClusterConfig::new(4, &[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]));
    c.run_for(ms(1_000));
    let leader = c.leader().unwrap();
    let others: Vec<u64> = [1, 2, 3, 4, 5].into_iter().filter(|n| *n != leader).collect();
    c.partition(&[leader, others[0]], &others[1..]);
    assert!(c.write_via(leader, "p", 1).is_err());
    let new_leader = c.run_until_leader_other_than(leader, ms(5_000)).unwrap();
    assert!(others[1..].contains(&new_leader));
    c.write("p", 2).unwrap();
    c.heal();
    c.run_for(ms(2_000));
    assert_eq!(c.read("p").unwrap(), Some(2));
    assert!(c.violations().is_empty(), "{:?}", c.violations());
}

#[test]
fn whole_cluster_power_loss_recovers_acknowledged_writes() {
    let mut c = SimCluster::new(ClusterConfig::new(5, &[(1, "a"), (2, "b"), (3, "c")]));
    c.run_for(ms(1_000));
    for i in 0..10u64 {
        c.write(&format!("k{i}"), i).unwrap();
    }
    for n in [1, 2, 3] {
        c.crash(n);
    }
    c.run_for(ms(500));
    for n in [3, 1, 2] {
        c.restart(n);
        c.run_for(ms(100));
    }
    c.run_for(ms(2_000));
    assert_eq!(c.read("k9").unwrap(), Some(9));
    assert!(c.violations().is_empty(), "{:?}", c.violations());
}

#[test]
fn same_seed_replays_identically() {
    let trace = |seed| {
        let mut c = SimCluster::new(ClusterConfig::new(seed, &[(1, "a"), (2, "b"), (3, "c")]));
        c.run_for(ms(1_000));
        c.write("a", 1).unwrap();
        let l = c.leader().unwrap();
        c.crash(l);
        c.run_for(ms(1_000));
        (c.leader(), c.now(), c.state_hashes())
    };
    assert_eq!(trace(9), trace(9));
}

#[test]
fn frequent_snapshots_and_compaction_survive_crashes() {
    let mut cfg = ClusterConfig::new(21, &[(1, "a"), (2, "b"), (3, "c")]);
    cfg.snapshot_every = Some(20);
    cfg.compaction_threshold = Some(2048);
    let mut c = SimCluster::new(cfg);
    c.run_for(ms(1_000));
    for i in 0..60u64 {
        c.write(&format!("k{}", i % 5), i).unwrap();
    }
    let lagging = c.leader().map(|l| if l == 3 { 2 } else { 3 }).unwrap();
    c.crash(lagging);
    for i in 60..200u64 {
        c.write(&format!("k{}", i % 5), i).unwrap();
    }
    c.restart(lagging);
    c.run_for(ms(2_000));
    for n in [1, 2, 3] {
        c.crash(n);
    }
    for n in [1, 2, 3] {
        c.restart(n);
    }
    c.run_for(ms(2_000));
    assert_eq!(c.read("k4").unwrap(), Some(199));
    let hashes = c.state_hashes();
    assert!(hashes.windows(2).all(|w| w[0] == w[1]), "{hashes:?}");
    assert!(c.snapshots_built() > 0);
    assert!(c.violations().is_empty(), "{:?}", c.violations());
}

#[test]
fn arbitrary_commands_apply_exactly_once_and_reads_see_them() {
    use quorium_core::kv::{KvOp, Reply};
    let mut cluster = quorium_sim::cluster::SimCluster::new(quorium_sim::cluster::ClusterConfig::new(
        11,
        &[(1, "a"), (2, "b"), (3, "c")],
    ));
    let op = KvOp::Put { key: "k".into(), value: vec![1, 2] };
    assert_eq!(cluster.submit(77, 1, op.clone(), std::time::Duration::from_secs(5)), Ok(Reply::Done));
    assert_eq!(cluster.submit(77, 1, op, std::time::Duration::from_secs(5)), Ok(Reply::Done));
    let value = cluster.read_state(|s| s.get("k").cloned(), std::time::Duration::from_secs(5));
    assert_eq!(value, Ok(Some(vec![1, 2])));
    let sessions = cluster.read_state(|s| s.session(77), std::time::Duration::from_secs(5));
    assert_eq!(sessions, Ok(Some(1)));
}

#[test]
fn skewed_node_clocks_change_how_often_they_act() {
    let appends = |drift: u64| {
        let mut config = ClusterConfig::new(5, &[(1, "a"), (2, "b"), (3, "c")]);
        config.max_drift_ppm = drift;
        let mut c = SimCluster::new(config);
        c.run_for(ms(10_000));
        let drifts: Vec<i64> = [1, 2, 3].iter().map(|id| c.drift_ppm(*id)).collect();
        (c.rpc_counts().append, drifts)
    };
    let (steady, none) = appends(0);
    let (skewed, drifts) = appends(500_000);
    assert_eq!(none, vec![0, 0, 0]);
    assert!(drifts.iter().any(|d| *d != 0), "{drifts:?}");
    assert_ne!(steady, skewed, "drift had no effect on the heartbeat traffic");
}
