use std::time::Duration;

use quorium::{LadderConfig, Quorium, StartError, TimingError};

fn builder(dir: &std::path::Path) -> quorium::Builder {
    let addr = quorium::testing::free_addrs(1)[0];
    Quorium::builder().data_dir(dir).bind(addr).insecure_plaintext().single_node_dev_mode()
}

#[tokio::test]
async fn conflicting_timings_refuse_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let started =
        builder(dir.path()).heartbeat(Duration::from_millis(500)).ladder(LadderConfig::default()).start().await;
    match started {
        Err(StartError::Timing(TimingError::BaseTooShort { minimum, .. })) => {
            assert_eq!(minimum, Duration::from_millis(1500))
        }
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("conflicting timings started"),
    }
}

#[tokio::test]
async fn timings_report_the_derived_values() {
    let dir = tempfile::tempdir().unwrap();
    let node = builder(dir.path())
        .heartbeat(Duration::from_millis(100))
        .leader_lease(Duration::from_millis(900))
        .rpc_timeout(Duration::from_secs(3))
        .write_wait(Duration::from_secs(4))
        .handoff_suspend(Duration::from_secs(7))
        .start()
        .await
        .unwrap();
    let t = node.timings();
    assert_eq!(t.heartbeat, Duration::from_millis(100));
    assert_eq!(t.leader_lease, Duration::from_millis(900));
    assert_eq!(t.ladder.base, Duration::from_millis(1000));
    assert_eq!(t.ladder.handoff_suspend, Duration::from_secs(7));
    assert_eq!(t.rpc_timeout, Duration::from_secs(3));
    assert_eq!(t.write_wait, Duration::from_secs(4));
    let estimate = node.failover_estimate();
    assert!(estimate.total >= t.ladder.base + t.ladder.max_campaign, "{estimate:?}");
    node.shutdown().await;
}
