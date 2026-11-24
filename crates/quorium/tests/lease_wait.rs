use std::time::{Duration, Instant};

use quorium::ClientError;
use quorium::testing::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_on_a_leader_without_a_quorum_fail_with_no_leader() {
    let mut cluster = Cluster::start(3).await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.expect("a leader");
    cluster.node(leader).write("warm", b"up".to_vec()).await.unwrap();
    for i in 0..3 {
        if i != leader {
            cluster.kill(i).await;
        }
    }
    let node = cluster.node(leader).clone();
    let started = Instant::now();
    let writers: Vec<_> = (0..64)
        .map(|i| {
            let node = node.clone();
            tokio::spawn(async move { node.write(&format!("k{i}"), b"v".to_vec()).await })
        })
        .collect();
    for w in writers {
        match w.await.unwrap() {
            Err(ClientError::NoLeader { .. }) => {}
            other => panic!("expected NoLeader, got {other:?}"),
        }
    }
    assert!(started.elapsed() < Duration::from_secs(8), "{:?}", started.elapsed());
    cluster.shutdown().await;
}
