use std::time::Duration;

use quorium::testing::Cluster;
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[state(key = u64, name = "watch.Score")]
pub struct Score {
    points: u64,
}

#[quorium::state]
impl Score {
    pub fn add(&mut self, n: u64) -> u64 {
        self.points += n;
        self.points
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watchers_on_any_replica_see_changes_in_commit_order() {
    let cluster = Cluster::start(3).await;
    cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let mut watch = cluster.node(2).entity::<Score>(1u64).watch();
    let writer = cluster.node(0).entity::<Score>(1u64);
    for i in 1..=3u64 {
        writer.add(1).await.unwrap();
        let seen = tokio::time::timeout(Duration::from_secs(5), watch.changed()).await.unwrap().unwrap();
        assert_eq!(seen.points, i);
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_watchers_skip_to_the_latest_version() {
    let cluster = Cluster::start(1).await;
    let node = cluster.node(0);
    let mut watch = node.entity::<Score>(9u64).watch();
    for _ in 0..10 {
        node.entity::<Score>(9u64).add(1).await.unwrap();
    }
    let seen = tokio::time::timeout(Duration::from_secs(5), watch.changed()).await.unwrap().unwrap();
    assert_eq!(seen.points, 10);
    assert!(tokio::time::timeout(Duration::from_millis(200), watch.changed()).await.is_err());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deletion_is_reported_as_the_default_state() {
    let cluster = Cluster::start(1).await;
    let node = cluster.node(0);
    let score = node.entity::<Score>(4u64);
    score.add(5).await.unwrap();
    let mut watch = score.watch();
    score.delete().await.unwrap();
    let seen = tokio::time::timeout(Duration::from_secs(5), watch.changed()).await.unwrap().unwrap();
    assert_eq!(seen, Score::default());
    cluster.shutdown().await;
}
