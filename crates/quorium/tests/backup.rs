use std::time::Duration;

use quorium::testing::Cluster;
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default, Debug, PartialEq)]
#[state(key = String, name = "backup.Account")]
pub struct Account {
    balance: i64,
}

#[quorium::state]
impl Account {
    pub fn deposit(&mut self, amount: i64) -> i64 {
        self.balance += amount;
        self.balance
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn export_then_import_restores_every_shard() {
    let source = Cluster::start(3).await;
    source.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let node = source.node(0);
    for i in 0..20 {
        node.entity::<Account>(format!("acct-{i}")).deposit(i * 10).await.unwrap();
    }
    node.write("motd", b"hello".to_vec()).await.unwrap();
    node.escrow("tickets").refill(30).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("backup.qbak");
    node.export(&archive).await.unwrap();
    source.shutdown().await;

    let target = Cluster::start(1).await;
    let fresh = target.node(0);
    fresh.import(&archive).await.unwrap();
    for i in 0..20 {
        assert_eq!(fresh.entity::<Account>(format!("acct-{i}")).get().await.unwrap().balance, i * 10);
    }
    assert_eq!(fresh.read("motd").await.unwrap(), Some(b"hello".to_vec()));
    assert_eq!(fresh.escrow("tickets").estimate().await.unwrap(), 30);
    assert!(fresh.import(&archive).await.is_err());
    target.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn damaged_archives_are_refused() {
    let cluster = Cluster::start(1).await;
    let node = cluster.node(0);
    node.write("k", b"v".to_vec()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("b.qbak");
    node.export(&archive).await.unwrap();
    let mut bytes = std::fs::read(&archive).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&archive, bytes).unwrap();
    let other = Cluster::start(1).await;
    assert!(other.node(0).import(&archive).await.is_err());
    other.shutdown().await;
    cluster.shutdown().await;
}
