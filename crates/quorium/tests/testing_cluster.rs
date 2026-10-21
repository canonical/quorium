use std::time::Duration;

use quorium::testing::Cluster;
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = u64)]
pub struct Tally {
    n: u64,
}

#[quorium::state]
impl Tally {
    pub fn bump(&mut self) -> u64 {
        self.n += 1;
        self.n
    }

    pub fn value(&self) -> u64 {
        self.n
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_harness_survives_killing_the_leader() {
    let mut cluster = Cluster::start(3).await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let tally = cluster.node(0).entity::<Tally>(1u64);
    assert_eq!(tally.bump().await, Ok(1));
    cluster.kill(leader).await;
    assert_eq!(cluster.running(), 2);
    let any = cluster.any_running();
    assert_eq!(cluster.node(any).entity::<Tally>(1u64).bump().await, Ok(2));
    cluster.restart(leader).await;
    assert_eq!(cluster.running(), 3);
    assert_eq!(cluster.node(leader).entity::<Tally>(1u64).value().await, Ok(2));
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_harness_for_unit_tests() {
    let cluster = Cluster::start(1).await;
    let tally = cluster.node(0).entity::<Tally>(5u64);
    assert_eq!(tally.bump().await, Ok(1));
    cluster.shutdown().await;
}

#[test]
fn free_addresses_avoid_ephemeral_ports_and_never_repeat() {
    let first = quorium::testing::free_addrs(5);
    let second = quorium::testing::free_addrs(5);
    let mut ports: Vec<u16> = first.iter().chain(&second).map(|a| a.port()).collect();
    assert!(ports.iter().all(|p| *p < 32_768 && *p >= 10_000), "{ports:?}");
    ports.sort();
    ports.dedup();
    assert_eq!(ports.len(), 10);
    for a in first.iter().chain(&second) {
        std::net::TcpListener::bind(a).unwrap();
        std::net::UdpSocket::bind(a).unwrap();
    }
}
