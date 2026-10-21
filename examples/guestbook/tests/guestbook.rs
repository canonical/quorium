use std::time::Duration;

use guestbook::Guestbook;
use quorium::testing::{Cluster, SimCluster};

#[tokio::test(flavor = "multi_thread")]
async fn signatures_survive_the_loss_of_the_leader() {
    let mut cluster = Cluster::start(3).await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let book = cluster.node(leader).entity::<Guestbook>("lobby");
    book.sign("ada".into()).await.unwrap();

    cluster.kill(leader).await;

    let survivor = cluster.any_running();
    let book = cluster.node(survivor).entity::<Guestbook>("lobby");
    assert_eq!(book.sign("grace".into()).await, Ok(2));
    assert_eq!(book.names().await.unwrap(), vec!["ada".to_string(), "grace".to_string()]);
    cluster.shutdown().await;
}

#[tokio::test]
async fn the_majority_side_of_a_partition_keeps_accepting_signatures() {
    let sim = SimCluster::start(7, 3);
    let book = sim.entity::<Guestbook>("lobby");
    book.sign("ada".into()).await.unwrap();

    let leader = sim.leader().unwrap();
    let others: Vec<u64> = sim.nodes().into_iter().filter(|n| *n != leader).collect();
    sim.partition(&[leader], &others);
    sim.run_for(Duration::from_secs(2));

    assert_eq!(book.sign("grace".into()).await, Ok(2));
    sim.heal();
    assert!(sim.violations().is_empty());
}
