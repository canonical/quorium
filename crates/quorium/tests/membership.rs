use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quorium::{Discovery, Quorium};

fn free_addrs(n: usize) -> Vec<SocketAddr> {
    quorium::testing::free_addrs(n)
}

async fn start(dir: &std::path::Path, addr: SocketAddr, seeds: &[SocketAddr], grace: Duration) -> Quorium {
    Quorium::builder()
        .data_dir(dir)
        .bind(addr)
        .advertise(addr)
        .discovery(Discovery::Static(seeds.to_vec()))
        .replicas(3)
        .replacement_grace(grace)
        .insecure_plaintext()
        .start()
        .await
        .unwrap()
}

async fn eventually(limit: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn newcomer_joins_as_learner_and_replaces_a_dead_voter() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(4);
    let seeds = &addrs[..3];
    let grace = Duration::from_secs(2);
    let mut nodes: Vec<Option<Quorium>> = Vec::new();
    for i in 0..3 {
        nodes.push(Some(start(dirs[i].path(), addrs[i], seeds, grace).await));
    }
    assert!(eventually(Duration::from_secs(10), || nodes[0].as_ref().unwrap().status().leader.is_some()).await);
    nodes[0].as_ref().unwrap().write("x", b"1".to_vec()).await.unwrap();

    let newcomer = start(dirs[3].path(), addrs[3], seeds, grace).await;
    let new_id = newcomer.status().id;
    assert!(
        eventually(Duration::from_secs(10), || nodes[0].as_ref().unwrap().membership().learners.contains(&new_id))
            .await
    );
    assert_eq!(nodes[0].as_ref().unwrap().membership().voters.len(), 3);
    assert!(eventually(Duration::from_secs(10), || newcomer.status().leader.is_some()).await);

    let leader = nodes[0].as_ref().unwrap().status().leader.unwrap();
    let victim = (0..3).find(|i| nodes[*i].as_ref().unwrap().status().id != leader).unwrap();
    let victim_id = nodes[victim].as_ref().unwrap().status().id;
    nodes[victim].take().unwrap().shutdown().await;
    let survivor = nodes.iter().flatten().next().unwrap().clone();
    assert!(
        eventually(Duration::from_secs(15), || {
            let m = survivor.membership();
            m.voters.contains(&new_id) && !m.voters.contains(&victim_id) && m.voters.len() == 3
        })
        .await,
        "{:?}",
        survivor.membership()
    );
    survivor.write("x", b"2".to_vec()).await.unwrap();
    assert_eq!(newcomer.read("x").await.unwrap(), Some(b"2".to_vec()));
    newcomer.shutdown().await;
    for n in nodes.into_iter().flatten() {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_voter_is_not_removed_without_a_replacement() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let grace = Duration::from_millis(500);
    let mut nodes: Vec<Option<Quorium>> = Vec::new();
    for i in 0..3 {
        nodes.push(Some(start(dirs[i].path(), addrs[i], &addrs, grace).await));
    }
    assert!(eventually(Duration::from_secs(10), || nodes[0].as_ref().unwrap().status().leader.is_some()).await);
    let leader = nodes[0].as_ref().unwrap().status().leader.unwrap();
    let victim = (0..3).find(|i| nodes[*i].as_ref().unwrap().status().id != leader).unwrap();
    nodes[victim].take().unwrap().shutdown().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let survivor = nodes.iter().flatten().next().unwrap().clone();
    assert_eq!(survivor.membership().voters.len(), 3);
    for n in nodes.into_iter().flatten() {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn newcomer_with_lowest_id_never_bootstraps_a_second_cluster() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    std::fs::write(dirs[3].path().join("node_id"), "1").unwrap();
    let addrs = free_addrs(4);
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(start(dirs[i].path(), addrs[i], &addrs[..3], Duration::from_secs(600)).await);
    }
    assert!(eventually(Duration::from_secs(10), || nodes[0].status().leader.is_some()).await);
    let newcomer = start(dirs[3].path(), addrs[3], &addrs, Duration::from_secs(600)).await;
    assert!(eventually(Duration::from_secs(10), || nodes[0].membership().learners.contains(&1)).await);
    assert!(eventually(Duration::from_secs(10), || newcomer.status().leader == nodes[0].status().leader).await);
    assert!(!newcomer.membership().voters.contains(&1));
    newcomer.shutdown().await;
    for n in nodes {
        n.shutdown().await;
    }
}
