use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quorium::{Discovery, Error, Quorium};

fn free_addrs(n: usize) -> Vec<SocketAddr> {
    quorium::testing::free_addrs(n)
}

async fn start(dir: &std::path::Path, addr: SocketAddr, seeds: &[SocketAddr], dc: &str) -> Quorium {
    Quorium::builder()
        .data_dir(dir)
        .bind(addr)
        .advertise(addr)
        .datacenter(dc)
        .discovery(Discovery::Static(seeds.to_vec()))
        .replicas(3)
        .replacement_grace(Duration::from_secs(2))
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

async fn drain(node: &Quorium, name: &str, limit: Duration) -> u64 {
    let counter = node.escrow(name);
    let deadline = Instant::now() + limit;
    let mut taken = 0;
    while Instant::now() < deadline {
        match counter.try_take(1).await {
            Ok(()) => taken += 1,
            Err(Error::SoldOut) => {
                if counter.sold().await.unwrap() == 100 {
                    return taken;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    taken
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_grant_of_a_removed_node_is_reclaimed_without_losing_its_sales() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(4);
    let seeds = &addrs[..3];
    let mut nodes = Vec::new();
    for (i, dc) in ["east", "east", "west"].iter().enumerate() {
        nodes.push(start(dirs[i].path(), addrs[i], seeds, dc).await);
    }
    assert!(eventually(Duration::from_secs(10), || nodes.iter().all(|n| n.status().leader.is_some())).await);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let seller = nodes[0].escrow("vip");
    seller.refill(100).await.unwrap();
    for _ in 0..5 {
        seller.try_take(1).await.unwrap();
    }
    assert!(seller.local_remaining() > 0, "the seller sold from a local grant");
    let seller_id = nodes[0].status().id;
    nodes.remove(0).shutdown().await;

    let newcomer = start(dirs[3].path(), addrs[3], seeds, "east").await;
    let survivor = nodes[0].clone();
    assert!(
        eventually(Duration::from_secs(20), || !survivor.membership().voters.contains(&seller_id)).await,
        "{:?}",
        survivor.membership()
    );
    let taken = drain(&survivor, "vip", Duration::from_secs(30)).await;
    assert_eq!(taken, 95, "every unit the removed node did not sell is sold exactly once");
    assert_eq!(survivor.escrow("vip").sold().await.unwrap(), 100);

    newcomer.shutdown().await;
    for n in nodes {
        n.shutdown().await;
    }
}
