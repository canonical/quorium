use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use quorium::Error;
use quorium::testing::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_takes_are_fast_and_bounded_by_stock() {
    let cluster = Cluster::start(1).await;
    let node = cluster.node(0);
    let tickets = node.escrow("concert");
    tickets.refill(100).await.unwrap();
    tickets.try_take(1).await.unwrap();
    let local = tickets.local_remaining();
    assert!(local > 0);
    let start = Instant::now();
    for _ in 0..local.min(10) {
        tickets.try_take(1).await.unwrap();
    }
    assert!(start.elapsed() < Duration::from_millis(200), "{:?}", start.elapsed());
    let mut sold = 1 + local.min(10);
    loop {
        match tickets.try_take(1).await {
            Ok(()) => sold += 1,
            Err(Error::SoldOut) => break,
            Err(e) => panic!("{e:?}"),
        }
    }
    assert_eq!(sold, 100);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_sellers_on_every_node_never_oversell() {
    let cluster = Cluster::start(3).await;
    cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    cluster.node(0).escrow("gpu").refill(250).await.unwrap();
    let sold = Arc::new(AtomicU64::new(0));
    let sellers = (0..12).map(|i| {
        let escrow = cluster.node(i % 3).escrow("gpu");
        let sold = sold.clone();
        async move {
            loop {
                match escrow.try_take(1).await {
                    Ok(()) => {
                        sold.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(Error::SoldOut) => return,
                    Err(e) => panic!("{e:?}"),
                }
            }
        }
    });
    futures::future::join_all(sellers).await;
    assert_eq!(sold.load(Ordering::SeqCst), 250);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_grants_are_settled_and_released() {
    let cluster = Cluster::start(1).await;
    let node = cluster.node(0);
    let e = node.escrow("seats");
    e.refill(40).await.unwrap();
    e.try_take(3).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let estimate = e.estimate().await.unwrap();
        if e.local_remaining() == 0 && estimate == 37 {
            break;
        }
        assert!(Instant::now() < deadline, "estimate {estimate}, local {}", e.local_remaining());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsettled_sales_survive_a_restart() {
    let mut cluster = Cluster::start(1).await;
    let e = cluster.node(0).escrow("rooms");
    e.refill(20).await.unwrap();
    e.try_take(5).await.unwrap();
    cluster.kill(0).await;
    cluster.restart(0).await;
    let e = cluster.node(0).escrow("rooms");
    let deadline = Instant::now() + Duration::from_secs(10);
    while e.sold().await.unwrap() != 5 {
        assert!(Instant::now() < deadline, "sold {}", e.sold().await.unwrap());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sales_are_held_by_a_same_datacenter_peer_until_settled() {
    use quorium::{Discovery, Quorium};
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = quorium::testing::free_addrs(3);
    let mut nodes = Vec::new();
    for (i, dc) in ["east", "east", "west"].iter().enumerate() {
        nodes.push(
            Quorium::builder()
                .data_dir(dirs[i].path())
                .bind(addrs[i])
                .advertise(addrs[i])
                .datacenter(dc)
                .discovery(Discovery::Static(addrs.clone()))
                .replicas(3)
                .insecure_plaintext()
                .start()
                .await
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes.iter().any(|n| n.status().leader.is_none()) {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let seller = nodes[0].escrow("vip");
    seller.refill(50).await.unwrap();
    seller.try_take(3).await.unwrap();
    let owner = nodes[0].status().id;
    assert!(nodes[1].escrow_held_for(owner, "vip") > 0 || seller.sold().await.unwrap() == 3);
    assert_eq!(nodes[2].escrow_held_for(owner, "vip"), 0);
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes[1].escrow_held_for(owner, "vip") != 0 || seller.sold().await.unwrap() != 3 {
        assert!(Instant::now() < deadline, "held {}", nodes[1].escrow_held_for(owner, "vip"));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for n in nodes {
        n.shutdown().await;
    }
}
