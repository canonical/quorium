use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quorium::{Discovery, Quorium};
use quorium_sim::checkers::linearizability::{History, Register, RegisterOp, RegisterRet, check_key};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = u64, name = "rebalance.Box")]
pub struct Item {
    n: u64,
}

#[quorium::state]
impl Item {
    pub fn set(&mut self, n: u64) {
        self.n = n;
    }

    pub fn value(&self) -> u64 {
        self.n
    }
}

fn free_addrs(n: usize) -> Vec<SocketAddr> {
    quorium::testing::free_addrs(n)
}

async fn cluster(groups: u64) -> (Vec<Quorium>, Vec<tempfile::TempDir>) {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (i, dc) in ["a", "b", "c"].iter().enumerate() {
        nodes.push(
            Quorium::builder()
                .data_dir(dirs[i].path())
                .bind(addrs[i])
                .advertise(addrs[i])
                .datacenter(dc)
                .discovery(Discovery::Static(addrs.clone()))
                .replicas(3)
                .shards(groups)
                .insecure_plaintext()
                .start()
                .await
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while (0..groups).any(|g| nodes.iter().any(|n| n.shard_status(g).leader.is_none())) {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (nodes, dirs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn moving_a_range_keeps_every_entity_reachable() {
    let (nodes, _dirs) = cluster(4).await;
    for k in 0..80u64 {
        nodes[(k % 3) as usize].entity::<Item>(k).set(k + 1).await.unwrap();
    }
    let before = nodes[0].routing().await.unwrap();
    let (lo, hi, from) = before.ranges[1];
    let mid = lo + (hi - lo) / 2;
    nodes[1].split_range(mid).await.unwrap();
    nodes[2].move_range(mid, hi, 3).await.unwrap();
    let after = nodes[0].routing().await.unwrap();
    assert!(after.generation > before.generation);
    assert!(after.ranges.contains(&(mid, hi, 3)), "{after:?}");
    assert!(after.ranges.contains(&(lo, mid - 1, from)), "{after:?}");
    for k in 0..80u64 {
        assert_eq!(nodes[((k + 1) % 3) as usize].entity::<Item>(k).value().await, Ok(k + 1));
    }
    for k in 0..80u64 {
        nodes[(k % 3) as usize].entity::<Item>(k).set(k + 100).await.unwrap();
        assert_eq!(nodes[0].entity::<Item>(k).value().await, Ok(k + 100));
    }
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writes_stay_linearizable_while_ranges_move() {
    let (nodes, _dirs) = cluster(2).await;
    let epoch = Instant::now();
    let micros = move || epoch.elapsed().as_micros() as u64;
    let histories: Arc<Mutex<Vec<History<Register>>>> = Arc::new(Mutex::new((0..8).map(|_| History::new()).collect()));
    let stop = epoch + Duration::from_secs(6);
    let mut workers = Vec::new();
    for w in 0..6u64 {
        let node = nodes[(w % 3) as usize].clone();
        let histories = histories.clone();
        workers.push(tokio::spawn(async move {
            let mut seq = 0u64;
            while Instant::now() < stop {
                let key = ((w * 7 + seq) % 8) as usize;
                let name = format!("lin-{key}");
                let call = micros();
                if seq.is_multiple_of(2) {
                    let value = (w << 32) | seq;
                    let ok = node.write(&name, value.to_be_bytes().to_vec()).await.is_ok();
                    let ret = micros();
                    histories.lock().unwrap()[key].record(
                        w,
                        RegisterOp::Write(value),
                        call,
                        ok.then_some((ret, RegisterRet::Ok)),
                    );
                } else if let Ok(v) = node.read(&name).await {
                    let ret = micros();
                    let value = v.and_then(|b| b.try_into().ok()).map(u64::from_be_bytes);
                    histories.lock().unwrap()[key].record(
                        w,
                        RegisterOp::Read,
                        call,
                        Some((ret, RegisterRet::Value(value))),
                    );
                }
                seq += 1;
            }
        }));
    }
    let table = nodes[0].routing().await.unwrap();
    let (lo, hi, _) = table.ranges[0];
    let quarter = lo + (hi - lo) / 4;
    nodes[0].split_range(quarter).await.unwrap();
    nodes[1].move_range(quarter, hi, 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    nodes[2].move_range(quarter, hi, 0).await.unwrap();
    for w in workers {
        w.await.unwrap();
    }
    for (i, h) in histories.lock().unwrap().iter().enumerate() {
        assert!(h.len() > 10, "key {i} saw only {} operations", h.len());
        check_key(&format!("lin-{i}"), h).unwrap();
    }
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adjacent_ranges_on_one_group_merge() {
    let (nodes, _dirs) = cluster(2).await;
    let table = nodes[0].routing().await.unwrap();
    let (lo, hi, group) = table.ranges[0];
    let mid = lo + (hi - lo) / 2;
    nodes[0].split_range(mid).await.unwrap();
    assert_eq!(nodes[0].routing().await.unwrap().ranges.len(), 3);
    nodes[0].merge_ranges().await.unwrap();
    let merged = nodes[0].routing().await.unwrap();
    assert_eq!(merged.ranges.len(), 2);
    assert!(merged.ranges.contains(&(lo, hi, group)));
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_move_is_finished_by_the_next_attempt() {
    let (nodes, _dirs) = cluster(2).await;
    for k in 0..30u64 {
        nodes[0].entity::<Item>(k).set(k).await.unwrap();
    }
    let table = nodes[0].routing().await.unwrap();
    let (lo, hi, _) = table.ranges[0];
    nodes[0].__test_begin_move(lo, hi, 1).await.unwrap();
    nodes[1].move_range(lo, hi, 1).await.unwrap();
    for k in 0..30u64 {
        assert_eq!(nodes[2].entity::<Item>(k).value().await, Ok(k));
    }
    assert!(nodes[0].routing().await.unwrap().ranges.contains(&(lo, hi, 1)));
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rebalancer_spreads_an_overloaded_group() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(
            Quorium::builder()
                .data_dir(dirs[i].path())
                .bind(addrs[i])
                .advertise(addrs[i])
                .discovery(Discovery::Static(addrs.clone()))
                .replicas(3)
                .shards(3)
                .auto_rebalance(Duration::from_millis(500), 50)
                .insecure_plaintext()
                .start()
                .await
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while (0..3).any(|g| nodes[0].shard_status(g).leader.is_none()) {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let hot: Vec<u64> = (0..100_000u64).filter(|k| nodes[0].shard_of::<Item>(k) == 0).take(150).collect();
    for k in &hot {
        nodes[0].entity::<Item>(*k).set(*k).await.unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let sizes = nodes[0].group_sizes().await;
        if sizes[0] < 120 && sizes.iter().skip(1).any(|s| *s > 20) {
            break;
        }
        assert!(Instant::now() < deadline, "never rebalanced: {sizes:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for k in &hot {
        assert_eq!(nodes[1].entity::<Item>(*k).value().await, Ok(*k));
    }
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_ranges_are_split_and_moved_by_load() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(
            Quorium::builder()
                .data_dir(dirs[i].path())
                .bind(addrs[i])
                .advertise(addrs[i])
                .discovery(Discovery::Static(addrs.clone()))
                .replicas(3)
                .shards(3)
                .auto_rebalance(Duration::from_millis(500), usize::MAX)
                .split_by_load(50)
                .insecure_plaintext()
                .start()
                .await
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while (0..3).any(|g| nodes[0].shard_status(g).leader.is_none()) {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let hot: Vec<u64> = (0..100_000u64).filter(|k| nodes[0].shard_of::<Item>(k) == 0).take(40).collect();
    let before = nodes[0].routing().await.unwrap();
    let writer = nodes[0].clone();
    let keys = hot.clone();
    let load = tokio::spawn(async move {
        let mut n = 0u64;
        loop {
            for k in &keys {
                n += 1;
                let _ = writer.entity::<Item>(*k).set(n).await;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let moved = loop {
        let table = nodes[1].routing().await.unwrap();
        let away = hot.iter().filter(|k| nodes[1].shard_of::<Item>(k) != 0).count();
        if table.generation > before.generation && away > 0 {
            break away;
        }
        assert!(Instant::now() < deadline, "never split by load: {table:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    load.abort();
    assert!(moved < hot.len(), "the whole hot range moved: {moved}");
    for k in &hot {
        assert!(nodes[2].entity::<Item>(*k).value().await.unwrap() > 0);
    }
    for n in nodes {
        n.shutdown().await;
    }
}
