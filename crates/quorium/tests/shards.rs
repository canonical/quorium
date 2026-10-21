use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quorium::{Discovery, Quorium};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = u64, name = "shards.Counter")]
pub struct Counter {
    n: u64,
}

#[quorium::state]
impl Counter {
    pub fn bump(&mut self) -> u64 {
        self.n += 1;
        self.n
    }

    pub fn value(&self) -> u64 {
        self.n
    }
}

fn free_addrs(n: usize) -> Vec<SocketAddr> {
    quorium::testing::free_addrs(n)
}

async fn cluster(shards: u64, dcs: &[&str], quiesce_after: Option<Duration>) -> (Vec<Quorium>, Vec<tempfile::TempDir>) {
    let dirs: Vec<_> = dcs.iter().map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(dcs.len());
    let mut nodes = Vec::new();
    for (i, dc) in dcs.iter().enumerate() {
        let mut b = Quorium::builder()
            .data_dir(dirs[i].path())
            .bind(addrs[i])
            .advertise(addrs[i])
            .datacenter(dc)
            .discovery(Discovery::Static(addrs.clone()))
            .replicas(dcs.len())
            .shards(shards)
            .handoff_suspend(Duration::from_secs(1))
            .insecure_plaintext();
        if let Some(q) = quiesce_after {
            b = b.quiesce_after(q);
        }
        nodes.push(b.start().await.unwrap());
    }
    (nodes, dirs)
}

async fn balanced(nodes: &[Quorium], shards: u64, limit: Duration) -> bool {
    let fair = (shards as usize).div_ceil(nodes.len());
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        let mut led: BTreeMap<u64, usize> = BTreeMap::new();
        for s in 0..shards {
            if let Some(l) = nodes[0].shard_status(s).leader {
                *led.entry(l).or_default() += 1;
            }
        }
        if led.len() == nodes.len().min(shards as usize) && led.values().all(|c| *c <= fair) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn all_shards_led(nodes: &[Quorium], shards: u64, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if (0..shards).all(|s| nodes.iter().all(|n| n.shard_status(s).leader.is_some())) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entities_spread_over_shards_and_leaders_spread_over_nodes() {
    let (nodes, _dirs) = cluster(12, &["a", "b", "c"], None).await;
    assert!(all_shards_led(&nodes, 12, Duration::from_secs(15)).await);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut led: BTreeMap<u64, usize> = BTreeMap::new();
        for s in 0..12 {
            if let Some(l) = nodes[0].shard_status(s).leader {
                *led.entry(l).or_default() += 1;
            }
        }
        if led.len() == 3 && led.values().all(|c| *c == 4) {
            break;
        }
        assert!(Instant::now() < deadline, "leaders never balanced: {led:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut used = std::collections::BTreeSet::new();
    for k in 0..60u64 {
        assert_eq!(nodes[(k % 3) as usize].entity::<Counter>(k).bump().await, Ok(1));
        used.insert(nodes[0].shard_of::<Counter>(&k));
    }
    assert!(used.len() >= 9, "{used:?}");
    for k in 0..60u64 {
        assert_eq!(nodes[((k + 1) % 3) as usize].entity::<Counter>(k).value().await, Ok(1));
    }
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_shard_fails_over_when_a_node_dies() {
    let (mut nodes, _dirs) = cluster(8, &["a", "b", "c"], None).await;
    assert!(all_shards_led(&nodes, 8, Duration::from_secs(15)).await);
    let victim = nodes.remove(1);
    let victim_id = victim.status().id;
    victim.shutdown().await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while (0..8).any(|s| nodes.iter().any(|n| n.shard_status(s).leader.is_none_or(|l| l == victim_id))) {
        assert!(Instant::now() < deadline, "some shard never failed over");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for k in 0..40u64 {
        assert_eq!(nodes[0].entity::<Counter>(k).bump().await, Ok(1));
    }
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_shards_quiesce_and_stop_heartbeating() {
    let (nodes, _dirs) = cluster(32, &["a", "b", "c"], Some(Duration::from_millis(300))).await;
    assert!(all_shards_led(&nodes, 32, Duration::from_secs(15)).await);
    assert!(balanced(&nodes, 32, Duration::from_secs(30)).await);
    nodes[0].entity::<Counter>(1u64).bump().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes.iter().map(|n| n.stats().quiesced_shards).sum::<usize>() < 32 {
        assert!(Instant::now() < deadline, "shards never quiesced");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let before: u64 = nodes.iter().map(|n| n.stats().appends_sent).sum();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let idle: u64 = nodes.iter().map(|n| n.stats().appends_sent).sum::<u64>() - before;
    assert!(idle < 10, "{idle} appends in 2s across 32 idle shards");
    assert_eq!(nodes[1].entity::<Counter>(1u64).bump().await, Ok(2));
    assert!((0..32).all(|s| nodes[0].shard_status(s).leader.is_some()));
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_leadership_follows_the_writer() {
    let (nodes, _dirs) = cluster(6, &["a", "b", "c"], None).await;
    assert!(all_shards_led(&nodes, 6, Duration::from_secs(15)).await);
    assert!(balanced(&nodes, 6, Duration::from_secs(30)).await);
    let writer = &nodes[2];
    let key = (0..1000u64).find(|k| {
        let s = writer.shard_of::<Counter>(k);
        writer.shard_status(s).leader != Some(writer.status().id)
    });
    let key = key.unwrap();
    let shard = writer.shard_of::<Counter>(&key);
    let deadline = Instant::now() + Duration::from_secs(60);
    while writer.shard_status(shard).leader != Some(writer.status().id) {
        assert!(Instant::now() < deadline, "leadership never followed the writer");
        writer.entity::<Counter>(key).bump().await.unwrap();
    }
    for n in nodes {
        n.shutdown().await;
    }
}

async fn idle_messages_per_second(shards: u64) -> f64 {
    let (nodes, _dirs) = cluster(shards, &["a", "b", "c"], Some(Duration::from_millis(300))).await;
    assert!(all_shards_led(&nodes, shards, Duration::from_secs(30)).await);
    assert!(balanced(&nodes, shards, Duration::from_secs(60)).await);
    let deadline = Instant::now() + Duration::from_secs(20);
    while nodes.iter().map(|n| n.stats().quiesced_shards).sum::<usize>() < shards as usize {
        assert!(Instant::now() < deadline, "shards never quiesced");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let total =
        |nodes: &[Quorium]| nodes.iter().map(|n| n.stats().appends_sent + n.stats().pings_received).sum::<u64>();
    let before = total(&nodes);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rate = (total(&nodes) - before) as f64 / 3.0;
    for n in nodes {
        n.shutdown().await;
    }
    rate
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_traffic_is_flat_as_shards_grow() {
    let one = idle_messages_per_second(1).await;
    let many = idle_messages_per_second(64).await;
    eprintln!("idle messages per second: 1 shard {one:.1}, 64 shards {many:.1}");
    assert!(many <= one * 1.25 + 2.0, "1 shard: {one:.1} msg/s, 64 shards: {many:.1} msg/s");
}
