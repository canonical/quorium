use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quorium::{Discovery, Quorium};

fn gauge(node: &Quorium, name: &str) -> u64 {
    let prefix = format!("{name}{{shard=\"0\"}} ");
    node.metrics_text().lines().find_map(|l| l.strip_prefix(&prefix)).map(|v| v.parse().unwrap()).unwrap_or_default()
}

async fn checkpointed_single_node(retention: Option<u64>) -> (u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let addr = quorium::testing::free_addrs(1)[0];
    let mut builder = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .single_node_dev_mode()
        .checkpoint_every(10)
        .insecure_plaintext();
    if let Some(entries) = retention {
        builder = builder.log_retention(entries);
    }
    let node = builder.start().await.unwrap();
    for i in 0..60 {
        node.write(&format!("k{i}"), vec![1]).await.unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while gauge(&node, "quorium_shard_checkpoint_index") < 50 {
        assert!(Instant::now() < deadline, "no checkpoint");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let result = (gauge(&node, "quorium_shard_checkpoint_index"), gauge(&node, "quorium_shard_purged_index"));
    node.shutdown().await;
    result
}

#[tokio::test]
async fn log_retention_keeps_entries_behind_a_checkpoint() {
    let (checkpoint, purged) = checkpointed_single_node(None).await;
    assert!(purged + 10 >= checkpoint && purged > 0, "checkpoint {checkpoint}, purged {purged}");
    let (checkpoint, purged) = checkpointed_single_node(Some(1_000)).await;
    assert!(checkpoint >= 50);
    assert_eq!(purged, 0, "checkpoint {checkpoint}");
}

async fn start(dir: &std::path::Path, addr: SocketAddr, seeds: &[SocketAddr]) -> Quorium {
    Quorium::builder()
        .data_dir(dir)
        .bind(addr)
        .advertise(addr)
        .discovery(Discovery::Static(seeds.to_vec()))
        .replicas(seeds.len())
        .checkpoint_every(20)
        .insecure_plaintext()
        .start()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshots_sent_are_counted_per_peer() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = quorium::testing::free_addrs(3);
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(Some(start(dir.path(), addrs[i], &addrs).await));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let leader = loop {
        if let Some(i) = (0..3).find(|i| nodes[*i].as_ref().unwrap().status().is_leader) {
            break i;
        }
        assert!(Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let lagger = (leader + 1) % 3;
    let lagger_id = nodes[lagger].as_ref().unwrap().status().id;
    nodes[lagger].take().unwrap().shutdown().await;
    let leader_node = nodes[leader].clone().unwrap();
    for i in 0..200 {
        leader_node.write(&format!("k{i}"), vec![1]).await.unwrap();
    }
    leader_node.checkpoint().await.unwrap();
    nodes[lagger] = Some(start(dirs[lagger].path(), addrs[lagger], &addrs).await);
    let deadline = Instant::now() + Duration::from_secs(15);
    while leader_node.stats().snapshots_sent.get(&lagger_id).copied().unwrap_or(0) == 0 {
        assert!(Instant::now() < deadline, "no snapshot was sent: {:?}", leader_node.stats().snapshots_sent);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(leader_node.metrics_text().contains(&format!("quorium_snapshots_sent_total{{peer=\"{lagger_id}\"}}")));
    for n in nodes.into_iter().flatten() {
        n.shutdown().await;
    }
}
