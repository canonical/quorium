
use quorium::Quorium;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prometheus_metrics_describe_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let addr = quorium::testing::free_addrs(1)[0];
    let metrics_addr = quorium::testing::free_addrs(1)[0];
    let node = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .datacenter("eu-1")
        .single_node_dev_mode()
        .shards(2)
        .metrics_bind(metrics_addr)
        .insecure_plaintext()
        .start()
        .await
        .unwrap();
    node.write("k", b"v".to_vec()).await.unwrap();
    let text = node.metrics_text();
    for needle in [
        "# TYPE quorium_shard_leader gauge",
        "quorium_shard_leader{shard=\"0\"} 1",
        "quorium_shard_leader{shard=\"1\"} 1",
        "quorium_shard_term{shard=\"0\"}",
        "quorium_shard_last_applied{shard=\"0\"}",
        "quorium_shard_quiesced{shard=\"1\"} 0",
        "quorium_shard_writes_per_second{shard=\"0\"}",
        "quorium_appends_sent_total",
        "quorium_wal_fsyncs_total",
        "quorium_wal_bytes",
        "quorium_wal_sync_latency_seconds",
        "quorium_node_info{datacenter=\"eu-1\"",
    ] {
        assert!(text.contains(needle), "missing {needle} in:\n{text}");
    }
    let mut stream = tokio::net::TcpStream::connect(metrics_addr).await.unwrap();
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("text/plain; version=0.0.4"));
    assert!(response.contains("quorium_shard_leader"));
    node.shutdown().await;
}

#[derive(quorium::State, serde::Serialize, serde::Deserialize, Default)]
#[state(key = String)]
pub struct Blob {
    bytes: Vec<u8>,
}

#[quorium::state]
impl Blob {
    pub fn fill(&mut self, n: u64) {
        self.bytes = vec![7; n as usize];
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn entities_above_one_mebibyte_are_counted() {
    let dir = tempfile::tempdir().unwrap();
    let addr = quorium::testing::free_addrs(1)[0];
    let node = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .single_node_dev_mode()
        .insecure_plaintext()
        .start()
        .await
        .unwrap();
    node.entity::<Blob>("small").fill(1000).await.unwrap();
    assert_eq!(node.stats().large_entity_writes, 0);
    node.entity::<Blob>("big").fill(1 << 20).await.unwrap();
    assert_eq!(node.stats().large_entity_writes, 1);
    assert!(node.metrics_text().contains("quorium_large_entity_writes_total 1"));
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoints_follow_the_configured_interval() {
    let dir = tempfile::tempdir().unwrap();
    let addr = quorium::testing::free_addrs(1)[0];
    let node = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .single_node_dev_mode()
        .checkpoint_every(10)
        .insecure_plaintext()
        .start()
        .await
        .unwrap();
    for i in 0..40 {
        node.write(&format!("k{i}"), vec![1]).await.unwrap();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let checkpointed = loop {
        let text = node.metrics_text();
        let index = text
            .lines()
            .find_map(|l| l.strip_prefix("quorium_shard_checkpoint_index{shard=\"0\"} "))
            .map(|v| v.parse::<u64>().unwrap())
            .unwrap_or_default();
        if index >= 10 || std::time::Instant::now() > deadline {
            break index;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert!(checkpointed >= 10, "checkpoint index {checkpointed}");
    node.shutdown().await;
}
