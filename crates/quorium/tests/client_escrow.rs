use std::time::Duration;

use quorium::{Client, Quorium, Transport};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clients_read_the_escrow_view() {
    let dir = tempfile::tempdir().unwrap();
    let addr = quorium::testing::free_addrs(1)[0];
    let node = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .single_node_dev_mode()
        .insecure_plaintext()
        .transport(Transport::Tcp)
        .start()
        .await
        .unwrap();
    node.escrow("stock").refill(10).await.unwrap();
    node.escrow("stock").try_take(1).await.unwrap();
    let client = Client::new(vec![addr], None)
        .with_transport(Transport::Tcp)
        .with_timeouts(Duration::from_secs(5), Duration::from_secs(1));
    let view = client.read_escrow("stock").await.unwrap().expect("the escrow exists");
    assert_eq!(view.pool + view.sold + view.outstanding.values().sum::<u64>(), 10);
    assert_eq!(client.read_escrow("missing").await.unwrap(), None);
    node.shutdown().await;
}
