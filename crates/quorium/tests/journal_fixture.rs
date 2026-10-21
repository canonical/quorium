use std::path::PathBuf;
use std::time::Duration;

use quorium::Quorium;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/escrow-journal-v1.bin")
}

async fn node(dir: &std::path::Path) -> Quorium {
    let addr = quorium::testing::free_addrs(1)[0];
    Quorium::builder().data_dir(dir).bind(addr).single_node_dev_mode().insecure_plaintext().start().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn escrow_journals_written_today_stay_readable() {
    if std::env::var("QUORIUM_WRITE_FIXTURES").is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let n = node(dir.path()).await;
        let stock = n.escrow("fixture-stock");
        stock.refill(100).await.unwrap();
        for _ in 0..3 {
            stock.try_take(1).await.unwrap();
        }
        let remaining = stock.local_remaining();
        n.shutdown().await;
        std::fs::copy(dir.path().join("escrow.journal"), fixture()).unwrap();
        std::fs::write(fixture().with_extension("remaining"), remaining.to_string()).unwrap();
    }
    let expected: u64 = std::fs::read_to_string(fixture().with_extension("remaining"))
        .expect("fixture exists; regenerate with QUORIUM_WRITE_FIXTURES=1")
        .trim()
        .parse()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(fixture(), dir.path().join("escrow.journal")).unwrap();
    let n = node(dir.path()).await;
    assert_eq!(n.escrow("fixture-stock").local_remaining(), expected);
    assert!(expected > 0);
    tokio::time::sleep(Duration::from_millis(10)).await;
    n.shutdown().await;
}
