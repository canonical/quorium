use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quorium::testing::Cluster;

#[derive(Default)]
struct Tracker {
    running: AtomicUsize,
    max_running: AtomicUsize,
    tokens: Mutex<Vec<(usize, u64)>>,
}

fn job(
    tracker: Arc<Tracker>,
    node: usize,
) -> impl Fn(quorium::Lease) -> futures::future::BoxFuture<'static, ()> + Send + Sync + 'static {
    move |lease: quorium::Lease| {
        let tracker = tracker.clone();
        Box::pin(async move {
            tracker.tokens.lock().unwrap().push((node, lease.token));
            let now = tracker.running.fetch_add(1, Ordering::SeqCst) + 1;
            tracker.max_running.fetch_max(now, Ordering::SeqCst);
            struct Done(Arc<Tracker>);
            impl Drop for Done {
                fn drop(&mut self) {
                    self.0.running.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _done = Done(tracker.clone());
            std::future::pending::<()>().await;
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exactly_one_node_runs_the_job_and_it_moves_on_failure() {
    let mut cluster = Cluster::start(3).await;
    cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let tracker = Arc::new(Tracker::default());
    let ttl = Duration::from_millis(900);
    let handles: Vec<_> =
        (0..3).map(|i| cluster.node(i).singleton_with("billing", ttl, job(tracker.clone(), i))).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while tracker.tokens.lock().unwrap().is_empty() {
        assert!(Instant::now() < deadline, "nobody ever ran the job");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(tracker.running.load(Ordering::SeqCst), 1);
    let (runner, first_token) = tracker.tokens.lock().unwrap()[0];
    drop(handles);
    let handles: Vec<_> = (0..3)
        .filter(|i| *i != runner)
        .map(|i| cluster.node(i).singleton_with("billing", ttl, job(tracker.clone(), i)))
        .collect();
    cluster.kill(runner).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let tokens = tracker.tokens.lock().unwrap().clone();
        if let Some((node, token)) = tokens.iter().rev().find(|(n, _)| *n != runner) {
            assert!(*token > first_token, "token did not rise: {tokens:?}");
            assert_ne!(*node, runner);
            break;
        }
        assert!(Instant::now() < deadline, "the job never moved: {tokens:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(tracker.max_running.load(Ordering::SeqCst), 1);
    let tokens: Vec<u64> = tracker.tokens.lock().unwrap().iter().map(|(_, t)| *t).collect();
    assert!(tokens.windows(2).all(|w| w[0] < w[1]), "{tokens:?}");
    drop(handles);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_handle_stops_the_job() {
    let cluster = Cluster::start(1).await;
    let tracker = Arc::new(Tracker::default());
    let handle = cluster.node(0).singleton_with("report", Duration::from_millis(600), job(tracker.clone(), 0));
    let deadline = Instant::now() + Duration::from_secs(5);
    while tracker.running.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(handle);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(tracker.running.load(Ordering::SeqCst), 0);
    cluster.shutdown().await;
}
