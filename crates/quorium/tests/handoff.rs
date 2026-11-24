use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use quorium::{Discovery, Quorium, StepDown};

const SHARDS: u64 = 4;

async fn start(dir: &std::path::Path, addr: SocketAddr, seeds: &[SocketAddr]) -> Quorium {
    Quorium::builder()
        .data_dir(dir)
        .bind(addr)
        .advertise(addr)
        .discovery(Discovery::Static(seeds.to_vec()))
        .replicas(seeds.len())
        .shards(SHARDS)
        .insecure_plaintext()
        .start()
        .await
        .unwrap()
}

struct Trio {
    _dirs: Vec<tempfile::TempDir>,
    nodes: Vec<Option<Quorium>>,
}

impl Trio {
    async fn start() -> Trio {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let addrs = quorium::testing::free_addrs(3);
        let mut nodes = Vec::new();
        for (i, dir) in dirs.iter().enumerate() {
            nodes.push(Some(start(dir.path(), addrs[i], &addrs).await));
        }
        let trio = Trio { _dirs: dirs, nodes };
        let deadline = Instant::now() + Duration::from_secs(15);
        while !trio.running().all(|n| n.is_stable(Duration::from_millis(300))) {
            assert!(Instant::now() < deadline, "the cluster never settled");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        trio
    }

    fn running(&self) -> impl Iterator<Item = &Quorium> {
        self.nodes.iter().flatten()
    }

    fn node(&self, i: usize) -> &Quorium {
        self.nodes[i].as_ref().unwrap()
    }

    fn led_by(&self, i: usize) -> Vec<u64> {
        let n = self.node(i);
        (0..SHARDS).filter(|s| n.shard_status(*s).is_leader).collect()
    }

    fn busiest(&self) -> usize {
        (0..3).max_by_key(|i| self.led_by(*i).len()).unwrap()
    }

    async fn shutdown(mut self) {
        for n in self.nodes.iter_mut() {
            if let Some(n) = n.take() {
                n.shutdown().await;
            }
        }
    }
}

struct Writers {
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<(usize, Duration, Vec<String>)>>,
}

fn spawn_writers(via: &Quorium, count: usize) -> Writers {
    let stop = Arc::new(AtomicBool::new(false));
    let tasks = (0..count)
        .map(|w| {
            let node = via.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let (mut done, mut slowest, mut errors) = (0, Duration::ZERO, Vec::new());
                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    match node.write(&format!("w{w}-{}", done % 16), b"v".to_vec()).await {
                        Ok(_) => done += 1,
                        Err(e) => errors.push(e.to_string()),
                    }
                    slowest = slowest.max(started.elapsed());
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                (done, slowest, errors)
            })
        })
        .collect();
    Writers { stop, tasks }
}

impl Writers {
    async fn finish(self) -> (usize, Duration, Vec<String>) {
        self.stop.store(true, Ordering::Relaxed);
        let mut total = (0, Duration::ZERO, Vec::new());
        for t in self.tasks {
            let (done, slowest, errors) = t.await.unwrap();
            total.0 += done;
            total.1 = total.1.max(slowest);
            total.2.extend(errors);
        }
        total
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stepping_down_under_writes_moves_every_shard_without_errors() {
    let trio = Trio::start().await;
    let leader = trio.busiest();
    let led = trio.led_by(leader);
    assert!(!led.is_empty());
    let other = (leader + 1) % 3;
    let writers = spawn_writers(trio.node(other), 8);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = Instant::now();
    let report =
        trio.node(leader).step_down(StepDown { deadline: Duration::from_secs(5), ..StepDown::default() }).await;
    let took = started.elapsed();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (done, slowest, errors) = writers.finish().await;
    let report = report.unwrap();
    assert_eq!(report.shards.iter().map(|h| h.shard).collect::<Vec<_>>(), led);
    let me = trio.node(leader).status().id;
    assert!(report.shards.iter().all(|h| h.new_leader.is_some_and(|l| l != me)), "{report:?}");
    assert!(trio.led_by(leader).is_empty());
    assert!(took < Duration::from_secs(5), "{took:?}");
    assert!(errors.is_empty(), "{errors:?}");
    assert!(done > 100, "only {done} writes");
    assert!(slowest < Duration::from_secs(1), "a write took {slowest:?}");
    trio.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survivors_keep_their_leaders_after_a_step_down_and_shutdown() {
    let mut trio = Trio::start().await;
    let leader = trio.busiest();
    let survivors: Vec<usize> = (0..3).filter(|i| *i != leader).collect();
    trio.node(leader).step_down(StepDown::default()).await.unwrap();
    let writers = spawn_writers(trio.node(survivors[0]), 4);
    let elections: u64 = survivors.iter().map(|i| trio.node(*i).stats().elections_started).sum();
    let node = trio.nodes[leader].take().unwrap();
    node.shutdown().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (done, _, errors) = writers.finish().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(done > 50, "only {done} writes");
    let after: u64 = survivors.iter().map(|i| trio.node(*i).stats().elections_started).sum();
    assert_eq!(after, elections, "the survivors started elections");
    for s in 0..SHARDS {
        let leaders: Vec<_> = survivors.iter().map(|i| trio.node(*i).shard_status(s).leader).collect();
        assert!(leaders[0].is_some() && leaders[0] == leaders[1], "shard {s}: {leaders:?}");
    }
    trio.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_preferred_target_receives_leadership() {
    let trio = Trio::start().await;
    let leader = trio.busiest();
    let preferred = (leader + 2) % 3;
    let prefer = trio.node(preferred).status().id;
    let report = trio.node(leader).step_down(StepDown { prefer: Some(prefer), ..StepDown::default() }).await.unwrap();
    assert!(!report.shards.is_empty());
    assert!(report.shards.iter().all(|h| h.new_leader == Some(prefer)), "{report:?}");
    trio.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stepping_down_without_leadership_is_a_no_op() {
    let trio = Trio::start().await;
    let leader = trio.busiest();
    trio.node(leader).step_down(StepDown::default()).await.unwrap();
    let again = trio.node(leader).step_down(StepDown::default()).await.unwrap();
    assert!(again.shards.is_empty());
    trio.shutdown().await;
}
