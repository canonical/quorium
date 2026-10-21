use std::sync::Arc;
use std::time::{Duration, Instant};

use quorium::{Client, Transport};
use quorium_core::kv::Reply;
use quorium_net::raft::{RaftService, Request, Response, ServiceFuture, serve};

struct SlowCommits {
    delay: Duration,
}

impl RaftService for SlowCommits {
    fn handle(&self, _from: u64, _shard: u64, request: Request) -> ServiceFuture<'_> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            match request {
                Request::Write(_) => Response::Written(Reply::Done),
                _ => Response::Failed("unexpected".into()),
            }
        })
    }
}

async fn slow_server(delay: Duration) -> quorium_net::transport::Listener {
    serve("127.0.0.1:0".parse().unwrap(), None, Arc::new(SlowCommits { delay })).await.unwrap()
}

#[tokio::test]
async fn writes_succeed_when_commits_take_longer_than_one_attempt() {
    let server = slow_server(Duration::from_millis(400)).await;
    let client = Client::new(vec![server.local_addr()], None)
        .with_transport(Transport::Tcp)
        .with_timeouts(Duration::from_secs(5), Duration::from_millis(150));
    let started = Instant::now();
    let reply = client.write("k", b"v".to_vec()).await;
    assert!(matches!(reply, Ok(Reply::Done)), "{reply:?}");
    assert!(started.elapsed() < Duration::from_secs(3), "{:?}", started.elapsed());
}

#[tokio::test]
async fn attempts_never_outlive_the_total_budget() {
    let server = slow_server(Duration::from_secs(30)).await;
    let client = Client::new(vec![server.local_addr()], None)
        .with_transport(Transport::Tcp)
        .with_timeouts(Duration::from_millis(1500), Duration::from_millis(100));
    let started = Instant::now();
    assert!(client.write("k", b"v".to_vec()).await.is_err());
    assert!(started.elapsed() < Duration::from_millis(2500), "{:?}", started.elapsed());
}

struct StaleFollower {
    leader: String,
}

impl RaftService for StaleFollower {
    fn handle(&self, _from: u64, _shard: u64, _request: Request) -> ServiceFuture<'_> {
        Box::pin(async move { Response::NotLeader { leader: Some(1), addr: Some(self.leader.clone()) } })
    }
}

#[tokio::test]
async fn a_timed_out_leader_hint_is_dropped_so_other_nodes_are_tried() {
    let dead = slow_server(Duration::from_secs(30)).await;
    let stale = serve(
        "127.0.0.1:0".parse().unwrap(),
        None,
        Arc::new(StaleFollower { leader: dead.local_addr().to_string() }),
    )
    .await
    .unwrap();
    let live = slow_server(Duration::ZERO).await;
    let client = Client::new(vec![stale.local_addr(), live.local_addr()], None)
        .with_transport(Transport::Tcp)
        .with_timeouts(Duration::from_secs(5), Duration::from_millis(100));
    let started = Instant::now();
    let reply = client.write("k", b"v".to_vec()).await;
    assert!(matches!(reply, Ok(Reply::Done)), "{reply:?}");
    assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());
}

#[tokio::test]
async fn redirects_to_a_node_already_being_waited_on_do_not_use_up_attempts() {
    let dead = slow_server(Duration::from_secs(30)).await;
    let mut stale = Vec::new();
    for _ in 0..3 {
        let follower = StaleFollower { leader: dead.local_addr().to_string() };
        stale.push(serve("127.0.0.1:0".parse().unwrap(), None, Arc::new(follower)).await.unwrap());
    }
    let live = slow_server(Duration::ZERO).await;
    let mut seeds: Vec<_> = stale.iter().map(|s| s.local_addr()).collect();
    seeds.push(live.local_addr());
    let client = Client::new(seeds, None)
        .with_transport(Transport::Tcp)
        .with_timeouts(Duration::from_secs(5), Duration::from_millis(100));
    let started = Instant::now();
    let reply = client.write("k", b"v".to_vec()).await;
    assert!(matches!(reply, Ok(Reply::Done)), "{reply:?}");
    assert!(started.elapsed() < Duration::from_millis(500), "{:?}", started.elapsed());
}

struct DiesAfter {
    answers: std::sync::atomic::AtomicUsize,
}

impl RaftService for DiesAfter {
    fn handle(&self, _from: u64, _shard: u64, _request: Request) -> ServiceFuture<'_> {
        Box::pin(async move {
            if self.answers.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                Response::Written(Reply::Done)
            } else {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Response::Failed("too late".into())
            }
        })
    }
}

#[tokio::test]
async fn a_client_that_saw_fast_replies_moves_on_from_a_silent_leader_quickly() {
    let leader = serve(
        "127.0.0.1:0".parse().unwrap(),
        None,
        Arc::new(DiesAfter { answers: std::sync::atomic::AtomicUsize::new(20) }),
    )
    .await
    .unwrap();
    let other = slow_server(Duration::ZERO).await;
    let client = Client::new(vec![leader.local_addr(), other.local_addr()], None)
        .with_transport(Transport::Tcp)
        .with_timeouts(Duration::from_secs(10), Duration::from_secs(1));
    for _ in 0..20 {
        client.write("k", b"v".to_vec()).await.unwrap();
    }
    let started = Instant::now();
    let reply = client.write("k", b"v".to_vec()).await;
    assert!(matches!(reply, Ok(Reply::Done)), "{reply:?}");
    assert!(started.elapsed() < Duration::from_millis(400), "{:?}", started.elapsed());
}
