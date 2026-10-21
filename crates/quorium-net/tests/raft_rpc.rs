use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::error::RPCError;
use openraft::impls::{BasicNode, Vote};
use openraft::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use quorium_core::kv::{Command, KvOp, Reply};
use quorium_core::ladder::LogPosition;
use quorium_net::raft::{Message, ProdTypes, RaftService, Request, Response, TcpNetwork, serve};

#[derive(Default)]
struct Fake {
    seen: Mutex<Vec<(u64, String)>>,
}

impl RaftService for Fake {
    fn handle(
        &self,
        from: u64,
        _shard: u64,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + '_>> {
        Box::pin(async move {
            let name = format!("{request:?}").split(['(', ' ', '{']).next().unwrap_or_default().to_string();
            self.seen.lock().unwrap().push((from, name));
            match request {
                Request::Vote(r) | Request::PreVote(r) => Response::Vote(VoteResponse::new(r.vote, None, true)),
                Request::Append(_) => Response::Append(AppendEntriesResponse::Success),
                Request::Ping { .. } => Response::Pong {
                    id: 2,
                    logs: vec![(0, LogPosition::default())],
                    quiesced: vec![],
                    datacenter: "b".into(),
                    initialized: true,
                    methods: vec![],
                },
                Request::Write(cmd) => Response::Written(if cmd.seq == 1 { Reply::Done } else { Reply::Stale }),
                Request::Read { .. } => Response::NotLeader { leader: Some(3), addr: None },
                _ => Response::Failed("unsupported".into()),
            }
        })
    }
}

fn option() -> RPCOption {
    RPCOption::new(Duration::from_secs(2))
}

#[test]
fn messages_round_trip_through_the_envelope() {
    let m = Message {
        from: 7,
        shard: 3,
        request: Request::Ping {
            logs: vec![(3, LogPosition { term: 2, index: 9 })],
            quiesced: vec![3],
            datacenter: "a".into(),
            addr: "x".into(),
            initialized: false,
            methods: vec![("T".into(), 1)],
        },
    };
    let back = Message::decode(&m.encode()).unwrap();
    assert_eq!(format!("{back:?}"), format!("{m:?}"));
    assert!(Message::decode(b"garbage").is_err());
}

#[tokio::test]
async fn raft_rpcs_reach_the_service_with_the_sender_id() {
    let fake = Arc::new(Fake::default());
    let server = serve("127.0.0.1:0".parse().unwrap(), None, fake.clone()).await.unwrap();
    let mut network = TcpNetwork::new(1, None);
    let node = BasicNode::new(server.local_addr().to_string());
    let mut conn = network.new_client(2, &node).await;
    let vote = VoteRequest::<ProdTypes>::new(Vote::new(3, 1), None);
    assert!(conn.vote(vote.clone(), option()).await.unwrap().is_granted_to(&Vote::new(3, 1)));
    assert!(conn.pre_vote(vote, option()).await.unwrap().is_granted_to(&Vote::new(3, 1)));
    let append = AppendEntriesRequest::<ProdTypes> {
        vote: Vote::new_committed(3, 1),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    };
    assert!(matches!(conn.append_entries(append, option()).await.unwrap(), AppendEntriesResponse::Success));
    let seen = fake.seen.lock().unwrap().clone();
    assert_eq!(seen, vec![(1, "Vote".to_string()), (1, "PreVote".to_string()), (1, "Append".to_string())]);
}

#[tokio::test]
async fn peer_requests_cover_ping_and_forwarding() {
    let fake = Arc::new(Fake::default());
    let server = serve("127.0.0.1:0".parse().unwrap(), None, fake.clone()).await.unwrap();
    let network = TcpNetwork::new(1, None);
    let addr = server.local_addr().to_string();
    let ping = Request::Ping {
        logs: vec![],
        quiesced: vec![],
        datacenter: "a".into(),
        addr: "x".into(),
        initialized: false,
        methods: vec![],
    };
    let pong = network.request(2, &addr, ping, Duration::from_secs(2)).await.unwrap();
    assert!(matches!(pong, Response::Pong { id: 2, .. }));
    let ping = Request::Ping {
        logs: vec![],
        quiesced: vec![],
        datacenter: "a".into(),
        addr: "x".into(),
        initialized: false,
        methods: vec![],
    };
    let pong = network.request_addr(&addr, ping, Duration::from_secs(2)).await.unwrap();
    assert!(matches!(pong, Response::Pong { id: 2, .. }));
    let cmd = Command::new(9, 1, KvOp::Get { key: "k".into() });
    let w = network.request(2, &addr, Request::Write(cmd), Duration::from_secs(2)).await.unwrap();
    assert!(matches!(w, Response::Written(Reply::Done)));
    let r = network.request(2, &addr, Request::Read { key: "k".into() }, Duration::from_secs(2)).await.unwrap();
    assert!(matches!(r, Response::NotLeader { leader: Some(3), .. }));
}

#[tokio::test]
async fn unreachable_peer_reports_unreachable() {
    let mut network = TcpNetwork::new(1, None);
    let mut conn = network.new_client(2, &BasicNode::new("127.0.0.1:1")).await;
    let vote = VoteRequest::<ProdTypes>::new(Vote::new(3, 1), None);
    assert!(matches!(conn.vote(vote, option()).await, Err(RPCError::Unreachable(_))));
}

#[tokio::test]
async fn consensus_and_snapshot_traffic_use_separate_connections() {
    let fake = Arc::new(Fake::default());
    let server = serve("127.0.0.1:0".parse().unwrap(), None, fake.clone()).await.unwrap();
    let mut network = TcpNetwork::new(1, None);
    let node = BasicNode::new(server.local_addr().to_string());
    let _ = network.new_client(2, &node).await;
    let peer = network.peer(2).unwrap();
    assert!(!Arc::ptr_eq(&peer.consensus, &peer.snapshots));
    let again = network.new_client(2, &node).await;
    drop(again);
    assert!(Arc::ptr_eq(&network.peer(2).unwrap().consensus, &peer.consensus));
}

#[cfg(feature = "quic")]
#[tokio::test]
async fn raft_rpcs_also_travel_over_quic() {
    use quorium_net::raft::serve_with;
    use quorium_net::transport::Protocol;
    let fake = Arc::new(Fake::default());
    let server = serve_with(Protocol::Quic, "127.0.0.1:0".parse().unwrap(), None, fake.clone()).await.unwrap();
    let mut network = TcpNetwork::with_protocol(1, None, Protocol::Quic);
    let node = BasicNode::new(server.local_addr().to_string());
    let mut conn = network.new_client(2, &node).await;
    let vote = VoteRequest::<ProdTypes>::new(Vote::new(3, 1), None);
    assert!(conn.vote(vote, option()).await.unwrap().is_granted_to(&Vote::new(3, 1)));
    let addr = server.local_addr().to_string();
    let cmd = Command::new(9, 1, KvOp::Get { key: "k".into() });
    let w = network.request_addr(&addr, Request::Write(cmd), Duration::from_secs(2)).await.unwrap();
    assert!(matches!(w, Response::Written(Reply::Done)));
    assert_eq!(fake.seen.lock().unwrap().len(), 2);
    server.shutdown().await;
}

fn big_append(n: u64, bytes: usize) -> AppendEntriesRequest<ProdTypes> {
    use openraft::entry::RaftEntry;
    let entries = (1..=n)
        .map(|i| {
            let cmd = Command::new(1, i, KvOp::Put { key: "k".into(), value: vec![7; bytes] });
            <ProdTypes as openraft::RaftTypeConfig>::Entry::new_normal(
                quorium_core::types::LogId::new(quorium_core::types::CommittedLeaderId::new(1), i),
                cmd,
            )
        })
        .collect();
    AppendEntriesRequest { vote: Vote::new_committed(1, 1), prev_log_id: None, entries, leader_commit: None }
}

#[test]
fn appends_are_cut_to_a_byte_budget() {
    use quorium_net::raft::limit_append;
    let (cut, last) = limit_append(big_append(10, 100_000), 350_000);
    assert_eq!(cut.entries.len(), 3);
    assert_eq!(last.map(|l| l.index), Some(3));
    let (whole, last) = limit_append(big_append(10, 100_000), 10_000_000);
    assert_eq!(whole.entries.len(), 10);
    assert_eq!(last, None);
    let (one, last) = limit_append(big_append(2, 1_000_000), 10);
    assert_eq!(one.entries.len(), 1);
    assert_eq!(last.map(|l| l.index), Some(1));
    let (empty, last) = limit_append(big_append(0, 1), 10);
    assert!(empty.entries.is_empty());
    assert_eq!(last, None);
}

#[tokio::test]
async fn oversized_appends_are_sent_in_part_and_reported_as_partial() {
    let fake = Arc::new(Fake::default());
    let server = serve("127.0.0.1:0".parse().unwrap(), None, fake.clone()).await.unwrap();
    let mut network = TcpNetwork::new(1, None);
    let mut conn = network.new_client(2, &BasicNode::new(server.local_addr().to_string())).await;
    let reply = conn.append_entries(big_append(40, 200_000), option()).await.unwrap();
    match reply {
        AppendEntriesResponse::PartialSuccess(Some(last)) => assert!(last.index < 40, "{last:?}"),
        other => panic!("{other:?}"),
    }
    let reply = conn.append_entries(big_append(3, 10), option()).await.unwrap();
    assert!(matches!(reply, AppendEntriesResponse::Success));
}

struct Slow;

impl RaftService for Slow {
    fn handle(
        &self,
        _from: u64,
        _shard: u64,
        _request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Response::Append(AppendEntriesResponse::Success)
        })
    }
}

#[tokio::test]
async fn short_openraft_deadlines_do_not_fail_slower_round_trips() {
    let server = serve("127.0.0.1:0".parse().unwrap(), None, Arc::new(Slow)).await.unwrap();
    let mut network = TcpNetwork::new(1, None);
    let mut conn = network.new_client(2, &BasicNode::new(server.local_addr().to_string())).await;
    let append = AppendEntriesRequest::<ProdTypes> {
        vote: Vote::new_committed(3, 1),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    };
    let reply = conn.append_entries(append, RPCOption::new(Duration::from_millis(50))).await;
    assert!(matches!(reply, Ok(AppendEntriesResponse::Success)), "{reply:?}");
}

#[tokio::test]
async fn pings_warm_the_connection_that_consensus_traffic_uses() {
    let fake = Arc::new(Fake::default());
    let server = serve("127.0.0.1:0".parse().unwrap(), None, fake.clone()).await.unwrap();
    let addr = server.local_addr().to_string();
    let mut network = TcpNetwork::new(1, None);
    let ping = Request::Ping {
        logs: vec![],
        quiesced: vec![],
        datacenter: "a".into(),
        addr: "x".into(),
        initialized: false,
        methods: vec![],
    };
    network.request_addr(&addr, ping, Duration::from_secs(2)).await.unwrap();
    let mut conn = network.new_client(2, &BasicNode::new(addr.clone())).await;
    let vote = VoteRequest::<ProdTypes>::new(Vote::new(3, 1), None);
    conn.vote(vote, option()).await.unwrap();
    assert!(Arc::ptr_eq(&network.peer(2).unwrap().consensus, &network.peer_at(&addr).unwrap()));
    assert_eq!(network.peer(2).unwrap().consensus.connections_opened(), 1);
}
