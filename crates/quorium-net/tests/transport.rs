use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quorium_net::tls::{TlsConfig, TlsMaterial};
use quorium_net::transport::{Client, RpcError, Server};

fn any_port() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn echo_server(tls: Option<TlsConfig>) -> Server {
    Server::bind(any_port(), tls, |body: Vec<u8>| async move {
        match body.first() {
            Some(b'!') => Err("refused".to_string()),
            Some(b's') => {
                tokio::time::sleep(Duration::from_millis(u64::from(body[1]))).await;
                Ok(body)
            }
            _ => Ok([b"echo:".as_slice(), &body].concat()),
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn request_response_round_trip() {
    let server = echo_server(None).await;
    let client = Client::new(server.local_addr(), None);
    assert_eq!(client.call(b"hi".to_vec(), Duration::from_secs(2)).await.unwrap(), b"echo:hi".to_vec());
}

#[tokio::test]
async fn concurrent_calls_are_multiplexed_on_one_connection() {
    let server = echo_server(None).await;
    let client = Client::new(server.local_addr(), None);
    let start = Instant::now();
    let calls: Vec<_> = (0..50).map(|_| client.call(vec![b's', 100], Duration::from_secs(5))).collect();
    for r in futures::future::join_all(calls).await {
        assert_eq!(r.unwrap(), vec![b's', 100]);
    }
    assert!(start.elapsed() < Duration::from_millis(1500), "{:?}", start.elapsed());
    assert_eq!(client.connections_opened(), 1);
}

#[tokio::test]
async fn slow_call_times_out() {
    let server = echo_server(None).await;
    let client = Client::new(server.local_addr(), None);
    assert_eq!(client.call(vec![b's', 200], Duration::from_millis(50)).await, Err(RpcError::Timeout));
}

#[tokio::test]
async fn handler_errors_propagate() {
    let server = echo_server(None).await;
    let client = Client::new(server.local_addr(), None);
    assert_eq!(client.call(b"!".to_vec(), Duration::from_secs(2)).await, Err(RpcError::Remote("refused".into())));
}

#[tokio::test]
async fn unreachable_peer_fails_fast() {
    let client = Client::new("127.0.0.1:1".parse().unwrap(), None);
    assert!(matches!(client.call(b"x".to_vec(), Duration::from_secs(2)).await, Err(RpcError::Connect(_))));
}

#[tokio::test]
async fn client_reconnects_after_server_restart() {
    let server = echo_server(None).await;
    let addr = server.local_addr();
    let client = Client::new(addr, None);
    client.call(b"a".to_vec(), Duration::from_secs(2)).await.unwrap();
    drop(server);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(client.call(b"b".to_vec(), Duration::from_secs(1)).await.is_err());
    let _server = Server::bind(addr, None, |b: Vec<u8>| async move { Ok(b) }).await.unwrap();
    let mut ok = false;
    for _ in 0..20 {
        if client.call(b"c".to_vec(), Duration::from_secs(1)).await == Ok(b"c".to_vec()) {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok);
}

#[tokio::test]
async fn mutual_tls_accepts_cluster_members() {
    let ca = TlsMaterial::generate_ca("quorium-test").unwrap();
    let server_cfg = ca.issue("node-1").unwrap().config(&ca);
    let client_cfg = ca.issue("node-2").unwrap().config(&ca).with_server_name("node-1");
    let server = echo_server(Some(server_cfg)).await;
    let client = Client::new(server.local_addr(), Some(client_cfg));
    assert_eq!(client.call(b"tls".to_vec(), Duration::from_secs(2)).await.unwrap(), b"echo:tls".to_vec());
}

#[tokio::test]
async fn mutual_tls_rejects_strangers() {
    let ca = TlsMaterial::generate_ca("quorium-test").unwrap();
    let other = TlsMaterial::generate_ca("someone-else").unwrap();
    let server = echo_server(Some(ca.issue("node-1").unwrap().config(&ca))).await;
    let stranger = other.issue("node-9").unwrap().config(&ca).with_server_name("node-1");
    let client = Client::new(server.local_addr(), Some(stranger));
    assert!(client.call(b"x".to_vec(), Duration::from_secs(2)).await.is_err());
    let plain = Client::new(server.local_addr(), None);
    assert!(plain.call(b"x".to_vec(), Duration::from_secs(2)).await.is_err());
}

#[tokio::test]
async fn timed_out_calls_do_not_leak_pending_entries() {
    let server = echo_server(None).await;
    let client = Client::new(server.local_addr(), None);
    for _ in 0..10 {
        assert_eq!(client.call(vec![b's', 100], Duration::from_millis(5)).await, Err(RpcError::Timeout));
    }
    assert_eq!(client.pending_requests().await, 0);
}

#[tokio::test]
async fn bursts_are_coalesced_into_fewer_writes() {
    let server = echo_server(None).await;
    let client = Client::new(server.local_addr(), None);
    client.call(b"warm".to_vec(), Duration::from_secs(2)).await.unwrap();
    let before = client.write_batches();
    let calls: Vec<_> = (0..200)
        .map(|i: u32| client.call([b"x".as_slice(), &i.to_le_bytes()].concat(), Duration::from_secs(5)))
        .collect();
    for r in futures::future::join_all(calls).await {
        r.unwrap();
    }
    let batches = client.write_batches() - before;
    assert!(batches < 200, "{batches} writes for 200 requests");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_the_port_immediately() {
    for _ in 0..20 {
        let server = echo_server(None).await;
        let addr = server.local_addr();
        server.shutdown().await;
        let again = Server::bind(addr, None, |b: Vec<u8>| async move { Ok(b) }).await;
        assert!(again.is_ok(), "{:?}", again.err());
    }
}
