#![cfg(feature = "quic")]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quorium_net::quic::{QuicClient, QuicServer};
use quorium_net::tls::{TlsConfig, TlsMaterial};
use quorium_net::transport::RpcError;

fn any_port() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn echo_server(tls: Option<TlsConfig>) -> QuicServer {
    QuicServer::bind(any_port(), tls, |body: Vec<u8>| async move {
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
    let client = QuicClient::new(server.local_addr(), None);
    assert_eq!(client.call(b"hi".to_vec(), Duration::from_secs(2)).await.unwrap(), b"echo:hi".to_vec());
}

#[tokio::test]
async fn large_bodies_cross_intact() {
    let server = echo_server(None).await;
    let client = QuicClient::new(server.local_addr(), None);
    let body: Vec<u8> = (0..3_000_000u32).map(|i| i as u8 | 1).collect();
    let reply = client.call(body.clone(), Duration::from_secs(10)).await.unwrap();
    assert_eq!(reply, [b"echo:".as_slice(), &body].concat());
}

#[tokio::test]
async fn concurrent_calls_share_one_connection() {
    let server = echo_server(None).await;
    let client = QuicClient::new(server.local_addr(), None);
    let start = Instant::now();
    let calls: Vec<_> = (0..200).map(|_| client.call(vec![b's', 100], Duration::from_secs(5))).collect();
    for r in futures::future::join_all(calls).await {
        assert_eq!(r.unwrap(), vec![b's', 100]);
    }
    assert!(start.elapsed() < Duration::from_millis(1500), "{:?}", start.elapsed());
    assert_eq!(client.connections_opened(), 1);
}

#[tokio::test]
async fn slow_call_times_out() {
    let server = echo_server(None).await;
    let client = QuicClient::new(server.local_addr(), None);
    assert_eq!(client.call(vec![b's', 200], Duration::from_millis(50)).await, Err(RpcError::Timeout));
}

#[tokio::test]
async fn handler_errors_propagate() {
    let server = echo_server(None).await;
    let client = QuicClient::new(server.local_addr(), None);
    assert_eq!(client.call(b"!".to_vec(), Duration::from_secs(2)).await, Err(RpcError::Remote("refused".into())));
}

#[tokio::test]
async fn unreachable_peer_fails_within_the_timeout() {
    let client = QuicClient::new("127.0.0.1:1".parse().unwrap(), None);
    let start = Instant::now();
    assert!(client.call(b"x".to_vec(), Duration::from_millis(500)).await.is_err());
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn client_reconnects_after_server_restart() {
    let server = echo_server(None).await;
    let addr = server.local_addr();
    let client = QuicClient::new(addr, None);
    client.call(b"a".to_vec(), Duration::from_secs(2)).await.unwrap();
    server.shutdown().await;
    assert!(client.call(b"b".to_vec(), Duration::from_millis(300)).await.is_err());
    let _server = QuicServer::bind(addr, None, |b: Vec<u8>| async move { Ok(b) }).await.unwrap();
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
    let server = echo_server(Some(ca.issue("node-1").unwrap().config(&ca))).await;
    let client_cfg = ca.issue("node-2").unwrap().config(&ca).with_server_name("node-1");
    let client = QuicClient::new(server.local_addr(), Some(client_cfg));
    assert_eq!(client.call(b"tls".to_vec(), Duration::from_secs(2)).await.unwrap(), b"echo:tls".to_vec());
}

#[tokio::test]
async fn mutual_tls_rejects_strangers() {
    let ca = TlsMaterial::generate_ca("quorium-test").unwrap();
    let other = TlsMaterial::generate_ca("someone-else").unwrap();
    let server = echo_server(Some(ca.issue("node-1").unwrap().config(&ca))).await;
    let stranger = other.issue("node-9").unwrap().config(&ca).with_server_name("node-1");
    let client = QuicClient::new(server.local_addr(), Some(stranger));
    assert!(client.call(b"x".to_vec(), Duration::from_secs(2)).await.is_err());
    let plain = QuicClient::new(server.local_addr(), None);
    assert!(plain.call(b"x".to_vec(), Duration::from_secs(2)).await.is_err());
}
