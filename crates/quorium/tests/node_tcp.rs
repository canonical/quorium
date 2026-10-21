use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quorium::{Discovery, Quorium, StartError};

fn free_addrs(n: usize) -> Vec<SocketAddr> {
    quorium::testing::free_addrs(n)
}

async fn start(dir: &std::path::Path, addr: SocketAddr, seeds: &[SocketAddr], dc: &str) -> Quorium {
    Quorium::builder()
        .data_dir(dir)
        .bind(addr)
        .advertise(addr)
        .datacenter(dc)
        .discovery(Discovery::Static(seeds.to_vec()))
        .replicas(seeds.len())
        .insecure_plaintext()
        .start()
        .await
        .unwrap()
}

async fn wait_for_leader(nodes: &[&Quorium], limit: Duration) -> Option<u64> {
    wait_for_leader_other_than(nodes, None, limit).await
}

async fn wait_for_leader_other_than(nodes: &[&Quorium], old: Option<u64>, limit: Duration) -> Option<u64> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        for n in nodes {
            if let Some(l) = n.status().leader.filter(|l| Some(*l) != old)
                && nodes.iter().all(|m| m.status().leader == Some(l))
            {
                return Some(l);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_form_a_cluster_and_replicate() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (i, dc) in ["a", "b", "c"].iter().enumerate() {
        nodes.push(start(dirs[i].path(), addrs[i], &addrs, dc).await);
    }
    let refs: Vec<&Quorium> = nodes.iter().collect();
    assert!(wait_for_leader(&refs, Duration::from_secs(10)).await.is_some());
    nodes[1].write("a", b"1".to_vec()).await.unwrap();
    nodes[2].write("b", b"2".to_vec()).await.unwrap();
    assert_eq!(nodes[0].read("a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(nodes[2].read("b").await.unwrap(), Some(b"2".to_vec()));
    for n in nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_loss_fails_over_and_restart_recovers_from_disk() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (i, dc) in ["a", "b", "c"].iter().enumerate() {
        nodes.push(Some(start(dirs[i].path(), addrs[i], &addrs, dc).await));
    }
    let refs: Vec<&Quorium> = nodes.iter().flatten().collect();
    let leader = wait_for_leader(&refs, Duration::from_secs(10)).await.unwrap();
    refs[0].write("k", b"before".to_vec()).await.unwrap();
    let idx = nodes.iter().position(|n| n.as_ref().is_some_and(|n| n.status().id == leader)).unwrap();
    nodes[idx].take().unwrap().shutdown().await;
    let survivors: Vec<&Quorium> = nodes.iter().flatten().collect();
    let started = Instant::now();
    let new_leader = wait_for_leader_other_than(&survivors, Some(leader), Duration::from_secs(10)).await.unwrap();
    assert_ne!(new_leader, leader);
    assert!(started.elapsed() < Duration::from_secs(3), "failover took {:?}", started.elapsed());
    survivors[0].write("k", b"after".to_vec()).await.unwrap();
    let dc = ["a", "b", "c"][idx];
    nodes[idx] = Some(start(dirs[idx].path(), addrs[idx], &addrs, dc).await);
    let restarted = nodes[idx].as_ref().unwrap();
    assert_eq!(restarted.status().id, leader);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if restarted.read("k").await.ok().flatten() == Some(b"after".to_vec()) {
            break;
        }
        assert!(Instant::now() < deadline, "restarted node never caught up");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for n in nodes.into_iter().flatten() {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_waits_for_the_expected_seeds() {
    let dir = tempfile::tempdir().unwrap();
    let addrs = free_addrs(3);
    let lone = start(dir.path(), addrs[0], &addrs, "a").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(lone.status().leader, None);
    assert!(lone.write("x", vec![]).await.is_err());
    lone.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_dev_mode_must_be_requested() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addrs(1)[0];
    let node = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .advertise(addr)
        .single_node_dev_mode()
        .insecure_plaintext()
        .start()
        .await
        .unwrap();
    let refs = [&node];
    assert_eq!(wait_for_leader(&refs, Duration::from_secs(5)).await, Some(node.status().id));
    node.write("dev", b"ok".to_vec()).await.unwrap();
    assert_eq!(node.read("dev").await.unwrap(), Some(b"ok".to_vec()));
    node.shutdown().await;
}

#[tokio::test]
async fn plaintext_requires_explicit_opt_in() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addrs(1)[0];
    let err = Quorium::builder().data_dir(dir.path()).bind(addr).single_node_dev_mode().start().await.err();
    assert!(matches!(err, Some(StartError::TlsRequired)));
}

#[tokio::test]
async fn replica_count_must_be_three_or_five() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addrs(1)[0];
    let err = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .discovery(Discovery::Static(vec![addr]))
        .replicas(4)
        .insecure_plaintext()
        .start()
        .await
        .err();
    assert!(matches!(err, Some(StartError::InvalidReplicas(4))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_identity_is_stable_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addrs(1)[0];
    let build = || {
        Quorium::builder().data_dir(dir.path()).bind(addr).advertise(addr).single_node_dev_mode().insecure_plaintext()
    };
    let first = build().start().await.unwrap();
    let id = first.status().id;
    first.shutdown().await;
    let second = build().start().await.unwrap();
    assert_eq!(second.status().id, id);
    second.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_client_routes_to_the_leader_and_survives_failover() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (i, dc) in ["a", "b", "c"].iter().enumerate() {
        nodes.push(Some(start(dirs[i].path(), addrs[i], &addrs, dc).await));
    }
    let refs: Vec<&Quorium> = nodes.iter().flatten().collect();
    let leader = wait_for_leader(&refs, Duration::from_secs(10)).await.unwrap();
    let client = quorium::Client::new(addrs.clone(), None);
    client.write("c", b"1".to_vec()).await.unwrap();
    assert_eq!(client.read("c").await.unwrap(), Some(b"1".to_vec()));
    let idx = nodes.iter().position(|n| n.as_ref().is_some_and(|n| n.status().id == leader)).unwrap();
    nodes[idx].take().unwrap().shutdown().await;
    client.write("c", b"2".to_vec()).await.unwrap();
    assert_eq!(client.read("c").await.unwrap(), Some(b"2".to_vec()));
    for n in nodes.into_iter().flatten() {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_writes_state_to_the_database_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addrs(1)[0];
    let build = || {
        Quorium::builder().data_dir(dir.path()).bind(addr).advertise(addr).single_node_dev_mode().insecure_plaintext()
    };
    let first = build().start().await.unwrap();
    assert!(wait_for_leader(&[&first], Duration::from_secs(5)).await.is_some());
    first.write("kept", b"v".to_vec()).await.unwrap();
    first.checkpoint().await.unwrap();
    first.shutdown().await;
    assert!(dir.path().join("state.redb").exists());
    assert!(!dir.path().join("state").join("shard-0").join("snapshot").exists());
    let second = build().start().await.unwrap();
    assert!(wait_for_leader(&[&second], Duration::from_secs(5)).await.is_some());
    assert_eq!(second.read("kept").await.unwrap(), Some(b"v".to_vec()));
    second.shutdown().await;
}

#[cfg(feature = "mdns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_find_each_other_through_mdns() {
    let service = format!("qnode{}", std::process::id() % 100_000);
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (dir, addr) in dirs.iter().zip(&addrs) {
        let node = Quorium::builder()
            .data_dir(dir.path())
            .bind(*addr)
            .advertise(*addr)
            .discovery(Discovery::Mdns { service: service.clone() })
            .replicas(3)
            .insecure_plaintext()
            .start()
            .await
            .unwrap();
        nodes.push(node);
    }
    let refs: Vec<&Quorium> = nodes.iter().collect();
    assert!(wait_for_leader(&refs, Duration::from_secs(20)).await.is_some());
    nodes[0].write("mdns", b"found".to_vec()).await.unwrap();
    assert_eq!(nodes[2].read("mdns").await.unwrap(), Some(b"found".to_vec()));
    for node in nodes {
        node.shutdown().await;
    }
}

#[cfg(feature = "quic")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_runs_over_quic_with_mutual_tls() {
    let ca = quorium::TlsMaterial::generate_ca("quic-test").unwrap();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_udp_addrs(3);
    let mut nodes = Vec::new();
    for (dir, addr) in dirs.iter().zip(&addrs) {
        let node = Quorium::builder()
            .data_dir(dir.path())
            .bind(*addr)
            .advertise(*addr)
            .discovery(Discovery::Static(addrs.clone()))
            .replicas(3)
            .transport(quorium::Transport::Quic)
            .tls(ca.issue("quic-cluster").unwrap().config(&ca))
            .start()
            .await
            .unwrap();
        nodes.push(node);
    }
    let refs: Vec<&Quorium> = nodes.iter().collect();
    assert!(wait_for_leader(&refs, Duration::from_secs(20)).await.is_some());
    nodes[0].write("quic", b"yes".to_vec()).await.unwrap();
    assert_eq!(nodes[2].read("quic").await.unwrap(), Some(b"yes".to_vec()));
    for node in nodes {
        node.shutdown().await;
    }
}

#[cfg(feature = "quic")]
fn free_udp_addrs(n: usize) -> Vec<SocketAddr> {
    let sockets: Vec<std::net::UdpSocket> = (0..n).map(|_| std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).collect();
    sockets.iter().map(|s| s.local_addr().unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_forms_over_mutual_tls_with_one_cluster_name() {
    let ca = quorium::TlsMaterial::generate_ca("tls-test").unwrap();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (dir, addr) in dirs.iter().zip(&addrs) {
        let node = Quorium::builder()
            .data_dir(dir.path())
            .bind(*addr)
            .advertise(*addr)
            .discovery(Discovery::Static(addrs.clone()))
            .replicas(3)
            .tls(ca.issue("shop-cluster").unwrap().config(&ca))
            .start()
            .await
            .unwrap();
        nodes.push(node);
    }
    let refs: Vec<&Quorium> = nodes.iter().collect();
    assert!(wait_for_leader(&refs, Duration::from_secs(20)).await.is_some());
    nodes[0].write("tls", b"yes".to_vec()).await.unwrap();
    assert_eq!(nodes[2].read("tls").await.unwrap(), Some(b"yes".to_vec()));
    let stranger_ca = quorium::TlsMaterial::generate_ca("other").unwrap();
    let client = quorium::Client::new(addrs.clone(), Some(stranger_ca.issue("shop-cluster").unwrap().config(&ca)))
        .with_timeouts(Duration::from_secs(2), Duration::from_millis(500));
    assert!(client.read("tls").await.is_err());
    let member = quorium::Client::new(addrs.clone(), Some(ca.issue("shop-cluster").unwrap().config(&ca)));
    assert_eq!(member.read("tls").await.unwrap(), Some(b"yes".to_vec()));
    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_reads_use_the_lease_instead_of_a_quorum_round() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (dir, addr) in dirs.iter().zip(&addrs) {
        nodes.push(start(dir.path(), *addr, &addrs, "dc").await);
    }
    let refs: Vec<&Quorium> = nodes.iter().collect();
    let leader_id = wait_for_leader(&refs, Duration::from_secs(20)).await.unwrap();
    let leader = nodes.iter().find(|n| n.status().id == leader_id).unwrap();
    leader.write("lease", b"v".to_vec()).await.unwrap();
    let before = leader.stats().appends_sent;
    let started = Instant::now();
    for _ in 0..200 {
        assert_eq!(leader.read("lease").await.unwrap(), Some(b"v".to_vec()));
    }
    let heartbeat_rounds = started.elapsed().as_millis() as u64 / 50 + 2;
    let sent = leader.stats().appends_sent - before;
    assert!(sent <= heartbeat_rounds * 2 + 10, "{sent} appends for 200 reads in {:?}", started.elapsed());
    for node in nodes {
        node.shutdown().await;
    }
}

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fjall_backend_checkpoints_and_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addrs(1)[0];
    let build = || {
        Quorium::builder()
            .data_dir(dir.path())
            .bind(addr)
            .advertise(addr)
            .single_node_dev_mode()
            .state_backend(quorium::StateBackend::Fjall)
            .insecure_plaintext()
    };
    let first = build().start().await.unwrap();
    assert!(wait_for_leader(&[&first], Duration::from_secs(5)).await.is_some());
    first.write("kept", b"f".to_vec()).await.unwrap();
    first.checkpoint().await.unwrap();
    first.shutdown().await;
    assert!(dir.path().join("state.fjall").exists());
    assert!(!dir.path().join("state.redb").exists());
    let second = build().start().await.unwrap();
    assert!(wait_for_leader(&[&second], Duration::from_secs(5)).await.is_some());
    assert_eq!(second.read("kept").await.unwrap(), Some(b"f".to_vec()));
    second.shutdown().await;
    let switched = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .advertise(addr)
        .single_node_dev_mode()
        .insecure_plaintext()
        .start()
        .await;
    assert!(switched.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bursts_of_large_writes_replicate_in_bounded_messages() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (dir, addr) in dirs.iter().zip(&addrs) {
        nodes.push(start(dir.path(), *addr, &addrs, "dc").await);
    }
    let refs: Vec<&Quorium> = nodes.iter().collect();
    wait_for_leader(&refs, Duration::from_secs(20)).await.unwrap();
    let keys: Vec<String> = (0..24u8).map(|i| format!("big{i}")).collect();
    let writes: Vec<_> = keys.iter().zip(0u8..).map(|(k, i)| nodes[0].write(k, vec![i; 800_000])).collect();
    for result in futures::future::join_all(writes).await {
        result.unwrap();
    }
    for i in 0..24u8 {
        assert_eq!(nodes[2].read(&format!("big{i}")).await.unwrap(), Some(vec![i; 800_000]));
    }
    for node in nodes {
        node.shutdown().await;
    }
}

#[cfg(feature = "quic")]
#[test]
fn quic_is_the_default_transport() {
    assert_eq!(quorium::Transport::default(), quorium::Transport::Quic);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_whose_lease_lapsed_accepts_writes_instead_of_redirecting() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let mut nodes = Vec::new();
    for (dir, addr) in dirs.iter().zip(&addrs) {
        nodes.push(
            Quorium::builder()
                .data_dir(dir.path())
                .bind(*addr)
                .advertise(*addr)
                .discovery(Discovery::Static(addrs.clone()))
                .replicas(3)
                .quiesce_after(Duration::from_millis(100))
                .insecure_plaintext()
                .start()
                .await
                .unwrap(),
        );
    }
    let refs: Vec<&Quorium> = nodes.iter().collect();
    let leader_id = wait_for_leader(&refs, Duration::from_secs(20)).await.unwrap();
    nodes[0].write("lease", b"1".to_vec()).await.unwrap();
    let leader = nodes.iter().position(|n| n.status().id == leader_id).unwrap();
    let network = quorium_net::raft::TcpNetwork::with_protocol(0, None, quorium::Transport::default());
    for round in 0..3 {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let cmd = quorium_core::kv::Command::new(
            77,
            round + 1,
            quorium_core::kv::KvOp::Put { key: "lease".into(), value: vec![2] },
        );
        let reply = network
            .request_addr(&addrs[leader].to_string(), quorium_net::raft::Request::Write(cmd), Duration::from_secs(3))
            .await
            .unwrap();
        assert!(matches!(reply, quorium_net::raft::Response::Written(_)), "round {round}: {reply:?}");
    }
    for node in nodes {
        node.shutdown().await;
    }
}
