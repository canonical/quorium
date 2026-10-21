use std::net::SocketAddr;

use quorium::Discovery;

#[tokio::test]
async fn static_discovery_returns_its_list() {
    let a: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    assert_eq!(Discovery::Static(vec![a]).resolve().await, vec![a]);
}

#[tokio::test]
async fn dns_discovery_resolves_names_with_the_port() {
    let found = Discovery::Dns { name: "localhost".into(), port: 7100 }.resolve().await;
    assert!(found.contains(&"127.0.0.1:7100".parse().unwrap()), "{found:?}");
    assert!(found.iter().all(|a| a.port() == 7100));
}

#[tokio::test]
async fn unresolvable_names_yield_nothing_rather_than_failing() {
    let found = Discovery::Dns { name: "no-such-host.invalid".into(), port: 1 }.resolve().await;
    assert!(found.is_empty());
}

#[test]
fn kubernetes_discovery_targets_the_headless_service() {
    let d = Discovery::Kubernetes { service: "shop".into(), namespace: "prod".into(), port: 7000 };
    assert_eq!(d.dns_name().as_deref(), Some("shop.prod.svc.cluster.local"));
}

#[tokio::test]
async fn resolved_addresses_are_sorted_and_unique() {
    let a: SocketAddr = "127.0.0.2:1".parse().unwrap();
    let b: SocketAddr = "127.0.0.1:1".parse().unwrap();
    assert_eq!(Discovery::Static(vec![a, b, a]).resolve().await, vec![b, a]);
}

#[cfg(feature = "mdns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mdns_discovery_finds_announced_nodes() {
    let service = format!("qtest{}", std::process::id());
    let a: SocketAddr = "127.0.0.1:7301".parse().unwrap();
    let b: SocketAddr = "127.0.0.1:7302".parse().unwrap();
    let _first = quorium::discovery::announce_mdns(&service, 1, a).unwrap();
    let _second = quorium::discovery::announce_mdns(&service, 2, b).unwrap();
    let discovery = Discovery::Mdns { service: service.clone() };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut found = Vec::new();
    while std::time::Instant::now() < deadline {
        found = discovery.resolve().await;
        if found.contains(&a) && found.contains(&b) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(found.contains(&a) && found.contains(&b), "{found:?}");
    let other = Discovery::Mdns { service: format!("{service}x") };
    assert!(other.resolve().await.is_empty());
}
