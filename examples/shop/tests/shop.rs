use std::time::Duration;

use quorium::testing::Cluster;
use shop::{Cart, Inventory, Order, ShopError, checkout};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkout_reserves_stock_and_places_an_order() {
    let cluster = Cluster::start(3).await;
    cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    let node = cluster.node(0);
    node.entity::<Inventory>("apple").restock(10).await.unwrap();
    node.entity::<Inventory>("pear").restock(1).await.unwrap();
    let cart = node.entity::<Cart>("alice");
    cart.add("apple".into(), 3).await.unwrap();
    cart.add("pear".into(), 1).await.unwrap();
    let order_id = checkout(node, "alice").await.unwrap();
    let order = node.entity::<Order>(order_id).get().await.unwrap();
    assert_eq!(order.lines.get("apple"), Some(&3));
    assert_eq!(node.entity::<Inventory>("apple").available().await, Ok(7));
    assert_eq!(node.entity::<Inventory>("pear").available().await, Ok(0));
    assert_eq!(cart.add("apple".into(), 1).await, Err(quorium::Error::Rejected(ShopError::CheckedOut)));
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sold_out_checkout_releases_partial_reservations() {
    let cluster = Cluster::start(1).await;
    let node = cluster.node(0);
    node.entity::<Inventory>("apple").restock(5).await.unwrap();
    node.entity::<Inventory>("plum").restock(0).await.unwrap();
    let cart = node.entity::<Cart>("bob");
    cart.add("apple".into(), 2).await.unwrap();
    cart.add("plum".into(), 1).await.unwrap();
    assert_eq!(checkout(node, "bob").await, Err(ShopError::SoldOut("plum".into())));
    assert_eq!(node.entity::<Inventory>("apple").available().await, Ok(5));
    assert_eq!(cart.lines().await.unwrap().len(), 2);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_shoppers_never_oversell() {
    let cluster = Cluster::start(3).await;
    cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    cluster.node(0).entity::<Inventory>("gift").restock(5).await.unwrap();
    let shoppers = (0..12).map(|i| {
        let node = cluster.node(i % 3).clone();
        async move {
            let user = format!("user-{i}");
            node.entity::<Cart>(user.clone()).add("gift".into(), 1).await.unwrap();
            checkout(&node, &user).await
        }
    });
    let results = futures::future::join_all(shoppers).await;
    let placed = results.iter().filter(|r| r.is_ok()).count();
    let sold_out = results.iter().filter(|r| **r == Err(ShopError::SoldOut("gift".into()))).count();
    assert_eq!(placed, 5);
    assert_eq!(sold_out, 7);
    assert_eq!(cluster.node(1).entity::<Inventory>("gift").available().await, Ok(0));
    cluster.shutdown().await;
}
