use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use quorium::testing::Cluster;
use quorium::{Error, Escrow};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String, name = "field.Concert")]
pub struct Concert {
    title: String,
    seats: Escrow<u64>,
}

#[quorium::state]
impl Concert {
    pub fn open(&mut self, title: String, seats: u64) {
        self.title = title;
        self.seats.refill(seats);
    }

    pub fn capacity(&self) -> u64 {
        self.seats.estimate()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn escrow_fields_sell_through_the_entity() {
    let cluster = Cluster::start(1).await;
    let concert = cluster.node(0).entity::<Concert>("gig");
    concert.open("Gig".into(), 20).await.unwrap();
    assert_eq!(concert.capacity().await, Ok(20));
    let seats = concert.seats();
    let mut sold = 0;
    loop {
        match seats.try_take(1).await {
            Ok(()) => sold += 1,
            Err(Error::SoldOut) => break,
            Err(e) => panic!("{e:?}"),
        }
    }
    assert_eq!(sold, 20);
    let deadline = Instant::now() + Duration::from_secs(10);
    while seats.sold().await.unwrap() != 20 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(concert.capacity().await, Ok(0));
    assert_eq!(seats.local_remaining(), 0);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn escrow_fields_never_oversell_across_nodes() {
    let cluster = Cluster::start(3).await;
    cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    cluster.node(0).entity::<Concert>("festival").open("Fest".into(), 120).await.unwrap();
    let sold = Arc::new(AtomicU64::new(0));
    let sellers = (0..9).map(|i| {
        let seats = cluster.node(i % 3).entity::<Concert>("festival").seats();
        let sold = sold.clone();
        async move {
            loop {
                match seats.try_take(1).await {
                    Ok(()) => {
                        sold.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(Error::SoldOut) => return,
                    Err(e) => panic!("{e:?}"),
                }
            }
        }
    });
    futures::future::join_all(sellers).await;
    assert_eq!(sold.load(Ordering::SeqCst), 120);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsettled_field_sales_are_settled_after_a_restart() {
    let mut cluster = Cluster::start(1).await;
    let concert = cluster.node(0).entity::<Concert>("encore");
    concert.open("Encore".into(), 10).await.unwrap();
    concert.seats().try_take(4).await.unwrap();
    cluster.kill(0).await;
    cluster.restart(0).await;
    let seats = cluster.node(0).entity::<Concert>("encore").seats();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seats.sold().await.unwrap() != 4 {
        assert!(Instant::now() < deadline, "sold {}", seats.sold().await.unwrap());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.shutdown().await;
}
