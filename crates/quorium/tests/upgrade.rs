use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quorium::{Discovery, Error, Quorium};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = u64, name = "upgrade.Meter")]
pub struct Meter {
    n: u64,
}

#[quorium::state]
impl Meter {
    pub fn tick(&mut self) -> u64 {
        self.n += 1;
        self.n
    }

    pub fn tock(&mut self) -> u64 {
        self.n += 10;
        self.n
    }
}

fn free_addrs(n: usize) -> Vec<SocketAddr> {
    quorium::testing::free_addrs(n)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_write_methods_wait_until_every_voter_runs_the_new_version() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let old_version: BTreeSet<(String, u32)> =
        [("upgrade.Meter".to_string(), quorium::__private::method_id("tick"))].into();
    let mut nodes = Vec::new();
    for i in 0..3 {
        let mut b = Quorium::builder()
            .data_dir(dirs[i].path())
            .bind(addrs[i])
            .advertise(addrs[i])
            .discovery(Discovery::Static(addrs.clone()))
            .replicas(3)
            .insecure_plaintext();
        if i == 2 {
            b = b.__test_advertise_methods(old_version.clone());
        }
        nodes.push(b.start().await.unwrap());
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes.iter().any(|n| n.status().leader.is_none()) {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let meter = nodes[0].entity::<Meter>(1u64);
    let deadline = Instant::now() + Duration::from_secs(10);
    while meter.tick().await.is_err() {
        assert!(Instant::now() < deadline, "old methods should work in a mixed cluster");
    }
    assert_eq!(meter.tock().await, Err(Error::Unavailable));
    let old = nodes.pop().unwrap();
    old.shutdown().await;
    let upgraded = Quorium::builder()
        .data_dir(dirs[2].path())
        .bind(addrs[2])
        .advertise(addrs[2])
        .discovery(Discovery::Static(addrs.clone()))
        .replicas(3)
        .insecure_plaintext()
        .start()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match meter.tock().await {
            Ok(n) => {
                assert!(n >= 11);
                break;
            }
            Err(Error::Unavailable) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    upgraded.shutdown().await;
    for n in nodes {
        n.shutdown().await;
    }
}
