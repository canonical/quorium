use quorium::State;
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = (String, u64), name = "locality.Profile", locality = profile_region)]
pub struct Profile {
    visits: u64,
}

fn profile_region(key: &(String, u64)) -> String {
    key.0.clone()
}

#[quorium::state]
impl Profile {
    pub fn visit(&mut self) -> u64 {
        self.visits += 1;
        self.visits
    }
}

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = u64, name = "locality.Plain")]
pub struct Plain {
    n: u64,
}

#[test]
fn locality_comes_from_the_declared_function() {
    assert_eq!(Profile::locality(&("eu".to_string(), 7)), Some("eu".to_string()));
    assert_eq!(Plain::locality(&7), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_regions_entities_share_a_shard_and_still_work() {
    let cluster = quorium::testing::Cluster::start(1).await;
    let node = cluster.node(0);
    let shard = node.shard_of::<Profile>(&("eu".to_string(), 1));
    for user in 0..20u64 {
        assert_eq!(node.shard_of::<Profile>(&("eu".to_string(), user)), shard);
        assert_eq!(node.entity::<Profile>(("eu".to_string(), user)).visit().await, Ok(1));
    }
    cluster.shutdown().await;
}
