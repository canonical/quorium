
use quorium::{Ctx, Error, MigrationError, Quorium, State};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[state(key = String)]
pub struct Cart {
    items: Vec<String>,
    checked_out: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CartError {
    CheckedOut,
    Empty,
}

#[quorium::state]
impl Cart {
    pub fn add(&mut self, item: String) -> Result<usize, CartError> {
        if self.checked_out {
            return Err(CartError::CheckedOut);
        }
        self.items.push(item);
        Ok(self.items.len())
    }

    pub fn checkout(&mut self, ctx: &Ctx) -> Result<u64, CartError> {
        if self.items.is_empty() {
            return Err(CartError::Empty);
        }
        self.checked_out = true;
        Ok(ctx.index())
    }

    #[quorium(id = 7)]
    pub fn clear(&mut self) {
        self.items.clear();
        self.checked_out = false;
    }

    pub fn total(&self) -> usize {
        self.items.len()
    }

    pub fn contains(&self, item: String) -> bool {
        self.items.contains(&item)
    }

    pub fn first(&self) -> Result<String, CartError> {
        self.items.first().cloned().ok_or(CartError::Empty)
    }
}

#[derive(quorium::State, Serialize, Deserialize, Default, Debug)]
#[state(key = u64, name = "shop.Counter", version = 2, migrate = migrate_counter)]
pub struct Counter {
    value: u64,
}

fn migrate_counter(from: u32, bytes: &[u8]) -> Result<Counter, MigrationError> {
    match from {
        1 => postcard::from_bytes::<u32>(bytes)
            .map(|v| Counter { value: u64::from(v) })
            .map_err(|e| MigrationError::new(e.to_string())),
        other => Err(MigrationError::new(format!("unknown version {other}"))),
    }
}

#[quorium::state]
impl Counter {
    pub fn incr(&mut self, by: u64) -> u64 {
        self.value += by;
        self.value
    }
}

async fn dev_node() -> (Quorium, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let addr = quorium::testing::free_addrs(1)[0];
    let node = Quorium::builder()
        .data_dir(dir.path())
        .bind(addr)
        .single_node_dev_mode()
        .insecure_plaintext()
        .start()
        .await
        .unwrap();
    (node, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_and_reads_go_through_generated_handles() {
    let (node, _dir) = dev_node().await;
    let cart = node.entity::<Cart>("alice");
    assert_eq!(cart.add("apple".into()).await, Ok(1));
    assert_eq!(cart.add("pear".into()).await, Ok(2));
    assert_eq!(cart.total().await, Ok(2));
    assert_eq!(cart.contains("pear".into()).await, Ok(true));
    assert_eq!(cart.first().await, Ok("apple".to_string()));
    let index = cart.checkout().await.unwrap();
    assert!(index > 0);
    assert_eq!(cart.add("fig".into()).await, Err(Error::Rejected(CartError::CheckedOut)));
    assert!(cart.exists().await.unwrap());
    cart.clear().await.unwrap();
    assert_eq!(cart.get().await.unwrap(), Cart::default());
    assert_eq!(cart.first().await, Err(Error::Rejected(CartError::Empty)));
    cart.delete().await.unwrap();
    assert!(!cart.exists().await.unwrap());
    assert_eq!(node.entity::<Cart>("bob").total().await, Ok(0));
    let counter = node.entity::<Counter>(9u64);
    assert_eq!(counter.incr(5).await, Ok(5));
    assert_eq!(counter.incr(2).await, Ok(7));
    assert_eq!(counter.read(|c| c.value * 10).await, Ok(70));
    node.shutdown().await;
}

#[test]
fn state_metadata_comes_from_the_attribute() {
    assert_eq!(<Cart as State>::NAME, "Cart");
    assert_eq!(<Cart as State>::VERSION, 1);
    assert_eq!(<Counter as State>::NAME, "shop.Counter");
    assert_eq!(<Counter as State>::VERSION, 2);
}

#[test]
fn write_ids_are_name_hashes_unless_pinned() {
    let reg = quorium::__private::lookup("Cart").unwrap();
    assert!(reg.accepts(quorium::__private::method_id("add")));
    assert!(reg.accepts(quorium::__private::method_id("checkout")));
    assert!(reg.accepts(7));
    assert!(!reg.accepts(quorium::__private::method_id("clear")));
    assert!(!reg.accepts(quorium::__private::method_id("total")));
}

#[test]
fn migrations_upgrade_old_state() {
    let reg = quorium::__private::lookup("shop.Counter").unwrap();
    let old = postcard::to_allocvec(&41u32).unwrap();
    let migrated = (reg.migrate.unwrap())(1, &old).unwrap();
    let c: Counter = postcard::from_bytes(&migrated).unwrap();
    assert_eq!(c.value, 41);
    assert!((reg.migrate.unwrap())(9, &old).is_err());
}

#[test]
fn only_transient_errors_are_retryable() {
    assert!(Error::<CartError>::Unavailable.is_retryable());
    assert!(Error::<CartError>::Timeout.is_retryable());
    assert!(!Error::Rejected(CartError::Empty).is_retryable());
    assert!(!Error::<CartError>::Shutdown.is_retryable());
    assert!(!Error::<CartError>::Internal("x".into()).is_retryable());
}

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = ())]
pub struct Settings {
    mode: String,
}

#[quorium::state]
impl Settings {
    pub fn set(&mut self, mode: String) {
        self.mode = mode;
    }

    pub fn mode(&self) -> String {
        self.mode.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_state_has_one_instance_per_type() {
    let (node, _dir) = dev_node().await;
    node.global::<Settings>().set("fast".into()).await.unwrap();
    assert_eq!(node.global::<Settings>().mode().await, Ok("fast".to_string()));
    assert_eq!(node.entity::<Settings>(()).mode().await, Ok("fast".to_string()));
    assert_eq!(node.global::<Settings>().stale().mode().await, Ok("fast".to_string()));
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_reads_answer_without_a_quorum() {
    let mut cluster = quorium::testing::Cluster::start(3).await;
    cluster.wait_for_leader(std::time::Duration::from_secs(20)).await.unwrap();
    cluster.node(0).global::<Settings>().set("kept".into()).await.unwrap();
    let survivor = 2;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while cluster.node(survivor).global::<Settings>().stale().mode().await != Ok("kept".to_string()) {
        assert!(std::time::Instant::now() < deadline, "the write never reached node {survivor}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    cluster.kill(0).await;
    cluster.kill(1).await;
    let stale = cluster.node(survivor).global::<Settings>().stale();
    assert_eq!(stale.mode().await, Ok("kept".to_string()));
    cluster.shutdown().await;
}

#[test]
fn types_can_also_be_registered_explicitly() {
    let _builder = Quorium::builder().register::<Settings>().register::<Settings>();
    assert!(quorium_core::entity::registered_names().contains(&"Settings"));
}

#[cfg(feature = "uuid")]
#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = uuid::Uuid)]
pub struct Session {
    hits: u64,
}

#[cfg(feature = "uuid")]
#[quorium::state]
impl Session {
    pub fn hit(&mut self) -> u64 {
        self.hits += 1;
        self.hits
    }
}

#[cfg(feature = "uuid")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uuids_identify_entities() {
    let (node, _dir) = dev_node().await;
    let a = uuid::Uuid::from_u128(1);
    let b = uuid::Uuid::from_u128(2);
    assert_eq!(node.entity::<Session>(a).hit().await, Ok(1));
    assert_eq!(node.entity::<Session>(a).hit().await, Ok(2));
    assert_eq!(node.entity::<Session>(b).hit().await, Ok(1));
    node.shutdown().await;
}
