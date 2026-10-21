#![allow(dead_code)]
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use quorium::Error;
use quorium::Escrow;
use quorium::Lease;
use quorium::StateBackend;
use quorium::TlsConfig;
use quorium::TlsMaterial;
use quorium::Transport;
use quorium::{Ctx, Discovery, MigrationError, Quorium};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AccountError {
    Insufficient,
}

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = u64, name = "bank.Account", version = 2, migrate = migrate_account)]
pub struct Account {
    balance: i64,
    currency: String,
}

#[derive(Deserialize)]
struct AccountV1 {
    balance: i64,
}

fn migrate_account(from: u32, bytes: &[u8]) -> Result<Account, MigrationError> {
    match from {
        1 => {
            let old: AccountV1 = postcard::from_bytes(bytes).map_err(|e| MigrationError::new(e.to_string()))?;
            Ok(Account { balance: old.balance, currency: "EUR".into() })
        }
        v => Err(MigrationError::new(format!("unknown Account version {v}"))),
    }
}

#[quorium::state]
impl Account {
    #[quorium(id = 1)]
    pub fn withdraw(&mut self, amount: i64) -> Result<i64, AccountError> {
        if amount > self.balance {
            return Err(AccountError::Insufficient);
        }
        self.balance -= amount;
        Ok(self.balance)
    }

    pub fn deposit(&mut self, ctx: &Ctx, amount: i64) -> u64 {
        self.balance += amount;
        ctx.index()
    }

    pub fn balance(&self) -> i64 {
        self.balance
    }
}

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = ())]
pub struct Settings {
    pub maintenance: bool,
}

#[quorium::state]
impl Settings {
    pub fn set_maintenance(&mut self, on: bool) {
        self.maintenance = on;
    }
}

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = (String, u64), locality = region_of)]
pub struct Profile {
    visits: u64,
}

fn region_of(key: &(String, u64)) -> String {
    key.0.clone()
}

#[quorium::state]
impl Profile {
    pub fn visit(&mut self) -> u64 {
        self.visits += 1;
        self.visits
    }
}

fn tls() -> std::io::Result<TlsConfig> {
    Ok(TlsConfig {
        ca_pem: std::fs::read_to_string("/etc/shop/tls/ca.pem")?,
        cert_pem: std::fs::read_to_string("/etc/shop/tls/node.pem")?,
        key_pem: std::fs::read_to_string("/etc/shop/tls/node.key")?,
        server_name: "shop-cluster".into(),
    })
}

fn tls_generated() -> std::io::Result<()> {
    let ca = TlsMaterial::generate_ca("shop-ca")?;
    let node = ca.issue("shop-cluster")?;
    std::fs::write("ca.pem", &ca.cert_pem)?;
    std::fs::write("node.pem", &node.cert_pem)?;
    std::fs::write("node.key", &node.key_pem)?;
    let config = node.config(&ca);
    let _: TlsConfig = config;
    Ok(())
}

async fn all(node: Quorium, tls: TlsConfig, seeds: Vec<SocketAddr>) -> Result<(), Box<dyn std::error::Error>> {
    let _ = Quorium::builder()
        .data_dir("/var/lib/shop")
        .bind("0.0.0.0:7000".parse()?)
        .advertise("10.0.0.5:7000".parse()?)
        .datacenter("eu-west-1a")
        .discovery(Discovery::Kubernetes { service: "shop".into(), namespace: "prod".into(), port: 7000 })
        .replicas(3)
        .tls(tls.clone())
        .heartbeat(Duration::from_millis(150))
        .transport(Transport::Tcp)
        .shards(16)
        .quiesce_after(Duration::from_secs(5))
        .auto_rebalance(Duration::from_secs(60), 1_000)
        .split_by_load(2_000)
        .metrics_bind("0.0.0.0:9100".parse()?)
        .replacement_grace(Duration::from_secs(600))
        .checkpoint_every(5_000)
        .register::<Concert>()
        .start()
        .await?;

    let concert = node.entity::<Concert>("gig");
    concert.open("Gig".into(), 500).await?;
    match concert.seats().try_take(2).await {
        Ok(()) => {}
        Err(Error::SoldOut) => {}
        Err(e) if e.is_retryable() => {}
        Err(e) => return Err(e.into()),
    }
    let tickets = node.escrow("concert-42");
    tickets.refill(1_000).await?;
    tickets.try_take(2).await?;
    let _ = (tickets.local_remaining(), tickets.estimate().await?, tickets.sold().await?);

    let account = node.entity::<Account>(7u64);
    match account.withdraw(50).await {
        Ok(left) => println!("{left}"),
        Err(Error::Rejected(AccountError::Insufficient)) => {}
        Err(e) => return Err(e.into()),
    }
    let _idx: u64 = account.deposit(10).await?;
    let _b = account.stale().balance().await?;
    let _all = account.get().await?;
    let _r = account.read(|a| a.balance * 2).await?;
    let _ = account.exists().await?;

    node.global::<Settings>().set_maintenance(true).await?;
    node.entity::<Profile>(("eu".to_string(), 7u64)).visit().await?;

    let mut watch = node.entity::<Account>(7u64).watch();
    while let Ok(acc) = watch.changed().await {
        println!("{}", acc.balance);
    }

    let _job = node.singleton("billing", |lease: Lease| async move {
        println!("billing with fencing token {}", lease.token);
    });
    let _job2 = node.singleton_with("report", Duration::from_secs(10), |_lease| async move {});

    let client = quorium::Client::new(seeds, Some(tls)).with_timeouts(Duration::from_secs(10), Duration::from_secs(1));
    client.write("greeting", b"hello".to_vec()).await?;
    let _v: Option<Vec<u8>> = client.read("greeting").await?;

    node.export(Path::new("/backups/shop.qbak")).await?;
    node.import(Path::new("/backups/shop.qbak")).await?;
    node.checkpoint().await?;
    let _m = node.membership();
    let _s = node.status();
    let _t = node.metrics_text();
    node.shutdown().await;
    Ok(())
}

async fn more(node: Quorium) -> Result<(), Box<dyn std::error::Error>> {
    let exporter = node.clone();
    let _backups = node.singleton("backup", move |_lease| {
        let node = exporter.clone();
        async move {
            loop {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
                let name = format!("/backups/{now}.qbak");
                if let Err(e) = node.export(Path::new(&name)).await {
                    eprintln!("backup failed: {e}");
                }
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
    });
    let shard = node.shard_of::<Profile>(&("eu".to_string(), 7));
    let sizes = node.group_sizes().await;
    let table = node.routing().await?;
    let _ = (shard, sizes, table);
    let _s = node.shard_status(0);
    let _st = node.stats();
    let pod_ip: std::net::IpAddr = std::env::var("POD_IP")?.parse()?;
    let _a = std::net::SocketAddr::new(pod_ip, 7000);
    let _c = quorium::Client::new(vec![], None).with_transport(quorium::Transport::Tcp);
    Ok(())
}

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Cart {
    items: Vec<String>,
}

#[quorium::state]
impl Cart {
    pub fn add(&mut self, item: String) -> usize {
        self.items.push(item);
        self.items.len()
    }
}

async fn run_billing(token: u64) {
    println!("billing with fencing token {token}");
}

async fn kubernetes() -> Result<Quorium, Box<dyn std::error::Error>> {
    let pod_ip: std::net::IpAddr = std::env::var("POD_IP")?.parse()?;
    let node = Quorium::builder()
        .data_dir("/var/lib/shop")
        .bind("0.0.0.0:7000".parse()?)
        .advertise(std::net::SocketAddr::new(pod_ip, 7000))
        .datacenter(&std::env::var("ZONE")?)
        .discovery(Discovery::Kubernetes { service: "shop".into(), namespace: "prod".into(), port: 7000 })
        .replicas(3)
        .tls(tls()?)
        .state_backend(StateBackend::Fjall)
        .replacement_grace(Duration::from_secs(300))
        .start()
        .await?;
    Ok(node)
}

async fn operations(
    node: Quorium,
    new_node: Quorium,
    seeds: Vec<SocketAddr>,
    tls: TlsConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    node.export(Path::new("/backups/2026-10-21.qbak")).await?;
    new_node.import(Path::new("/backups/2026-10-21.qbak")).await?;

    let status = node.status();
    println!("node {} sees leader {:?} in term {}", status.id, status.leader, status.term);
    let stats = node.stats();
    let membership = node.membership();
    println!("voters {:?}, learners {:?}", membership.voters, membership.learners);
    let _ = stats;

    let mut watch = node.entity::<Cart>("alice").watch();
    while let Ok(cart) = watch.changed().await {
        println!("{} items", cart.items.len());
    }

    let _job = node.singleton("billing", |lease: Lease| async move {
        run_billing(lease.token).await;
    });

    let concert = node.entity::<Concert>("gig");
    concert.seats().try_take(2).await?;

    let client = quorium::Client::new(seeds, Some(tls)).with_timeouts(Duration::from_secs(10), Duration::from_secs(1));
    let value: Option<Vec<u8>> = client.read("greeting").await?;
    let _ = value;
    Ok(())
}

fn tls_client(seeds: Vec<SocketAddr>) -> std::io::Result<quorium::Client> {
    let client = quorium::Client::new(seeds, Some(tls()?));
    Ok(client)
}
