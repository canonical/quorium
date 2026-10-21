use std::time::Duration;

use quorium::testing::SimCluster;
use quorium::{Ctx, Error};
use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Tally {
    total: u64,
    stamps: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TallyError {
    TooBig,
}

#[quorium::state]
impl Tally {
    pub fn add(&mut self, ctx: &Ctx, n: u64) -> Result<u64, TallyError> {
        if n > 100 {
            return Err(TallyError::TooBig);
        }
        self.total += n;
        self.stamps.push(ctx.now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64));
        Ok(self.total)
    }

    pub fn total(&self) -> u64 {
        self.total
    }
}

#[tokio::test]
async fn entities_run_on_the_simulator_through_failover() {
    let sim = SimCluster::start(42, 3);
    let tally = sim.entity::<Tally>("t");
    assert_eq!(tally.add(2).await, Ok(2));
    assert_eq!(tally.add(500).await, Err(Error::Rejected(TallyError::TooBig)));
    assert_eq!(tally.total().await, Ok(2));
    let leader = sim.leader().unwrap();
    sim.crash(leader);
    assert_eq!(tally.add(3).await, Ok(5));
    assert_ne!(sim.leader(), Some(leader));
    sim.restart(leader);
    sim.run_for(Duration::from_secs(1));
    assert!(tally.exists().await.unwrap());
    tally.delete().await.unwrap();
    assert_eq!(tally.total().await, Ok(0));
    assert!(sim.violations().is_empty(), "{:?}", sim.violations());
}

#[tokio::test]
async fn a_minority_partition_cannot_write_and_heals() {
    let sim = SimCluster::start(7, 3);
    let tally = sim.entity::<Tally>("p");
    tally.add(1).await.unwrap();
    let leader = sim.leader().unwrap();
    let others: Vec<u64> = sim.nodes().into_iter().filter(|n| *n != leader).collect();
    sim.partition(&[leader], &others);
    sim.run_for(Duration::from_secs(2));
    assert_ne!(sim.leader(), Some(leader));
    assert_eq!(tally.add(1).await, Ok(2));
    sim.heal();
    sim.run_for(Duration::from_secs(1));
    assert_eq!(tally.total().await, Ok(2));
}

fn history(seed: u64) -> (Vec<u64>, Duration) {
    let sim = SimCluster::start(seed, 3);
    let tally = sim.entity::<Tally>("d");
    futures::executor::block_on(async {
        for i in 0..5 {
            tally.add(i).await.unwrap();
            if i == 2 {
                sim.crash(sim.leader().unwrap());
            }
        }
    });
    let stamps = futures::executor::block_on(tally.read(|t| t.stamps.clone())).unwrap();
    (stamps, sim.now())
}

#[test]
fn the_same_seed_replays_the_same_history() {
    assert_eq!(history(3), history(3));
    assert_ne!(history(3).1, history(4).1);
}

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Gig {
    seats: quorium::Escrow<u64>,
}

#[quorium::state]
impl Gig {
    pub fn open(&mut self, seats: u64) {
        self.seats.refill(seats);
    }
}

#[tokio::test]
async fn escrow_sells_through_consensus_in_simulation() {
    let sim = SimCluster::start(9, 3);
    let gig = sim.entity::<Gig>("g");
    gig.open(3).await.unwrap();
    gig.seats().try_take(2).await.unwrap();
    assert_eq!(gig.seats().try_take(2).await, Err(Error::SoldOut));
    gig.seats().try_take(1).await.unwrap();
    assert_eq!(gig.seats().sold().await, Ok(3));
    let named = sim.escrow("tickets");
    named.refill(5).await.unwrap();
    sim.crash(sim.leader().unwrap());
    named.try_take(5).await.unwrap();
    assert_eq!(named.try_take(1).await, Err(Error::SoldOut));
    assert_eq!(named.estimate().await, Ok(0));
    assert_eq!(named.local_remaining(), 0);
}
