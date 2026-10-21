use std::time::Duration;

use quorium_sim::network::{Delivery, LinkConfig, SimNetwork};
use quorium_sim::rng::SimRng;

fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

fn net() -> SimNetwork {
    SimNetwork::new(LinkConfig { min_latency: ms(1), max_latency: ms(5), loss: 0.0, duplicate: 0.0 })
}

#[test]
fn healthy_link_delivers_once_within_latency() {
    let mut n = net();
    let mut r = SimRng::new(1);
    for _ in 0..1000 {
        let d = n.send(1, 2, &mut r);
        assert_eq!(d.len(), 1);
        assert!(d[0] >= ms(1) && d[0] <= ms(5));
    }
}

#[test]
fn symmetric_partition_blocks_both_ways() {
    let mut n = net();
    let mut r = SimRng::new(1);
    n.partition(&[1, 2], &[3, 4, 5]);
    assert!(n.send(1, 3, &mut r).is_empty());
    assert!(n.send(4, 2, &mut r).is_empty());
    assert_eq!(n.send(1, 2, &mut r).len(), 1);
    assert_eq!(n.send(3, 5, &mut r).len(), 1);
    n.heal();
    assert_eq!(n.send(1, 3, &mut r).len(), 1);
}

#[test]
fn one_way_cut_blocks_one_direction() {
    let mut n = net();
    let mut r = SimRng::new(1);
    n.cut(1, 2);
    assert!(n.send(1, 2, &mut r).is_empty());
    assert_eq!(n.send(2, 1, &mut r).len(), 1);
}

#[test]
fn isolated_node_reaches_nobody() {
    let mut n = net();
    let mut r = SimRng::new(1);
    n.isolate(3, &[1, 2, 3, 4]);
    for p in [1, 2, 4] {
        assert!(n.send(3, p, &mut r).is_empty());
        assert!(n.send(p, 3, &mut r).is_empty());
    }
}

#[test]
fn loss_and_duplication_follow_config() {
    let mut n = SimNetwork::new(LinkConfig { min_latency: ms(1), max_latency: ms(1), loss: 1.0, duplicate: 0.0 });
    let mut r = SimRng::new(1);
    assert!(n.send(1, 2, &mut r).is_empty());
    n.set_config(LinkConfig { min_latency: ms(1), max_latency: ms(1), loss: 0.0, duplicate: 1.0 });
    assert_eq!(n.send(1, 2, &mut r).len(), 2);
}

#[test]
fn latency_spike_adds_delay() {
    let mut n = net();
    let mut r = SimRng::new(1);
    n.spike(1, 2, ms(500));
    let d: Delivery = n.send(1, 2, &mut r);
    assert!(d[0] >= ms(501));
    n.heal();
    assert!(n.send(1, 2, &mut r)[0] <= ms(5));
}

#[test]
fn self_send_is_always_delivered() {
    let mut n = net();
    let mut r = SimRng::new(1);
    n.isolate(1, &[1, 2]);
    assert_eq!(n.send(1, 1, &mut r), vec![Duration::ZERO]);
}
