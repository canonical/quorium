use std::time::Duration;

use quorium_core::backoff::{Backoff, Throttle};

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

#[test]
fn backoff_doubles_up_to_its_maximum() {
    let mut b = Backoff::new(ms(10), ms(70));
    let delays: Vec<Duration> = (0..6).map(|_| b.next_delay()).collect();
    assert_eq!(delays, vec![ms(10), ms(20), ms(40), ms(70), ms(70), ms(70)]);
}

#[test]
fn backoff_never_starts_above_its_maximum() {
    let mut b = Backoff::new(ms(100), ms(30));
    assert_eq!(b.next_delay(), ms(30));
    assert_eq!(b.next_delay(), ms(30));
}

#[test]
fn throttle_permits_once_per_interval() {
    let t = Throttle::new(ms(25));
    assert!(t.permit(ms(0)));
    assert!(!t.permit(ms(0)));
    assert!(!t.permit(ms(24)));
    assert!(t.permit(ms(25)));
    assert!(!t.permit(ms(30)));
    assert!(t.permit(ms(60)));
}

#[test]
fn throttle_gives_concurrent_callers_one_permit() {
    let t = std::sync::Arc::new(Throttle::new(ms(1000)));
    let granted: usize = (0..8)
        .map(|_| {
            let t = t.clone();
            std::thread::spawn(move || (0..1000).filter(|_| t.permit(ms(5))).count())
        })
        .map(|h| h.join().unwrap())
        .sum();
    assert_eq!(granted, 1);
}
