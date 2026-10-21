use quorium_sim::rng::SimRng;

#[test]
fn same_seed_same_sequence() {
    let mut a = SimRng::new(42);
    let mut b = SimRng::new(42);
    let xs: Vec<u64> = (0..100).map(|_| a.next_u64()).collect();
    let ys: Vec<u64> = (0..100).map(|_| b.next_u64()).collect();
    assert_eq!(xs, ys);
}

#[test]
fn different_seeds_diverge() {
    let mut a = SimRng::new(1);
    let mut b = SimRng::new(2);
    assert_ne!(a.next_u64(), b.next_u64());
}

#[test]
fn range_stays_in_bounds() {
    let mut r = SimRng::new(7);
    for _ in 0..10_000 {
        let v = r.range(10, 20);
        assert!((10..20).contains(&v));
    }
    assert_eq!(r.range(5, 5), 5);
}

#[test]
fn chance_extremes() {
    let mut r = SimRng::new(9);
    assert!((0..1000).all(|_| !r.chance(0.0)));
    assert!((0..1000).all(|_| r.chance(1.0)));
}

#[test]
fn chance_is_roughly_calibrated() {
    let mut r = SimRng::new(11);
    let hits = (0..100_000).filter(|_| r.chance(0.25)).count();
    assert!((23_000..27_000).contains(&hits), "{hits}");
}

#[test]
fn fork_is_independent_and_deterministic() {
    let mut a = SimRng::new(5);
    let mut b = SimRng::new(5);
    let mut fa = a.fork();
    let mut fb = b.fork();
    assert_eq!(fa.next_u64(), fb.next_u64());
    assert_eq!(a.next_u64(), b.next_u64());
}

#[test]
fn shuffle_is_a_permutation() {
    let mut r = SimRng::new(3);
    let mut v: Vec<u32> = (0..50).collect();
    r.shuffle(&mut v);
    let mut sorted = v.clone();
    sorted.sort();
    assert_eq!(sorted, (0..50).collect::<Vec<_>>());
    assert_ne!(v, sorted);
}
