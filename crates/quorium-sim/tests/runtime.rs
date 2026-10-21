use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft_rt::testing::Suite;
use openraft_rt::{AsyncRuntime, Instant};
use quorium_sim::runtime::{SimInstant, SimRuntime};

#[test]
fn conforms_to_openraft_runtime_suite() {
    Suite::<SimRuntime>::test_all();
}

#[test]
fn virtual_time_does_not_wait_on_the_wall_clock() {
    let wall = std::time::Instant::now();
    let mut rt = SimRuntime::with_seed(1);
    let elapsed = rt.block_on(async {
        let start = SimInstant::now();
        SimRuntime::sleep(Duration::from_secs(3600)).await;
        SimInstant::now() - start
    });
    assert_eq!(elapsed, Duration::from_secs(3600));
    assert!(wall.elapsed() < Duration::from_secs(5));
}

#[test]
fn clock_starts_at_zero_and_reports_through_runtime() {
    let mut rt = SimRuntime::with_seed(1);
    rt.block_on(async {
        SimRuntime::sleep(Duration::from_millis(250)).await;
    });
    assert_eq!(rt.now(), Duration::from_millis(250));
}

fn interleaving(seed: u64) -> Vec<u32> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut rt = SimRuntime::with_seed(seed);
    let shared = log.clone();
    rt.block_on(async move {
        let mut handles = Vec::new();
        for task in 0..8u32 {
            let log = shared.clone();
            handles.push(SimRuntime::spawn(async move {
                for _ in 0..4 {
                    log.lock().unwrap().push(task);
                    SimRuntime::sleep(Duration::ZERO).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
    Arc::try_unwrap(log).unwrap().into_inner().unwrap()
}

#[test]
fn same_seed_replays_the_same_interleaving() {
    assert_eq!(interleaving(7), interleaving(7));
}

#[test]
fn different_seeds_explore_different_interleavings() {
    let distinct: std::collections::BTreeSet<_> = (0..20).map(interleaving).collect();
    assert!(distinct.len() > 10);
}

#[test]
fn timers_fire_in_deadline_order() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut rt = SimRuntime::with_seed(3);
    let shared = log.clone();
    rt.block_on(async move {
        let mut handles = Vec::new();
        for ms in [30u64, 10, 20] {
            let log = shared.clone();
            handles.push(SimRuntime::spawn(async move {
                SimRuntime::sleep(Duration::from_millis(ms)).await;
                log.lock().unwrap().push((ms, SimInstant::now().as_duration()));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
    let log = log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec![(10, Duration::from_millis(10)), (20, Duration::from_millis(20)), (30, Duration::from_millis(30)),]
    );
}

#[test]
fn run_until_stops_at_horizon_with_tasks_pending() {
    let mut rt = SimRuntime::with_seed(1);
    let ticks = Arc::new(Mutex::new(0u32));
    let t = ticks.clone();
    rt.spawn_detached(async move {
        loop {
            SimRuntime::sleep(Duration::from_millis(100)).await;
            *t.lock().unwrap() += 1;
        }
    });
    rt.run_until(Duration::from_millis(1050));
    assert_eq!(*ticks.lock().unwrap(), 10);
    assert_eq!(rt.now(), Duration::from_millis(1050));
    rt.run_until(Duration::from_millis(2000));
    assert_eq!(*ticks.lock().unwrap(), 20);
}

#[test]
fn tasks_can_run_on_a_skewed_clock_that_their_children_inherit() {
    use openraft_rt::Instant as _;
    use quorium_sim::runtime::SimInstant;
    let mut rt = SimRuntime::with_seed(3);
    let seen: std::sync::Arc<std::sync::Mutex<Vec<(&str, std::time::Duration)>>> = Default::default();
    let (fast, normal) = (seen.clone(), seen.clone());
    rt.with_clock_rate(1_000_000, || {
        rt.spawn_detached(async move {
            <SimRuntime as openraft_rt::AsyncRuntime>::sleep(std::time::Duration::from_millis(100)).await;
            fast.lock().unwrap().push(("fast", SimInstant::now().as_duration()));
            let child = fast.clone();
            <SimRuntime as openraft_rt::AsyncRuntime>::spawn(async move {
                child.lock().unwrap().push(("child", SimInstant::now().as_duration()));
            });
        })
    });
    rt.spawn_detached(async move {
        <SimRuntime as openraft_rt::AsyncRuntime>::sleep(std::time::Duration::from_millis(100)).await;
        normal.lock().unwrap().push(("normal", SimInstant::now().as_duration()));
    });
    rt.run_for(std::time::Duration::from_millis(49));
    assert!(seen.lock().unwrap().is_empty());
    rt.run_for(std::time::Duration::from_millis(2));
    assert_eq!(seen.lock().unwrap().iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec!["fast", "child"]);
    assert!(seen.lock().unwrap().iter().all(|(_, t)| *t >= std::time::Duration::from_millis(100)));
    rt.run_for(std::time::Duration::from_millis(60));
    let all = seen.lock().unwrap().clone();
    assert_eq!(all.last().unwrap().0, "normal");
    assert_eq!(all.last().unwrap().1, std::time::Duration::from_millis(100));
}
