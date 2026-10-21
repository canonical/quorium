use std::time::Duration;

use quorium_sim::scheduler::Scheduler;

#[test]
fn events_pop_in_time_order() {
    let mut s = Scheduler::new();
    s.schedule_at(Duration::from_millis(30), "c");
    s.schedule_at(Duration::from_millis(10), "a");
    s.schedule_at(Duration::from_millis(20), "b");
    let order: Vec<_> = std::iter::from_fn(|| s.pop()).map(|(_, e)| e).collect();
    assert_eq!(order, vec!["a", "b", "c"]);
}

#[test]
fn equal_times_pop_in_insertion_order() {
    let mut s = Scheduler::new();
    for e in ["x", "y", "z"] {
        s.schedule_at(Duration::from_millis(5), e);
    }
    let order: Vec<_> = std::iter::from_fn(|| s.pop()).map(|(_, e)| e).collect();
    assert_eq!(order, vec!["x", "y", "z"]);
}

#[test]
fn clock_advances_to_popped_event() {
    let mut s = Scheduler::new();
    s.schedule_at(Duration::from_millis(7), ());
    assert_eq!(s.now(), Duration::ZERO);
    let (t, _) = s.pop().unwrap();
    assert_eq!(t, Duration::from_millis(7));
    assert_eq!(s.now(), Duration::from_millis(7));
}

#[test]
fn schedule_after_is_relative_to_now() {
    let mut s = Scheduler::new();
    s.schedule_at(Duration::from_millis(100), 1);
    s.pop();
    s.schedule_after(Duration::from_millis(5), 2);
    assert_eq!(s.pop().unwrap().0, Duration::from_millis(105));
}

#[test]
fn past_events_run_now_not_backwards() {
    let mut s = Scheduler::new();
    s.schedule_at(Duration::from_millis(50), 1);
    s.pop();
    s.schedule_at(Duration::from_millis(10), 2);
    assert_eq!(s.pop().unwrap().0, Duration::from_millis(50));
}

#[test]
fn pop_until_respects_horizon() {
    let mut s = Scheduler::new();
    s.schedule_at(Duration::from_millis(5), 1);
    s.schedule_at(Duration::from_millis(15), 2);
    assert_eq!(s.pop_until(Duration::from_millis(10)).map(|e| e.1), Some(1));
    assert_eq!(s.pop_until(Duration::from_millis(10)), None);
    assert_eq!(s.now(), Duration::from_millis(10));
    assert_eq!(s.len(), 1);
}
