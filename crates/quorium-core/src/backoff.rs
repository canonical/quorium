use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backoff {
    next: Duration,
    max: Duration,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self { next: initial.min(max), max }
    }

    pub fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = (self.next * 2).min(self.max);
        delay
    }
}

#[derive(Debug)]
pub struct Throttle {
    interval: Duration,
    last: AtomicU64,
}

const NEVER: u64 = u64::MAX;

impl Throttle {
    pub fn new(interval: Duration) -> Self {
        Self { interval, last: AtomicU64::new(NEVER) }
    }

    pub fn permit(&self, now: Duration) -> bool {
        let now = now.as_micros() as u64;
        let interval = self.interval.as_micros() as u64;
        let last = self.last.load(Ordering::Acquire);
        if last != NEVER && now.saturating_sub(last) < interval {
            return false;
        }
        self.last.compare_exchange(last, now, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }
}
