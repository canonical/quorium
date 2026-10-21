use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Scheduler<T> {
    queue: BTreeMap<(Duration, u64), T>,
    seq: u64,
    now: Duration,
}

impl<T> Default for Scheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Scheduler<T> {
    pub fn new() -> Self {
        Self { queue: BTreeMap::new(), seq: 0, now: Duration::ZERO }
    }

    pub fn now(&self) -> Duration {
        self.now
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn schedule_at(&mut self, at: Duration, event: T) {
        let at = at.max(self.now);
        self.queue.insert((at, self.seq), event);
        self.seq += 1;
    }

    pub fn schedule_after(&mut self, delay: Duration, event: T) {
        self.schedule_at(self.now + delay, event);
    }

    pub fn peek_time(&self) -> Option<Duration> {
        self.queue.keys().next().map(|(t, _)| *t)
    }

    pub fn pop(&mut self) -> Option<(Duration, T)> {
        let ((at, _), event) = self.queue.pop_first()?;
        self.now = at;
        Some((at, event))
    }

    pub fn pop_until(&mut self, horizon: Duration) -> Option<(Duration, T)> {
        match self.peek_time() {
            Some(t) if t <= horizon => self.pop(),
            _ => {
                self.now = self.now.max(horizon);
                None
            }
        }
    }
}
