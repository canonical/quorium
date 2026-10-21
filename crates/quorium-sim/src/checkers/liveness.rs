use std::time::Duration;

use super::Violation;

#[derive(Debug, Clone)]
pub struct LivenessChecker {
    bound: Duration,
    healed_at: Option<Duration>,
    last_commit: Option<Duration>,
}

impl LivenessChecker {
    pub fn new(bound: Duration) -> Self {
        Self { bound, healed_at: None, last_commit: None }
    }

    pub fn healed(&mut self, at: Duration) {
        self.healed_at = Some(at);
    }

    pub fn committed(&mut self, at: Duration) {
        self.last_commit = Some(self.last_commit.map_or(at, |c| c.max(at)));
    }

    pub fn check(&self, now: Duration) -> Option<Violation> {
        let healed_at = self.healed_at?;
        let progressed = self.last_commit.is_some_and(|c| c >= healed_at);
        (!progressed && now > healed_at + self.bound).then_some(Violation::NoProgress { healed_at, bound: self.bound })
    }
}
