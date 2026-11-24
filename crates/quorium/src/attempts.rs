use std::time::Instant;

use crate::node::ClientError;

pub(crate) struct Attempts {
    leaderless_since: Option<Instant>,
    last: String,
}

impl Attempts {
    pub(crate) fn new(initial: &str) -> Self {
        Self { leaderless_since: None, last: initial.to_string() }
    }

    pub(crate) fn no_leader(&mut self) {
        self.leaderless_since.get_or_insert_with(Instant::now);
        self.last = String::from("no leader");
    }

    pub(crate) fn failed(&mut self, reason: impl Into<String>) {
        self.leaderless_since = None;
        self.last = reason.into();
    }

    pub(crate) fn give_up(self) -> ClientError {
        match self.leaderless_since {
            Some(since) => ClientError::NoLeader { for_at_least: since.elapsed() },
            None => ClientError::Unavailable(self.last),
        }
    }
}
