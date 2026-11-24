use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateState {
    Open,
    Holding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refused {
    Held,
}

pub(crate) struct WriteGate {
    state: watch::Sender<GateState>,
    inflight: AtomicUsize,
    drained: Notify,
}

pub(crate) struct Pass<'a> {
    gate: &'a WriteGate,
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        if self.gate.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.gate.drained.notify_waiters();
        }
    }
}

impl Default for WriteGate {
    fn default() -> Self {
        Self { state: watch::channel(GateState::Open).0, inflight: AtomicUsize::new(0), drained: Notify::new() }
    }
}

impl WriteGate {
    pub(crate) async fn enter(&self, wait: Duration) -> Result<Pass<'_>, Refused> {
        let mut state = self.state.subscribe();
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            self.inflight.fetch_add(1, Ordering::AcqRel);
            let pass = Pass { gate: self };
            let current = *state.borrow_and_update();
            match current {
                GateState::Open => return Ok(pass),
                GateState::Holding => drop(pass),
            }
            if tokio::time::timeout_at(deadline, state.changed()).await.is_err() {
                return Err(Refused::Held);
            }
        }
    }

    pub(crate) fn hold(&self) -> bool {
        self.state.send_if_modified(|s| {
            let open = *s == GateState::Open;
            if open {
                *s = GateState::Holding;
            }
            open
        })
    }

    pub(crate) fn release(&self) {
        self.state.send_if_modified(|s| {
            let holding = *s == GateState::Holding;
            if holding {
                *s = GateState::Open;
            }
            holding
        });
    }

    pub(crate) async fn drained(&self) {
        loop {
            let notified = self.drained.notified();
            if self.inflight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}
