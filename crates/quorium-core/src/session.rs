use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    next: u64,
    in_flight: BTreeSet<u64>,
}

#[derive(Clone, Default)]
pub struct Sequencer {
    state: Arc<Mutex<State>>,
}

pub struct Ticket {
    state: Arc<Mutex<State>>,
    seq: u64,
    ack_below: u64,
}

impl Sequencer {
    pub fn issue(&self) -> Ticket {
        let mut state = self.state.lock().unwrap();
        state.next += 1;
        let seq = state.next;
        state.in_flight.insert(seq);
        let ack_below = state.in_flight.first().copied().unwrap_or(seq);
        Ticket { state: self.state.clone(), seq, ack_below }
    }
}

impl Ticket {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn ack_below(&self) -> u64 {
        self.ack_below
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.state.lock().unwrap().in_flight.remove(&self.seq);
    }
}
