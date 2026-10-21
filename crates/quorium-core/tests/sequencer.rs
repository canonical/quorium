use std::sync::Arc;

use quorium_core::session::Sequencer;

#[test]
fn every_issued_sequence_is_at_or_above_every_acknowledgement_floor() {
    let seq = Arc::new(Sequencer::default());
    let mut handles = Vec::new();
    for _ in 0..8 {
        let seq = seq.clone();
        handles.push(std::thread::spawn(move || {
            let mut issued = Vec::new();
            for _ in 0..2_000 {
                let ticket = seq.issue();
                issued.push((ticket.seq(), ticket.ack_below()));
            }
            issued
        }));
    }
    let all: Vec<(u64, u64)> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    let mut seqs: Vec<u64> = all.iter().map(|(s, _)| *s).collect();
    seqs.sort();
    seqs.dedup();
    assert_eq!(seqs.len(), all.len());
    for (s, ack) in &all {
        assert!(ack <= s, "ticket {s} acknowledged below {ack}");
    }
}

#[test]
fn acknowledgement_floor_waits_for_the_oldest_ticket() {
    let seq = Sequencer::default();
    let first = seq.issue();
    let second = seq.issue();
    assert_eq!(second.ack_below(), first.seq());
    drop(first);
    let third = seq.issue();
    assert_eq!(third.ack_below(), second.seq());
    drop(second);
    drop(third);
    let fourth = seq.issue();
    assert_eq!(fourth.ack_below(), fourth.seq());
}
