use quorium_core::escrow::Holding;

#[test]
fn a_settlement_never_includes_a_sale_whose_peer_copy_is_pending() {
    let mut h = Holding::default();
    h.grant(10);
    assert!(h.take(3));
    assert_eq!(h.begin_settle(1, false), None);
    h.unsell(3);
    assert_eq!(h.remaining, 10);
    assert_eq!(h.unsettled, 0);
}

#[test]
fn confirmed_sales_are_settled_and_unsold_ones_return() {
    let mut h = Holding::default();
    h.grant(10);
    assert!(h.take(3));
    assert!(h.take(2));
    h.confirm(3);
    h.unsell(2);
    assert_eq!(h.begin_settle(7, false), Some((3, 0)));
    assert_eq!(h.pending, Some((7, 3, 0)));
    assert_eq!(h.remaining, 7);
    assert_eq!(h.begin_settle(8, true), None);
    h.settled(7);
    assert_eq!(h.begin_settle(9, true), Some((0, 7)));
    assert_eq!(h.remaining, 0);
}

#[test]
fn takes_beyond_the_remaining_grant_are_refused() {
    let mut h = Holding::default();
    h.grant(2);
    assert!(!h.take(3));
    assert!(h.take(2));
    assert!(!h.take(1));
}

#[test]
fn a_settlement_waits_for_the_previous_one() {
    let mut h = Holding::default();
    h.grant(4);
    assert!(h.take(1));
    h.confirm(1);
    assert_eq!(h.begin_settle(1, false), Some((1, 0)));
    assert!(h.take(1));
    h.confirm(1);
    assert_eq!(h.begin_settle(2, false), None);
    h.settled(2);
    assert_eq!(h.pending, Some((1, 1, 0)));
    h.settled(1);
    assert_eq!(h.begin_settle(3, false), Some((1, 0)));
}

#[test]
fn replay_counts_sales_with_an_unknown_copy_outcome_as_sold() {
    let mut h = Holding::default();
    h.grant(10);
    assert!(h.take(4));
    h.finish_replay();
    assert_eq!(h.unsettled, 4);
    assert_eq!(h.remaining, 6);
}

#[test]
fn replay_of_journals_without_copy_records_draws_settlements_from_pending_copies() {
    let mut h = Holding::default();
    h.grant(10);
    assert!(h.take(5));
    h.replay_settle(3, 5, 0);
    h.finish_replay();
    assert_eq!(h.unsettled, 0);
    assert_eq!(h.pending, Some((3, 5, 0)));
    assert_eq!(h.remaining, 5);
}

#[test]
fn replayed_sales_never_underflow_the_grant() {
    let mut h = Holding::default();
    h.grant(2);
    h.replay_sold(3);
    assert_eq!(h.remaining, 0);
    h.finish_replay();
    assert_eq!(h.unsettled, 3);
}

#[test]
fn running_totals_count_confirmed_and_taken_units() {
    let mut h = Holding::default();
    h.grant(10);
    assert!(h.take(2));
    assert_eq!(h.taken_total(), 2);
    assert!(h.take(3));
    assert_eq!(h.taken_total(), 5);
    h.confirm(2);
    h.unsell(3);
    assert_eq!(h.taken_total(), 2);
    assert_eq!(h.sold_total, 2);
    assert_eq!(h.begin_settle(1, false), Some((2, 0)));
    assert_eq!(h.pending_through, 2);
    h.settled(1);
    assert!(h.take(1));
    h.confirm(1);
    assert_eq!(h.begin_settle(2, false), Some((1, 0)));
    assert_eq!(h.pending_through, 3);
}

#[test]
fn replay_rebuilds_the_running_total_of_a_pending_settlement() {
    let mut h = Holding::default();
    h.grant(10);
    h.replay_sold(4);
    h.replay_settle(3, 4, 0);
    h.replay_sold(1);
    h.finish_replay();
    assert_eq!(h.pending_through, 4);
    assert_eq!(h.sold_total, 5);
    assert_eq!(h.unsettled, 1);
}
