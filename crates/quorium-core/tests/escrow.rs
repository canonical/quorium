use quorium_core::kv::{Command, EscrowAction, EscrowKey, EscrowOp, KvOp, KvState, Reply};

fn escrow(s: &mut KvState, seq: u64, node: u64, action: EscrowAction) -> Reply {
    s.apply(&Command::new(node, seq, KvOp::Escrow(EscrowOp { name: "sku".into(), node, action })))
}

fn granted(r: Reply) -> u64 {
    match r {
        Reply::Escrow { granted, .. } => granted,
        other => panic!("{other:?}"),
    }
}

#[test]
fn grants_shrink_as_the_pool_runs_low() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 1, EscrowAction::Refill { units: 100 });
    let first = granted(escrow(&mut s, 2, 1, EscrowAction::Grant { want: 1_000 }));
    let second = granted(escrow(&mut s, 3, 2, EscrowAction::Grant { want: 1_000 }));
    assert!(first > second && second > 0, "{first} {second}");
    let mut total = first + second;
    for seq in 4..200 {
        total += granted(escrow(&mut s, seq, 3, EscrowAction::Grant { want: 1_000 }));
    }
    assert_eq!(total, 100);
    assert_eq!(granted(escrow(&mut s, 200, 3, EscrowAction::Grant { want: 1 })), 0);
}

#[test]
fn settling_records_sales_and_releases_unused_units() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 1, EscrowAction::Refill { units: 10 });
    let g = granted(escrow(&mut s, 2, 1, EscrowAction::Grant { want: 10 }));
    escrow(&mut s, 3, 1, EscrowAction::Settle { sold: 1, release: g - 1 });
    let view = s.escrow("sku").unwrap();
    assert_eq!((view.pool, view.sold, view.outstanding.get(&1).copied().unwrap_or(0)), (9, 1, 0));
}

#[test]
fn settling_more_than_was_granted_is_refused() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 1, EscrowAction::Refill { units: 10 });
    let g = granted(escrow(&mut s, 2, 1, EscrowAction::Grant { want: 2 }));
    assert!(matches!(escrow(&mut s, 3, 1, EscrowAction::Settle { sold: g + 1, release: 0 }), Reply::ApplyFailed(_)));
    assert_eq!(s.escrow("sku").unwrap().sold, 0);
}

#[test]
fn consensus_buy_is_the_fallback_when_grants_are_exhausted() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 1, EscrowAction::Refill { units: 3 });
    assert_eq!(granted(escrow(&mut s, 2, 2, EscrowAction::Buy { units: 2 })), 2);
    assert_eq!(granted(escrow(&mut s, 3, 2, EscrowAction::Buy { units: 2 })), 0);
    let view = s.escrow("sku").unwrap();
    assert_eq!((view.pool, view.sold), (1, 2));
}

#[test]
fn stock_is_conserved() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 50 });
    let mut seq = 2;
    for node in 1..=3u64 {
        let g = granted(escrow(&mut s, seq, node, EscrowAction::Grant { want: 20 }));
        seq += 1;
        escrow(&mut s, seq, node, EscrowAction::Settle { sold: g / 2, release: 0 });
        seq += 1;
    }
    let v = s.escrow("sku").unwrap();
    assert_eq!(v.pool + v.sold + v.outstanding.values().sum::<u64>(), 50);
    assert_eq!(v.estimate(), v.pool + v.outstanding.values().sum::<u64>());
}

fn as_client(s: &mut KvState, client: u64, seq: u64, holder: u64, action: EscrowAction) -> Reply {
    s.apply(&Command::new(client, seq, KvOp::Escrow(EscrowOp { name: "sku".into(), node: holder, action })))
}

fn conserved(s: &KvState, stock: u64) {
    let v = s.escrow("sku").unwrap();
    assert_eq!(v.pool + v.sold + v.outstanding.values().sum::<u64>(), stock, "{v:?}");
}

#[test]
fn settlements_carry_running_totals_and_count_only_new_sales() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 100 });
    let g = granted(escrow(&mut s, 2, 1, EscrowAction::Grant { want: 20 }));
    escrow(&mut s, 3, 1, EscrowAction::SettleThrough { through: 4, release: 0 });
    escrow(&mut s, 4, 1, EscrowAction::SettleThrough { through: 7, release: 0 });
    let v = s.escrow("sku").unwrap();
    assert_eq!(v.sold, 7);
    assert_eq!(v.outstanding.get(&1), Some(&(g - 7)));
    assert_eq!(s.escrow_holder(&EscrowKey::Named("sku".into()), 1).unwrap().settled_through, 7);
    assert!(matches!(escrow(&mut s, 5, 1, EscrowAction::SettleThrough { through: 6, release: 0 }), Reply::ApplyFailed(_)));
    conserved(&s, 100);
}

#[test]
fn a_reclaim_counts_sales_since_the_last_settlement_and_returns_the_rest() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 100 });
    let g = granted(escrow(&mut s, 2, 1, EscrowAction::Grant { want: 20 }));
    escrow(&mut s, 3, 1, EscrowAction::SettleThrough { through: 3, release: 0 });
    as_client(&mut s, 50, 1, 1, EscrowAction::Reclaim { through: 5 });
    let v = s.escrow("sku").unwrap();
    assert_eq!(v.sold, 5);
    assert_eq!(v.outstanding.get(&1), None);
    assert_eq!(v.pool, 100 - 5);
    assert!(g >= 5);
    assert!(s.escrow_holder(&EscrowKey::Named("sku".into()), 1).unwrap().reclaimed);
    conserved(&s, 100);
}

#[test]
fn a_reclaimed_holder_cannot_settle_late_or_take_a_new_grant() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 100 });
    granted(escrow(&mut s, 2, 1, EscrowAction::Grant { want: 20 }));
    as_client(&mut s, 50, 1, 1, EscrowAction::Reclaim { through: 2 });
    assert!(matches!(escrow(&mut s, 3, 1, EscrowAction::SettleThrough { through: 2, release: 0 }), Reply::ApplyFailed(_)));
    assert_eq!(granted(escrow(&mut s, 4, 1, EscrowAction::Grant { want: 20 })), 0);
    assert!(matches!(as_client(&mut s, 50, 2, 1, EscrowAction::Reclaim { through: 2 }), Reply::ApplyFailed(_)));
    conserved(&s, 100);
}

#[test]
fn a_reclaim_never_counts_more_than_the_grant_as_sold() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 100 });
    let g = granted(escrow(&mut s, 2, 1, EscrowAction::Grant { want: 4 }));
    as_client(&mut s, 50, 1, 1, EscrowAction::Reclaim { through: g + 10 });
    let v = s.escrow("sku").unwrap();
    assert_eq!(v.sold, g);
    conserved(&s, 100);
}

#[test]
fn a_reclaim_of_a_node_without_a_grant_only_marks_it() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 10 });
    as_client(&mut s, 50, 1, 7, EscrowAction::Reclaim { through: 0 });
    let v = s.escrow("sku").unwrap();
    assert_eq!((v.pool, v.sold), (10, 0));
    assert_eq!(granted(escrow(&mut s, 2, 7, EscrowAction::Grant { want: 5 })), 0);
}

#[test]
fn holders_lists_every_node_that_was_granted_units() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 100 });
    granted(escrow(&mut s, 2, 1, EscrowAction::Grant { want: 5 }));
    granted(escrow(&mut s, 3, 2, EscrowAction::Grant { want: 5 }));
    let mut held: Vec<u64> = s.escrow_holders().map(|(_, node, _)| node).collect();
    held.sort();
    assert_eq!(held, vec![1, 2]);
}

#[test]
fn a_grant_names_the_peer_that_copies_its_sales() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 100 });
    let g = granted(escrow(&mut s, 2, 1, EscrowAction::GrantVia { want: 8, peer: 4 }));
    let h = *s.escrow_holder(&EscrowKey::Named("sku".into()), 1).unwrap();
    assert_eq!((h.copy_peer, h.held), (Some(4), g));
    escrow(&mut s, 3, 1, EscrowAction::SettleThrough { through: 2, release: 1 });
    assert_eq!(s.escrow_holder(&EscrowKey::Named("sku".into()), 1).unwrap().held, g - 3);
}

#[test]
fn the_copy_peer_cannot_change_while_units_are_held() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 100 });
    let g = granted(escrow(&mut s, 2, 1, EscrowAction::GrantVia { want: 8, peer: 4 }));
    assert_eq!(granted(escrow(&mut s, 3, 1, EscrowAction::GrantVia { want: 8, peer: 5 })), 0);
    assert!(granted(escrow(&mut s, 4, 1, EscrowAction::GrantVia { want: 8, peer: 4 })) > 0);
    let held = s.escrow_holder(&EscrowKey::Named("sku".into()), 1).unwrap().held;
    escrow(&mut s, 5, 1, EscrowAction::SettleThrough { through: 0, release: held });
    assert!(granted(escrow(&mut s, 6, 1, EscrowAction::GrantVia { want: 8, peer: 5 })) > 0);
    assert_eq!(s.escrow_holder(&EscrowKey::Named("sku".into()), 1).unwrap().copy_peer, Some(5));
    assert!(g > 0);
    conserved(&s, 100);
}

#[test]
fn a_reclaim_empties_the_holder() {
    let mut s = KvState::default();
    escrow(&mut s, 1, 9, EscrowAction::Refill { units: 100 });
    granted(escrow(&mut s, 2, 1, EscrowAction::GrantVia { want: 8, peer: 4 }));
    as_client(&mut s, 50, 1, 1, EscrowAction::Reclaim { through: 3 });
    let h = *s.escrow_holder(&EscrowKey::Named("sku".into()), 1).unwrap();
    assert_eq!((h.held, h.reclaimed, h.settled_through), (0, true, 3));
    conserved(&s, 100);
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

#[test]
fn random_histories_conserve_stock_and_keep_holders_in_step_with_the_view() {
    for seed in 1..200u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let mut s = KvState::default();
        let mut stock = 0;
        let mut sold_by = [0u64; 4];
        let mut seq = [0u64; 6];
        for _ in 0..200 {
            let node = rng.below(4);
            let client = if rng.below(5) == 0 { 5 } else { node as usize };
            seq[client] += 1;
            let action = match rng.below(6) {
                0 => {
                    let units = rng.below(20);
                    stock += units;
                    EscrowAction::Refill { units }
                }
                1 => EscrowAction::GrantVia { want: rng.below(10) + 1, peer: rng.below(2) + 10 },
                2 => {
                    let held = s.escrow_holder(&EscrowKey::Named("sku".into()), node).map_or(0, |h| h.held);
                    let settled = s.escrow_holder(&EscrowKey::Named("sku".into()), node).map_or(0, |h| h.settled_through);
                    let sold = rng.below(held + 1);
                    let release = rng.below(held - sold + 1);
                    sold_by[node as usize] = settled + sold;
                    EscrowAction::SettleThrough { through: settled + sold, release }
                }
                3 => EscrowAction::Reclaim { through: sold_by[node as usize] + rng.below(5) },
                4 => EscrowAction::Buy { units: rng.below(3) + 1 },
                _ => EscrowAction::Grant { want: rng.below(10) + 1 },
            };
            as_client(&mut s, client as u64, seq[client], node, action);
            let Some(v) = s.escrow("sku") else { continue };
            assert_eq!(v.pool + v.sold + v.outstanding.values().sum::<u64>(), stock, "seed {seed}");
            for n in 0..4u64 {
                let held = s.escrow_holder(&EscrowKey::Named("sku".into()), n).map_or(0, |h| h.held);
                assert_eq!(held, v.outstanding.get(&n).copied().unwrap_or(0), "seed {seed} node {n}");
            }
        }
    }
}
