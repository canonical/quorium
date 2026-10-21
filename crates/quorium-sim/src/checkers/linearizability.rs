use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

use super::Violation;

pub trait Model {
    type State: Clone + Eq + Hash + Default;
    type Op: Clone + Debug;
    type Ret: Clone + Eq + Debug;

    fn step(state: &Self::State, op: &Self::Op) -> (Self::State, Self::Ret);

    fn relevant(ops: &[Operation<Self>]) -> Vec<bool>
    where
        Self: Sized,
    {
        vec![true; ops.len()]
    }
}

#[derive(Debug, Clone)]
pub struct Operation<M: Model> {
    pub client: u64,
    pub op: M::Op,
    pub call: u64,
    pub ret: Option<(u64, M::Ret)>,
}

#[derive(Debug, Clone)]
pub struct History<M: Model> {
    ops: Vec<Operation<M>>,
}

impl<M: Model> Default for History<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Model> History<M> {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn record(&mut self, client: u64, op: M::Op, call: u64, ret: Option<(u64, M::Ret)>) {
        self.ops.push(Operation { client, op, call, ret });
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn operations(&self) -> &[Operation<M>] {
        &self.ops
    }
}

pub const DEFAULT_BUDGET: usize = 2_000_000;

pub fn check<M: Model>(history: &History<M>) -> Result<(), Violation> {
    check_key("", history)
}

pub fn check_key<M: Model>(key: &str, history: &History<M>) -> Result<(), Violation> {
    check_with_budget(key, history, DEFAULT_BUDGET)
}

fn zobrist(i: usize) -> u128 {
    let mut x = (i as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut next = || {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    (u128::from(next()) << 64) | u128::from(next())
}

struct Frame<S> {
    state: S,
    next: usize,
    chose: Option<usize>,
}

pub fn check_with_budget<M: Model>(key: &str, history: &History<M>, budget: usize) -> Result<(), Violation> {
    let keep = M::relevant(&history.ops);
    let mut ops: Vec<&Operation<M>> = history.ops.iter().zip(keep).filter(|(_, k)| *k).map(|(o, _)| o).collect();
    ops.sort_by_key(|o| o.call);
    let n = ops.len();
    let keys: Vec<u128> = (0..n).map(zobrist).collect();
    let mut done = vec![false; n];
    let mut fingerprint: u128 = 0;
    let mut pending_returns: std::collections::BTreeMap<u64, usize> = std::collections::BTreeMap::new();
    for o in &ops {
        if let Some((ret, _)) = &o.ret {
            *pending_returns.entry(*ret).or_default() += 1;
        }
    }
    let mut first_undone = 0usize;
    let mut seen: HashSet<(u128, M::State)> = HashSet::new();
    let mut stack: Vec<Frame<M::State>> = vec![Frame { state: M::State::default(), next: 0, chose: None }];
    let operations = history.ops.len();
    let mut fresh = true;
    while let Some(top) = stack.last_mut() {
        let Some((&horizon, _)) = pending_returns.iter().next() else {
            return Ok(());
        };
        if fresh {
            if !seen.insert((fingerprint, top.state.clone())) {
                let popped = stack.pop().expect("a frame is on the stack");
                if let Some(i) = popped.chose {
                    done[i] = false;
                    fingerprint ^= keys[i];
                    first_undone = first_undone.min(i);
                    if let Some((ret, _)) = &ops[i].ret {
                        *pending_returns.entry(*ret).or_default() += 1;
                    }
                }
                fresh = false;
                continue;
            }
            if seen.len() > budget {
                return Err(Violation::CheckBudgetExceeded { key: key.to_string(), operations });
            }
        }
        let mut chosen = None;
        let mut i = top.next.max(first_undone);
        while i < n && ops[i].call <= horizon {
            if !done[i] {
                let (next_state, ret) = M::step(&top.state, &ops[i].op);
                if ops[i].ret.as_ref().is_none_or(|(_, expected)| *expected == ret) {
                    chosen = Some((i, next_state));
                    break;
                }
            }
            i += 1;
        }
        match chosen {
            Some((i, next_state)) => {
                top.next = i + 1;
                done[i] = true;
                fingerprint ^= keys[i];
                if let Some((ret, _)) = &ops[i].ret {
                    let count = pending_returns.get_mut(ret).expect("return was counted");
                    *count -= 1;
                    if *count == 0 {
                        pending_returns.remove(ret);
                    }
                }
                while first_undone < n && done[first_undone] {
                    first_undone += 1;
                }
                stack.push(Frame { state: next_state, next: 0, chose: Some(i) });
                fresh = true;
            }
            None => {
                let popped = stack.pop().expect("a frame is on the stack");
                if let Some(i) = popped.chose {
                    done[i] = false;
                    fingerprint ^= keys[i];
                    first_undone = first_undone.min(i);
                    if let Some((ret, _)) = &ops[i].ret {
                        *pending_returns.entry(*ret).or_default() += 1;
                    }
                }
                fresh = false;
            }
        }
    }
    Err(Violation::NotLinearizable { key: key.to_string(), operations })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterOp {
    Read,
    Write(u64),
    Cas(u64, u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterRet {
    Ok,
    Value(Option<u64>),
    Swapped(bool),
}

#[derive(Debug, Clone, Copy)]
pub struct Register;

impl Model for Register {
    type State = Option<u64>;
    type Op = RegisterOp;
    type Ret = RegisterRet;

    fn step(state: &Option<u64>, op: &RegisterOp) -> (Option<u64>, RegisterRet) {
        match *op {
            RegisterOp::Read => (*state, RegisterRet::Value(*state)),
            RegisterOp::Write(v) => (Some(v), RegisterRet::Ok),
            RegisterOp::Cas(old, new) if *state == Some(old) => (Some(new), RegisterRet::Swapped(true)),
            RegisterOp::Cas(..) => (*state, RegisterRet::Swapped(false)),
        }
    }

    fn relevant(ops: &[Operation<Self>]) -> Vec<bool> {
        let mut observed = HashSet::new();
        for o in ops {
            match (&o.op, &o.ret) {
                (_, Some((_, RegisterRet::Value(Some(v))))) => {
                    observed.insert(*v);
                }
                (RegisterOp::Cas(expected, _), Some((_, RegisterRet::Swapped(true)))) => {
                    observed.insert(*expected);
                }
                _ => {}
            }
        }
        ops.iter()
            .map(|o| match (&o.op, &o.ret) {
                (_, Some(_)) => true,
                (RegisterOp::Read, None) => false,
                (RegisterOp::Write(v) | RegisterOp::Cas(_, v), None) => observed.contains(v),
            })
            .collect()
    }
}
