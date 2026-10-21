# How Quorium is verified

A consensus system's bugs hide in rare interleavings: a crash between two writes, a message delayed past a timeout. Ordinary tests rarely hit them. Quorium is verified in four layers, each catching what the previous one cannot.

## 1. A formal model

The election rules, including the Ladder, are specified in TLA+ (`spec/Ladder.tla`). The TLC model checker explores every reachable state of small clusters and checks that there is at most one leader per term, that committed entries are never lost, and that a leader is eventually elected.

A model only proves the model. The next layer ties it to the code.

## 2. Trace conformance

The simulator records traces of the real implementation: terms, votes, roles and logs of every node, sampled every simulated millisecond. TLC then checks each trace against the model: every state must satisfy the model's invariants, and every step must be one the model allows. 10,000 seeds (20,000 traces) conform.

As a control, a deliberately planted bug (a node forgetting its vote) must be caught, and it is.

## 3. Deterministic simulation

`quorium-sim` runs whole clusters of real Quorium code on a simulated runtime, network and disk. One seed determines every scheduling decision, message delay and fault, so any failure can be replayed exactly.

Each seed runs a scenario with faults: crashes, power loss with torn and lost writes, partitions, message loss and reordering, clock drift. Checkers then verify:

- election safety: at most one leader per term;
- state-machine agreement: every replica applies the same entry at each position;
- durability: no acknowledged write is lost;
- linearizability of every history, with a Wing–Gong checker;
- escrow: sales never exceed stock;
- liveness: the cluster recovers once faults stop.

Planted bugs in each category must be caught by their checker. The release gate runs 10,000 seeds per scenario with zero violations allowed, and background sweeps run 10 million seeds per scenario.

## 4. Real processes under faults

The simulator cannot catch bugs in real files, sockets and timers. A test rig runs five real nodes in LXD containers, in three emulated regions with real network delays, and injects faults: SIGKILL, SIGSTOP, network partitions with iptables, container power-off and packet loss. Jepsen-style workloads run throughout:

- a register workload whose history must be linearizable;
- a bank workload whose transfers must conserve the total;
- an escrow counter that must never oversell.

The same rig runs a 72-hour soak test.

## Also

- Stored and wire formats have fixtures that every later build must decode.
- Fuzz targets cover the log reader and the message and snapshot decoders.
- Mutation testing measures whether the tests notice changes to the code.

The contributor documents describe each layer in detail: [conformance](https://github.com/canonical/quorium/blob/main/docs/CONFORMANCE.md), [the rig](https://github.com/canonical/quorium/blob/main/docs/RIG.md), [formats](https://github.com/canonical/quorium/blob/main/docs/FORMATS.md).
