# Spec conformance

`spec/Ladder.tla` is the model, checked exhaustively with TLC on small instances (`MCSafety.cfg`, `MCLiveness.cfg`). Simulator traces are also checked against the model, so the model and the code cannot drift apart unnoticed.

## How it works

1. `quorium_sim::trace::record(seed, cfg)` runs a simulated cluster on the in-memory store, takes a sample every simulated millisecond, and projects each node onto the spec's variables:
   - `up`
   - `term` and `voted`, from the durably stored vote
   - `role` (`F`, `C`, `L`)
   - `log`, the term of every entry, with openraft index `i` mapped to spec position `i + 1`
   - `committed`: every entry a node has applied, tagged with the highest term seen when it was first applied

   Consecutive identical samples are merged.
2. `check_with_tlc` writes the trace as a TLA+ module (`TraceData`). TLC then walks it with `spec/TraceCheck.tla`, which instantiates `Ladder` with the trace substituted for the spec's variables.
3. TLC checks the spec's own invariants on every state: `ElectionSafety`, `LeaderCompleteness` and `CommittedUnique`.
4. TLC checks step rules taken from the spec's actions on every pair of consecutive states:

| Rule | Spec action it comes from |
|---|---|
| `TermsNeverDecrease` | No action lowers `term`, including `Crash` and `Restart` |
| `VotesAreStable` | `GrantVote` needs `voted ∈ {Nil, n}` within a term |
| `LeaderVotedForItself` | `Elect` sets `voted` to the candidate |
| `LeadersAppendOwnTerm` | `BecomeLeader` and `ClientWrite` append entries of the leader's term, and never truncate |
| `EntriesComeFromLeaders` | `Replicate` copies a leader's log. A new entry is either the node's own append as leader, or a prefix that some other node holds |
| `LogTermsBounded` | Entries are never newer than their holder's term |

## Where the projection is coarser than the spec

- The spec's actions are atomic, while the implementation passes messages. One sample can therefore cover several spec steps, and the step rules are written to accept such compositions. A follower may receive only part of a leader's log, which `Replicate` would not allow, so the rule asks for a prefix match.
- openraft stores the leader a node has accepted in the same field as its vote (a *committed* vote). The projection keeps a node's earlier vote when a committed vote for another node arrives within the same term.
- Only the in-memory store is traced, and snapshots are off so that logs stay complete.

## Running it

```sh
QUORIUM_TLA_JAR=/path/to/tla2tools.jar QUORIUM_CONFORMANCE_SEEDS=200 \
  cargo test --release -p quorium-sim --test conformance
```

Each seed checks a 3-node vote-hunting trace and a 5-node swarm trace. `QUORIUM_CONFORMANCE_FROM` sets the first seed, so disjoint ranges can run in parallel. CI runs 20 seeds on every change and the nightly workflow runs 1,000. As a control, a trace from the planted `ForgetVote` bug must fail; it fails `TermsNeverDecrease` on seed 1.
