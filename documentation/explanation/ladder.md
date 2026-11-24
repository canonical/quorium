# Leader elections and the Ladder

When a leader fails, its followers must notice and elect a new one. Failover time is the sum of the two: detection, then election. Classic Raft tunes both with one randomized election timeout, which forces a trade-off. A short timeout detects failures quickly but lets several followers time out together, split the vote and try again. A long timeout avoids split votes but detects failures slowly.

Quorium replaces the randomized timeout with the **Ladder**: a deterministic order in which followers stand for election.

## Ranking candidates

Every follower knows, from liveness pings, the log position of every other node. Each follower ranks the live nodes, except the failed leader and nodes [standing aside](#standing-aside):

1. **Most complete log first.** Only a node with an up-to-date log can win, so the best candidates go first.
2. **Then nodes in the failed leader's datacenter.** Leadership stays close to where it was, and to the clients that were using it.
3. **Then a fixed order** derived from node IDs.

A follower's rank is the number of nodes ahead of it.

## Waiting for your turn

When the leader goes quiet, each follower waits `base + rank × step` before standing for election:

- **base** is the detection delay: at least the leader lease plus one heartbeat (3 heartbeats by default), stretched to twice the longest heartbeat gap recently observed, so that a slow leader is not mistaken for a dead one. Followers refuse to vote while the old leader's lease may still be valid, so standing earlier would be wasted.
- **step** is at least one consensus round trip: the time for the first candidate to collect its votes, including the voters' fsync.

The first-ranked follower therefore normally wins before the second one's turn comes. There is no randomness to wait out, and split votes become rare.

If an election fails anyway, the candidate retries after a short delay, also scaled by the measured round trip.

## Pre-vote

Every campaign starts with a pre-vote: the candidate asks whether others would vote for it, without raising its term. A node that was only cut off briefly cannot disrupt a healthy leader by forcing a new term on its return.

## Handing leadership back

After a failover, leadership may sit in a datacenter that is not its preferred one, for example far from the clients writing a shard's data. Once the shard has been idle for 2 seconds and the preferred node has caught up, the leader transfers leadership to it. Transfers are suspended for `handoff_suspend` (30 seconds by default) after an election, so that leadership does not swing back and forth while the cluster is unstable.

Every transfer, automatic or requested, follows the same steps. The leader holds new writes on the shard, waits for writes in progress, waits until the target holds every entry of its log, asks the target to take over and waits until it leads. Holding writes matters: the moment a Raft leader starts a transfer it stops accepting writes, so a target that is still behind would leave the shard unable to write until an election. If the target never takes over, the remaining voters elect a leader as after a crash.

## Standing aside

A node that is about to stop should not be elected, nor receive leadership from a rebalancing peer. `step_down` and `shutdown_gracefully` make it **stand aside** for a while: it does not stand for election, and it refuses leadership transfers to itself. It also tells its peers, which then leave it out when they count the nodes ranked ahead of them, and when they choose a transfer target. The next-best follower therefore stands at once when the leader goes quiet, instead of waiting one step for a node that will not campaign.

A planned stop then needs no election at all: leadership has already moved, and the remaining voters keep their leader for as long as the node is gone.

## Adapting to slow disks

The Ladder measures fsync latency and includes it in the consensus round trip. On a slow disk, steps get longer, so candidates wait for each other instead of colliding. Failover gets slower, but it stays orderly.

## Verified against a model

The Ladder's rules, including standing aside, are written as a TLA+ specification and checked with TLC: elections stay safe (at most one leader per term, committed entries never lost) and live (a leader is eventually elected). Traces from the implementation are checked against the same model. See [How Quorium is verified](verification.md).

## What it achieves

In simulation, single-region failover takes 170 ms at the 99th percentile, and 486 ms across three regions 15–40 ms apart. On real processes with a 100 ms heartbeat and fast disks, it takes 650 ms at p99. Most of that is detection: about three heartbeats. `node.failover_estimate()` computes the expected time for a cluster's own settings and round trip. See [Known limitations](limitations.md).
