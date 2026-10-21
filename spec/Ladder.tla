------------------------------- MODULE Ladder -------------------------------
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Nodes, MaxTerm, MaxLog, MaxCrashes, LadderMode, DCOf(_)

VARIABLES term, voted, state, log, votes, up, quiet, crashes, lastLeader, committed

vars == <<term, voted, state, log, votes, up, quiet, crashes, lastLeader, committed>>

Nil == 0

Quorum(S) == 2 * Cardinality(S) > Cardinality(Nodes)

LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)]

UpToDate(a, b) ==
    \/ LastTerm(log[a]) > LastTerm(log[b])
    \/ /\ LastTerm(log[a]) = LastTerm(log[b])
       /\ Len(log[a]) >= Len(log[b])

Local(x) == lastLeader # Nil /\ DCOf(x) = DCOf(lastLeader)

Ahead(a, b) ==
    \/ ~UpToDate(b, a)
    \/ /\ UpToDate(a, b) /\ UpToDate(b, a)
       /\ \/ Local(a) /\ ~Local(b)
          \/ Local(a) = Local(b) /\ a < b

Rank(n) == Cardinality({m \in Nodes : m # n /\ up[m] /\ Ahead(m, n)})

LiveLeader(t) == \E m \in Nodes : up[m] /\ state[m] = "L" /\ term[m] >= t

Init ==
    /\ term = [n \in Nodes |-> 0]
    /\ voted = [n \in Nodes |-> Nil]
    /\ state = [n \in Nodes |-> "F"]
    /\ log = [n \in Nodes |-> <<>>]
    /\ votes = [n \in Nodes |-> {}]
    /\ up = [n \in Nodes |-> TRUE]
    /\ quiet = [n \in Nodes |-> FALSE]
    /\ crashes = 0
    /\ lastLeader = Nil
    /\ committed = {}

Timeout(n) ==
    /\ up[n]
    /\ ~quiet[n]
    /\ ~LiveLeader(term[n])
    /\ quiet' = [quiet EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<term, voted, state, log, votes, up, crashes, lastLeader, committed>>

PreVoteGranted(n) ==
    Quorum({m \in Nodes : up[m] /\ (m = n \/ (quiet[m] /\ UpToDate(n, m)))})

Elect(n) ==
    /\ up[n]
    /\ state[n] # "L"
    /\ quiet[n]
    /\ term[n] < MaxTerm
    /\ LadderMode => state[n] = "F" /\ Rank(n) = 0 /\ ~\E m \in Nodes : m # n /\ up[m] /\ state[m] = "C" /\ UpToDate(m, n)
    /\ PreVoteGranted(n)
    /\ term' = [term EXCEPT ![n] = term[n] + 1]
    /\ state' = [state EXCEPT ![n] = "C"]
    /\ voted' = [voted EXCEPT ![n] = n]
    /\ votes' = [votes EXCEPT ![n] = {n}]
    /\ UNCHANGED <<log, up, quiet, crashes, lastLeader, committed>>

CanGrant(n, m) ==
    /\ n # m
    /\ up[n] /\ up[m]
    /\ state[n] = "C"
    /\ term[n] >= term[m]
    /\ term[n] > term[m] \/ voted[m] \in {Nil, n}
    /\ UpToDate(n, m)
    /\ m \notin votes[n]

GrantVote(n, m) ==
    /\ CanGrant(n, m)
    /\ term' = [term EXCEPT ![m] = term[n]]
    /\ voted' = [voted EXCEPT ![m] = n]
    /\ state' = [state EXCEPT ![m] = IF term[n] > term[m] THEN "F" ELSE state[m]]
    /\ votes' = [votes EXCEPT ![n] = votes[n] \cup {m}]
    /\ UNCHANGED <<log, up, quiet, crashes, lastLeader, committed>>

BecomeLeader(n) ==
    /\ up[n]
    /\ state[n] = "C"
    /\ Quorum(votes[n])
    /\ state' = [state EXCEPT ![n] = "L"]
    /\ lastLeader' = n
    /\ log' = [log EXCEPT ![n] = IF Len(log[n]) < MaxLog THEN Append(log[n], term[n]) ELSE log[n]]
    /\ UNCHANGED <<term, voted, votes, up, quiet, crashes, committed>>

Abandon(n) ==
    /\ up[n]
    /\ state[n] = "C"
    /\ ~Quorum(votes[n])
    /\ \A m \in Nodes : ~CanGrant(n, m)
    /\ state' = [state EXCEPT ![n] = "F"]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ UNCHANGED <<term, voted, log, up, quiet, crashes, lastLeader, committed>>

ClientWrite(n) ==
    /\ up[n]
    /\ state[n] = "L"
    /\ Len(log[n]) < MaxLog
    /\ log' = [log EXCEPT ![n] = Append(log[n], term[n])]
    /\ UNCHANGED <<term, voted, state, votes, up, quiet, crashes, lastLeader, committed>>

Replicate(n, m) ==
    /\ n # m
    /\ up[n] /\ up[m]
    /\ state[n] = "L"
    /\ term[m] <= term[n]
    /\ \/ log[m] # log[n]
       \/ quiet[m]
       \/ state[m] # "F"
       \/ term[m] # term[n]
    /\ log' = [log EXCEPT ![m] = log[n]]
    /\ voted' = [voted EXCEPT ![m] = IF term[m] < term[n] THEN Nil ELSE voted[m]]
    /\ term' = [term EXCEPT ![m] = term[n]]
    /\ state' = [state EXCEPT ![m] = "F"]
    /\ quiet' = [quiet EXCEPT ![m] = FALSE]
    /\ UNCHANGED <<votes, up, crashes, lastLeader, committed>>

Crash(n) ==
    /\ up[n]
    /\ crashes < MaxCrashes
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ state' = [state EXCEPT ![n] = "F"]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<term, voted, log, quiet, lastLeader, committed>>

Restart(n) ==
    /\ ~up[n]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ quiet' = [quiet EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<term, voted, state, log, votes, crashes, lastLeader, committed>>

Prefix(l, i) == SubSeq(l, 1, i)

Commit(n) ==
    /\ up[n]
    /\ state[n] = "L"
    /\ \E i \in 1..Len(log[n]) :
        /\ log[n][i] = term[n]
        /\ Quorum({m \in Nodes : Len(log[m]) >= i /\ Prefix(log[m], i) = Prefix(log[n], i)})
        /\ committed' = committed \cup {<<j, log[n][j], term[n]>> : j \in 1..i}
        /\ committed' # committed
    /\ UNCHANGED <<term, voted, state, log, votes, up, quiet, crashes, lastLeader>>

Next ==
    \/ \E n \in Nodes :
        \/ Timeout(n) \/ Elect(n) \/ BecomeLeader(n) \/ ClientWrite(n) \/ Crash(n) \/ Restart(n) \/ Commit(n) \/ Abandon(n)
    \/ \E n, m \in Nodes : GrantVote(n, m) \/ Replicate(n, m)

Fairness ==
    /\ \A n \in Nodes :
        /\ WF_vars(Timeout(n)) /\ WF_vars(Elect(n)) /\ WF_vars(BecomeLeader(n))
        /\ WF_vars(Restart(n)) /\ WF_vars(Abandon(n))
    /\ \A n, m \in Nodes : WF_vars(GrantVote(n, m)) /\ WF_vars(Replicate(n, m))

Spec == Init /\ [][Next]_vars

FairSpec == Spec /\ Fairness

TypeOK ==
    /\ term \in [Nodes -> 0..MaxTerm]
    /\ voted \in [Nodes -> Nodes \cup {Nil}]
    /\ state \in [Nodes -> {"F", "C", "L"}]
    /\ votes \in [Nodes -> SUBSET Nodes]
    /\ up \in [Nodes -> BOOLEAN]
    /\ quiet \in [Nodes -> BOOLEAN]

ElectionSafety ==
    \A a, b \in Nodes : state[a] = "L" /\ state[b] = "L" /\ term[a] = term[b] => a = b

LeaderCompleteness ==
    \A e \in committed : \A n \in Nodes :
        (state[n] = "L" /\ term[n] > e[3]) => (Len(log[n]) >= e[1] /\ log[n][e[1]] = e[2])

CommittedUnique ==
    \A a, b \in committed : a[1] = b[1] => a[2] = b[2]

LeaderRecurs == []<>(\E n \in Nodes : up[n] /\ state[n] = "L")

=============================================================================
