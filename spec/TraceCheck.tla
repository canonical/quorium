----------------------------- MODULE TraceCheck -----------------------------
EXTENDS Naturals, Sequences, FiniteSets, TraceData

VARIABLE i

S == Trace[i]

L == INSTANCE Ladder WITH
    Nodes <- TraceNodes,
    MaxTerm <- 1000000000,
    MaxLog <- 1000000000,
    MaxCrashes <- 1000000000,
    LadderMode <- FALSE,
    DCOf <- LAMBDA n : 0,
    term <- S.term,
    voted <- S.voted,
    state <- S.role,
    log <- S.log,
    votes <- [n \in TraceNodes |-> {}],
    up <- S.up,
    quiet <- [n \in TraceNodes |-> FALSE],
    crashes <- 0,
    lastLeader <- 0,
    committed <- S.committed

Init == i = 1

Next == i < Len(Trace) /\ i' = i + 1

Prefix(l, k) == SubSeq(l, 1, k)

IsPrefix(a, b) == Len(a) <= Len(b) /\ Prefix(b, Len(a)) = a

Step(P(_, _)) == i < Len(Trace) => P(Trace[i], Trace[i + 1])

ElectionSafety == L!ElectionSafety

LeaderCompleteness == L!LeaderCompleteness

CommittedUnique == L!CommittedUnique

LogTermsBounded == \A n \in TraceNodes : \A k \in 1..Len(S.log[n]) : S.log[n][k] <= S.term[n]

LeaderVotedForItself == \A n \in TraceNodes : S.role[n] = "L" => S.voted[n] = n

TermsRise(a, b) == \A n \in TraceNodes : b.term[n] >= a.term[n]

VotesStay(a, b) ==
    \A n \in TraceNodes : (b.term[n] = a.term[n] /\ a.voted[n] # 0) => b.voted[n] = a.voted[n]

OwnTermAppends(a, b) ==
    \A n \in TraceNodes :
        (a.role[n] = "L" /\ b.role[n] = "L" /\ a.term[n] = b.term[n]) =>
            /\ IsPrefix(a.log[n], b.log[n])
            /\ \A k \in (Len(a.log[n]) + 1)..Len(b.log[n]) : b.log[n][k] = b.term[n]

Originated(a, b) ==
    \A m \in TraceNodes : \A k \in 1..Len(b.log[m]) :
        (k > Len(a.log[m]) \/ a.log[m][k] # b.log[m][k]) =>
            \/ b.role[m] = "L" /\ b.log[m][k] = b.term[m]
            \/ \E n \in TraceNodes \ {m} :
                \E x \in {a.log[n], b.log[n]} : Len(x) >= k /\ Prefix(x, k) = Prefix(b.log[m], k)

TermsNeverDecrease == Step(TermsRise)

VotesAreStable == Step(VotesStay)

LeadersAppendOwnTerm == Step(OwnTermAppends)

EntriesComeFromLeaders == Step(Originated)

=============================================================================
