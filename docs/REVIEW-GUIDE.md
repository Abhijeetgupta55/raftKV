# REVIEW-GUIDE — how to read this code and defend it

This is your reading map for the Raft work (M2–M4). It is written to be
read *with the code open*, in the order given. For each unit it lists the
files to read, the invariants to verify and exactly where they are
enforced, every deliberate deviation from the paper, and a set of
checkpoint questions you must be able to answer cold — with model answers.

> Note: the "path-to-10 doc §6" checkpoint list referenced in the brief
> was not present in the repo during this session, so §6 below is a
> generated equivalent — a per-layer interview-defense set. Replace or
> merge with the canonical list if it resurfaces.

---

## §1. The layering — read this first

Read in order:
1. `internal/raft/core.go` (the pure state machine — no goroutines, no
   I/O beyond the log/state files it is handed).
2. `internal/raft/node.go` (the event loop that owns the single `core` and
   turns everything — ticks, RPCs, proposals — into messages).
3. `internal/raft/service.go` + `transport.go` (gRPC in/out).

**The one structural idea that makes the whole thing testable and correct:**
the `core` is single-threaded and synchronous. Its RPC handlers *return*
responses and *accumulate* outbound messages in an outbox; they never send.
The `Node` loop (`node.go`, `dispatch` → `afterStep`) drains that outbox
and applies committed entries **only after the core call returns** — and
the core persists term/vote/log *before* returning. Therefore persistence
structurally precedes visibility: no vote, append, or apply can ever be
observed before it is durable. Verify this by reading `Node.run` →
`afterStep`: the `takeOutbox`/`takeCommitted`/`takeReadOutcomes` drains all
happen after `dispatch(m)` has returned.

Invariants to confirm here:
- The core has **no** `go`, `time.`, or network calls. (`grep -n "go \|time\.\|Transport" internal/raft/core.go` — should be empty of those.)
- Every `saveState`/`store.append` call precedes the `return` that exposes its effect.

---

## §2. Election core (unit ③, committed) — `core.go`

Invariants and where enforced:
- **One vote per term, durable.** `handleRequestVote`: the `canVote`
  check + `saveState(term, votedFor)` *before* setting
  `resp.VoteGranted`. → `TestOneVotePerTerm`, `TestFollowerGrantsVoteAndPersistsIt`.
- **Election restriction (§5.4.1).** `logUpToDate` — later last-term wins,
  else longer log wins. → `TestElectionRestriction`.
- **Term discipline.** Every handler adopts a higher term via `stepDown`
  first; `stepDown` deliberately does **not** reset the election timer
  (discovering a bigger term is not a heartbeat). → `TestHigherTermAdoptedEvenWhenVoteDenied`.
- **Denied votes / higher-term responses don't reset the timer** — a
  stale-logged node must not be able to suppress elections. →
  `TestDeniedVoteDoesNotResetElectionTimer`.
- **Randomized timeout re-rolled every reset.** `resetElectionTimer`. →
  `TestElectionTimeoutStaysWithinBounds`.

---

## §3. Replication (unit ④) — `replication.go`, commit rule in `core.go`

Read `handleAppendEntries` (`replication.go`) top to bottom, then
`handleAppendResponse` and `maybeAdvanceCommit`.

Invariants:
- **Log matching.** `prevLogIndex/prevLogTerm` check before appending.
  Two failure modes handled distinctly: log **too short** (reject, hint
  `conflictIndex = lastLogIndex+1`) and **term mismatch** at prevIndex
  (reject, hint the whole conflicting term so the leader backtracks by a
  term, not an entry). → `TestFollowerRejectsWhenLogTooShort`,
  `TestFollowerConflictHintsNameTheIntrudingTerm`,
  `TestLeaderBacktracksByWholeTerm`.
- **Followers never truncate committed entries.** →
  `TestFollowerRefusesToTruncateCommittedEntries`.
- **Idempotent / out-of-order appends.** A duplicate or stale append does
  not corrupt the log. → `TestDuplicateAppendIsIdempotent`,
  `TestStaleLeaderHeartbeatRejected`.
- **Figure-8 commit rule.** `maybeAdvanceCommit` advances `commitIndex`
  only to entries of the **current term** reaching a majority; earlier
  terms commit transitively once a current-term entry does. → `TestFigure8`,
  `TestCommitRequiresMajorityNotHope`.
- **Term-start no-op.** `maybeWinElection` appends a NOOP so something of
  the leader's term exists to commit (and to serve as ReadIndex's commit
  floor). → `TestLeaderAppendsNoopOnElection`.
- **In-order apply with lastApplied.** `Node.applyEntry` advances
  `n.applied` monotonically and only applies NORMAL entries to the SM.

Deviations from the paper: fast backtracking by conflict term (an
optimization from §5.3's last paragraph / the etcd tradition, not the
naive decrement-by-one); the leader no-op is committed immediately in a
single-node cluster.

---

## §4. Production Raft (M3) — `raftsnap.go`, `membership.go`, `leadership.go`

- **Compaction + InstallSnapshot.** `maybeCompact` (in `node.go`) snapshots
  the SM and calls `core.compact` once `CompactionThreshold` applied
  entries pile up; a follower too far behind the compacted log is caught
  up via `InstallSnapshot` (`raftsnap.go`), with index translation so
  post-snapshot indices line up. → `TestInstallSnapshotCatchesUpNewMember`.
- **Single-server membership (§4.1).** `membership.go`: one change at a
  time (a second is refused while the first is uncommitted), and a config
  entry takes effect **on append, not on commit** — hence
  `noteAppendedConfigs` on the follower path and the recompute in
  `truncateAndAppend` if a config entry is truncated away. →
  `TestMembershipAddNodeCatchesUp`.
- **Pre-vote (§9.6).** `leadership.go`: a would-be candidate probes for
  votes at `term+1` without mutating any state, and campaigns for real
  only after a majority says yes — so a partitioned node's inflated term
  never disrupts a healthy leader. **See War Story WS-1 in DESIGN.md** for
  the recency-counter bug this hid. → `TestPreVoteGrantedOnlyAfterLeaderSilence`.
- **Leadership transfer (§3.10).** `transferLeadership` stops taking
  proposals, ensures the target's log is complete, then `TimeoutNow` tells
  it to campaign immediately (skipping pre-vote and its own timeout). →
  `TestLeadershipTransfer`.

---

## §5. Linearizable reads (M4) — `readindex.go`

Read the file's header comment; it is the clearest statement of why a
naive leader read is unsafe. Invariants:
- A read records `commitIndex` as its read index **only after** a
  current-term entry has committed (`termAt(commitIndex) == term`), else
  `ErrLeaderNotReady`.
- The leader confirms it is *still* leader by collecting a majority of
  **fresh** heartbeat acks (the `minSeq`/`appendSeq` machinery
  distinguishes acks to sends issued after the read arrived).
- The read is served only once the SM has applied through the read index
  (`Node.resolveRead` + `fireAppliedWaiters`).
→ `TestLinearizableReadReflectsCommittedWrite`.

**Gap (see STATUS.md):** client-session exactly-once dedup is not
implemented, and the "demonstrate the naive stale read first" test is
absent.

---

## §6. Checkpoint questions (be able to answer these cold)

**Q1. Why can the `core` have no locks and no goroutines, and why is that
a feature rather than a limitation?**
A. Because exactly one goroutine (the `Node` loop) ever touches it; every
input is serialized through `msgc`. That makes the consensus logic a
deterministic function of its input sequence — hand-drivable in tests with
`tick()` and direct calls, no sleeps, no flakiness — and it removes a
whole class of data races from the hardest-to-reason-about code. The
concurrency lives in `node.go`, which is small and boring.

**Q2. Walk me through why an acknowledged write can never be lost, even on
a `kill -9` immediately after the ack.**
A. A leader appends to its log and fsyncs before counting the entry; it
acks the client only after a majority has done the same (each follower
fsyncs before responding — `store.append` precedes the response return).
So an acked write is on a majority's disk. Any future leader must have
been elected by a majority, which by the election restriction
(`logUpToDate`) overlaps the storing majority in at least one node whose
log is at least as complete — so the entry survives into the new leader's
log. Election Safety + Leader Completeness together.

**Q3. Explain Figure 8 and how your commit rule avoids it.**
A. An old-term entry replicated to a majority is *not* safe to commit: a
later leader could still overwrite it, because a node with a
higher-term-but-shorter log can win election and truncate it. So a leader
never commits an entry just because it reached a majority *in an earlier
term*; it waits until an entry **of its own current term** reaches a
majority, at which point everything before it commits transitively.
Enforced in `maybeAdvanceCommit`; proven by `TestFigure8`.

**Q4. Your pre-vote broke failover. What exactly was wrong and what does
the fix teach?**
A. (WS-1.) The stickiness guard reused `electionElapsed`, which resets not
only on leader contact but also when the node starts its own pre-campaign
or grants a vote. Two partition survivors thus perpetually vetoed each
other's probes. The fix is a separate `ticksSinceLeader` reset only by
genuine leader contact. Lesson: one counter answering two predicates is a
bug waiting for those predicates to diverge.

**Q5. Why is a naive read from the leader not linearizable, and what does
ReadIndex add?**
A. A leader partitioned from the majority doesn't know it's been deposed;
a new leader on the other side may have committed newer writes, so the old
leader would serve stale data. ReadIndex fixes it by (1) pinning the read
to `commitIndex` only after a current-term entry has committed, (2) proving
still-leadership via a fresh majority of heartbeat acks, and (3) waiting
for the SM to apply through that index before serving.

**Q6. Why single-server membership changes instead of joint consensus?**
A. Adding/removing exactly one server guarantees any old-config majority
and any new-config majority overlap in at least one node, so two disjoint
quorums (two leaders) can't form during the transition — no joint phase
needed. Arbitrary multi-server changes break that overlap, which is the
failure joint consensus exists to prevent. Enforced by the one-change-at-a-
time rule in `proposeConfChange`.

**Q7. What in this codebase is NOT proven, and how would you attack it
first?** (Answer honestly from STATUS.md: server wiring, session dedup,
M5/M6/M7.) The honest answer is the strong answer in an interview.
