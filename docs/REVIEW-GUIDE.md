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

---

## §7. Server wiring + M4 sessions (added session 3)

Read in order:
1. `internal/kvraft/statemachine.go` — the command envelope
   `(clientID, serial, inner)` and the replicated `stateMachine`
   (KV map + session table), implementing `raft.StateMachine`.
2. `internal/kvraft/service.go` — `KVService` on top of `raft.Node`:
   `Put`/`Delete` → `Propose`, `Get` → `ReadBarrier`, `toStatus` for
   leader hints.
3. `cmd/server/main.go` — the standalone-vs-cluster branch and how the
   `kvv1` + `raftv1` services share one listener.
4. `cmd/cli/main.go` — `leaderClient.retry` following `leader_addr=` hints.
5. `test/cluster/cluster_test.go` — the real-process acceptance test.

Invariants and where enforced:
- **A write is durable + replicated before it is acknowledged.**
  `KVService.Put` blocks on `node.Propose`, which returns only once the
  entry is applied on this (leader) node — i.e. committed by a majority.
  Proven end-to-end by `TestClusterFailoverNoAckedLoss` (kill -9 the
  leader; every acked write survives).
- **Reads are linearizable.** `KVService.Get` calls `node.ReadBarrier`
  before touching the state machine; a deposed leader fails the barrier.
- **A follower never silently serves/accepts.** `toStatus` maps
  `raft.NotLeaderError` to `FailedPrecondition` with `leader_addr=`; the
  CLI/test client retries there.
- **Exactly-once writes.** `stateMachine.Apply` drops a `(clientID, serial)`
  it has already applied; the table is snapshot-included. Proven by the
  `internal/kvraft` test pair.
- **M1 durability is untouched.** No `--peers` → the original
  `storage.DurableStore` path; `test/crash` still passes unmodified.

Deviations / shortcuts (all in STATUS.md Known Limitations): `Delete.Existed`
is best-effort; read-path session caching is not implemented (writes dedup,
reads are idempotent); the leader hint travels in the status *message*
(string-parsed) rather than a typed gRPC detail, to avoid a proto change.

### Checkpoint questions (session-3 additions)

**Q8. When `KVService.Put` returns nil to the client, what is guaranteed?**
A. The command is in the Raft log on a majority of nodes and has been
applied to this leader's state machine — so it survives the loss of any
minority, including this leader. `Propose` doesn't return until the entry
commits and applies; a timeout instead returns an *unknown* outcome, which
is exactly why writes carry `(client_id, serial)` for a safe retry.

**Q9. A client sends Put, times out, and retries — but between the two a
different client overwrote the key. Why doesn't the retry clobber the newer
value?**
A. Both of the first client's attempts carry the same `(client_id, serial)`.
The state machine recorded that serial as applied on the first delivery, so
the retry's `Apply` is a no-op — the newer client's write stands. Without
the session id (client_id 0) the retry *would* clobber; that contrast is
the `TestRetryWithoutSessionClobbers` → `TestSessionDedupPreventsStaleRetry`
pair.

**Q10. Why must the session table be in the snapshot?**
A. If a replica restored from a snapshot forgot which serials it had
applied, a duplicate arriving after the snapshot boundary would re-apply —
breaking exactly-once across compaction/InstallSnapshot. `snapshotImage`
carries both the data and the sessions; `TestSnapshotIncludesSessions`
pins it.

**Q11. Why keep a standalone durable path instead of always running Raft?**
A. A single node needs durability, not consensus; paying election +
replication overhead for one node is waste, and it lets the M1 crash test
keep exercising the WAL/snapshot/torn-write engine directly. `--peers`
selects Raft; its absence selects the M1 store. Same binary, same KV API.

---

## §8. Sharding — multi-Raft (RUN 1)

Read in order:
1. DESIGN.md "Sharding (M5)" — the decisions (hash vs range, static count,
   total placement, shared ticker) with trade-offs.
2. `internal/kvraft/sharding.go` — `ShardFor`, `NewSharded` (per-shard
   dirs, shard-count pin, shared `tickLoop`), `ShardedKV` routing.
3. `internal/kvraft/service.go` — the `shard=K leader_addr=A` hint.
4. `cmd/cli/main.go` — `leaderClient` per-shard cache + invalidation.
5. `test/cluster/sharded_test.go` — the real-process acceptance gate.

Invariants and where enforced:
- **A key routes to exactly one group, forever.** `ShardFor` is a pure
  function of (key, shardCount); shardCount is pinned on disk
  (`pinShardCount`) and a mismatched restart refuses to boot. →
  `TestShardDistribution`, `TestShardCountPinRefusesMismatch`.
- **Groups are independent.** Each has its own log, snapshots, elections,
  quorum (`Group` stamped on every RPC; `raft.Service` routes by it). →
  `TestShardsSurviveNodeLossIndependently`.
- **One clock, many groups.** `ShardedServer.tickLoop` fans a single
  ticker out as non-blocking enqueues; no per-group timer goroutines.
- **Zero acked loss under multi-shard node death.** →
  `TestShardedClusterFailoverNoAckedLoss` (real processes, kill -9).

### Checkpoint questions (RUN-1 gate)

**Q12. Trace a Put whose shard leader moved mid-flight.**
A. Client sends Put(k) to its cached leader for shard g=ShardFor(k). That
node's group-g replica is no longer leader, so `Propose` returns
`NotLeaderError`; `toStatus` turns it into FailedPrecondition with
`shard=g leader_id=… leader_addr=…` (the *new* leader, learned from the
rejecting node's `leaderID`). The client invalidates its stale cache entry
for g (and only g), stores the hinted address, and retries there. If the
hint is empty (election still in flight), the client rotates nodes until a
rejection carries a hint or the op lands. The write itself is safe under
all of this because the entry commits in exactly one group's log — retries
are deduped by (client_id, serial) in that group's state machine.

**Q13. What does multi-group buy, and what does it cost?**
A. Buys: write throughput scales with groups (S leaders commit in
parallel instead of one leader serializing everything); failure blast
radius shrinks (one group's election stalls 1/S of the keyspace); logs,
snapshots and recovery parallelize. Costs: **cross-shard atomicity is
gone** — two keys on different shards commit in two independent logs, so
there is no single index that orders them; a multi-key transaction needs a
coordination protocol *above* Raft (2PC with per-shard participant logs,
or deterministic ordering à la Calvin). That is exactly why transactions
are a stretch goal, not a checkbox: the primitive this milestone builds is
per-shard linearizability, and pretending it composes for free across
shards would be false.

**Q14. How do S groups in one process avoid starving each other?**
A. Three mechanisms. (1) Each group's event loop is its own goroutine, so
a group busy applying can't block another's message processing — the
scheduler preempts. (2) Time is delivered by one shared ticker as
non-blocking channel enqueues (`Node.Tick` → buffered `msgc`); a slow
group drops behind on its *own* clock rather than delaying anyone else's
(worst case it misses ticks when its buffer is full, which only delays its
own election timeout). (3) Persistence is per-group files, so one group's
fsync doesn't hold another group's lock — they contend only on the shared
disk, which is honest hardware contention, not a software serialization
point.
