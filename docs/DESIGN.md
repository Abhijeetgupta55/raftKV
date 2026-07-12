# raftkv design

This document records the system's requirements, the position it takes on
the classic distributed-systems trade-offs, and the honest list of what it
does not yet do. It grows with each milestone; sections marked *(planned)*
describe committed design direction, not implemented behavior.

## Requirements

- Store key → value pairs; keys are UTF-8 strings ≤ 4 KiB, values are
  opaque bytes ≤ 1 MiB.
- Serve `Put` / `Get` / `Delete` over gRPC.
- **Durability** *(done, M1)*: an acknowledged write survives a process
  crash — fsynced write-ahead log, snapshots, verified by a
  kill-the-process test.
- **Fault tolerance** *(planned, M2–M3)*: a shard of N replicas keeps
  serving writes while a minority ⌊(N−1)/2⌋ of its nodes are down, with no
  acknowledged write lost and no split-brain.
- **Linearizable reads** *(planned, M4)*: reads that can never observe
  stale state, via ReadIndex or leader leases.
- **Horizontal scale** *(planned, M5)*: keys partition across shards;
  each shard is an independent Raft group.
- **Verified correctness** *(planned, M6)*: fault injection plus a
  linearizability checker over recorded histories.

### Non-goals

- No web UI, auth, or multi-tenancy — this is infrastructure, and every
  line of that kind of surface area would dilute the systems focus.
- No SQL, secondary indexes, or query language. It is a KV store.
- No raw-throughput contest with Redis. The engineering target is
  correctness under failure, measured by the fault-injection harness (M6),
  not by benchmark marketing.
- No third-party consensus library. Raft is implemented from the paper —
  that is the point of the project.

## Position on the CAP spectrum

raftkv chooses **CP**: linearizable writes through Raft, at the price of a
minority partition refusing writes (they cannot reach quorum). For a
system of record, refusing service beats silently diverging; AP-style
eventual consistency pushes conflict resolution onto every client, which
is the wrong default for a general KV store. Read consistency gets its own
treatment in M4 (ReadIndex), because naive leader reads are *not*
automatically linearizable — a deposed leader can serve stale data without
knowing it has been replaced.

## Current architecture (through Milestone 1)

Layers, each independently testable, with deliberately boring seams:

1. **State machine** (`internal/storage.MemStore`) — a mutex-guarded map.
   This is what Raft will later replicate, so it knows nothing about
   networking, persistence, or consensus. `Put`/`Get` copy value slices so
   no caller can alias the store's internal data.
2. **Storage engine** (`internal/storage.DurableStore`) — wraps the state
   machine with durability:
   - Every mutation is encoded as a self-contained binary *command*,
     appended to a **write-ahead log**, and **fsynced before the client
     sees an acknowledgment**. The in-memory apply happens only after the
     disk write succeeds, so an error always means "not stored", never
     "maybe stored".
   - Records carry strictly increasing sequence numbers and CRC32-C
     checksums. The sequence number becomes the Raft log index in M2.
   - **Snapshots** (checksummed full dumps, written tmp-then-rename for
     atomicity) bound WAL growth and recovery time; after a snapshot the
     WAL rotates and covered segments are deleted whole.
   - **Recovery** = newest snapshot + WAL tail replay. Replay and live
     writes funnel through the same `applyCommand`, so they cannot
     diverge. Format details and rationale: ADR 0003.
3. **Service** (`internal/server`) — implements the `kv.v1` gRPC service.
   Owns request validation (empty keys, size limits) and the mapping from
   storage results to wire responses; storage errors surface as gRPC
   `INTERNAL`.
4. **Entrypoints** (`cmd/server`, `cmd/cli`) — flag parsing, wiring, and
   graceful shutdown.

### The torn-write problem (why WAL recovery is subtle)

A crash can interrupt an append anywhere, leaving a half-written record at
the log's tail. Recovery must answer: is an unreadable record a torn write
(harmless — it was never acknowledged, because acknowledgment follows
fsync) or corruption of acknowledged data (must not be ignored)? The
policy: an invalid record at the tail of the **newest** segment is torn —
truncate and continue; an invalid record in any **finished** segment, or a
gap in sequence numbers, refuses startup. Within the newest segment,
corruption *before* the tail is indistinguishable from a torn write; that
ambiguity is a documented limitation, pinned by a test.

## Failure model

- **Crash-stop of the single node (tolerated, M1):** acknowledged writes
  survive any process death — kill -9 mid-write included — via
  fsync-before-ack and snapshot + WAL-tail recovery. Verified by an
  integration test that repeatedly hard-kills a real server process
  during a write storm (crossing snapshot and rotation boundaries) and
  audits every acknowledged write after restart.
- **Node loss / partitions (planned, M2):** Raft leader election with
  randomized timeouts, heartbeat failure detection, quorum commit;
  split-brain prevented by election safety (at most one leader per term).
- **Out of scope at every milestone:** Byzantine failures (nodes lying,
  disks returning plausible-but-wrong data — CRCs catch bit rot, not
  adversaries).

## Sharding (M5): multiple independent Raft groups

Horizontal scale comes from partitioning the keyspace across independent
Raft groups ("shards"), each with its own leader, log, snapshots, and
membership. Raft is completely unaware of this: a group replicates opaque
bytes exactly as before; sharding is purely a routing layer above it.

**Partitioning: hash, not range.** A key's shard is
`fnv1a64(key) % shardCount` (`kvraft.ShardFor`). Chosen because it gives
uniform load with zero knowledge of the key distribution, O(1) stateless
routing (any node, any client can compute it), and no shard-map metadata
service. The honest cost: no efficient range scans (a scan would fan out
to every shard) and no locality for related keys. Range partitioning wins
when scans matter and enables targeted shard-splitting of hot ranges, but
needs split/merge management and a shard-map service — the wrong
complexity for a KV API that today has no scan operation. If scans arrive,
this decision gets revisited (ADR-worthy).

**Shard count is static** (`--shards N`, fixed at cluster birth; every node
must agree). Changing it re-maps `hash % N` for almost every key, so
resharding = dual-write migration or consistent hashing with virtual
nodes — documented future work, deliberately not built: the systems
substance here is multi-group consensus, not online resharding.
A `shards` file in the data root pins the count and refuses startup on
mismatch, so a mis-flagged restart cannot silently route keys to the
wrong group's log.

**Placement is static and total**: every node hosts a replica of every
shard (same member set per group). With N nodes and S shards that is S
independent quorums over the same machines — real independence of
elections/commit, not real hardware isolation. A placement map (subset of
nodes per shard) is future work; the `Group` field on every Raft RPC and
the group-keyed `raft.Service` already support it.

**Routing is two-layer.** Server-side: any node computes the shard from
the key and serves it from its local replica of that group — leader check
included; a follower rejects with a hint naming both the shard and its
leader (`shard=K leader_addr=…`). Client-side: the CLI computes nothing;
it caches leader address **per shard** from those hints and invalidates
the entry on the next NotLeader for that shard. One shard's failover
does not evict another shard's perfectly good cached leader.

**One process, many groups — the starvation question.** Each group's
event loop is its own goroutine (that is the per-group apply bound), but
*time* is shared: a single ticker goroutine drives `Tick()` on every
local group in round-robin (each node is created with `TickInterval: 0`).
One misbehaving group cannot stall another group's elections by hogging a
timer, because ticks are delivered as non-blocking enqueues onto each
group's own message channel. What the shared ticker costs: tick delivery
skew of at most the loop iteration time (microseconds for tens of groups),
which is noise against 10–20-tick election timeouts.

## The nemesis (M6 part 1): fault injection against real processes

The verification harness (`internal/nemesis`, scenarios in
`test/nemesis`) attacks a cluster of REAL `kvserver` processes over real
TCP. Its parts:

**Interposition: userspace TCP proxies, not iptables.** Every directed
edge (i → j) of the cluster graph gets its own proxy; node i's `--peers`
names j's proxy address, so every inter-node byte crosses the harness.
Faults: cut an edge (connections severed, new ones refused), heal, inject
per-chunk delay. Why proxies over iptables/WFP: no root, no OS-specific
firewall state to leak on a crashed test, works identically on the
Windows dev box and Linux CI, and per-directed-edge granularity falls out
of the design instead of requiring source-port gymnastics. The honest
limitation: this is **connection-level** asymmetry, not packet-level. A
cut edge i → j stops i *initiating* to j, but responses on a connection j
initiated to i still flow (they ride j's TCP stream). True one-way packet
loss needs kernel help (iptables/tc/WFP) — documented, not faked. The
asymmetric scenario is therefore precisely "a node that can be reached
but cannot initiate", which is a real production failure (broken egress,
half-open NAT).

**Process faults.** kill -9 (`Process.Kill`) and suspend/resume — SIGSTOP
/ SIGCONT on unix, `NtSuspendProcess`/`NtResumeProcess` (the debug API)
on Windows: every thread freezes, sockets stay open, and the process
keeps believing whatever it believed — the zombie-leader fault.

**Seeding.** Every scenario derives its workload op sequence (and the
soak its fault schedule) from one printed seed; `-args -seed=N` reruns
those exact inputs. Honesty note: with real processes, a seed reproduces
the *inputs* bit-for-bit (op sequences, fault types/order/durations), not
the OS's interleaving — leader identity and message timing remain
nondeterministic, as in any real-system harness.

**Workload + history.** Concurrent session clients (exactly-once writes
via client_id/serial) storm the cluster and record every operation with
[invoke, return] wall-clock windows into a JSONL history
(`internal/nemesis/history.go`) — the format the Porcupine checker
consumes in M6 part 2. A write whose retries all timed out is recorded
`unknown: true`: it may still commit later, so the checker must treat it
as possibly-applied. Retries of one session write fold into one logical
op (dedup makes them a single application).

**RUN-2 verification (pre-Porcupine).** Clients own disjoint key ranges,
so the legal final value of every key is exactly computable: the last
acknowledged write, or any *later-serial* write whose ack the nemesis ate
(session dedup's high-water mark means an *earlier* unknown write can
never override a later ack — see `Workload.keyState`). Zero acknowledged
loss, stated precisely. Shared-key linearizability is Porcupine's job.

## The proof (M6 part 2): Porcupine over recorded histories

Every nemesis scenario's history now runs through a linearizability
checker (`internal/nemesis/checker.go`, using Porcupine) before the
scenario may pass. The model: each key is an independent register —
linearizability composes over independent objects, so the history
partitions by key and the search stays tractable. Acked ops must
linearize inside their [invoke, return] window; **unknown** (timed-out)
puts keep an infinite window — if one never actually executed, the
checker can always place it after every observation, so phantoms cannot
false-alarm; unobserved gets/deletes constrain nothing and are dropped.
Timestamp ties resolve return-before-invoke (WS-2 — the sound direction,
because Return stamps postdate completion and Invoke stamps predate
submission).

Honest bounds of the model: session dedup gives the system a property
the register model doesn't encode (an unknown older-serial write can
never apply over a newer acked one), so the checker is marginally more
permissive than the system — sound, no false alarms, slightly less
strict. Encoding serials into the model is documented future work.

**The checker is itself verified twice over.** Synthetic histories pin
the model (stale reads and lost acked writes rejected; legal concurrency
and phantom unknowns accepted). And the **mutation check**
(`TestMutationCheckerCatchesDisabledReadBarrier`) proves the whole
pipeline end-to-end against real processes: a build with the read
barrier deliberately disabled (`RAFTKV_UNSAFE_NO_READ_BARRIER=1`, a
screaming test-only hook) must produce a history the checker rejects.
A green run of the harness is only meaningful because this red run is
demonstrably red. The soak additionally proves the nemesis bites: node
logs must show faults forcing re-elections during the storm, or the soak
fails regardless of the checker's verdict.

## The five safety properties, mapped to code (M2–M3)

Raft's correctness rests on five properties (paper §5.2–§5.4). Where each
is enforced in this codebase:

1. **Election Safety** — at most one leader per term. Enforced by
   at-most-one-vote-per-term: `handleRequestVote`
   (`internal/raft/core.go`) grants only when `votedFor` is unset or
   already the candidate, and **persists the vote via `saveState` before
   returning the granted response**, so a crash can't forget it and vote
   twice. Test: `TestOneVotePerTerm`.
2. **Leader Append-Only** — a leader never overwrites or deletes its own
   log. Leaders only ever `append` (`maybeWinElection`'s no-op, `propose`);
   truncation lives exclusively on the follower path
   (`truncateAndAppend`).
3. **Log Matching** — if two logs share an entry at (index, term), they
   are identical up to there. Enforced by the `prevLogIndex/prevLogTerm`
   consistency check in `handleAppendEntries` (`replication.go`). Tests:
   `TestFollowerRejectsWhenLogTooShort`,
   `TestFollowerTruncatesConflictingSuffix`.
4. **Leader Completeness** — a committed entry is present in every future
   leader's log. Enforced by the election restriction (§5.4.1) in
   `logUpToDate` (`core.go`): a voter refuses any candidate whose log is
   less complete than its own. Test: `TestElectionRestriction`.
5. **State Machine Safety** — no two nodes apply different commands at the
   same index. Follows from 1–4 plus the Figure-8 commit rule: a leader
   commits an entry only once an entry **of its own term** has reached a
   majority (`maybeAdvanceCommit`), never counting replicas of a stale-term
   entry as committed. Test: `TestFigure8`, `TestCommitRequiresMajorityNotHope`.

## War stories

Bugs found by the harness, kept as cautionary tales (symptom → root cause
→ fix → regression test). Per project policy, no bug is fixed silently.

### WS-1: pre-vote deadlocks failover (found 2026-07-07)

**Symptom.** The first multi-node integration test to partition the
leader — `TestClusterSurvivesLeaderFailure` — hung: after isolating the
leader, the two surviving followers never elected a replacement, timing
out after 3s with "no unique leader". Single-node and non-partitioned
tests were all green, so the base election and replication logic were
fine; the fault was specific to pre-vote under a real partition (the unit
tests disable pre-vote to drive the direct-campaign path, so this was
pre-vote's first genuine exercise).

**Root cause.** The pre-vote stickiness guard in `handlePreVote` refused
to grant a probe while `electionElapsed < electionTimeout` — the intent
being "don't disrupt a cluster whose leader we've heard from recently."
But `electionElapsed` is reset by *three* events, only one of which is
leader contact: it also resets when the node **starts its own
pre-campaign** and when it **grants a vote**. So the two survivors, each
periodically starting a pre-campaign (which reset its own timer) and
receiving the other's probe, were almost never simultaneously past their
election timeout. Each kept vetoing the other's probe as if a live leader
still existed — a stable livelock. Pre-vote, whose entire purpose is
availability, was *destroying* availability.

**Fix.** Split the two meanings apart. Added `ticksSinceLeader` to the
core, incremented every tick and reset to zero **only on genuine leader
contact** (a valid `AppendEntries` in `replication.go` or `InstallSnapshot`
in `raftsnap.go`) — never by starting a campaign or granting a vote. The
pre-vote guard now refuses only while `ticksSinceLeader <
ElectionTicksMin`. Once the leader vanishes, this counter climbs
monotonically on every survivor regardless of their own election
activity, so the veto lifts on all of them together and the election
proceeds. This is the CheckQuorum-style leader-recency test the
dissertation describes (§9.6), which the original conflation had
approximated incorrectly.

**Regression tests.** `TestPreVoteGrantedOnlyAfterLeaderSilence` (a
follower keeps refusing while the leader is fresh, but grants after
`ElectionTicksMin` ticks of silence — even across a pre-campaign that
resets `electionElapsed`), `TestGenuineLeaderContactResetsRecency` (a
heartbeat re-arms the veto), and the integration test that first exposed
it.

**Lesson.** A single counter serving two distinct predicates ("has my own
election timer expired?" and "have I heard from a leader recently?") is a
latent bug the moment those predicates need to diverge. Pre-vote is
exactly that moment.

### WS-2: the checker's first catch was itself (found 2026-07-12)

**Symptom.** The mutation check — run a build with the read barrier
deliberately disabled, watch the linearizability checker reject the
resulting history — failed in the wrong direction: the deposed leader
demonstrably served a stale read (`v1` after `v2` had committed on the
majority, both recorded), and Porcupine **accepted** the history. A
checker that blesses a provably broken build is worse than no checker: it
manufactures false confidence at the exact moment the project claims
"verified correctness."

**Root cause.** Timestamp ties from a coarse clock. The recorder stamps
ops with `time.Since(start)`; Windows' monotonic clock ticks at ~0.5ms,
so the second put's *Return* and the stale get's *Invoke* landed on the
identical nanosecond. Porcupine treats touching intervals as concurrent —
correctly, given its input — and a "concurrent" stale read may legally
linearize before the put it failed to observe. The violation vanished
into clock granularity. The same history with distinct timestamps was
already rejected by the model's unit test (`TestCheckerFlagsStaleRead`) —
synthetic tests can't catch what only a real clock does.

**First fix — itself a bug (act two).** The initial repair exploited
which side of each stamp reality sits on (Return stamps postdate
completion, Invoke stamps predate submission, so a tie must order
return-before-invoke) via the interval transform `Call' = 2t+1,
Return' = 2t`. It passed the mutation check — and then the very next
full-suite run flagged `NemesisPartitionLeader` as NOT LINEARIZABLE. The
node logs showed a textbook clean failover, which smelled like a false
alarm, and triage confirmed it: an op that invokes and returns within a
single 0.5ms clock tick (loopback RPCs routinely do) got `Call' = 2t+1 >
Return' = 2t` — an **inverted interval**, undefined input that let
Porcupine manufacture a violation. A checker that can cry wolf is as
useless as one that's blind.

**Final fix.** Kill ties at the source: `Recorder.Now()` is strictly
monotonic (atomic compare-and-swap bump). Soundness: the CAS serializes
the stamping events in their true order; since every Return stamp is an
upper bound of a completion and every Invoke stamp a lower bound of a
submission, stamp order implies a valid real-time order — no false
constraints, no ties, and every op keeps `Call < Return`. The checker
consumes raw stamps again; the triage rig (history persisted before the
verdict, violating key's ops dumped on failure) stays, because the next
alarm must arrive with its own evidence.

**Regression tests.** `TestMutationCheckerCatchesDisabledReadBarrier`
(caught act one), `TestNemesisPartitionLeader` under the checker gate
(caught act two), and the synthetic checker suite — all green after the
final fix.

**Lesson.** Verify the verifier — then verify the fix to the verifier.
Both bugs lived in the seam between correct components: a correct model
fed degraded timestamps, then a correct model fed malformed intervals.
Seams are where verification tooling rots, and the mutation check plus
the false-alarm triage discipline are the two clamps holding this one
shut.

## Known limitations (through Milestone 1)

- **Single node.** No replication, no failover. Durability protects
  against process death, not disk death.
- **One fsync per write.** Throughput is bounded by disk sync latency
  (hundreds of writes/sec locally). Group commit — batching concurrent
  appends into one fsync — is the standard fix, deferred until after Raft
  lands to avoid tuning the same code twice.
- **Snapshots pause writes** for the duration of the dump (the write lock
  is held). Copy-on-write iteration is the known fix if state grows.
- **Newest-segment ambiguity**: corruption before the tail of the active
  WAL segment is treated as a torn write, silently dropping whatever
  followed it (see above).
- **Directory fsync is a no-op on Windows** (unsupported by the OS); file
  creations/renames rely on the NTFS metadata journal. On Linux — where
  CI runs and any real deployment would live — directories are fsynced.
- **Plaintext gRPC.** No TLS; the server binds to loopback by default and
  must not be exposed beyond a trusted network.
- **No backpressure or rate limiting.** A hostile client can fill memory
  with 1 MiB values; there is no eviction and no total-size cap.
- **CI cannot detect stale generated proto code** (accepted in ADR 0001).

### Added / deferred at Milestones 2–4 (see STATUS.md for the full ledger)

- **Raft is wired into the server only in cluster mode.** With `--peers`,
  `cmd/server` runs the Raft-backed KV service (`internal/kvraft`) over
  real gRPC, proven end-to-end by the `test/cluster` kill-9 failover test.
  Without `--peers` it runs the unchanged M1 durable single-node store.
  Deliberate: a single node needs durability, not consensus. A one-command
  way to *bootstrap* a fresh multi-node cluster's membership (beyond static
  `--peers`) is not yet provided.
- **`Delete` reports `existed` best-effort.** It reads the leader's applied
  state just before proposing, so under concurrency the flag can be
  slightly stale. A linearizable answer needs `StateMachine.Apply` to
  return a result to the proposer; deferred.
- **Read-path sessions not implemented.** Writes are exactly-once via the
  session table; reads are not cached per session (they are idempotent, so
  this affects only a hypothetical read-your-writes-token API, not
  correctness).
- **Leader hint travels in the gRPC status message** (`shard=… leader_addr=…`,
  string-parsed by the client) rather than a typed error detail — a
  pragmatic choice to avoid a proto change; a structured detail is the
  clean version.

### Added at Milestone 5 (sharding)

- **Shard count is fixed for the life of the data dir.** Pinned in
  `dataRoot/shards`; changing it means a new cluster and a migration.
  Online resharding (consistent hashing / shard splits with dual-write
  cutover) is sketched in the sharding section and deliberately not built.
- **Placement is total** — every node replicates every shard. Real
  placement (subsets of nodes per shard, balancing, rebalancing on
  add/remove) is future work; the group-keyed plumbing supports it.
- **No cross-shard operations of any kind.** Each key is linearizable
  within its shard; nothing orders writes on different shards relative to
  each other. Multi-key transactions would need 2PC or similar above Raft.
- **A future scan/range API would fan out to every shard** — the known
  cost of hash partitioning, accepted because the API has no scans today.
- **Snapshot restore reloads the whole state machine** (`Restore` replaces
  it wholesale). Fine at test scale; a streaming/chunked InstallSnapshot
  is the known fix for large state.
- **Race detector runs only in CI.** Local dev on Windows without a
  64-bit cgo toolchain cannot run `-race`; the Linux CI job is the
  authoritative gate. Do not treat a local green run as a race-free
  guarantee.
