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
