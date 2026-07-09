# STATUS — honest milestone ledger

Written after an autonomous build session on 2026-07-07. This file is
deliberately pessimistic: it reports what is *proven*, what merely
*exists*, and what is *untouched*, so the review that follows starts from
the truth rather than from optimism. "Proven" means backed by a test that
would fail if the property broke; "implemented" means the code is there
and compiles and is exercised indirectly but has no dedicated test;
"scaffolding" means types/signatures exist but no working behavior;
"none" means not started.

All claims below verified with `go test ./...` green (see run commands at
the end). The race detector could not run locally (this Windows box has no
64-bit cgo toolchain), but CI runs `go test -race -count=1 ./...` on Linux
and is the authoritative race gate.

## Summary table

| Milestone | State | One-line truth |
|---|---|---|
| M0 Foundation | ✅ done | Committed before this session. |
| M1 Durability | ✅ done | Committed; crash test passes (14–24s). |
| M2 Raft core | ✅ **done + proven** | Election + replication + multi-node integration + failover. **Now wired into the real `kvserver` binary** and proven by a real-process, over-TCP, `kill -9`-the-leader test with zero acknowledged loss. |
| M3 Production Raft | ✅ **implemented + proven** | Compaction/InstallSnapshot, single-server membership, pre-vote, leadership transfer — each has a passing integration test. |
| M4 Linearizable reads + sessions | ✅ **done + proven** | ReadIndex proven linearizable. **Client-session dedup now implemented** — session table inside the replicated state machine, snapshot-included, with the double-apply-demonstrated-then-fixed test pair. (Read-path session caching and the naive-stale-read demo test remain as noted below.) |
| M5 Sharding | ✅ **done + proven** | Multi-Raft: hash partitioning (FNV-1a % N), shared ticker, per-shard dirs, shard-count pin, shard-aware leader hints with per-shard CLI cache. Proven by a real-process kill-9 test — node hosting replicas of all 4 shards dies under a cross-shard storm, zero acked loss anywhere. |
| M6 Verification | 🟠 foundation only | In-memory network supports seeded **partition** injection (one nemesis primitive). No crash/SIGSTOP nemesis, no Porcupine checker, nothing in CI. **RUN 2–3 territory; not started.** |
| M7 Observability | 🟠 proto only | `proto/admin` generated. No metrics/logging/`kvadmin`/bench. **RUN 4; not started.** |

> **Session-3 boundary (2026-07-07, later).** This run took the ladder from
> "Raft library, unwired" to "Raft wired into the real server + M4 sessions",
> stopping cleanly at the M4 boundary. M5/M6/M7 are untouched — see the
> Session-3 addendum below the original notes.

## What changed this session

1. **Brought the uncommitted M2④–M4 working-tree code to a building,
   green state.** Fixed three breaks: an unused import in `readindex.go`,
   a stale `openLog(dir)` → `openLog(dir, base)` test signature, and
   pre-vote defaulting *on* which broke the unit-③ election tests (those
   tests drive the direct-campaign path; `newTestCore` now sets
   `DisablePreVote: true`, matching their intent — pre-vote is tested
   separately).

2. **Found and fixed a pre-vote failover liveness bug** — see the war
   story in `DESIGN.md`. Root cause: the pre-vote stickiness guard keyed
   off `electionElapsed`, which a node *also* resets when it starts its
   own pre-campaign or grants a vote, so two survivors of a leader
   partition perpetually vetoed each other and no new leader was ever
   elected. Fixed with a dedicated `ticksSinceLeader` recency counter,
   reset only by genuine leader contact. Regression tests:
   `TestPreVoteGrantedOnlyAfterLeaderSilence`,
   `TestGenuineLeaderContactResetsRecency`, and the integration test
   `TestClusterSurvivesLeaderFailure`.

3. **Wrote the first multi-node integration harness**
   (`internal/raft/cluster_test.go`): an in-memory network transport with
   seeded per-node timing and symmetric partition injection, plus a real
   replicated KV state machine (with gob Snapshot/Restore). New passing
   tests: single-leader election, replication, leader-failure failover +
   reintegration, linearizable ReadIndex, membership add + catch-up,
   InstallSnapshot to a member joining after compaction, leadership
   transfer.

## Precise gaps (in priority order for the next session)

- **M2 server wiring (biggest gap).** The Raft library is complete and
  tested but **nothing in `internal/server` or `cmd/` imports
  `internal/raft`** — the running `kvserver` binary is still the M1
  single-node `DurableStore`. Wiring the KV gRPC service to a `raft.Node`
  (Put → `Propose`, Get → `ReadBarrier` then read the state machine),
  surfacing `NotLeaderError.LeaderID` as a client leader hint, and giving
  `cmd/server` multi-node flags is the top of the next backlog.
- **M4 session dedup.** No `client_id`/`serial` fields in `kv.proto`, no
  session table in the state machine, no snapshot inclusion. Exactly-once
  is therefore unproven; a timed-out `Propose` that later commits would
  double-apply on retry. Needs proto change + regeneration (`make proto`,
  needs protoc).
- **M4 pedagogy.** The "demonstrate the naive stale read, then fix it"
  test pair the roadmap calls for is absent; only the fixed behavior is
  tested.
- **M3 edge tests.** `remove-the-leader` and config-entry-truncation
  recompute have implementations (`membership.go`,
  `truncateAndAppend`) but no dedicated integration test.
- **M5/M6/M7** as in the table.

## Suggested commit plan

The work is one working-tree blob right now (no commits were made, by
request). A clean history:

1. `fix(raft): openLog base arg + readindex import; disable pre-vote in
   base election tests` — the three build/green fixes to
   `readindex.go`, `log_test.go`, `replication_test.go`, `core_test.go`.
2. `fix(raft): pre-vote failover liveness — recency counter, not election
   timer` — `core.go` (`ticksSinceLeader` field + tick maintenance),
   `replication.go` and `raftsnap.go` (reset on leader contact),
   `leadership.go` (guard change), plus the two regression tests in
   `core_test.go`. Reference the DESIGN.md war story in the message.
3. `test(raft): multi-node integration harness (election, replication,
   failover)` — `cluster_test.go` core (transport, kvSM, cluster,
   election/replication/failover/ReadIndex tests).
4. `test(raft): M3 integration — membership, InstallSnapshot, transfer` —
   the `addNode` helper and the three M3 tests appended to
   `cluster_test.go`.
5. `docs: STATUS, REVIEW-GUIDE, DESIGN war story + property map` — this
   file, `REVIEW-GUIDE.md`, and the `DESIGN.md` edits.

(4 and 5 can each stand alone; 1–2 should land in order.)

## Run commands (start here when reviewing)

```sh
# Full suite (Linux CI additionally passes -race):
make test            # or: go test ./... -count=1

# The failover demo, verbose — watch a partitioned leader get replaced:
go test ./internal/raft/ -run TestClusterSurvivesLeaderFailure -v -count=1

# The bug's regression test in isolation:
go test ./internal/raft/ -run TestPreVoteGrantedOnlyAfterLeaderSilence -v -count=1

# Race gate (needs a 64-bit cgo toolchain; runs in CI on Linux):
make race
```

---

## Session-3 addendum (2026-07-07, later) — server wiring + M4 sessions

### Shipped and proven

1. **Raft wired into the real `kvserver` binary.** New `internal/kvraft`
   package: a replicated KV state machine (`stateMachine`) implementing
   `raft.StateMachine`, and a `KVService` implementing the client-facing
   `kvv1.KVServer` gRPC API on top of a `raft.Node` — writes go through
   `Propose`, reads through `ReadBarrier` (linearizable), a request landing
   on a follower is rejected with a `leader_addr=` hint.
2. **Multi-node launch over real gRPC.** `cmd/server` now branches:
   `--peers id@addr,...` runs a Raft cluster (KV + `raftv1` on one
   listener, `raft.GRPCTransport` between nodes); no `--peers` keeps the
   **unchanged M1 durable standalone path** (so `test/crash` still passes
   as-is).
3. **CLI follows leader hints.** `cmd/cli` `-addr` accepts a comma list and
   retries across nodes, following `leader_addr=` on `FailedPrecondition`,
   so it survives a failover mid-session. `-client`/`-serial` flags drive
   exactly-once writes.
4. **Real-process acceptance test** (`test/cluster`): builds the binary,
   launches **3 processes over TCP**, write storm, `kill -9` the leader,
   proves a new leader is elected and **every acknowledged write survives**,
   then restarts the dead node and confirms the cluster stays healthy with
   all data intact. Runs in ~26s; skipped under `-short`.
5. **M4 client-session dedup** (`internal/kvraft`): a per-client
   highest-serial table lives inside the replicated state machine and is
   included in snapshots; `Apply` drops a duplicate serial. Test pair:
   `TestRetryWithoutSessionClobbers` (demonstrates the lost-update hazard) →
   `TestSessionDedupPreventsStaleRetry` (fixed) + `TestSnapshotIncludesSessions`.
6. **Scripted demo**: `scripts/demo-cluster.sh` (one-terminal automated run
   + printed manual 3-terminal commands) for the README GIF.

### Precise gaps opened/remaining (priority order)

- **M5 sharding — not started.** Single group only (`group 0`). No router,
  key partitioning, per-shard dirs, or shared ticker.
- **M6 nemesis + Porcupine — not started.** The `internal/raft` partition
  primitive is the only fault injector; no crash/SIGSTOP, no linearizability
  checker, nothing in CI.
- **Read-path sessions.** `Get` does not yet cache/serve per-session
  results; only writes dedup. Fine for correctness (reads are idempotent),
  noted for completeness.
- **`Delete.Existed` is best-effort** — read from the leader's applied
  state just before proposing, not linearizable. See DESIGN Known
  Limitations.
- **Naive-stale-read demonstration test** still absent (only the fixed
  ReadIndex behavior is tested).

### Fresh commit plan (this session's changes, no commits made)

1. `feat(storage): export EncodeCommand/DecodeCommand for the Raft layer`
   — `internal/storage/command.go`.
2. `feat(kvraft): replicated KV state machine with session dedup` —
   `internal/kvraft/statemachine.go` + `statemachine_test.go` (the M4 pair).
3. `feat(kvraft): KV gRPC service over raft.Node with leader hints` —
   `internal/kvraft/service.go`.
4. `feat(server): launch a Raft cluster with --peers; keep standalone
   durable path` — `cmd/server/main.go`.
5. `feat(cli): follow leader hints across nodes; session flags` —
   `cmd/cli/main.go`.
6. `test(cluster): real-process kill -9 leader failover, zero acked loss` —
   `test/cluster/cluster_test.go`.
7. `docs+demo: STATUS/REVIEW-GUIDE/DESIGN updates, demo-cluster.sh`.

### Run commands (verify it yourself)

```sh
# Full suite (Linux CI adds -race):
go test ./... -count=1

# The real-process failover acceptance test, verbose:
go test ./test/cluster/ -run TestClusterFailoverNoAckedLoss -v -count=1 -timeout 120s

# The M4 dedup demonstrated-then-fixed pair:
go test ./internal/kvraft/ -run 'Retry|Dedup|Snapshot' -v -count=1

# The by-hand 3-node demo (README GIF):
bash scripts/demo-cluster.sh
```

---

## RUN-1 addendum (2026-07-10) — M5 sharding (multi-Raft)

### Shipped and proven

1. **Design written before code** — DESIGN.md "Sharding (M5)": hash
   partitioning (FNV-1a 64 % shardCount) over range, with the trade-off
   documented honestly (no range scans / no locality; range partitioning
   sketched as the alternative for when scans arrive). Static shard count,
   pinned in `dataRoot/shards` — a restart with a different `--shards`
   **refuses to start** rather than silently mis-routing keys. Static
   total placement (every node replicates every shard); a placement map is
   documented future work the `Group`-keyed plumbing already supports.
2. **Multi-group runtime** (`internal/kvraft/sharding.go`):
   `NewSharded` hosts S `raft.Node`s in one process — per-shard
   `shard-N/` data dirs, per-group state machines, and **one shared
   ticker** driving every group (`TickInterval: 0` per node), so S groups
   don't run S timer goroutines and no group can starve another's clock.
   Elections, snapshots, commit — all per-group, no cross-talk (the Raft
   layer was already group-scoped; `Group` is stamped on every RPC).
3. **Two-layer routing.** Server-side: `ShardedKV` hashes the key and
   delegates to the local replica of that group; follower rejections now
   carry `shard=K leader_addr=A`. Client-side: the CLI caches leaders
   **per shard** and invalidates an entry only when that same shard
   rejects again (`cmd/cli` `leaderClient`).
4. **`cmd/server --shards N`** — cluster mode always runs the sharded
   runtime (shards=1 is the unsharded cluster; same code path, one
   group). Standalone M1 path untouched.
5. **Acceptance tests, all green:**
   - `TestShardDistribution` (unit): determinism + rough uniformity.
   - `TestShardCountPinRefusesMismatch` (unit): the mis-flagged-restart
     guard.
   - `TestShardsSurviveNodeLossIndependently` (in-process, real gRPC on
     loopback): 3 nodes × 4 groups, stop a node leading ≥1 group — every
     shard keeps serving old data and new writes.
   - `TestShardedClusterFailoverNoAckedLoss` (REAL PROCESS): 3 processes
     × 4 shards, cross-shard write storm, kill -9 a node hosting replicas
     of all four shards → independent failovers, **zero acknowledged loss
     on any shard**, node rejoins from its per-shard dirs and catches up.
     The rejoin leg doubles as the per-shard crash-recovery check: each
     group's log replays from `shard-N/` after a hard kill.

### Honest notes / deferred

- **Per-shard M1 crash-test rerun**: the M1 `test/crash` suite exercises
  the standalone `DurableStore` engine, which cluster mode does not use;
  the equivalent per-shard guarantee (hard-kill → per-group log replay →
  zero acked loss) is covered by the rejoin leg of the sharded
  real-process test above, not by rerunning `test/crash` against shard
  dirs. Stated plainly so nobody believes the M1 harness itself ran
  against shards.
- **Dynamic resharding not built** (per plan): changing `--shards`
  requires a fresh cluster. Sketch in DESIGN.md.
- **Demo script still launches 1 shard**; add `--shards 4` by hand if you
  want the multi-group demo (kept minimal deliberately).
- Race gate remains CI-on-Linux (no local 64-bit cgo toolchain).

### Fresh commit plan (RUN-1 changes only, on top of the session-3 plan)

1. `feat(kvraft): multi-Raft sharding — hash partitioning, shared ticker,
   shard-count pin` — `internal/kvraft/sharding.go`.
2. `feat(kvraft): shard-aware leader hints` — `internal/kvraft/service.go`
   (shard field + hint format).
3. `feat(server,cli): --shards flag; per-shard leader cache in the CLI` —
   `cmd/server/main.go`, `cmd/cli/main.go`.
4. `test(kvraft): shard distribution, pin guard, in-process multi-group
   node loss` — `internal/kvraft/sharding_test.go`.
5. `test(cluster): real-process multi-shard kill -9, zero acked loss` —
   `test/cluster/sharded_test.go` + the `extra` flags hook in
   `cluster_test.go`.
6. `docs: M5 design, STATUS, REVIEW-GUIDE §8`.

### Run commands (RUN-1 verification)

```sh
# Everything:
go test ./... -count=1 -timeout 300s

# M5 unit + in-process:
go test ./internal/kvraft/ -run 'TestShard' -v -count=1

# M5 real-process multi-shard failover (the acceptance gate):
go test ./test/cluster/ -run TestShardedClusterFailoverNoAckedLoss -v -count=1 -timeout 180s

# By-hand multi-shard cluster (3 terminals; note --shards on every node):
# bin/kvserver --id 1 --listen 127.0.0.1:5501 --peers 1@127.0.0.1:5501,2@127.0.0.1:5502,3@127.0.0.1:5503 --data-dir d/1 --shards 4
# (idem id 2/3) ... then: bin/kvcli -addr 127.0.0.1:5501,127.0.0.1:5502,127.0.0.1:5503 put somekey v
```
