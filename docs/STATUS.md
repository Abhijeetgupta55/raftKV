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
| M2 Raft core | ✅ **done + proven** | Election + replication + multi-node integration + failover, all green. A real pre-vote failover bug was found and fixed this session. |
| M3 Production Raft | ✅ **implemented + proven** | Compaction/InstallSnapshot, single-server membership, pre-vote, leadership transfer — each now has a passing integration test. |
| M4 Linearizable reads | 🟡 **partial** | ReadIndex implemented and proven linearizable. Client-session dedup is **not implemented**. Naive-stale-read demonstration test absent. |
| M5 Sharding | 🟠 scaffolding | `Group` field + group-routed `raft.Service` exist. No router, no key partitioning, no shared-ticker server, no multi-group binary. |
| M6 Verification | 🟠 foundation only | The in-memory network supports seeded **partition** injection (one nemesis primitive). No crash/SIGSTOP nemesis, no Porcupine checker, no recorded histories, nothing in CI. |
| M7 Observability | 🟠 proto only | `proto/admin` generated. No metrics, no trace-id logging, no `kvadmin`, no benchmarks. |

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
