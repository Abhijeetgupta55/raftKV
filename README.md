# raftkv

A distributed key-value store built from first principles in Go: gRPC API,
hand-rolled write-ahead log, Raft consensus implemented from the paper (no
consensus libraries), and consistent-hash sharding across multiple Raft
groups.

This is backend infrastructure — there is no web UI. Clients talk to the
cluster over gRPC (`PUT` / `GET` / `DELETE`) via a CLI or any generated
client. The project's focus is **correctness under failure**: crash
recovery, leader failover, and network partitions, verified by a
fault-injection test harness.

## Status

| Milestone | State |
|---|---|
| 0 — Single-node KV over gRPC (server, CLI, tests, CI) | ✅ done |
| 1 — Persistence: WAL + snapshots + crash recovery | planned |
| 2 — Raft consensus: election, replication, failover | planned |
| 3 — Sharding: consistent hashing, multiple Raft groups | planned |
| 4 — Fault-injection harness + linearizability checking | planned |

The store is currently **single-node and in-memory**; data does not survive
a restart. That is what Milestones 1–2 exist to fix, in that order.

## Target architecture

```
        clients (CLI / generated stubs)
                 │  gRPC: Put / Get / Delete
                 ▼
        ┌─────────────────────────────┐
        │   Router / shard resolver   │  consistent hashing → shard
        └─────────────────────────────┘
                 │
     ┌───────────┼───────────────────┐
     ▼           ▼                   ▼
 ┌────────┐  ┌────────┐          ┌────────┐
 │Shard A │  │Shard B │   ...    │Shard N │   each shard = one Raft
 │(Raft   │  │(Raft   │          │(Raft   │   group of 3+ replicas
 │ group) │  │ group) │          │ group) │
 └────────┘  └────────┘          └────────┘
     │  within a shard:
     ▼
 leader ──replicates log──▶ followers
     │ apply committed entries
     ▼
 KV state machine → WAL + snapshots on disk
```

Design rationale, trade-offs, and known limitations live in
[docs/DESIGN.md](docs/DESIGN.md); individual decisions are recorded in
[docs/DECISIONS/](docs/DECISIONS/).

## Quickstart

Requires Go 1.26+. (`make proto` additionally needs `protoc` with the Go
plugins, but generated code is committed, so regular builds don't.)

```sh
make build          # builds bin/kvserver and bin/kvcli
make test           # unit + integration tests
make race           # same, under the race detector

bin/kvserver                          # serves on 127.0.0.1:5001
bin/kvcli put greeting "hello"        # → OK
bin/kvcli get greeting                # → hello
bin/kvcli delete greeting             # → deleted
bin/kvcli get greeting                # → exit code 1, "key not found"
```

On Windows, run `make` from Git Bash.

## Command reference

```
kvcli [-addr host:port] put <key> <value>    store value under key
kvcli [-addr host:port] get <key>            print value; exit 1 if absent
kvcli [-addr host:port] delete <key>         remove key (idempotent)

kvserver [-listen host:port]                 run a node (default 127.0.0.1:5001)
```

Keys are UTF-8 strings up to 4 KiB; values are opaque bytes up to 1 MiB.

## Repository layout

```
proto/       gRPC schema + committed generated code
cmd/server   node entrypoint
cmd/cli      kvcli client
internal/
  storage/   state machine (in-memory store; WAL and snapshots land here)
  server/    gRPC service: validation + response mapping
docs/        DESIGN.md and ADRs
```

## Testing

`go test -race ./...` runs everything CI runs: unit tests for the storage
engine and service layer, plus an end-to-end test over an in-memory gRPC
connection. CI (GitHub Actions) enforces gofmt, `go vet`, the build, and
the race-detector test run on every push.
