# raftkv design

This document records the system's requirements, the position it takes on
the classic distributed-systems trade-offs, and the honest list of what it
does not yet do. It grows with each milestone; sections marked *(planned)*
describe committed design direction, not implemented behavior.

## Requirements

- Store key → value pairs; keys are UTF-8 strings ≤ 4 KiB, values are
  opaque bytes ≤ 1 MiB.
- Serve `Put` / `Get` / `Delete` over gRPC.
- **Durability** *(planned, M1)*: an acknowledged write survives a process
  crash.
- **Fault tolerance** *(planned, M2)*: a shard of N replicas keeps serving
  writes while a minority ⌊(N−1)/2⌋ of its nodes are down, with no
  acknowledged write lost and no split-brain.
- **Horizontal scale** *(planned, M3)*: keys partition across shards via
  consistent hashing; each shard is an independent Raft group.

### Non-goals

- No web UI, auth, or multi-tenancy — this is infrastructure, and every
  line of that kind of surface area would dilute the systems focus.
- No SQL, secondary indexes, or query language. It is a KV store.
- No raw-throughput contest with Redis. The engineering target is
  correctness under failure, which is measured by the fault-injection
  harness (M4), not by benchmark marketing.
- No third-party consensus library. Raft is implemented from the paper —
  that is the point of the project.

## Position on the CAP spectrum

raftkv chooses **CP**: linearizable writes through Raft, at the price of a
minority partition refusing writes (they cannot reach quorum). For a
system of record, refusing service beats silently diverging; AP-style
eventual consistency pushes conflict resolution onto every client, which
is the wrong default for a general KV store. Read consistency options
(leader reads vs. stale follower reads) will be documented in M2 when they
exist.

## Current architecture (Milestone 0)

Three layers, each independently testable, with deliberately boring seams:

1. **Storage** (`internal/storage`) — a mutex-guarded map. This is the
   state machine that Raft will later replicate, so it knows nothing about
   networking, persistence, or consensus. `Put`/`Get` copy value slices so
   no caller can alias the store's internal data.
2. **Service** (`internal/server`) — implements the `kv.v1` gRPC service.
   Owns request validation (empty keys, size limits) and the mapping from
   storage results to wire responses. Nothing else, so it survives the
   layers beneath it changing completely.
3. **Entrypoints** (`cmd/server`, `cmd/cli`) — flag parsing, wiring, and
   graceful shutdown. No logic worth testing beyond what the layers
   already cover.

The API's representation choices (string keys / bytes values, miss as
`found: false`, limits as server policy) are argued in
[ADR 0002](DECISIONS/0002-client-api-shape.md).

## Failure model

- **Today (M0):** none. A crash loses everything (in-memory only); a
  single node is a single point of failure. This is the honest baseline
  the next two milestones exist to fix.
- **M1** adds crash-stop tolerance for a single node: a write-ahead log
  fsynced before acknowledgment, snapshots to bound replay time, and
  recovery = snapshot + WAL tail.
- **M2** adds crash and partition tolerance across replicas: Raft leader
  election with randomized timeouts, heartbeats for failure detection, and
  quorum commit — split-brain is prevented by the election safety
  property (at most one leader per term).

Byzantine failures (nodes lying, disk corruption presented as valid data)
are out of scope at every milestone.

## Known limitations (Milestone 0)

- **No persistence.** All data is lost on restart, by design at this
  stage.
- **Single node.** No replication, no failover.
- **Plaintext gRPC.** No TLS; the server binds to loopback by default and
  must not be exposed beyond a trusted network.
- **No backpressure or rate limiting.** A hostile client can fill memory
  with 1 MiB values; there is no eviction and no total-size cap.
- **CI cannot detect stale generated proto code** (accepted in
  [ADR 0001](DECISIONS/0001-commit-generated-proto-code.md)).
