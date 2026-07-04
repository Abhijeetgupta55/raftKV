# ADR 0002: Client API shape — string keys, bytes values, miss as a flag

**Status:** accepted (2026-07-04)

## Context

The client-facing surface (`Put`/`Get`/`Delete`) is the hardest part of
the system to change later, so its representation choices deserve a
written rationale.

## Decision

1. **Keys are `string`, values are `bytes`.** Values are opaque payloads —
   the store has no business interpreting them, and `bytes` keeps binary
   payloads first-class. Keys as proto3 `string` (enforced UTF-8) keep the
   CLI, logs, and debugging humane. etcd uses byte keys; that flexibility
   buys nothing here and costs readability everywhere keys appear.

2. **A `Get` miss returns `found: false`, not gRPC `NOT_FOUND`.** In a KV
   workload a miss is a normal answer, not an exceptional condition.
   Reserving status errors for real failures means a client error-handling
   path signals something actually went wrong — which matters more once
   "not the leader, retry over there" errors exist (Raft milestone).

3. **Size limits are server policy, not schema.** Key ≤ 4 KiB,
   value ≤ 1 MiB, enforced at the service layer with `INVALID_ARGUMENT`.
   Limits belong where they can change without a wire-format break.

## Consequences

- Clients check a boolean instead of matching on status codes for the
  common miss case.
- An empty value and an absent key are distinguishable (`found` covers
  the ambiguity `bytes` alone would leave).
- If byte keys are ever genuinely needed, that is a `kv.v2` service —
  an accepted cost for the readability win.
