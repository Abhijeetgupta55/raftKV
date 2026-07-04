# ADR 0001: Commit generated protobuf code to the repository

**Status:** accepted (2026-07-04)

## Context

`protoc` plus two Go plugins are required to turn `proto/kv/v1/kv.proto`
into Go code. Requiring every contributor and every CI run to install a
specific protoc toolchain is friction, and version skew between protoc
installations produces noisy diffs.

## Decision

Generated files (`*.pb.go`, `*_grpc.pb.go`) are committed next to their
`.proto` source. `make proto` regenerates them; only people changing the
schema need the protoc toolchain installed. etcd and many other Go
projects make the same choice.

## Consequences

- `go build` works on a fresh clone with nothing but the Go toolchain.
- CI does not install protoc and therefore cannot detect a stale
  generated file; whoever edits the `.proto` must re-run `make proto`
  and commit the result. Accepted as a known limitation while the
  schema is small and rarely edited.
