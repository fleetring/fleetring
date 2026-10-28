---
id: FR-023
title: gRPC protocol crate and node transport
status: done
priority: P0
size: M
milestone: M5 Daemon foundations
depends_on: [FR-022]
adrs: [ADR-0019]
---

# FR-023: gRPC protocol crate and node transport

## Goal

Define the wire contract between nodes and run it over tonic, both on real networks and under simulation.

## Scope

- **`fleetring-proto`:** `.proto` files for package `fleetring.v1`, with the `Control`, `Replication`, and `Database` services from ADR-0019, compiled with tonic and prost. Code generation needs no system `protoc`.
- **Transport:** a tonic server in `fleetringd` and a pooled client per peer, with timeouts, retries with backoff for idempotent calls, and optional TLS through rustls.
- **Simulation connector:** tonic over turmoil's TCP streams, so that the same services run in the simulation harness (FR-024).
- **Opaque batches:** positions and payloads are `bytes` fields interpreted only by the engine adapter (ADR-0016). No SQLite types appear in the `.proto` files.
- **Version check at `Join`:** a peer with an unsupported protocol version is refused with a clear error.

## Acceptance criteria

- [x] Two nodes exchange `Join` and `Heartbeat` over real TCP, and over turmoil in a simulation test.
- [x] A 4 MiB frame batch round-trips through `Replicate` as one message, not one per frame.
- [x] A peer with an unsupported protocol version is refused at `Join`.
- [x] The workspace builds on a machine without `protoc`.

## Progress

- 2026-10-28: Done.
  - `fleetring-proto`: `proto/fleetring/v1/fleetring.proto` defines `Control`, `Replication` (`Replicate` is a two-way stream: batches and checkpoint markers out, acknowledgements back), and `Database`. Positions and payloads are `bytes`. `build.rs` compiles it with protox, so no `protoc` is needed: verified by building with `protoc` hidden from `PATH`. `check_protocol` refuses a peer whose major version differs, naming both versions.
  - `fleetringd::transport`: a `Peers` pool with one lazily connected channel per peer, connect and request timeouts, servers and clients that accept messages up to 64 MiB (tonic's default of 4 MiB would reject a 4 MiB batch), and `retry` with deterministic exponential backoff for idempotent calls. It retries only `UNAVAILABLE`, `DEADLINE_EXCEEDED`, and `RESOURCE_EXHAUSTED`.
  - `fleetringd::transport::sim`, behind the `simulation` feature, runs tonic over turmoil's TCP for the harness (FR-024).
  - Tests: `Join` and `Heartbeat` over real TCP and over turmoil, a 4 MiB batch acknowledged through `Replicate`, a protocol-version refusal, and retry behavior under paused time.
  - Deferred: TLS between nodes (rustls). It needs a certificate story and nothing in v0.1 requires it; it's in the backlog.
