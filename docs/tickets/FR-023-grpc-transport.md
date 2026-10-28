---
id: FR-023
title: gRPC protocol crate and node transport
status: todo
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

- [ ] Two nodes exchange `Join` and `Heartbeat` over real TCP, and over turmoil in a simulation test.
- [ ] A 4 MiB frame batch round-trips through `Replicate` as one message, not one per frame.
- [ ] A peer with an unsupported protocol version is refused at `Join`.
- [ ] The workspace builds on a machine without `protoc`.
