# ADR-0019: Nodes talk gRPC over tonic

- **Status:** Proposed (2026-10-28)
- **Date:** 2026-10-28
- **Related:** ADR-0013, ADR-0015, ADR-0016, ADR-0017, ADR-0020

## Context

Nodes exchange three kinds of traffic: control calls (join, heartbeat, leases, positions), replication streams (frame batches one way, acknowledgements the other), and snapshots (large and occasional). The transport needs streaming in both directions, typed messages that can evolve across versions, and TLS, and it must run inside the simulation harness.

## Decision

1. **gRPC over HTTP/2 with tonic and prost.** Messages are defined in `.proto` files in `fleetring-proto`, package `fleetring.v1`. Code generation uses a pure-Rust protobuf compiler, so builds don't need a system `protoc`.
2. **Services:**
   - `Control`: `Join`, `Heartbeat`, `WatchMembership` (a server stream), `CreateDatabase`, `Locate`, `AcquireLease`, and the promotion calls.
   - `Replication`: `Replicate` (a bidirectional stream: batches and checkpoint markers from the primary, acknowledgements from the follower), `Fence` (the promotion step in ADR-0018), and `Snapshot` (a server stream of chunks).
   - `Database`, in v0.1 only: `Execute` and `Query` on a database's primary, for tests and the CLI. The router (MVP step 4) replaces it for applications.
3. **Batches are opaque.** A batch holds its epoch, its engine-defined start and end positions, and the engine's payload as one byte string, never one message per frame or row. The protocol never parses positions or payloads (ADR-0016).
4. **Versioning.** Fields evolve compatibly under protobuf's rules. A breaking change gets a new package version. Peers exchange protocol versions at `Join`, and a node refuses a version it doesn't support.
5. **Security.** TLS through rustls is optional for a local cluster and required between machines. Mutual TLS and API keys arrive with the router.
6. **Simulation.** tonic runs over turmoil's simulated TCP through a custom connector (ADR-0020), so the same services run in tests.

## Alternatives considered

- **QUIC with quinn.** Multiplexed streams without head-of-line blocking help over the internet, but we would design and maintain our own framing, and QUIC is harder to simulate.
- **HTTP/2 with JSON.** Easy to debug, but the weakest fit for high-rate binary frame streams.

## Consequences

- The `.proto` files are the wire contract between daemon versions and get the same review care as the placement spec.
- Large snapshots stream in chunks, so no message ever has to hold a whole database.
