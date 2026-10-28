# ADR-0015: `fleetringd` architecture and crate layout

- **Status:** Proposed (2026-10-28)
- **Date:** 2026-10-28
- **Related:** ADR-0001, ADR-0009, ADR-0011, ADR-0013, ADR-0016, ADR-0017, ADR-0018, ADR-0019, ADR-0020

## Context

`docs/mvp.md` step 2 is `fleetringd`: a daemon that keeps each database on a primary with followers, using leases and heartbeats. The placement crate (`fleetring` 0.1) decides where each database should live, and the daemon makes it so. Later steps add live migration, the router, and the client, and the control plane moves to Raft. The layout chosen now decides how much of that later work is a swap rather than a rewrite.

## Decision

1. **Five crates in the workspace:**

   | Crate | Kind | Role |
   |---|---|---|
   | `fleetring` | library, sync, pure | Placement. Unchanged. |
   | `fleetring-control` | library, sync, pure | The control state machine: commands, state, and a deterministic `apply` (ADR-0017). |
   | `fleetring-wal` | library, sync | The SQLite WAL format: read, validate, and mirror frames (ADR-0016). |
   | `fleetring-proto` | library | gRPC services and messages (ADR-0019). |
   | `fleetringd` | binary and library, async | The daemon: control service, data node, per-database actors, replication. |

2. **One binary, two roles.** `fleetringd` runs as a data node, as the control node, or as both, chosen by configuration. A single-machine cluster is one process.
3. **Purity where it pays.** Like `fleetring`, `fleetring-control` does no I/O and reads no clock: time arrives inside commands. That keeps it deterministic, unit-testable, and ready to sit behind Raft. It follows `fleetring`'s determinism rules: no floats and no `std::hash` collections.
4. **Async boundaries follow ADR-0013.** Tokio lives in `fleetringd` only. SQLite calls run on a dedicated thread per open database, or inline under simulation (ADR-0020). The ring is published through `ArcSwap`.
5. **Each open database is an actor** that owns its connection, serializes writes, captures committed frames, and streams them to followers (ADR-0013, ADR-0016).
6. **Engine adapter.** The actor reaches storage through a `DatabaseEngine` trait with two shapes, a frame source or a self-replicating engine, and engine-defined positions and payloads (ADR-0016). v0.1 implements it with plain SQLite through `rusqlite`. libSQL, Turso, and Postgres adapters come later behind the same trait, and nothing above the adapter depends on SQLite's WAL.
7. **Policy follows ADR-0009** for every crate. `unsafe_code` stays forbidden. If an engine adapter ever needs FFI, it lives in its own crate and opts out explicitly. The daemon crates may set a newer `rust-version` than `fleetring` if their dependencies need one; `fleetring` keeps its own MSRV.

## Alternatives considered

- **One big daemon crate.** Faster to start, but the control logic and the WAL code would grow entangled with async I/O, and the move to Raft would touch everything.
- **Separate binaries for the control and data roles.** Clean roles, but it breaks the one-binary home-lab story.

## Consequences

- `fleetring` stays untouched. `fleetringd` depends on it from the workspace.
- Two more pure crates carry the same standards as `fleetring`: property tests, no `unwrap`, and documented invariants.
- New dependencies: `tokio`, `tonic`, `prost`, `rusqlite` with bundled SQLite, `arc-swap`, `tracing`, and `turmoil` for tests. `deny.toml` must accept their licenses.
