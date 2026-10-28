---
id: FR-022
title: Workspace crates and the fleetringd skeleton
status: done
priority: P0
size: M
milestone: M5 Daemon foundations
depends_on: []
adrs: [ADR-0009, ADR-0013, ADR-0015]
---

# FR-022: Workspace crates and the `fleetringd` skeleton

## Goal

Lay out the daemon's crates (ADR-0015), and build a `fleetringd` binary that starts, reads its configuration, and shuts down cleanly.

## Scope

- **Crates:** `fleetring-control`, `fleetring-wal`, `fleetring-proto`, and `fleetringd` (a binary plus a library), all with the workspace lints from ADR-0009. `fleetring-control` also gets `fleetring`'s determinism lints: no floats and no `std::hash` collections.
- **Configuration:** a TOML file plus command-line flags for node ID, zone, host, weight, data directory, listen address, control node address, and roles (data, control, or both).
- **Runtime:** Tokio, `tracing` logs, and a graceful shutdown on SIGINT and SIGTERM that stops taking work, finishes in-flight writes, and closes databases.
- **Data directory:** an exclusive lock file, so that two daemons can't share one directory (ADR-0016).
- **CI:** the new crates join the existing jobs. The pure crates also run on the cross targets.

## Acceptance criteria

- [x] `fleetringd --config node.toml` starts, logs its configuration, and exits cleanly on SIGINT.
- [x] A second daemon pointed at the same data directory refuses to start, with a clear error.
- [x] Invalid configuration is rejected with an error that names the field.
- [x] fmt, clippy, tests, docs, the MSRV check, and cargo-deny pass for every crate.

## Notes

- If `tokio` or `tonic` need a newer Rust than 1.88, the daemon crates set their own `rust-version`. `fleetring` keeps its MSRV.

## Progress

- 2026-10-28: Done.
  - `fleetring-control`, `fleetring-wal`, and `fleetring-proto` exist as documented library crates; the first two use `fleetring`'s determinism lints and `clippy.toml`.
  - `fleetringd`: TOML configuration with `--config` and overrides for node ID, data directory, listen and control addresses, and role. Unknown fields are rejected, invalid values name their field, and a data-only node must name the control node.
  - The data directory is locked with `File::try_lock`, so a crashed daemon never leaves a stale lock. That needs Rust 1.89, so `fleetringd` sets `rust-version = "1.89"` and CI checks it separately; the library crates stay on 1.88.
  - Graceful shutdown on Ctrl-C and SIGTERM. A Unix test runs the binary, sees it log its start, sees a second daemon refused for the same directory, sends SIGINT, and checks a clean exit.
  - The cross-target jobs (i686, s390x, wasm32-wasip1) now also test `fleetring-control` and `fleetring-wal`. Every new dependency's license is on the `deny.toml` allow list; CI's cargo-deny job confirms it on push.
