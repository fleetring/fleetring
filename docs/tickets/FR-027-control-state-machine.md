---
id: FR-027
title: Control state machine
status: todo
priority: P0
size: M
milestone: M5 Daemon foundations
depends_on: [FR-022]
adrs: [ADR-0017, ADR-0018]
---

# FR-027: Control state machine

## Goal

Route every change to control state through one pure, deterministic function, so that the control plane can move to Raft later without changing its logic (ADR-0017).

## Scope

- **`fleetring-control`:** `Command`, `State`, `Outcome`, and `apply(&mut State, Command) -> Outcome`, with no I/O, clock, or randomness. Each command carries its control-node timestamp.
- **Commands:** join, leave, heartbeat (with reported log positions), create a database, acquire a lease, renew a lease, start a promotion, and complete a promotion. Migration commands arrive with MVP step 3.
- **State:** members with their placement attributes and liveness, the membership epoch, databases with their replication factor and engine (`sqlite` in v0.1; the field exists for later engines and engine changes, ADR-0021), and each database's lease (holder, epoch, and expiry).
- **ADR-0018 rules:** epochs only increase, a new epoch is issued only after the current lease has expired plus a margin, and a promotion completes only with a promotion quorum.
- **Snapshots:** serialize and restore `State` exactly.
- **Ring:** build the `fleetring::Ring` for the current membership. State never stores computed placement.

## Acceptance criteria

- [ ] Replay test: applying a command log to a fresh state gives the same state and outcomes as the original run, and so does restoring a snapshot taken midway and replaying the rest.
- [ ] Property tests: no lease is granted at an epoch at or below the current one, no two leases for one database share an epoch, and a member marked down rejoins with its old ID cleanly.
- [ ] `fleetring`'s determinism lints (no floats, no `std::hash` collections) pass.
