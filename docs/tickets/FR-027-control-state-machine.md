---
id: FR-027
title: Control state machine
status: done
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

- [x] Replay test: applying a command log to a fresh state gives the same state and outcomes as the original run, and so does restoring a snapshot taken midway and replaying the rest.
- [x] Property tests: no lease is granted at an epoch at or below the current one, no two leases for one database share an epoch, and a member marked down rejoins with its old ID cleanly.
- [x] `fleetring`'s determinism lints (no floats, no `std::hash` collections) pass.

## Progress

- 2026-10-28: Done. `apply(&mut State, Command) -> Outcome`, where a `Command` is a stamped `Action`: join, leave, heartbeat, create database, acquire lease, renew lease, start promotion, record fenced, complete promotion.
  - **Time:** a command runs at `max(stamp, now)`, so stamps that go backwards count as the last accepted time. A rejected command changes nothing, `now` included. Liveness is computed from stamps and never stored.
  - **Leases:** `AcquireLease` grants only a database's first lease, at epoch 1. Every later holder goes through a promotion, so a new epoch never skips the quorum fence. A holder may renew late as long as no newer epoch exists.
  - **Promotions:** the epoch is issued when a promotion starts. The candidate is a live replica-set member with the highest reported position (byte order), ties going to the earliest in preference order. Completion needs Q fenced members of the current replica set, the candidate included. A stalled promotion can be superseded by a newer epoch.
  - **Placement:** the replica set is the ring over all members; liveness only filters candidates. A join that would make the ring invalid is rejected, so `ring()` fails only on an empty cluster.
  - **Types:** the membership epoch reuses `fleetring::Epoch`, and the per-database fencing token is a separate `LeaseEpoch`. Positions are opaque and compare as bytes. Everything derives serde; no type derives `Hash`.
  - **Tests:** 21 integration tests (a 4,000-command replay, snapshot restore at four split points, five properties at 64 cases each, and twelve scenarios) plus 12 doctests, about 0.1 s in debug, with no filesystem access.
- **For FR-028:**
  - choose the on-disk log and snapshot encoding, and fsync before apply;
  - validate snapshot invariants if snapshots can come from outside;
  - turn daemon events into commands: a primary's heartbeat into `RenewLease`, an expired lease into `StartPromotion`, then `RecordFenced` from replicas, then `CompletePromotion`. Liveness only changes through commands, so the service must send one to act on an expiry;
  - wire `ControlConfig` from configuration, and decide whether the ring's strategy and seed belong in control state. Today they're the defaults.
- **Not yet built:** dropping a database, and the migration and engine-change commands (MVP step 3).
