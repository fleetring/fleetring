---
id: FR-029
title: Membership, heartbeats, and ring publication
status: done
priority: P0
size: M
milestone: M5 Daemon foundations
depends_on: [FR-028]
adrs: [ADR-0013, ADR-0017]
---

# FR-029: Membership, heartbeats, and ring publication

## Goal

Data nodes join the cluster and report liveness, and every node holds the same `Ring` for the same membership epoch (ADR-0013, ADR-0017).

## Scope

- **Join and leave:** a node joins with its ID, zone, host, and weight, and leaves for a planned removal.
- **Heartbeats** at a fixed interval, carrying each hosted database's role and log position. The control node marks a node down after a timeout.
- **`WatchMembership`:** a stream of membership changes. Each data node rebuilds its `Ring` off the hot path and publishes it through `ArcSwap`.
- **Diagnostics:** `fleetring`'s ring diagnostics are logged on each change, for example fewer zones than replicas.
- A sleeping laptop counts as a down node and comes back without a manual step.

## Acceptance criteria

- [x] After a join, a leave, or a node going down, every live node publishes a ring with the same fingerprint within two heartbeat intervals (simulation test).
- [x] A node that misses heartbeats is marked down, and marked up again when they resume.
- [x] Ring lookups never wait on a membership update: readers only load the `ArcSwap`.

## Notes from FR-028

- Data nodes call `ControlClient::join`, run a heartbeat loop, and consume `watch_membership` (current membership first, then one update per epoch change), then rebuild the ring and publish it through `ArcSwap`.
- A data-only role starts no server yet. This ticket adds it.
- Liveness changes alone don't bump the membership epoch, so they don't produce a `WatchMembership` update. Decide whether data nodes need liveness pushed or can ask for it.

## Progress

- 2026-10-28: Done.
  - **Membership:** `Membership::start(member, client, settings)` runs two tasks and returns a cheap, cloneable `Membership` handle plus a `MembershipTasks` guard (`stop()`, or drop to abort).
    - The handle offers `ring() -> Option<Arc<Ring>>`, `epoch()`, `view() -> Arc<MembershipView>`, `changed()`, and `wait_for(condition)`.
    - A `MembershipView` holds the epoch, the ring, its fingerprint, and every member with its `Liveness`.
  - **Heartbeats:** the node joins, then sends a heartbeat every interval, carrying an empty database list, because no database has a role yet. FR-030 fills it in.
    - Each join and heartbeat is bounded by the interval. Failures are logged and tried again at the next tick; nothing crashes the daemon.
    - A timed-out call makes the client dial a new connection (`ControlClient::reconnect`), because a partition or a sleep can leave a connection open while nothing crosses it.
    - `NOT_FOUND` (the node was removed) triggers a rejoin.
    - The interval uses `MissedTickBehavior::Delay`, so a laptop waking from sleep sends one heartbeat, not a burst, and is marked live again without a manual step.
  - **Ring publication:** the watch task follows `WatchMembership(after_epoch = 0)`, so every reopened stream starts with the whole membership.
    - On a new epoch it rebuilds the ring with `fleetring_control::membership_ring`, the same function `State::ring` now uses, so data nodes and the control plane compute the same fingerprint. It logs `Ring::diagnostics`, then stores the view in an `ArcSwap`.
    - A liveness-only update reuses the ring.
    - The stream is reopened if it fails or ends, when heartbeats recover after failing, or when two heartbeat replies in a row report a later epoch than the published one.
  - **Liveness:** `WatchMembership` now also sends an update when a member's liveness flag changes, not only when the epoch grows. Every update carries the current `live` flags.
    - Liveness is judged at the time of the last applied command, so it moves as other members' heartbeats arrive.
    - A lone data node that stops sending heartbeats isn't marked down until some command arrives. A periodic tick command would fix that, but it needs a new `Action`.
  - **Readers never wait:** `ring()` and `view()` are synchronous `ArcSwap` loads with no lock or `await`. The ring is built before it is stored, so a reader sees one whole view. A unit test reads views on two threads during 40 publications and checks that each view's ring matches its epoch, members, and fingerprint.
  - **Daemon:** every role now serves gRPC on `listen`. The control role adds `Control`; a data-only node serves no services yet, and calls to it return `UNIMPLEMENTED`.
    - The data role starts `Membership`. With both roles, it reaches its own control node through the bound address, with loopback in place of an unspecified IP.
    - `Daemon::membership()` and `Daemon::listen_address()` are new.
    - Shutdown stops the membership tasks first. There is no planned leave, because the protocol has no `Leave` RPC yet.
  - **Config:** a new `[membership]` table with `heartbeat_interval_ms` (default 1000).
    - On a data node, `validate` requires `HEARTBEATS_PER_TIMEOUT` (3) intervals to fit in `control.heartbeat_timeout_ms` and returns `ConfigError::Invalid` otherwise.
    - A data-only node checks against its own `[control]` table, so that table's timeout must match the control node's.
  - **Tests:**
    - A turmoil run with one control host and three data hosts covers joins, a partition (marked down, then up), a crash, a leave, a restart and rejoin, and a control node outage. Each time, every live node publishes the control plane's epoch, fingerprint, and liveness within two heartbeat intervals.
    - The run is repeated with the same seed and its traces compared: event order and each node's publication sequence.
    - Simulated times aren't compared. Tokio seeds its internal `select!` randomness only under `--cfg tokio_unstable`, so events can shift by a few milliseconds.
    - Also: real-TCP convergence of a control daemon and two data daemons, a both-roles daemon joining itself, config validation, and unit tests for view publication.

## Follow-up from FR-024

- **At 100 ms network latency, about 1 seed in 200 misses the two-heartbeat-interval bound after a node restart (seed 111).** The cause: `watch_loop` gives opening the stream 200 ms, then sleeps a full interval after a timeout. The harness uses 0–20 ms latency by default. Retrying the stream open sooner would make the bound hold at higher latency.
