---
id: FR-031
title: Leases, epochs, quorums, and failover
status: done
priority: P0
size: L
milestone: M6 Replication and failover
depends_on: [FR-030]
adrs: [ADR-0018, ADR-0022]
---

# FR-031: Leases, epochs, quorums, and failover

## Goal

One writer per database, automatic failover when a primary dies, and no acknowledged write lost (ADR-0018).

## Scope

- **Leases:** the primary acquires its lease at epoch e and renews it through heartbeats. Every batch and every write carries the epoch.
- **Fencing:** followers durably record the highest epoch they have seen and refuse lower ones.
- **Promotion:** after a lease expires plus a margin, the control plane picks a candidate (ADR-0018, point 7). The candidate fences Q replicas, adopts the most advanced log, fetches any missing frames, has divergent tails truncated, completes the promotion, and starts accepting writes.
- **Demotion:** a former primary that learns of a newer epoch stops accepting writes, truncates any unacknowledged tail, and rejoins as a follower.
- **Configuration:** lease duration, renewal interval, and failover margin, with defaults suited to a LAN.

## Acceptance criteria

- [x] Simulation invariant: no acknowledged write is ever lost, across primary crashes, partitions that isolate the primary, control node outages, and flapping nodes.
- [x] Simulation invariant: at most one primary acknowledges writes in any epoch.
- [x] With default settings, killing a primary in a three-node cluster moves writes to a new primary within the lease duration plus the margin plus one second.
- [x] While the control node is down, current primaries keep accepting writes.

## Notes from FR-028

- The control service has no promotion RPCs or expiry timer yet. Add the promotion calls named in ADR-0019 to the proto, and have the service turn an expired lease into `StartPromotion`, then `RecordFenced` from fenced replicas, then `CompletePromotion`. Liveness only changes through commands, so something must submit a command to act on an expiry.
- Decide whether a primary's heartbeat also renews its lease. `DatabaseStatus.epoch` in heartbeats is ignored today, and that is its only possible use.

## Notes from FR-029

- **Liveness is evaluated at the time of the last applied command**, so it only changes when some command arrives. A lone node that stops sending heartbeats isn't marked down until another command lands. Failover needs a periodic tick: a new `Action` the control node submits on a timer, so expiry and liveness advance without traffic.
- **No planned leave:** the protocol has no `Leave` RPC. Add one, and call it from `MembershipTasks::stop`, so a graceful shutdown leaves instead of looking like a failure.

## Notes from FR-024

- **Extend `sim::check::one_primary_per_epoch`** with the roles data nodes report. Today it checks control-plane grants only.
- **Crashes are modeled as power loss:** peers' connections stall rather than reset. A process kill, where peers get a reset, isn't modeled; doing it deterministically would mean closing connections in a sorted order.

## Progress

- 2026-10-28: Done. The protocol and its open questions are recorded in [ADR-0022](../adr/0022-failover-protocol.md), which amends ADR-0018 points 4, 5, and 7.
  - **Control state (`fleetring-control`, additive and replay-safe):** `Action::Tick` (`Applied::Ticked`) only moves time on. `HostedReplica::with_log_epoch` and `Report::log_epoch` carry the epoch of the primary that wrote a replica's last frames (serde-defaulted, so old logs and snapshots load and replay the same). `StartPromotion` ranks candidates by log epoch, then position; `State::promotions_due(at)` lists the databases a `StartPromotion` would start a promotion for, from the same check `apply` runs.
  - **Proto (additive):** `Control.Leave`, `StartPromotion`, `RecordFenced`, and `CompletePromotion`; `Replication.Fetch` and `Promote`; `FenceRequest.candidate`, `FenceResponse.log_epoch`, `ReplicateOpen.epoch_start`, and `ExecuteResponse.primary` and `epoch`. `DatabaseStatus.epoch` is now the replica's log epoch, and `Placement.primary` is unset while a promotion runs.
  - **Control node:** `control::Failover` ticks after `[control] tick_interval_ms` without a command, submits `StartPromotion` for every due database, and calls `Replication.Promote` on each promotion's candidate every second until it completes. It waits a lease duration plus the margin after the control node starts, so leases that expired only because control was down get renewed first.
  - **Data node:** `data::LeaseKeeper` renews every held lease each `[control] lease_renewal_ms` (default: a fifth of the lease duration), demotes on a refusal that names a newer epoch or another holder (the database closes and its copy follows), and keeps serving while control is unreachable. After a restart it locates every database it has an epoch record for and leads those it holds the lease on at once. It carries out promotions: fence itself, fence the others in parallel until Q are recorded, adopt the most advanced log (log epoch, then position) and fetch what it lacks, `CompletePromotion`, then lead from the adopted position.
  - **Replication:** `replication/failover.rs` holds fencing (`Fence` records the epoch and candidate durably while no stream applies, steps a lower-epoch primary down, and reports position and log epoch), `Fetch`, `Promote`, stream admission (an epoch's recorded holder only; a higher epoch steps a local primary down), divergent-tail truncation at stream open, and log-epoch recording. `Replication::lead` records the lease durably before serving and refuses a superseded epoch; `demote`, `serving`, `highest_epoch`, `fence_local`, `fence_peer`, `catch_up`, `recorded_databases`, and `recover_positions` are new. `epochs.rs` keeps a JSON record per database (highest epoch, its holder, log epoch, and the start of a led epoch) and still reads the old bare-number files. A primary whose follower's position doesn't fit its log, or a follower that can't truncate, ends the stream as needing a snapshot instead of retrying forever.
  - **Engine:** `FrameFollower::committed_since` and `truncate_to` (SQLite: `Mirror::truncate` and the WAL reader); `Registry::close`.
  - **Membership:** `MembershipTasks::stop` leaves the cluster unless `[membership] on_shutdown = "stay"`.
  - **Configuration:** `[control] lease_duration_ms` (10 s), `lease_renewal_ms` (lease / 5), `failover_margin_ms` (2 s), and `tick_interval_ms` (250 ms). A data node needs three renewals to fit in a lease.
  - **Simulation:** `Ack::Written` names the primary and epoch, and `check::one_primary_per_epoch` holds every acknowledged write's (database, epoch) to one node, next to the granted leases. `Timings` gains `lease_renewal` and `tick_interval`, and `Timings::daemon_defaults()` runs the daemon's own defaults. `tests/sim/failover.rs`: a crash with default timings moves writes within the lease plus margin plus 1 s (11.3 s on seed 31; budget 13 s), no acknowledged write is lost across a crash, an isolating partition, a control outage, and flapping (with at least two failovers and three primaries), primaries serve through a control outage of three leases with no failover after it, and an isolated primary acknowledges nothing and its divergent tail is truncated on rejoin. FR-030's replication scenario stops followers with `on_shutdown = "stay"`, since one that leaves rejoins behind a checkpoint and needs FR-032.
  - **Tests elsewhere:** `tests/control.rs` drives an expired lease through the timer and the promotion RPCs, and `Leave`; `tests/database.rs` runs `db exec` through a failover over TCP; `tests/actor/sqlite.rs` truncates a follower and reads its frames back; `epochs.rs` and `check.rs` unit tests cover holders, records, and write primaries.
- 2026-10-28: Integration with FR-032. Every copy the log can't serve is now re-seeded through FR-032's `Reseed`; nothing ends a stream as needing a snapshot any more, so nothing retries forever.
  - A follower that can't truncate a divergent tail (the epoch's start is at or behind its last checkpoint, or in an older generation), or whose primary's frames don't continue its copy, sets `ReplicateAck.needs_snapshot` (proto field 2) and takes no frames until the primary answers with `Reseed`. The primary never counts that acknowledgement's position. The `fleetring-needs-snapshot` error metadata is gone. `failover::reconcile` returns `Reconciled::Follows` or `Reconciled::NeedsSnapshot`, and lost its `installed` flag: every installed snapshot (initial copy, re-seed, or `Replication::seed`) records the primary's epoch as the copy's log epoch, so a re-seeded copy is never cut back again.
  - The re-seed runs under the slot's apply lock (`FollowerSlot::applying`, an `Arc<tokio::sync::Mutex<()>>`), the lock fences, fetches, and seeds take, so no fence interleaves with a snapshot swap.
  - Engine: the follower loads `data.db-checkpoint` on open and keeps it current. `SqliteFollower::committed_since` refuses an earlier-generation position other than that marker, as the primary does, and `truncate_to` refuses any position at or behind it. `Fetch` sends the caller's position as the batch's start, as a stream does, so a candidate can catch up across a generation switch.
  - Simulation: FR-030's scenario runs with the default `on_shutdown` again; a follower that leaves and rejoins behind a checkpoint is re-seeded. `primaries_keep_serving_while_the_control_node_is_down` writes through the primary's node (`Cluster::client_through`), since the control-routed writer can't locate the primary during the outage and the check passed only when a write happened to be in flight at the crash.
  - Tests: `tests/actor/sqlite.rs` checks the follower's marker refusals, `tests/replication/snapshot.rs` re-seeds a follower that diverges behind its last checkpoint exactly once, and `tests/replication/protocol.rs` checks the mid-stream request. ADR-0022 points 3 and its consequences carry an amendment.
- 2026-10-28: Seed deadlock fixed. A follower stream took the apply lock and checked its session, but subscribed to session changes only after opening or bootstrapping the copy, so a `Replication::seed` that began a session in that window waited for as long as the primary kept the stream open. `Follow::run` now subscribes before it takes the lock, and seeds take the copy over through `FollowerSlot::take_over`, which begins a session, waits for the lock, and begins again whenever another session supersedes it; seeds queue for their turn, so two never supersede each other forever. A fence that arrives while a seed waits goes ahead of it, and the seed re-admits its epoch once it holds the lock, so a seed at an older epoch fails instead of replacing the copy a fence reported on. A fence that arrives while a seed holds the lock waits for it, within `open_timeout`. `tests/replication/snapshot.rs` adds `a_seed_during_a_streams_first_transfer_takes_over_the_copy` and `a_fence_during_a_seeds_wait_goes_first_and_the_seed_then_fails`.
- 2026-10-28: Stability under load. A data node's heartbeat and join loops dialed a new control connection on every call that timed out (one heartbeat interval); on a loaded machine each slow reply opened another connection, and parallel test runs exhausted the ephemeral ports. They now dial again only after three timeouts in a row (`STALLED_HEARTBEATS`). `db_exec_keeps_working_through_a_primary_failover_after_a_retry` accepts any epoch from 2 on, since a promotion that stalls past the lease and the margin is legitimately replaced at the next epoch.

## Follow-ups

- **FR-032:** re-seed a follower flagged as needing a snapshot, which now also covers a copy that diverges where it can't be truncated (two failovers with the replica down for the middle one, or a tail in a newer WAL generation than the epoch's start).
- A primary that shuts down gracefully could hand its leases over instead of letting them expire; today a planned stop fails over a lease duration plus the margin later.
- Renewals and the control service's lease scan are one call per database; batch them for nodes that lead many databases.
- A graceful leave reshapes replica sets; a restart that should keep its place sets `on_shutdown = "stay"`.
