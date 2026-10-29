---
id: FR-038
title: Graceful departure: drain a node's primaries before it leaves
status: done
priority: P1
size: M
milestone: M7 Durability and operations
depends_on: [FR-031]
adrs: [ADR-0018, ADR-0022]
---

# FR-038: Graceful departure: drain a node's primaries before it leaves

## Goal

A planned stop (`Ctrl-C`, a reboot, `fleetringd node drain`) hands the node's primaries to other replicas before the node goes, so clients see a short redirect instead of a failover. Today a planned leave is announced, but its leases still expire the slow way: a lease plus the margin, with writes refused meanwhile. Lease transfer is standard practice (CockroachDB transfers range leases on drain; Kafka moves partition leadership on controlled shutdown).

## Scope

- **`fleetringd node drain <id>`** and drain-on-shutdown (the default for `on_shutdown = "leave"`; `"stay"` keeps today's behaviour for a quick restart).
- **Lease transfer:** for each database the node is primary of, the control plane picks the most advanced live follower, the node stops accepting writes, ships its last frames, and the follower is promoted at the next epoch through the existing fencing path, without waiting for expiry. The old primary answers `UNAVAILABLE` with the new primary's address during the hand-over.
- **Then leave:** the node sends `Leave` only once it holds no lease. A drain that can't complete within a bound (no live follower for some database) reports which databases are stuck and leaves them to the normal failover.
- **Bounded:** a drain takes at most a few seconds per node, regardless of the number of databases, by transferring in parallel with a concurrency limit.

## Acceptance criteria

- [x] Draining a node with 50 primaries moves all of them within 5 s on a three-node local cluster, and a client writing throughout sees redirects but no refused write longer than one retry.
- [x] Simulation: drains under faults never lose an acknowledged write, and never leave two primaries in one epoch.
- [x] `Ctrl-C` on a primary drains it first; `on_shutdown = "stay"` doesn't.

## Progress

- 2026-10-29: Done.
  - **Control state machine (additive):** `Action::Drain { id }` marks a member as draining (`Applied::MemberDraining`); a draining member gets no first lease (`Rejection::MemberDraining`) and is never a promotion's candidate, and only a join or a leave clears the mark. `Action::TransferLease { database, target }` starts the ordinary promotion at the next epoch without waiting for the lease to expire, provided the holder is draining (`Rejection::HolderNotDraining` otherwise), with `target` as the candidate if it is a live, non-draining replica-set member, or else the most advanced such member other than the holder. The outcome is the same `Applied::PromotionStarted`, and the `Promotion` records who handed over (`Promotion::transferred_from`). The candidate rule for `StartPromotion` is unchanged apart from skipping draining members, which old logs never have, so old logs replay unchanged; snapshots without `draining` or `handed_over_by` load.
  - **Protocol (additive):** `Control.Drain` (answers the databases the member holds leases on), `Control.TransferLease`, and `Member.draining` in membership updates, which the service also sends when a draining flag changes.
  - **The drain (`fleetringd::data::drain`):** for each lease, in parallel up to 16 at a time: refuse requests for the database (`UNAVAILABLE`, "moving to a new primary; retry"); pick the connected, live follower that has acknowledged the most (ring order on ties) and wait until it has acknowledged everything committed; ask `TransferLease` naming it, falling back to the control plane's own pick if it refuses the target; tell the candidate at once through `Replication.Promote`; forget the lease and close the database; and wait, bounded, until the control plane names the new primary. A database whose transfer can't start keeps serving and is reported as stuck; it fails over the slow way once the node stops. `fleetringd node drain <id>` marks the node through the control plane, and the lease keeper runs the drain when its membership view says the node is draining. A shutdown with `on_shutdown = "leave"` marks itself, drains, and only then leaves; `"stay"` does neither.
  - **Safety:** a transfer is the failover protocol with a chosen candidate: a new epoch, Q fenced members, the most advanced fenced log adopted. The old holder refuses writes before it asks for the transfer, and once the next epoch is issued its renewals are refused, so one primary per epoch holds as it does after an expiry; W + Q > R puts every acknowledged write on a fenced member whatever the drain managed to ship. ADR-0022 is amended accordingly.
  - **Measured:** the TCP test drains the busiest of three nodes, 21 of 50 databases, in about 1 s over loopback on a laptop, with one of the writer's 21 writes needing retries; the sim scenario's operator drain moves a node's primaries in a few hundred simulated milliseconds and a graceful stop with a drain takes under a second of simulated time.
  - **Tests:** `fleetring-control` scenarios for the target choice, a draining member's leases and candidacy, replay with the new actions, and old snapshots; `tests/database.rs` drains a node with 50 databases under a writing client and then stops it; `tests/daemon.rs` sends SIGINT to a primary with and without `on_shutdown = "stay"`; `tests/sim/drain.rs` drains under an operator's command, a graceful stop, a stop while partitioned from a follower, and a crash halfway through a drain, and checks the history, including one primary per epoch, after each.
  - **Integration with FR-035:** the drain's target choice respects the database's `acks`. Under `quorum` and `all` the node still names the connected live follower that acknowledged the most, since every follower holds the current epoch's log. Under `local` a follower may be whole epochs behind, so the node names no target and `TransferLease` falls back to the control plane's `best_candidate` ranking (log epoch, then position); the node then ships the tail to the candidate it is told of before it steps down. Covered by a `fleetring-control` scenario (`tests/it/durability.rs`). `deploy/fleetringd.toml` documents `[membership] on_shutdown`.
  - **Follow-ups:** a drain reports stuck databases only in its log; a `node status` could show them. `node drain` has no undo short of a restart; a `node undrain` would clear the mark.
