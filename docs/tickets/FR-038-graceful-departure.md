---
id: FR-038
title: Graceful departure: drain a node's primaries before it leaves
status: todo
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

- [ ] Draining a node with 50 primaries moves all of them within 5 s on a three-node local cluster, and a client writing throughout sees redirects but no refused write longer than one retry.
- [ ] Simulation: drains under faults never lose an acknowledged write, and never leave two primaries in one epoch.
- [ ] `Ctrl-C` on a primary drains it first; `on_shutdown = "stay"` doesn't.
