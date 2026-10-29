---
id: FR-043
title: Keyed leases: acquire, renew, release, and watch a lease on any key, with no database behind it
status: todo
priority: P2
size: L
milestone: M8 Access and integration
depends_on: [FR-042, FR-029]
adrs: [ADR-0017, ADR-0018]
---

# FR-043: Keyed leases: acquire, renew, release, and watch a lease on any key, with no database behind it

## Context

Leases today exist only for databases: the control plane grants one holder per database per epoch, fences the rest, and the ring decides who should hold it (ADR-0018). The same mechanism answers a broader question that applications keep asking: who owns `tenant:acme`'s background job, or `agent:42`'s session, right now, and how does the owner know when it no longer does? Temporal's shard ownership and Orleans' grain directory are the usual answers: a placement decision plus a fenced, renewable lease, with the holder notified when it loses it. FleetRing already has the placement (the ring) and the fencing (epochs). What's missing is a lease that isn't tied to opening a database. This ticket is gated on an ADR that decides the semantics before any code: whether a keyed lease is placed by the ring or requested by a holder, what renewal and expiry look like without a replication stream to prove liveness, and what the holder is promised across a control-plane restart.

## Scope

- **An ADR first** (`docs/adr/`): the lease model for keys, the epoch and fencing rules, expiry on missed renewals, and what a watcher sees. Accepted before implementation starts.
- **One control action family in `fleetring-control`:** `AcquireLease { key, holder, ttl }`, `RenewLease { key, holder, epoch }`, `ReleaseLease { key, holder, epoch }`, applied through the pure `apply` with time inside the command (ADR-0017), replay-safe, and carrying an epoch that every later command must present. Nothing else: no values, no locks on groups of keys, no fairness.
- **`WatchLeases` extended** (FR-042) to keyed leases, with the key in place of the database name, so a holder learns it lost the lease the same way a data node does.
- **The Node service exposes the four calls** for co-located workers; the control node applies them.
- **Not in scope:** storing data under keys, leader election across clusters, and placement of keys by anything but the key's name.

## Definition of done

- [ ] The ADR is accepted and linked from `docs/adr/README.md`.
- [ ] A lease on a key has at most one holder per epoch; a stale holder's renew and release are refused with the current epoch, and the replay test covers every command.
- [ ] A holder that stops renewing loses the lease after its TTL, by the control plane's clock inside commands, and a watcher sees `Lost` then `Gained` for the next holder in epoch order.
- [ ] A control-plane restart neither grants a second holder nor loses a renewed lease; the simulation history checker verifies single ownership per epoch under faults.
