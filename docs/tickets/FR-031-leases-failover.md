---
id: FR-031
title: Leases, epochs, quorums, and failover
status: todo
priority: P0
size: L
milestone: M6 Replication and failover
depends_on: [FR-030]
adrs: [ADR-0018]
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

- [ ] Simulation invariant: no acknowledged write is ever lost, across primary crashes, partitions that isolate the primary, control node outages, and flapping nodes.
- [ ] Simulation invariant: at most one primary acknowledges writes in any epoch.
- [ ] With default settings, killing a primary in a three-node cluster moves writes to a new primary within the lease duration plus the margin plus one second.
- [ ] While the control node is down, current primaries keep accepting writes.

## Notes from FR-028

- The control service has no promotion RPCs or expiry timer yet. Add the promotion calls named in ADR-0019 to the proto, and have the service turn an expired lease into `StartPromotion`, then `RecordFenced` from fenced replicas, then `CompletePromotion`. Liveness only changes through commands, so something must submit a command to act on an expiry.
- Decide whether a primary's heartbeat also renews its lease. `DatabaseStatus.epoch` in heartbeats is ignored today, and that is its only possible use.

## Notes from FR-029

- **Liveness is evaluated at the time of the last applied command**, so it only changes when some command arrives. A lone node that stops sending heartbeats isn't marked down until another command lands. Failover needs a periodic tick: a new `Action` the control node submits on a timer, so expiry and liveness advance without traffic.
- **No planned leave:** the protocol has no `Leave` RPC. Add one, and call it from `MembershipTasks::stop`, so a graceful shutdown leaves instead of looking like a failure.
