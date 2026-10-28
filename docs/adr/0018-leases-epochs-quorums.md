# ADR-0018: Leases, epochs, and quorums: one writer per database, no acknowledged write lost

- **Status:** Proposed (2026-10-28)
- **Date:** 2026-10-28
- **Related:** ADR-0006, ADR-0016, ADR-0017, ADR-0020

## Context

Exactly one node may accept writes for a database at a time, and a failover must never lose a write that a client saw succeed. Leases alone guarantee neither: clocks drift, processes pause, and networks partition. Epochs, used as fencing tokens, and intersecting quorums do.

## Decision

1. **Every database has an epoch**, a counter in control state. A lease names the primary for one epoch. Only the control plane issues a new epoch.
2. **Followers fence by epoch.** Each follower durably records the highest epoch it has seen and refuses any batch from a lower one.
3. **Write quorum W.** A write is acknowledged once W members of the database's replica set, the primary included, have stored it durably. R is the replica set's size, and by default W = ⌊R/2⌋ + 1.
4. **Promotion quorum Q = R − W + 1.** To become primary at epoch e + 1, a node must fence at least Q members of the replica set, itself included. Each member records the new epoch and reports its log position in one step. The new primary adopts the most advanced log among them, fetches any frames it lacks, and only then accepts writes. Because W + Q > R, every acknowledged write reached at least one fenced member, and once Q members are fenced the old primary can no longer collect W acknowledgements.
5. **Divergent tails are discarded.** Frames that a member holds beyond the adopted position were never acknowledged. The member truncates its WAL back to that position before accepting frames from the new epoch.
6. **Leases decide when, not whether.** The primary renews its lease through heartbeats. The control plane starts a failover only after the lease has expired by the control node's clock, plus a margin. Safety doesn't depend on any clock: points 2–5 hold however slow or wrong a clock is. While the control plane is down, the current primary keeps serving, because no other node can be granted a newer epoch.
7. **Choosing the new primary.** Among live candidates that hold the most advanced log, the control plane prefers the earliest in the ring's preference list (ADR-0006). Moving the primary back to the ring's preferred node later is a migration.

## Alternatives considered

- **Leases with synchronized clocks** (bounded drift) as the safety mechanism. They are simpler, but one paused process or skewed clock can produce two writers.
- **Acknowledging after any one follower, whatever R is.** It is the same as W for R ≤ 3, but for larger R a promotion would need every follower, so one more failure would block failover.

## Consequences

- With R = 3, a write waits for one follower and a failover needs two of the three replicas. A replica set survives one failure without losing an acknowledged write.
- With R = 2, W = 2: every write waits for the follower, and a promotion needs only the follower. With R = 1 there is no failover.
- A primary cut off from its followers can't acknowledge writes. Its clients see timeouts, which are ambiguous by nature: the write may or may not survive.
- These rules are the invariants that the simulation tests check (ADR-0020).
