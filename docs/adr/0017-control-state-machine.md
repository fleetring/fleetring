# ADR-0017: A control plane built as a deterministic state machine: one node now, Raft later

- **Status:** Proposed (2026-10-28)
- **Date:** 2026-10-28
- **Related:** ADR-0001, ADR-0013, ADR-0015, ADR-0018, ADR-0019

## Context

Some facts need exactly one answer across the cluster: which nodes are members, which node holds each database's lease and at what epoch, and where each migration stands. `docs/mvp.md` keeps them in a small control database and adds Raft later. Adding Raft later is cheap only if the control state already behaves like a replicated state machine.

## Decision

1. **Commands, not ad-hoc updates.** Every change is a `Command`: join, leave, heartbeat, create a database, acquire or renew a lease, start or complete a promotion. `fleetring-control` applies commands in order with one pure function, `apply(state, command) -> outcome`. Nothing else mutates control state.
2. **Determinism.** `apply` reads no clock, no randomness, and no environment. The control node stamps each command with its current time before logging it, and `apply` uses only that stamp, so leases and liveness come out the same wherever the log is replayed.
3. **One node in v0.1.** The control node appends each command to a log in its own SQLite database, fsyncs it, applies it, and snapshots the state periodically. On restart it loads the latest snapshot and replays the log after it.
4. **A client that can follow a leader.** Data nodes reach the control plane through a client that understands a "not the leader, try X" reply. A single control node never sends one. Under Raft it will, and no caller changes.
5. **Membership and liveness.** Nodes join with their placement attributes (ID, zone, host, weight) and send heartbeats. The control node marks a node down after a timeout, measured with command timestamps. Each membership change bumps a membership epoch. Data nodes watch it, rebuild the `Ring`, and publish it through `ArcSwap` (ADR-0013).
6. **Placement is computed, not stored.** The ring decides where each database should live. Control state stores only what the ring can't know: which databases exist, their leases (where each one actually is), and migrations in flight.

## Adding Raft later

openraft replicates a log of opaque entries and asks the application for a state machine. With points 1–4 in place, the work is to:

- implement openraft's log storage over the existing command log, and its state machine over `apply`;
- implement its network over the gRPC transport (ADR-0019);
- send writes to the leader, which the client already does.

A replay test guards this from the start: replaying a command log into a fresh state must give an identical state and identical outcomes, and so must restoring a snapshot taken midway and replaying the rest.

## Alternatives considered

- **Raft from day one.** No single point of failure, but much more to build and test before anything works end to end.
- **etcd.** Proven, but it is one more service to run, against the one-binary goal.

## Consequences

- While the control node is down, membership changes, new databases, and failovers wait. Existing primaries keep serving (ADR-0018, point 6).
- The control node's clock is the only clock that leases depend on. Under Raft, the leader's clock takes that role.
