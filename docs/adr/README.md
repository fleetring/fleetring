# Architecture decision records

Decisions that shape the `fleetring` crate. Each record covers one decision: its context, what was decided, the alternatives considered, and the consequences.

- **Numbering:** files are named `NNNN-short-title.md`. Numbers are never reused.
- **Status:** `Proposed` → `Accepted` → `Deprecated` or `Superseded by ADR-NNNN`.
- **Changing a decision:** write a new ADR that supersedes the old one, then update the old record's status line. Don't rewrite an accepted decision. Fixing typos and links is fine.
- **Placement-affecting decisions** also need a new placement spec version (ADR-0008).

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-pure-deterministic-library.md) | `fleetring` is a pure, deterministic placement library | Accepted (point 7 amended by 0011) |
| [0002](0002-stable-hasher-xxh3.md) | Stable hashing behind a `StableHasher` trait, with XXH3-64 as the default | Accepted |
| [0003](0003-integer-only-arithmetic.md) | Placement arithmetic uses integers only | Accepted |
| [0004](0004-placement-strategies.md) | Weighted rendezvous by default, vnode ring as an option, no jump hash | Accepted |
| [0005](0005-identity.md) | Identity: node IDs, keys, labels, and weights | Accepted |
| [0006](0006-replica-sets-topology-liveness.md) | Replica sets, topology-aware selection, and liveness filtering | Accepted |
| [0007](0007-bounded-loads-policy.md) | Bounded loads as a deterministic policy applied at assignment time | Accepted |
| [0008](0008-placement-spec-vectors-fingerprints.md) | Versioned placement spec, golden vectors, and ring fingerprints | Accepted |
| [0009](0009-toolchain-lints-ci.md) | Toolchain, lint, dependency, and CI policy | Accepted (test matrix amended by 0014) |
| [0010](0010-replica-anti-entropy-hash-trees.md) | Replica anti-entropy with page-level hash trees (replication layer, not this crate) | Proposed (deferred) |
| [0011](0011-generic-placement-core.md) | `fleetring` is general-purpose placement infrastructure; the product stays SQLite-focused | Accepted |
| [0012](0012-cells-nested-placement.md) | Cells: nested placement by composing rings (directory or ring picks the cell, the cell's ring picks nodes) | Accepted |
| [0013](0013-async-boundaries.md) | Async boundaries: synchronous placement, async I/O, engine-owned concurrency | Accepted |
| [0014](0014-os-test-matrix.md) | Run the full feature and profile matrix on Linux only | Accepted |
| [0015](0015-fleetringd-architecture.md) | `fleetringd` architecture and crate layout | Accepted |
| [0016](0016-wal-frame-replication.md) | Replication through engine adapters, starting with SQLite WAL shipping | Accepted |
| [0017](0017-control-state-machine.md) | A control plane built as a deterministic state machine: one node now, Raft later | Accepted |
| [0018](0018-leases-epochs-quorums.md) | Leases, epochs, and quorums: one writer per database, no acknowledged write lost | Accepted (points 4, 5, 7 amended by 0022) |
| [0019](0019-grpc-transport.md) | Nodes talk gRPC over tonic | Accepted |
| [0020](0020-simulation-testing.md) | Deterministic simulation testing with turmoil | Accepted |
| [0021](0021-engine-changing-migrations.md) | Migrations that change a database's engine | Accepted |
| [0022](0022-failover-protocol.md) | Failover: the candidate drives the promotion, and logs are ranked by log epoch | Accepted |
| [0023](0023-configurable-durability.md) | Configurable durability: acknowledgement modes, a replica floor, and per-database settings | Proposed |

## Template

```markdown
# ADR-NNNN: <the decision, as a short sentence>

- **Status:** Proposed
- **Date:** YYYY-MM-DD
- **Related:** ADR-…; tickets FR-…

## Context
The forces at play: constraints, facts, and goals.

## Decision
What we will do, stated so that someone can check it.

## Alternatives considered
Each serious option and why it lost.

## Consequences
What gets easier, what gets harder, and what follow-up work this creates.
```
