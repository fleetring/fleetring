# FleetRing tickets

Local tickets for FleetRing: the `fleetring` placement crate (MVP step 1 in [`docs/mvp.md`](../mvp.md), released as 0.1.0) and the `fleetringd` daemon (MVP step 2). Design decisions are recorded in [`docs/adr/`](../adr/README.md), and tickets link to the ADRs they implement.

## Conventions

- **One file per ticket**, named `FR-NNN-short-slug.md`. Each file starts with YAML front matter:
  - `status`: `todo`, `in-progress`, `blocked`, or `done`
  - `priority`: `P0` blocks `fleetringd` work, `P1` is required for 0.1.0, `P2` may slip past 0.1.0. For daemon tickets (FR-022 on), `P0` is required for `fleetringd` v0.1, `P1` should ship with it, and `P2` may slip.
  - `size`: `S` is half a day or less, `M` is 1–2 days, `L` is 3–4 days
  - `depends_on`: the IDs of prerequisite tickets
  - `adrs`: the ADRs the ticket implements
- **Commits** follow Conventional Commits, using the crate area as the scope (`feat(ring): …`) and a `Refs: FR-NNN` trailer.
- **Done** means every acceptance criterion is checked, CI is green, and the definition of done below holds.

## Definition of done

These items follow the Rust API Guidelines and common Rust best practices.

- `cargo fmt`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`, and `cargo doc --no-deps` (with `-D warnings`) are all clean.
- No `unwrap()` outside tests. Every `expect`, `assert`, and `panic` message includes the relevant values.
- Every public item is documented. The first line is a one-sentence summary, the examples use `?`, and `# Errors` and `# Panics` sections appear where relevant.
- Public types derive `Debug` and the applicable common traits. Error types implement `Error + Display + Send + Sync + 'static`.
- Public signatures have no `bool` parameters, no `&String` or `&Vec<T>`, and no third-party types.
- No floats, `std::hash`, `HashMap`, or `HashSet` appear in placement code (ADR-0002, ADR-0003).
- New placement behavior is covered by property tests and, from FR-014 on, by golden vectors.

## Milestones

| Milestone | Tickets | Outcome |
|---|---|---|
| M0 Foundations | FR-001, FR-002 | The workspace and CI enforce ADR-0009 on every platform where determinism usually breaks |
| M1 Hashing and model | FR-003 – FR-006 | Stable hasher, domain types, fixed-point math, and a deterministic test harness |
| M2 Placement core | FR-007 – FR-011 | Ring, rendezvous, topology-aware replica sets, vnode ring, and bounded loads |
| M3 Change and interop | FR-012 – FR-015, FR-020, FR-021 | Diff, `RingSpec` and fingerprint, spec v1 with golden vectors, diagnostics, and cells |
| M4 Hardening and release | FR-016 – FR-019 | Benchmarks, fuzzing, docs, and the 0.1.0 release with the spec frozen |
| M5 Daemon foundations | FR-022 – FR-029 | `fleetringd` crates, transport, simulation harness, WAL library, database actors, control plane, and membership |
| M6 Replication and failover | FR-030 – FR-033 | WAL shipping, leases and failover with no acknowledged write lost, snapshot catch-up, and a CLI |

## Index

| ID | Title | P | Size | Depends on | Status |
|---|---|---|---|---|---|
| [FR-001](FR-001-workspace-hygiene.md) | Workspace hygiene: edition 2024, MSRV, lints, licenses | P0 | S | — | done |
| [FR-002](FR-002-ci-pipeline.md) | CI pipeline across the platforms where determinism breaks | P0 | M | FR-001 | done |
| [FR-003](FR-003-stable-hasher.md) | `StableHasher` trait and the default `Xxh3` hasher | P0 | S | FR-001 | done |
| [FR-004](FR-004-domain-types.md) | Domain types: `NodeId`, `Zone`, `Host`, `Weight`, `ReplicationFactor`, `Node`, `Epoch` | P0 | M | FR-001 | done |
| [FR-005](FR-005-fixed-point-math.md) | Fixed-point −log₂ and exact score comparison | P0 | M | FR-001 | done |
| [FR-006](FR-006-test-harness.md) | Deterministic test harness, key corpora, balance statistics | P0 | S | FR-004 | done |
| [FR-007](FR-007-ring-builder.md) | `Ring` and `RingBuilder` with validation and canonical membership | P0 | M | FR-003, FR-004 | done |
| [FR-008](FR-008-rendezvous-strategy.md) | Weighted rendezvous candidate order (default strategy) | P0 | M | FR-005, FR-006, FR-007 | done |
| [FR-009](FR-009-replica-selection.md) | Topology-aware replica selection, `ReplicaSet`, preference lists | P0 | L | FR-008 | done |
| [FR-010](FR-010-vnode-strategy.md) | Weighted virtual-node ring strategy | P1 | M | FR-009 | done |
| [FR-011](FR-011-bounded-loads.md) | Bounded-load placement policy | P1 | M | FR-009 | done |
| [FR-012](FR-012-ring-diff.md) | Ring diff and movement summary for migration planning | P1 | M | FR-009 | done |
| [FR-013](FR-013-ring-spec-fingerprint.md) | `RingSpec`, canonical encoding, fingerprint, serde support | P0 | M | FR-009 | done |
| [FR-014](FR-014-placement-spec-vectors.md) | Placement spec v1, golden vectors, independent reference check | P1 | L | FR-002, FR-010, FR-011, FR-013 | done |
| [FR-015](FR-015-diagnostics.md) | Ring diagnostics and expected load shares | P2 | S | FR-009 | done |
| [FR-016](FR-016-benchmarks.md) | Benchmarks and performance budget | P1 | S | FR-010, FR-012 | done |
| [FR-017](FR-017-fuzzing.md) | Fuzzing for spec decoding, builder input, lookup invariants | P2 | S | FR-013 | done |
| [FR-018](FR-018-docs-examples.md) | Crate documentation, examples, README accuracy | P1 | M | FR-011, FR-012, FR-013 | done |
| [FR-019](FR-019-release-0-1-0.md) | Release `fleetring` 0.1.0 and freeze placement spec v1 | P1 | S | FR-014, FR-016, FR-018 | done |
| [FR-020](FR-020-cells.md) | Cells: nested placement with directories and per-cell rings | P1 | L | FR-012, FR-013 | done |
| [FR-021](FR-021-cells-spec.md) | `CellsSpec`: serializable cells layers | P2 | M | FR-013, FR-020 | done |
| [FR-022](FR-022-daemon-skeleton.md) | Workspace crates and the `fleetringd` skeleton | P0 | M | — | done |
| [FR-023](FR-023-grpc-transport.md) | gRPC protocol crate and node transport | P0 | M | FR-022 | done |
| [FR-024](FR-024-simulation-harness.md) | Simulation harness with turmoil | P0 | M | FR-022, FR-023 | todo |
| [FR-025](FR-025-wal-format.md) | WAL format library | P0 | M | FR-022 | done |
| [FR-026](FR-026-database-actor.md) | Engine adapter and per-database actor | P0 | L | FR-022, FR-025 | todo |
| [FR-027](FR-027-control-state-machine.md) | Control state machine | P0 | M | FR-022 | done |
| [FR-028](FR-028-control-node.md) | Control node service and client | P0 | M | FR-023, FR-027 | done |
| [FR-029](FR-029-membership.md) | Membership, heartbeats, and ring publication | P0 | M | FR-028 | todo |
| [FR-030](FR-030-replication-stream.md) | Replication stream and follower mirroring | P0 | L | FR-023, FR-026, FR-029 | todo |
| [FR-031](FR-031-leases-failover.md) | Leases, epochs, quorums, and failover | P0 | L | FR-030 | todo |
| [FR-032](FR-032-snapshot-catch-up.md) | Snapshot catch-up | P1 | M | FR-030 | todo |
| [FR-033](FR-033-database-api-cli.md) | Database API and CLI for `fleetringd` v0.1 | P1 | M | FR-026, FR-028 | todo |

## Dependency graph

```mermaid
flowchart LR
  FR001[FR-001 workspace] --> FR002[FR-002 CI]
  FR001 --> FR003[FR-003 hasher]
  FR001 --> FR004[FR-004 domain types]
  FR001 --> FR005[FR-005 fixed-point math]
  FR004 --> FR006[FR-006 test harness]
  FR003 --> FR007[FR-007 ring + builder]
  FR004 --> FR007
  FR005 --> FR008[FR-008 rendezvous]
  FR006 --> FR008
  FR007 --> FR008
  FR008 --> FR009[FR-009 replica selection]
  FR009 --> FR010[FR-010 vnode ring]
  FR009 --> FR011[FR-011 bounded loads]
  FR009 --> FR012[FR-012 diff]
  FR009 --> FR013[FR-013 RingSpec + fingerprint]
  FR009 --> FR015[FR-015 diagnostics]
  FR002 --> FR014[FR-014 spec v1 + vectors]
  FR010 --> FR014
  FR011 --> FR014
  FR013 --> FR014
  FR010 --> FR016[FR-016 benchmarks]
  FR012 --> FR016
  FR013 --> FR017[FR-017 fuzzing]
  FR011 --> FR018[FR-018 docs + examples]
  FR012 --> FR018
  FR013 --> FR018
  FR014 --> FR019[FR-019 release 0.1.0]
  FR016 --> FR019
  FR018 --> FR019
  FR012 --> FR020[FR-020 cells]
  FR013 --> FR020
  FR013 --> FR021[FR-021 cells spec]
  FR020 --> FR021
  FR022[FR-022 daemon skeleton] --> FR023[FR-023 gRPC transport]
  FR022 --> FR024[FR-024 simulation harness]
  FR023 --> FR024
  FR022 --> FR025[FR-025 WAL format]
  FR022 --> FR026[FR-026 database actor]
  FR025 --> FR026
  FR022 --> FR027[FR-027 control state machine]
  FR023 --> FR028[FR-028 control node]
  FR027 --> FR028
  FR028 --> FR029[FR-029 membership + ring]
  FR023 --> FR030[FR-030 replication stream]
  FR026 --> FR030
  FR029 --> FR030
  FR030 --> FR031[FR-031 leases + failover]
  FR030 --> FR032[FR-032 snapshot catch-up]
  FR026 --> FR033[FR-033 database API + CLI]
  FR028 --> FR033
```

## Critical path and estimate

The critical path is: FR-001 → FR-003 / FR-004 / FR-005 (parallel) → FR-007 → FR-008 → FR-009 → FR-010 → FR-014 → FR-019.

| Scope | Tickets | Estimate (one engineer) |
|---|---|---|
| P0: unblocks `fleetringd` | 10 tickets | ≈ 14 engineer-days |
| P0 + P1: the 0.1.0 release | 18 tickets | ≈ 28 engineer-days |
| Everything, including P2 | 20 tickets | ≈ 29 engineer-days |

For `fleetringd` v0.1, the critical path is FR-022 → FR-023 → FR-028 → FR-029 → FR-030 → FR-031, with FR-025 → FR-026 and FR-027 running in parallel. All twelve daemon tickets come to about 24 engineer-days.

`docs/mvp.md` allows 4–6 weeks for all eight MVP steps, with the ring crate as step 1. At about three weeks, P0 alone takes more than a proportional share. If the timeline is fixed, start `fleetringd` once P0 is done and run P1 in parallel. P2 can slip past 0.1.0 without affecting placement.

## Backlog (not yet ticketed)

**Engines and durability.** These must fit behind the engine adapter without changing anything above it (ADR-0016):

- **libSQL and Turso adapters** as frame sources, capturing commits inside the engine.
- **Turso MVCC adapter**, for concurrent writers within one database. MVCC lifts SQLite's one-write-transaction-at-a-time limit inside a node; it doesn't change FleetRing's one writer *node* per database, which leases and epochs keep (ADR-0013, point 5; ADR-0018).
- **Postgres adapter**, self-replicating through streaming replication. `fleetringd` only places, leases, and fences.
- **TLS between nodes** through rustls, deferred from FR-023, with a certificate story (generated per cluster, or supplied).
- **Durability sinks:** S3 snapshots and log archiving for point-in-time restore, as a trait separate from replicas (MVP step 5).
- **Engine-changing migrations**, such as SQLite to Postgres behind an `upgrade` command (ADR-0021), after live migration (MVP step 3).

**Placement crate.**


- **Keyed hasher** (SipHash-1-3 with a secret key) for adversarial key spaces.
- **`no_std + alloc`** support. The `std` feature is already reserved.
- **WASM bindings and the ring playground**, plus TypeScript and Python conformance kits built from the vectors.
- **Client-side override cache**: home placement plus redirects, with a cache of actual placements.
- **Load-aware rebalance planner** for hot-spot moves in `fleetringd` v0.2.
- **Replica anti-entropy** ([ADR-0010](../adr/0010-replica-anti-entropy-hash-trees.md), proposed). An incremental XOR page checksum proves a follower matches its primary, and an on-demand, page-level BLAKE3 hash tree repairs only the pages that drifted. This belongs in the replication crate, not in `fleetring`.
