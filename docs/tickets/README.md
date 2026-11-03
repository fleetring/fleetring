# FleetRing tickets: the archive

**New and open work is tracked in [GitHub Issues](https://github.com/fleetring/fleetring/issues) and the FleetRing roadmap project** (an organization project, linked from the repository's Projects tab). This directory is the archive of the tickets that were done before work moved there. Its files stay, so links from accepted ADRs keep working. The rows below that link to an issue were still open at the move; the ticket's text now lives in the issue, and the implementation plans of the larger ones are in [`docs/plans/`](../plans/).

The `fleetring` placement crate (MVP step 1 in [`docs/mvp.md`](../mvp.md), released as 0.1.0) and the `fleetringd` daemon (MVP step 2) were planned here. Design decisions are recorded in [`docs/adr/`](../adr/README.md). The epics that lead to a release safe to depend on are in [`docs/roadmap.md`](../roadmap.md), and [`docs/review.md`](../review.md) records the known bottlenecks and the standards the design follows or departs from.

## Conventions

- **Issues keep the `FR-NNN` number in their title** (`FR-049: An S3 durability sink …`), with the priority, size, and milestone as labels and a milestone. The archived files here start with YAML front matter (`status`, `priority`, `size`, `depends_on`, `adrs`), described by:
  - `priority`: `P0` blocks `fleetringd` work, `P1` is required for 0.1.0, `P2` may slip past 0.1.0. For daemon tickets (FR-022 on), `P0` is required for `fleetringd` v0.1, `P1` should ship with it, and `P2` may slip.
  - `size`: `S` is half a day or less, `M` is 1–2 days, `L` is 3–4 days
- **Commits** follow Conventional Commits, using the crate area as the scope (`feat(ring): …`) and a `Refs: FR-NNN` or `Refs: #N` trailer. A pull request body says `Fixes #N` to close its issue.
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

M7 to M12 are also GitHub milestones, with the outcome as the description.

| Milestone | Tickets | Outcome |
|---|---|---|
| M0 Foundations | FR-001, FR-002 | The workspace and CI enforce ADR-0009 on every platform where determinism usually breaks |
| M1 Hashing and model | FR-003 – FR-006 | Stable hasher, domain types, fixed-point math, and a deterministic test harness |
| M2 Placement core | FR-007 – FR-011 | Ring, rendezvous, topology-aware replica sets, vnode ring, and bounded loads |
| M3 Change and interop | FR-012 – FR-015, FR-020, FR-021 | Diff, `RingSpec` and fingerprint, spec v1 with golden vectors, diagnostics, and cells |
| M4 Hardening and release | FR-016 – FR-019 | Benchmarks, fuzzing, docs, and the 0.1.0 release with the spec frozen |
| M5 Daemon foundations | FR-022 – FR-029 | `fleetringd` crates, transport, simulation harness, WAL library, database actors, control plane, and membership |
| M6 Replication and failover | FR-030 – FR-034 | WAL shipping, leases and failover with no acknowledged write lost, snapshot catch-up, a CLI, and release builds |
| M7 Durability and operations | FR-035 – FR-038, FR-051, FR-054 | Configurable durability per database, and a history checker for every simulation run |
| M8 Access and integration | FR-039 – FR-043, FR-061 | Change streams, lease watching and keyed leases, then the router, the client, and the MCP server |
| M9 Archives and verification | FR-044, FR-045, FR-049, FR-052, FR-053 | A hash-chained log, Merkle anti-entropy, and verifiable archives with point-in-time restore, so replicas and archives can prove what they hold |
| M10 Security | FR-046 – FR-048, FR-050, FR-055 | Mutual TLS with a built-in certificate authority, principals and scopes with tokens, the threat model and signed releases, and node calls bound to placement |
| M11 Operator experience | FR-056 – FR-059, FR-062 | One vocabulary (fleet, not cluster, and one word per thing), CLI contexts with stable JSON output, a k9s-style TUI, and a desktop viewer, all over one client crate |
| M12 Distribution | FR-060 | Native packages: `.deb` and `.rpm` from the release workflow, the AUR, and a Homebrew tap, each traceable to a signed release |

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
| [FR-024](FR-024-simulation-harness.md) | Simulation harness with turmoil | P0 | M | FR-022, FR-023 | done |
| [FR-025](FR-025-wal-format.md) | WAL format library | P0 | M | FR-022 | done |
| [FR-026](FR-026-database-actor.md) | Engine adapter and per-database actor | P0 | L | FR-022, FR-025 | done |
| [FR-027](FR-027-control-state-machine.md) | Control state machine | P0 | M | FR-022 | done |
| [FR-028](FR-028-control-node.md) | Control node service and client | P0 | M | FR-023, FR-027 | done |
| [FR-029](FR-029-membership.md) | Membership, heartbeats, and ring publication | P0 | M | FR-028 | done |
| [FR-030](FR-030-replication-stream.md) | Replication stream and follower mirroring | P0 | L | FR-023, FR-026, FR-029 | done |
| [FR-031](FR-031-leases-failover.md) | Leases, epochs, quorums, and failover | P0 | L | FR-030 | done |
| [FR-032](FR-032-snapshot-catch-up.md) | Snapshot catch-up | P1 | M | FR-030 | done |
| [FR-033](FR-033-database-api-cli.md) | Database API and CLI for `fleetringd` v0.1 | P1 | M | FR-026, FR-028 | done |
| [FR-034](FR-034-release-builds.md) | Release builds, ARM CI, and container images for `fleetringd` | P1 | M | FR-022 | done |
| [FR-035](FR-035-configurable-durability.md) | Configurable durability (`acks`, `min_replicas`, per database) | P1 | L | FR-030, FR-031 | done |
| [FR-036](FR-036-history-checker.md) | History checker for simulation runs | P1 | M | FR-024, FR-031 | done |
| [FR-037](FR-037-cli-polish.md) | CLI polish: `db list`, `db drop`, `status --node`, clear answers from data-only nodes | P1 | M | FR-033 | done |
| [FR-038](FR-038-graceful-departure.md) | Graceful departure: drain a node's primaries before it leaves | P1 | M | FR-031 | done |
| [FR-039](FR-039-change-streams.md) | Change streams: a sink trait that sends each database's changes elsewhere | P2 | L | FR-030 | done |
| [FR-040](https://github.com/fleetring/fleetring/issues/53) | Turso adapter: a self-replicating engine whose change stream reads `turso_cdc` | P2 | L | FR-039 | todo |
| [FR-041](https://github.com/fleetring/fleetring/issues/49) | Replicated sink positions, so a new primary resumes a change stream without duplicates | P2 | M | FR-039 | todo |
| [FR-042](FR-042-lease-watch.md) | `WatchLeases`: a stream of the leases this node gains and loses | P2 | M | FR-031, FR-037 | done |
| [FR-043](https://github.com/fleetring/fleetring/issues/50) | Keyed leases: acquire, renew, release, and watch a lease on any key | P2 | L | FR-042, FR-029, FR-047 | todo |
| [FR-044](FR-044-hash-chained-log.md) | A hash-chained replication log, so replicas can prove they share a prefix | P2 | L | FR-030, FR-032 | done |
| [FR-045](https://github.com/fleetring/fleetring/issues/51) | Merkle anti-entropy over pages, so a returning follower fetches only what differs | P2 | L | FR-044, FR-032, FR-047 | todo |
| [FR-046](FR-046-mutual-tls.md) | Mutual TLS between nodes and clients, with a built-in certificate authority | P0 | L | FR-029, FR-037 | done |
| [FR-047](FR-047-scoped-credentials.md) | Principals and scopes: tokens and prefix rules enforced at every RPC | P0 | L | FR-046 | done |
| [FR-048](FR-048-threat-model-and-signed-releases.md) | A threat model in SECURITY.md and verifiable release artifacts | P1 | M | FR-034, FR-046, FR-047 | done |
| [FR-049](https://github.com/fleetring/fleetring/issues/52) | An S3 durability sink with verifiable point-in-time restore | P1 | L | FR-039, FR-041, FR-044, FR-047 | todo |
| [FR-050](../plans/FR-050-node-calls-bound-to-placement.md) | Bind node-to-node calls to placement, so a stolen node key reaches only its own databases | P1 | M | FR-047 | done |
| [FR-051](https://github.com/fleetring/fleetring/issues/54) | Find and fix the replication tests that fail intermittently in CI | P1 | M | — | in-progress |
| [FR-052](https://github.com/fleetring/fleetring/issues/55) | A durability mode that acknowledges a write only once it is archived | P2 | M | FR-049, FR-035 | todo |
| [FR-053](https://github.com/fleetring/fleetring/issues/56) | Object-store credentials from workload identity, not only static keys | P2 | S | FR-049 | todo |
| [FR-054](https://github.com/fleetring/fleetring/issues/57) | `fleetringd upgrade` and a tested rolling upgrade | P1 | L | FR-038, FR-048 | todo |
| [FR-055](https://github.com/fleetring/fleetring/issues/58) | Pick a client identity without repeating `--cert` and `--key` | P2 | S | FR-047 | todo |
| [FR-056](https://github.com/fleetring/fleetring/issues/59) | CLI contexts, `--json` on every read command, shell completions, and man pages | P1 | L | FR-055, FR-059 | todo |
| [FR-057](https://github.com/fleetring/fleetring/issues/60) | A k9s-style TUI for watching and operating a fleet | P2 | L | FR-056, FR-042 | todo |
| [FR-058](https://github.com/fleetring/fleetring/issues/61) | A Tauri and React desktop app, starting as a viewer | P2 | L | FR-056 | todo |
| [FR-059](FR-059-fleet-rename.md) | Rename cluster to fleet, as a clean break in 0.3.0 | P1 | M | FR-047 | done |
| [FR-060](https://github.com/fleetring/fleetring/issues/62) | Distribution packages for apt, yum, the AUR, and Homebrew | P2 | L | FR-034, FR-048 | todo |
| [FR-061](https://github.com/fleetring/fleetring/issues/63) | A `fleetring` Ruby gem and an ActiveRecord adapter that place each tenant's database | P2 | L | FR-047, the router (no ticket yet) | todo |
| [FR-062](FR-062-one-word-per-thing.md) | One word per thing: resolve the lexicon's naming conflicts in code, CLI, protocol, and docs | P1 | M | FR-059 | done |

## Critical path and estimate

The critical path is: FR-001 → FR-003 / FR-004 / FR-005 (parallel) → FR-007 → FR-008 → FR-009 → FR-010 → FR-014 → FR-019.

| Scope | Tickets | Estimate (one engineer) |
|---|---|---|
| P0: unblocks `fleetringd` | 10 tickets | ≈ 14 engineer-days |
| P0 + P1: the 0.1.0 release | 18 tickets | ≈ 28 engineer-days |
| Everything, including P2 | 20 tickets | ≈ 29 engineer-days |

For `fleetringd` v0.1, the critical path is FR-022 → FR-023 → FR-028 → FR-029 → FR-030 → FR-031, with FR-025 → FR-026 and FR-027 running in parallel. All thirteen daemon tickets come to about 26 engineer-days.

`docs/mvp.md` allows 4–6 weeks for all eight MVP steps, with the ring crate as step 1. At about three weeks, P0 alone takes more than a proportional share. If the timeline is fixed, start `fleetringd` once P0 is done and run P1 in parallel. P2 can slip past 0.1.0 without affecting placement.

## Backlog (not yet ticketed)

**Engines and durability.** These must fit behind the engine adapter without changing anything above it (ADR-0016):

- **libSQL and Turso adapters** as frame sources, capturing commits inside the engine.
- **Turso MVCC adapter**, for concurrent writers within one database. MVCC lifts SQLite's one-write-transaction-at-a-time limit inside a node; it doesn't change FleetRing's one writer *node* per database, which leases and epochs keep (ADR-0013, point 5; ADR-0018).
- **Postgres adapter**, self-replicating through streaming replication. `fleetringd` only places, leases, and fences.
- **TLS between nodes** through rustls, deferred from FR-023, with a certificate story (generated per fleet, or supplied).
- **Engine-changing migrations**, such as SQLite to Postgres behind an `upgrade` command (ADR-0021), after live migration (MVP step 3).

**Placement crate.**


- **Keyed hasher** (SipHash-1-3 with a secret key) for adversarial key spaces.
- **`no_std + alloc`** support. The `std` feature is already reserved.
- **WASM bindings and the ring playground**, plus TypeScript and Python conformance kits built from the vectors.
- **Client-side override cache**: home placement plus redirects, with a cache of actual placements.
- **Load-aware rebalance planner** for hot-spot moves in `fleetringd` v0.2.
- **Replica anti-entropy** ([ADR-0010](../adr/0010-replica-anti-entropy-hash-trees.md), proposed). An incremental XOR page checksum proves a follower matches its primary, and an on-demand, page-level BLAKE3 hash tree repairs only the pages that drifted. This belongs in the replication crate, not in `fleetring`.
