# Changelog

All notable changes to the `fleetring` crate are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Placement output is versioned separately, by the [placement spec](docs/spec/placement-v1.md): from 0.1.0 on, any change to which nodes a key maps to needs a new spec version, whatever the crate version.

## [Unreleased]

The first functional release of the placement crate. Releasing it as 0.1.0 also freezes placement spec v1.

### Added

- **Rings.** `Ring` and `RingBuilder` build immutable, validated rings. A ring keeps its nodes sorted by ID, so placement depends only on the set of nodes, never on the order they were added in. Each `Node` has a stable `NodeId`, a `Weight`, and optional `Zone` and `Host` labels. `Ring::to_builder` derives the next ring for a membership change, and an `Epoch` versions each ring without affecting placement.
- **Placement strategies.** Weighted rendezvous hashing is the default: weights are honored exactly, using an integer-only fixed-point logarithm, and a lookup costs O(N). A weighted virtual-node ring (`Strategy::Vnodes`, `VnodeParams`) trades exact balance for O(log T) lookups in very large fleets.
- **Stable hashing.** The `StableHasher` trait, with XXH3-64 (`Xxh3`) as the default hasher.
- **Replica selection.** `Ring::replicas` and `Ring::replicas_n` return a key's `ReplicaSet`: the preferred primary, then its followers, spread across zones first and then hosts. `Ring::primary` returns the preferred primary alone.
  - The set for a replication factor is a prefix of the set for any larger one.
  - Adding, removing, or reweighting one node changes at most one member of any set.
  - A ring with too few nodes returns a shorter set that reports itself under-replicated, rather than an error.
  - Lookups don't allocate for up to 8 replicas.
- **Preference lists.** `Ring::preference` returns every node in a key's failover order.
- **Filters.** `Ring::replicas_where` and `Ring::preference_where` skip the nodes a predicate rejects, such as down nodes, before replicas spread across zones. The result equals the placement of a ring built without the rejected nodes, so filters never move data.
- **Bounded loads.** `BoundedLoads`, `Headroom`, `LoadView`, and `BoundedPlacement` implement consistent hashing with bounded loads as a policy applied when a key is assigned, with loads in any unit. Each placement reports whether it matches home placement and whether it saturated.
- **Migration planning.** `Ring::diff` lazily yields a `Change` for each key whose replica set differs between two rings. `Diff::summarize` and `MovementSummary::from_changes` count the changes in a `MovementSummary`, which displays as a table. `Ring::estimate_movement` estimates the same counts without a list of keys.
- **Ring specs and fingerprints.** `RingSpec`, `NodeSpec`, and `StrategySpec` describe a ring portably. `Ring::to_spec` writes a spec, and `Ring::from_spec` validates one and rebuilds its ring. A `Fingerprint`, from `Ring::fingerprint`, covers every placement input and the placements of 64 probe keys, so peers can detect rings that would place keys differently.
- **Cells: nested placement** (ADR-0012). A `Cells` layer picks a cell for each key with a `Directory` of exact-key and longest-prefix rules, then a selector ring whose nodes are the cells, weighted explicitly; the cell's own ring places all of the key's replicas. Cells are `Ring`s or nested layers, through the sealed `Placement` trait, and `Cells::replicas` returns a `CellPlacement`: the key's path of `CellId`s plus its `ReplicaSet`. `CellsBuilder::build` validates every selector target and rejects a ring that shares a seed with a selector ring above it (`BuildCellsError`). `Cells::fingerprint` covers every layer, and `Cells::diff` tells cross-cell moves from in-cell changes (`CellChange`, `CellMovementSummary`). Placement spec v1 gains a composition section, with golden vectors.
- **Diagnostics.** `Ring::diagnostics` reports under-replication, too few hosts or zones, zones that receive far more than their share of replicas, and vnode nodes with too few tokens. `Ring::expected_shares` reports each node's exact share of primaries and its estimated share of replicas.
- **Placement spec v1** (draft), in `docs/spec/placement-v1.md`: the normative, language-neutral definition of placement, from byte encodings to the fingerprint, so ports in other languages can match the Rust crate exactly.
- **Feature flags.** `serde` serializes ring specs, `test-util` adds the `test_util::TableHasher` test double, and `std`, on by default, is reserved for future `no_std` support.
- **Documentation and examples.** A crate-level guide to the concepts, the choice of strategy, and the determinism guarantees. Seven runnable examples cover a database fleet, key-value sharding, chat-room failover, rebalancing, bounded loads, regional cells, and a ring spec round trip.
- **Benchmarks** for lookups, builds, bounded loads, and diffs, with budgets and results in `docs/perf.md`.
- **MSRV and lint policy.**
  - The minimum supported Rust version is 1.88, on edition 2024. It rises only in a minor release, and every change is noted here.
  - Unsafe code is forbidden. Every public item is documented, and `unwrap` and lossy casts are linted.
  - Floating point is denied in the crate, and hash maps, hash sets, and the standard library's hashers are banned from it, because they would break bit-identical placement.
  - CI treats warnings as errors and runs the tests on Linux, macOS, and Windows, and on 32-bit, big-endian, and WebAssembly targets. `cargo deny` checks dependencies and licenses.

## [0.0.1] - 2026-10-27

A placeholder that reserved the crate name on crates.io. It has no placement functionality.

[Unreleased]: https://github.com/fleetring/fleetring/commits/main
[0.0.1]: https://crates.io/crates/fleetring/0.0.1
