---
id: FR-009
title: Topology-aware replica selection, ReplicaSet, and preference lists
status: done
priority: P0
size: L
milestone: M2 Placement core
depends_on: [FR-008]
adrs: [ADR-0001, ADR-0006]
---

# FR-009: Topology-aware replica selection, `ReplicaSet`, and preference lists

## Goal

Implement ADR-0006 on top of any strategy's candidate order. Ordered replica sets spread across zones and hosts, and failover filtering keeps that diversity.

## Scope

- **Selection:** three passes over the candidate order (zone, then host, then anything), producing the preference sequence.
- **`ReplicaSet<'ring>`** (never empty):
  - accessors: `primary()`, `followers()`, `iter()`, `get(i)`, `len()`, `contains(&NodeId)`, `is_under_replicated()`, `to_ids() -> Vec<NodeId>`;
  - `Display` renders as `[a, b, c]`;
  - `IntoIterator` is implemented for `&ReplicaSet`;
  - storage is inline for R ≤ 8 (`smallvec`, not exposed).
- **`Ring` methods**
  - `replicas(key) -> ReplicaSet` and `replicas_n(key, ReplicationFactor) -> ReplicaSet`.
  - `replicas_where(key, pred) -> Option<ReplicaSet>`, which returns `None` only when the predicate rejects every node.
  - `preference(key)` and `preference_where(key, pred)`, which return a `Preference<'ring>` iterator.
- **Predicates** have type `FnMut(&Node) -> bool` and are applied before the passes.
- **Allocation test:** a counting-allocator integration test (`tests/alloc_free.rs`) that installs its own `#[global_allocator]`.

## Acceptance criteria

These are property tests over random memberships, keys, R, and predicates, run for every strategy.

- [x] Members are distinct, `len == min(R, eligible)`, and `is_under_replicated()` is true exactly when `len < R`.
- [x] **Prefix consistency:** `replicas_n(k, r)` is a prefix of `replicas_n(k, r + 1)` and of `preference(k)`.
- [x] **Diversity:** the first `min(R, eligible zones)` members are in distinct zones, and hosts stay distinct as long as enough hosts exist.
- [x] **Minimal churn:** adding, removing, or reweighting a single node swaps at most one member (one out, one in) for every key. The primary changes only if the changed node is, or becomes, the top candidate.
- [x] **Filter semantics**
  - `replicas_where(k, |_| true) == Some(replicas(k))`.
  - Filtering by liveness gives the same result as `replicas(k)` on a ring built without the dead nodes.
- [x] `replicas()` makes zero heap allocations on the 1000-node, 8-zone fixture with R ≤ 8.
- [x] The rustdoc shows primary, followers, and failover with a liveness predicate.

## Notes

- `ReplicaSet` is never empty, so `len()` comes without `is_empty()`. Silence `clippy::len_without_is_empty` with `#[expect(..., reason = "a replica set is never empty")]`.
- An allocation-free approach for small R: keep the best candidate per zone in a stack array, and if Z < R, rescan for the remaining host-diverse picks. Results must match the naive three-pass reference exactly.

## Progress

- 2026-10-27: Done so far:
  - `select()` and `preference()` with the three passes.
  - `ReplicaSet`, `ReplicaIter`, and `Preference`. Equality compares node IDs in order and ignores the replication factor.
  - Property tests for minimal churn on add, remove, and reweight, plus zone diversity.

  Still open:
  - the `Ring` methods;
  - the liveness-equivalence property;
  - the counting-allocator test.
- 2026-10-27: Done.
  - **Public lookups:** `replicas`, `replicas_n`, `replicas_where` (returns `Option`), `primary`, `preference`, and `preference_where`.
  - **Allocation check:** `tests/alloc_free.rs` (dhat, no harness) measures 0 heap allocations across 9,000 lookups on a 1000-node, 8-zone ring with mixed weights, for both strategies.
  - **Failover equivalence:** a public-API proptest shows that filtering out down nodes gives the same replica set as rebuilding the ring without them. It runs for both strategies.
