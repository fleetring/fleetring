---
id: FR-008
title: Weighted rendezvous candidate order (default strategy)
status: todo
priority: P0
size: M
milestone: M2 Placement core
depends_on: [FR-005, FR-006, FR-007]
adrs: [ADR-0003, ADR-0004]
---

# FR-008: Weighted rendezvous candidate order (default strategy)

## Goal

Implement the default strategy: for each key, produce a total order over nodes that is exactly proportional to weight and that changes minimally when membership changes (ADR-0003, ADR-0004).

## Scope

- **Internal candidate-order abstraction.** Add an internal trait or enum-dispatched function that FR-009 and FR-010 share. Given a key hash, it yields nodes in preference order, either lazily or into a caller-provided buffer.
- **Rendezvous order**, computed per key:
  - `key_hash = H(key, seed)`
  - for each node, `x = H(le64(node_hash) ‖ le64(key_hash), seed)` and `L = neg_log2_q32(x)`
  - nodes are sorted with the FR-005 comparator.
- **Exactly-equivalent fast paths**
  - Top-k selection without a full sort.
  - The equal-weight shortcut: compare raw draws only.
  - Lazy or memoized computation of `L`.
- **`Ring::primary(key) -> &Node`** as the first consumer, so the order can be tested end to end before FR-009 lands.

## Acceptance criteria

- [ ] The order is a permutation of all nodes, is deterministic, and doesn't depend on build order.
- [ ] **Insertion property:** adding a node X leaves the relative order of every other node unchanged, for every key. Removing X and reweighting X likewise move only X. Prove both with proptest over memberships × keys.
- [ ] **Proportionality.** Over the FR-006 corpora, each node's primary count passes a per-node 5σ check and the χ² check against wᵢ/Σw. Run this on fixtures with 10 equal nodes, weights 1/2/3/4, and weights 1 and 1000.
- [ ] Sequential keys show no bias compared with random keys: both corpora pass the same checks.
- [ ] On proptest inputs, the fast paths produce exactly the same order as a naive reference that computes every `L` and sorts fully.
- [ ] A bench hook exists for `primary()` at N ∈ {3, 100, 1000}. Record the numbers in FR-016.
