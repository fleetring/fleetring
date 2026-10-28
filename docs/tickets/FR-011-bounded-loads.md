---
id: FR-011
title: Bounded-load placement policy
status: todo
priority: P1
size: M
milestone: M2 Placement core
depends_on: [FR-009]
adrs: [ADR-0003, ADR-0007]
---

# FR-011: Bounded-load placement policy

## Goal

Implement ADR-0007: consistent hashing with bounded loads (MTZ forwarding) as a deterministic policy applied at assignment time. Callers supply their own loads, in any unit.

## Scope

- **`Headroom`:** ε as an integer percentage, e.g. `Headroom::percent(25)`. It's validated and has an accessor.
- **`BoundedLoads::new(headroom)`**
- **`BoundedLoads::load_view(&ring, loads) -> Result<LoadView<'ring>, LoadViewError>`**
  - `loads` is any `IntoIterator<Item = (NodeId, u64)>`.
  - The view borrows the ring, so it can't be used with a different one.
  - An unknown `NodeId` is an error. Nodes with no entry have load 0.
  - Per-node loads are indexed the same way as the ring, and the total is precomputed in `u128`.
- **`LoadView::replicas(key, item_load) -> BoundedPlacement<'ring>`**
  - Capacity: `⌈(100 + ε%) · (total + r·item) · wᵢ / (100 · W)⌉`, computed with `u128` ceiling division (FR-005).
  - Selection: `replicas_where(key, |n| load(n) + item ≤ capacity(n))`. If fewer than R nodes are eligible, the remaining slots are filled from the unfiltered preference list.
- **`BoundedPlacement`** exposes `replicas()`, `is_saturated()`, and `is_home()`. `is_home()` is false when the result differs from home placement, which tells the daemon to record an override.

## Acceptance criteria

- [ ] With every load at zero, the result equals home placement and `is_home()` is true.
- [ ] A node is never chosen with `load + item > capacity` unless `is_saturated()` is true.
- [ ] **Simulation.** Place 100k items one at a time, with count loads and ε = 25%, on the FR-006 fixtures, for both strategies:
  - No node ever exceeds its capacity, except in placements flagged as saturated.
  - On fixtures with N ≥ 2R, the saturation count is 0.
- [ ] Zone and host diversity rules still hold under filtering (reuse the FR-009 property checks).
- [ ] There is no overflow for loads up to `u64::MAX` and weights up to `u32::MAX`. Prove it with proptest in debug builds.
- [ ] Results are deterministic. Golden vectors for them are added in FR-014.

## Out of scope

- Rebalancing existing databases (hot-spot moves). That belongs to `fleetringd` in v0.2.
