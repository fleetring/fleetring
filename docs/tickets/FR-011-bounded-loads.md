---
id: FR-011
title: Bounded-load placement policy
status: done
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

- [x] With every load at zero, the result equals home placement and `is_home()` is true.
- [x] A node is never chosen with `load + item > capacity` unless `is_saturated()` is true.
- [x] **Simulation.** Place 100k items one at a time, with count loads and ε = 25%, on the FR-006 fixtures, for both strategies:
  - No node ever exceeds its capacity, except in placements flagged as saturated.
  - On fixtures with N ≥ 2R, the saturation count is 0.
- [x] Zone and host diversity rules still hold under filtering (reuse the FR-009 property checks).
- [x] There is no overflow for loads up to `u64::MAX` and weights up to `u32::MAX`. Prove it with proptest in debug builds.
- [x] Results are deterministic. Golden vectors for them are added in FR-014.

## Out of scope

- Rebalancing existing databases (hot-spot moves). That belongs to `fleetringd` in v0.2.

## Progress

- 2026-10-28: Done. A subagent built it in an isolated copy, and I merged and re-verified it. Its tests run with both strategies.
  - **API:** `BoundedLoads::new(Headroom::from_percent(25))` → `load_view(&ring, (id, u64)…)` → `LoadView::replicas(key, item_load)` → `BoundedPlacement` with `is_saturated()` and `is_home()`.
  - **Capacity:** computed as `⌊q/d⌋·f + ⌈(q mod d)·f/d⌉`, which stays below 2¹⁰⁸ for any input. Proptests run at the extremes (`u64::MAX` loads, `u32::MAX` weights, ε = `u16::MAX`).
  - **Simulation** (100k placements, ε = 25%): `three_zones(4)` peaked at 1.0065× the fair share and `fleet(12, 3)` at 1.0073×, with no saturations. `homelab` (N = R) saturates by construction.
- Decisions made during the merge:
  - **Replica count:** `r = min(R, N)`, the number of replicas actually placed. ADR-0007 and spec §9 are updated to match.
  - **Saturation fill:** it comes from the home replica set, which provably equals filling from the preference list and avoids an allocation.
  - **Headroom range:** ε is a `u16` percentage.
