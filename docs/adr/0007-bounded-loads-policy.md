# ADR-0007: Bounded loads as a deterministic policy applied at assignment time

- **Status:** Accepted (confirmed by Sam Ruberti, 2026-10-27)
- **Date:** 2026-10-27
- **Related:** ADR-0001, ADR-0003, ADR-0006; ticket FR-011

## Context

Consistent hashing with bounded loads (Mirrokni, Thorup, Zadimoghaddam, SODA 2018) caps each bin at `⌈(1+ε)·m/n⌉` balls. When a ball's first-choice bin is full, the ball goes to the next bin in its order. This guarantees that no bin holds more than (1+ε)× the average load. The catch is that the result depends on current loads, so it can't be recomputed from membership alone.

Two facts about FleetRing's workload:

- **Database counts are already balanced.** Take m databases and a node holding weight share p. The node's database count has a relative standard deviation of √((1−p)/(m·p)). For 1M databases on 10 equal nodes, that's 0.3%. At fleet scale, weighted rendezvous needs no help balancing counts. Small fleets with few databases are the exception.
- **The real imbalance comes from size and traffic.** Tenants follow power laws, so one database can be 1000× larger than another. Hashing balances counts, not bytes or QPS.

The control db already holds an authoritative db→node map (ADR-0001), and clients already follow redirects when a database moves.

## Decision

1. **The ring stays pure.** Home placement depends only on the ring and the key.

2. **Bounded loads is a separate policy,** applied when a database is created or re-placed:

   ```rust
   let policy = BoundedLoads::new(Headroom::percent(25));         // ε = 0.25
   let view = policy.load_view(&ring, current_loads)?;            // (NodeId, u64) pairs, any unit
   let placed = view.replicas("agent:sam-researcher", item_load); // deterministic
   ```

   - **Load unit.** The caller picks it: database count, bytes, QPS, or anything else that fits in a `u64`. Loads count every replica a node hosts.
   - **Binding.** The view borrows the ring, so a snapshot can't be used with a different ring. Building a view fails on node IDs the ring doesn't know.
   - **Capacity.** Node *i*'s capacity is `⌈(100 + ε%) · (total + r·item) · wᵢ / (100 · W)⌉`, computed in `u128` (ADR-0003). Here `total` is the current load summed over all nodes, `r` is the number of replicas being placed, and `W` is the ring's total weight.
   - **Selection.** The policy picks `replicas_where(key, |n| load(n) + item ≤ capacity(n))`, which keeps zone and host diversity (ADR-0006). This is the MTZ forwarding rule applied to our preference order.
   - **Saturation.** If fewer than R nodes are eligible, which can happen with large items, the remaining slots are filled from the unfiltered preference list and the result reports `is_saturated()`. The policy never fails and never returns an empty set.
   - **Home placement check.** `is_home()` reports whether the result matches home placement. If it doesn't, the daemon must record an override.
   - **Determinism.** The same ring, key, loads, ε, and item load give the same result on every platform.

3. **`fleetringd` records the result** in the control db, which is authoritative. Clients compute home placement and follow a redirect for the few databases placed elsewhere. Placements must be made one at a time, or in batches with a fresh snapshot each time, so each one sees the loads left by the previous ones.

4. **No slot or partition layer.** A two-level key→slot→node table would make bounded loads a pure function. But it moves databases in slot-sized batches, ships a table to every client, and still can't see database size or traffic.

5. **Rebalancing existing databases is out of scope for v0.1.** Hot-spot moves (v0.2) belong to `fleetringd`. A later library helper may plan moves from current assignments and loads.

## Consequences

- The README's "bounded loads" claim stays accurate: this is MTZ forwarding over our preference order, with capacity in any unit.
- The bound holds at the moment of placement. Databases that grow afterward push their nodes past it, and the v0.2 rebalancer corrects that drift.
- For some databases, actual placement differs from home placement. Redirects and the control db handle that by design (ADR-0001).
