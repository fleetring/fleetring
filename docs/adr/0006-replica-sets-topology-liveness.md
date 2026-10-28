# ADR-0006: Replica sets, topology-aware selection, and liveness filtering

- **Status:** Accepted (2026-10-27)
- **Date:** 2026-10-27
- **Related:** ADR-0001, ADR-0004, ADR-0007; tickets FR-004, FR-009, FR-015

## Context

`docs/mvp.md` says: "A database's first ring node is its primary; the next N−1 are followers pulling its WAL. Leases in a small control database pick the primary."

Replicas should land in different zones when possible, and on different hosts whenever enough hosts exist. A sleeping laptop counts as "a temporarily down node": it must not trigger data movement, but traffic still has to fail over around it.

## Decision

1. **Replica set.** A replica set is an ordered list of distinct nodes. Index 0 is the *preferred primary*; `fleetringd`'s lease confirms it or fails over to another replica. The next R−1 nodes are followers. `ReplicationFactor` is a nonzero `u8` and defaults to 3. A `ReplicaSet` is never empty.
2. **Topology labels.** Every node has a zone and a host.
   - A node without a zone belongs to the single default zone.
   - A node without a host label is its own host.
   - A host label must belong to exactly one zone, and `build()` rejects rings that break this rule.
   - Regions are not a spreading level. Placing data across regions is a residency/pool question (see the backlog), not a matter of spreading replicas.
3. **Selection makes three passes over the strategy's candidate order** (ADR-0004):
   1. Take each node whose **zone** hasn't been used yet.
   2. Take each remaining node whose **host** hasn't been used yet.
   3. Take every remaining node.

   The three passes concatenated form the key's **preference list**, and the replica set is its first R entries. When no topology labels are set, this is just the plain candidate order.
4. **Guarantees.** FR-009 verifies each of these with property tests.
   - **Distinct and bounded:** members are distinct, and `len = min(R, eligible nodes)`.
   - **Prefix-consistent:** the replica set for R is a prefix of the replica set for R+1. Changing a database's replication factor never reshuffles its existing replicas.
   - **Maximally diverse:** with Z zones, the first min(R, Z) replicas sit in distinct zones, and hosts are distinct whenever enough hosts exist.
   - **Minimal churn:** adding, removing, or reweighting one node changes at most one member of any replica set. The primary changes only if the changed node is, or becomes, the key's top candidate.
   - **Weight-proportional primaries:** the primary is always the top candidate, so under rendezvous each node is primary for exactly wᵢ/Σw of the keys.
5. **Too few nodes is not an error.** If fewer than R nodes are eligible, the replica set is shorter and `is_under_replicated()` returns true. Running `fleetring up` on a single laptop just works.
6. **Liveness and other runtime filters.** `preference_where(key, |node| …)` and `replicas_where(key, |node| …)` apply the predicate *before* the three passes, so failover keeps zones diverse: if a zone's best node is down, that zone's next-best node replaces it. Filtering by liveness gives the same result as building a ring without the down nodes, and a filter never moves data. `replicas_where` returns `None` only when the predicate rejects every node.
7. **The hot path doesn't allocate.** `replicas()` makes no heap allocation in typical shapes (R ≤ 8 and ≤ 8 zones), which a counting-allocator test verifies. The full `preference()` iterator may allocate O(N).

## Alternatives considered

- **Cassandra-style rack-aware ring walk:** essentially the same idea. Our candidate order generalizes the ring walk so it also covers rendezvous.
- **CRUSH-style hierarchy (choose zones by aggregate weight, then a node within each zone):** balances replicas better when zone sizes are skewed, but loses minimal churn. Adding a node raises its zone's weight, which shuffles keys among that zone's existing nodes.
- **Failing when R exceeds the number of nodes or zones:** this would break bootstrap and home-lab clusters with no safety benefit. The under-replication flag and diagnostics surface the problem instead.

## Consequences

- **Known trade-off:** strict zone diversity concentrates replicas in small zones. With zones of 10, 10, and 2 nodes and R = 3, every database keeps one replica in the 2-node zone. The constraint makes this unavoidable, so diagnostics (FR-015) report it rather than silently weakening diversity.
- Failover order is deterministic, so every router picks the same fallback when a node is down.
