# ADR-0004: Weighted rendezvous by default, vnode ring as an option, no jump hash

- **Status:** Accepted (confirmed by Sam Ruberti, 2026-10-27)
- **Date:** 2026-10-27
- **Related:** ADR-0003, ADR-0006, ADR-0008; tickets FR-008, FR-010

## Context

The MVP calls for "weighted virtual nodes, replication factor, host/zone-aware placement, bounded loads, pluggable jump/rendezvous strategy." Fleets range from three laptops to about a thousand machines for neocloud operators. Capacity varies, with old laptops sitting next to servers. Any node can leave at any time, because laptops sleep and machines die. Replicas must spread across zones.

Requirements for a strategy, in priority order:

1. Deterministic, portable output (ADR-0002, ADR-0003).
2. Minimal disruption: adding, removing, or reweighting one node moves only the keys that must move.
3. Placement proportional to weight.
4. A total preference order for each key, which replica selection needs (ADR-0006).
5. Lookups in microseconds and a small memory footprint.

## Decision

1. **Strategies are a closed enum, not a trait.**

   ```rust
   #[non_exhaustive]
   pub enum Strategy {
       Rendezvous,          // default
       Vnodes(VnodeParams), // tokens_per_weight, ...
   }
   ```

   Placement algorithms are part of the versioned spec (ADR-0008). A closed set can be serialized and chosen at runtime from a spec read out of the control db, and every port can implement it. The extension point is the hasher (ADR-0002). A new strategy means a new variant and a new spec version.

2. **The default is weighted rendezvous (highest random weight).** For each key, every node gets a draw `x = H(le64(node_hash) ‖ le64(key_hash), seed)`, and nodes are ordered by the exact integer comparison in ADR-0003.
   - **Proportional:** placement is exactly proportional to weight, at any weight scale.
   - **Minimal disruption:** other nodes' draws don't change, so adding a node inserts it into each key's order, removing a node deletes it, and reweighting moves only that node.
   - **Memory:** nothing beyond the node list.
   - **Replicas:** zone-aware replica selection behaves best on rendezvous order (ADR-0006).
   - **Cost:** a lookup is O(N), about one 16-byte hash per node, plus one fixed-point logarithm per node when weights differ. FR-016 sets and checks the budget, initially ≤ 2 µs at 100 nodes with R = 3.

3. **Optional: a weighted virtual-node ring**, i.e. consistent hashing as described by Karger et al. and used in Dynamo and Cassandra.
   - **Tokens:** a node gets `weight × tokens_per_weight` tokens (default 256 per unit of weight). Token *j* is `H(le64(node_hash) ‖ le32(j), seed)`. Tokens are sorted by `(token, NodeId)`.
   - **Candidate order:** a key's order is a clockwise walk from `key_hash`, taking each node at its first token.
   - **Minimal disruption:** a node's token count depends only on its own weight, so adding, removing, and reweighting stay minimal.
   - **Cost:** a lookup is O(log T). Balance error per node is about 1/√tokens, roughly ±6% at 256 tokens.
   - **Limit:** total tokens are capped at 2²², and `build()` fails beyond that.
   - **When to use it:** when N × lookup rate makes O(N) per lookup too expensive.

4. **Jump consistent hash is not offered.** Lamping and Veach call its "main limitation" that "the buckets must be numbered sequentially." Removing node *k* means one of two things:
   - renumbering, which remaps every bucket above *k*; or
   - moving the last bucket into the hole, which roughly doubles the movement and makes placement depend on the history of removals. That history then has to be stored and shipped with the ring.

   Jump hash also has no weights, and it needs extra machinery to produce distinct, zone-aware replicas. FleetRing removes nodes routinely, so jump hash doesn't fit. If a fixed-size slot layer ever appears, it can be reconsidered there.

5. **Shared contract with replica selection.** For each key, a strategy produces a total order of nodes with three properties:
   - adding a node only inserts that node;
   - removing a node only deletes that node;
   - reweighting a node only moves that node.

   Replica selection (ADR-0006) relies on nothing else, so both strategies inherit its guarantees.

## Alternatives considered

- **Strategy as an open trait:** third parties could add algorithms, but those couldn't be serialized, specified, or ported. A trait can still be added later without a breaking change.
- **Maglev hashing:** O(1) lookups through a permutation table, but by design it doesn't minimize disruption. It was built for load balancers, where extra churn is cheap. For us, churn means migrating data.
- **AnchorHash, MementoHash:** support arbitrary removals in little memory, but they depend on removal history and have no weights.
- **Multi-probe consistent hashing:** good balance with one token per node, but weights and zone-aware replicas are awkward.
- **Hierarchical rendezvous (pick a zone, then a node within it):** faster at very large N. But adding a node raises its zone's weight, which shuffles keys among that zone's existing nodes. It's on the backlog in case FR-016 shows that N > ~1000 needs it.

## Consequences

- The README's "jump and rendezvous strategies" now reads "weighted rendezvous (default) or a virtual-node ring".
- Spec v1 has to specify two algorithms, and the golden vectors cover both.
- Vnode weights should be small integers in relative units, because the token count scales with weight. Rendezvous accepts any `u32` weight.
