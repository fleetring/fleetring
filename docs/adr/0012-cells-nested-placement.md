# ADR-0012: Cells: nested placement by composing rings

- **Status:** Accepted (confirmed by Sam Ruberti, 2026-10-28)
- **Date:** 2026-10-28
- **Related:** ADR-0001, ADR-0004, ADR-0006, ADR-0008, ADR-0011; ticket FR-020

## Context

Most large systems place keys in two layers.
1. A **directory or ring picks the cell**: a region, a cell, a tenant pool, or a jurisdiction.
2. A **consistent-hashing ring inside that cell** picks the nodes.

AWS cell-based architectures, Cloudflare's jurisdictions and colos, Cassandra's datacenters, Ceph's CRUSH buckets, and Vitess keyspaces all work this way. FleetRing needs the same structure for four reasons:

- **Residency:** EU tenants must stay on EU machines. A sovereign deployment is its own cell.
- **Blast radius:** a bad deploy, overload, or operator mistake stays inside one cell.
- **Scale:** rendezvous costs O(N) per lookup (ADR-0004). Splitting 10 000 nodes into 100 cells of 100 nodes turns one 10 000-way scan into a 100-way scan plus another 100-way scan.
- **Multi-region:** some data needs one replica in each of several regions.

The suggestion was to let "a node be another ring", for example `Ring<Cell<Ring<Node>>>`, so that nesting comes almost for free.

Four facts shape the design:

1. **Nesting partitions; topology labels spread.** If every level picked a single child, every replica of a key would land in one leaf: one region, one zone, one rack. Replicas have to *spread* across zones and hosts, which zone- and host-aware selection inside a ring already does (ADR-0006). Hierarchy is the right tool for *partitioning* (which region or cell owns a key) and the wrong tool for spreading. Region → cell are layers. Zone → host are labels inside the cell's ring.
2. **Layers must hash independently.** If a cell's ring hashes keys with the same seed as the layer above it, the keys that reach the cell are correlated with their positions in the cell's ring. With a vnode ring, a cell's keys all come from the arcs the cell won one layer up, so the cell's ring sees a skewed slice of the circle and balances badly. Each layer needs its own hash.
3. **Deriving a cell's weight from its contents moves data across cells.** If a cell's weight were the sum of its nodes' weights, adding one node would shift some keys *into* that cell from every other cell. That is a cross-cell migration triggered by an in-cell change, and it is the churn problem ADR-0004 cites against hierarchical rendezvous.
4. **A ring member is a machine.** It has an identity, a weight, a zone, and a host. A cell has an identity and a capacity share, but no zone or host. Making `Ring` generic over members would add a type parameter to every signature. It would also blur what the spec means by "node" and force placeholder labels onto cells (ADR-0011 rejected `Ring<N>` for the same reasons).

## Decision

1. **Compose; don't generalize `Ring`.**
   - A new type, `Cells<P = Ring>`, holds a *cell selector* plus a cell of type `P` for each `CellId`.
   - `P` is any type that implements a small `Placement` trait, which both `Ring<H>` and `Cells<P>` implement. Nesting is therefore free: `Cells<Ring>` is cell → nodes, and `Cells<Cells<Ring>>` is region → cell → nodes.
   - `Ring` stays exactly as it is.

   ```rust
   let eu = Ring::builder().seed(1).nodes(eu_nodes).build()?;
   let us = Ring::builder().seed(2).nodes(us_nodes).build()?;
   let fleet = Cells::builder()
       .directory(Directory::new().prefix("eu:", "eu".parse()?))   // residency rules first
       .ring_selector(Ring::builder().seed(7).nodes(cell_members).build()?) // then hash
       .cell("eu".parse()?, eu)
       .cell("us".parse()?, us)
       .build()?;
   let placed = fleet.replicas("eu:tenant:acme");   // replicas inside the "eu" cell
   assert_eq!(placed.cell().as_str(), "eu");
   ```

2. **The selector is a directory, a ring, or both.**
   - The directory is a set of explicit rules matched by longest prefix or exact key, for residency, pinning, and migrations in flight.
   - The ring is a `Ring` whose nodes are the cells, with **explicit** weights for capacity shares.
   - Directory rules win, and unmatched keys fall through to the ring.
   - Both are pure data. They are serializable and covered by the fingerprint.

3. **Cell weights are explicit.** A cell's share is the weight of its entry in the selector ring, never the sum of its nodes. Adding, removing, or reweighting a node inside a cell moves keys only within that cell. Growing a cell's share is a deliberate cross-cell migration, which `Cells::diff` reports separately.

4. **Every layer hashes independently.**
   - `build()` rejects a cell ring whose seed equals its selector ring's seed.
   - The docs recommend deriving seeds, for example from the cell ID.
   - Distinct seeds make each layer's key hash independent of the layer above, so vnode balance holds inside every cell.

5. **Two placement modes.**
   - **Within** (the default): all of a key's replicas live in one cell. The cell's ring spreads them across its zones and hosts. This mode covers residency and blast-radius isolation.
   - **Across** (later): the selector's top R cells each contribute their preferred primary, giving one replica per region. It is opt-in, with an explicit replication factor.

6. **Results carry their path.** `CellPlacement` holds the cell path (for example `["eu", "eu-cell-3"]`) and the leaf ring's `ReplicaSet`. That is enough to route the request and to explain the decision.

7. **The spec, fingerprints, and diffs extend naturally.**
   - **Spec:** composition becomes a new section of placement spec v1. The section is additive, and it lands before the 0.1.0 freeze.
   - **Fingerprint:** a cell layer's fingerprint is XXH3 over the selector (the directory rules plus the ring fingerprint), the mode, and every cell's ID, weight, and fingerprint, in ID order.
   - **Diff:** `Cells::diff` separates cross-cell moves from in-cell moves.

## Alternatives considered

- **`Ring<M>` with nodes that can be rings**, the original suggestion: one recursive type, and nesting really is free in code. But it puts machines and cells into one member type, which forces placeholder zone and host labels onto cells. It also makes replica selection over mixed members ill-defined, and it adds a type parameter to every API. It can't express directories, explicit cell weights, independent seeds, or the Within/Across modes without bloating `Ring`.
- **Picking one child per level all the way down (region → zone → rack).** That concentrates every replica in one rack. Zones and hosts belong inside a ring as labels (ADR-0006).
- **Hierarchical rendezvous with cell weights derived from contents** (ADR-0004's backlog item). Every node change would cause cross-cell churn. Explicit weights fix this.
- **Leaving composition to callers.** Each caller would rediscover the seed-correlation trap and the weight trap, and nothing would fingerprint the result.

## Consequences

- One new public area: `Cells`, `CellId`, `Directory`, `CellPlacement`, the `Placement` trait, and their errors. `Ring` doesn't change.
- The backlog items "placement pools and residency" and "hierarchical rendezvous" are resolved by design.
- Two-level lookups cost roughly a selector lookup plus a cell lookup: about 2–3 µs for 100 cells of 100 nodes, against about 100 µs for one flat 10 000-node rendezvous ring. FR-020 measures it.
- Release scope (confirmed 2026-10-28): 0.1.0 ships cells in Within mode only. Across mode waits for multi-region work in v0.2.
