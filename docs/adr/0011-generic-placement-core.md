# ADR-0011: `fleetring` is general-purpose placement infrastructure; FleetRing the product stays SQLite-focused

- **Status:** Accepted (2026-10-27)
- **Date:** 2026-10-27
- **Amends:** ADR-0001, point 7 ("Keys are database names")
- **Related:** ADR-0001, ADR-0002, ADR-0004, ADR-0006, ADR-0008, ADR-0010; ticket FR-018

## Context

The `fleetring` crate was written for one job: deciding which machines hold each of millions of SQLite databases. But nothing in its design is about SQLite:
- keys are opaque bytes;
- nodes are validated labels with weights and topology;
- the hasher is pluggable;
- the crate performs no I/O and has no storage dependencies (ADR-0001).

Only the documentation and the package metadata say "SQLite".

The problem the crate solves is everywhere. "Given a key, which nodes, in what order, and what moves when the fleet changes?" comes up in:

| Domain | What a key is | What the crate gives it |
|---|---|---|
| Database-per-tenant fleets (FleetRing) | a database name | primary + followers, zone-aware, minimal migration |
| Key-value stores and caches (Redis-style sharding) | a key or a key's hash slot | owner nodes, bounded hot spots, cheap resharding |
| Chat rooms, actors, Durable Objects-style routing | a room or actor ID | one home node, deterministic failover |
| Multiplayer games | a match or world ID | session affinity that survives node churn |
| Queues and streams | a partition or consumer group | balanced assignment, minimal rebalancing |
| Multi-tenant SaaS | a tenant ID | residency within zones, predictable placement |
| LLM inference | a conversation or prefix | affinity to the GPU that holds its KV cache, bounded load |

Rust consistent-hashing crates generally offer a hash ring or a rendezvous function and stop there. Few combine all of the following:
- exact weights;
- replica sets spread across zones and hosts;
- bounded loads;
- placement that is bit-identical across platforms and languages, pinned by a spec;
- fingerprints that detect divergent peers;
- migration planning.

That combination is what this crate already is. Presenting it as SQLite plumbing would hide it from most of the people who need it.

## Decision

1. **The crate is general-purpose, and the product isn't.** The `fleetring` crate is presented as *deterministic, cross-language placement with zone-aware replicas*. FleetRing, the product (daemon, router, migration, backups), stays focused on SQLite, libSQL, and Turso fleets. FleetRing is the flagship user of the crate, not its definition.

2. **The vocabulary is generic.** Public docs, item names, and error messages speak of **keys**, **nodes**, **replica sets**, **preference lists**, and **home placement**, never of databases or SQLite.
   - A key identifies any unit that must live somewhere: a database, room, tenant, actor, shard, or session.
   - Examples span several domains, with FleetRing's database-per-tenant fleet as one of them.
   - ADR-0001 point 7 now reads: *keys identify placeable units and are handled as opaque bytes; in FleetRing they are database names.*

3. **The semantics are generic, with no new types.**
   - `replicas()` returns an ordered set whose first member is the **preferred primary**. Each kind of system reads it in its own way:
     - single-leader systems use it as primary plus followers;
     - leaderless systems use it as a quorum set;
     - caches use R = 1.
   - Rings carry identities, not endpoints or payloads. Callers map `NodeId`s to addresses themselves, which keeps `Ring` free of a second type parameter (ADR-0005).
   - Bounded loads accept any load unit (ADR-0007), and filters accept any predicate (ADR-0006). Both already fit every domain above.

4. **The dependency arrow points one way.** The crate MUST NOT depend on SQLite, libSQL, a storage engine, a network stack, or an async runtime. Product crates depend on `fleetring`, never the reverse. Product concepts stay in product crates: WAL shipping, leases, migration, and page checksums (ADR-0010).

5. **The name stays.** The general-purpose core keeps the name `fleetring`, so the brand rides on the most widely reusable piece. Product crates take scoped names: `fleetringd`, `fleetring-router`, `fleetring-client`.

6. **The spec is the contract for other ecosystems.** The placement spec and its golden vectors (ADR-0008) let Go, TypeScript, Python, or Java services compute the same placements as Rust services. A general-purpose audience makes this the crate's main differentiator. Bindings and ports move up the backlog.

7. **Metadata matches the positioning.**
   - The crate description, keywords (`consistent-hashing`, `rendezvous-hashing`, `placement`, `sharding`, `distributed`), categories, README, and crate docs all present the general-purpose use cases.
   - The repository README stays product-first and points to the crate for general use.

## Alternatives considered

- **Keep the crate SQLite-branded.** That narrows the audience for no technical reason, and it misleads readers: the crate contains nothing SQLite-specific.
- **Publish the core under a separate, neutral name** (for example `placement` or `ringcore`). That splits attention between two names and adds a crate boundary with no technical benefit.
- **Make rings generic over a node payload (`Ring<N>`).** Callers could attach addresses or metadata, but every signature would gain a type parameter, and the spec could no longer say what a node is. A map from `NodeId` to anything does the same job.

## Consequences

- Documentation and metadata are rewritten, and no type or behavior changes. FR-018 covers examples across several domains.
- External users raise the stakes on stability. The semver discipline (ADR-0009) and the frozen placement spec (ADR-0008) become commitments to people outside the project.
- WASM, TypeScript, and Python bindings, and conformance kits built from the vectors, matter more. They are now backlog priorities.
- Product documents (`docs/mvp.md`, the repository README) remain SQLite-focused, and that's intentional.
