# ADR-0001: `fleetring` is a pure, deterministic placement library

- **Status:** Accepted (2026-10-27)
- **Date:** 2026-10-27
- **Related:** ADR-0006, ADR-0007, ADR-0008; tickets FR-007, FR-009, FR-012

## Context

FleetRing places millions of small SQLite/libSQL databases, one per agent, tenant, or device, on machines the operator chooses. The pitch is "Durable Objects, self-hosted" (`docs/mvp.md`). The MVP builds the ring crate first. After it come `fleetringd` (per-database primary, followers, leases), live migration, a router, and a Rust client that embeds the ring to save a network hop.

Several consumers will run the same placement code: the daemon, the router, the Rust client, a WASM playground, and later TypeScript and Python SDKs. They must agree on where every database lives. Placement must also be testable without machines, clocks, or networks.

Durable Objects guarantee one live instance per name. Hashing alone can't give that guarantee. During a membership change, two clients holding different ring versions compute different primaries. Single-writer safety has to come from leases and epochs, not from the hash function.

## Decision

1. **The library is pure.** `fleetring` performs no I/O. It reads no clocks, randomness, environment variables, or global state. It spawns no threads and needs no async runtime. Every input is an explicit argument, and the same inputs always produce the same outputs.
2. **What it answers.** Given a ring (membership plus configuration) and a key, it returns:
   - the **home placement**: an ordered replica set whose first node is the preferred primary (ADR-0006);
   - the full **preference list**, optionally filtered by a caller predicate such as liveness or load (ADR-0006, ADR-0007);
   - which keys move between two rings, for migration planning;
   - a serializable, versioned ring spec with a fingerprint (ADR-0008);
   - diagnostics about balance and topology.
3. **What it does not do.** It doesn't store placements, detect failures, hold leases, move data, open SQLite files, or speak network protocols. `fleetringd`, the router, and the client own those jobs.
4. **Home placement vs. actual placement.** The ring computes where a database *should* live. `fleetringd` records where it *does* live, in the control db's db→node map. The two differ during migrations and for databases placed by the bounded-load policy (ADR-0007). Clients compute home placement locally and follow a redirect when it's stale.
5. **Membership vs. liveness.** Membership (which nodes exist, plus their weights, zones, and hosts) determines placement, so changing it moves data. Liveness never changes placement. A sleeping laptop stays a member, and callers skip it at lookup time with a predicate. To drain a node, build a target ring without it; the daemon then migrates from the current ring to the target.
6. **Rings are immutable.** A `Ring` is an immutable value and is `Send + Sync`. A membership change builds a new ring with `ring.to_builder()…build()`. Each ring carries an opaque `Epoch`, assigned by the control plane, that tells peers which ring is newer. Epochs don't affect placement.
7. **One database, one key.** Keys are database names, treated as opaque bytes. Each key is placed independently. There is no slot or partition layer (ADR-0007 explains why).
8. **The API is library-grade.** The public API follows the Rust API Guidelines: private by default, `#[non_exhaustive]` on enums that may grow, typed errors, no panics on caller input, and docs on every public item.

## Alternatives considered

- **A placement service instead of a library.** Every lookup would cost a network hop, and the MVP's "client embeds the ring" design rules it out.
- **A library that also manages membership (gossip, heartbeats).** That would pull I/O, clocks, and failure detection into the crate. The MVP assigns membership to the control db and `fleetringd`.
- **Mutable rings with in-place `add_node`/`remove_node`.** This is faster for huge vnode rings, but in-place mutation invites torn reads and makes "which ring produced this answer?" impossible to answer. At fleet sizes a rebuild is cheap; FR-016 measures it.

## Consequences

- Placement can be tested exhaustively, with golden vectors and property tests, and embedded anywhere Rust compiles, including WASM.
- The daemon owns all state: the current and target rings, the db→node map, leases, and epochs.
- A client with a stale ring sends traffic to the wrong node. Recovery (a redirect plus a ring refresh) happens outside the library, and the epoch makes the staleness detectable.
