# ADR-0013: Async boundaries: synchronous placement, async I/O, engine-owned concurrency

- **Status:** Accepted (2026-10-28)
- **Date:** 2026-10-28
- **Related:** ADR-0001, ADR-0006, ADR-0010, ADR-0011

## Context

FleetRing mixes three very different kinds of work.

1. **Placement** is CPU-bound arithmetic that takes a few microseconds per lookup and does no I/O (the `fleetring` crate).
2. **Network and disk I/O** covers peer RPC, heartbeats and leases, WAL streaming, migrations, S3 snapshots, and the Hrana/HTTP router. It spends most of its time waiting.
3. **Database execution** happens inside the storage engine.
   - SQLite and libSQL expose blocking APIs.
   - Turso's new engine adds async I/O and MVCC concurrent writes, but that concurrency belongs to the engine, at the SQLite layer.

If these kinds of work are mixed carelessly, blocking calls stall the async executor, library crates impose a runtime on their users, and engine-level concurrency gets confused with cluster-level ownership.

## Decision

1. **Placement stays synchronous and runtime-agnostic.**
   - `fleetring` has no async API and never will (ADR-0001). Its types are `Send + Sync`, take no locks, and never block, so async code calls `ring.replicas(key)` directly.
   - Only batch work (diffing millions of keys, building rings with millions of tokens, re-placement sweeps) belongs on `spawn_blocking` or a worker thread.
2. **Ring updates are lock-free swaps.** Services hold `Arc<Ring>` behind `ArcSwap` or a `watch` channel. A membership change builds a new immutable ring off the hot path and publishes it. Readers never wait.
3. **Async lives at the I/O boundary.**
   - `fleetringd`, the router, and the client are async on Tokio, the default runtime for libSQL clients and HTTP stacks.
   - Product library crates accept handles or executors and don't spawn runtimes of their own.
4. **The storage engine sits behind an async adapter in `fleetringd`.**
   - **SQLite/libSQL:** blocking calls run on one dedicated thread per open database, or on a bounded blocking pool, never on executor threads.
   - **Turso's async engine:** used natively once it's production-ready. Its MVCC orders transactions *within* a node.
5. **Cluster ownership and engine concurrency are separate layers.** The per-database lease (ADR-0006, `docs/mvp.md`) decides *which node* may write a database. The engine's MVCC (or SQLite's single writer) decides how transactions interleave *on that node*. Neither replaces the other.
6. **Each open database is an actor.** A task with a mailbox owns the database's connections, serializes writes, and streams its WAL to followers. This matches the single-threaded model of Durable Objects and makes migration a hand-off between actors.

## Alternatives considered

- **An async `fleetring` API.** It would add overhead to microsecond lookups and force a runtime on users, and placement involves no waiting.
- **Running blocking SQLite calls on executor threads.** One slow query stalls unrelated requests.
- **Relying on engine MVCC instead of leases.** MVCC only coordinates transactions inside one process, so two nodes would still both accept writes for the same database.

## Consequences

- The placement crate needs no changes; this ADR protects that boundary.
- `fleetringd` gets a `DatabaseEngine` adapter, so switching from SQLite to Turso's async engine doesn't touch placement or replication.
- Deterministic simulation testing of the daemon (`docs/mvp.md`) needs a simulatable Tokio layer, such as turmoil or madsim. That choice is made when daemon work starts.
