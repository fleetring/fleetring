# ADR-0021: Migrations that change a database's engine

- **Status:** Accepted (2026-10-28). Planned now; built after live migration (MVP step 3).
- **Date:** 2026-10-28
- **Related:** ADR-0012, ADR-0016, ADR-0017, ADR-0018

## Context

A tenant may start on SQLite, in development and in production, and later need Postgres, for example through an `upgrade` command in a client tool. Live migration (MVP step 3) moves a database between nodes on the same engine, by snapshot and log catch-up. Changing engines can't copy pages or WAL frames, because the formats differ. The rest of the machinery (leases, epochs, fencing, cutover) is the same, so it should be designed in now, even though it's built later.

## Decision

1. **Every database records its engine** in control state (`sqlite`, and later `turso`, `postgres`, and so on). The router and failover read it.
2. **An engine change is a migration with a logical copy:**
   1. create the target database on the target engine's nodes, placed by the ring or a cells layer of nodes running that engine (ADR-0012);
   2. copy the schema and rows, translating types where the engines differ;
   3. catch up with changes made during the copy, through a change stream from the source;
   4. cut over under a new epoch (ADR-0018): fence the source, apply the last changes, switch the database's engine and lease in one control command, and redirect clients.
3. **Compatibility is checked before the copy starts.** Features that don't translate, such as some SQLite type affinities or Postgres-only types going the other way, fail fast with a report, not partway through.
4. **Reversible until cutover.** The source stays authoritative until the cutover command, so an aborted upgrade leaves the tenant where it was.

## Consequences

- v0.1 work: only the engine field in control state (FR-027).
- Each engine adapter (ADR-0016) needs a logical export or import and a change stream to take part in engine changes. That's in addition to its replication shape.
- The client-facing `upgrade` command is a thin layer over this migration.
