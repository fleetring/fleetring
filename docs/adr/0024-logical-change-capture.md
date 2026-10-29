# ADR-0024: Logical change capture through a replicated change table

- **Status:** Accepted (2026-10-29)
- **Date:** 2026-10-29
- **Related:** ADR-0016, ADR-0021, ADR-0022; tickets FR-039, FR-040, FR-041

## Context

A logical change stream (FR-039) hands sinks the rows each committed transaction inserted, updated, or deleted. Replication ships SQLite's WAL frames (ADR-0016), which are pages, not rows, so something has to turn a transaction into row changes. Three constraints shape the choice:

1. **A promoted follower must continue the stream.** A follower receives frames and never executes SQL (ADR-0016, point 6). Whatever the primary records about its rows has to be on the follower too when it is promoted (ADR-0022), or the stream stops at every failover.
2. **The daemon builds without a C toolchain beyond the bundled SQLite.** `rusqlite`'s `preupdate_hook` and `session` features need `bindgen` and `clang-sys` at build time.
3. **Nothing above the engine adapter learns SQLite** (ADR-0016, point 2). The capture mechanism is the SQLite adapter's business; the rest of the daemon sees engine-neutral `RowChange`s behind `LogicalChanges`.

Other systems that capture row changes from SQLite-shaped engines have converged on one design: write the changes into a table inside the database, where they commit and replicate with the data.

- **Turso** ([CDC docs](https://docs.turso.tech/tursodb/cdc)): `PRAGMA capture_data_changes_conn('<mode>[,table]')` records into `turso_cdc`, stable since Turso 0.5.0. Modes are `id`, `before`, `after`, and `full`. The table's columns are `change_id`, `change_time`, `change_txn_id`, `change_type` (1 insert, 0 update, −1 delete, 2 commit), `table_name`, `id`, `before`, `after`, and `updates`.
- **Marmot** ([github.com/maxpert/marmot](https://github.com/maxpert/marmot)): triggers per table write into per-table change-log tables, which its replication reads and publishes.
- **cr-sqlite** ([github.com/vlcn-io/cr-sqlite](https://github.com/vlcn-io/cr-sqlite)): metadata tables beside each replicated table, read through the `crsql_changes` virtual table.
- **rqlite 9** ([CDC docs](https://rqlite.io/docs/guides/cdc/)): in-process capture, a per-node disk FIFO, delivery from the leader only, at-least-once, and a replicated high-water mark so a new leader resumes without duplicates.

## Decision

1. **Capture through temporary triggers into a change table inside the database.** The SQLite adapter's write connection carries one `TEMP` trigger per user table and operation (`AFTER INSERT`, `AFTER UPDATE`, `AFTER DELETE`), rebuilt whenever `PRAGMA schema_version` changes. Each trigger writes the row it saw into `_fleetring_cdc` in the same database. The rows commit with the transaction, replicate with its frames, and survive restarts; a promoted follower decodes a batch from its own copy. The triggers live in the temporary schema, so they never persist or replicate.
2. **The table is Turso-shaped**, so that one decoder reads either `_fleetring_cdc` or `turso_cdc`: `change_id INTEGER PRIMARY KEY`, `change_txn_id`, `change_type` (Turso's codes), `table_name`, `id`, `before`, `after`, `updates`. One commit record (`change_type = 2`) follows the rows of every transaction that recorded any. The deltas from Turso: no `change_time`, because the adapter reads no clock; `id` holds the row's key as JSON, not the bare rowid, so composite and `WITHOUT ROWID` keys fit; `before`, `after`, and `updates` are JSON (one flat array of `name, type, value` triples, blobs as hex, and a name array for `updates`) rather than Turso's binary records; and a `start` column holds the log position the transaction started from, which keys the range reads. The table carries no index: `change_id`s are contiguous and `start` never decreases along them, so a bound is a binary search of point lookups and a captured transaction costs one extra page in the log. The SQLite adapter's `changes` module documents the table column by column.
3. **Capture modes per database.** `id` records the key alone; `after` (the default) adds the new values of inserted and updated rows; `full` adds the old values of updated and deleted rows and the names of the updated columns. A sink asks for a mode (`capture = "id" | "after" | "full"`), and a database named by several sinks records in the strongest mode asked for. The mode travels through `LogicalChanges::start_recording(mode)`, so a Turso adapter maps it onto its pragma (FR-040).
4. **The table is reserved.** Names beginning with `_fleetring_` are the adapter's. A user statement, through `db exec` or `db query`, that reads, writes, creates, alters, or drops anything so named is refused as a statement error, through SQLite's authorizer on both connections; a trigger or a view whose body names one is refused when defined. The table still shows in `sqlite_master`, which the daemon doesn't filter; it is documented as reserved.
5. **The write connection runs with `recursive_triggers` on**, so the rows an `INSERT OR REPLACE` replaces are recorded as deletes.
6. **Rows are pruned once every logical sink has delivered them.** `LogicalChanges::release` records the lowest delivered position, and the next write transaction deletes the rows of transactions that started before it, inside that transaction, so the deletion replicates like any write. The table exists only in databases some logical sink names; a database no sink names never gets it.

## Alternatives considered

- **SQLite's session extension.** It diffs a connection's changes against the schema and produces a changeset. It is per connection: a follower that only receives frames has nothing to diff, so a promoted primary couldn't decode batches committed before its promotion. It also skips tables without a declared primary key, and `rusqlite`'s `session` feature needs `bindgen` and `clang` at build time.
- **The pre-update hook.** Same per-connection limit: the hook sees only the connection's own writes, so nothing survives a failover. Same build-time cost through `rusqlite`'s `preupdate_hook` feature.
- **Decoding rows from WAL frames.** The frames are there on every replica, so this would need no table. But it needs a page-to-table map maintained across schema changes, pages freed by a delete often never reach the WAL (the freelist trunk does), and rows move between pages on splits and vacuums, so a diff of two page images doesn't yield row operations reliably. One solo project attempts it; nobody established ships it.
- **A per-node change log outside the database** (rqlite's FIFO). Simpler to prune, but it lives on the primary alone: a new primary starts without the old one's backlog. rqlite accepts that and resumes from a high-water mark; FleetRing's followers hold the database anyway, so the backlog may as well be in it.

## Consequences

- **Write amplification by mode.** Every captured row costs one more row write in the same transaction: a key in `id`, a key and a JSON row in `after`, two JSON rows and a name array for updates in `full`. The commit record adds one row per transaction. `docs/perf.md` records the measured cost; `id` is the mode for a sink that only needs to know which rows to re-read.
- **A visible reserved table.** `_fleetring_cdc` shows in `sqlite_master`, and `ALTER TABLE` on user tables is still free. Tooling that lists tables should skip the `_fleetring_` prefix.
- **Not captured:** schema changes, including `ALTER TABLE … RENAME` (the triggers follow the schema version, so later rows of a renamed table carry its new name, but the rename itself produces no record); SQLite's own tables; and writes through any connection but the daemon's, which there are none of while the daemon holds the database.
- **A Turso adapter** (FR-040) reads `turso_cdc` through the same `LogicalChanges` capability and maps the mode to `capture_data_changes_conn`; the decoder above the adapter sees the same `RowChange`s.
- **Sink positions** stay per node for now (FR-039). Carrying them in control state or in the database, after rqlite's high-water mark, so that a new primary resumes without duplicates, is FR-041.
