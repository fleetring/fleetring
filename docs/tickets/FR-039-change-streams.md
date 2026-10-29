---
id: FR-039
title: Change streams: a sink trait that sends each database's changes elsewhere
status: done
priority: P2
size: L
milestone: M8 Access and integration
depends_on: [FR-030]
adrs: [ADR-0016]
---

# FR-039: Change streams: a sink trait that sends each database's changes elsewhere

## Goal

Let an operator fan a database's committed changes out to other systems, such as Kafka, Google Pub/Sub, a webhook, an analytics store, or another FleetRing cluster, through a pluggable sink, without touching replication or placement.

## Scope

- **A `ChangeSink` trait** in `fleetringd`: `publish(database, change) -> Future<Result<Ack>>`, fed from the same committed-batch stream the replication primary uses, after the batch is acknowledged (so sinks never see unacknowledged writes). Sinks run beside replication, never in its acknowledgement path.
- **Two shapes of change:** physical (the opaque batch, for a mirror or an archive) and logical (row changes, decoded from SQLite's session extension or by diffing pages; decide in design). Logical is what Kafka consumers want.
- **Built-in sinks:** a file or directory sink (for tests and archives), and a webhook sink. Kafka and Pub/Sub as separate feature-gated crates or plugins, so the daemon's default build has no cloud SDKs.
- **Delivery semantics:** at-least-once with per-database positions, so a restarted sink resumes from its last acknowledged position. Positions are the engine-defined opaque positions (ADR-0016).
- **Configuration per database or per name prefix**, so `tenant:*` can stream to one topic and `agent:*` to another.
- **Not in scope:** exactly-once, transformations, and schema registries.

## Acceptance criteria

- [x] A database's writes arrive at a file sink in commit order, each exactly once after a clean run and at least once across a sink restart.
- [x] A slow or failing sink never delays a write's acknowledgement.
- [x] A webhook sink delivers logical row changes with the database name, position, and epoch.
- [x] Logical decoding: inserts, updates, and deletes with keys and values; a transaction with several statements yields one change per row, in order.
- [x] Simulation: a file sink per node under faults and failover; each node's output for each database is an in-order, gapless sequence of acknowledged writes (duplicates allowed, no phantoms), checked by `sim::check::change_stream_consistent`.

## Progress

- 2026-10-29: Done.
  - **Trait and types (`fleetringd::changes`):** `ChangeSink { name, publish(Change) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send>> }`. A `Change` carries the database name, the epoch, the engine-defined `Position` the batch continues from and ends at, and a `ChangeKind`: `Physical(Payload)` or `Logical(Vec<RowChange>)`. `RowChange` is engine-neutral: table, `insert`/`update`/`delete`, key columns, and column `Value`s.
  - **Feeding, never in the acknowledgement path:** `ChangeStreams` starts one pump per sink and per database the node serves as primary (it watches `Replication::served`). A pump reads a `PrimaryFeed`: `Primary::quorum_held` is the highest position the write quorum holds, computed from the followers' acknowledged positions alone (the `needed`-th highest, capped at the committed end; the primary alone when `needed` is 0), so it doesn't depend on any writer still waiting. Whenever that position passes the pump's, the pump reads the log from its position up to it as one batch (`FrameSource::committed_between`, a new engine method; the SQLite adapter cuts the WAL batch at the quorum's position with `fleetring_wal::Batch::up_to`) and publishes it. Writes never touch a sink: the actor publishes to the replication sink as before, and the pump runs on its own task, so a sink that sleeps or errors delays nothing. Polling the log after acknowledgement was chosen over hooking the acknowledgement future because the future belongs to the writer and is dropped with a disconnected client, while the quorum's position is state the primary holds anyway.
  - **Delivery:** at least once. The pump records the delivered position in `<data_dir>/changes/<sink>/<database>` (written whole and renamed) after each publish, retries a failing sink with backoff (50 ms doubling to 5 s), and holds one batch at a time: a sink that falls behind is caught up from the log, not from memory. A lagging sink holds the primary's checkpoints back like a lagging follower, until `replication.lag_limit_ms`; after that the log may restart past it, and the pump restarts from the log's beginning and counts a reset in its status.
  - **Logical decoding (SQLite adapter only, behind `Database::logical_changes` and `DatabaseEngine::logical_support`):** the write connection carries one temporary trigger per user table and operation (`AFTER INSERT/UPDATE/DELETE`), rebuilt whenever `PRAGMA schema_version` changes, after each statement. Each trigger writes the row it saw, with its key and `OLD`/`NEW` values as JSON, into a side table `_fleetring_changes` inside the database, keyed by the log position the transaction started from, which the adapter records in a temporary table at `BEGIN`. Changes therefore commit, replicate, and survive restarts exactly as the user's rows do, and a new primary decodes them from its own copy. Decoding a batch is a range query on that table; delivered rows are deleted once every logical sink has passed them (`LogicalChanges::release`). Recording starts when a database that a logical sink names opens (`Registry::with_recorded_changes`). The pre-update hook and the session extension were rejected: both need `rusqlite`'s bindgen-at-build-time features, which pull in `bindgen` and `clang-sys`; the session extension also skips tables without a declared primary key. Not captured: schema changes, SQLite's own tables, and rows an `INSERT OR REPLACE` replaces (SQLite fires those triggers only with `recursive_triggers`); the side table is visible in `sqlite_master`.
  - **Built-in sinks:** `FileSink` (one append-only NDJSON file per database, synced per change) and `WebhookSink` (HTTP/1.1 POST of the JSON `ChangeRecord` through `hyper` directly, with exponential backoff on 5xx, timeouts, and transport failures, bounded by `retries`; 4xx is reported as rejected; both are retried by the pump). `build_sink` is the plugin point: a Kafka or Pub/Sub sink is a `ChangeSink`, a `SinkKind` variant, and a branch there, behind a feature.
  - **Configuration:** `[[changes.sink]]` entries with `name`, `kind = "file" | "webhook"`, `format = "logical" | "physical"`, `databases = ["tenant:*", "exact"]` (`DatabaseMatcher`: an exact name or a prefix ending in `*`), and `path`, or `url`, `timeout_ms`, `retries`. Validated with the rest of the configuration; documented in `deploy/fleetringd.toml`.
  - **Observability:** `NodeStatus` gains `sinks` (additive): each sink's per-database position, the quorum's position, backlog, failures, resets, and last error; `fleetringd status --node` prints them.
  - **Tests:** `tests/changes/` (file sink exactly once and at least once across a killed pump; a gated sink and a failing sink delay no acknowledgement, by causality rather than timing; logical decoding of inserts, updates, deletes, rowid and composite keys, rollbacks, and JSON round trips; a webhook test server that fails the first two requests; `[[changes.sink]]` parsing, validation, a daemon fed from its configuration, and `status --node`). `tests/sim/changes.rs` streams to a file sink on every node under a primary crash, an isolated primary, and flapping replicas; `Cluster::finish` checks every file sink with `check::change_stream_consistent`.
  - **Follow-ups:** Kafka and Pub/Sub sinks as feature-gated plugins; exactly-once (consumers dedupe by position today); replicated sink positions, so a new primary continues where the old one stopped instead of from its own log's beginning; self-replicating engines, which ship no batch, get no stream until their adapters provide logical changes.
