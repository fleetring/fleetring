---
id: FR-041
title: Replicated sink positions, so a new primary resumes a change stream without duplicates
status: todo
priority: P2
size: M
milestone: M8 Access and integration
depends_on: [FR-039]
adrs: [ADR-0017, ADR-0022, ADR-0024, ADR-0028]
---

# FR-041: Replicated sink positions, so a new primary resumes a change stream without duplicates

## Goal

A sink's delivered position lives on the primary alone today (`<data_dir>/changes/<sink>/<database>`). After a failover the new primary starts its pumps from its log's beginning, so a sink receives again everything recorded since the last checkpoint; consumers dedupe by position. Carry the position with the database instead, after rqlite's replicated high-water mark, so that a new primary continues where the old one stopped.

## Scope

- **Where the position lives: decide between two places**, and record the decision as an ADR:
  - **Control state.** A `SinkDelivered { database, sink, position }` command through `fleetring-control`'s pure `apply`, batched so that a busy stream doesn't flood the control log; the pump reads the position on start from the control view it already has.
  - **The database.** A `_fleetring_sinks` table beside `_fleetring_cdc` (ADR-0024), written by the primary after each delivery inside a small write transaction, so it replicates with the data; the new primary reads it from its own copy. This costs one replicated write per delivery, which could be folded into the next user transaction the way pruning is.

  *Decided by ADR-0028: the database, folded into the next write transaction after a delivery, through an engine-neutral `SinkMarks` capability; an engine without it keeps local positions and says so.*
- **Resuming.** On promotion, each pump starts from the recorded position if the new log still continues from it, and from the log's beginning (counting a reset, as today) if not.
- **Pruning.** `LogicalChanges::release` uses the replicated positions, so rows aren't dropped before a sink the old primary was feeding has them.
- **Not in scope:** exactly-once delivery. At-least-once stays; this ticket only removes the duplicates a failover causes.

## Acceptance criteria

- [ ] In `tests/sim/changes.rs`, a primary crash no longer makes the file sink on the new primary repeat records the old primary delivered; `check::change_stream_consistent` gains a duplicates bound of one batch per failover.
  - *Refined by ADR-0028 point 6:* a failover repeats only what the old primary delivered after the newest mark that reached the write quorum. The deterministic test `a_new_primary_resumes_from_the_mark_and_repeats_one_delivery` (step 4) proves the one-batch bound for a delivery followed by a write; the simulation, where the next write may already be running when a mark is staged, checks at most two repeated records per failover (`REPEATS_PER_FAILOVER = 2`).
- [ ] A sink's position outlives the node that recorded it: after the old primary's data directory is deleted, the new primary still resumes from the right place.
- [ ] `status --node` shows whether a sink's position is replicated or local.
- [ ] ADR-0028 is accepted and linked from `docs/adr/README.md`.
- [ ] A user statement that names `_fleetring_sinks` is refused like one naming `_fleetring_cdc`, and `rows_affected` never counts mark rows.
- [ ] The cost of a mark on the write path is measured in `docs/perf.md` beside the capture costs.

## Implementation plan

Each step is about one commit (`feat(fleetringd): …`, `Refs: FR-041`). Paths are under `crates/fleetringd/` unless they say otherwise. New public types derive `Debug` and the applicable traits and join `engine`'s or `changes`' `assert_debug_send_sync` list; errors are `#[non_exhaustive]` with lowercase messages. The control crate and the protocol's control and replication services don't change.

### Step 1: the `SinkMarks` capability and the SQLite marks table

- `src/engine/adapter.rs`:

  ```rust
  /// Delivered positions of change sinks, kept inside the database so that they replicate,
  /// fence, and survive with it (ADR-0028).
  ///
  /// A mark is staged after its sink has taken a batch, and recorded by the next write
  /// transaction, before the transaction's own statements; the engine never starts a
  /// transaction just to record one.
  pub trait SinkMarks: fmt::Debug + Send {
      /// Every recorded mark, by sink name, as this copy holds them.
      ///
      /// # Errors
      ///
      /// [`EngineError::Storage`] if the marks can't be read.
      fn marks(&mut self) -> Result<BTreeMap<String, Position>, EngineError>;

      /// Stages `delivered` as `sink`'s mark, replacing any mark staged for it before. The next
      /// write transaction records it; one that rolls back leaves it staged.
      fn stage(&mut self, sink: &str, delivered: &Position);
  }
  ```

  and on trait `Database`: `fn sink_marks(&mut self) -> Option<&mut dyn SinkMarks> { None }`. Re-export `SinkMarks` from `src/engine/mod.rs` and add `assert_debug_send::<dyn SinkMarks>()`.
- New `src/engine/sqlite/marks.rs` (declared in `engine/sqlite.rs`), documented column by column like `changes.rs`:

  ```rust
  pub(super) const TABLE: &str = "_fleetring_sinks";
  const CREATE: &str = "CREATE TABLE IF NOT EXISTS _fleetring_sinks (sink TEXT PRIMARY KEY, position BLOB NOT NULL) STRICT, WITHOUT ROWID";
  const UPSERT: &str = "INSERT INTO _fleetring_sinks (sink, position) VALUES (?1, ?2) ON CONFLICT (sink) DO UPDATE SET position = excluded.position";
  const READ: &str = "SELECT sink, position FROM _fleetring_sinks ORDER BY sink";

  #[derive(Debug, Default)]
  pub(super) struct Marks { staged: BTreeMap<String, Vec<u8>>, created: bool }
  impl Marks {
      pub(super) fn stage(&mut self, sink: &str, position: &[u8]);
      /// Inside an open write transaction: creates the table if needed and upserts every staged
      /// mark. Returns how many rows it wrote, which the transaction's `rows_affected` excludes.
      pub(super) fn flush(&mut self, writer: &Connection) -> Result<u64, EngineError>;
      /// After `COMMIT` succeeded: forgets the marks `flush` wrote.
      pub(super) fn committed(&mut self);
      /// Reads every mark; an absent table is no marks.
      pub(super) fn read(conn: &Connection) -> Result<BTreeMap<String, Vec<u8>>, EngineError>;
  }
  ```

  Statements run through the same internal path the recorder's do, so the authorizer lets them through (`engine/sqlite/changes.rs` is the model).
- `src/engine/sqlite.rs`, `SqliteDatabase`: a `marks: Marks` field. In `Database::execute`, right after `BEGIN IMMEDIATE` and **before** `recorder.begin` (which may prune), call `self.marks.flush(&self.writer)`; on error roll back and return it. Subtract its count from `rows_affected` as `recorded` is subtracted. After a successful `COMMIT`, call `self.marks.committed()`. `fn sink_marks(&mut self) -> Option<&mut dyn SinkMarks> { Some(self) }`, with `impl SinkMarks for SqliteDatabase` delegating to `Marks` (reading through the writer connection, converting bytes to `Position`).
- The authorizer already refuses user statements naming `_fleetring_*` (ADR-0024 point 4); add the table to the module docs' list of reserved tables.
- Tests in `tests/changes/logical.rs`:
  - `a_staged_mark_rides_on_the_next_write`: stage `p` for sink `s`; `marks()` is empty; one write; `marks()` is `{s: p}`; a second stage and write replaces it.
  - `a_mark_staged_with_no_later_write_is_not_recorded`: stage, no write, close, reopen: no marks.
  - `a_rolled_back_write_keeps_the_mark_staged`: stage, a failing statement, then a good write: the mark is recorded.
  - `marks_never_count_in_rows_affected`: a one-row insert with a staged mark reports `rows_affected = 1`.
  - `user_statements_naming_the_marks_table_are_refused`: `SELECT`, `INSERT`, `DELETE`, and `DROP` on `_fleetring_sinks` fail as statement errors on both connections.
  - `marks_replicate_to_a_follower`: with the replication harness of `tests/changes/common.rs` and R = 2, a recorded mark is in the follower's copy once it is promoted (`Replication::serve_primary` at epoch 2 on the follower, then `sink_marks().marks()` through its handle).

### Step 2: actor requests

- `src/actor.rs`: `Request::SinkMarks(Reply<Option<BTreeMap<String, Position>>>)` and `Request::StageSinkMark(String, Position, Reply<MarkHome>)`, served on the database's thread: the first returns `None` when `Database::sink_marks()` is `None`; the second stages and returns `MarkHome::Replicated`, or returns `MarkHome::Local` without staging when the engine has no marks.

  ```rust
  /// Where a sink's delivered position is kept (ADR-0028).
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  #[non_exhaustive]
  pub enum MarkHome {
      /// Inside the database, replicated with it.
      Replicated,
      /// In a file on this node only, because the engine can't keep marks.
      Local,
  }

  impl DatabaseHandle {
      pub(crate) async fn sink_marks(&self) -> Result<Option<BTreeMap<String, Position>>, DatabaseError>;
      pub(crate) async fn stage_sink_mark(&self, sink: String, delivered: Position) -> Result<MarkHome, DatabaseError>;
  }
  ```

  `MarkHome` lives in `src/changes/mod.rs` and is re-exported there.

### Step 3: pumps start from and stage marks

- `src/changes/pump.rs`:
  - `PumpState` gains `home: Option<MarkHome>` (`None` until the pump opened its database).
  - At the start of `run`, before the first `primary_feed`, open the database through `ctx.replication.registry().open(&database)` and call `handle.sink_marks()`:
    - `Some(marks)`: `home = Replicated`; `position = marks.get(&sink).cloned()`; the local `PositionStore` file is neither read nor written for this pump from now on;
    - `None`: `home = Local`; `position = ctx.store.load()` as today.
    - An error opening or reading: log at `warn` and fall back to `Local` as today.
  - After `Delivery::Delivered`: when `home == Replicated`, call `handle.stage_sink_mark(sink.clone(), until.clone())` (log at `debug` and go on if it fails: the next delivery stages a newer mark); otherwise `ctx.store.save(&until)` as today. Stage **before** `streams.release(&database)`, so that the adapter never prunes past a mark it hasn't staged.
  - `Delivery::LogMoved` keeps its behavior (restart from `None`, count a reset); nothing is staged for `None`.
- `src/changes/mod.rs`: `SinkStatus` gains `pub home: Option<MarkHome>`, filled from the pump state in `ChangeStreams::status`.
- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`, `ChangeSinkStatus`: `// Set when the sink's position is kept in the database and replicates with it (FR-041); unset when it is kept on this node only, or by a node that predates the field.` `bool replicated = 9;`
- `src/data/convert.rs` and `src/data/client.rs` (`NodeStatus`'s sink entries gain `pub replicated: bool` data, not a parameter); `src/main.rs`'s `status --node` prints `position replicated` or `position local` on each sink line.
- `src/changes/mod.rs` module docs: the *Feeding* paragraph describes marks and the local fallback.
- Tests in `tests/changes/file.rs`:
  - `a_pump_restarted_on_the_same_node_resumes_from_the_mark`: deliver, write once (records the mark), kill the pump, restart it: the first new change's `start` is the mark.
  - `status_reports_where_each_sinks_position_lives`: a SQLite database reports `home = Some(Replicated)`; an engine without marks (below) reports `Some(Local)`, and `fleetringd status --node` prints both words (`tests/changes/daemon.rs`).
  - `an_engine_without_marks_keeps_positions_on_the_node`: wrap `SqliteEngine` in a test-only `NoMarks` engine in `tests/changes/common.rs` whose `Database` delegates every method except `sink_marks` (which stays `None`); the position file under `<data_dir>/changes/<sink>/` is written as before.

### Step 4: failover tests

- New `tests/changes/failover.rs` (`mod failover;` in `tests/changes/main.rs`), using two replication nodes from `tests/changes/common.rs` with R = 2, a logical `MemorySink` named `stream` on each, and `Replication::serve_primary` to move the primary by hand (epoch 1 on `a`, then epoch 2 on `b` after `a` is killed):
  - `a_new_primary_resumes_from_the_mark_and_repeats_one_delivery`: on `a`, write `w1`, wait until `stream` has it (delivery D1, end `p1`), write `w2` (which records mark `p1`), wait until `stream` has it (D2, end `p2`), wait until both are acknowledged by `b`, kill `a`. Serve on `b`: the first change `b`'s sink receives starts at `p1` and holds `w2` alone. Exactly one delivery (D2) is repeated.
  - `a_mark_outlives_the_data_directory_that_wrote_it`: as above, then delete `a`'s data directory before `b` serves; `b` still starts at `p1`.
  - `a_mark_behind_a_restarted_log_restarts_from_the_beginning`: with the replication settings' `lag_limit` at 0 and a checkpoint forced on `b` after promotion (`DatabaseHandle::checkpoint` until the log restarts), a pump whose mark is in the old generation restarts from `None` and its status counts one reset.
  - `pruning_waits_for_the_marks_of_every_logical_sink`: two logical sinks on `a`, one of them stalled with a gate (as `tests/changes/isolation.rs` gates sinks); after many writes and a promotion to `b`, `b`'s copy still holds the `_fleetring_cdc` rows after the stalled sink's mark, and the stalled sink, released on `b`, receives them.

### Step 5: the simulation checks the resume

- `src/sim/check.rs`:
  - `pub const REPEATS_PER_FAILOVER: usize = 2;` with a doc comment citing ADR-0028 point 6.
  - `change_stream_consistent` (per host) changes one rule: a record whose `start` doesn't continue the host's last delivered position, and isn't `None`, is accepted when it starts at a position that ends some record of **any** host at an **earlier** epoch (a replicated mark), instead of always being a gap. Its signature gains `marks: &BTreeSet<(u64, Position)>`, the `(epoch, end)` of every record across hosts, which `Cluster::check_change_streams` collects from all files first.
  - New `Invariant::ChangeStreamResumes` ("change stream resumes at a replicated mark") and

    ```rust
    /// Across every host's stream of `database`, ordered by epoch: the first record of each epoch
    /// after the first starts at a position some earlier epoch delivered up to, or from the log's
    /// beginning only when no earlier epoch delivered anything; and the records of earlier epochs
    /// that end after that start, the repeats, number at most `repeats_per_failover`.
    pub fn change_streams_resume(database: &DatabaseName, streams: &[(String, Vec<ChangeRecord>)], repeats_per_failover: usize) -> Result<(), Violation>;
    ```

  - `Cluster::check_change_streams` runs it with `REPEATS_PER_FAILOVER` after the per-host checks; `Cluster::finish` already calls it.
- `tests/sim/changes.rs`: after `check_change_streams`, assert that at least one epoch after the first started at `Some(position)` (the resume happened), and update the module docs (the new primary's sink starts at the mark, not at its own position).
- Unit tests in `check.rs`: `a_stream_resumed_at_a_mark_passes`, `a_resume_past_every_delivered_position_is_a_gap`, `more_repeats_than_the_bound_fail`, `a_first_record_from_the_beginning_after_deliveries_fails`.

### Step 6: measurement and documentation

- `benches/capture.rs`: a `marks` group, the one-row and 100-row writes with a mark staged before each (logical capture off, then `after`); numbers into `docs/perf.md` under "Change capture", with one sentence on what a mark costs.
- `deploy/fleetringd.toml` (`[[changes.sink]]` comment): positions are kept inside the database and survive failover; the configuration must be the same on every node; `_fleetring_sinks` joins `_fleetring_cdc` in the reserved-table sentence.
- `crates/fleetringd/README.md`: the change-streams section says a new primary resumes from the replicated mark and what a failover can repeat.
- `docs/adr/0024-logical-change-capture.md`: append to the last consequence "(*Settled by ADR-0028:* positions are marks inside the database.)".
- ADR-0028's status to Accepted with the date, its index row, and this ticket's Progress.

## Open risks

- **Sink configuration drift.** A sink configured on the old primary and not on the new one isn't fed, and its rows are pruned under the sinks that are (ADR-0028 point 7). Nothing checks configurations across nodes yet; a later ticket could report sinks whose marks exist in a database but which no pump feeds.
- **One more page per write after a delivery.** Measured in step 6; if it matters for a workload, a later change can stage marks every N deliveries, trading a larger repeat bound for fewer pages.
- **Self-replicating engines** keep local positions until their adapters implement `SinkMarks` (FR-040 for Turso).

## Progress

- 2026-11-02: ADR-0028 proposed; implementation plan written. Waiting for the ADR's acceptance.
