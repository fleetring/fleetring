---
id: FR-066
title: The router: a libSQL/Hrana front door that routes each request to its database's primary
status: todo
priority: P1
size: L
milestone: M8 Access and integration
depends_on: [FR-042, FR-046, FR-047, FR-050]
adrs: [ADR-0019, ADR-0025, ADR-0026, ADR-0031, ADR-0037]
---

# FR-066: The router: a libSQL/Hrana front door that routes each request to its database's primary

Issue: [#69](https://github.com/fleetring/fleetring/issues/69). FR-061 ([#63](https://github.com/fleetring/fleetring/issues/63), the Rails adapter) is blocked by this ticket.

## Context

Applications can't use FleetRing yet without speaking its gRPC `Database` service, which ADR-0019 marks as v0.1-only. The router is MVP step 4 and roadmap E7: a front door that speaks libSQL's Hrana protocol, so existing libSQL and Turso clients reach the right primary with no code change. It is where client requests are authenticated: ADR-0026 already lets a principal with `forward_tokens = true` carry its callers' tokens to data nodes. ADR-0037 decides the protocol surface, how a request names its database, how placement is cached and redirects followed, how interactive transactions hold a primary's writer, and how the router is deployed.

FR-061 (#63, the Rails adapter) depends on this ticket.

## Scope

- **`fleetringd router --config <file>`:** a long-running process with its own client identity (a principal with `forward_tokens = true`), serving Hrana over HTTP with JSON: `GET /v2` and `/v3`, `POST /v2/pipeline`, `/v3/pipeline`, and `/v3/cursor`, on HTTPS (or plaintext on loopback).
- **Naming the database:** the path form `/db/<percent-encoded name>/…`, and an optional host form, `<name>.<host_suffix>`, for names that are DNS labels.
- **Hrana v1 surface:** `execute`, `batch` with conditions, `sequence`, `describe`, `store_sql`, `close_sql`, `get_autocommit`, `close`, batons with `base_url`, and interactive transactions. Out: WebSockets, the protobuf encoding, replica reads.
- **Data plane:** a new `Database.Session` bidirectional RPC that runs statements on the primary's writer with SQLite's own transaction semantics, returns per-statement results (columns, rows, affected rows, last insert row ID), and acknowledges each commit only once its write quorum holds it. A session without `write` on the database is read-only, which the engine enforces (amends ADR-0026 point 4).
- **Auth:** a FleetRing token in `Authorization: Bearer`; the router verifies it and checks `read` on the database before any network hop, then forwards it; data nodes enforce it again with the denylist.
- **Routing:** placement cached per database from `Locate`, updated by `fleetring-primary` redirects; a request is retried at the new primary only if it never ran. One HTTP/2 connection per data node, shared by every session.
- **Bounds:** stream idle timeout, transaction lifetime, and caps on open streams and waiting transactions.
- **Simulation:** the router as a turmoil host, with a history-checked scenario across failovers.
- **Not in scope:** follower reads, the embedded Rust client (E7), WebSocket Hrana, and engines other than SQLite.

## Acceptance criteria

- [ ] ADR-0037 is accepted and listed in `docs/adr/README.md`, and ADR-0026's index row notes the amendment.
- [ ] Unchanged client code runs `execute`, `batch` (with a rollback on error), a multi-statement script, and interactive transactions that commit and that roll back through the router against a three-node fleet: in CI with the `libsql` Rust crate's remote client, and in the nightly job with a Node script on `@libsql/client`.
- [ ] A request without a token gets `401`; a token without `read` on the database gets `403` before any node is called; a read-only token runs a `SELECT` and gets an error naming `write` for an `INSERT`; and a data node refuses the same `INSERT` with that token when called directly.
- [ ] After a lease transfer (a drain), the next autocommit request reaches the new primary through a redirect without a client-visible error; a request whose reply was lost returns an error and is never sent again.
- [ ] An interactive transaction left open past `transaction_timeout_ms` rolls back and its stream errors, and another client's write then succeeds.
- [ ] A baton sent with another principal's token is refused with `403`, and a baton idle past `idle_timeout_ms` is refused as expired.
- [ ] The simulation scenario `tests/sim/router.rs` writes and reads through the router across a primary crash and a drain, and the history checker passes on the fixed seeds.
- [ ] `Database.Session` is in `auth::RPCS` with `read on the database` and its handler's write check; the proto `Scope:` comment, the daemon README's table, and the tests agree.
- [ ] `deploy/router.toml`, a "Router" section in the daemon README, `docs/cli.md` (the `router` command), the lexicon, and `docs/roadmap.md` (E7) are updated.

## Hrana v1 support matrix

| Item | Kind | v1 | Notes |
|---|---|---|---|
| HTTP, JSON encoding | transport | in | HTTP/1.1 and HTTP/2 through `hyper`, TLS through `rustls`. |
| WebSocket (`hrana2`, `hrana3` subprotocols) | transport | out | `404` on upgrade requests (`Upgrade: websocket`), Hrana error naming HTTP. |
| Protobuf encoding (`/v3-protobuf/…`) | encoding | out | `404`. |
| `GET /v2`, `GET /v3` | endpoint | in | `200`, empty body. |
| `POST /v2/pipeline` | endpoint | in | v3-only items below are refused with `PROTOCOL_ERROR`. |
| `POST /v3/pipeline` | endpoint | in | |
| `POST /v3/cursor` | endpoint | in | Newline-delimited JSON, entries streamed as the node answers. |
| sqld `/v1/execute`, `/v1/batch`, `/health`, `/version` | endpoint | out | `404`. |
| `execute` | request | in | Positional `args` and `named_args`; `want_rows`. |
| `batch` | request | in | Steps run in order on the node, each with its condition. |
| `sequence` | request | in | `sql` or `sql_id`; statements run one by one until the first error. |
| `describe` | request | in | `params`, `cols` with `decltype`, `is_explain`, `is_readonly`. |
| `store_sql`, `close_sql` | request | in | Kept in the router per stream, up to `[streams] max_sql_per_stream`. |
| `get_autocommit` | request | in (v3) | Answered from the stream's last known state. |
| `close` | request | in | Closes the node session; the response's baton is `null`. |
| Condition `ok`, `error`, `not`, `and`, `or` | condition | in | Evaluated on the node. |
| Condition `is_autocommit` | condition | in (v3) | Evaluated on the node. |
| Values `null`, `integer` (decimal string), `float`, `text`, `blob` (`base64`) | value | in | Non-finite floats in results are refused with `FLEETRING_VALUE`. |
| Batons, `base_url` | stream | in | `base_url` only when `[http] advertise_url` is set. |
| Interactive transactions across requests | stream | in | Bounded by `[sessions] transaction_timeout_ms` on the node. |
| `SAVEPOINT` outside a transaction | statement | out | Refused: begin with `BEGIN`. Inside a transaction, in. |
| `rows_read`, `rows_written`, `query_duration_ms` in results | result field | out | Omitted; step 11's conformance tests pin that clients accept their absence. |
| Reads served by followers | routing | out | Every session runs on the primary. |

## Implementation plan

Each step is about one commit (`<type>(<scope>): …`, ending with `Refs: FR-066`). Every path is under `crates/fleetringd/` unless it says otherwise. New public types derive `Debug` and are added to `src/lib.rs`'s `assert_debug_send_sync` list; errors are `#[non_exhaustive]` `thiserror` enums with lowercase messages; no `bool` parameters (use two-variant enums); no `HashMap`/`HashSet`; time only through `tokio::time` or `clock::Clock`; randomness only through an injected generator.

### Step 1: the lexicon and the CLI guide (`docs: name the router's words`)

- `docs/lexicon.md`:
  - **Router:** drop *planned*; Code: `fleetringd::router`, `[[principal]] forward_tokens = true`; CLI: `fleetringd router --config <file>`.
  - New entry **Session**: "The data plane's half of a Hrana stream: one `Database.Session` RPC from a router to a database's primary, in which statements run with SQLite's own transaction semantics on the primary's writer, so a transaction can span several requests (ADR-0037). A session without `write` on its database is read-only. Applies to: daemon. Code: `Database.Session`, `SessionCall`, `engine::SqlSessions`, `[sessions]`. CLI: none. Not: connection (the HTTP or gRPC connection under it), transaction (a session can hold several, one at a time), cursor."
  - New entry **Stream (Hrana)** under a heading **Hrana stream**: "A client's sequence of Hrana requests that share one session, continued across HTTP requests by a baton (ADR-0037). Always say *Hrana stream*: a change stream and a replication stream are other things. Applies to: daemon. Code: `router::streams`, `HranaStream`. CLI: none. Not: stream alone, connection, session (that is the node side)."
  - New entry **Baton**: "The opaque string that continues a Hrana stream: the router returns one with each response and accepts it once, with the same principal's token, on the next request (ADR-0037). Applies to: daemon. Code: `router::Baton`. CLI: none. Not: session ID, cursor, ticket, token (a token is a credential)."
  - "Words we don't use": add `connection (for a Hrana stream)` → `Hrana stream`.
- `docs/cli.md` section 1, "Top-level commands": add `router` to the list, with the reason "a long-running process with its own identity, like the daemon itself (ADR-0037)".

### Step 2: session types in the engine (`feat(fleetringd): add session statements and results to the engine`)

- New file `src/engine/session.rs`, re-exported from `src/engine/mod.rs` (`pub use session::{…}`):

  ```rust
  /// Whether a session's statements may write (ADR-0037, point 17).
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  #[non_exhaustive]
  pub enum SessionAccess { ReadWrite, ReadOnly }

  /// Whether a statement's result should carry its rows.
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
  pub enum WantRows { #[default] Rows, NoRows }

  /// One statement in a session: SQL text with positional and named arguments.
  #[derive(Debug, Clone, PartialEq)]
  pub struct SessionStatement {
      pub sql: String,
      pub args: Vec<Value>,
      /// Names include their prefix character (`:a`, `@a`, `$a`), as Hrana sends them.
      pub named_args: Vec<(String, Value)>,
      pub want_rows: WantRows,
  }

  #[derive(Debug, Clone, PartialEq)]
  pub enum BatchCondition { Ok(u32), Error(u32), Not(Box<BatchCondition>), And(Vec<BatchCondition>), Or(Vec<BatchCondition>), IsAutocommit }

  #[derive(Debug, Clone, PartialEq)]
  pub struct BatchStep { pub condition: Option<BatchCondition>, pub statement: SessionStatement }

  /// One call a session makes.
  #[derive(Debug, Clone, PartialEq)]
  #[non_exhaustive]
  pub enum SessionCall {
      Execute(SessionStatement),
      Batch(Vec<BatchStep>),
      /// Several statements in one text, run in order until the first error.
      Sequence(String),
      Describe(String),
      GetAutocommit,
  }

  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct Column { pub name: String, pub decltype: Option<String> }

  /// A statement that ran.
  #[derive(Debug, Clone, PartialEq, Default)]
  pub struct StatementOutcome {
      pub columns: Vec<Column>,
      pub rows: Vec<Vec<Value>>,
      pub affected_row_count: u64,
      /// `sqlite3_last_insert_rowid` after the statement, if it inserted a row.
      pub last_insert_rowid: Option<i64>,
  }

  /// Why one statement failed. The session goes on.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct StatementError {
      pub message: String,
      /// SQLite's extended result code name (`SQLITE_CONSTRAINT_UNIQUE`), or one of
      /// `PERMISSION_DENIED`, `FLEETRING_SAVEPOINT`, `FLEETRING_VALUE`.
      pub code: String,
  }

  #[derive(Debug, Clone, PartialEq)]
  pub enum StepOutcome { Ok(StatementOutcome), Error(StatementError), Skipped }

  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct Description {
      /// Each parameter's name with its prefix, or `None` for a positional one.
      pub params: Vec<Option<String>>,
      pub columns: Vec<Column>,
      pub is_explain: bool,
      pub is_readonly: bool,
  }

  #[derive(Debug, Clone, PartialEq)]
  #[non_exhaustive]
  pub enum CallOutcome {
      Statement(Result<StatementOutcome, StatementError>),
      Batch(Vec<StepOutcome>),
      Sequence(Result<(), StatementError>),
      Described(Result<Description, StatementError>),
      Autocommit,
  }

  /// Whether the writer is outside a transaction after a call.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum Autocommit { On, InTransaction }

  /// Whether a call may write, which decides whether the replica floor is checked first.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum WriteIntent { ReadOnly, MayWrite }

  #[derive(Debug, Clone, PartialEq)]
  pub struct CallResult { pub outcome: CallOutcome, pub autocommit: Autocommit }

  /// Runs Hrana-shaped calls with the engine's own transaction semantics (ADR-0037, point 12).
  pub trait SqlSessions: fmt::Debug + Send {
      /// Whether `call` may write: any statement that isn't read-only, or that begins a
      /// transaction. Prepares every statement and runs none.
      fn intent(&mut self, call: &SessionCall) -> Result<WriteIntent, EngineError>;
      /// Runs `call`. Statement failures are in the outcome; `Err` is a storage failure.
      fn run(&mut self, call: &SessionCall, access: SessionAccess) -> Result<CallResult, EngineError>;
      /// Whether a transaction is open on the writer.
      fn autocommit(&self) -> Autocommit;
      /// Rolls back the open transaction, if any.
      fn roll_back(&mut self) -> Result<(), EngineError>;
  }
  ```

- `src/engine/adapter.rs`, trait `Database`, add after `logical_changes`:

  ```rust
  /// The database's SQL sessions, if its engine runs them (ADR-0037). The default is `None`,
  /// and a `Database.Session` on such a database is refused with `FAILED_PRECONDITION`.
  fn sql_sessions(&mut self) -> Option<&mut dyn SqlSessions> { None }
  ```

- `BatchCondition` evaluation lives here as a pure function, used by the engine and unit-tested here:

  ```rust
  /// Evaluates `condition` against the outcomes of the steps before it.
  /// A step index at or past `outcomes.len()` is false.
  pub fn holds(condition: &BatchCondition, outcomes: &[StepOutcome], autocommit: Autocommit) -> bool;
  ```

  `Ok(i)`: step `i` is `StepOutcome::Ok`. `Error(i)`: step `i` is `StepOutcome::Error`. A skipped step is neither. `IsAutocommit`: `autocommit == Autocommit::On`.
- Unit tests in `src/engine/session.rs`: `conditions_follow_the_outcomes_of_earlier_steps` (each variant, including a skipped step being neither ok nor error and a forward reference being false).

### Step 3: SQLite runs sessions (`feat(fleetringd): run session calls on the SQLite writer`)

All in `src/engine/sqlite.rs`.

- `Confinement` gains `control: Mutex<Option<Control>>`, where `enum Control { Begin, Commit, Rollback, Savepoint, Release, RollbackTo }`, set by the authorizer from `AuthAction::Transaction { operation }` (`TransactionOperation::Begin|Commit|Rollback`) and `AuthAction::Savepoint { operation, .. }` (`Begin` → `Savepoint`, `Release` → `Release`, `Rollback` → `RollbackTo`) while `user` is set. It never refuses them; `run_statements` keeps refusing transaction control for `execute` through the autocommit check it has today. Add `fn take_control(&self) -> Option<Control>`.
- `SqliteDatabase` gains `transaction: Option<OpenTransaction>` with `struct OpenTransaction { wrote: Wrote }` and `enum Wrote { No, Yes }`.
- `impl Database for SqliteDatabase { fn sql_sessions(&mut self) -> Option<&mut dyn SqlSessions> { Some(self) } }`.
- `impl SqlSessions for SqliteDatabase`:
  - **Preparing a statement** (`fn prepare_session(&self, sql: &str) -> Result<Prepared, StatementError>`): prepare on the writer inside `UserStatement::enter(&self.writing)`; read `stmt.readonly()` and `self.writing.take_control()`; bind `args` by index (1-based) and `named_args` with `parameter_index(name)` (an unknown name is a `StatementError` with code `SQLITE_RANGE`). Errors go through `self.writing.classify` and become `StatementError { message, code }` with `code` from `rusqlite::Error::SqliteFailure(e, _)` mapped by `fn code_name(extended: i32) -> &'static str` (a `match` over the extended codes rusqlite exposes in `ffi`, falling back to the primary code's name, then `"SQLITE_ERROR"`).
  - **`intent`:** for each statement in the call (`Sequence` and `Describe` split with `rusqlite::Connection::prepare` on the tail, using `Statement`'s tail offset; `Batch` all steps), `MayWrite` if any is not read-only or is `Control::Begin`; `ReadOnly` otherwise. Prepares only.
  - **`run` with `SessionAccess::ReadOnly`:** a statement that isn't read-only or that controls a transaction is `StatementError { code: "PERMISSION_DENIED", message: "the session can only read: this statement needs write on the database" }` without running. Everything else runs on the **reader** connection (`self.reader`, `self.reading` confinement), as `query` does.
  - **`run` with `SessionAccess::ReadWrite`**, per statement:
    1. Prepared read-only, no control statement, and `self.transaction.is_none()`: run on the reader.
    2. `Control::Savepoint` with `self.transaction.is_none()`: `StatementError { code: "FLEETRING_SAVEPOINT", message: "start a transaction with BEGIN before SAVEPOINT" }`.
    3. `Control::Begin`: step it on the writer; set `self.transaction = Some(OpenTransaction { wrote: Wrote::No })`.
    4. Any statement inside a transaction that isn't read-only: if `wrote` is `No` and a recorder exists, `recorder.begin(&self.writer, self.end)` first, then set `wrote = Yes`; step it; then `recorder.after_statement` and `refuse_programs_on_reserved_names` as `run_statements` does.
    5. `Control::Commit` (`COMMIT` or `END`): if `wrote` is `Yes` and a recorder exists, `recorder.commit(&self.writer)` first; step it. If the writer is still in a transaction afterwards (a deferred constraint failed), run `self.roll_back()` and return the statement's error with ` ; the transaction was rolled back` appended to its message. Clear `self.transaction`.
    6. `Control::Rollback`: step it; clear `self.transaction`. `Release` and `RollbackTo` inside a transaction: step them.
    7. A write outside a transaction (autocommit): `BEGIN IMMEDIATE`, `recorder.begin`, step, `recorder.after_statement`, `recorder.commit`, `COMMIT`, exactly as `execute` does for one statement; a failure rolls back that statement only.
    - After every statement: `rows` collected when `want_rows` is `Rows` (columns from `column_name` and `column_decltype`), `affected_row_count` as `total_changes` before and after minus recorded rows (as `execute` computes it), `last_insert_rowid` from `self.writer.last_insert_rowid()` when the statement inserted on the writer and changed at least one row, else `None`.
    - `Batch`: evaluate each step's condition with `session::holds` against the outcomes so far and `self.autocommit()`; a false condition is `StepOutcome::Skipped`.
    - `Sequence`: run each statement of the text in order with `WantRows::NoRows`; stop at the first error.
    - `Describe`: prepare only; `params` from `parameter_count` and `parameter_name`, `columns` as above, `is_explain` from `sqlite3_stmt_isexplain` (rusqlite `Statement::is_explain() > 0`), `is_readonly` from `readonly()`.
    - `GetAutocommit`: `CallOutcome::Autocommit`.
    - Result `autocommit`: `Autocommit::On` if `self.writer.is_autocommit()`, else `InTransaction`.
  - **`roll_back`:** `self.roll_back()`; clear `self.transaction`.
  - A real value read back that isn't finite is `StatementError { code: "FLEETRING_VALUE", message: "a REAL value isn't finite and can't be sent as JSON" }` for that statement.
- `checkpoint` on a frame source returns `EngineError::CheckpointBlocked` without trying while `self.transaction.is_some()`.
- Tests in `tests/actor/sqlite.rs` (engine-level, opening `SqliteEngine` directly as the file's other tests do):
  - `a_session_statement_autocommits_and_reports_its_results`: `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`; `INSERT … RETURNING id` returns one row, `affected_row_count == 1`, `last_insert_rowid == Some(1)`, `Autocommit::On`; `committed_since(None)` holds both commits.
  - `a_transaction_spans_calls_and_commits_once`: `BEGIN`; two `INSERT`s in separate calls (each `InTransaction`, and `committed_since` after them returns nothing new); `COMMIT` → `On`, and exactly one new transaction in the log (`FrameSourceEngine::transactions` count).
  - `a_failed_batch_step_leaves_earlier_steps_committed`: batch `INSERT 1`, `INSERT` violating a unique constraint, `INSERT 3` with condition `ok(1)`: outcomes `Ok`, `Error(SQLITE_CONSTRAINT_UNIQUE)`, `Skipped`; the table holds row 1.
  - `a_rolled_back_transaction_leaves_nothing`: `BEGIN`, `INSERT`, `ROLLBACK`; table empty, log unchanged.
  - `a_read_only_session_refuses_writes_and_transaction_control`: `ReadOnly` access; `SELECT` succeeds; `INSERT` and `BEGIN` give `PERMISSION_DENIED`; nothing ran.
  - `a_savepoint_outside_a_transaction_is_refused`: code `FLEETRING_SAVEPOINT`; inside `BEGIN … COMMIT`, `SAVEPOINT a; RELEASE a` work.
  - `session_statements_stay_confined`: `ATTACH` and `VACUUM INTO` through a session give the confinement's errors (ADR-0026 point 8).
  - `a_session_records_row_changes_for_logical_streams`: with a recorder started, a two-call transaction produces one change record whose rows match `execute`'s for the same statements.
  - `describe_reports_parameters_and_columns`: `SELECT a AS x FROM t WHERE b = :b AND c = ?` gives `params == [Some(":b"), None]`, one column `x`, `is_readonly`.
  - `intent_says_may_write_without_running_anything`.

### Step 4: the actor holds transactions (`feat(fleetringd): hold session transactions in the database actor`)

- `src/actor.rs`:
  - `pub(crate) struct SessionId(u64)` (`Copy`, `Ord`), minted by `DataNode` from an `AtomicU64` (step 6).
  - `Request` gains:
    - `SessionCall { session: SessionId, call: SessionCall, access: SessionAccess, reply: Reply<(CallResult, Option<Acknowledgement>)> }`
    - `SessionEnd { session: SessionId, reply: Reply<()> }`
    - Both answered with `DatabaseError` from `fail` like the rest.
  - `DatabaseError` gains:
    - `Busy { name }`: `"database {name} has too many writes waiting for a transaction"`.
    - `TransactionTimedOut { name }`: `"database {name} rolled back a transaction open longer than the session limit"`.
    - `NoSessions { name }`: `"database {name}'s engine can't run sql sessions"`.
  - `ActorConfig` gains `sessions: SessionLimits` with `pub(crate) struct SessionLimits { pub(crate) transaction_timeout: Duration, pub(crate) max_waiting_writes: usize }`.
  - `Actor` gains `holder: Option<Holder>` (`struct Holder { session: SessionId, deadline: tokio::time::Instant }`), `waiting: VecDeque<Request>`, and `expired: BTreeSet<SessionId>`.
  - `serve`: while `holder` is `Some`, the `select!` also waits on `time::sleep_until(holder.deadline)`; on expiry, run `sql_sessions().roll_back()` on the executor, insert the session into `expired`, clear `holder`, log at warn (`database`, `session`), and drain `waiting` in order. The idle timeout doesn't apply while `holder` is `Some`.
  - `handle`, while `holder` is `Some(h)`: `Execute`, `Checkpoint`, `Snapshot`, `PinSnapshot`, and `SessionCall` from a session other than `h.session` are pushed onto `waiting` if `waiting.len() < max_waiting_writes`, else answered `DatabaseError::Busy`. Everything else is handled at once. A `SessionCall` from a session in `expired` is answered `TransactionTimedOut` and the id removed.
  - `session_call(session, call, access)`:
    1. `failed` → `Failed`, as `execute`.
    2. On the executor: `let intent = sessions.intent(&call)?`; if `MayWrite` and `access` is `ReadWrite`, `self.publisher.admit(&self.name)` → `Refused` before running.
    3. On the executor: `sessions.run(&call, access)`, then capture `frames.committed_since(last)` whatever the outcome, as `execute` does.
    4. If the captured batch holds more than one transaction (`FrameSourceEngine::transactions(&batch)?.len() > 1`, through `config.engine.capability()`), publish one `CommittedBatch` per transaction, each read with `committed_between(previous_end, transaction_end)`, in order; keep the last `Acknowledgement`. Otherwise publish the batch as `execute` does.
    5. After the call: if `result.autocommit` is `InTransaction`, set `holder = Some(Holder { session, deadline: now + transaction_timeout })` unless it is already this session's (keep the original deadline); if `On` and `holder` is this session, clear it and drain `waiting`.
  - `session_end(session)`: if `holder` is this session, roll back, clear, drain `waiting`; remove from `expired`; reply `Ok(())`.
  - `DatabaseHandle` gains `pub(crate) async fn session_call(&self, session, call, access) -> Result<SessionCalled, DatabaseError>` (waits for the acknowledgement as `execute_reserved` does) and `pub(crate) async fn session_end(&self, session) -> Result<(), DatabaseError>`, with `pub(crate) struct SessionCalled { pub(crate) result: CallResult, pub(crate) acknowledged: Option<Acknowledged> }`.
- `src/registry.rs`: `pub(crate) async fn session_call(&self, name, session, call, access) -> Result<SessionCalled, DatabaseError>` and `session_end`, opening the database as `execute` does.
- `src/config.rs`: `Config` gains `#[serde(default)] pub sessions: SessionsConfig`:

  ```toml
  [sessions]
  transaction_timeout_ms = 30000   # a session transaction open longer is rolled back
  max_waiting_writes = 64          # writes queued behind an open transaction, per database
  ```

  `SessionsConfig { transaction_timeout_ms: NonZeroU64, max_waiting_writes: NonZeroUsize }`, `deny_unknown_fields`, threaded into `ActorConfig` through `Registry::new`'s `EngineConfig` argument (add a `sessions: SessionLimits` field to the registry's settings). Export `SessionsConfig` from `lib.rs`.
- `src/data/service.rs` `database_status`: `Busy` → `RESOURCE_EXHAUSTED`, `TransactionTimedOut` → `ABORTED`, `NoSessions` → `FAILED_PRECONDITION`.
- Tests in `tests/actor/sqlite.rs` (through `Registry`/`DatabaseHandle` with `RecordingPublisher`):
  - `an_open_transaction_makes_other_writes_wait`: session A `BEGIN; INSERT 1`; an `execute(INSERT 2)` doesn't complete within 50 ms (`tokio::time::timeout` with paused time); A `COMMIT`; the execute completes; the recorded batches are A's transaction then the execute's.
  - `reads_go_on_while_a_transaction_is_open`: during A's transaction, `query` returns the committed rows only.
  - `too_many_waiting_writes_are_refused_busy`: `max_waiting_writes = 2`; the third waiting write gets `Busy`.
  - `a_transaction_past_its_limit_rolls_back`: `transaction_timeout = 1 s`; after advancing 1.1 s, A's next call is `TransactionTimedOut`, its insert is gone, and a waiting write ran.
  - `ending_a_session_rolls_back_its_transaction`.
  - `a_call_that_commits_several_transactions_publishes_one_batch_each`: a batch of three autocommit inserts records three `CommittedBatch`es whose positions chain.
  - `a_session_write_below_the_replica_floor_is_refused_before_it_runs`: a publisher whose `admit` refuses; the insert never ran; a `SELECT` in the same session still runs.

### Step 5: the protocol (`feat(proto): add the Database.Session stream`)

- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`, service `Database`, after `Verify`:

  ```proto
  // One Hrana stream's statements on the database's primary (ADR-0037): the first message opens
  // the session, then calls run in order with SQLite's own transaction semantics on the
  // primary's writer. A node that isn't the primary refuses the open with UNAVAILABLE and the
  // `fleetring-primary` hint. A caller without write on the database gets a read-only session.
  // A call that committed is answered once its write quorum holds it. The session ends with
  // UNAVAILABLE and the hint when the node loses the lease, and with ABORTED when a commit
  // can't reach its write quorum or its transaction outlived the node's limit.
  // Scope: read on the database; statements that write need write on it.
  rpc Session(stream SessionRequest) returns (stream SessionResponse);
  ```

  Messages, after `CreateResponse`:

  ```proto
  message SessionRequest {
    oneof request {
      SessionOpen open = 1;
      SessionCall call = 2;
    }
  }

  message SessionOpen {
    string database = 1;
  }

  message SessionCall {
    // Chosen by the caller, increasing; the result carries it back.
    uint64 call_id = 1;
    oneof call {
      SessionStatement execute = 2;
      SessionBatch batch = 3;
      // Several statements in one text, run in order until the first error.
      string sequence = 4;
      // One statement to describe without running it.
      string describe = 5;
      GetAutocommit get_autocommit = 6;
    }
  }

  message SessionStatement {
    string sql = 1;
    repeated Value args = 2;
    repeated NamedValue named_args = 3;
    // Unset or true: the result carries rows.
    optional bool want_rows = 4;
  }

  message NamedValue {
    // With its prefix character: `:a`, `@a`, or `$a`.
    string name = 1;
    Value value = 2;
  }

  message SessionBatch {
    repeated BatchStep steps = 1;
  }

  message BatchStep {
    optional BatchCondition condition = 1;
    SessionStatement statement = 2;
  }

  message BatchCondition {
    oneof condition {
      uint32 ok = 1;
      uint32 error = 2;
      BatchCondition not = 3;
      BatchConditions and = 4;
      BatchConditions or = 5;
      IsAutocommit is_autocommit = 6;
    }
  }

  message BatchConditions {
    repeated BatchCondition conditions = 1;
  }

  message IsAutocommit {}

  message GetAutocommit {}

  message SessionResponse {
    oneof response {
      SessionOpened opened = 1;
      SessionResult result = 2;
    }
  }

  message SessionOpened {
    // The primary's node ID and the epoch it serves at.
    string primary = 1;
    uint64 epoch = 2;
    // Set when the caller may only read.
    bool read_only = 3;
  }

  message SessionResult {
    uint64 call_id = 1;
    oneof result {
      StatementResult statement = 2;
      BatchResult batch = 3;
      SequenceResult sequence = 4;
      Described described = 5;
      AutocommitResult autocommit_result = 6;
      // The call's statement failed (execute, sequence, describe); the session goes on.
      SessionError error = 7;
    }
    // Whether the writer is outside a transaction after the call.
    bool autocommit = 8;
    // Set when the call committed one or more transactions, once the last one's write quorum
    // holds it.
    optional Commit commit = 9;
  }

  message StatementResult {
    repeated SessionColumn columns = 1;
    repeated Row rows = 2;
    uint64 affected_row_count = 3;
    optional sint64 last_insert_rowid = 4;
  }

  message SessionColumn {
    string name = 1;
    optional string decltype = 2;
  }

  message BatchResult {
    repeated BatchStepResult steps = 1;
  }

  message BatchStepResult {
    oneof step {
      StatementResult ok = 1;
      SessionError error = 2;
      Skipped skipped = 3;
    }
  }

  message Skipped {}

  message SequenceResult {}

  message AutocommitResult {}

  message Described {
    repeated DescribedParam params = 1;
    repeated SessionColumn columns = 2;
    bool is_explain = 3;
    bool is_readonly = 4;
  }

  message DescribedParam {
    optional string name = 1;
  }

  message SessionError {
    string message = 1;
    // SQLite's extended result code name, or PERMISSION_DENIED, FLEETRING_SAVEPOINT,
    // FLEETRING_VALUE, or FLEETRING_UNACKNOWLEDGED.
    string code = 2;
  }

  message Commit {
    uint32 acknowledged_by = 1;
    uint32 replicas = 2;
    bool under_replicated = 3;
    uint64 epoch = 4;
  }
  ```

- `crates/fleetring-proto/src/lib.rs`: `PROTOCOL_VERSION` minor `0` → `1`. FR-064 bumps it too; whichever lands second takes the next minor.
- `src/auth/policy.rs`, `RPCS`, `Database` rows: `rpc("/fleetring.v1.Database/Session", Requirement::Database(Action::Read), Binding::None)`.
- `src/auth/boundary.rs`, `impl Authorized`: `pub fn may(&self, action: Action, database: &DatabaseName) -> bool`, the caller's scopes checked without an audit line (`Caller::sees` already has this logic; delegate to it). Doc: "for a handler that narrows what an already authorized request may do".
- `src/data/convert.rs`: `session_call_from_proto(SessionCall) -> Result<(u64, engine::SessionCall), Status>` (an empty `oneof` is `INVALID_ARGUMENT`; a condition nested deeper than 32 is `INVALID_ARGUMENT`), `call_result_to_proto(call_id, &CallResult, Option<Acknowledged>, epoch) -> SessionResult`, and the value conversions reused from `params_to_proto`/`rows_to_proto`.
- Tests: the existing table tests in `src/auth/policy.rs` that compare `RPCS` with the proto's `Scope:` lines and the README pass with the new row (the README row is added in step 13; add it here so the test passes: `| Database.Session | read on the database; write for statements that write |`). Add `convert` unit tests `session_calls_round_trip_through_the_protocol` and `a_condition_too_deep_is_refused`.

### Step 6: the node serves sessions (`feat(fleetringd): serve Database.Session on the primary`)

- `src/data/node.rs`: `DataNode` gains `sessions: AtomicU64` in its inner state and `pub(crate) fn next_session(&self) -> SessionId`.
- New file `src/data/session.rs`:

  ```rust
  pub(crate) type SessionStream = Pin<Box<dyn Stream<Item = Result<SessionResponse, Status>> + Send>>;

  /// Serves one `Database.Session` (ADR-0037, points 11–17).
  pub(crate) async fn open(node: DataNode, request: Request<Streaming<SessionRequest>>) -> Result<SessionStream, Status>;
  ```

  1. Read the first message; anything but `open` is `INVALID_ARGUMENT` ("a session starts with open"). Parse the name; `require_database(&request, &name)` (`read`); `access = if authorized.may(Action::Write, &name) { ReadWrite } else { ReadOnly }`.
  2. `let lease = node.route(&name).await?` (a redirect or `UNAVAILABLE` here leaves nothing behind). If the database's engine has no SQL sessions, the first call answers `FAILED_PRECONDITION` (the registry returns `NoSessions`).
  3. Send `SessionOpened { primary: node.id(), epoch: lease.epoch, read_only: access == ReadOnly }`.
  4. Spawn a task holding `node.replication().served()` (the watch of held leases). Loop with `select!`:
     - the next incoming message: a second `open` is `INVALID_ARGUMENT`; a `call` runs through `registry.session_call(&name, session, call, access)`; the reply's `CallResult` and acknowledgement become a `SessionResult` with `commit` set when an acknowledgement came back. A `DatabaseError::Replication` (the quorum failed after the commit) is sent as `SessionResult { error: SessionError { code: "FLEETRING_UNACKNOWLEDGED", message: "the transaction committed on the primary but no write quorum acknowledged it; it may or may not survive" } }`, then the stream ends with `ABORTED`. `TransactionTimedOut` ends it with `ABORTED`. Any other `DatabaseError` ends it with `database_status(&error)`.
     - `served.changed()`: if the database's epoch is no longer `lease.epoch`, end with `not_primary(name, primary)` when the new primary's address is known from the control plane (`node.locate`), else `UNAVAILABLE` "database {name} is moving to a new primary".
     - the incoming stream ending: stop.
  5. On every exit, `registry.session_end(&name, session)`.
  6. The response stream is wrapped in `authorized.guard(…)`, so a session opened with a token ends when the token expires or is revoked (ADR-0026 point 7).
- `src/data/service.rs`: `type SessionStream = SessionStream;` and `async fn session(&self, request: Request<Streaming<SessionRequest>>) -> Result<Response<Self::SessionStream>, Status>` calling `session::open`.
- Tests in `tests/database.rs` (with the file's existing three-node fleet helpers and test authority):
  - `a_session_runs_a_transaction_across_calls`: open on the primary; `BEGIN`, `INSERT`, `INSERT`, `COMMIT`; the `COMMIT` result has `commit.acknowledged_by >= 2`, `autocommit == true`; a follower's copy (after `await_replicated`) holds both rows.
  - `a_session_on_a_follower_is_redirected`: opening on a follower gives `UNAVAILABLE` with `primary_hint` naming the primary's address.
  - `a_read_only_token_gets_a_read_only_session`: token `read:notes`; `SessionOpened.read_only`; `SELECT` works; `INSERT` gives `SessionError.code == "PERMISSION_DENIED"` with "write" in the message; `Execute` of the same `INSERT` with the same token is `PERMISSION_DENIED`.
  - `a_token_without_read_cant_open_a_session`: `PERMISSION_DENIED` at open.
  - `losing_the_lease_ends_sessions_and_rolls_back`: `BEGIN; INSERT`; drain the primary; the stream ends `UNAVAILABLE` with the hint; the new primary's table lacks the row.
  - `a_session_closed_mid_transaction_rolls_back`: drop the request stream after `BEGIN; INSERT`; a later `Execute` succeeds at once and the row is absent.

### Step 7: per-call credentials for control and data calls (`refactor(fleetringd): send a caller's credentials per call`)

- `src/control/client.rs`: `pub async fn locate_as(&self, request: LocateRequest, credentials: &Credentials) -> Result<LocateResponse, Status>`, retried and redirected exactly as `locate`. Implement by giving `current()` and `following()` a `credentials: &Credentials` argument and passing `&self.shared.credentials` from every existing method.
- `src/transport.rs`: no change; the router uses `database_client_with(channel, &credentials)` with `Peers::channel_to(address)`.
- Test in `tests/control.rs`: `locate_as_carries_the_given_token`: a client whose own credentials are none locates with a `read:notes` token, and with a token for another prefix gets `PERMISSION_DENIED`.

### Step 8: router configuration, naming, and Hrana's JSON (`feat(fleetringd): add the router's configuration and Hrana types`)

- `Cargo.toml` (`crates/fleetringd`): `hyper` features `client`, `http1`, `http2`, `server`; `hyper-util` features add `server-auto`, `http2`; add `percent-encoding = "2"`. Check `cargo deny check` passes.
- `src/lib.rs`: `pub mod router;`.
- `src/router/mod.rs`: module docs summarizing ADR-0037; `pub use` of `Router`, `RouterConfig`, `RouterError`, `StartError`.
- `src/router/config.rs`:

  ```rust
  /// The router's configuration file (ADR-0037).
  #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
  #[serde(deny_unknown_fields)]
  pub struct RouterConfig {
      /// Where the router serves Hrana.
      pub listen: SocketAddr,
      #[serde(default)] pub http: HttpConfig,
      /// The router's client identity toward the fleet: `ca`, `cert`, `key` (a client
      /// certificate for a principal every node lists with `forward_tokens = true`).
      pub tls: TlsConfig,
      pub control: RouterControl,
      #[serde(default)] pub routing: RoutingConfig,
      #[serde(default)] pub streams: StreamsConfig,
  }
  pub struct HttpConfig { pub cert: Option<PathBuf>, pub key: Option<PathBuf>, pub advertise_url: Option<String>, pub allow_plaintext: Plaintext }
  pub struct RouterControl { pub address: Option<SocketAddr>, pub addresses: Vec<SocketAddr> }   // exactly one of the two is set
  pub struct RoutingConfig { pub host_suffix: Option<String>, pub placement_ttl_ms: NonZeroU64, pub placement_cache_size: NonZeroUsize, pub open_retry_ms: u64 }
  pub struct StreamsConfig { pub idle_timeout_ms: NonZeroU64, pub max_streams: NonZeroUsize, pub max_request_bytes: NonZeroUsize, pub max_sql_per_stream: NonZeroUsize }
  ```

  Defaults: `placement_ttl_ms = 60000`, `placement_cache_size = 100000`, `open_retry_ms = 5000`, `idle_timeout_ms = 10000`, `max_streams = 10000`, `max_request_bytes = 8388608`, `max_sql_per_stream = 128`, `allow_plaintext = false`. `RouterConfig::load(path)` and `from_toml(text)` validate:
  - `http.cert` and `http.key` both set or both unset (`ConfigError::Invalid { field: "http.cert", … }`);
  - without them, `listen` must be a loopback address unless `allow_plaintext` (`ConfigError::Invalid { field: "listen", reason: "serving plaintext on a non-loopback address needs http.allow_plaintext = true, or set http.cert and http.key" }`);
  - `advertise_url` parses as an `http`/`https` URL with a host and no query;
  - `host_suffix`, if set, is a valid DNS name without a leading dot;
  - `control`: exactly one of `address` and `addresses` (`addresses` non-empty).
  Reuse `crate::config::{ConfigError, TlsConfig, Plaintext}`. `RouterControl::addresses()` returns the list. If FR-064 has landed, `ControlClient` takes the list; otherwise the router uses the first address and a later commit wires the rest.
- `src/router/naming.rs`:

  ```rust
  /// The database a request names, and the endpoint under it.
  pub(crate) struct Target { pub(crate) database: DatabaseName, pub(crate) endpoint: Endpoint }
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum Endpoint { Version(Hrana), Pipeline(Hrana), Cursor }
  #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
  pub(crate) enum Hrana { V2, V3 }
  pub(crate) fn target(method: &Method, path: &str, host: Option<&str>, host_suffix: Option<&str>) -> Result<Target, NotFound>;
  ```

  Path: split on `/`, drop empty segments; `["db", name, "v2" | "v3"]` (GET) is `Version`; `["db", name, "v2" | "v3", "pipeline"]` (POST) is `Pipeline`; `["db", name, "v3", "cursor"]` (POST) is `Cursor`; `name` percent-decoded (`percent_encoding::percent_decode_str`, UTF-8, then `DatabaseName::from_str`). Host form, when `host_suffix` is set and the path has no `db` prefix: `host` without its port must end with `.<host_suffix>`; the label before it must match `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`; the path is then `["v2" | "v3", …]`. `NotFound`'s message: `"name the database in the path, /db/<percent-encoded name>/v3/pipeline, or in the host, <name>.<host_suffix>"` (the host half only when `host_suffix` is set). A `GET` with `Upgrade: websocket` is `NotFound` with "this router serves Hrana over HTTP only".
- `src/router/hrana.rs`: `serde` types for Hrana's JSON, `#[serde(tag = "type", rename_all = "snake_case")]` where Hrana tags:
  - `PipelineRequest { baton: Option<String>, requests: Vec<StreamRequest> }`, `PipelineResponse { baton: Option<String>, base_url: Option<String>, results: Vec<StreamResult> }`.
  - `StreamRequest::{Close, Execute { stmt: Stmt }, Batch { batch: Batch }, Sequence { sql: Option<String>, sql_id: Option<i32> }, Describe { sql: Option<String>, sql_id: Option<i32> }, StoreSql { sql_id: i32, sql: String }, CloseSql { sql_id: i32 }, GetAutocommit}`; unknown types fail to decode → `400` `PROTOCOL_ERROR`.
  - `Stmt { sql: Option<String>, sql_id: Option<i32>, args: Vec<HValue> (default), named_args: Vec<NamedArg> (default), want_rows: Option<bool> }`, `NamedArg { name: String, value: HValue }`.
  - `Batch { steps: Vec<BatchStepJson> }`, `BatchStepJson { condition: Option<BatchCondJson>, stmt: Stmt }`, `BatchCondJson::{Ok { step: u32 }, Error { step: u32 }, Not { cond: Box<BatchCondJson> }, And { conds: Vec<BatchCondJson> }, Or { conds: Vec<BatchCondJson> }, IsAutocommit}`.
  - `HValue::{Null, Integer { value: String }, Float { value: f64 }, Text { value: String }, Blob { base64: String }}`; integers parse as `i64` (else `PROTOCOL_ERROR`); blobs decode with standard base64, padding optional, and encode with padding (step 11 pins what clients send).
  - `StreamResult::{Ok { response: StreamResponse }, Error { error: HranaError }}`, `StreamResponse::{Close, Execute { result: StmtResult }, Batch { result: BatchResult }, Sequence, Describe { result: DescribeResult }, StoreSql, CloseSql, GetAutocommit { is_autocommit: bool }}`.
  - `StmtResult { cols: Vec<Col>, rows: Vec<Vec<HValue>>, affected_row_count: u64, last_insert_rowid: Option<String> }`, `Col { name: Option<String>, decltype: Option<String> }`, `BatchResult { step_results: Vec<Option<StmtResult>>, step_errors: Vec<Option<HranaError>> }`, `DescribeResult { params: Vec<DescribeParam>, cols: Vec<Col>, is_explain: bool, is_readonly: bool }`.
  - `HranaError { message: String, code: Option<String> }`.
  - `CursorRequest { baton: Option<String>, batch: Batch }`, `CursorResponseHeader { baton: Option<String>, base_url: Option<String> }`, `CursorEntry::{StepBegin { step: u32, cols: Vec<Col> }, StepEnd { affected_row_count: u64, last_insert_rowid: Option<String> }, StepError { step: u32, error: HranaError }, Row { row: Vec<HValue> }, Error { error: HranaError }}`.
- Router error codes (in `HranaError.code`): `PROTOCOL_ERROR`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `STREAM_EXPIRED`, `STREAM_INVALID`, `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `FLEETRING_OUTCOME_UNKNOWN`, `FLEETRING_TRANSACTION_ABORTED`, `FLEETRING_UNACKNOWLEDGED`; SQLite's codes pass through from the node.
- Unit tests: `naming::the_path_form_decodes_any_name` (`tenant%3Aacme%2Fnotes`, double slashes), `naming::the_host_form_takes_dns_labels_only`, `naming::anything_else_names_both_forms`, `hrana::json_round_trips` over captured request and response samples for every request type (fixtures under `tests/fixtures/hrana/*.json`, taken from the Hrana 3 specification's examples), `hrana::integers_are_decimal_strings`, `config::plaintext_off_loopback_needs_the_flag`, `config::control_takes_one_of_address_or_addresses`.

### Step 9: authentication, placement, and node sessions (`feat(fleetringd): authenticate router requests and open sessions on primaries`)

- `src/router/auth.rs`:

  ```rust
  pub(crate) struct Authenticated { pub(crate) principal: Principal, pub(crate) token: BearerToken, scopes: Scopes }
  pub(crate) fn authenticate(headers: &HeaderMap, verifier: &TokenVerifier, now: Timestamp) -> Result<Authenticated, Refusal>;
  pub(crate) fn require_read(caller: &Authenticated, database: &DatabaseName) -> Result<(), Refusal>;
  pub(crate) enum Refusal { Unauthenticated(String), PermissionDenied(String) }
  ```

  `Authorization: Bearer <token>` only; the verifier is `Identity::token_verifier()` of the router's identity; revocations are `Revocations::default()` (the router has no denylist; the node checks it). `Unauthenticated` → `401` with `HranaError { code: "UNAUTHENTICATED", message: TokenError's text }`; `PermissionDenied` → `403`, message `"this token can't read database '<name>'"`. The time comes from the router's `Clock`.
- `src/router/placement.rs`:

  ```rust
  pub(crate) struct PlacementCache { entries: Mutex<BTreeMap<DatabaseName, Cached>>, order: Mutex<BTreeMap<(tokio::time::Instant, u64), DatabaseName>>, ttl: Duration, capacity: usize, … }
  struct Cached { primary: Option<String> /* host:port */, epoch: LeaseEpoch, fetched_at: tokio::time::Instant }
  impl PlacementCache {
      pub(crate) async fn primary(&self, database: &DatabaseName, control: &ControlClient, credentials: &Credentials) -> Result<Option<String>, Status>;
      pub(crate) fn redirected(&self, database: &DatabaseName, primary: String);
      pub(crate) fn forget(&self, database: &DatabaseName);
  }
  ```

  A miss or an entry older than the TTL calls `control.locate_as`; the primary's address is the `Replica.address` of `Placement.primary`'s node; `None` when the placement has no primary (a promotion). Inserting past `capacity` evicts the oldest `fetched_at`.
- `src/router/session.rs`:

  ```rust
  /// An open `Database.Session` to a database's primary.
  pub(crate) struct NodeSession { calls: mpsc::Sender<SessionRequest>, results: Streaming<SessionResponse>, next_call: u64, pub(crate) primary: String, pub(crate) epoch: LeaseEpoch, pub(crate) read_only: ReadOnly }
  pub(crate) async fn open(database: &DatabaseName, caller: &Authenticated, ctx: &RouterContext) -> Result<NodeSession, OpenError>;
  impl NodeSession {
      /// Sends one call and waits for its result. A session that ends first is
      /// `CallError::OutcomeUnknown`; the call is never resent.
      pub(crate) async fn call(&mut self, call: session_call::Call) -> Result<SessionResult, CallError>;
  }
  pub(crate) enum OpenError { NotFound(String), Unavailable(String), Denied(Status), Failed(Status) }
  pub(crate) enum CallError { OutcomeUnknown(Status), Ended(Status) }
  ```

  `open`: address from the cache (a `None` primary waits and retries as `UNAVAILABLE` below); dial `ctx.peers.channel_to(address)`; `database_client_with(channel, &Credentials::bearer(&caller.token)?)`; send `SessionOpen` first through an `mpsc` channel wrapped in `ReceiverStream`; read `SessionOpened`. On `UNAVAILABLE` with `primary_hint`: `cache.redirected` and try there, up to 3 redirects. On `UNAVAILABLE` without a hint: `cache.forget`, sleep with `transport::Backoff` (its default schedule) and retry while the total wait is under `open_retry_ms`. `NOT_FOUND` with `missing_database` → `NotFound`. `PERMISSION_DENIED`/`UNAUTHENTICATED` → `Denied`. A dial or transport error before `SessionOpened` → `cache.forget` and retry like `UNAVAILABLE`.
- `RouterContext` (in `src/router/mod.rs`): `{ config: RouterConfig, verifier: TokenVerifier, control: ControlClient, peers: Peers, cache: PlacementCache, streams: Streams, clock: Arc<dyn Clock>, entropy: Arc<dyn Entropy> }`.
- Tests (`src/router/placement.rs` unit tests with a paused clock): `entries_expire_after_the_ttl`, `the_oldest_entry_is_evicted_past_capacity`, `a_redirect_replaces_the_entry`.

### Step 10: Hrana streams, the pipeline, and the cursor (`feat(fleetringd): serve Hrana pipelines and cursors`)

- `src/router/streams.rs`:

  ```rust
  /// Where batons' random bytes come from (ADR-0020).
  pub trait Entropy: fmt::Debug + Send + Sync { fn fill(&self, bytes: &mut [u8; 16]); }
  #[derive(Debug, Default)] pub struct SystemEntropy;   // ring::rand::SystemRandom; panics only if the OS has no randomness
  pub(crate) struct Baton { pub(crate) stream: u64, secret: [u8; 16], pub(crate) sequence: u32 }
  impl Baton { pub(crate) fn encode(&self) -> String; pub(crate) fn decode(text: &str) -> Option<Self>; }   // URL_SAFE_NO_PAD over 28 bytes, big-endian integers
  pub(crate) struct HranaStream { id: u64, secret: [u8; 16], sequence: u32, principal: Principal, token: BearerToken, database: DatabaseName, sql: BTreeMap<i32, String>, session: Option<NodeSession>, autocommit: Autocommit, broken: Option<HranaError>, last_used: tokio::time::Instant }
  pub(crate) struct Streams { next_id: AtomicU64, table: Mutex<BTreeMap<u64, Arc<tokio::sync::Mutex<HranaStream>>>>, limits: StreamsConfig }
  impl Streams {
      pub(crate) fn create(&self, caller: &Authenticated, database: &DatabaseName, entropy: &dyn Entropy) -> Result<Arc<tokio::sync::Mutex<HranaStream>>, Refusal>;   // max_streams → 503 RESOURCE_EXHAUSTED
      pub(crate) fn resume(&self, baton: &str, caller: &Authenticated, database: &DatabaseName) -> Result<Arc<tokio::sync::Mutex<HranaStream>>, StreamRefusal>;
      pub(crate) fn remove(&self, id: u64);
      /// Removes streams idle past the timeout and closes their sessions; run every second.
      pub(crate) async fn expire(&self, now: tokio::time::Instant);
  }
  ```

  `resume` refuses: an undecodable baton or unknown stream → `400` `STREAM_EXPIRED` ("the stream expired or never existed"); a secret that doesn't match (constant-time compare with `ring::constant_time::verify_slices_are_equal`) or a sequence other than the current → `400` `STREAM_INVALID`; another principal → `403` `PERMISSION_DENIED` ("this stream belongs to another principal"); another database → `400` `STREAM_INVALID`. Expired streams are dropped from the table, so their batons are `STREAM_EXPIRED`. Closing a stream drops its `NodeSession`, which ends the RPC and makes the node roll back.
- `src/router/pipeline.rs`:

  ```rust
  pub(crate) async fn pipeline(ctx: &RouterContext, target: &Target, caller: &Authenticated, request: PipelineRequest) -> Result<(PipelineResponse, CommitHeaders), HttpRefusal>;
  ```

  1. A stream from `resume(baton)` or `create`; lock it for the whole request (requests on one stream are sequential).
  2. A broken stream answers every request with its stored error.
  3. For each `StreamRequest`, in order:
     - `store_sql`: insert (`max_sql_per_stream` → error `PROTOCOL_ERROR`); `close_sql`: remove.
     - `get_autocommit` (v3 only; on v2 → `PROTOCOL_ERROR`): `is_autocommit` from `stream.autocommit`.
     - `close`: drop the session; remove the stream after the request; no more requests run.
     - `execute`, `batch`, `sequence`, `describe`: resolve `sql_id`s (unknown → `PROTOCOL_ERROR`); on v2, an `is_autocommit` condition → `PROTOCOL_ERROR`; ensure a session (`session::open` if `None`: a `NotFound` → this request's error `NOT_FOUND`, `Denied` → `PERMISSION_DENIED`, `Unavailable` → `UNAVAILABLE`; if the stream's autocommit was `InTransaction` and the session is gone → `FLEETRING_TRANSACTION_ABORTED`, stream broken); convert to a `SessionCall`; `session.call`. On `CallError::OutcomeUnknown` → error `FLEETRING_OUTCOME_UNKNOWN` ("the connection to the primary broke before this request's result arrived; it may or may not have run"), mark the stream broken, drop the session. On `CallError::Ended` with `ABORTED` after a `FLEETRING_UNACKNOWLEDGED` result → return that result's error and break the stream. Otherwise convert the result, update `stream.autocommit`, and, if `commit` is set, remember it for the headers.
  4. The response's `baton`: `None` if the stream closed, else a new baton with the next sequence. `base_url`: `config.http.advertise_url` when a baton is returned.
  5. A stream in autocommit whose session ended between requests (the stream's `session` `Some` but its RPC has ended) is reopened at step 3 without error.
- `src/router/cursor.rs`: `pub(crate) async fn cursor(ctx, target, caller, request: CursorRequest) -> Result<(CursorResponseHeader, mpsc::Receiver<CursorEntry>), HttpRefusal>`: as one `batch` call through the stream; the header line first, then for each step `step_begin`, its `row`s, `step_end`, or `step_error`; a call error becomes one `error` entry. Entries are written as newline-delimited JSON (`application/x-ndjson` is not required by clients; use `text/plain` as sqld does; step 11 pins it).
- `src/router/http.rs`: `pub(crate) async fn serve(listener: Listener, tls: Option<TlsAcceptor>, ctx: Arc<RouterContext>, shutdown: impl Future<Output = ()>)`: `hyper_util::server::conn::auto::Builder` per accepted connection (`TokioIo` over the TCP or simulated stream, through `tokio_rustls::TlsAcceptor` when TLS is set). A request: `naming::target` → `404`; body over `max_request_bytes` → `413`; `authenticate` → `401`; `require_read` → `403`; dispatch by endpoint; JSON in and out (`content-type: application/json`). Request-level failures are an HTTP status with a JSON `HranaError` body (`{"message": …, "code": …}`). Response headers when the last call committed: `fleetring-primary`, `fleetring-epoch`, `fleetring-acknowledged-by`, `fleetring-replicas`.
- `src/router/mod.rs`:

  ```rust
  pub struct Router { address: SocketAddr, stop: oneshot::Sender<()>, task: JoinHandle<()> }
  impl Router {
      pub async fn start(config: RouterConfig) -> Result<Self, StartError>;   // Environment::production()
      pub async fn start_with(config: RouterConfig, environment: Environment, entropy: Arc<dyn Entropy>) -> Result<Self, StartError>;
      pub fn listen_address(&self) -> SocketAddr;
      pub async fn run_until(self, shutdown: impl Future<Output = ()>);
  }
  #[non_exhaustive] pub enum StartError { Config(ConfigError), Identity(IdentityError), Listen { address, source }, ServerTls(String) }
  ```

  `start_with` loads the client identity (`Identity::load`), puts it on the environment's network (`with_security(Security::Tls(identity))`, so every node call is mutual TLS as the router's principal), builds the `ControlClient` with `network.control_client(address)`, binds `listen` with the same `bind` logic as the daemon (`Listener::Tcp` or `Listener::Simulated`), loads `http.cert`/`http.key` into a `rustls::ServerConfig` (no client authentication), spawns `http::serve` and a task that calls `streams.expire` every second, and logs `router started` with `listen`, `fleet`, `principal`, and `tls`.
- `src/main.rs`: `Command::Router { #[arg(long)] config: PathBuf }` with help "Runs the router: a libSQL/Hrana front door that routes each request to its database's primary"; `run_router(config)` = `RouterConfig::load`, `Router::start`, `run_until(shutdown_signal())`. The logger defaults to `info` for `router` as for the daemon.
- `src/lib.rs`: assert `Debug + Send + Sync` for `router::Router`, `router::RouterConfig`, `router::StartError`, `router::SystemEntropy`.
- Tests in a new `tests/router.rs` (three data nodes and a control node over real TCP with the test authority, as `tests/database.rs` starts them; nodes list `[[principal]] name = "router", forward_tokens = true`; a router on `127.0.0.1:0` in plaintext; requests with `hyper`'s client over `hyper_util::rt::TokioIo`):
  - `a_pipeline_runs_and_closes`: `execute` `CREATE TABLE`, `execute` `INSERT … RETURNING id`, `close`; results ok, `last_insert_rowid == "1"`, baton `null`, `fleetring-primary` header names the primary.
  - `a_transaction_spans_requests`: request 1 `BEGIN`, `INSERT` → baton; request 2 with the baton: `INSERT`, `COMMIT`, `close`; both rows present.
  - `a_batch_rolls_back_on_error`: the shape `@libsql/client` sends for `batch(…, "write")`: `BEGIN`, steps with `ok` conditions, `COMMIT` with `ok`, `ROLLBACK` with `not ok`; a unique violation in step 2 → nothing committed.
  - `the_cursor_streams_rows`: `/v3/cursor` with a two-step batch; entries in order `step_begin`, `row`×N, `step_end`, `step_begin`, …
  - `version_probes_answer`: `GET /db/notes/v3` and `/v2` → `200`; `GET /db/notes/v4` → `404`.
  - `v2_refuses_v3_only_requests`.
  - `requests_without_a_token_get_401` and `a_token_without_read_gets_403_before_any_node_is_called` (the nodes' audit logs, captured with a `tracing` test subscriber, show no request for that database).
  - `a_read_only_token_reads_and_cant_write`: `SELECT` ok; `INSERT` → error code `PERMISSION_DENIED`, message contains "write".
  - `a_drain_redirects_the_next_autocommit_request`: write, `fleetringd node drain` the primary through the client, write again on the same stream → ok, `fleetring-primary` names the new primary.
  - `a_lost_reply_is_an_error_and_never_resent`: a unit test in `src/router/pipeline.rs` (`#[cfg(test)]`, so it can build a `RouterContext` on a plaintext `Network`): a fake `Database` server on `127.0.0.1:0` whose `session` answers `SessionOpened`, counts the calls it receives, and ends the stream with `UNAVAILABLE` after the first call without a result; the placement cache is seeded with `redirected(db, fake_address)`. The pipeline's `INSERT` gets `FLEETRING_OUTCOME_UNKNOWN`, the stream is broken (its next request gets the same error), and the fake received exactly one call and no second open.
  - `an_idle_transaction_rolls_back_and_unblocks_writers`: nodes with `transaction_timeout_ms = 500`; stream A `BEGIN; INSERT`; wait 700 ms (`tokio::time::sleep`); stream A's next request → `FLEETRING_TRANSACTION_ABORTED`; stream B's `INSERT` → ok.
  - `a_baton_with_another_principal_is_refused` (`403`) and `an_idle_baton_expires` (router `idle_timeout_ms = 300`; `400` `STREAM_EXPIRED`).
  - `the_host_form_routes_dns_label_names`.

### Step 11: conformance with real clients (`test(router): run the libsql clients against the router`)

- **Rust, in CI.** A new crate outside the workspace, `tests/router-conformance/` (its own `Cargo.toml` with an empty `[workspace]` table, `publish = false`, its own `Cargo.lock`), so the `libsql` crate's dependency tree never enters the workspace, its MSRV jobs, its cross-target jobs (s390x, i686, wasm), or its `cargo deny` graph:
  - `[dependencies] fleetringd = { path = "../../crates/fleetringd" }`, `tokio`, and `libsql = { version = "<latest 0.x>", default-features = false, features = ["remote"] }`. First check with `cargo tree -e features -i cc` that no C is built (no `libsql-ffi`), that `libsql`'s licence is MIT, and record the version and both facts in the crate's README. If `remote` without defaults can't make TLS connections, the test uses plaintext to the router on loopback, which needs no TLS feature.
  - `tests/conformance.rs` with a `fleet()` helper (control node, three data nodes, the router, a token for `write:*`) and one test per scenario: `execute_returns_rows_and_last_insert_rowid`, `batch_rolls_back_on_error` (`Connection::execute_batch` with a failing statement inside `BEGIN … COMMIT`), `a_multi_statement_script_runs`, `a_transaction_commits` and `a_transaction_rolls_back` (`Connection::transaction()`), `named_and_positional_arguments_bind`, `blobs_and_floats_round_trip`. Each opens `libsql::Builder::new_remote(format!("http://{router}/db/{db}/"), token)`.
  - `.github/workflows/ci.yml`: a job `router conformance (libsql)` on `ubuntu-latest`: `cargo test --manifest-path tests/router-conformance/Cargo.toml --locked`.
- **Node, nightly.** `tests/router/node/package.json` pinning `@libsql/client` to an exact version, `tests/router/node/conformance.mjs` running the same scenario list with `createClient({ url, authToken })` against `process.env.ROUTER_URL`, exiting non-zero on the first failure; `tests/router/start-fleet.sh` starts a control node, three data nodes, and a router from a release build with `fleetringd cert init`, `cert node`, `cert client router`, and `token issue --scope write:*`, and prints `ROUTER_URL` and `FLEETRING_TOKEN`; `.github/workflows/router-conformance.yml` runs it nightly (`cron: "23 4 * * *"`) and on dispatch.
- Both suites also pin what this plan leaves to clients: that `@libsql/client` keeps the URL's path with a trailing slash; that both clients accept results without `rows_read`, `rows_written`, and `query_duration_ms`; what padding their blobs carry; and how they surface a JSON error body with a non-200 status. A client that does otherwise gets a fix in the router in the same pull request.

### Step 12: simulation (`test(sim): route writes through the router under faults`)

- `src/sim/fleet.rs`:
  - `FleetBuilder::router(self, name: &str) -> Self`: a turmoil host running `Router::start_with` with `listen = 0.0.0.0:{ROUTER_PORT}` (`pub const ROUTER_PORT: u16 = 8080`), `allow_plaintext = true`, a client identity for principal `router` from the fleet's authority, `control.address` the control host, `entropy` a `SimEntropy` seeded from the fleet's `SimRng` (fork it, as clients are), and every data host's `principals` gains `PrincipalConfig { name: "router", scopes: Scopes::default(), forward_tokens: TokenForwarding::Allowed }`.
  - `Fleet::client_via_router(&mut self, name: &str, router: &str, workload: impl Workload)`: the client's database operations go through Hrana to `router:8080`, with a token for `workload` with scopes `admin:*`, issued by `authority.token_signer()`.
- `src/sim/client.rs`: a `Transport::{Grpc, Router { address, token }}` choice per client host; under `Router`, `Write` is one pipeline `[execute INSERT INTO writes (value) VALUES (?), close]`, `Read` is `[execute SELECT value FROM writes, close]` (or `WHERE value = ?`), over `hyper::client::conn::http1::handshake` on `turmoil::net::TcpStream`. Outcomes: a `200` with an ok result → `Acknowledged(Ack::Written { acknowledged_by, replicas, under_replicated, primary, epoch })` from the four headers; an ok read → `Ack::Read`; a result error whose code is a SQLite code or `PERMISSION_DENIED`/`NOT_FOUND` → `Rejected`; `FLEETRING_OUTCOME_UNKNOWN`, `FLEETRING_UNACKNOWLEDGED`, `UNAVAILABLE`, a timeout, or a transport error → `Indeterminate`. `Create` stays a gRPC call (Hrana can't create databases).
- `src/sim/workload.rs`: `Operation::TransactionWrite { database: DatabaseName, values: Vec<i64> }`: two pipeline requests on one stream, `[execute BEGIN, execute INSERT v1]`, then `[execute INSERT v2, …, execute COMMIT, close]`; acknowledged as `Ack::Written` like a write. A workload `RouterWrites::new(databases: usize, replication_factor: u32, writes: usize)` that creates its databases through gRPC, then interleaves `Write`, `TransactionWrite` (two or three values), and `Read`.
- `src/sim/history.rs` and `src/sim/check.rs`: a `TransactionWrite`'s values are each held to the write rules (ok → durable and readable, fail → absent, info → either). New invariant `Invariant::TransactionsAtomic` ("transactions are atomic"): on every replica, and in every read, a transaction's values are all present or all absent. Display string "transactions are atomic"; checked in `history::check`.
- `tests/sim/router.rs` (add `mod router;` to `tests/sim/main.rs`): `writes_through_the_router_survive_a_crash_and_a_drain`, over `seeds::FIXED_HEAVY`: a control host, three data hosts, a router host, and two clients through the router running `RouterWrites::new(2, 3, 150)`; at 30% of the run crash the primary of the first database (from `Locate`) and restart it 2 s later; at 60% drain the primary of the second database. Waits for workloads and convergence, then `Fleet::finish()` must pass (history checker with `OnePrimaryPerEpoch`, `AcknowledgedWritesReplicated`, `NoPhantomWrites`, `TransactionsAtomic`, `ReplicasConverged`), and the test asserts at least one `Written` ack at two different epochs per database.
- A second scenario, `a_partitioned_router_breaks_streams_without_losing_writes`: partition the router from the primary for 3 s mid-run; same checks.

### Step 13: metrics (`feat(fleetringd): router metrics`)

Only once FR-063 (#66) has landed; otherwise this step waits and the ticket can close without it (record that in Progress). Register on the router's `Metrics` registry, following ADR-0034's names and its no-database-label rule:

- `fleetring_router_requests_total{endpoint="pipeline|cursor|version", outcome="ok|client_error|unauthenticated|permission_denied|not_found|unavailable|internal"}` counter;
- `fleetring_router_request_duration_seconds{endpoint}` histogram;
- `fleetring_router_streams` gauge (open Hrana streams);
- `fleetring_router_placement_lookups_total{result="hit|miss|expired"}` counter;
- `fleetring_router_redirects_total` counter;
- `fleetring_router_session_opens_total{outcome="ok|redirected|unavailable|not_found|denied"}` counter;
- on data nodes: `fleetring_sessions{state="autocommit|transaction"}` gauge and `fleetring_session_waiting_writes` gauge (summed over databases), `fleetring_session_transaction_timeouts_total` counter.

The router serves the admin listener of ADR-0034 with `[admin]` in `router.toml`, with `/readyz` ready once it has a control-plane answer. Test: `router_metrics_have_no_database_labels`.

### Step 14: documentation (`docs: the router`)

See Documentation below; also this ticket's Progress and ADR-0037's status once accepted.

## Documentation

- `deploy/router.toml`: a commented example with every field and its default:

  ```toml
  listen = "0.0.0.0:8080"

  [http]
  cert = "/etc/fleetring/router/server.crt"   # the router's server certificate, from any authority
  key = "/etc/fleetring/router/server.key"
  advertise_url = "https://router-1.example.com:8080"   # returned as base_url; unset: none
  # allow_plaintext = false                   # true to serve plaintext on a non-loopback address

  [tls]                                       # the router's identity toward the fleet
  ca = "/etc/fleetring/tls/ca.crt"
  cert = "/etc/fleetring/tls/client.router.crt"
  key = "/etc/fleetring/tls/client.router.key"

  [control]
  address = "10.0.0.1:7400"                   # or addresses = ["10.0.0.1:7400", "10.0.0.2:7400"]

  [routing]
  # host_suffix = "db.example.com"            # also route <name>.db.example.com
  placement_ttl_ms = 60000
  placement_cache_size = 100000
  open_retry_ms = 5000

  [streams]
  idle_timeout_ms = 10000
  max_streams = 10000
  max_request_bytes = 8388608
  max_sql_per_stream = 128
  ```

- `deploy/fleetringd.toml`: `[sessions]` with both fields, and a commented `[[principal]] name = "router" scopes = [] forward_tokens = true` entry.
- `crates/fleetringd/README.md`: a "Router" section (what it is; starting it; the client URL forms, with the trailing slash; tokens; the support matrix in short; transactions and their limits; what happens on failover and why a lost reply is an error); the scope table row for `Database.Session`.
- `docs/cli.md`: the `router` command in section 1's top-level list (step 1).
- `docs/lexicon.md`: step 1's entries.
- `docs/adr/README.md`: ADR-0037's row; ADR-0026's row gains "point 4 amended by 0037".
- `docs/roadmap.md`: E7's router line links FR-066.
- `SECURITY.md`: the router as an actor (what a compromised router can do: what its callers' unexpired tokens allow; it holds tokens in memory only and never logs them; it has no denylist, so revocation is enforced by nodes within ADR-0026's bound).
- The FR-061 issue (#63): replace "the router, which has no ticket yet" with FR-066 (#69).

## Owner decisions (2026-11-03)

The open questions this plan raised were decided as recommended:

- The protocol minor version: whichever of FR-064 and FR-066 lands first takes 1.1, the other 1.2.
- Interactive transactions stay in v1, with a 30 s timeout and at most 64 waiting writes per database.
- If `@libsql/client` drops the URL path, document the host form for that client rather than add a header form.
- No denylist on the router in v1; data nodes stay the authoritative token check.

## Open risks

- **Client behaviour this plan can't verify from here.** That `@libsql/client` keeps a URL's path when it has a trailing slash, accepts results without `rows_read`/`rows_written`, sends blobs with a given padding, and reports a JSON error body; and which `libsql` crate version builds without C. Step 11's suites pin each; a mismatch is fixed in the router, not the clients. If `@libsql/client` drops the path regardless, the host form is the documented way for it.
- **Writers wait behind open transactions.** One slow client holds a database's writes for up to `transaction_timeout_ms` (30 s). That is SQLite's model on one machine too, but a fleet makes it visible across tenants only within one database. The timeout, the waiting cap, and FR-063's gauges bound and expose it.
- **Interactive transactions don't survive a failover,** by design: the client gets `FLEETRING_TRANSACTION_ABORTED` and must retry the transaction. ORMs that retry on serialization errors handle it; others surface it.
- **Placement TTLs at scale.** With 100,000 databases and a 60 s TTL, a router can make up to ~1,700 `Locate` calls per second in steady state if every database is busy. Redirects keep it correct; a `WatchLeases`-driven cache (E4) removes the load.
- **Statements that SQLite reports as read-only but have effects** (some table-valued functions, `PRAGMA` reads that the confinement already restricts) run on the reader in read-only sessions; the reader is `query_only`, so they can't write, which is the same guarantee `Query` has today.
- **A router restart breaks every open Hrana stream.** Clients see `STREAM_EXPIRED` on their next request and reopen; open transactions roll back on the nodes when their sessions end.

## Progress

- Not started.
