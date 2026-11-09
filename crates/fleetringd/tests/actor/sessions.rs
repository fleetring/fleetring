//! SQL sessions (ADR-0037): SQLite's own transaction semantics in the engine, and the actor that
//! lets one session's transaction hold a database's writer.

use std::path::Path;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::engine::{
    Autocommit, BatchCondition, BatchStep, CallOutcome, CallResult, CaptureMode, Database,
    DatabaseCapability, DatabaseEngine, EngineCapability, Execution, Interrupt, Interruption,
    OpenMode, PERMISSION_DENIED, SAVEPOINT_OUTSIDE_TRANSACTION, SessionAccess, SessionCall,
    SessionStatement, SqliteEngine, Statement, StatementOutcome, StepOutcome, Transaction, Value,
    WantRows, WriteIntent,
};
use fleetringd::{
    Acknowledgement, BatchPublisher, CommittedBatch, DatabaseError, RecordingPublisher, Registry,
    ReplicationError, SessionCalled, SessionId, SessionLimits,
};
use tempfile::TempDir;

use crate::common::{PAGE_SIZE, TestResult, config, name};

fn execute(sql: &str) -> SessionCall {
    SessionCall::Execute(SessionStatement::new(sql))
}

fn with_args(sql: &str, args: impl IntoIterator<Item = Value>) -> SessionStatement {
    SessionStatement {
        args: args.into_iter().collect(),
        ..SessionStatement::new(sql)
    }
}

/// An open SQLite database, created empty.
fn open(dir: &Path) -> Result<Box<dyn Database>, Box<dyn std::error::Error>> {
    let engine = SqliteEngine::new(PAGE_SIZE)?;
    Ok(engine.open(dir, OpenMode::Create)?)
}

/// Runs `call` on `db` with `access`.
fn run(db: &mut dyn Database, call: &SessionCall, access: SessionAccess) -> CallResult {
    db.sql_sessions()
        .expect("sqlite runs sessions")
        .run(call, access, &Interrupt::new())
        .expect("storage works")
}

/// The statement outcome of an `Execute` call, which must have succeeded.
fn ok(result: &CallResult) -> &StatementOutcome {
    match &result.outcome {
        CallOutcome::Statement(Ok(outcome)) => outcome,
        other => panic!("expected a statement that ran: {other:?}"),
    }
}

/// The error code of an `Execute` call, which must have failed.
fn failed(result: &CallResult) -> &str {
    match &result.outcome {
        CallOutcome::Statement(Err(error)) => &error.code,
        other => panic!("expected a statement that failed: {other:?}"),
    }
}

/// How many transactions the database's log holds.
fn transactions(db: &mut dyn Database) -> usize {
    let DatabaseCapability::FrameSource(frames) = db.capability() else {
        panic!("sqlite is a frame source");
    };
    let Some(batch) = frames.committed_since(None).expect("readable log") else {
        return 0;
    };
    let engine = SqliteEngine::new(PAGE_SIZE).expect("page size");
    let EngineCapability::FrameSource(source) = engine.capability() else {
        panic!("sqlite is a frame source");
    };
    source.transactions(&batch).expect("valid batch").len()
}

/// The values of `sql`'s first column, read with a plain query.
fn column(db: &mut dyn Database, sql: &str) -> Vec<Value> {
    db.query(&Statement::new(sql), &Interrupt::new())
        .expect("the query runs")
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect()
}

#[test]
fn a_session_statement_autocommits_and_reports_its_results() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    let created = run(
        db.as_mut(),
        &execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"),
        rw,
    );
    assert_eq!(ok(&created).affected_row_count, 0);
    let call = SessionCall::Execute(with_args(
        "INSERT INTO t (v) VALUES (?) RETURNING id",
        [Value::from("a")],
    ));
    let inserted = run(db.as_mut(), &call, rw);
    let outcome = ok(&inserted);
    assert_eq!(outcome.rows, vec![vec![Value::Integer(1)]]);
    assert_eq!(outcome.columns.len(), 1);
    assert_eq!(outcome.columns[0].name.as_deref(), Some("id"));
    assert_eq!(outcome.affected_row_count, 1);
    assert_eq!(outcome.last_insert_rowid, Some(1));
    assert_eq!(inserted.autocommit, Autocommit::On);
    assert_eq!(
        transactions(db.as_mut()),
        2,
        "each statement committed alone"
    );
    let selected = run(db.as_mut(), &execute("SELECT id, v FROM t"), rw);
    let read = ok(&selected);
    assert_eq!(read.rows, vec![vec![Value::Integer(1), Value::from("a")]]);
    assert_eq!(read.columns[1].decltype.as_deref(), Some("TEXT"));
    assert_eq!(read.last_insert_rowid, None, "a read inserts nothing");
    let quiet = SessionCall::Execute(SessionStatement {
        want_rows: WantRows::NoRows,
        ..SessionStatement::new("SELECT id FROM t")
    });
    assert!(ok(&run(db.as_mut(), &quiet, rw)).rows.is_empty());
    Ok(())
}

#[test]
fn a_transaction_spans_calls_and_commits_once() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    run(db.as_mut(), &execute("CREATE TABLE t (v)"), rw);
    let before = transactions(db.as_mut());
    let began = run(db.as_mut(), &execute("BEGIN"), rw);
    assert_eq!(began.autocommit, Autocommit::InTransaction);
    for value in [1, 2] {
        let call =
            SessionCall::Execute(with_args("INSERT INTO t VALUES (?)", [Value::from(value)]));
        let inserted = run(db.as_mut(), &call, rw);
        assert_eq!(inserted.autocommit, Autocommit::InTransaction);
        assert_eq!(transactions(db.as_mut()), before, "nothing commits yet");
    }
    // The transaction's own statements see its writes.
    let inside = run(db.as_mut(), &execute("SELECT count(*) FROM t"), rw);
    assert_eq!(ok(&inside).rows, vec![vec![Value::Integer(2)]]);
    let committed = run(db.as_mut(), &execute("COMMIT"), rw);
    assert_eq!(committed.autocommit, Autocommit::On);
    assert_eq!(transactions(db.as_mut()), before + 1, "one transaction");
    assert_eq!(
        column(db.as_mut(), "SELECT v FROM t ORDER BY v"),
        [Value::Integer(1), Value::Integer(2)]
    );
    Ok(())
}

#[test]
fn a_failed_batch_step_leaves_earlier_steps_committed() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    run(
        db.as_mut(),
        &execute("CREATE TABLE t (id INTEGER PRIMARY KEY)"),
        rw,
    );
    let step = |sql: &str, condition| BatchStep {
        condition,
        statement: SessionStatement::new(sql),
    };
    let batch = SessionCall::Batch(vec![
        step("INSERT INTO t VALUES (1)", None),
        step("INSERT INTO t VALUES (1)", None),
        step("INSERT INTO t VALUES (3)", Some(BatchCondition::Ok(1))),
        step(
            "INSERT INTO t VALUES (4)",
            Some(BatchCondition::Not(Box::new(BatchCondition::Ok(1)))),
        ),
    ]);
    let result = run(db.as_mut(), &batch, rw);
    let CallOutcome::Batch(steps) = &result.outcome else {
        panic!("a batch's outcome: {result:?}");
    };
    assert!(matches!(steps[0], StepOutcome::Ok(_)));
    let StepOutcome::Error(error) = &steps[1] else {
        panic!("the duplicate fails: {steps:?}");
    };
    assert_eq!(error.code, "SQLITE_CONSTRAINT_PRIMARYKEY");
    assert_eq!(steps[2], StepOutcome::Skipped);
    assert!(matches!(steps[3], StepOutcome::Ok(_)));
    assert_eq!(
        column(db.as_mut(), "SELECT id FROM t ORDER BY id"),
        [Value::Integer(1), Value::Integer(4)]
    );
    Ok(())
}

#[test]
fn a_rolled_back_transaction_leaves_nothing() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    run(db.as_mut(), &execute("CREATE TABLE t (v)"), rw);
    let before = transactions(db.as_mut());
    run(db.as_mut(), &execute("BEGIN"), rw);
    run(db.as_mut(), &execute("INSERT INTO t VALUES (1)"), rw);
    let rolled_back = run(db.as_mut(), &execute("ROLLBACK"), rw);
    assert_eq!(rolled_back.autocommit, Autocommit::On);
    assert!(column(db.as_mut(), "SELECT v FROM t").is_empty());
    assert_eq!(transactions(db.as_mut()), before);
    // The session goes on, and its next write commits.
    run(db.as_mut(), &execute("INSERT INTO t VALUES (2)"), rw);
    assert_eq!(column(db.as_mut(), "SELECT v FROM t"), [Value::Integer(2)]);
    Ok(())
}

#[test]
fn a_read_only_session_refuses_writes_and_transaction_control() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    run(
        db.as_mut(),
        &execute("CREATE TABLE t (v)"),
        SessionAccess::ReadWrite,
    );
    run(
        db.as_mut(),
        &execute("INSERT INTO t VALUES (1)"),
        SessionAccess::ReadWrite,
    );
    let ro = SessionAccess::ReadOnly;
    let read = run(db.as_mut(), &execute("SELECT v FROM t"), ro);
    assert_eq!(ok(&read).rows, vec![vec![Value::Integer(1)]]);
    for sql in [
        "INSERT INTO t VALUES (2)",
        "BEGIN",
        "CREATE TABLE u (v)",
        "SAVEPOINT a",
    ] {
        let refused = run(db.as_mut(), &execute(sql), ro);
        assert_eq!(failed(&refused), PERMISSION_DENIED, "{sql}");
        assert_eq!(refused.autocommit, Autocommit::On);
    }
    let sequence = run(
        db.as_mut(),
        &SessionCall::Sequence("SELECT 1; DELETE FROM t;".to_owned()),
        ro,
    );
    let CallOutcome::Sequence(Err(error)) = &sequence.outcome else {
        panic!("the delete is refused: {sequence:?}");
    };
    assert_eq!(error.code, PERMISSION_DENIED);
    assert!(error.message.contains("write"), "{}", error.message);
    assert_eq!(column(db.as_mut(), "SELECT v FROM t"), [Value::Integer(1)]);
    Ok(())
}

#[test]
fn a_savepoint_outside_a_transaction_is_refused() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    run(db.as_mut(), &execute("CREATE TABLE t (v)"), rw);
    let refused = run(db.as_mut(), &execute("SAVEPOINT a"), rw);
    assert_eq!(failed(&refused), SAVEPOINT_OUTSIDE_TRANSACTION);
    assert_eq!(refused.autocommit, Autocommit::On);
    let script = "BEGIN; INSERT INTO t VALUES (1); SAVEPOINT a; INSERT INTO t VALUES (2); \
                  ROLLBACK TO a; RELEASE a; COMMIT;";
    let sequence = run(db.as_mut(), &SessionCall::Sequence(script.to_owned()), rw);
    assert_eq!(sequence.outcome, CallOutcome::Sequence(Ok(())));
    assert_eq!(sequence.autocommit, Autocommit::On);
    assert_eq!(column(db.as_mut(), "SELECT v FROM t"), [Value::Integer(1)]);
    Ok(())
}

#[test]
fn session_statements_stay_confined() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let other = dir.path().join("other.db");
    let attach = format!("ATTACH DATABASE '{}' AS other", other.display());
    for sql in [
        attach.as_str(),
        "PRAGMA journal_mode = DELETE",
        "CREATE TABLE _fleetring_cdc (x)",
    ] {
        let refused = run(db.as_mut(), &execute(sql), SessionAccess::ReadWrite);
        assert_eq!(failed(&refused), "SQLITE_AUTH", "{sql}");
    }
    assert!(!other.exists(), "nothing was attached");
    Ok(())
}

#[test]
fn a_session_records_row_changes_as_execute_does() -> TestResult {
    let sessions_dir = TempDir::new()?;
    let execute_dir = TempDir::new()?;
    let mut via_session = open(sessions_dir.path())?;
    let mut via_execute = open(execute_dir.path())?;
    for db in [&mut via_session, &mut via_execute] {
        db.execute(
            &Transaction::from(Statement::new("CREATE TABLE t (id INTEGER PRIMARY KEY, v)")),
            &Interrupt::new(),
        )?;
        db.logical_changes()
            .expect("sqlite records changes")
            .start_recording(CaptureMode::Full)?;
    }
    let rw = SessionAccess::ReadWrite;
    run(via_session.as_mut(), &execute("BEGIN"), rw);
    run(
        via_session.as_mut(),
        &execute("INSERT INTO t VALUES (1, 'a')"),
        rw,
    );
    run(
        via_session.as_mut(),
        &execute("UPDATE t SET v = 'b' WHERE id = 1"),
        rw,
    );
    run(via_session.as_mut(), &execute("COMMIT"), rw);
    run(
        via_session.as_mut(),
        &execute("INSERT INTO t VALUES (2, 'c')"),
        rw,
    );
    via_execute.execute(
        &Transaction::new([
            Statement::new("INSERT INTO t VALUES (1, 'a')"),
            Statement::new("UPDATE t SET v = 'b' WHERE id = 1"),
        ]),
        &Interrupt::new(),
    )?;
    via_execute.execute(
        &Transaction::from(Statement::new("INSERT INTO t VALUES (2, 'c')")),
        &Interrupt::new(),
    )?;
    let mut recorded = Vec::new();
    for db in [&mut via_session, &mut via_execute] {
        let end = match db.capability() {
            DatabaseCapability::FrameSource(frames) => frames.position()?.expect("a log"),
            _ => panic!("sqlite is a frame source"),
        };
        let changes = db
            .logical_changes()
            .expect("sqlite records changes")
            .changes_between(None, &end)?;
        recorded.push(
            changes
                .into_iter()
                .map(|change| (change.table, change.operation, change.after))
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(recorded[0].len(), 3, "{:?}", recorded[0]);
    assert_eq!(recorded[0], recorded[1]);
    Ok(())
}

#[test]
fn describe_reports_parameters_and_columns() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    run(
        db.as_mut(),
        &execute("CREATE TABLE t (a INTEGER, b, c)"),
        rw,
    );
    let described = run(
        db.as_mut(),
        &SessionCall::Describe("SELECT a AS x FROM t WHERE b = :b AND c = ?".to_owned()),
        rw,
    );
    let CallOutcome::Described(Ok(description)) = &described.outcome else {
        panic!("a description: {described:?}");
    };
    assert_eq!(description.params, [Some(":b".to_owned()), None]);
    assert_eq!(description.columns.len(), 1);
    assert_eq!(description.columns[0].name.as_deref(), Some("x"));
    assert_eq!(description.columns[0].decltype.as_deref(), Some("INTEGER"));
    assert!(description.is_readonly);
    assert!(!description.is_explain);
    let explain = run(
        db.as_mut(),
        &SessionCall::Describe("EXPLAIN DELETE FROM t".to_owned()),
        rw,
    );
    let CallOutcome::Described(Ok(description)) = &explain.outcome else {
        panic!("a description: {explain:?}");
    };
    assert!(description.is_explain);
    Ok(())
}

#[test]
fn intent_says_may_write_without_running_anything() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    run(
        db.as_mut(),
        &execute("CREATE TABLE t (v)"),
        SessionAccess::ReadWrite,
    );
    let sessions = db.sql_sessions().expect("sqlite runs sessions");
    for (call, intent) in [
        (execute("SELECT v FROM t"), WriteIntent::ReadOnly),
        (execute("INSERT INTO t VALUES (1)"), WriteIntent::MayWrite),
        (execute("BEGIN"), WriteIntent::MayWrite),
        (execute("SELECT * FROM missing"), WriteIntent::MayWrite),
        (
            SessionCall::Sequence("SELECT 1; SELECT 2".to_owned()),
            WriteIntent::ReadOnly,
        ),
        (
            SessionCall::Sequence("SELECT 1; DELETE FROM t".to_owned()),
            WriteIntent::MayWrite,
        ),
        (
            SessionCall::Describe("DELETE FROM t".to_owned()),
            WriteIntent::ReadOnly,
        ),
        (SessionCall::GetAutocommit, WriteIntent::ReadOnly),
    ] {
        assert_eq!(sessions.intent(&call)?, intent, "{call:?}");
    }
    assert!(column(db.as_mut(), "SELECT v FROM t").is_empty());
    Ok(())
}

#[test]
fn arguments_bind_by_position_and_by_name_with_or_without_a_prefix() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    let statement = SessionStatement {
        args: vec![Value::Integer(1)],
        named_args: vec![
            ("b".to_owned(), Value::from("two")),
            ("@c".to_owned(), Value::Blob(vec![3])),
        ],
        ..SessionStatement::new("SELECT ?1, :b, @c")
    };
    let selected = run(db.as_mut(), &SessionCall::Execute(statement), rw);
    assert_eq!(
        ok(&selected).rows,
        vec![vec![
            Value::Integer(1),
            Value::from("two"),
            Value::Blob(vec![3])
        ]]
    );
    let unknown = SessionStatement {
        named_args: vec![("nope".to_owned(), Value::Null)],
        ..SessionStatement::new("SELECT :b")
    };
    let refused = run(db.as_mut(), &SessionCall::Execute(unknown), rw);
    assert_eq!(failed(&refused), "SQLITE_RANGE");
    let too_many = SessionCall::Execute(with_args("SELECT ?", [Value::Null, Value::Null]));
    assert_eq!(failed(&run(db.as_mut(), &too_many, rw)), "SQLITE_RANGE");
    Ok(())
}

#[test]
fn a_raised_interrupt_stops_session_statements() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let interrupt = Interrupt::new();
    interrupt.raise(Interruption::Deadline);
    let result = db.sql_sessions().expect("sqlite runs sessions").run(
        &execute("CREATE TABLE t (v)"),
        SessionAccess::ReadWrite,
        &interrupt,
    )?;
    assert_eq!(failed(&result), "DEADLINE_EXCEEDED");
    assert!(column(db.as_mut(), "SELECT name FROM sqlite_master").is_empty());
    Ok(())
}

/// A registry over SQLite with `limits`, running inline so that paused time stays exact.
fn registry(limits: SessionLimits) -> (Registry, RecordingPublisher, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let publisher = RecordingPublisher::new();
    let registry = Registry::new(
        SqliteEngine::new(PAGE_SIZE).expect("page size"),
        publisher.clone(),
        dir.path().join("databases"),
        config(Execution::Inline, 16, Duration::from_secs(3600)),
    )
    .with_sessions(limits);
    (registry, publisher, dir)
}

async fn create(registry: &Registry, db: &DatabaseName) -> TestResult {
    registry.create(db).await?;
    registry
        .execute(db, Transaction::from(Statement::new("CREATE TABLE t (v)")))
        .await?;
    Ok(())
}

async fn call(
    registry: &Registry,
    db: &DatabaseName,
    session: u64,
    sql: &str,
    holding: Autocommit,
) -> Result<SessionCalled, DatabaseError> {
    registry
        .session_call(
            db,
            SessionId::new(session),
            execute(sql),
            SessionAccess::ReadWrite,
            holding,
            None,
        )
        .await
}

fn count(publisher: &RecordingPublisher) -> usize {
    publisher.batches().len()
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn an_open_transaction_makes_other_writes_wait() -> TestResult {
    let (registry, publisher, _dir) = registry(SessionLimits::default());
    let db = name("waits");
    create(&registry, &db).await?;
    let before = count(&publisher);
    let began = call(&registry, &db, 1, "BEGIN", Autocommit::On).await?;
    assert_eq!(began.result.autocommit, Autocommit::InTransaction);
    call(
        &registry,
        &db,
        1,
        "INSERT INTO t VALUES (1)",
        Autocommit::InTransaction,
    )
    .await?;
    let waiting = tokio::spawn({
        let registry = registry.clone();
        let db = db.clone();
        async move {
            registry
                .execute(
                    &db,
                    Transaction::from(Statement::new("INSERT INTO t VALUES (2)")),
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !waiting.is_finished(),
        "the write waits for the transaction"
    );
    // Another session's write waits too.
    let other = tokio::spawn({
        let registry = registry.clone();
        let db = db.clone();
        async move {
            call(
                &registry,
                &db,
                2,
                "INSERT INTO t VALUES (3)",
                Autocommit::On,
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!other.is_finished(), "so does another session's");
    assert_eq!(count(&publisher), before, "nothing committed yet");
    let committed = call(&registry, &db, 1, "COMMIT", Autocommit::InTransaction).await?;
    assert_eq!(committed.result.autocommit, Autocommit::On);
    waiting.await??;
    other.await??;
    let batches = publisher.batches();
    assert_eq!(batches.len(), before + 3, "one batch per transaction");
    let rows = registry
        .query(&db, Statement::new("SELECT v FROM t ORDER BY rowid"))
        .await?;
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ],
        "the transaction first, then the writes in arrival order"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reads_go_on_while_a_transaction_is_open() -> TestResult {
    let (registry, _publisher, _dir) = registry(SessionLimits::default());
    let db = name("reads");
    create(&registry, &db).await?;
    registry
        .execute(
            &db,
            Transaction::from(Statement::new("INSERT INTO t VALUES (1)")),
        )
        .await?;
    call(&registry, &db, 1, "BEGIN", Autocommit::On).await?;
    call(
        &registry,
        &db,
        1,
        "INSERT INTO t VALUES (2)",
        Autocommit::InTransaction,
    )
    .await?;
    let rows = registry
        .query(&db, Statement::new("SELECT v FROM t"))
        .await?;
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1)]],
        "committed rows only"
    );
    // Another session's read runs now too, against the latest commit.
    let read = call(&registry, &db, 2, "SELECT count(*) FROM t", Autocommit::On).await?;
    assert_eq!(
        read.result.outcome,
        CallOutcome::Statement(Ok(StatementOutcome {
            columns: vec![fleetringd::engine::ResultColumn {
                name: Some("count(*)".to_owned()),
                decltype: None,
            }],
            rows: vec![vec![Value::Integer(1)]],
            ..StatementOutcome::default()
        }))
    );
    assert_eq!(read.result.autocommit, Autocommit::On);
    call(&registry, &db, 1, "COMMIT", Autocommit::InTransaction).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn too_many_waiting_writes_are_refused_busy() -> TestResult {
    let limits = SessionLimits {
        max_waiting_writes: 2,
        ..SessionLimits::default()
    };
    let (registry, _publisher, _dir) = registry(limits);
    let db = name("busy");
    create(&registry, &db).await?;
    call(&registry, &db, 1, "BEGIN", Autocommit::On).await?;
    let mut waiting = Vec::new();
    for value in 0..2 {
        let registry = registry.clone();
        let db = db.clone();
        waiting.push(tokio::spawn(async move {
            registry
                .execute(
                    &db,
                    Transaction::from(Statement::new(format!("INSERT INTO t VALUES ({value})"))),
                )
                .await
        }));
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let refused = registry
        .execute(
            &db,
            Transaction::from(Statement::new("INSERT INTO t VALUES (9)")),
        )
        .await
        .expect_err("a third write is refused");
    assert!(matches!(refused, DatabaseError::Busy { .. }), "{refused:?}");
    call(&registry, &db, 1, "COMMIT", Autocommit::InTransaction).await?;
    for write in waiting {
        write.await??;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_transaction_past_its_limit_rolls_back() -> TestResult {
    let limits = SessionLimits {
        transaction_timeout: Duration::from_secs(1),
        ..SessionLimits::default()
    };
    let (registry, _publisher, _dir) = registry(limits);
    let db = name("expires");
    create(&registry, &db).await?;
    call(&registry, &db, 1, "BEGIN", Autocommit::On).await?;
    call(
        &registry,
        &db,
        1,
        "INSERT INTO t VALUES (1)",
        Autocommit::InTransaction,
    )
    .await?;
    let waiting = tokio::spawn({
        let registry = registry.clone();
        let db = db.clone();
        async move {
            registry
                .execute(
                    &db,
                    Transaction::from(Statement::new("INSERT INTO t VALUES (2)")),
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1100)).await;
    waiting.await??;
    let late = call(
        &registry,
        &db,
        1,
        "INSERT INTO t VALUES (3)",
        Autocommit::InTransaction,
    )
    .await
    .expect_err("the transaction is gone");
    assert!(
        matches!(late, DatabaseError::TransactionTimedOut { .. }),
        "{late:?}"
    );
    let rows = registry
        .query(&db, Statement::new("SELECT v FROM t"))
        .await?;
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(2)]],
        "its insert is gone"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn ending_a_session_rolls_back_its_transaction() -> TestResult {
    let (registry, _publisher, _dir) = registry(SessionLimits::default());
    let db = name("ends");
    create(&registry, &db).await?;
    call(&registry, &db, 1, "BEGIN", Autocommit::On).await?;
    call(
        &registry,
        &db,
        1,
        "INSERT INTO t VALUES (1)",
        Autocommit::InTransaction,
    )
    .await?;
    registry.session_end(&db, SessionId::new(1)).await;
    registry
        .execute(
            &db,
            Transaction::from(Statement::new("INSERT INTO t VALUES (2)")),
        )
        .await?;
    let rows = registry
        .query(&db, Statement::new("SELECT v FROM t"))
        .await?;
    assert_eq!(rows.rows, vec![vec![Value::Integer(2)]]);
    // A session that still thinks it holds the transaction is told it doesn't.
    let lost = call(&registry, &db, 1, "COMMIT", Autocommit::InTransaction)
        .await
        .expect_err("the transaction is gone");
    assert!(
        matches!(lost, DatabaseError::TransactionLost { .. }),
        "{lost:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_call_that_commits_several_transactions_publishes_one_batch_each() -> TestResult {
    let (registry, publisher, _dir) = registry(SessionLimits::default());
    let db = name("several");
    create(&registry, &db).await?;
    let before = publisher.take();
    let mut last_end = before.last().map(|batch| batch.batch.end.clone());
    let sequence = SessionCall::Sequence(
        "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3);".to_owned(),
    );
    let called = registry
        .session_call(
            &db,
            SessionId::new(1),
            sequence,
            SessionAccess::ReadWrite,
            Autocommit::On,
            None,
        )
        .await?;
    assert_eq!(called.result.outcome, CallOutcome::Sequence(Ok(())));
    assert!(called.acknowledged.is_some());
    let batches = publisher.take();
    assert_eq!(batches.len(), 3, "one batch per autocommitted statement");
    for published in &batches {
        assert_eq!(
            Some(&published.batch.start),
            last_end.as_ref(),
            "batches chain"
        );
        last_end = Some(published.batch.end.clone());
    }
    Ok(())
}

/// A publisher that refuses every write, as one below its replica floor does.
#[derive(Debug, Clone, Default)]
struct BelowFloor(RecordingPublisher);

impl BatchPublisher for BelowFloor {
    fn admit(&self, _database: &DatabaseName) -> Result<(), ReplicationError> {
        Err(ReplicationError::BelowFloor {
            min_replicas: 2,
            reachable: 1,
        })
    }

    fn publish(&self, batch: CommittedBatch) -> Acknowledgement {
        self.0.publish(batch)
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_session_write_below_the_replica_floor_is_refused_before_it_runs() -> TestResult {
    let dir = TempDir::new()?;
    let databases = dir.path().join("databases");
    let db = name("floor");
    // Created through a registry that admits writes, then served by one that doesn't.
    let (setup, _publisher, setup_dir) = registry(SessionLimits::default());
    create(&setup, &db).await?;
    setup.close_all().await;
    std::fs::rename(setup_dir.path().join("databases"), &databases)?;
    let refusing = Registry::new(
        SqliteEngine::new(PAGE_SIZE)?,
        BelowFloor::default(),
        databases,
        config(Execution::Inline, 16, Duration::from_secs(3600)),
    );
    let refused = call(
        &refusing,
        &db,
        1,
        "INSERT INTO t VALUES (1)",
        Autocommit::On,
    )
    .await
    .expect_err("below the floor");
    assert!(
        matches!(refused, DatabaseError::Refused { .. }),
        "{refused:?}"
    );
    let read = call(&refusing, &db, 1, "SELECT count(*) FROM t", Autocommit::On).await?;
    let CallOutcome::Statement(Ok(outcome)) = read.result.outcome else {
        panic!("the read runs: {read:?}");
    };
    assert_eq!(outcome.rows, vec![vec![Value::Integer(0)]], "nothing ran");
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_read_only_session_never_waits_for_another_sessions_transaction() -> TestResult {
    let (registry, _publisher, _dir) = registry(SessionLimits::default());
    let db = name("readers");
    create(&registry, &db).await?;
    call(&registry, &db, 1, "BEGIN", Autocommit::On).await?;
    call(
        &registry,
        &db,
        1,
        "INSERT INTO t VALUES (1)",
        Autocommit::InTransaction,
    )
    .await?;
    // Its write is refused at once, not queued behind the transaction.
    let refused = tokio::time::timeout(
        Duration::from_millis(10),
        registry.session_call(
            &db,
            SessionId::new(2),
            execute("INSERT INTO t VALUES (2)"),
            SessionAccess::ReadOnly,
            Autocommit::On,
            None,
        ),
    )
    .await??;
    let CallOutcome::Statement(Err(error)) = &refused.result.outcome else {
        panic!("refused: {refused:?}");
    };
    assert_eq!(error.code, PERMISSION_DENIED);
    call(&registry, &db, 1, "COMMIT", Autocommit::InTransaction).await?;
    let rows = registry
        .query(&db, Statement::new("SELECT v FROM t"))
        .await?;
    assert_eq!(rows.rows, vec![vec![Value::Integer(1)]]);
    Ok(())
}
