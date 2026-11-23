//! SQL sessions (ADR-0037): SQLite's own transaction semantics in the engine, and the actor that
//! lets one session's transaction hold a database's writer.

use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::engine::{
    Autocommit, BatchCondition, BatchStep, CallOutcome, CallResult, CaptureMode, Database,
    DatabaseCapability, DatabaseEngine, EngineCapability, EngineError, Execution, Interrupt,
    Interruption, OpenMode, PERMISSION_DENIED, Position, ReadLimit, SAVEPOINT_OUTSIDE_TRANSACTION,
    SessionAccess, SessionCall, SessionStatement, SqliteEngine, Statement, StatementOutcome,
    StepOutcome, Transaction, Value, WantRows, WriteIntent,
};
use fleetringd::{
    Acknowledgement, BatchPublisher, CommittedBatch, DatabaseError, RecordingPublisher, Registry,
    ReplicationError, SessionCalled, SessionId, SessionLimits, SessionTurn, StatementLimit,
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
fn temporary_objects_are_refused_in_sessions_and_execute() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    run(db.as_mut(), &execute("CREATE TABLE t (v)"), rw);
    for sql in [
        "CREATE TEMP TABLE t (v)",
        "CREATE TEMPORARY TABLE scratch AS SELECT 1",
        "CREATE TEMP VIEW peek AS SELECT v FROM main.t",
        "CREATE TEMP TRIGGER tr AFTER INSERT ON main.t BEGIN SELECT 1; END",
    ] {
        let refused = run(db.as_mut(), &execute(sql), rw);
        assert_eq!(failed(&refused), "SQLITE_AUTH", "{sql}");
        // Inside a transaction too, and the transaction goes on.
        run(db.as_mut(), &execute("BEGIN"), rw);
        let refused = run(db.as_mut(), &execute(sql), rw);
        assert_eq!(failed(&refused), "SQLITE_AUTH", "{sql}");
        run(db.as_mut(), &execute("ROLLBACK"), rw);
        // And through `Execute`.
        let error = db
            .execute(&Transaction::from(Statement::new(sql)), &Interrupt::new())
            .expect_err("a temporary object");
        assert!(error.to_string().contains("temporary"), "{sql}: {error}");
    }
    // Every write still lands in the main table, on the log.
    run(db.as_mut(), &execute("INSERT INTO t VALUES (1)"), rw);
    assert_eq!(
        column(db.as_mut(), "SELECT v FROM main.t"),
        [Value::Integer(1)]
    );
    assert!(column(db.as_mut(), "SELECT name FROM temp.sqlite_master").is_empty());
    Ok(())
}

/// The statement messages of a call that failed.
fn message(result: &CallResult) -> &str {
    match &result.outcome {
        CallOutcome::Statement(Err(error)) => &error.message,
        other => panic!("expected a statement that failed: {other:?}"),
    }
}

#[test]
fn schema_checks_still_run_after_a_savepoint_is_rolled_back() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = open(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    for sql in [
        "CREATE TABLE t (v)",
        "CREATE TABLE plain (v)",
        "CREATE VIRTUAL TABLE docs USING vec0(e float[2], kind TEXT)",
        "INSERT INTO docs (rowid, e, kind) VALUES (1, '[1, 1]', 'a kind longer than sixteen bytes')",
    ] {
        ok(&run(db.as_mut(), &execute(sql), rw));
    }
    let names =
        |db: &mut dyn Database| column(db, "SELECT name FROM main.sqlite_master ORDER BY name");
    let before = names(db.as_mut());
    // `ROLLBACK TO` takes the schema's version back to the savepoint's, so the next schema change
    // brings it to a version the checks already read; they must still look.
    for (change, reason) in [
        ("ALTER TABLE docs RENAME TO moved", "can't be renamed"),
        ("ALTER TABLE plain RENAME TO _fleetring_evil", "reserved"),
    ] {
        for sql in [
            "BEGIN",
            "INSERT INTO t VALUES (50)",
            "SAVEPOINT s",
            "CREATE TABLE junk (x)",
            "ROLLBACK TO s",
        ] {
            ok(&run(db.as_mut(), &execute(sql), rw));
        }
        let refused = run(db.as_mut(), &execute(change), rw);
        assert_eq!(failed(&refused), "SQLITE_AUTH", "{change}");
        assert!(message(&refused).contains(reason), "{change}: {refused:?}");
        assert_eq!(refused.autocommit, Autocommit::On, "{change}: rolled back");
        // The same through `Execute`, which checks after every statement.
        let error = db
            .execute(
                &Transaction::new([
                    Statement::new("INSERT INTO t VALUES (51)"),
                    Statement::new("SAVEPOINT s"),
                    Statement::new("CREATE TABLE junk (x)"),
                    Statement::new("ROLLBACK TO s"),
                    Statement::new(change),
                ]),
                &Interrupt::new(),
            )
            .expect_err("the schema change is refused");
        assert!(error.to_string().contains(reason), "{change}: {error}");
    }
    assert_eq!(names(db.as_mut()), before);
    assert!(column(db.as_mut(), "SELECT v FROM t").is_empty());
    // A savepoint's schema change that stays is checked as usual, and so is one after a
    // `RELEASE`.
    for sql in ["BEGIN", "SAVEPOINT s", "CREATE TABLE kept (x)", "RELEASE s"] {
        ok(&run(db.as_mut(), &execute(sql), rw));
    }
    let refused = run(
        db.as_mut(),
        &execute("ALTER TABLE docs RENAME TO moved"),
        rw,
    );
    assert_eq!(failed(&refused), "SQLITE_AUTH");
    let kind = column(db.as_mut(), "SELECT kind FROM docs WHERE rowid = 1");
    assert_eq!(kind, [Value::from("a kind longer than sixteen bytes")]);
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

/// A database with the table `t (id, a, b)`, whose row changes are recorded in full.
fn recorded(dir: &Path) -> Result<Box<dyn Database>, Box<dyn std::error::Error>> {
    let mut db = open(dir)?;
    db.execute(
        &Transaction::from(Statement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a, b)",
        )),
        &Interrupt::new(),
    )?;
    db.logical_changes()
        .expect("sqlite records changes")
        .start_recording(CaptureMode::Full)?;
    Ok(db)
}

/// Where the log ends.
fn log_end(db: &mut dyn Database) -> Option<Position> {
    let DatabaseCapability::FrameSource(frames) = db.capability() else {
        panic!("sqlite is a frame source");
    };
    frames.position().expect("readable log")
}

/// The ID and the column names of every row recorded after `after`, in order. A delete records
/// its old row, and anything else its new one.
fn recorded_rows(db: &mut dyn Database, after: Option<&Position>) -> Vec<(i64, Vec<String>)> {
    let end = log_end(db).expect("a log");
    db.logical_changes()
        .expect("sqlite records changes")
        .changes_between(after, &end)
        .expect("readable changes")
        .into_iter()
        .map(|change| {
            let row = change.after.or(change.before).expect("full capture");
            let id = row
                .iter()
                .find(|column| column.name == "id")
                .map(|column| match column.value {
                    Value::Integer(id) => id,
                    _ => panic!("an integer id: {:?}", column.value),
                })
                .expect("a row has its id");
            (id, row.into_iter().map(|column| column.name).collect())
        })
        .collect()
}

fn names(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|&column| column.to_owned()).collect()
}

/// Runs `sql` in a session on `db`, which must succeed.
fn session_ok(db: &mut dyn Database, sql: &str) {
    ok(&run(db, &execute(sql), SessionAccess::ReadWrite));
}

/// Runs `statements` through `Execute` as one transaction.
fn execute_all(db: &mut dyn Database, statements: &[&str]) -> Result<(), EngineError> {
    db.execute(
        &Transaction::new(statements.iter().map(|&sql| Statement::new(sql))),
        &Interrupt::new(),
    )
    .map(|_| ())
}

/// One way to run a statement on the writer.
#[derive(Debug, Clone, Copy)]
enum StatementPath {
    Execute,
    Session,
    SessionTransaction,
    Batch,
    Sequence,
}

impl StatementPath {
    const ALL: [Self; 5] = [
        Self::Execute,
        Self::Session,
        Self::SessionTransaction,
        Self::Batch,
        Self::Sequence,
    ];

    fn run(self, db: &mut dyn Database, sql: &str) {
        let rw = SessionAccess::ReadWrite;
        match self {
            Self::Execute => execute_all(db, &[sql]).expect("the statement runs"),
            Self::Session => session_ok(db, sql),
            Self::SessionTransaction => {
                session_ok(db, "BEGIN");
                session_ok(db, sql);
                session_ok(db, "COMMIT");
            }
            Self::Batch => {
                let batch = SessionCall::Batch(vec![BatchStep {
                    condition: None,
                    statement: SessionStatement::new(sql),
                }]);
                let result = run(db, &batch, rw);
                let CallOutcome::Batch(steps) = &result.outcome else {
                    panic!("a batch's outcome: {result:?}");
                };
                assert!(matches!(steps[0], StepOutcome::Ok(_)), "{sql}: {steps:?}");
            }
            Self::Sequence => {
                let script = format!("BEGIN; {sql}; COMMIT;");
                let result = run(db, &SessionCall::Sequence(script), rw);
                assert_eq!(result.outcome, CallOutcome::Sequence(Ok(())), "{sql}");
            }
        }
    }
}

#[test]
fn every_alter_of_a_recorded_table_works_on_every_path() -> TestResult {
    for path in StatementPath::ALL {
        let dir = TempDir::new()?;
        let mut db = recorded(dir.path())?;
        // A comment, odd casing, and a schema name change nothing about what the statement is.
        let steps: [(&str, &[&str]); 3] = [
            ("ALTER TABLE t ADD COLUMN c", &["id", "a", "b", "c"]),
            (
                "/* first */ aLtEr /* x */ TaBlE t RENAME COLUMN c TO d",
                &["id", "a", "b", "d"],
            ),
            (
                "-- why\nALTER TABLE main.t DROP COLUMN a",
                &["id", "b", "d"],
            ),
        ];
        for (index, (alter, columns)) in (1_i64..).zip(steps) {
            path.run(db.as_mut(), alter);
            let before = log_end(db.as_mut());
            path.run(db.as_mut(), &format!("INSERT INTO t (id) VALUES ({index})"));
            path.run(
                db.as_mut(),
                &format!("UPDATE t SET b = 1 WHERE id = {index}"),
            );
            assert_eq!(
                recorded_rows(db.as_mut(), before.as_ref()),
                [(index, names(columns)), (index, names(columns))],
                "{path:?}, after {alter}"
            );
        }
    }
    Ok(())
}

#[test]
fn a_failed_alter_leaves_recording_as_it_was() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    // SQLite refuses this while it runs, after the statement was prepared.
    let drop_b = "ALTER TABLE t DROP COLUMN b";
    execute_all(db.as_mut(), &["CREATE VIEW v AS SELECT b FROM t"])?;
    let full = names(&["id", "a", "b"]);

    let before = log_end(db.as_mut());
    session_ok(db.as_mut(), "BEGIN");
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (1)");
    let refused = run(db.as_mut(), &execute(drop_b), rw);
    assert_eq!(failed(&refused), "SQLITE_ERROR", "{refused:?}");
    assert_eq!(refused.autocommit, Autocommit::InTransaction);
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (2)");
    session_ok(db.as_mut(), "COMMIT");
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(1, full.clone()), (2, full.clone())]
    );

    // Alone in a session, and through `Execute`, twice, so the second may come from a cache.
    for _ in 0..2 {
        let refused = run(db.as_mut(), &execute(drop_b), rw);
        assert_eq!(failed(&refused), "SQLITE_ERROR", "{refused:?}");
        execute_all(db.as_mut(), &[drop_b]).expect_err("a view reads the column");
    }
    let before = log_end(db.as_mut());
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (3)");
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(3, full.clone())]
    );

    // The same text, once it can succeed, still takes the triggers off.
    execute_all(db.as_mut(), &["DROP VIEW v"])?;
    execute_all(db.as_mut(), &[drop_b])?;
    let before = log_end(db.as_mut());
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (4)");
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(4, names(&["id", "a"]))]
    );
    Ok(())
}

#[test]
fn a_rollback_of_an_alter_restores_recording() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    for sql in [
        "BEGIN",
        "ALTER TABLE t DROP COLUMN b",
        "INSERT INTO t (id) VALUES (1)",
        "ROLLBACK",
    ] {
        session_ok(db.as_mut(), sql);
    }
    let before = log_end(db.as_mut());
    session_ok(db.as_mut(), "INSERT INTO t (id, b) VALUES (2, 'kept')");
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(2, names(&["id", "a", "b"]))]
    );
    Ok(())
}

#[test]
fn a_rollback_to_a_savepoint_keeps_recording_the_right_columns() -> TestResult {
    let script = [
        "INSERT INTO t (id) VALUES (1)",
        "SAVEPOINT s",
        "ALTER TABLE t DROP COLUMN b",
        "INSERT INTO t (id) VALUES (50)",
        "ROLLBACK TO s",
        "INSERT INTO t (id) VALUES (2)",
        // This change brings the schema's version back to the one the dropped column's had.
        "ALTER TABLE t ADD COLUMN c",
        "INSERT INTO t (id) VALUES (3)",
    ];
    let want = [
        (1, names(&["id", "a", "b"])),
        (2, names(&["id", "a", "b"])),
        (3, names(&["id", "a", "b", "c"])),
    ];

    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let before = log_end(db.as_mut());
    session_ok(db.as_mut(), "BEGIN");
    for sql in script {
        session_ok(db.as_mut(), sql);
    }
    session_ok(db.as_mut(), "COMMIT");
    assert_eq!(recorded_rows(db.as_mut(), before.as_ref()), want);

    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let before = log_end(db.as_mut());
    let sequence = format!("BEGIN; {}; COMMIT;", script.join("; "));
    let result = run(
        db.as_mut(),
        &SessionCall::Sequence(sequence),
        SessionAccess::ReadWrite,
    );
    assert_eq!(result.outcome, CallOutcome::Sequence(Ok(())));
    assert_eq!(recorded_rows(db.as_mut(), before.as_ref()), want);

    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let before = log_end(db.as_mut());
    execute_all(db.as_mut(), &script)?;
    assert_eq!(recorded_rows(db.as_mut(), before.as_ref()), want);
    Ok(())
}

#[test]
fn a_table_created_after_a_rollback_to_is_recorded() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let before = log_end(db.as_mut());
    // The second `CREATE TABLE` brings the schema's version back to the one the first reached, so
    // the triggers look current unless the `ROLLBACK TO` made the recorder look again.
    for sql in [
        "BEGIN",
        "SAVEPOINT s",
        "CREATE TABLE junk (id INTEGER PRIMARY KEY)",
        "ROLLBACK TO s",
        "CREATE TABLE u (id INTEGER PRIMARY KEY)",
        "INSERT INTO u VALUES (7)",
        "COMMIT",
    ] {
        session_ok(db.as_mut(), sql);
    }
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(7, names(&["id"]))]
    );

    // The same through `Execute`.
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let before = log_end(db.as_mut());
    execute_all(
        db.as_mut(),
        &[
            "SAVEPOINT s",
            "CREATE TABLE junk (id INTEGER PRIMARY KEY)",
            "ROLLBACK TO s",
            "CREATE TABLE u (id INTEGER PRIMARY KEY)",
            "INSERT INTO u VALUES (7)",
        ],
    )?;
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(7, names(&["id"]))]
    );
    Ok(())
}

#[test]
fn a_savepoint_before_the_first_write_keeps_what_recording_set_up() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let before = log_end(db.as_mut());
    for sql in [
        "BEGIN",
        "SAVEPOINT s",
        "INSERT INTO t (id) VALUES (1)",
        "ROLLBACK TO s",
        "INSERT INTO t (id) VALUES (2)",
        "COMMIT",
    ] {
        session_ok(db.as_mut(), sql);
    }
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(2, names(&["id", "a", "b"]))]
    );
    Ok(())
}

#[test]
fn an_alter_of_a_recorded_table_stays_confined() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    execute_all(
        db.as_mut(),
        &[
            "CREATE TABLE plain (v)",
            "CREATE VIRTUAL TABLE docs USING vec0(e float[2])",
        ],
    )?;
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (1)");
    let full = names(&["id", "a", "b"]);
    for (alter, reason) in [
        ("ALTER TABLE _fleetring_cdc DROP COLUMN updates", "reserved"),
        (
            "/* x */ aLtEr TaBlE _FLEETRING_cdc ADD COLUMN c",
            "reserved",
        ),
        ("ALTER TABLE plain RENAME TO _fleetring_evil", "reserved"),
        ("ALTER TABLE t RENAME TO _Fleetring_evil", "reserved"),
        ("-- x\nALTER TABLE docs RENAME TO moved", "can't be renamed"),
        ("ALTER TABLE plain RENAME TO docs_vector_chunks01", "shadow"),
    ] {
        let refused = run(db.as_mut(), &execute(alter), rw);
        assert_eq!(failed(&refused), "SQLITE_AUTH", "{alter}: {refused:?}");
        assert!(message(&refused).contains(reason), "{alter}: {refused:?}");

        session_ok(db.as_mut(), "BEGIN");
        session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (50)");
        let refused = run(db.as_mut(), &execute(alter), rw);
        assert_eq!(failed(&refused), "SQLITE_AUTH", "{alter}: {refused:?}");
        if refused.autocommit == Autocommit::InTransaction {
            // Refused as it was prepared, before anything ran: the transaction is still open.
            session_ok(db.as_mut(), "ROLLBACK");
        }

        let error = execute_all(db.as_mut(), &[alter]).expect_err("refused through Execute");
        assert!(error.to_string().contains(reason), "{alter}: {error}");

        let script = format!("SELECT 1; {alter}; INSERT INTO t (id) VALUES (51);");
        let result = run(db.as_mut(), &SessionCall::Sequence(script), rw);
        let CallOutcome::Sequence(Err(error)) = &result.outcome else {
            panic!("{alter}: refused in a sequence: {result:?}");
        };
        assert_eq!(error.code, "SQLITE_AUTH", "{alter}");

        // Nothing of it stayed, and recording goes on over the same columns.
        let before = log_end(db.as_mut());
        session_ok(db.as_mut(), "INSERT OR REPLACE INTO t (id) VALUES (2)");
        assert_eq!(
            recorded_rows(db.as_mut(), before.as_ref()).last(),
            Some(&(2, full.clone())),
            "{alter}"
        );
    }
    assert_eq!(
        column(db.as_mut(), "SELECT id FROM t ORDER BY id"),
        [Value::Integer(1), Value::Integer(2)]
    );

    // `EXPLAIN` of an `ALTER TABLE` changes nothing, and recording goes on.
    session_ok(db.as_mut(), "EXPLAIN ALTER TABLE t DROP COLUMN b");
    let before = log_end(db.as_mut());
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (3)");
    assert_eq!(recorded_rows(db.as_mut(), before.as_ref()), [(3, full)]);
    Ok(())
}

/// Stages `position` as the mark of the sink `s`.
fn stage(db: &mut dyn Database, position: &Position) {
    db.sink_marks()
        .expect("sqlite keeps marks")
        .stage("s", position);
}

/// The mark recorded for the sink `s`.
fn recorded_mark(db: &mut dyn Database) -> Option<Position> {
    db.sink_marks()
        .expect("sqlite keeps marks")
        .marks()
        .expect("readable marks")
        .remove("s")
}

#[test]
fn a_mark_staged_during_a_transaction_survives_its_commit() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let (first, second) = (Position::new([1]), Position::new([2]));
    stage(db.as_mut(), &first);
    session_ok(db.as_mut(), "BEGIN");
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (1)");
    stage(db.as_mut(), &second);
    session_ok(db.as_mut(), "COMMIT");
    assert_eq!(recorded_mark(db.as_mut()), Some(first));
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (2)");
    assert_eq!(recorded_mark(db.as_mut()), Some(second));
    Ok(())
}

#[test]
fn a_rollback_keeps_the_marks_staged_through_a_later_commit_that_wrote_nothing() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let mark = Position::new([1]);
    stage(db.as_mut(), &mark);
    for sql in [
        "BEGIN",
        "INSERT INTO t (id) VALUES (1)",
        "ROLLBACK",
        // Commits nothing, so it may not forget marks the rollback took back.
        "BEGIN",
        "COMMIT",
    ] {
        session_ok(db.as_mut(), sql);
    }
    assert_eq!(recorded_mark(db.as_mut()), None);
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (2)");
    assert_eq!(recorded_mark(db.as_mut()), Some(mark));
    Ok(())
}

#[test]
fn a_transaction_that_only_reads_under_a_savepoint_writes_nothing() -> TestResult {
    let scripts = [
        "BEGIN; SAVEPOINT s; SELECT count(*) FROM t; RELEASE s; COMMIT;",
        "BEGIN; SAVEPOINT s; SELECT count(*) FROM t; ROLLBACK TO s; COMMIT;",
    ];
    // With no change table yet, and with one.
    for writes_first in [false, true] {
        let dir = TempDir::new()?;
        let mut db = recorded(dir.path())?;
        if writes_first {
            session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (1)");
        }
        let mark = Position::new([7]);
        stage(db.as_mut(), &mark);
        let before = log_end(db.as_mut());
        for _ in 0..3 {
            for script in scripts {
                let result = run(
                    db.as_mut(),
                    &SessionCall::Sequence(script.to_owned()),
                    SessionAccess::ReadWrite,
                );
                assert_eq!(result.outcome, CallOutcome::Sequence(Ok(())), "{script}");
            }
            for sql in [
                "BEGIN",
                "SAVEPOINT s",
                "SELECT count(*) FROM t",
                "RELEASE s",
                "COMMIT",
            ] {
                session_ok(db.as_mut(), sql);
            }
            assert_eq!(log_end(db.as_mut()), before, "first write {writes_first}");
        }
        assert_eq!(recorded_mark(db.as_mut()), None, "still staged");
        // The next real write carries it, and records its row.
        session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (2)");
        assert_eq!(recorded_mark(db.as_mut()), Some(mark));
        assert_eq!(
            recorded_rows(db.as_mut(), before.as_ref()),
            [(2, names(&["id", "a", "b"]))]
        );
    }
    Ok(())
}

#[test]
fn a_write_undone_by_rollback_to_leaves_its_mark_staged() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let mark = Position::new([7]);
    stage(db.as_mut(), &mark);
    for sql in [
        "BEGIN",
        "SAVEPOINT s",
        "INSERT INTO t (id) VALUES (1)",
        "ROLLBACK TO s",
        "COMMIT",
    ] {
        session_ok(db.as_mut(), sql);
    }
    assert_eq!(
        recorded_mark(db.as_mut()),
        None,
        "the rollback took it back"
    );
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (2)");
    assert_eq!(recorded_mark(db.as_mut()), Some(mark));
    Ok(())
}

#[test]
fn a_first_write_undone_under_a_savepoint_leaves_the_record_readable() -> TestResult {
    for first in [
        "INSERT INTO t (id) VALUES (9)",
        "CREATE TABLE junk (id INTEGER PRIMARY KEY)",
    ] {
        let dir = TempDir::new()?;
        let mut db = recorded(dir.path())?;
        let before = log_end(db.as_mut());
        for sql in ["BEGIN", "SAVEPOINT s", first, "ROLLBACK TO s", "COMMIT"] {
            session_ok(db.as_mut(), sql);
        }
        let end = log_end(db.as_mut()).expect("a log");
        assert_ne!(
            Some(&end),
            before.as_ref(),
            "{first}: the commit wrote frames"
        );
        // A watcher at the old end reads nothing, and isn't told anything was released.
        let limit = ReadLimit::new(NonZeroUsize::MAX, NonZeroUsize::MAX);
        let read = db
            .logical_changes()
            .expect("sqlite records changes")
            .transactions_after(before.as_ref(), &end, limit)?;
        assert!(read.is_empty(), "{first}: {read:?}");
        // A resume from the old end still works once the next write has committed.
        session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (2)");
        let end = log_end(db.as_mut()).expect("a log");
        let read = db
            .logical_changes()
            .expect("sqlite records changes")
            .transactions_after(before.as_ref(), &end, limit)?;
        assert_eq!(read.len(), 1, "{first}: {read:?}");
        assert_eq!(read[0].changes.len(), 1, "{first}: {read:?}");
        assert_eq!(
            read[0].changes[0].key[0].value,
            Value::Integer(2),
            "{first}"
        );
    }
    Ok(())
}

#[test]
fn a_rollback_to_an_inner_savepoint_keeps_the_transaction_whole() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let before = log_end(db.as_mut());
    let mark = Position::new([7]);
    stage(db.as_mut(), &mark);
    for sql in [
        "BEGIN",
        "SAVEPOINT a",
        "INSERT INTO t (id) VALUES (1)",
        "SAVEPOINT b",
        "INSERT INTO t (id) VALUES (50)",
        "ROLLBACK TO b",
        "INSERT INTO t (id) VALUES (2)",
        "COMMIT",
        "BEGIN",
        "SAVEPOINT c",
        "INSERT INTO t (id) VALUES (3)",
        "SAVEPOINT d",
        "ROLLBACK TO d",
        "COMMIT",
        "INSERT INTO t (id) VALUES (4)",
    ] {
        session_ok(db.as_mut(), sql);
    }
    assert_eq!(recorded_mark(db.as_mut()), Some(mark));
    let end = log_end(db.as_mut()).expect("a log");
    let limit = ReadLimit::new(NonZeroUsize::MAX, NonZeroUsize::MAX);
    let transactions = db
        .logical_changes()
        .expect("sqlite records changes")
        .transactions_after(before.as_ref(), &end, limit)?;
    let ids: Vec<Vec<Value>> = transactions
        .iter()
        .map(|transaction| {
            transaction
                .changes
                .iter()
                .flat_map(|change| change.key.iter().map(|column| column.value.clone()))
                .collect()
        })
        .collect();
    assert_eq!(
        ids,
        [
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
        ]
    );
    Ok(())
}

#[test]
fn a_recorded_table_can_be_dropped_and_created_again_on_every_path() -> TestResult {
    for path in StatementPath::ALL {
        let dir = TempDir::new()?;
        let mut db = recorded(dir.path())?;
        // Twice, so that the same `DROP TABLE` text can come from the statement cache.
        for (round, column) in [(1, "c"), (2, "d")] {
            path.run(
                db.as_mut(),
                &format!("CREATE TABLE gone (id INTEGER PRIMARY KEY, {column})"),
            );
            path.run(db.as_mut(), "INSERT INTO gone (id) VALUES (1)");
            path.run(db.as_mut(), "DROP TABLE gone");
            path.run(
                db.as_mut(),
                &format!("CREATE TABLE gone (id INTEGER PRIMARY KEY, new_{column})"),
            );
            let before = log_end(db.as_mut());
            path.run(db.as_mut(), "INSERT INTO gone (id) VALUES (2)");
            path.run(db.as_mut(), &format!("INSERT INTO t (id) VALUES ({round})"));
            assert_eq!(
                recorded_rows(db.as_mut(), before.as_ref()),
                [
                    (2, names(&["id", &format!("new_{column}")])),
                    (round, names(&["id", "a", "b"])),
                ],
                "{path:?}"
            );
            path.run(db.as_mut(), "DROP TABLE gone");
        }
    }
    Ok(())
}

#[test]
fn dropping_a_table_opens_no_way_to_the_recorders_objects() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    let rw = SessionAccess::ReadWrite;
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (1)");
    session_ok(db.as_mut(), "CREATE TABLE gone (id INTEGER PRIMARY KEY)");
    for sql in [
        "DROP TRIGGER temp._fleetring_trg_ins_t",
        "DROP TRIGGER _fleetring_trg_del_t",
        "/* x */ dRoP tRiGgEr TEMP._FLEETRING_TRG_UPD_T",
        "DROP TABLE _fleetring_cdc",
        "DROP TABLE main._FLEETRING_CDC",
    ] {
        let refused = run(db.as_mut(), &execute(sql), rw);
        assert_eq!(failed(&refused), "SQLITE_AUTH", "{sql}: {refused:?}");
        let error = execute_all(db.as_mut(), &[sql]).expect_err("refused through Execute");
        assert!(error.to_string().contains("reserved"), "{sql}: {error}");
    }
    // In one transaction with a `DROP TABLE`, the drop of another table's trigger is refused.
    let drop_trigger = "DROP TRIGGER temp._fleetring_trg_ins_t";
    let error = execute_all(db.as_mut(), &["DROP TABLE gone", drop_trigger])
        .expect_err("refused through Execute");
    assert!(error.to_string().contains("reserved"), "{error}");
    let script = format!("BEGIN; DROP TABLE gone; {drop_trigger}; COMMIT;");
    let result = run(db.as_mut(), &SessionCall::Sequence(script), rw);
    let CallOutcome::Sequence(Err(error)) = &result.outcome else {
        panic!("refused in a sequence: {result:?}");
    };
    assert_eq!(error.code, "SQLITE_AUTH");
    session_ok(db.as_mut(), "ROLLBACK");
    assert_eq!(
        column(db.as_mut(), "SELECT count(*) FROM gone"),
        [Value::Integer(0)],
        "the table is still there"
    );
    let before = log_end(db.as_mut());
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (2)");
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(2, names(&["id", "a", "b"]))]
    );
    Ok(())
}

#[test]
fn a_rollback_of_a_drop_table_keeps_the_table_recorded() -> TestResult {
    let dir = TempDir::new()?;
    let mut db = recorded(dir.path())?;
    for script in [
        [
            "BEGIN",
            "INSERT INTO t (id) VALUES (10)",
            "DROP TABLE t",
            "ROLLBACK",
        ]
        .as_slice(),
        [
            "BEGIN",
            "INSERT INTO t (id) VALUES (20)",
            "SAVEPOINT s",
            "DROP TABLE t",
            "ROLLBACK TO s",
            "COMMIT",
        ]
        .as_slice(),
    ] {
        for sql in script {
            session_ok(db.as_mut(), sql);
        }
    }
    let before = log_end(db.as_mut());
    session_ok(db.as_mut(), "INSERT INTO t (id) VALUES (30)");
    assert_eq!(
        recorded_rows(db.as_mut(), before.as_ref()),
        [(30, names(&["id", "a", "b"]))]
    );
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
            SessionTurn::new(
                SessionId::new(session),
                "app",
                execute(sql),
                SessionAccess::ReadWrite,
                holding,
            ),
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
            SessionTurn::new(
                SessionId::new(1),
                "app",
                sequence,
                SessionAccess::ReadWrite,
                Autocommit::On,
            ),
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
            SessionTurn::new(
                SessionId::new(2),
                "app",
                execute("INSERT INTO t VALUES (2)"),
                SessionAccess::ReadOnly,
                Autocommit::On,
            ),
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

/// A call of session `session`, opened by `principal`, in autocommit, run in the background.
fn spawn_call(
    registry: &Registry,
    db: &DatabaseName,
    session: u64,
    principal: &str,
    sql: &str,
) -> tokio::task::JoinHandle<Result<SessionCalled, DatabaseError>> {
    let registry = registry.clone();
    let db = db.clone();
    let turn = SessionTurn::new(
        SessionId::new(session),
        principal,
        execute(sql),
        SessionAccess::ReadWrite,
        Autocommit::On,
    );
    tokio::spawn(async move { registry.session_call(&db, turn, None).await })
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_write_waiting_behind_a_transaction_gets_its_whole_statement_time() -> TestResult {
    let limits = SessionLimits {
        transaction_timeout: Duration::from_secs(1),
        ..SessionLimits::default()
    };
    let (registry, _publisher, _dir) = registry(limits);
    let db = name("patient");
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
    // 300 ms to run, counted once it starts, and up to 5 s to wait for its turn.
    let waiting = tokio::spawn({
        let registry = registry.clone();
        let db = db.clone();
        async move {
            let limit = StatementLimit {
                budget: Duration::from_millis(300),
                latest: tokio::time::Instant::now() + Duration::from_secs(5),
            };
            registry
                .execute_until(
                    &db,
                    Transaction::from(Statement::new("INSERT INTO t VALUES (2)")),
                    limit,
                )
                .await
        }
    });
    // The transaction runs for its whole second, then rolls back, and the write runs.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    waiting.await??;
    let rows = registry
        .query(&db, Statement::new("SELECT v FROM t"))
        .await?;
    assert_eq!(rows.rows, vec![vec![Value::Integer(2)]]);
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_write_waits_no_later_than_its_latest_moment() -> TestResult {
    let limits = SessionLimits {
        transaction_timeout: Duration::from_secs(10),
        ..SessionLimits::default()
    };
    let (registry, _publisher, _dir) = registry(limits);
    let db = name("impatient");
    create(&registry, &db).await?;
    call(&registry, &db, 1, "BEGIN", Autocommit::On).await?;
    let limit = StatementLimit {
        budget: Duration::from_secs(30),
        latest: tokio::time::Instant::now() + Duration::from_millis(500),
    };
    let waiting = tokio::spawn({
        let registry = registry.clone();
        let db = db.clone();
        async move {
            registry
                .execute_until(
                    &db,
                    Transaction::from(Statement::new("INSERT INTO t VALUES (2)")),
                    limit,
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(600)).await;
    call(&registry, &db, 1, "COMMIT", Autocommit::InTransaction).await?;
    let late = waiting
        .await?
        .expect_err("it waited past its latest moment");
    assert!(
        matches!(
            late.engine_error(),
            Some(fleetringd::engine::EngineError::Interrupted(
                Interruption::Deadline
            ))
        ),
        "{late:?}"
    );
    let rows = registry
        .query(&db, Statement::new("SELECT v FROM t"))
        .await?;
    assert!(rows.rows.is_empty(), "it never ran");
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn one_principal_takes_at_most_half_the_waiting_places() -> TestResult {
    let limits = SessionLimits {
        max_waiting_writes: 4,
        ..SessionLimits::default()
    };
    let (registry, _publisher, _dir) = registry(limits);
    let db = name("shared");
    create(&registry, &db).await?;
    call(&registry, &db, 1, "BEGIN", Autocommit::On).await?;
    let mut queued = Vec::new();
    for session in [2, 3] {
        queued.push(spawn_call(&registry, &db, session, "greedy", "BEGIN"));
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let refused = spawn_call(&registry, &db, 4, "greedy", "BEGIN")
        .await?
        .expect_err("past its share");
    assert!(matches!(refused, DatabaseError::Busy { .. }), "{refused:?}");
    // Another principal still finds a place.
    queued.push(spawn_call(
        &registry,
        &db,
        5,
        "polite",
        "INSERT INTO t VALUES (5)",
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(queued.iter().all(|call| !call.is_finished()), "all wait");
    call(&registry, &db, 1, "COMMIT", Autocommit::InTransaction).await?;
    // The first greedy session's BEGIN takes the writer; end it, and the rest run.
    tokio::time::sleep(Duration::from_millis(10)).await;
    for session in [2, 3] {
        registry.session_end(&db, SessionId::new(session)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for call in queued {
        call.await??;
    }
    let rows = registry
        .query(&db, Statement::new("SELECT v FROM t"))
        .await?;
    assert_eq!(rows.rows, vec![vec![Value::Integer(5)]]);
    Ok(())
}
