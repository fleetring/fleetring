//! FR-085 in the adapters: a fork's reset, one conditional transaction that clears the change
//! record and the sink marks a fork inherits and leaves a marker naming the fork (ADR-0046,
//! point 6).

use std::num::NonZeroUsize;
use std::path::Path;

use fleetringd::engine::{
    CaptureMode, Database, DatabaseCapability, DatabaseEngine, EngineCapability, EngineError,
    ForkMarker, ForkReset, Interrupt, MemoryEngine, OpenMode, Position, ReadLimit, SqliteEngine,
    Statement, Transaction, Value,
};

use crate::common::{PAGE_SIZE, TestResult};

fn marker(fork_incarnation: u64, operation: u8) -> ForkMarker {
    ForkMarker {
        fork_incarnation,
        operation: [operation; 16],
        // Above `i64::MAX`, so the adapter's storage of the full 64 bits is checked too.
        source_incarnation: u64::MAX - u64::from(operation),
        at: Position::new([0, 0, 0, operation]),
    }
}

fn sqlite_database(dir: &Path) -> Result<Box<dyn Database>, EngineError> {
    let engine = SqliteEngine::new(PAGE_SIZE).expect("a valid page size");
    let mut db = engine.open(dir, OpenMode::Create)?;
    exec(&mut *db, "CREATE TABLE t (v)")?;
    exec(&mut *db, "INSERT INTO t VALUES (1)")?;
    Ok(db)
}

fn exec(db: &mut dyn Database, sql: &str) -> Result<(), EngineError> {
    db.execute(&Transaction::from(Statement::new(sql)), &Interrupt::new())
        .map(drop)
}

/// The position just past the log's last transaction.
fn position(db: &mut dyn Database) -> Result<Option<Position>, EngineError> {
    match db.capability() {
        DatabaseCapability::FrameSource(frames) => frames.position(),
        DatabaseCapability::SelfReplicating(engine) => engine.position(),
        _ => Err(EngineError::Unsupported {
            what: "this test's replication shapes",
        }),
    }
}

/// Every recorded transaction after `after` up to the log's end.
fn recorded_after(
    db: &mut dyn Database,
    after: Option<&Position>,
) -> Result<Vec<fleetringd::engine::RecordedTransaction>, EngineError> {
    let end = position(db)?.expect("a log with transactions");
    let limit = ReadLimit::new(NonZeroUsize::MAX, NonZeroUsize::MAX);
    db.logical_changes()
        .expect("SQLite records changes")
        .transactions_after(after, &end, limit)
}

/// A reset reports the position of its own transaction, and the marker reads back.
fn reset_writes(db: &mut dyn Database, marker: &ForkMarker) -> TestResult {
    let before = position(db)?;
    let ForkReset::Written { position: written } = db.fork_reset(marker)? else {
        return Err("the first reset should write".into());
    };
    assert!(
        Some(&written) > before.as_ref(),
        "the reset is a new transaction"
    );
    assert_eq!(
        position(db)?.as_ref(),
        Some(&written),
        "the log ends with it"
    );
    assert_eq!(db.fork_marker()?.as_ref(), Some(marker));
    Ok(())
}

#[test]
fn a_reset_runs_once_and_a_second_call_writes_nothing() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = sqlite_database(dir.path())?;
    assert_eq!(db.fork_marker()?, None, "a database that never forked");
    let marker = marker(7, 1);
    reset_writes(&mut *db, &marker)?;
    let after = position(&mut *db)?;
    assert_eq!(db.fork_reset(&marker)?, ForkReset::AlreadyDone);
    assert_eq!(position(&mut *db)?, after, "the second call wrote nothing");
    // Only the identity decides: a marker for the same fork with other provenance is the same.
    let same_fork = ForkMarker {
        at: Position::new([9]),
        ..marker.clone()
    };
    assert_eq!(db.fork_reset(&same_fork)?, ForkReset::AlreadyDone);
    assert_eq!(db.fork_marker()?, Some(marker.clone()));

    // The marker is in the database, so it survives closing and reopening.
    db.close()?;
    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let mut reopened = engine.open(dir.path(), OpenMode::Existing)?;
    assert_eq!(reopened.fork_marker()?, Some(marker.clone()));
    assert_eq!(reopened.fork_reset(&marker)?, ForkReset::AlreadyDone);
    Ok(())
}

#[test]
fn a_reset_clears_the_inherited_change_record_and_sink_marks() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = sqlite_database(dir.path())?;
    db.logical_changes()
        .ok_or("SQLite records changes")?
        .start_recording(CaptureMode::After)?;
    let recording_from = position(&mut *db)?;
    exec(&mut *db, "INSERT INTO t VALUES (2)")?;
    let delivered = position(&mut *db)?.ok_or("a position")?;
    db.sink_marks()
        .ok_or("SQLite keeps sink marks")?
        .stage("audit", &delivered);
    // The next write records the staged mark.
    exec(&mut *db, "INSERT INTO t VALUES (3)")?;
    assert_eq!(
        db.sink_marks().ok_or("marks")?.marks()?.len(),
        1,
        "the source's sink mark is recorded"
    );
    assert_eq!(recorded_after(&mut *db, recording_from.as_ref())?.len(), 2);
    // A mark staged but not yet recorded goes too.
    db.sink_marks().ok_or("marks")?.stage("audit", &delivered);

    let reset_from = position(&mut *db)?.ok_or("a position")?;
    reset_writes(&mut *db, &marker(1, 1))?;
    let reset_to = position(&mut *db)?.ok_or("a position")?;

    assert!(
        db.sink_marks().ok_or("marks")?.marks()?.is_empty(),
        "the source's sink marks are gone"
    );
    // The change record holds nothing from before the reset, and says so.
    match recorded_after(&mut *db, recording_from.as_ref()) {
        Err(EngineError::ChangesReleased { oldest, .. }) => {
            assert_eq!(oldest, Some(reset_from.clone()), "held from the reset on");
        }
        other => return Err(format!("expected the history to be gone, got {other:?}").into()),
    }
    // The reset itself recorded nothing.
    assert!(recorded_after(&mut *db, Some(&reset_from))?.is_empty());

    // The fork's own writes are recorded from the reset on, and the staged mark stayed gone.
    exec(&mut *db, "INSERT INTO t VALUES (4)")?;
    let recorded = recorded_after(&mut *db, Some(&reset_from))?;
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].start.as_ref(), Some(&reset_to));
    assert_eq!(
        recorded[0].changes[0].values()[0].value,
        Value::Integer(4),
        "{recorded:?}"
    );
    assert!(db.sink_marks().ok_or("marks")?.marks()?.is_empty());
    Ok(())
}

#[test]
fn a_reset_on_a_database_with_no_change_record_or_marks_still_writes_its_marker() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = sqlite_database(dir.path())?;
    reset_writes(&mut *db, &marker(3, 3))?;
    assert!(db.sink_marks().ok_or("marks")?.marks()?.is_empty());
    Ok(())
}

#[test]
fn a_fork_of_a_fork_replaces_the_marker_it_inherits() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut source = sqlite_database(&dir.path().join("source"))?;
    let first = marker(10, 1);
    reset_writes(&mut *source, &first)?;
    exec(&mut *source, "INSERT INTO t VALUES (2)")?;

    // The fork of the fork starts as a copy of the first fork, its marker included.
    let copy = dir.path().join("copy");
    source.snapshot(&copy)?;
    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let mut fork = engine.open(&copy, OpenMode::Existing)?;
    assert_eq!(fork.fork_marker()?, Some(first.clone()), "inherited");

    let second = marker(20, 2);
    reset_writes(&mut *fork, &second)?;
    assert_eq!(fork.fork_reset(&second)?, ForkReset::AlreadyDone);
    // Running the first fork's reset again on the copy would be a different fork's: it runs.
    assert!(matches!(
        fork.fork_reset(&first)?,
        ForkReset::Written { .. }
    ));
    assert_eq!(
        fork.fork_marker()?,
        Some(first),
        "one row, the last reset's"
    );
    // The source's own marker is untouched.
    assert_eq!(source.fork_marker()?, Some(marker(10, 1)));
    Ok(())
}

#[test]
fn user_statements_cannot_touch_the_marker_table() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = sqlite_database(dir.path())?;
    reset_writes(&mut *db, &marker(5, 5))?;
    let reserved = "names beginning with `_fleetring_` are reserved for fleetringd";
    for sql in [
        "SELECT * FROM _fleetring_fork",
        "INSERT INTO _fleetring_fork VALUES (1, x'00', 1, x'00')",
        "UPDATE _fleetring_fork SET at = x'00'",
        "DELETE FROM _fleetring_fork",
        "DROP TABLE _fleetring_fork",
        "ALTER TABLE _fleetring_fork RENAME TO stolen",
        "CREATE TRIGGER spy AFTER INSERT ON _fleetring_fork BEGIN SELECT 1; END",
        "CREATE INDEX _fleetring_fork_idx ON t (v)",
        "SELECT * FROM _FLEETRING_FORK",
    ] {
        match exec(&mut *db, sql) {
            Err(EngineError::Statement { message }) => {
                assert_eq!(message, reserved, "{sql}");
            }
            other => return Err(format!("`{sql}` should be refused, got {other:?}").into()),
        }
    }
    // The adapter's own read, word for word, which reading the marker has just prepared.
    db.fork_marker()?;
    for sql in [
        "SELECT count(*) FROM _fleetring_fork",
        "SELECT fork_incarnation, operation, source_incarnation, at FROM _fleetring_fork \
         ORDER BY fork_incarnation, operation LIMIT 2",
    ] {
        match db.query(&Statement::new(sql), &Interrupt::new()) {
            Err(EngineError::Statement { message }) => assert_eq!(message, reserved, "{sql}"),
            other => return Err(format!("`{sql}` should be refused, got {other:?}").into()),
        }
    }
    // A user table can't be renamed into the marker's place either.
    exec(&mut *db, "CREATE TABLE decoy (v)")?;
    assert!(matches!(
        exec(&mut *db, "ALTER TABLE decoy RENAME TO _fleetring_fork"),
        Err(EngineError::Statement { .. })
    ));
    assert_eq!(
        db.fork_marker()?,
        Some(marker(5, 5)),
        "the marker is intact"
    );

    // The marker table is the adapter's own, so it doesn't stop change recording or sink marks.
    db.logical_changes()
        .ok_or("changes")?
        .start_recording(CaptureMode::Id)?;
    exec(&mut *db, "INSERT INTO t VALUES (2)")?;
    db.sink_marks().ok_or("marks")?.marks()?;
    Ok(())
}

#[test]
fn the_reset_replicates_through_the_log_like_any_write() -> TestResult {
    let dir = tempfile::tempdir()?;
    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let mut primary = sqlite_database(&dir.path().join("primary"))?;
    let follower_dir = dir.path().join("follower");
    let seeded = primary.snapshot(&follower_dir)?;
    let marker = marker(42, 9);
    reset_writes(&mut *primary, &marker)?;

    let EngineCapability::FrameSource(frames) = engine.capability() else {
        return Err("SQLite is a frame source".into());
    };
    let mut follower = frames.open_follower(&follower_dir)?;
    let DatabaseCapability::FrameSource(source) = primary.capability() else {
        return Err("SQLite is a frame source".into());
    };
    let batch = source
        .committed_since(seeded.position.as_ref())?
        .ok_or("the reset shipped")?;
    let applied = follower.apply(&batch)?;
    assert_eq!(Some(&applied), position(&mut *primary)?.as_ref());
    drop(follower);

    // Promoted, the follower finds the marker in its own copy and doesn't reset again.
    let mut promoted = engine.open(&follower_dir, OpenMode::Existing)?;
    assert_eq!(promoted.fork_marker()?, Some(marker.clone()));
    assert_eq!(promoted.fork_reset(&marker)?, ForkReset::AlreadyDone);
    Ok(())
}

#[test]
fn the_memory_engine_keeps_the_marker_with_the_same_conditions() -> TestResult {
    let engine = MemoryEngine::new();
    let mut db = engine.open(Path::new("source"), OpenMode::Create)?;
    db.execute(
        &Transaction::from(Statement::with_params(
            "put",
            [Value::from("k"), Value::from(1)],
        )),
        &Interrupt::new(),
    )?;
    assert_eq!(db.fork_marker()?, None);
    let first = marker(1, 1);
    reset_writes(&mut *db, &first)?;
    let after = position(&mut *db)?;
    assert_eq!(db.fork_reset(&first)?, ForkReset::AlreadyDone);
    assert_eq!(position(&mut *db)?, after);

    // A copy inherits the marker; another fork's reset replaces it.
    db.snapshot(Path::new("copy"))?;
    let mut copy = engine.open(Path::new("copy"), OpenMode::Existing)?;
    assert_eq!(copy.fork_marker()?, Some(first.clone()));
    let second = marker(2, 2);
    reset_writes(&mut *copy, &second)?;
    assert_eq!(copy.fork_reset(&second)?, ForkReset::AlreadyDone);
    assert_eq!(db.fork_marker()?, Some(first), "the source keeps its own");
    Ok(())
}
