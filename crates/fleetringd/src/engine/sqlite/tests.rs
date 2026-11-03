//! The confinement of user statements to the database they name.

use super::*;

type TestResult = Result<(), Box<dyn Error>>;

/// A database at `dir` holding table `t` with one row, `1`.
fn database(dir: &Path) -> Result<SqliteDatabase, Box<dyn Error>> {
    fs::create_dir_all(dir)?;
    let mut db = SqliteDatabase::open(
        dir.join(DATABASE_FILE),
        OpenMode::Create,
        4096,
        Arc::default(),
    )?;
    db.execute(&Transaction::new([
        Statement::new("CREATE TABLE t (v)"),
        Statement::new("INSERT INTO t VALUES (1)"),
    ]))?;
    Ok(db)
}

/// The message of a statement error, failing on any other outcome.
fn refusal<T: fmt::Debug>(result: Result<T, EngineError>) -> Result<String, Box<dyn Error>> {
    match result {
        Err(EngineError::Statement { message }) => Ok(message),
        other => Err(format!("expected a statement error, got {other:?}").into()),
    }
}

fn exec(db: &mut SqliteDatabase, sql: &str) -> Result<Executed, EngineError> {
    db.execute(&Transaction::from(Statement::new(sql)))
}

fn query(db: &mut SqliteDatabase, sql: &str) -> Result<Rows, EngineError> {
    db.query(&Statement::new(sql))
}

fn values(db: &mut SqliteDatabase) -> Result<Vec<Vec<Value>>, Box<dyn Error>> {
    Ok(query(db, "SELECT v FROM t ORDER BY v")?.rows)
}

#[test]
fn attaching_another_database_is_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(&dir.path().join("a"))?;
    let mut other = database(&dir.path().join("b"))?;
    let path = dir.path().join("b").join(DATABASE_FILE);
    let attach = format!("ATTACH DATABASE '{}' AS z", path.display());

    let message = refusal(exec(&mut db, &attach))?;
    assert!(message.contains("attach"), "{message}");
    refusal(query(&mut db, &attach))?;
    // A filename that isn't a literal reaches the authorizer without it.
    let bound = Statement::with_params(
        "ATTACH DATABASE ? AS z",
        [Value::from(path.display().to_string())],
    );
    refusal(db.execute(&Transaction::from(bound.clone())))?;
    refusal(db.query(&bound))?;
    let concatenated = format!("ATTACH DATABASE '{}' || '' AS z", path.display());
    refusal(exec(&mut db, &concatenated))?;

    // Nothing stayed attached, so nothing reaches the other database.
    refusal(query(&mut db, "SELECT v FROM z.t"))?;
    refusal(exec(&mut db, "INSERT INTO z.t VALUES (2)"))?;
    assert_eq!(values(&mut other)?, [[Value::Integer(1)]]);
    let listed = query(
        &mut db,
        "SELECT name FROM pragma_database_list ORDER BY seq",
    );
    refusal(listed)?;
    Ok(())
}

#[test]
fn detaching_is_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    let message = refusal(exec(&mut db, "DETACH DATABASE z"))?;
    assert!(message.contains("detach"), "{message}");
    refusal(query(&mut db, "DETACH DATABASE temp"))?;
    Ok(())
}

#[test]
fn vacuum_into_a_file_is_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(&dir.path().join("a"))?;
    let target = dir.path().join("copy.db");
    let vacuum = format!("VACUUM INTO '{}'", target.display());
    // `VACUUM INTO` attaches its target while it runs.
    let message = refusal(query(&mut db, &vacuum))?;
    assert!(message.contains("attach"), "{message}");
    refusal(exec(&mut db, &vacuum))?;
    assert!(!target.try_exists()?, "no file was written");
    Ok(())
}

#[test]
fn loading_an_extension_is_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    let message = refusal(query(&mut db, "SELECT load_extension('nope')"))?;
    assert_eq!(message, "statements can't load extensions");
    refusal(exec(&mut db, "SELECT LOAD_EXTENSION('nope', 'entry')"))?;
    // A trigger can't smuggle it in either: its body is authorized with the firing statement.
    exec(
        &mut db,
        "CREATE TRIGGER load AFTER INSERT ON t BEGIN SELECT load_extension('nope'); END",
    )?;
    refusal(exec(&mut db, "INSERT INTO t VALUES (2)"))?;
    assert_eq!(values(&mut db)?, [[Value::Integer(1)]]);
    Ok(())
}

#[test]
fn extension_loading_is_off_on_the_connections() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db = database(dir.path())?;
    // Outside a user statement the authorizer allows everything, so this is SQLite's own answer.
    for conn in [&db.writer, &db.reader] {
        let loaded: rusqlite::Result<i64> =
            conn.query_row("SELECT load_extension('nope')", [], |row| row.get(0));
        let error = loaded.expect_err("extension loading is disabled");
        assert!(error.to_string().contains("not authorized"), "{error}");
    }
    Ok(())
}

#[test]
fn pragmas_that_change_the_database_are_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    for sql in [
        "PRAGMA journal_mode = DELETE",
        "PRAGMA journal_mode",
        "PRAGMA writable_schema = 1",
        "PRAGMA wal_checkpoint",
        "PRAGMA wal_checkpoint(TRUNCATE)",
        "PRAGMA wal_autocheckpoint = 1",
        "PRAGMA synchronous = OFF",
        "PRAGMA locking_mode = EXCLUSIVE",
        "PRAGMA user_version = 5",
        "PRAGMA application_id = 5",
        "PRAGMA schema_version = 99",
        "PRAGMA page_size = 1024",
        "PRAGMA cache_size = 0",
        "PRAGMA cache_spill = 0",
        "PRAGMA trusted_schema = 1",
        "PRAGMA query_only = 0",
        "PRAGMA journal_size_limit = -1",
        "PRAGMA main.Journal_Mode = OFF",
    ] {
        let message =
            refusal(exec(&mut db, sql)).map_err(|error| format!("exec `{sql}`: {error}"))?;
        assert!(message.starts_with("pragma `"), "`{sql}`: {message}");
        refusal(query(&mut db, sql)).map_err(|error| format!("query `{sql}`: {error}"))?;
    }
    // Nor through a pragma's table-valued function.
    refusal(query(&mut db, "SELECT * FROM pragma_journal_mode"))?;
    refusal(exec(&mut db, "SELECT * FROM pragma_journal_mode"))?;

    let journal: String = db
        .writer
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    assert_eq!(journal, "wal");
    let query_only: i64 = db
        .reader
        .query_row("PRAGMA query_only", [], |row| row.get(0))?;
    assert_eq!(query_only, 1);
    let version: i64 = db
        .writer
        .query_row("PRAGMA user_version", [], |row| row.get(0))?;
    assert_eq!(version, 0);
    Ok(())
}

#[test]
fn introspection_pragmas_still_work() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    exec(&mut db, "CREATE INDEX t_v ON t (v)")?;
    for sql in [
        "PRAGMA table_info(t)",
        "PRAGMA table_xinfo('t')",
        "PRAGMA index_list(t)",
        "PRAGMA index_info(t_v)",
        "PRAGMA index_xinfo(t_v)",
        "PRAGMA foreign_key_list(t)",
        "PRAGMA foreign_key_check",
        "PRAGMA foreign_key_check(t)",
        "PRAGMA integrity_check",
        "PRAGMA quick_check(10)",
        "PRAGMA user_version",
        "PRAGMA application_id",
        "PRAGMA schema_version",
        "PRAGMA page_count",
        "PRAGMA PAGE_SIZE",
        "PRAGMA main.freelist_count",
        "PRAGMA compile_options",
        "SELECT name FROM pragma_table_info('t')",
    ] {
        query(&mut db, sql).map_err(|error| format!("query `{sql}`: {error}"))?;
        exec(&mut db, sql).map_err(|error| format!("exec `{sql}`: {error}"))?;
    }
    let columns = query(&mut db, "SELECT name FROM pragma_table_info('t')")?;
    assert_eq!(columns.rows, [[Value::from("v")]]);
    let size = query(&mut db, "PRAGMA page_size")?;
    assert_eq!(size.rows, [[Value::Integer(4096)]]);
    Ok(())
}

#[test]
fn reserved_names_are_still_refused_with_their_reason() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    let message = refusal(exec(&mut db, "CREATE TABLE _fleetring_x (v)"))?;
    assert_eq!(
        message,
        "names beginning with `_fleetring_` are reserved for fleetringd"
    );
    Ok(())
}

#[test]
fn the_adapters_own_operations_still_work() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    // A refusal leaves the connections usable.
    refusal(exec(&mut db, "PRAGMA journal_mode = DELETE"))?;
    refusal(query(&mut db, "PRAGMA wal_checkpoint"))?;

    db.start_recording(CaptureMode::Full)?;
    let start = db.position()?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    let end = db.position()?.ok_or("a position after a write")?;
    let changes = db.changes_between(start.as_ref(), &end)?;
    assert_eq!(changes.len(), 1, "{changes:?}");
    let batch = db
        .committed_since(start.as_ref())?
        .ok_or("a batch after a write")?;
    assert_eq!(batch.end, end);

    let marker = db.checkpoint()?;
    assert_eq!(marker.position, end);
    exec(&mut db, "INSERT INTO t VALUES (3)")?;
    assert!(
        db.committed_since(Some(&end))?.is_some(),
        "the next generation continues from the checkpoint"
    );
    let snapshot = db.snapshot(&dir.path().join("snapshot"))?;
    assert!(snapshot.position.is_some());
    assert_eq!(
        values(&mut db)?,
        [
            [Value::Integer(1)],
            [Value::Integer(2)],
            [Value::Integer(3)]
        ]
    );
    Box::new(db).close()?;

    let mut reopened = SqliteDatabase::open(
        dir.path().join(DATABASE_FILE),
        OpenMode::Existing,
        4096,
        Arc::default(),
    )?;
    assert_eq!(values(&mut reopened)?.len(), 3);
    Ok(())
}
