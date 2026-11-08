//! The confinement of user statements to the database they name, and the schema the adapter
//! reports.

use super::*;
use crate::engine::{Internal, MAX_SCHEMA_OBJECTS, ObjectKind, RowCount, SchemaObject};

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
        "PRAGMA data_version = 1",
        "PRAGMA foreign_key_check",
        "PRAGMA foreign_key_check(t)",
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
        "PRAGMA integrity_check",
        "PRAGMA quick_check(10)",
        "PRAGMA user_version",
        "PRAGMA application_id",
        "PRAGMA schema_version",
        "PRAGMA page_count",
        "PRAGMA PAGE_SIZE",
        "PRAGMA main.freelist_count",
        "PRAGMA data_version",
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

fn request(count_limit: u64, internal: Internal) -> SchemaRequest {
    SchemaRequest {
        count_limit,
        internal,
    }
}

/// `(kind, name)` of each object, in order.
fn listed(schema: &Schema) -> Vec<(ObjectKind, &str)> {
    schema
        .objects
        .iter()
        .map(|object| (object.kind, object.name.as_str()))
        .collect()
}

fn object<'a>(schema: &'a Schema, name: &str) -> Result<&'a SchemaObject, Box<dyn Error>> {
    schema
        .objects
        .iter()
        .find(|object| object.name == name)
        .ok_or_else(|| format!("no object {name} in {schema:?}").into())
}

#[test]
fn the_schema_lists_tables_indexes_views_and_triggers_with_their_definitions() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.execute(&Transaction::new([
        Statement::new("CREATE TABLE facts (id INTEGER PRIMARY KEY, topic TEXT UNIQUE, body TEXT)"),
        Statement::new("CREATE INDEX facts_body ON facts (body)"),
        Statement::new("CREATE TRIGGER facts_seen AFTER INSERT ON facts BEGIN SELECT 1; END"),
        Statement::new("CREATE VIEW recent AS SELECT body FROM facts ORDER BY id DESC"),
        Statement::new("INSERT INTO facts (topic, body) VALUES ('a', 'x'), ('b', 'y')"),
    ]))?;

    let schema = db.schema(&request(100, Internal::Hide))?;
    assert!(!schema.truncated);
    // Each table is followed by its indexes and triggers; the generated unique index is hidden.
    assert_eq!(
        listed(&schema),
        [
            (ObjectKind::Table, "facts"),
            (ObjectKind::Index, "facts_body"),
            (ObjectKind::Trigger, "facts_seen"),
            (ObjectKind::View, "recent"),
            (ObjectKind::Table, "t"),
        ]
    );
    let facts = object(&schema, "facts")?;
    assert_eq!(facts.rows, Some(RowCount::Exact(2)));
    assert_eq!(facts.table, None);
    assert_eq!(
        facts.definition.as_deref(),
        Some("CREATE TABLE facts (id INTEGER PRIMARY KEY, topic TEXT UNIQUE, body TEXT)")
    );
    let index = object(&schema, "facts_body")?;
    assert_eq!(index.table.as_deref(), Some("facts"));
    assert_eq!(
        index.definition.as_deref(),
        Some("CREATE INDEX facts_body ON facts (body)")
    );
    assert_eq!(index.rows, None);
    let trigger = object(&schema, "facts_seen")?;
    assert_eq!(trigger.table.as_deref(), Some("facts"));
    assert!(
        trigger
            .definition
            .as_deref()
            .is_some_and(|sql| sql.starts_with("CREATE TRIGGER"))
    );
    let view = object(&schema, "recent")?;
    assert_eq!(view.table, None);
    assert_eq!(view.rows, None, "a view's query isn't run to count it");

    // With the engine's own objects shown, the generated index has no definition.
    let all = db.schema(&request(100, Internal::Show))?;
    let generated = all
        .objects
        .iter()
        .find(|object| object.name.starts_with("sqlite_autoindex_facts"))
        .ok_or("no generated index")?;
    assert_eq!(generated.kind, ObjectKind::Index);
    assert_eq!(generated.definition, None);
    Ok(())
}

#[test]
fn an_empty_database_has_an_empty_schema() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = SqliteDatabase::open(
        dir.path().join(DATABASE_FILE),
        OpenMode::Create,
        4096,
        Arc::default(),
    )?;
    assert_eq!(db.schema(&request(100, Internal::Show))?, Schema::default());
    Ok(())
}

#[test]
fn row_counts_are_exact_below_the_limit_and_a_lower_bound_above_it() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.execute(&Transaction::new([
        Statement::new("CREATE TABLE two (v)"),
        Statement::new("INSERT INTO two VALUES (1), (2)"),
        Statement::new("CREATE TABLE five (v)"),
        Statement::new("INSERT INTO five VALUES (1), (2), (3), (4), (5)"),
        Statement::new("CREATE TABLE three (v)"),
        Statement::new("INSERT INTO three VALUES (1), (2), (3)"),
    ]))?;
    let schema = db.schema(&request(3, Internal::Hide))?;
    assert_eq!(object(&schema, "two")?.rows, Some(RowCount::Exact(2)));
    assert_eq!(
        object(&schema, "three")?.rows,
        Some(RowCount::Exact(3)),
        "a table of exactly the limit is exact"
    );
    assert_eq!(object(&schema, "five")?.rows, Some(RowCount::AtLeast(3)));
    Ok(())
}

#[test]
fn a_count_limit_of_zero_counts_nothing() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    let schema = db.schema(&request(0, Internal::Hide))?;
    assert_eq!(listed(&schema), [(ObjectKind::Table, "t")]);
    assert_eq!(object(&schema, "t")?.rows, None);
    Ok(())
}

#[test]
fn the_scan_budget_is_spent_across_tables() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.execute(&Transaction::new([
        Statement::new("CREATE TABLE a (v)"),
        Statement::new("INSERT INTO a VALUES (1), (2), (3), (4)"),
        Statement::new("CREATE TABLE b (v)"),
        Statement::new("INSERT INTO b VALUES (1)"),
    ]))?;
    // `a` takes the whole budget, so the tables after it go uncounted, however small.
    let schema = schema::read_within(&db.reader, &request(100, Internal::Hide), 3)?;
    assert_eq!(object(&schema, "a")?.rows, Some(RowCount::AtLeast(3)));
    assert_eq!(object(&schema, "b")?.rows, None);
    assert_eq!(object(&schema, "t")?.rows, None);
    Ok(())
}

#[test]
fn reserved_objects_are_never_shown() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::Full)?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    let reserved: i64 = db.writer.query_row(
        "SELECT count(*) FROM main.sqlite_schema WHERE name LIKE '\\_fleetring\\_%' ESCAPE '\\'",
        [],
        |row| row.get(0),
    )?;
    assert!(reserved > 0, "the recorder keeps a table and triggers");
    for internal in [Internal::Hide, Internal::Show] {
        let schema = db.schema(&request(100, internal))?;
        for object in &schema.objects {
            assert!(
                !object.name.starts_with(RESERVED_PREFIX)
                    && !object
                        .table
                        .as_deref()
                        .is_some_and(|table| table.starts_with(RESERVED_PREFIX)),
                "{object:?}"
            );
        }
        assert_eq!(object(&schema, "t")?.rows, Some(RowCount::Exact(2)));
    }
    Ok(())
}

#[test]
fn fts5_tables_take_writes_and_searches_in_later_statements() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    // FTS5 runs `PRAGMA data_version` while a statement steps, under the confinement, once the
    // table was created by an earlier transaction.
    exec(
        &mut db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
    )?;
    exec(
        &mut db,
        "CREATE VIRTUAL TABLE notes_fts USING fts5(body, content = 'notes', content_rowid = 'id')",
    )?;
    exec(
        &mut db,
        "CREATE TRIGGER notes_ai AFTER INSERT ON notes BEGIN \
         INSERT INTO notes_fts (rowid, body) VALUES (new.id, new.body); END",
    )?;
    exec(
        &mut db,
        "INSERT INTO notes VALUES (1, 'alpha beta'), (2, 'gamma')",
    )?;
    exec(&mut db, "CREATE VIRTUAL TABLE docs USING fts5(body)")?;
    exec(&mut db, "INSERT INTO docs VALUES ('alpha')")?;

    let found = query(
        &mut db,
        "SELECT rowid FROM notes_fts WHERE notes_fts MATCH 'alpha' ORDER BY bm25(notes_fts)",
    )?;
    assert_eq!(found.rows, [[Value::Integer(1)]]);
    let docs = query(&mut db, "SELECT rowid FROM docs WHERE docs MATCH 'alpha'")?;
    assert_eq!(docs.rows, [[Value::Integer(1)]]);
    Ok(())
}

#[test]
fn a_recorded_database_takes_triggers_views_and_fts5_tables() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    // The recorder's own table, `_fleetring_cdc`, mentions the reserved prefix in its definition;
    // only triggers and views are checked for it.
    db.start_recording(CaptureMode::Full)?;
    exec(
        &mut db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
    )?;
    exec(&mut db, "CREATE VIEW recent AS SELECT id FROM notes")?;
    // A virtual table and its shadow tables get none of the recorder's triggers.
    exec(
        &mut db,
        "CREATE VIRTUAL TABLE notes_fts USING fts5(body, content = 'notes', content_rowid = 'id')",
    )?;
    exec(
        &mut db,
        "CREATE TRIGGER notes_ai AFTER INSERT ON notes BEGIN \
         INSERT INTO notes_fts (rowid, body) VALUES (new.id, new.body); END",
    )?;
    let start = db.position()?;
    exec(&mut db, "INSERT INTO notes VALUES (1, 'alpha')")?;
    let end = db.position()?.ok_or("a position after a write")?;
    let changes = db.changes_between(start.as_ref(), &end)?;
    assert_eq!(
        changes.len(),
        1,
        "only the content table's row: {changes:?}"
    );

    let found = query(
        &mut db,
        "SELECT rowid FROM notes_fts WHERE notes_fts MATCH 'alpha'",
    )?;
    assert_eq!(found.rows, [[Value::Integer(1)]]);
    // A trigger that does name a reserved table is still refused.
    let message = refusal(exec(
        &mut db,
        "CREATE TRIGGER sneaky AFTER INSERT ON t BEGIN DELETE FROM _fleetring_cdc; END",
    ))?;
    assert!(message.contains("reserved"), "{message}");
    Ok(())
}

#[test]
fn fts5_tables_are_virtual_uncounted_and_hide_their_shadow_tables() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    // The bundled SQLite has FTS5, which the agent skill relies on.
    db.execute(&Transaction::new([
        Statement::new("CREATE VIRTUAL TABLE docs USING fts5(body)"),
        Statement::new("INSERT INTO docs VALUES ('alpha'), ('beta')"),
    ]))?;
    let schema = db.schema(&request(100, Internal::Hide))?;
    assert_eq!(
        listed(&schema),
        [(ObjectKind::VirtualTable, "docs"), (ObjectKind::Table, "t")]
    );
    let docs = object(&schema, "docs")?;
    assert_eq!(docs.rows, None, "counting a virtual table runs its query");
    assert!(
        docs.definition
            .as_deref()
            .is_some_and(|sql| sql.starts_with("CREATE VIRTUAL TABLE docs USING fts5"))
    );

    let all = db.schema(&request(100, Internal::Show))?;
    for shadow in [
        "docs_data",
        "docs_idx",
        "docs_content",
        "docs_docsize",
        "docs_config",
    ] {
        assert_eq!(object(&all, shadow)?.kind, ObjectKind::Table, "{shadow}");
    }
    Ok(())
}

#[test]
fn object_lists_past_the_maximum_are_truncated() -> TestResult {
    let tables = |from: usize| {
        Transaction::new(
            (from..MAX_SCHEMA_OBJECTS).map(|n| Statement::new(format!("CREATE TABLE x{n:04} (v)"))),
        )
    };
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    // `t` and 1,000 more.
    db.execute(&tables(0))?;
    let schema = db.schema(&request(10, Internal::Hide))?;
    assert_eq!(schema.objects.len(), MAX_SCHEMA_OBJECTS);
    assert!(schema.truncated);

    // `t` and 999 more are exactly the maximum.
    let other = tempfile::tempdir()?;
    let mut db = database(other.path())?;
    db.execute(&tables(1))?;
    let schema = db.schema(&request(10, Internal::Hide))?;
    assert_eq!(schema.objects.len(), MAX_SCHEMA_OBJECTS);
    assert!(!schema.truncated, "exactly the maximum isn't more");
    Ok(())
}

#[test]
fn identifiers_with_quotes_are_counted_safely() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.execute(&Transaction::new([
        Statement::new("CREATE TABLE \"we\"\"ird\" (v)"),
        Statement::new("INSERT INTO \"we\"\"ird\" VALUES (1), (2)"),
    ]))?;
    let schema = db.schema(&request(100, Internal::Hide))?;
    assert_eq!(object(&schema, "we\"ird")?.rows, Some(RowCount::Exact(2)));
    Ok(())
}

#[test]
fn a_query_stops_at_its_row_limit_and_says_so() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(&dir.path().join("a"))?;
    exec(&mut db, "INSERT INTO t VALUES (2), (3)")?;
    let all = "SELECT v FROM t ORDER BY v";
    let limited = |max: u32| -> Result<Statement, Box<dyn Error>> {
        let max = std::num::NonZeroU32::new(max).ok_or("a limit of 0")?;
        Ok(Statement::new(all).with_max_rows(max))
    };

    let rows = db.query(&limited(2)?)?;
    assert_eq!(rows.rows, [[Value::Integer(1)], [Value::Integer(2)]]);
    assert!(rows.truncated, "a third row exists");
    let rows = db.query(&limited(3)?)?;
    assert_eq!(rows.rows.len(), 3);
    assert!(!rows.truncated, "exactly the limit isn't truncated");
    let rows = query(&mut db, all)?;
    assert_eq!(rows.rows.len(), 3);
    assert!(!rows.truncated);
    Ok(())
}

#[test]
fn a_second_statement_is_refused_and_nothing_runs() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(&dir.path().join("a"))?;
    let message = refusal(exec(
        &mut db,
        "INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)",
    ))?;
    assert_eq!(
        message,
        "a call runs one statement; send each in a call of its own"
    );
    assert_eq!(values(&mut db)?, [[Value::Integer(1)]]);
    let message = refusal(query(&mut db, "SELECT 1; SELECT 2"))?;
    assert_eq!(
        message,
        "a call runs one statement; send each in a call of its own"
    );
    Ok(())
}

/// The `type` and `name` of every schema object in `main` and `temp` whose name or table is
/// reserved, read on the write connection with the confinement clear.
fn reserved_objects(db: &SqliteDatabase) -> Result<Vec<(String, String)>, Box<dyn Error>> {
    let mut statement = db.writer.prepare(
        "SELECT type, name FROM (
            SELECT type, name, tbl_name FROM main.sqlite_master UNION ALL
            SELECT type, name, tbl_name FROM temp.sqlite_master)
         WHERE (name LIKE '\\_fleetring\\_%' ESCAPE '\\' OR tbl_name LIKE '\\_fleetring\\_%' ESCAPE '\\')
           AND name NOT LIKE '\\_fleetring\\_trg\\_%' ESCAPE '\\'
         ORDER BY type, name",
    )?;
    let objects = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(objects)
}

fn objects(names: &[(&str, &str)]) -> Vec<(String, String)> {
    names
        .iter()
        .map(|&(kind, name)| (kind.to_owned(), name.to_owned()))
        .collect()
}

/// The refusal of a statement that names, or reaches, the reserved namespace.
fn reserved_refusal() -> String {
    Refusal::Reserved.to_string()
}

#[test]
fn renaming_a_table_into_the_reserved_namespace_is_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    for target in [
        "_fleetring_sinks",
        "_fleetring_cdc",
        "_FLEETRING_cdc",
        "\"_Fleetring_Sinks\"",
        "_fleetring_mine",
    ] {
        let sql = format!("ALTER TABLE t RENAME TO {target}");
        let message = refusal(exec(&mut db, &sql)).map_err(|error| format!("`{sql}`: {error}"))?;
        assert_eq!(message, reserved_refusal(), "`{sql}`");
    }
    // The refusal rolls back the whole transaction, and the table keeps its name and rows.
    let message = refusal(db.execute(&Transaction::new([
        Statement::new("INSERT INTO t VALUES (2)"),
        Statement::new("ALTER TABLE t RENAME TO _fleetring_sinks"),
    ])))?;
    assert_eq!(message, reserved_refusal());
    assert_eq!(values(&mut db)?, [[Value::Integer(1)]]);
    assert_eq!(reserved_objects(&db)?, objects(&[]));

    // Nor can a temporary table be renamed there, where it would shadow the adapter's tables.
    exec(
        &mut db,
        "CREATE TEMP TABLE scratch (sink TEXT, position BLOB)",
    )?;
    let message = refusal(exec(
        &mut db,
        "ALTER TABLE temp.scratch RENAME TO _fleetring_sinks",
    ))?;
    assert_eq!(message, reserved_refusal());
    assert_eq!(reserved_objects(&db)?, objects(&[]));

    // With the adapter's own tables in place, too.
    db.start_recording(CaptureMode::Full)?;
    db.stage("s", &Position::new([1]));
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    for sql in [
        "ALTER TABLE t RENAME TO _fleetring_other",
        "ALTER TABLE _fleetring_sinks RENAME TO mine",
        "ALTER TABLE _FLEETRING_CDC RENAME TO mine",
        "ALTER TABLE _fleetring_cdc ADD COLUMN extra",
        "ALTER TABLE _fleetring_sinks RENAME COLUMN position TO at",
        "ALTER TABLE _fleetring_cdc DROP COLUMN updates",
    ] {
        refusal(exec(&mut db, sql)).map_err(|error| format!("`{sql}`: {error}"))?;
    }
    assert_eq!(
        reserved_objects(&db)?,
        objects(&[("table", "_fleetring_cdc"), ("table", "_fleetring_sinks")])
    );
    assert_eq!(db.marks()?.len(), 1);
    Ok(())
}

#[test]
fn names_in_any_case_are_reserved() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::Full)?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    for sql in [
        "CREATE TABLE _FLEETRING_x (v)",
        "CREATE TEMP TABLE _Fleetring_Sinks (sink, position)",
        "CREATE TEMP VIEW _FLEETRING_CDC AS SELECT 1",
        "CREATE INDEX _FleetRing_i ON t (v)",
        "CREATE TRIGGER _FLEETRING_TRG_t AFTER INSERT ON t BEGIN SELECT 1; END",
        "INSERT INTO _FLEETRING_CDC (change_txn_id, change_type, start) VALUES (0, 2, x'')",
    ] {
        let message = refusal(exec(&mut db, sql)).map_err(|error| format!("`{sql}`: {error}"))?;
        assert_eq!(message, reserved_refusal(), "`{sql}`");
    }
    refusal(query(&mut db, "SELECT * FROM _FLEETRING_CDC"))?;
    assert_eq!(
        reserved_objects(&db)?,
        objects(&[("table", "_fleetring_cdc")])
    );
    Ok(())
}

#[test]
fn renaming_and_altering_ordinary_tables_still_works() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.execute(&Transaction::new([
        Statement::new("ALTER TABLE t ADD COLUMN w"),
        Statement::new("ALTER TABLE t RENAME COLUMN w TO x"),
        Statement::new("ALTER TABLE t DROP COLUMN x"),
        Statement::new("CREATE TEMP TABLE scratch (v)"),
        Statement::new("ALTER TABLE temp.scratch RENAME TO scratched"),
    ]))?;
    db.start_recording(CaptureMode::Full)?;
    db.stage("s", &Position::new([1]));
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    db.execute(&Transaction::new([
        Statement::new("ALTER TABLE t RENAME TO u"),
        Statement::new("ALTER TABLE u ADD COLUMN w"),
        Statement::new("ALTER TABLE u RENAME COLUMN w TO x"),
    ]))?;
    let start = db.position()?;
    exec(&mut db, "INSERT INTO u (v) VALUES (3)")?;
    let end = db.position()?.ok_or("a position after a write")?;
    let changes = db.changes_between(start.as_ref(), &end)?;
    assert_eq!(
        changes.len(),
        1,
        "the renamed table is recorded: {changes:?}"
    );
    assert_eq!(
        query(&mut db, "SELECT v FROM u ORDER BY v")?.rows,
        [
            [Value::Integer(1)],
            [Value::Integer(2)],
            [Value::Integer(3)]
        ]
    );
    assert_eq!(db.marks()?.len(), 1);
    Ok(())
}

#[test]
fn a_table_with_triggers_has_no_route_into_the_reserved_namespace() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    // The review's attack: a table shaped like the marks table, whose triggers copy every row
    // into a table of the user's, renamed into the namespace before the adapter creates its own.
    db.execute(&Transaction::new([
        Statement::new("CREATE TABLE plain (sink TEXT PRIMARY KEY, position BLOB NOT NULL)"),
        Statement::new("CREATE TABLE stolen (sink, position)"),
        Statement::new(
            "CREATE TRIGGER copy_insert AFTER INSERT ON plain BEGIN \
             INSERT INTO stolen VALUES (new.sink, new.position); END",
        ),
        Statement::new(
            "CREATE TRIGGER copy_update AFTER UPDATE ON plain BEGIN \
             INSERT INTO stolen VALUES (new.sink, new.position); END",
        ),
    ]))?;
    for sql in [
        "ALTER TABLE plain RENAME TO _fleetring_sinks",
        "ALTER TABLE plain RENAME TO _fleetring_cdc",
        // Renaming the table a trigger writes to rewrites the trigger to name the new name.
        "ALTER TABLE stolen RENAME TO _fleetring_sinks",
    ] {
        let message = refusal(exec(&mut db, sql)).map_err(|error| format!("`{sql}`: {error}"))?;
        assert_eq!(message, reserved_refusal(), "`{sql}`");
    }
    db.start_recording(CaptureMode::Full)?;
    db.stage("s", &Position::new([7]));
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    db.stage("s", &Position::new([8]));
    exec(&mut db, "INSERT INTO t VALUES (3)")?;

    assert_eq!(
        db.marks()?,
        BTreeMap::from([("s".to_owned(), Position::new([8]))])
    );
    assert_eq!(
        query(&mut db, "SELECT count(*) FROM stolen")?.rows,
        [[Value::Integer(0)]],
        "no mark reached the user's table"
    );
    let triggers = query(
        &mut db,
        "SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name",
    )?;
    assert_eq!(
        triggers.rows,
        [[Value::from("copy_insert")], [Value::from("copy_update")]]
    );
    Ok(())
}

#[test]
fn a_planted_change_table_with_a_trigger_is_not_adopted() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    // Planted with the confinement clear, as a database written before the namespace was
    // guarded, or by another program, could hold it.
    db.writer.execute_batch(
        "CREATE TABLE \"_fleetring_cdc\" (
            change_id INTEGER PRIMARY KEY,
            change_txn_id INTEGER NOT NULL,
            change_type INTEGER NOT NULL,
            table_name TEXT,
            id TEXT,
            before TEXT,
            after TEXT,
            updates TEXT,
            start BLOB NOT NULL);
         CREATE TABLE stolen (row);
         CREATE TRIGGER evil AFTER INSERT ON _fleetring_cdc BEGIN
            INSERT INTO stolen VALUES (new.after); END;",
    )?;
    let message = refusal(db.start_recording(CaptureMode::Full))?;
    assert!(
        message.starts_with("trigger `evil` in the namespace reserved for fleetringd"),
        "{message}"
    );
    assert!(db.recorder.is_none(), "nothing is recorded");
    // Writes that don't touch the adapter's tables go on, and nothing is dropped.
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    assert_eq!(
        reserved_objects(&db)?,
        objects(&[("table", "_fleetring_cdc"), ("trigger", "evil")])
    );
    assert_eq!(
        query(&mut db, "SELECT count(*) FROM stolen")?.rows,
        [[Value::Integer(0)]]
    );
    // A sink mark isn't written or read either.
    db.stage("s", &Position::new([1]));
    let message = refusal(exec(&mut db, "INSERT INTO t VALUES (3)"))?;
    assert!(message.contains("`evil`"), "{message}");
    refusal(db.marks())?;
    assert_eq!(values(&mut db)?, [[Value::Integer(1)], [Value::Integer(2)]]);
    Ok(())
}

#[test]
fn a_trigger_planted_after_recording_started_stops_it() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::Full)?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    db.writer.execute_batch(
        "CREATE TABLE stolen (row);
         CREATE TRIGGER evil AFTER INSERT ON _fleetring_cdc BEGIN
            INSERT INTO stolen VALUES (new.after); END;",
    )?;
    let message = refusal(exec(&mut db, "INSERT INTO t VALUES (3)"))?;
    assert!(message.contains("`evil`"), "{message}");
    assert_eq!(
        query(&mut db, "SELECT count(*) FROM stolen")?.rows,
        [[Value::Integer(0)]]
    );
    assert_eq!(values(&mut db)?, [[Value::Integer(1)], [Value::Integer(2)]]);
    Ok(())
}

#[test]
fn planted_objects_of_another_shape_are_not_adopted() -> TestResult {
    for (planted, kind, name) in [
        // Not `STRICT` or `WITHOUT ROWID`.
        (
            "CREATE TABLE _fleetring_sinks (sink TEXT PRIMARY KEY, position BLOB NOT NULL)",
            "table",
            "_fleetring_sinks",
        ),
        // Another column.
        (
            "CREATE TABLE _fleetring_sinks (sink TEXT PRIMARY KEY, position BLOB NOT NULL, \
             extra ANY) STRICT, WITHOUT ROWID",
            "table",
            "_fleetring_sinks",
        ),
        // A view in the table's place.
        (
            "CREATE VIEW _fleetring_sinks AS SELECT 'x' AS sink, x'00' AS position",
            "view",
            "_fleetring_sinks",
        ),
        // A temporary table, which the adapter's statements would find first.
        (
            "CREATE TEMP TABLE _fleetring_sinks (sink TEXT PRIMARY KEY, position BLOB NOT NULL) \
             STRICT, WITHOUT ROWID",
            "table",
            "_fleetring_sinks",
        ),
        // The adapter's name in another case.
        ("CREATE TABLE _FLEETRING_x (v)", "table", "_FLEETRING_x"),
    ] {
        let dir = tempfile::tempdir()?;
        let mut db = database(dir.path())?;
        db.writer.execute_batch(planted)?;
        db.stage("s", &Position::new([1]));
        let message = refusal(exec(&mut db, "INSERT INTO t VALUES (2)"))?;
        assert!(
            message.starts_with(&format!(
                "{kind} `{name}` in the namespace reserved for fleetringd"
            )),
            "`{planted}`: {message}"
        );
        refusal(db.marks())?;
        assert_eq!(values(&mut db)?, [[Value::Integer(1)]], "`{planted}`");
    }
    Ok(())
}

#[test]
fn a_view_reading_the_namespace_is_refused_when_created_and_when_run() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::Full)?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    for sql in [
        "CREATE VIEW peek AS SELECT * FROM _fleetring_cdc",
        "CREATE VIEW peek AS SELECT * FROM _FLEETRING_CDC",
        "CREATE TEMP VIEW peek AS SELECT * FROM main._fleetring_cdc",
    ] {
        let message = refusal(exec(&mut db, sql)).map_err(|error| format!("`{sql}`: {error}"))?;
        assert!(message.contains("reserved"), "`{sql}`: {message}");
    }
    // A view that got in anyway is refused when it runs, on either connection.
    db.writer
        .execute_batch("CREATE VIEW peek AS SELECT * FROM _FLEETRING_CDC")?;
    let message = refusal(query(&mut db, "SELECT * FROM peek"))?;
    assert_eq!(message, reserved_refusal());
    let message = refusal(exec(&mut db, "SELECT * FROM peek"))?;
    assert_eq!(message, reserved_refusal());
    Ok(())
}

#[test]
fn foreign_key_check_is_refused_as_an_oracle_for_reserved_rows() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::Full)?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    // Declaring the key is allowed. Enforcing it reads the reserved table, which is refused, and
    // the pragma that would report which reserved rows exist is refused in every form.
    exec(
        &mut db,
        "CREATE TABLE probe (id INTEGER REFERENCES _fleetring_cdc (change_id))",
    )?;
    let message = refusal(exec(&mut db, "INSERT INTO probe VALUES (1)"))?;
    assert_eq!(message, reserved_refusal());
    for sql in [
        "PRAGMA foreign_key_check",
        "PRAGMA foreign_key_check(probe)",
        "SELECT * FROM pragma_foreign_key_check('probe')",
    ] {
        let message = refusal(query(&mut db, sql)).map_err(|error| format!("`{sql}`: {error}"))?;
        assert!(
            message.starts_with("pragma `foreign_key_check` isn't allowed"),
            "`{sql}`: {message}"
        );
        refusal(exec(&mut db, sql)).map_err(|error| format!("`{sql}`: {error}"))?;
    }
    Ok(())
}

#[test]
fn a_user_table_taken_for_a_shadow_table_stays_readable_and_is_kept() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::Full)?;
    exec(&mut db, "CREATE TABLE notes_content (v)")?;
    exec(&mut db, "INSERT INTO notes_content VALUES (1)")?;
    // A contentless FTS5 table creates no `_content` table, so SQLite takes the user's table of
    // that name for its shadow table: defensive mode then keeps it read-only, and the recorder,
    // which skips shadow tables, records nothing for it, since nothing can change it.
    exec(
        &mut db,
        "CREATE VIRTUAL TABLE notes USING fts5(body, content = '')",
    )?;
    let message = refusal(exec(&mut db, "INSERT INTO notes_content VALUES (2)"))?;
    assert!(message.contains("may not be modified"), "{message}");
    refusal(exec(&mut db, "DROP TABLE notes_content"))?;
    assert_eq!(
        query(&mut db, "SELECT v FROM notes_content")?.rows,
        [[Value::Integer(1)]]
    );
    // Dropping the virtual table gives the table back once the database is opened again: the
    // connection keeps the table marked as a shadow table until it reloads the schema.
    exec(&mut db, "DROP TABLE notes")?;
    Box::new(db).close()?;
    let mut db = SqliteDatabase::open(
        dir.path().join(DATABASE_FILE),
        OpenMode::Existing,
        4096,
        Arc::default(),
    )?;
    db.start_recording(CaptureMode::Full)?;
    let start = db.position()?;
    exec(&mut db, "INSERT INTO notes_content VALUES (2)")?;
    let end = db.position()?.ok_or("a position after a write")?;
    let changes = db.changes_between(start.as_ref(), &end)?;
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert_eq!(
        query(&mut db, "SELECT v FROM notes_content ORDER BY v")?.rows,
        [[Value::Integer(1)], [Value::Integer(2)]]
    );
    Ok(())
}

/// What a watcher reads after `after` up to the log's end, as `(start, inserted values)` per
/// transaction, with the last transaction's resume position.
fn watched(
    db: &mut SqliteDatabase,
    after: Option<&Position>,
    limit: ReadLimit,
) -> Result<Vec<RecordedTransaction>, Box<dyn Error>> {
    let end = db.position()?.ok_or("a position")?;
    Ok(db.transactions_after(after, &end, limit)?)
}

fn inserted_values(transaction: &RecordedTransaction) -> Vec<Value> {
    transaction
        .changes
        .iter()
        .flat_map(|change| change.values().iter().map(|column| column.value.clone()))
        .collect()
}

fn limit(transactions: usize) -> Result<ReadLimit, Box<dyn Error>> {
    Ok(ReadLimit::new(
        transactions.try_into()?,
        std::num::NonZeroUsize::MAX,
    ))
}

#[test]
fn a_watcher_reads_whole_transactions_with_positions_to_resume_after() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::After)?;
    let start = db.position()?;
    // A transaction that records nothing first, as a fresh table's creation does: the range is
    // still held, though the first recorded row starts after the watcher's position.
    exec(&mut db, "CREATE TABLE u (w)")?;
    db.execute(&Transaction::new([
        Statement::new("INSERT INTO t VALUES (2)"),
        Statement::new("INSERT INTO t VALUES (3)"),
    ]))?;
    let between = db.position()?.ok_or("a position")?;
    exec(&mut db, "INSERT INTO t VALUES (4)")?;
    let end = db.position()?.ok_or("a position")?;

    let all = watched(&mut db, start.as_ref(), limit(10)?)?;
    assert_eq!(all.len(), 2, "{all:?}");
    assert_eq!(
        inserted_values(&all[0]),
        [Value::Integer(2), Value::Integer(3)],
        "one transaction, whole"
    );
    assert_eq!(inserted_values(&all[1]), [Value::Integer(4)]);
    assert_eq!(all[1].start.as_ref(), Some(&between));
    assert_eq!(
        all[0].position, between,
        "a transaction resumes where the next one starts"
    );
    assert_eq!(all[1].position, end, "the last resumes at the range's end");

    // A full read stops after whole transactions, and resuming after its last position
    // continues with the next.
    let first = watched(&mut db, start.as_ref(), limit(1)?)?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].position, between);
    let rest = watched(&mut db, Some(&first[0].position), limit(10)?)?;
    assert_eq!(rest, all[1..]);
    // Nothing after the end.
    assert!(watched(&mut db, Some(&end), limit(10)?)?.is_empty());
    Ok(())
}

/// A transaction inserting `values` into `t`.
/// The log's end is read after the write, as the actor does when it captures each batch, so
/// the next transaction records where it starts.
fn insert_all(db: &mut SqliteDatabase, values: &[i64]) -> Result<(), EngineError> {
    db.execute(&Transaction::new(values.iter().map(|value| {
        Statement::new(format!("INSERT INTO t VALUES ({value})"))
    })))?;
    db.position()?;
    Ok(())
}

fn row_limit(rows: usize) -> Result<ReadLimit, Box<dyn Error>> {
    Ok(ReadLimit::new(
        std::num::NonZeroUsize::MAX,
        rows.try_into()?,
    ))
}

#[test]
fn a_row_budget_never_cuts_a_transaction() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::After)?;
    let start = db.position()?;
    // A has one row, B three, C one; the budget is three rows, which B reaches inside itself.
    insert_all(&mut db, &[10])?;
    insert_all(&mut db, &[20, 21, 22])?;
    insert_all(&mut db, &[30])?;
    let read = watched(&mut db, start.as_ref(), row_limit(3)?)?;
    assert_eq!(read.len(), 2, "A and B, B whole: {read:?}");
    assert_eq!(inserted_values(&read[0]), [Value::Integer(10)]);
    assert_eq!(
        inserted_values(&read[1]),
        [Value::Integer(20), Value::Integer(21), Value::Integer(22)]
    );
    let b = &read[1];
    assert!(
        Some(&b.position) > b.start.as_ref(),
        "B resumes past itself: {b:?}"
    );
    let rest = watched(&mut db, Some(&b.position), row_limit(3)?)?;
    assert_eq!(rest.len(), 1);
    assert_eq!(inserted_values(&rest[0]), [Value::Integer(30)]);

    // The budget reached exactly at a transaction's last row stops after that transaction.
    let exact = watched(&mut db, start.as_ref(), row_limit(4)?)?;
    assert_eq!(exact.len(), 2, "{exact:?}");
    assert_eq!(exact[1].position, rest[0].start.clone().ok_or("C's start")?);
    Ok(())
}

#[test]
fn a_transaction_larger_than_the_budget_is_read_whole() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::After)?;
    let start = db.position()?;
    let values: Vec<i64> = (0..50).collect();
    insert_all(&mut db, &values)?;
    insert_all(&mut db, &[99])?;
    let read = watched(&mut db, start.as_ref(), row_limit(10)?)?;
    assert_eq!(read.len(), 1, "the big one alone, whole");
    assert_eq!(read[0].changes.len(), 50);
    assert!(Some(&read[0].position) > read[0].start.as_ref());
    let rest = watched(&mut db, Some(&read[0].position), row_limit(10)?)?;
    assert_eq!(rest.len(), 1);
    assert_eq!(inserted_values(&rest[0]), [Value::Integer(99)]);
    Ok(())
}

#[test]
fn a_pruned_range_is_refused_with_the_oldest_position_held() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::After)?;
    let start = db.position()?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    let released = db.position()?.ok_or("a position")?;
    exec(&mut db, "INSERT INTO t VALUES (3)")?;
    db.release(&released)?;
    // The prune runs within the next 64 transactions.
    for value in 4..70 {
        exec(&mut db, &format!("INSERT INTO t VALUES ({value})"))?;
    }
    let refused = watched(&mut db, start.as_ref(), limit(10)?);
    let Err(error) = refused else {
        return Err(format!("a pruned range is refused: {refused:?}").into());
    };
    match error.downcast_ref::<EngineError>() {
        Some(EngineError::ChangesReleased { oldest, .. }) => {
            assert_eq!(oldest.as_ref(), Some(&released), "the floor is the release");
        }
        other => return Err(format!("expected ChangesReleased, got {other:?}").into()),
    }
    // From the floor on, every transaction is there, the first being the one released after.
    let held = watched(&mut db, Some(&released), limit(100)?)?;
    assert_eq!(held.len(), 67, "{}", held.len());
    assert_eq!(inserted_values(&held[0]), [Value::Integer(3)]);
    // Sinks still read the range, with the floor skipped.
    let end = db.position()?.ok_or("a position")?;
    assert_eq!(db.changes_between(Some(&released), &end)?.len(), 67);
    Ok(())
}

#[test]
fn without_a_floor_the_oldest_row_stands_in_for_one() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::After)?;
    let start = db.position()?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    let second = db.position()?.ok_or("a position")?;
    exec(&mut db, "INSERT INTO t VALUES (3)")?;
    // As a table an older release pruned: no floor, and the first transaction's rows gone.
    db.writer.execute(
        "DELETE FROM _fleetring_cdc WHERE change_id < (SELECT min(change_id) FROM _fleetring_cdc \
         WHERE start = ?1)",
        params![second.as_bytes()],
    )?;
    let refused = watched(&mut db, start.as_ref(), limit(10)?);
    assert!(
        matches!(
            refused.as_ref().map_err(|error| error.downcast_ref::<EngineError>()),
            Err(Some(EngineError::ChangesReleased { oldest: Some(oldest), .. })) if *oldest == second
        ),
        "{refused:?}"
    );
    let held = watched(&mut db, Some(&second), limit(10)?)?;
    assert_eq!(held.len(), 1);
    assert_eq!(inserted_values(&held[0]), [Value::Integer(3)]);
    Ok(())
}

#[test]
fn transactions_from_before_recording_are_never_held() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    let before = db.position()?;
    exec(&mut db, "INSERT INTO t VALUES (2)")?;
    db.start_recording(CaptureMode::After)?;
    let refused = watched(&mut db, before.as_ref(), limit(10)?);
    assert!(
        matches!(
            refused
                .as_ref()
                .map_err(|error| error.downcast_ref::<EngineError>()),
            Err(Some(EngineError::ChangesReleased { oldest: None, .. }))
        ),
        "no change table yet: {refused:?}"
    );
    let recording = db.position()?;
    exec(&mut db, "INSERT INTO t VALUES (3)")?;
    let refused = watched(&mut db, before.as_ref(), limit(10)?);
    assert!(
        matches!(
            refused.as_ref().map_err(|error| error.downcast_ref::<EngineError>()),
            Err(Some(EngineError::ChangesReleased { oldest: Some(oldest), .. })) if Some(oldest) == recording.as_ref()
        ),
        "the floor is where recording began: {refused:?}"
    );
    assert_eq!(watched(&mut db, recording.as_ref(), limit(10)?)?.len(), 1);
    Ok(())
}

#[test]
fn a_rolled_back_first_recorded_transaction_leaves_recording_working() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut db = database(dir.path())?;
    db.start_recording(CaptureMode::After)?;
    let start = db.position()?;
    // The first transaction recording runs creates the change table and the triggers, then fails
    // and rolls them back with it.
    let failed = db.execute(&Transaction::new([
        Statement::new("INSERT INTO t VALUES (2)"),
        Statement::new("INSERT INTO missing VALUES (1)"),
    ]));
    assert!(failed.is_err());
    exec(&mut db, "INSERT INTO t VALUES (3)")?;
    let end = db.position()?.ok_or("a position after a write")?;
    let changes = db.changes_between(start.as_ref(), &end)?;
    assert_eq!(
        changes.len(),
        1,
        "the insert after the rollback is recorded: {changes:?}"
    );
    Ok(())
}
