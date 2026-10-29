//! Logical decoding: inserts, updates, and deletes with their keys and values, one change per
//! row in statement order within a transaction, in each capture mode; and the reservation of
//! the change table (ADR-0024).

use fleetringd::changes::{ChangeKind, ConfiguredSink, Format, MemorySink};
use fleetringd::engine::{
    CaptureMode, Column, RowChange, RowOperation, Statement, Transaction, Value,
};

use crate::common::{Harness, TestResult, every_database, name};

fn rows(sink: &MemorySink) -> Vec<RowChange> {
    sink.changes()
        .iter()
        .flat_map(|change| match &change.kind {
            ChangeKind::Logical(rows) => rows.clone(),
            ChangeKind::Physical(_) => Vec::new(),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn inserts_updates_and_deletes_are_decoded_with_keys_and_values() -> TestResult {
    let harness = Harness::start().await?;
    let sink = MemorySink::new("rows");
    let node = harness
        .node(vec![ConfiguredSink::new(
            sink.clone(),
            Format::Logical,
            every_database(),
        )])
        .await?;
    let db = name("people");
    node.create(
        &db,
        "CREATE TABLE person (id INTEGER PRIMARY KEY, name TEXT, score REAL)",
    )
    .await?;
    node.await_caught_up().await?;
    assert!(rows(&sink).is_empty(), "schema changes aren't captured");

    // One transaction, several statements: one change per row, in order.
    node.execute(
        &db,
        Transaction::new([
            Statement::with_params(
                "INSERT INTO person (id, name, score) VALUES (?1, ?2, ?3)",
                [Value::Integer(1), Value::from("ada"), Value::Real(1.5)],
            ),
            Statement::with_params(
                "INSERT INTO person (id, name, score) VALUES (?1, ?2, ?3)",
                [Value::Integer(2), Value::from("bob"), Value::Null],
            ),
            Statement::with_params(
                "UPDATE person SET score = ?1 WHERE id = ?2",
                [Value::Real(2.0), Value::Integer(1)],
            ),
            Statement::with_params("DELETE FROM person WHERE id = ?1", [Value::Integer(2)]),
        ]),
    )
    .await?;
    node.await_caught_up().await?;
    // The schema change was a batch with no rows; the transaction is one change.
    let changes: Vec<_> = sink
        .changes()
        .into_iter()
        .filter(|change| !matches!(&change.kind, ChangeKind::Logical(rows) if rows.is_empty()))
        .collect();
    assert_eq!(changes.len(), 1, "one transaction, one change: {changes:?}");
    assert_eq!(changes[0].database, db);
    assert_eq!(changes[0].epoch.get(), 1);
    let decoded = rows(&sink);
    let person = |id: i64, name: &str, score: Value| {
        vec![
            Column::new("id", id),
            Column::new("name", name),
            Column {
                name: "score".to_owned(),
                value: score,
            },
        ]
    };
    // The default mode, `after`: new values after an insert or update, the key alone for a
    // delete.
    assert_eq!(
        decoded,
        vec![
            RowChange::new("person", RowOperation::Insert, [Column::new("id", 1_i64)])
                .with_after(person(1, "ada", Value::Real(1.5))),
            RowChange::new("person", RowOperation::Insert, [Column::new("id", 2_i64)])
                .with_after(person(2, "bob", Value::Null)),
            RowChange::new("person", RowOperation::Update, [Column::new("id", 1_i64)])
                .with_after(person(1, "ada", Value::Real(2.0))),
            RowChange::new("person", RowOperation::Delete, [Column::new("id", 2_i64)]),
        ]
    );
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tables_without_a_declared_key_use_the_rowid_and_composite_keys_are_kept() -> TestResult {
    let harness = Harness::start().await?;
    let sink = MemorySink::new("rows");
    let node = harness
        .node(vec![ConfiguredSink::new(
            sink.clone(),
            Format::Logical,
            every_database(),
        )])
        .await?;
    let db = name("keys");
    node.create(&db, "CREATE TABLE plain (value INTEGER NOT NULL)")
        .await?;
    node.execute(
        &db,
        Transaction::from(Statement::new(
            "CREATE TABLE pair (a TEXT, b INTEGER, note BLOB, PRIMARY KEY (b, a)) WITHOUT ROWID",
        )),
    )
    .await?;
    node.execute(
        &db,
        Transaction::new([
            Statement::new("INSERT INTO plain (value) VALUES (7)"),
            Statement::with_params(
                "INSERT INTO pair (a, b, note) VALUES (?1, ?2, ?3)",
                [Value::from("x"), Value::Integer(9), Value::Blob(vec![1, 2])],
            ),
            Statement::new("UPDATE pair SET note = NULL WHERE a = 'x'"),
        ]),
    )
    .await?;
    node.await_caught_up().await?;
    let decoded = rows(&sink);
    assert_eq!(decoded.len(), 3, "{decoded:?}");
    assert_eq!(decoded[0].table, "plain");
    assert_eq!(decoded[0].key, vec![Column::new("rowid", 1_i64)]);
    assert_eq!(decoded[0].values(), [Column::new("value", 7_i64)]);
    // Key columns in declared key order, not table order.
    assert_eq!(
        decoded[1].key,
        vec![Column::new("b", 9_i64), Column::new("a", "x")]
    );
    assert_eq!(decoded[1].values()[2].value, Value::Blob(vec![1, 2]));
    assert_eq!(decoded[2].operation, RowOperation::Update);
    assert_eq!(decoded[2].values()[2].value, Value::Null);

    // A rolled-back transaction leaves no change behind.
    let failed = node
        .registry()
        .execute(
            &db,
            Transaction::new([
                Statement::new("INSERT INTO plain (value) VALUES (8)"),
                Statement::new("INSERT INTO nowhere VALUES (1)"),
            ]),
        )
        .await;
    assert!(failed.is_err());
    node.execute(
        &db,
        Transaction::from(Statement::new("INSERT INTO plain (value) VALUES (9)")),
    )
    .await?;
    node.await_caught_up().await?;
    let values: Vec<Value> = rows(&sink)
        .iter()
        .filter(|row| row.table == "plain")
        .map(|row| row.values()[0].value.clone())
        .collect();
    assert_eq!(values, vec![Value::Integer(7), Value::Integer(9)]);
    node.kill().await;
    Ok(())
}

/// The statements every mode test runs: an insert, an update of one column, and a delete.
fn insert_update_delete() -> Transaction {
    Transaction::new([
        Statement::new("INSERT INTO item (id, name, qty) VALUES (1, 'bolt', 10)"),
        Statement::new("UPDATE item SET qty = 11 WHERE id = 1"),
        Statement::new("DELETE FROM item WHERE id = 1"),
    ])
}

fn item(qty: i64) -> Vec<Column> {
    vec![
        Column::new("id", 1_i64),
        Column::new("name", "bolt"),
        Column::new("qty", qty),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn the_id_mode_records_keys_alone() -> TestResult {
    let harness = Harness::start().await?;
    let sink = MemorySink::new("rows");
    let node = harness
        .node_capturing(
            vec![ConfiguredSink::new(
                sink.clone(),
                Format::Logical,
                every_database(),
            )],
            CaptureMode::Id,
        )
        .await?;
    let db = name("ids");
    node.create(
        &db,
        "CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)",
    )
    .await?;
    node.execute(&db, insert_update_delete()).await?;
    node.await_caught_up().await?;
    let key = [Column::new("id", 1_i64)];
    assert_eq!(
        rows(&sink),
        vec![
            RowChange::new("item", RowOperation::Insert, key.clone()),
            RowChange::new("item", RowOperation::Update, key.clone()),
            RowChange::new("item", RowOperation::Delete, key),
        ]
    );
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_full_mode_records_old_and_new_values_and_the_updated_columns() -> TestResult {
    let harness = Harness::start().await?;
    let sink = MemorySink::new("rows");
    let node = harness
        .node_capturing(
            vec![ConfiguredSink::new(
                sink.clone(),
                Format::Logical,
                every_database(),
            )],
            CaptureMode::Full,
        )
        .await?;
    let db = name("full");
    node.create(
        &db,
        "CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)",
    )
    .await?;
    node.execute(&db, insert_update_delete()).await?;
    node.await_caught_up().await?;
    let key = [Column::new("id", 1_i64)];
    assert_eq!(
        rows(&sink),
        vec![
            RowChange::new("item", RowOperation::Insert, key.clone()).with_after(item(10)),
            RowChange::new("item", RowOperation::Update, key.clone())
                .with_before(item(10))
                .with_after(item(11))
                .with_updated(["qty".to_owned()]),
            RowChange::new("item", RowOperation::Delete, key).with_before(item(11)),
        ]
    );
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rows_an_insert_or_replace_replaces_are_recorded_as_deletes() -> TestResult {
    let harness = Harness::start().await?;
    let sink = MemorySink::new("rows");
    let node = harness
        .node(vec![ConfiguredSink::new(
            sink.clone(),
            Format::Logical,
            every_database(),
        )])
        .await?;
    let db = name("replace");
    node.create(
        &db,
        "CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER, UNIQUE (v))",
    )
    .await?;
    node.execute(
        &db,
        Transaction::new([
            Statement::new("INSERT INTO kv (k, v) VALUES ('a', 1)"),
            Statement::new("INSERT INTO kv (k, v) VALUES ('b', 2)"),
        ]),
    )
    .await?;
    // Replaces both rows: 'a' by key, 'b' by the unique value.
    node.execute(
        &db,
        Transaction::from(Statement::new(
            "INSERT OR REPLACE INTO kv (k, v) VALUES ('a', 2)",
        )),
    )
    .await?;
    node.await_caught_up().await?;
    let operations: Vec<(RowOperation, Vec<Column>)> = rows(&sink)
        .into_iter()
        .skip(2)
        .map(|row| (row.operation, row.key))
        .collect();
    // SQLite removes the conflicting rows in its own order, before the new row goes in.
    let (deleted, inserted) = operations.split_at(2);
    let mut deleted: Vec<String> = deleted
        .iter()
        .map(|(operation, key)| {
            assert_eq!(*operation, RowOperation::Delete, "{operations:?}");
            format!("{:?}", key[0].value)
        })
        .collect();
    deleted.sort();
    assert_eq!(deleted, ["Text(\"a\")", "Text(\"b\")"], "{operations:?}");
    assert_eq!(
        inserted,
        [(RowOperation::Insert, vec![Column::new("k", "a")])]
    );
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_change_table_is_reserved_and_hidden_from_statements() -> TestResult {
    let harness = Harness::start().await?;
    let sink = MemorySink::new("rows");
    let node = harness
        .node(vec![ConfiguredSink::new(
            sink.clone(),
            Format::Logical,
            every_database(),
        )])
        .await?;
    let db = name("reserved");
    node.create(&db, "CREATE TABLE t (value INTEGER)").await?;
    node.execute(
        &db,
        Transaction::from(Statement::new("INSERT INTO t (value) VALUES (1)")),
    )
    .await?;
    let refused = [
        "SELECT * FROM _fleetring_cdc",
        "DELETE FROM _fleetring_cdc",
        "INSERT INTO _fleetring_cdc (change_txn_id, change_type, start) VALUES (0, 2, x'')",
        "UPDATE _fleetring_cdc SET change_type = 9",
        "DROP TABLE _fleetring_cdc",
        "ALTER TABLE _fleetring_cdc RENAME TO mine",
        "CREATE INDEX _fleetring_mine ON t (value)",
        "CREATE TABLE _fleetring_other (x)",
        "CREATE TRIGGER mine AFTER INSERT ON t BEGIN DELETE FROM _fleetring_cdc; END",
        "CREATE VIEW peek AS SELECT * FROM _fleetring_cdc",
    ];
    for sql in refused {
        let error = node
            .registry()
            .execute(&db, Transaction::from(Statement::new(sql)))
            .await
            .err()
            .ok_or_else(|| format!("`{sql}` was accepted"))?;
        assert!(
            error.to_string().contains("reserved"),
            "`{sql}` failed for another reason: {error}"
        );
    }
    let queried = node
        .registry()
        .query(&db, Statement::new("SELECT count(*) FROM _fleetring_cdc"))
        .await;
    assert!(
        queried.is_err_and(|error| error.to_string().contains("reserved")),
        "a query reads the change table"
    );
    // The user's own statements still work, and a write counts only its own rows.
    let stored = node
        .registry()
        .execute(
            &db,
            Transaction::from(Statement::new("INSERT INTO t (value) VALUES (2), (3)")),
        )
        .await?;
    assert_eq!(stored.rows_affected, 2);
    let rows = node
        .registry()
        .query(&db, Statement::new("SELECT value FROM t ORDER BY value"))
        .await?;
    assert_eq!(rows.rows.len(), 3);
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_change_record_round_trips_through_json() -> TestResult {
    use fleetringd::changes::{Change, ChangeRecord};
    use fleetringd::engine::Position;

    let change = Change {
        database: name("ledger"),
        epoch: fleetring_control::LeaseEpoch::new(4),
        start: None,
        position: Position::new([0, 1, 255]),
        kind: ChangeKind::Logical(vec![
            RowChange::new("t", RowOperation::Delete, [Column::new("rowid", 3_i64)])
                .with_before([Column::new("blob", Value::Blob(vec![0, 7]))]),
        ]),
    };
    let record = ChangeRecord::from(&change);
    let text = serde_json::to_string(&record)?;
    assert!(text.contains("\"kind\":\"logical\""), "{text}");
    assert!(text.contains("\"position\":\"0001ff\""), "{text}");
    assert!(text.contains("{\"blob\":[0,7]}"), "{text}");
    let parsed: ChangeRecord = serde_json::from_str(&text)?;
    assert_eq!(Change::try_from(parsed)?, change);
    Ok(())
}
