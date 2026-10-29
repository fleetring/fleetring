//! Logical decoding: inserts, updates, and deletes with their keys and values, one change per
//! row in statement order within a transaction.

use fleetringd::changes::{ChangeKind, ConfiguredSink, Format, MemorySink};
use fleetringd::engine::{Column, RowChange, RowOperation, Statement, Transaction, Value};

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
    assert_eq!(
        decoded,
        vec![
            RowChange {
                table: "person".to_owned(),
                operation: RowOperation::Insert,
                key: vec![Column::new("id", 1_i64)],
                values: person(1, "ada", Value::Real(1.5)),
            },
            RowChange {
                table: "person".to_owned(),
                operation: RowOperation::Insert,
                key: vec![Column::new("id", 2_i64)],
                values: person(2, "bob", Value::Null),
            },
            RowChange {
                table: "person".to_owned(),
                operation: RowOperation::Update,
                key: vec![Column::new("id", 1_i64)],
                values: person(1, "ada", Value::Real(2.0)),
            },
            RowChange {
                table: "person".to_owned(),
                operation: RowOperation::Delete,
                key: vec![Column::new("id", 2_i64)],
                values: person(2, "bob", Value::Null),
            },
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
    assert_eq!(decoded[0].values, vec![Column::new("value", 7_i64)]);
    // Key columns in declared key order, not table order.
    assert_eq!(
        decoded[1].key,
        vec![Column::new("b", 9_i64), Column::new("a", "x")]
    );
    assert_eq!(decoded[1].values[2].value, Value::Blob(vec![1, 2]));
    assert_eq!(decoded[2].operation, RowOperation::Update);
    assert_eq!(decoded[2].values[2].value, Value::Null);

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
        .map(|row| row.values[0].value.clone())
        .collect();
    assert_eq!(values, vec![Value::Integer(7), Value::Integer(9)]);
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
        kind: ChangeKind::Logical(vec![RowChange {
            table: "t".to_owned(),
            operation: RowOperation::Delete,
            key: vec![Column::new("rowid", 3_i64)],
            values: vec![Column::new("blob", Value::Blob(vec![0, 7]))],
        }]),
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
