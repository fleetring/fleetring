//! Row-change recording for the SQLite adapter (FR-039).
//!
//! The write connection carries one temporary trigger per table and operation. Each trigger
//! writes the row it saw, with its key and values, into a side table inside the database,
//! `_fleetring_changes`, keyed by the log position the transaction started from, which the
//! adapter puts in a temporary table at the start of every write transaction. The rows therefore
//! commit, replicate, and survive restarts exactly as the user's rows do, and a new primary
//! decodes them from its own copy. A change stream asks for the rows of a position range, and
//! the rows are deleted once every stream has delivered them.
//!
//! The triggers live in the connection's temporary schema: they never persist, replicate, or
//! show in the user's schema. They are rebuilt whenever the schema version changes, after each
//! statement, so a table created earlier in the same transaction is covered too.
//!
//! Not captured: schema changes (`CREATE`, `ALTER`, `DROP`), SQLite's own tables, and the rows
//! an `INSERT OR REPLACE` replaces, which SQLite reports only with recursive triggers.

use std::collections::BTreeSet;

use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;

use super::storage;
use crate::engine::{Column, EngineError, RowChange, RowOperation, Value};

/// The side table that holds recorded row changes, inside the database they belong to.
pub(super) const CHANGES_TABLE: &str = "_fleetring_changes";

/// The temporary table holding the position the current transaction started from.
const TXN_TABLE: &str = "_fleetring_txn";

/// The prefix of every temporary trigger the recorder creates.
const TRIGGER_PREFIX: &str = "_fleetring_";

/// The column the key of a row holds when its table declares no primary key.
const ROWID: &str = "rowid";

/// Records row changes on one write connection.
#[derive(Debug)]
pub(super) struct Recorder {
    /// The schema version the triggers were built for.
    schema_version: Option<i64>,
    /// The position every stream has passed, whose older rows the next transaction deletes.
    released: Option<Vec<u8>>,
}

impl Recorder {
    /// Creates the temporary table on `writer` and starts recording from the next transaction.
    pub(super) fn install(writer: &Connection) -> Result<Self, EngineError> {
        writer
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS \"{TXN_TABLE}\" (start BLOB NOT NULL);
                 DELETE FROM temp.\"{TXN_TABLE}\";
                 INSERT INTO temp.\"{TXN_TABLE}\" (start) VALUES (x'');"
            ))
            .map_err(storage)?;
        Ok(Self {
            schema_version: None,
            released: None,
        })
    }

    /// Prepares a transaction that has just begun on `writer`: makes sure the side table exists,
    /// deletes released rows, records `start`, the position the transaction starts from (`None`
    /// for an empty log), and brings the triggers up to date.
    pub(super) fn begin(
        &mut self,
        writer: &Connection,
        start: Option<fleetring_wal::Position>,
    ) -> Result<(), EngineError> {
        writer
            .execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS \"{CHANGES_TABLE}\" (
                    seq INTEGER PRIMARY KEY, start BLOB NOT NULL, tbl TEXT NOT NULL,
                    op INTEGER NOT NULL, key TEXT NOT NULL, row TEXT NOT NULL);
                 CREATE INDEX IF NOT EXISTS \"{CHANGES_TABLE}_start\" ON \"{CHANGES_TABLE}\" (start);"
            ))
            .map_err(storage)?;
        if let Some(before) = self.released.take() {
            writer
                .execute(
                    &format!("DELETE FROM \"{CHANGES_TABLE}\" WHERE start < ?1"),
                    params![before],
                )
                .map_err(storage)?;
        }
        writer
            .execute(
                &format!("UPDATE temp.\"{TXN_TABLE}\" SET start = ?1"),
                params![start_bytes(start)],
            )
            .map_err(storage)?;
        self.sync_triggers(writer)
    }

    /// Brings the triggers up to date after a statement, in case it changed the schema.
    pub(super) fn after_statement(&mut self, writer: &Connection) -> Result<(), EngineError> {
        self.sync_triggers(writer)
    }

    /// How many rows the triggers recorded for the transaction that started at `start`.
    pub(super) fn recorded(
        &self,
        writer: &Connection,
        start: Option<fleetring_wal::Position>,
    ) -> Result<u64, EngineError> {
        let count: i64 = writer
            .query_row(
                &format!("SELECT count(*) FROM \"{CHANGES_TABLE}\" WHERE start = ?1"),
                params![start_bytes(start)],
                |row| row.get(0),
            )
            .map_err(storage)?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    /// Remembers that rows of transactions started before `before` can go.
    pub(super) fn release(&mut self, before: &[u8]) {
        self.released = Some(before.to_vec());
    }

    /// Rebuilds the triggers if the schema changed since they were built.
    fn sync_triggers(&mut self, writer: &Connection) -> Result<(), EngineError> {
        let version: i64 = writer
            .query_row("PRAGMA schema_version", [], |row| row.get(0))
            .map_err(storage)?;
        if self.schema_version == Some(version) {
            return Ok(());
        }
        let tables = user_tables(writer)?;
        let mut sql = String::new();
        for trigger in existing_triggers(writer)? {
            sql.push_str(&format!(
                "DROP TRIGGER IF EXISTS temp.{};\n",
                quote(&trigger)
            ));
        }
        for table in &tables {
            let info = table_info(writer, table)?;
            sql.push_str(&triggers_for(table, &info));
        }
        writer.execute_batch(&sql).map_err(storage)?;
        self.schema_version = Some(version);
        Ok(())
    }
}

fn start_bytes(start: Option<fleetring_wal::Position>) -> Vec<u8> {
    start
        .map(|position| position.to_bytes().to_vec())
        .unwrap_or_default()
}

/// The tables the user owns: everything but SQLite's and the side table.
fn user_tables(conn: &Connection) -> Result<Vec<String>, EngineError> {
    let mut statement = conn
        .prepare(
            "SELECT name FROM main.sqlite_master WHERE type = 'table'
             AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name <> ?1 ORDER BY name",
        )
        .map_err(storage)?;
    let names = statement
        .query_map(params![CHANGES_TABLE], |row| row.get::<_, String>(0))
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    Ok(names)
}

/// The recorder's triggers in the temporary schema.
fn existing_triggers(conn: &Connection) -> Result<BTreeSet<String>, EngineError> {
    let mut statement = conn
        .prepare("SELECT name FROM temp.sqlite_master WHERE type = 'trigger' AND name LIKE ?1")
        .map_err(storage)?;
    let names = statement
        .query_map(params![format!("{TRIGGER_PREFIX}%")], |row| {
            row.get::<_, String>(0)
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<BTreeSet<_>>>()
        .map_err(storage)?;
    Ok(names)
}

/// A table's columns and primary key, from `PRAGMA table_info`.
#[derive(Debug, Clone)]
struct TableInfo {
    columns: Vec<String>,
    /// The declared primary key's columns, in key order.
    key: Vec<String>,
}

fn table_info(conn: &Connection, table: &str) -> Result<TableInfo, EngineError> {
    let mut columns = Vec::new();
    let mut key: Vec<(i64, String)> = Vec::new();
    conn.pragma(None, "table_info", table, |row| {
        let name: String = row.get("name")?;
        let pk: i64 = row.get("pk")?;
        if pk > 0 {
            key.push((pk, name.clone()));
        }
        columns.push(name);
        Ok(())
    })
    .map_err(storage)?;
    key.sort_unstable();
    Ok(TableInfo {
        columns,
        key: key.into_iter().map(|(_, name)| name).collect(),
    })
}

/// The SQL creating the three triggers of `table`.
fn triggers_for(table: &str, info: &TableInfo) -> String {
    let mut sql = String::new();
    let cases = [
        ("ins", "INSERT", "NEW", "NEW", 0),
        ("upd", "UPDATE", "OLD", "NEW", 1),
        ("del", "DELETE", "OLD", "OLD", 2),
    ];
    for (tag, event, key_row, value_row, op) in cases {
        let name = quote(&format!("{TRIGGER_PREFIX}{tag}_{table}"));
        let key = if info.key.is_empty() {
            format!("json_array(json_array('{ROWID}', 'integer', {key_row}.rowid))")
        } else {
            json_columns(key_row, &info.key)
        };
        let values = json_columns(value_row, &info.columns);
        sql.push_str(&format!(
            "CREATE TEMP TRIGGER {name} AFTER {event} ON main.{table_id} BEGIN
                INSERT INTO \"{CHANGES_TABLE}\" (start, tbl, op, key, row)
                VALUES ((SELECT start FROM temp.\"{TXN_TABLE}\"), {table_text}, {op}, {key}, {values});
             END;\n",
            table_id = quote(table),
            table_text = text(table),
        ));
    }
    sql
}

/// A `json_array` of `[name, type, value]` triples for `columns` of the trigger row `row`, with
/// blobs as hex so that JSON can hold them.
fn json_columns(row: &str, columns: &[String]) -> String {
    let items: Vec<String> = columns
        .iter()
        .map(|column| {
            let value = format!("{row}.{}", quote(column));
            format!(
                "json_array({name}, typeof({value}), CASE typeof({value}) WHEN 'blob' THEN hex({value}) ELSE {value} END)",
                name = text(column),
            )
        })
        .collect();
    format!("json_array({})", items.join(", "))
}

/// `name` as a quoted identifier.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `value` as a string literal.
fn text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// One recorded column: its name, SQLite type, and value as the trigger wrote it.
#[derive(Debug, Deserialize)]
struct Recorded(String, String, serde_json::Value);

fn column(recorded: Recorded) -> Result<Column, EngineError> {
    let Recorded(name, kind, value) = recorded;
    let invalid = || {
        storage(std::io::Error::other(format!(
            "recorded column {name} has an invalid {kind} value"
        )))
    };
    let value = match kind.as_str() {
        "null" => Value::Null,
        "integer" => Value::Integer(value.as_i64().ok_or_else(invalid)?),
        "real" => Value::Real(value.as_f64().ok_or_else(invalid)?),
        "text" => Value::Text(value.as_str().ok_or_else(invalid)?.to_owned()),
        "blob" => Value::Blob(unhex(value.as_str().ok_or_else(invalid)?).ok_or_else(invalid)?),
        _ => return Err(invalid()),
    };
    Ok(Column { name, value })
}

/// The bytes `text` spells in hex, as SQLite's `hex()` writes them.
fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| {
            text.get(at..at + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
        })
        .collect()
}

fn operation_from_code(code: i64) -> Result<RowOperation, EngineError> {
    match code {
        0 => Ok(RowOperation::Insert),
        1 => Ok(RowOperation::Update),
        2 => Ok(RowOperation::Delete),
        other => Err(storage(std::io::Error::other(format!(
            "unknown row operation code {other} in {CHANGES_TABLE}"
        )))),
    }
}

/// Reads the recorded changes of transactions that started at or after `lower` (none for the
/// log's beginning) and before `until`, in commit and statement order.
pub(super) fn changes_between(
    reader: &Connection,
    lower: Option<&[u8]>,
    until: &[u8],
) -> Result<Vec<RowChange>, EngineError> {
    let exists: Option<i64> = reader
        .query_row(
            "SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?1",
            params![CHANGES_TABLE],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    if exists.is_none() {
        return Ok(Vec::new());
    }
    let mut statement = reader
        .prepare_cached(&format!(
            "SELECT tbl, op, key, row FROM \"{CHANGES_TABLE}\"
             WHERE start >= ?1 AND start < ?2 ORDER BY seq"
        ))
        .map_err(storage)?;
    let lower = lower.unwrap_or_default();
    let rows = statement
        .query_map(params![lower, until], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(storage)?;
    let mut changes = Vec::new();
    for row in rows {
        let (table, op, key, values) = row.map_err(storage)?;
        let decode = |json: &str| -> Result<Vec<Column>, EngineError> {
            serde_json::from_str::<Vec<Recorded>>(json)
                .map_err(storage)?
                .into_iter()
                .map(column)
                .collect()
        };
        changes.push(RowChange {
            table,
            operation: operation_from_code(op)?,
            key: decode(&key)?,
            values: decode(&values)?,
        });
    }
    Ok(changes)
}
