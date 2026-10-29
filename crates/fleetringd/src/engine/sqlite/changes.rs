//! Row-change recording for the SQLite adapter (FR-039, ADR-0024).
//!
//! The write connection carries one temporary trigger per table and operation. Each trigger
//! writes the row it saw into a change table inside the database, `_fleetring_cdc`, shaped like
//! Turso's `turso_cdc` so that one decoder reads either. The rows therefore commit, replicate,
//! and survive restarts exactly as the user's rows do, and a new primary decodes them from its
//! own copy. A change stream asks for the rows of a position range, and the rows are deleted once
//! every stream has delivered them.
//!
//! # The change table
//!
//! | Column | Turso | Here |
//! |---|---|---|
//! | `change_id` | `INTEGER PRIMARY KEY` | the same |
//! | `change_time` | seconds since the epoch | absent: the adapter reads no clock |
//! | `change_txn_id` | the transaction | the `change_id` the transaction's first row got |
//! | `change_type` | 1 insert, 0 update, -1 delete, 2 commit | the same |
//! | `table_name` | the table | the same; `NULL` on a commit record |
//! | `id` | the row's rowid | the row's key as JSON, so composite and `WITHOUT ROWID` keys fit |
//! | `before` | the old row as a record | the old row as JSON, when the mode records it |
//! | `after` | the new row as a record | the new row as JSON, when the mode records it |
//! | `updates` | the changed columns as a record | the changed columns' names as a JSON array |
//! | `start` | — | the log position the transaction started from, which keys the range reads |
//!
//! Row JSON is one flat array of `name, type, value` triples, with blobs as hex. One commit
//! record follows the rows of every transaction that recorded any.
//!
//! The table has no index: `change_id`s are contiguous (rows are only ever pruned from the
//! front) and `start` never decreases along them, so a position bound is found by binary search
//! over `change_id` with point lookups, and range reads and prunes are rowid ranges. That keeps
//! every captured transaction to one extra page in the log.
//!
//! The triggers live in the connection's temporary schema: they never persist, replicate, or
//! show in the user's schema. They are rebuilt whenever the schema version changes, which is
//! checked after every statement that isn't plain DML by its first keyword, so a table created
//! earlier in the same transaction is covered too. The write connection runs with recursive
//! triggers, so the rows an `INSERT OR REPLACE` replaces are recorded as deletes.
//!
//! The per-transaction work is kept to a few cached statements: the change table is created
//! once per connection, the transaction's position and id reach the triggers through two scalar
//! functions on the connection rather than a table, the commit record is one insert, and
//! released rows are pruned at most once every [`PRUNE_EVERY`] transactions.
//!
//! Not captured: schema changes (`CREATE`, `ALTER`, `DROP`), including a table rename, and
//! SQLite's own tables.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;

use super::storage;
use crate::engine::{CaptureMode, Column, EngineError, RowChange, RowOperation, Value};

/// The prefix of every name the adapter reserves: its change table, its functions, and its
/// triggers.
pub(super) const RESERVED_PREFIX: &str = "_fleetring_";

/// The change table, inside the database it belongs to.
pub(super) const CHANGES_TABLE: &str = "_fleetring_cdc";

/// The scalar function that gives the triggers the position the current transaction started from.
const START_FUNCTION: &str = "_fleetring_start";

/// The scalar function that gives the triggers the current transaction's id.
const TXN_FUNCTION: &str = "_fleetring_txn";

/// The prefix of every temporary trigger the recorder creates.
pub(super) const TRIGGER_PREFIX: &str = "_fleetring_trg_";

/// The column the key of a row holds when its table declares no primary key.
const ROWID: &str = "rowid";

/// How many transactions may pass between prunes of released rows. A prune costs a range
/// delete, so it is amortized; the table holds at most this many delivered transactions.
const PRUNE_EVERY: u32 = 64;

/// Turso's change types.
const INSERT: i64 = 1;
const UPDATE: i64 = 0;
const DELETE: i64 = -1;
const COMMIT: i64 = 2;

/// The current transaction, as the connection's scalar functions read it.
#[derive(Debug, Default)]
struct Txn {
    /// The position the transaction started from, encoded; empty for an empty log.
    start: Mutex<Vec<u8>>,
    /// The `change_id` the transaction's first row gets.
    id: AtomicI64,
}

/// Records row changes on one write connection.
#[derive(Debug)]
pub(super) struct Recorder {
    mode: CaptureMode,
    /// The current transaction, shared with the connection's functions.
    txn: Arc<Txn>,
    /// The schema version the triggers were built for.
    schema_version: Option<i64>,
    /// Whether the change table has been created on this connection.
    created: bool,
    /// The position every stream has passed, whose older rows the next prune deletes.
    released: Option<Vec<u8>>,
    /// Transactions begun since the last prune.
    since_prune: u32,
}

impl Recorder {
    /// Installs the transaction functions on `writer` and starts recording from the next
    /// transaction.
    pub(super) fn install(writer: &Connection, mode: CaptureMode) -> Result<Self, EngineError> {
        let txn = Arc::new(Txn::default());
        let start = Arc::clone(&txn);
        writer
            .create_scalar_function(START_FUNCTION, 0, FunctionFlags::SQLITE_UTF8, move |_| {
                Ok(start
                    .start
                    .lock()
                    .expect("transaction lock poisoned")
                    .clone())
            })
            .map_err(storage)?;
        let id = Arc::clone(&txn);
        writer
            .create_scalar_function(TXN_FUNCTION, 0, FunctionFlags::SQLITE_UTF8, move |_| {
                Ok(id.id.load(Ordering::Relaxed))
            })
            .map_err(storage)?;
        Ok(Self {
            mode,
            txn,
            schema_version: None,
            created: false,
            released: None,
            since_prune: 0,
        })
    }

    /// The mode recording runs in.
    pub(super) fn mode(&self) -> CaptureMode {
        self.mode
    }

    /// Prepares a transaction that has just begun on `writer`: creates the change table the first
    /// time, prunes released rows now and then, records `start`, the position the transaction
    /// starts from (`None` for an empty log), and builds the triggers if they aren't built yet.
    pub(super) fn begin(
        &mut self,
        writer: &Connection,
        start: Option<fleetring_wal::Position>,
    ) -> Result<(), EngineError> {
        if !self.created {
            writer
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS \"{CHANGES_TABLE}\" (
                        change_id INTEGER PRIMARY KEY,
                        change_txn_id INTEGER NOT NULL,
                        change_type INTEGER NOT NULL,
                        table_name TEXT,
                        id TEXT,
                        before TEXT,
                        after TEXT,
                        updates TEXT,
                        start BLOB NOT NULL);"
                ))
                .map_err(storage)?;
            self.created = true;
        }
        self.since_prune = self.since_prune.saturating_add(1);
        if self.since_prune >= PRUNE_EVERY
            && let Some(before) = self.released.take()
        {
            if let Some(first_kept) = first_id_at_or_after(writer, &before)? {
                writer
                    .prepare_cached(&format!(
                        "DELETE FROM \"{CHANGES_TABLE}\" WHERE change_id < ?1"
                    ))
                    .map_err(storage)?
                    .execute(params![first_kept])
                    .map_err(storage)?;
            }
            self.since_prune = 0;
        }
        let next = highest_id(writer)?.saturating_add(1);
        self.txn.id.store(next, Ordering::Relaxed);
        *self.txn.start.lock().expect("transaction lock poisoned") = start_bytes(start);
        if self.schema_version.is_none() {
            self.sync_triggers(writer)?;
        }
        Ok(())
    }

    /// Brings the triggers up to date after the statement `sql`, if it could have changed the
    /// schema: plain DML, by its first keyword, can't, and is the only kind that skips the
    /// check.
    pub(super) fn after_statement(
        &mut self,
        writer: &Connection,
        sql: &str,
    ) -> Result<(), EngineError> {
        if is_plain_dml(sql) {
            return Ok(());
        }
        self.sync_triggers(writer)
    }

    /// Ends the recording of the current transaction, before it commits: writes its commit
    /// record if the triggers recorded any row, and returns how many rows the change table
    /// gained, the commit record included.
    pub(super) fn commit(&self, writer: &Connection) -> Result<u64, EngineError> {
        // The transaction's rows got the ids from its id on, so the highest id tells how many.
        let txn = self.txn.id.load(Ordering::Relaxed);
        let recorded = u64::try_from(highest_id(writer)? - txn + 1).unwrap_or(0);
        if recorded == 0 {
            return Ok(0);
        }
        let start = self
            .txn
            .start
            .lock()
            .expect("transaction lock poisoned")
            .clone();
        writer
            .prepare_cached(&format!(
                "INSERT INTO \"{CHANGES_TABLE}\" (change_txn_id, change_type, start)
                 VALUES (?1, {COMMIT}, ?2)"
            ))
            .map_err(storage)?
            .execute(params![txn, start])
            .map_err(storage)?;
        Ok(recorded + 1)
    }

    /// Remembers that rows of transactions started before `before` can go.
    pub(super) fn release(&mut self, before: &[u8]) {
        self.released = Some(before.to_vec());
    }

    /// Rebuilds the triggers if the schema changed since they were built.
    fn sync_triggers(&mut self, writer: &Connection) -> Result<(), EngineError> {
        let version: i64 = writer
            .prepare_cached("PRAGMA schema_version")
            .map_err(storage)?
            .query_row([], |row| row.get(0))
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
            sql.push_str(&triggers_for(table, &info, self.mode));
        }
        writer.execute_batch(&sql).map_err(storage)?;
        self.schema_version = Some(version);
        Ok(())
    }
}

/// Whether `sql` starts with a keyword that only reads or writes rows, so it can't change the
/// schema: `SELECT`, `INSERT`, `REPLACE`, `UPDATE`, `DELETE`, `WITH`, or `VALUES`.
fn is_plain_dml(sql: &str) -> bool {
    let word: String = sql
        .trim_start()
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .map(|c| c.to_ascii_uppercase())
        .collect();
    matches!(
        word.as_str(),
        "SELECT" | "INSERT" | "REPLACE" | "UPDATE" | "DELETE" | "WITH" | "VALUES"
    )
}

/// The first `change_id` whose `start` is at or after `bound`, or `None` if the table is empty.
/// A table that holds only earlier transactions gives the id just past its last row.
///
/// `change_id`s are contiguous and `start` never decreases along them, so this is a binary
/// search with one point lookup per step.
fn first_id_at_or_after(conn: &Connection, bound: &[u8]) -> Result<Option<i64>, EngineError> {
    let (min, max): (Option<i64>, Option<i64>) = conn
        .prepare_cached(&format!(
            "SELECT min(change_id), max(change_id) FROM \"{CHANGES_TABLE}\""
        ))
        .map_err(storage)?
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(storage)?;
    let (Some(mut low), Some(max)) = (min, max) else {
        return Ok(None);
    };
    let mut start_of = conn
        .prepare_cached(&format!(
            "SELECT start FROM \"{CHANGES_TABLE}\" WHERE change_id = ?1"
        ))
        .map_err(storage)?;
    let mut high = max.saturating_add(1);
    while low < high {
        let mid = low + (high - low) / 2;
        let start: Vec<u8> = start_of
            .query_row(params![mid], |row| row.get(0))
            .map_err(storage)?;
        if start.as_slice() < bound {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(Some(low))
}

/// The highest `change_id` in the change table, or 0 if it is empty.
fn highest_id(writer: &Connection) -> Result<i64, EngineError> {
    writer
        .prepare_cached(&format!(
            "SELECT coalesce(max(change_id), 0) FROM \"{CHANGES_TABLE}\""
        ))
        .map_err(storage)?
        .query_row([], |row| row.get(0))
        .map_err(storage)
}

fn start_bytes(start: Option<fleetring_wal::Position>) -> Vec<u8> {
    start
        .map(|position| position.to_bytes().to_vec())
        .unwrap_or_default()
}

/// The tables the user owns: everything but SQLite's and the adapter's.
fn user_tables(conn: &Connection) -> Result<Vec<String>, EngineError> {
    let mut statement = conn
        .prepare(
            "SELECT name FROM main.sqlite_master WHERE type = 'table'
             AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE ?1 ESCAPE '\\'
             ORDER BY name",
        )
        .map_err(storage)?;
    let names = statement
        .query_map(params![reserved_pattern()], |row| row.get::<_, String>(0))
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    Ok(names)
}

/// A `LIKE` pattern matching reserved names, with the prefix's underscores escaped.
fn reserved_pattern() -> String {
    format!("{}%", RESERVED_PREFIX.replace('_', "\\_"))
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

/// The SQL creating the three triggers of `table`, recording what `mode` asks for.
fn triggers_for(table: &str, info: &TableInfo, mode: CaptureMode) -> String {
    let mut sql = String::new();
    let old = || json_columns("OLD", &info.columns);
    let new = || json_columns("NEW", &info.columns);
    let null = || "NULL".to_owned();
    let cases = [
        (
            "ins",
            "INSERT",
            "NEW",
            INSERT,
            null(),
            on_after(mode, new),
            null(),
        ),
        (
            "upd",
            "UPDATE",
            "OLD",
            UPDATE,
            on_full(mode, old),
            on_after(mode, new),
            on_full(mode, || updated_columns(&info.columns)),
        ),
        (
            "del",
            "DELETE",
            "OLD",
            DELETE,
            on_full(mode, old),
            null(),
            null(),
        ),
    ];
    for (tag, event, key_row, change_type, before, after, updates) in cases {
        let name = quote(&format!("{TRIGGER_PREFIX}{tag}_{table}"));
        let key = if info.key.is_empty() {
            format!("json_array('{ROWID}', 'integer', {key_row}.rowid)")
        } else {
            json_columns(key_row, &info.key)
        };
        sql.push_str(&format!(
            "CREATE TEMP TRIGGER {name} AFTER {event} ON main.{table_id} BEGIN
                INSERT INTO \"{CHANGES_TABLE}\"
                    (change_txn_id, change_type, table_name, id, before, after, updates, start)
                VALUES ({TXN_FUNCTION}(), {change_type}, {table_text}, {key}, {before}, {after},
                        {updates}, {START_FUNCTION}());
             END;\n",
            table_id = quote(table),
            table_text = text(table),
        ));
    }
    sql
}

/// `expression()` in the `after` and `full` modes, `NULL` otherwise.
fn on_after(mode: CaptureMode, expression: impl FnOnce() -> String) -> String {
    if mode >= CaptureMode::After {
        expression()
    } else {
        "NULL".to_owned()
    }
}

/// `expression()` in the `full` mode, `NULL` otherwise.
fn on_full(mode: CaptureMode, expression: impl FnOnce() -> String) -> String {
    if mode >= CaptureMode::Full {
        expression()
    } else {
        "NULL".to_owned()
    }
}

/// One flat `json_array` of `name, type, value` triples for `columns` of the trigger row `row`,
/// with blobs as hex so that JSON can hold them.
fn json_columns(row: &str, columns: &[String]) -> String {
    let items: Vec<String> = columns
        .iter()
        .map(|column| {
            let value = format!("{row}.{}", quote(column));
            format!(
                "{name}, typeof({value}), CASE typeof({value}) WHEN 'blob' THEN hex({value}) ELSE {value} END",
                name = text(column),
            )
        })
        .collect();
    format!("json_array({})", items.join(", "))
}

/// A JSON array of the names of `columns` whose value an update changed, in table order.
fn updated_columns(columns: &[String]) -> String {
    let arms: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            format!(
                "SELECT {index} AS n, {name} AS name WHERE OLD.{id} IS NOT NEW.{id}",
                name = text(column),
                id = quote(column),
            )
        })
        .collect();
    format!(
        "(SELECT json_group_array(name) FROM ({} ORDER BY n))",
        arms.join(" UNION ALL ")
    )
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

/// The operation a row record's `change_type` names, or `None` for a commit record.
fn operation_from_type(change_type: i64) -> Result<Option<RowOperation>, EngineError> {
    match change_type {
        INSERT => Ok(Some(RowOperation::Insert)),
        UPDATE => Ok(Some(RowOperation::Update)),
        DELETE => Ok(Some(RowOperation::Delete)),
        COMMIT => Ok(None),
        other => Err(storage(std::io::Error::other(format!(
            "unknown change type {other} in {CHANGES_TABLE}"
        )))),
    }
}

/// One row of the change table, as read.
struct Row {
    change_type: i64,
    table: Option<String>,
    id: Option<String>,
    before: Option<String>,
    after: Option<String>,
    updates: Option<String>,
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
    let Some(first) = first_id_at_or_after(reader, lower.unwrap_or_default())? else {
        return Ok(Vec::new());
    };
    let Some(past) = first_id_at_or_after(reader, until)? else {
        return Ok(Vec::new());
    };
    let mut statement = reader
        .prepare_cached(&format!(
            "SELECT change_type, table_name, id, before, after, updates FROM \"{CHANGES_TABLE}\"
             WHERE change_id >= ?1 AND change_id < ?2 ORDER BY change_id"
        ))
        .map_err(storage)?;
    let rows = statement
        .query_map(params![first, past], |row| {
            Ok(Row {
                change_type: row.get(0)?,
                table: row.get(1)?,
                id: row.get(2)?,
                before: row.get(3)?,
                after: row.get(4)?,
                updates: row.get(5)?,
            })
        })
        .map_err(storage)?;
    let mut changes = Vec::new();
    for row in rows {
        let row = row.map_err(storage)?;
        let Some(operation) = operation_from_type(row.change_type)? else {
            continue;
        };
        let missing = |what: &str| {
            storage(std::io::Error::other(format!(
                "a {operation} record in {CHANGES_TABLE} has no {what}"
            )))
        };
        let table = row.table.ok_or_else(|| missing("table name"))?;
        let key = decode_columns(&row.id.ok_or_else(|| missing("key"))?)?;
        let mut change = RowChange::new(table, operation, key);
        change.before = row.before.as_deref().map(decode_columns).transpose()?;
        change.after = row.after.as_deref().map(decode_columns).transpose()?;
        change.updated = row
            .updates
            .as_deref()
            .map(|json| serde_json::from_str::<Vec<String>>(json).map_err(storage))
            .transpose()?;
        changes.push(change);
    }
    Ok(changes)
}

/// Decodes a flat array of `name, type, value` triples.
fn decode_columns(json: &str) -> Result<Vec<Column>, EngineError> {
    let flat: Vec<serde_json::Value> = serde_json::from_str(json).map_err(storage)?;
    if !flat.len().is_multiple_of(3) {
        return Err(storage(std::io::Error::other(format!(
            "a row record in {CHANGES_TABLE} has {} values, not a multiple of three",
            flat.len()
        ))));
    }
    let malformed = || {
        storage(std::io::Error::other(
            "a row record has a non-text name or type",
        ))
    };
    flat.as_chunks::<3>()
        .0
        .iter()
        .map(|triple| {
            let name = triple[0].as_str().ok_or_else(malformed)?.to_owned();
            let kind = triple[1].as_str().ok_or_else(malformed)?.to_owned();
            column(Recorded(name, kind, triple[2].clone()))
        })
        .collect()
}
