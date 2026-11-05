//! A database's schema from SQLite's catalog (ADR-0041, point 6).
//!
//! This is the only place that reads `sqlite_schema`. The statements are the adapter's own, so
//! they run with the confinement of user statements off, on the read connection: in WAL mode
//! that connection never blocks the writer.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OptionalExtension, params};

use super::changes::RESERVED_PREFIX;
use super::storage;
use crate::engine::{
    EngineError, Internal, MAX_COUNT_LIMIT, MAX_SCHEMA_OBJECTS, ObjectKind, RowCount, Schema,
    SchemaObject, SchemaRequest,
};

/// The most rows one schema request scans across all its tables. The per-table cap bounds one
/// table; this bounds a database of many large tables. The database's thread does the counting,
/// so a write queued behind the request waits for it: at about a million rows a second or
/// better, this keeps that to a second at most. Tables left once it is spent get no count.
const SCAN_BUDGET: u64 = 1_000_000;

/// What SQLite's catalog says about one object, before its definition is read.
#[derive(Debug)]
struct Entry {
    kind: ObjectKind,
    name: String,
    /// The table the object belongs to: for a table or a view, itself.
    owner: String,
    /// Whether SQLite generated the object, so that it has no definition.
    automatic: bool,
}

/// Reads the schema of the database `conn` is open on.
pub(super) fn read(conn: &Connection, request: &SchemaRequest) -> Result<Schema, EngineError> {
    read_within(conn, request, SCAN_BUDGET)
}

/// As [`read`], scanning at most `budget` rows in all.
pub(super) fn read_within(
    conn: &Connection,
    request: &SchemaRequest,
    budget: u64,
) -> Result<Schema, EngineError> {
    let table_types = table_types(conn)?;
    let mut entries = entries(conn, &table_types)?;
    let hidden: BTreeSet<String> = match request.internal {
        Internal::Show => BTreeSet::new(),
        Internal::Hide => entries
            .iter()
            .filter(|entry| is_internal(entry, &table_types))
            .map(|entry| entry.name.clone())
            .collect(),
    };
    entries.retain(|entry| {
        let internal = request.internal == Internal::Hide
            && (entry.automatic
                || has_prefix(&entry.name, "sqlite_")
                || hidden.contains(&entry.owner));
        !is_reserved(entry) && !internal
    });
    // Each table, then its indexes and triggers.
    entries.sort_by(|a, b| (&a.owner, a.kind, &a.name).cmp(&(&b.owner, b.kind, &b.name)));
    let truncated = entries.len() > MAX_SCHEMA_OBJECTS;
    entries.truncate(MAX_SCHEMA_OBJECTS);

    let mut remaining = budget;
    let mut objects = Vec::with_capacity(entries.len());
    for entry in entries {
        let rows = match entry.kind {
            ObjectKind::Table => count(conn, &entry.name, request.count_limit, &mut remaining)?,
            _ => None,
        };
        let definition = definition(conn, &entry)?;
        let table = match entry.kind {
            ObjectKind::Index | ObjectKind::Trigger => Some(entry.owner),
            _ => None,
        };
        objects.push(SchemaObject {
            kind: entry.kind,
            name: entry.name,
            table,
            definition,
            rows,
        });
    }
    Ok(Schema { objects, truncated })
}

/// Each table's type in `pragma_table_list`: `table`, `view`, `virtual`, or `shadow`.
fn table_types(conn: &Connection) -> Result<BTreeMap<String, String>, EngineError> {
    let mut statement = conn
        .prepare("SELECT name, type FROM pragma_table_list WHERE schema = 'main'")
        .map_err(storage)?;
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(storage)?;
    rows.collect::<Result<_, _>>().map_err(storage)
}

/// Every object in the catalog, without its definition.
fn entries(
    conn: &Connection,
    table_types: &BTreeMap<String, String>,
) -> Result<Vec<Entry>, EngineError> {
    let mut statement = conn
        .prepare("SELECT type, name, tbl_name, sql IS NULL FROM main.sqlite_schema")
        .map_err(storage)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })
        .map_err(storage)?;
    let mut entries = Vec::new();
    for row in rows {
        let (kind, name, table, automatic) = row.map_err(storage)?;
        let (kind, owner) = match kind.as_str() {
            "table" if table_types.get(&name).is_some_and(|kind| kind == "virtual") => {
                (ObjectKind::VirtualTable, name.clone())
            }
            "table" => (ObjectKind::Table, name.clone()),
            "view" => (ObjectKind::View, name.clone()),
            "index" => (ObjectKind::Index, table),
            "trigger" => (ObjectKind::Trigger, table),
            _ => continue,
        };
        entries.push(Entry {
            kind,
            name,
            owner,
            automatic,
        });
    }
    Ok(entries)
}

/// Whether `entry` is a table SQLite keeps for itself: one named `sqlite_...`, or a virtual
/// table's shadow table.
fn is_internal(entry: &Entry, table_types: &BTreeMap<String, String>) -> bool {
    matches!(entry.kind, ObjectKind::Table | ObjectKind::VirtualTable)
        && (has_prefix(&entry.name, "sqlite_")
            || table_types
                .get(&entry.name)
                .is_some_and(|kind| kind == "shadow"))
}

/// Whether `entry`, or the table it belongs to, is reserved for fleetringd.
fn is_reserved(entry: &Entry) -> bool {
    entry.name.starts_with(RESERVED_PREFIX) || entry.owner.starts_with(RESERVED_PREFIX)
}

/// Whether `name` starts with `prefix`, ignoring ASCII case as SQLite does for its own names.
fn has_prefix(name: &str, prefix: &str) -> bool {
    name.get(..prefix.len())
        .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
}

/// The `CREATE` statement that made `entry`, if SQLite kept one.
fn definition(conn: &Connection, entry: &Entry) -> Result<Option<String>, EngineError> {
    let kind = match entry.kind {
        ObjectKind::Table | ObjectKind::VirtualTable => "table",
        ObjectKind::View => "view",
        ObjectKind::Index => "index",
        ObjectKind::Trigger => "trigger",
    };
    let sql: Option<Option<String>> = conn
        .query_row(
            "SELECT sql FROM main.sqlite_schema WHERE type = ?1 AND name = ?2",
            params![kind, entry.name],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    Ok(sql.flatten())
}

/// Counts the rows of table `name` up to `limit` (itself capped at [`MAX_COUNT_LIMIT`]) and the
/// scan budget `remaining`, which it spends.
///
/// The count reads at most one row past its limit, so a table of any size costs a bounded scan.
/// `None` if `limit` is 0 or the budget is spent.
fn count(
    conn: &Connection,
    name: &str,
    limit: u64,
    remaining: &mut u64,
) -> Result<Option<RowCount>, EngineError> {
    let limit = limit.min(MAX_COUNT_LIMIT).min(*remaining);
    if limit == 0 {
        return Ok(None);
    }
    let quoted = name.replace('"', "\"\"");
    let sql = format!("SELECT count(*) FROM (SELECT 1 FROM main.\"{quoted}\" LIMIT ?1)");
    let scan = i64::try_from(limit.saturating_add(1)).map_err(storage)?;
    let found: i64 = conn
        .query_row(&sql, [scan], |row| row.get(0))
        .map_err(storage)?;
    let found = u64::try_from(found).map_err(storage)?;
    *remaining = remaining.saturating_sub(found);
    Ok(Some(if found > limit {
        RowCount::AtLeast(limit)
    } else {
        RowCount::Exact(found)
    }))
}
