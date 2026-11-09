//! The reserved namespace: the objects the adapter keeps under names beginning with
//! `_fleetring_`, and the two checks that keep everything else out of it (FR-070).
//!
//! SQLite's authorizer sees a table's old name on `ALTER TABLE … RENAME TO`, never its new one,
//! so refusing statements by the names they mention can't stop a user table from being renamed
//! into the namespace, triggers and all. Two checks close that, each enough on its own:
//!
//! - **No user statement changes the namespace.** Before a write transaction's statements run,
//!   [`Reserved::remember`] reads every schema object in `main` and `temp` whose name or table
//!   is reserved; after each user statement, [`Reserved::changed`] reads them again, and any
//!   difference refuses the statement and rolls its transaction back. The read is skipped while
//!   neither schema's version has moved, so a statement that changes no schema costs two cookie
//!   reads. The recorder's temporary triggers, which sit on user tables and are rebuilt whenever
//!   the schema changes, are left out of the comparison; a user statement can't create one.
//! - **The adapter adopts no foreign object.** Before it records changes or reads or writes sink
//!   marks, the adapter calls [`Reserved::foreign`], which finds any object in the namespace that
//!   isn't one of its own: a change or marks table of another shape, any trigger on a reserved
//!   table, any index or view there, or any temporary object, which would shadow the adapter's
//!   tables. The adapter's own statements run with the confinement clear, so a trigger it adopted
//!   would run unconfined. A database holding such an object isn't recorded or marked, and the
//!   object is left in place: removing it is the operator's decision.

use std::fmt;

use rusqlite::{Connection, params};

use super::changes::{CHANGES_TABLE, RESERVED_PREFIX, TRIGGER_PREFIX};
use super::{marks, storage};
use crate::engine::EngineError;

/// Whether `name` is reserved for the adapter: whether it begins with [`RESERVED_PREFIX`],
/// ignoring ASCII case as SQLite's names do.
pub(super) fn is_reserved(name: &str) -> bool {
    name.as_bytes()
        .get(..RESERVED_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(RESERVED_PREFIX.as_bytes()))
}

/// Whether `sql` mentions the reserved prefix anywhere, in any ASCII case: in a name, a string, or
/// a comment alike.
pub(super) fn mentions_reserved(sql: &str) -> bool {
    sql.as_bytes()
        .windows(RESERVED_PREFIX.len())
        .any(|window| window.eq_ignore_ascii_case(RESERVED_PREFIX.as_bytes()))
}

/// The schema versions of `main` and `temp`. Every schema change in either moves its version.
type Versions = (i64, i64);

/// One schema object whose name or table is reserved.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    /// `main` or `temp`.
    schema: String,
    /// `table`, `index`, `view`, or `trigger`.
    kind: String,
    name: String,
    /// The table an index or trigger belongs to; an object's own name otherwise.
    table: String,
    /// The object's definition; `NULL` for an automatic index.
    sql: Option<String>,
}

impl Entry {
    /// Whether this is one of the recorder's temporary triggers, which sit on user tables.
    fn is_recorder_trigger(&self) -> bool {
        self.schema == "temp"
            && self.kind == "trigger"
            && self.name.starts_with(TRIGGER_PREFIX)
            && !is_reserved(&self.table)
    }
}

/// A column as `pragma_table_xinfo` reports it: name, declared type, `NOT NULL`, and position in
/// the primary key.
type ColumnShape = (&'static str, &'static str, bool, i64);

/// The change table's columns, as the recorder creates them.
const CHANGES_COLUMNS: &[ColumnShape] = &[
    ("change_id", "INTEGER", false, 1),
    ("change_txn_id", "INTEGER", true, 0),
    ("change_type", "INTEGER", true, 0),
    ("table_name", "TEXT", false, 0),
    ("id", "TEXT", false, 0),
    ("before", "TEXT", false, 0),
    ("after", "TEXT", false, 0),
    ("updates", "TEXT", false, 0),
    ("start", "BLOB", true, 0),
];

/// The marks table's columns, as the marks module creates them.
const MARKS_COLUMNS: &[ColumnShape] = &[("sink", "TEXT", true, 1), ("position", "BLOB", true, 0)];

/// A table the adapter creates.
struct OwnTable {
    name: &'static str,
    columns: &'static [ColumnShape],
    /// Whether it is `WITHOUT ROWID` and whether it is `STRICT`: the marks table is both, and the
    /// change table neither.
    without_rowid: bool,
    strict: bool,
}

const OWN_TABLES: [OwnTable; 2] = [
    OwnTable {
        name: CHANGES_TABLE,
        columns: CHANGES_COLUMNS,
        without_rowid: false,
        strict: false,
    },
    OwnTable {
        name: marks::TABLE,
        columns: MARKS_COLUMNS,
        without_rowid: true,
        strict: true,
    },
];

/// An object in the reserved namespace that the adapter didn't create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Foreign {
    /// The object's kind, as `sqlite_schema` names it.
    pub(super) kind: String,
    /// The object's name.
    pub(super) name: String,
    /// What is wrong with it.
    pub(super) reason: &'static str,
}

impl fmt::Display for Foreign {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} `{}` in the namespace reserved for fleetringd (`{RESERVED_PREFIX}`) isn't one fleetringd created ({}), so fleetringd won't record changes or sink marks in this database while it exists",
            self.kind, self.name, self.reason
        )
    }
}

/// The reserved part of one write connection's schema, as last read.
#[derive(Debug, Default)]
pub(super) struct Reserved {
    /// The objects whose name or table is reserved, but the recorder's triggers, at the
    /// versions they were read at.
    remembered: Option<(Versions, Vec<Entry>)>,
    /// The versions at which [`foreign`](Self::foreign) last found nothing.
    clean_at: Option<Versions>,
}

impl Reserved {
    /// Inside a write transaction, before its user statements: remembers the reserved part of
    /// the schema, unless it is already remembered at the current versions.
    pub(super) fn remember(&mut self, conn: &Connection) -> Result<(), EngineError> {
        let now = versions(conn)?;
        if self.remembered.as_ref().is_some_and(|(at, _)| *at == now) {
            return Ok(());
        }
        self.remembered = Some((now, comparable(conn)?));
        Ok(())
    }

    /// After a user statement: whether it changed the reserved part of the schema since
    /// [`remember`](Self::remember). Nothing remembered counts as a change.
    pub(super) fn changed(&mut self, conn: &Connection) -> Result<bool, EngineError> {
        let now = versions(conn)?;
        let Some((at, entries)) = &mut self.remembered else {
            return Ok(true);
        };
        if *at == now {
            return Ok(false);
        }
        if comparable(conn)? != *entries {
            return Ok(true);
        }
        *at = now;
        Ok(false)
    }

    /// Forgets what was read, after a transaction rolled back and took its schema changes with
    /// it, so that versions reached again later aren't mistaken for the ones read.
    pub(super) fn forget(&mut self) {
        self.remembered = None;
        self.clean_at = None;
    }

    /// The first object in the reserved namespace that the adapter didn't create, if any.
    pub(super) fn foreign(&mut self, conn: &Connection) -> Result<Option<Foreign>, EngineError> {
        let now = versions(conn)?;
        if self.clean_at == Some(now) {
            return Ok(None);
        }
        let found = find_foreign(conn)?;
        if found.is_none() {
            self.clean_at = Some(now);
        }
        Ok(found)
    }
}

fn versions(conn: &Connection) -> Result<Versions, EngineError> {
    let read = |sql: &str| -> Result<i64, EngineError> {
        conn.prepare_cached(sql)
            .map_err(storage)?
            .query_row([], |row| row.get(0))
            .map_err(storage)
    };
    Ok((
        read("PRAGMA main.schema_version")?,
        read("PRAGMA temp.schema_version")?,
    ))
}

/// Every object in `main` or `temp` whose name or table is reserved, in a fixed order.
fn entries(conn: &Connection) -> Result<Vec<Entry>, EngineError> {
    let mut statement = conn
        .prepare_cached(
            "SELECT 'main', type, name, tbl_name, sql FROM main.sqlite_master
              WHERE name LIKE ?1 ESCAPE '\\' OR tbl_name LIKE ?1 ESCAPE '\\'
             UNION ALL
             SELECT 'temp', type, name, tbl_name, sql FROM temp.sqlite_master
              WHERE name LIKE ?1 ESCAPE '\\' OR tbl_name LIKE ?1 ESCAPE '\\'
             ORDER BY 1, 2, 3, 4",
        )
        .map_err(storage)?;
    let pattern = format!("{}%", RESERVED_PREFIX.replace('_', "\\_"));
    statement
        .query_map(params![pattern], |row| {
            Ok(Entry {
                schema: row.get(0)?,
                kind: row.get(1)?,
                name: row.get(2)?,
                table: row.get(3)?,
                sql: row.get(4)?,
            })
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)
}

/// The entries a user statement must leave as they are: all but the recorder's triggers.
fn comparable(conn: &Connection) -> Result<Vec<Entry>, EngineError> {
    let mut entries = entries(conn)?;
    entries.retain(|entry| !entry.is_recorder_trigger());
    Ok(entries)
}

fn find_foreign(conn: &Connection) -> Result<Option<Foreign>, EngineError> {
    for entry in entries(conn)? {
        // An automatic index answers for its table, which is checked in its own right.
        if entry.is_recorder_trigger() || (entry.kind == "index" && entry.sql.is_none()) {
            continue;
        }
        let own = OWN_TABLES
            .iter()
            .find(|own| entry.schema == "main" && entry.kind == "table" && entry.name == own.name);
        let reason = match own {
            Some(own) => shape_mismatch(conn, own)?,
            None if entry.kind == "trigger" && is_reserved(&entry.table) => {
                Some("a trigger on a reserved table")
            }
            None if entry.schema == "temp" => Some("a temporary object"),
            None => Some("not an object fleetringd keeps"),
        };
        if let Some(reason) = reason {
            return Ok(Some(Foreign {
                kind: entry.kind,
                name: entry.name,
                reason,
            }));
        }
    }
    Ok(None)
}

/// What differs between `main`'s table named `own.name` and the table the adapter creates.
///
/// Its statements take the reserved name as a parameter, so their text doesn't mention the
/// reserved prefix; they are prepared afresh, never cached, so that no user statement of the same
/// text can take them from the cache and run them on a reserved table (see `prepare_user`).
fn shape_mismatch(conn: &Connection, own: &OwnTable) -> Result<Option<&'static str>, EngineError> {
    let (kind, without_rowid, strict): (String, bool, bool) = conn
        .prepare(
            "SELECT type, wr, strict FROM pragma_table_list WHERE schema = 'main' AND name = ?1",
        )
        .map_err(storage)?
        .query_row(params![own.name], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(storage)?;
    if kind != "table" || without_rowid != own.without_rowid || strict != own.strict {
        return Ok(Some("a table of another kind"));
    }
    let mut statement = conn
        .prepare(
            "SELECT name, type, \"notnull\", pk, dflt_value IS NULL AND hidden = 0
               FROM pragma_table_xinfo(?1, 'main') ORDER BY cid",
        )
        .map_err(storage)?;
    let columns = statement
        .query_map(params![own.name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, bool>(4)?,
            ))
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    let same = columns.len() == own.columns.len()
        && columns.iter().zip(own.columns).all(
            |(
                (name, kind, not_null, pk, plain),
                &(want_name, want_kind, want_not_null, want_pk),
            )| {
                name == want_name
                    && kind == want_kind
                    && *not_null == want_not_null
                    && *pk == want_pk
                    && *plain
            },
        );
    Ok((!same).then_some("a table with other columns"))
}
