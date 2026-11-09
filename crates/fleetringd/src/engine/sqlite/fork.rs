//! The fork-reset write for the SQLite adapter (ADR-0046, point 6).
//!
//! A fork starts as a byte copy of its source, change record (`_fleetring_cdc`) and sink marks
//! (`_fleetring_sinks`) included, though both describe the source's history. The reset clears them
//! and records a marker naming the fork in a third table in the reserved namespace,
//! `_fleetring_fork`, all in one write transaction, so the marker commits, replicates, and
//! survives exactly when the reset does. The table holds exactly one row, the last reset's:
//!
//! | Column | Holds |
//! |---|---|
//! | `fork_incarnation` | the fork's incarnation, its 64 bits stored as a signed integer |
//! | `operation` | the fork operation's 16-byte ID |
//! | `source_incarnation` | the source's incarnation, stored as `fork_incarnation` is |
//! | `at` | the source's position the fork was seeded at, as the adapter's opaque bytes |
//!
//! The reset is conditional: inside its transaction it first looks for a row with the fork's
//! incarnation and operation, and writes nothing if there is one. A fork of a fork inherits its
//! source's row, which names another fork, so its reset runs and leaves only its own row.
//!
//! The transaction runs none of the user's statements, so the recorder's triggers, which sit on
//! user tables only, record nothing of it, and the adapter writes no commit record for it.

use rusqlite::{Connection, OptionalExtension, params};

use super::marks::Marks;
use super::{changes, storage};
use crate::engine::{EngineError, ForkMarker, Position};

/// The marker table, inside the database it belongs to.
pub(super) const TABLE: &str = "_fleetring_fork";

const CREATE: &str = "CREATE TABLE IF NOT EXISTS _fleetring_fork (\
    fork_incarnation INTEGER NOT NULL, \
    operation BLOB NOT NULL, \
    source_incarnation INTEGER NOT NULL, \
    at BLOB NOT NULL, \
    PRIMARY KEY (fork_incarnation, operation)) STRICT, WITHOUT ROWID";

const HELD: &str =
    "SELECT 1 FROM _fleetring_fork WHERE fork_incarnation = ?1 AND operation = ?2 LIMIT 1";

const CLEAR: &str = "DELETE FROM _fleetring_fork";

const INSERT: &str = "INSERT INTO _fleetring_fork \
    (fork_incarnation, operation, source_incarnation, at) VALUES (?1, ?2, ?3, ?4)";

/// At most two rows, so that a table holding more than its one is noticed.
const READ: &str = "SELECT fork_incarnation, operation, source_incarnation, at \
    FROM _fleetring_fork ORDER BY fork_incarnation, operation LIMIT 2";

/// What [`reset`] did inside its transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    /// The fork's marker was already there; nothing was written.
    Held,
    /// The reset was written, and commits with the transaction.
    Written,
}

/// Inside an open write transaction that starts at `start`: unless the database already holds
/// `marker`'s fork's marker, clears the change record and the sink marks and replaces the marker
/// table's contents with `marker`.
pub(super) fn reset(
    writer: &Connection,
    marker: &ForkMarker,
    start: Option<fleetring_wal::Position>,
) -> Result<Outcome, EngineError> {
    if table_exists(writer)? && holds(writer, marker)? {
        return Ok(Outcome::Held);
    }
    changes::clear(writer, start)?;
    Marks::clear(writer)?;
    writer.execute_batch(CREATE).map_err(storage)?;
    writer
        .prepare_cached(CLEAR)
        .map_err(storage)?
        .execute([])
        .map_err(storage)?;
    writer
        .prepare_cached(INSERT)
        .map_err(storage)?
        .execute(params![
            marker.fork_incarnation.cast_signed(),
            marker.operation,
            marker.source_incarnation.cast_signed(),
            marker.at.as_bytes(),
        ])
        .map_err(storage)?;
    Ok(Outcome::Written)
}

/// The marker the database holds, if any.
///
/// # Errors
///
/// [`EngineError::Storage`] if the table can't be read or holds anything but one well-formed
/// row.
pub(super) fn read(conn: &Connection) -> Result<Option<ForkMarker>, EngineError> {
    if !table_exists(conn)? {
        return Ok(None);
    }
    let mut statement = conn.prepare_cached(READ).map_err(storage)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    if rows.len() > 1 {
        return Err(malformed("it holds more than one marker"));
    }
    let Some((fork_incarnation, operation, source_incarnation, at)) = rows.into_iter().next()
    else {
        return Ok(None);
    };
    let operation: [u8; 16] = operation
        .try_into()
        .map_err(|_| malformed("its operation isn't 16 bytes"))?;
    Ok(Some(ForkMarker {
        fork_incarnation: fork_incarnation.cast_unsigned(),
        operation,
        source_incarnation: source_incarnation.cast_unsigned(),
        at: Position::from(at),
    }))
}

/// Whether the marker table holds `marker`'s fork's row.
fn holds(conn: &Connection, marker: &ForkMarker) -> Result<bool, EngineError> {
    Ok(conn
        .prepare_cached(HELD)
        .map_err(storage)?
        .query_row(
            params![marker.fork_incarnation.cast_signed(), marker.operation],
            |_| Ok(()),
        )
        .optional()
        .map_err(storage)?
        .is_some())
}

/// Whether the marker table exists in the database `conn` reads.
fn table_exists(conn: &Connection) -> Result<bool, EngineError> {
    Ok(conn
        .prepare_cached("SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?1")
        .map_err(storage)?
        .query_row(params![TABLE], |_| Ok(()))
        .optional()
        .map_err(storage)?
        .is_some())
}

fn malformed(reason: &str) -> EngineError {
    storage(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("the fork marker table `{TABLE}` is malformed: {reason}"),
    ))
}
