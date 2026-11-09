//! Sink marks for the SQLite adapter (FR-041, ADR-0028).
//!
//! A change sink's delivered position lives in a table inside the database, `_fleetring_sinks`,
//! so that it commits, replicates, and is fenced exactly as the user's rows are, and a promoted
//! follower reads it from its own copy. The table has one row per sink:
//!
//! | Column | Holds |
//! |---|---|
//! | `sink` | the sink's name, the primary key |
//! | `position` | the last position that sink was delivered, as the adapter's opaque bytes |
//!
//! The table is created by the first write that carries a mark, so a database that no sink names
//! never has one.
//!
//! A mark is staged in memory after its sink has taken a batch, and written by the next write
//! transaction, at its start and before the transaction's own statements and before any prune of
//! the change table, so that the marks, the user's write, and a prune commit or roll back
//! together. A transaction that rolls back leaves the marks staged. No transaction is ever
//! started just to write a mark: it would be a new batch, whose delivery would stage a new mark,
//! and so on forever.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, params};

use super::storage;
use crate::engine::EngineError;

/// The marks table, inside the database it belongs to.
pub(super) const TABLE: &str = "_fleetring_sinks";

const CREATE: &str = "CREATE TABLE IF NOT EXISTS _fleetring_sinks \
    (sink TEXT PRIMARY KEY, position BLOB NOT NULL) STRICT, WITHOUT ROWID";

const UPSERT: &str = "INSERT INTO _fleetring_sinks (sink, position) VALUES (?1, ?2) \
    ON CONFLICT (sink) DO UPDATE SET position = excluded.position";

const CLEAR: &str = "DELETE FROM _fleetring_sinks";

const READ: &str = "SELECT sink, position FROM _fleetring_sinks ORDER BY sink";

/// The marks staged for the next write transaction, on one write connection.
#[derive(Debug, Default)]
pub(super) struct Marks {
    /// The newest staged position per sink, not yet recorded.
    staged: BTreeMap<String, Vec<u8>>,
    /// Whether a committed transaction has seen to the table's existence on this connection. A
    /// transaction that creates the table and rolls back takes the table with it, so this is
    /// set only once a commit succeeded.
    created: bool,
    /// Whether the last [`flush`](Self::flush) wrote marks, which the transaction's commit then
    /// makes permanent.
    flushed: bool,
}

impl Marks {
    /// Stages `position` as `sink`'s mark, replacing any mark staged for it before.
    pub(super) fn stage(&mut self, sink: &str, position: &[u8]) {
        match self.staged.get_mut(sink) {
            Some(staged) => {
                staged.clear();
                staged.extend_from_slice(position);
            }
            None => {
                self.staged.insert(sink.to_owned(), position.to_vec());
            }
        }
    }

    /// Whether any mark is staged, for the next write transaction to record.
    pub(super) fn is_staged(&self) -> bool {
        !self.staged.is_empty()
    }

    /// Inside an open write transaction: creates the table if needed and upserts every staged
    /// mark. Returns how many rows it wrote.
    pub(super) fn flush(&mut self, writer: &Connection) -> Result<u64, EngineError> {
        self.flushed = false;
        if self.staged.is_empty() {
            return Ok(0);
        }
        if !self.created {
            writer.execute_batch(CREATE).map_err(storage)?;
        }
        let mut upsert = writer.prepare_cached(UPSERT).map_err(storage)?;
        for (sink, position) in &self.staged {
            upsert.execute(params![sink, position]).map_err(storage)?;
        }
        self.flushed = true;
        Ok(u64::try_from(self.staged.len()).unwrap_or(u64::MAX))
    }

    /// After `COMMIT` succeeded: forgets the marks the last [`flush`](Self::flush) wrote.
    pub(super) fn committed(&mut self) {
        if std::mem::take(&mut self.flushed) {
            self.staged.clear();
            self.created = true;
        }
    }

    /// Forgets every staged mark, after a fork's reset cleared the recorded ones: they were
    /// staged for positions of the history the reset ends.
    pub(super) fn discard_staged(&mut self) {
        self.staged.clear();
    }

    /// Inside an open write transaction: deletes every recorded mark. A fork's reset does this,
    /// since the marks it inherited are its source's sinks' (ADR-0046, point 6.1).
    pub(super) fn clear(writer: &Connection) -> Result<(), EngineError> {
        if !Self::table_exists(writer)? {
            return Ok(());
        }
        writer
            .prepare_cached(CLEAR)
            .map_err(storage)?
            .execute([])
            .map_err(storage)?;
        Ok(())
    }

    /// Reads every recorded mark; an absent table is no marks.
    pub(super) fn read(conn: &Connection) -> Result<BTreeMap<String, Vec<u8>>, EngineError> {
        if !Self::table_exists(conn)? {
            return Ok(BTreeMap::new());
        }
        let mut statement = conn.prepare_cached(READ).map_err(storage)?;
        let marks = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(storage)?
            .collect::<rusqlite::Result<_>>()
            .map_err(storage)?;
        Ok(marks)
    }

    /// Whether the marks table exists in the database `conn` reads.
    fn table_exists(conn: &Connection) -> Result<bool, EngineError> {
        Ok(conn
            .prepare_cached("SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?1")
            .map_err(storage)?
            .query_row(params![TABLE], |_| Ok(()))
            .optional()
            .map_err(storage)?
            .is_some())
    }
}
