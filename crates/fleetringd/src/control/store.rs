//! `control.db` format 2: the control group's Raft log, its metadata, and control snapshots, in
//! one SQLite database (ADR-0035 point 4, ADR-0040 points 3, 5, 7, and 8).
//!
//! # Format
//!
//! - `meta(key, value)`: `format` (`2`); `genesis`, the [`ControlConfig`] the first state was
//!   created with, as in format 1; `vote` (`{"term", "node", "committed"}`); and `committed` and
//!   `purged` (`{"term", "node", "index"}`).
//! - `log(log_index, term, leader_node, kind, payload)`: one row per Raft entry, in `codec`'s
//!   encoding. `leader_node` is a Raft ID as 16 hex digits.
//! - `snapshots(log_index, term, leader_node, membership, state)`: control snapshots, the newest
//!   last. `state` is the [`State`] as format 1 stores it.
//!
//! Every write uses `synchronous=FULL` in WAL mode, as format 1 does. The store converts
//! openraft's value types through `codec`; it doesn't implement openraft's storage traits.
//!
//! # Migration
//!
//! A format-1 file migrates on open, in one `BEGIN IMMEDIATE` transaction, to a store whose
//! first control snapshot holds the state at format 1's last index, with this node as its only
//! voter (ADR-0040 point 8). Before it does, it replays every format-1 command from genesis and
//! requires the replay to reach the recovered state, so a corrupt snapshot never becomes the only
//! base.

use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};

use fleetring_control::{Command, ControlConfig, State, apply};
use rusqlite::{Connection, OptionalExtension, params};

use crate::control::format1::{CommandLog, LogError, LogIndex, check_state};
use crate::control::raft::codec::{
    CodecError, EntryKind, EntryWire, HexId, LogIdWire, PayloadWire, StoredMembershipWire,
    VoteWire, decode_raft_id,
};
use crate::control::raft::state_machine::{Decision, decide};
use crate::control::raft::{
    ControlPeer, ControlRequest, Entry, LogId, Membership, RaftId, SnapshotMeta, StoredMembership,
    Vote, raft_id_text,
};

/// The control log format this store writes and reads.
pub(crate) const STORE_FORMAT: u32 = 2;

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
    CREATE TABLE IF NOT EXISTS log (
        log_index INTEGER PRIMARY KEY,
        term INTEGER NOT NULL,
        leader_node TEXT NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('command', 'membership', 'blank')),
        payload TEXT NOT NULL
    ) STRICT;
    CREATE TABLE IF NOT EXISTS snapshots (
        log_index INTEGER PRIMARY KEY,
        term INTEGER NOT NULL,
        leader_node TEXT NOT NULL,
        membership TEXT NOT NULL,
        state TEXT NOT NULL
    ) STRICT;
";

/// A control snapshot: its metadata, its last log ID and membership, and its state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlSnapshot {
    /// The snapshot's last log ID and membership.
    pub meta: SnapshotMeta,
    /// The state after the entry at the snapshot's last log ID.
    pub state: State,
}

/// The control group's log, metadata, and control snapshots: `control.db` format 2.
///
/// The store is blocking. In async code, call it from a dedicated thread, or inline under
/// simulation (ADR-0020).
#[derive(Debug)]
pub struct ControlStore {
    path: PathBuf,
    connection: Connection,
    genesis: ControlConfig,
}

impl ControlStore {
    /// Opens the store at `path`, creating it with `config` as its genesis configuration if the
    /// file is new, and migrating it if format 1 wrote it.
    ///
    /// `own` is this node's Raft ID and peer: the only voter of the group a non-empty format-1
    /// file migrates to. An existing store keeps its own genesis configuration; `config` is then
    /// ignored.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the file can't be opened, a newer build wrote it, its genesis is corrupt,
    /// it holds a non-empty format-1 `commands` table beside format 2
    /// ([`LogError::StrayTable`]), or a format-1 file can't migrate ([`LogError::Migration`]):
    /// `own` is missing, or its snapshot doesn't match replaying its log from genesis.
    pub fn open(
        path: impl Into<PathBuf>,
        config: ControlConfig,
        own: Option<(RaftId, ControlPeer)>,
    ) -> Result<Self, LogError> {
        let path = path.into();
        let connection = connect(&path)?;
        let found = read_format(&connection).map_err(|source| sqlite(&path, source))?;
        match found.as_deref() {
            None => create(&path, &connection, config)?,
            Some("1") => {
                // Format 1's own connection reads the log; the migration takes this one.
                migrate(&path, &connection, own)?;
            }
            Some(text) if text == STORE_FORMAT.to_string() => {
                drop_stray_commands(&path, &connection)?;
            }
            Some(text) => {
                return Err(LogError::UnsupportedFormat {
                    found: text.to_owned(),
                    supported: STORE_FORMAT,
                });
            }
        }
        let genesis = read_genesis(&path, &connection)?;
        Ok(Self {
            path,
            connection,
            genesis,
        })
    }

    /// Returns the store's database file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the configuration the store's first state was created with.
    #[must_use]
    pub fn genesis(&self) -> ControlConfig {
        self.genesis
    }

    /// Returns the vote this node last saved, if any.
    ///
    /// # Errors
    ///
    /// [`LogError`] if it can't be read or decoded.
    pub fn vote(&self) -> Result<Option<Vote>, LogError> {
        let wire: Option<VoteWire> = self.read_meta("vote")?;
        Ok(wire.map(Vote::from))
    }

    /// Saves `vote`, durably.
    ///
    /// # Errors
    ///
    /// [`LogError`] if it can't be written.
    pub fn save_vote(&mut self, vote: &Vote) -> Result<(), LogError> {
        self.write_meta("vote", Some(&VoteWire::from(vote)))
    }

    /// Returns the last committed log ID this node saved, if any.
    ///
    /// # Errors
    ///
    /// [`LogError`] if it can't be read or decoded.
    pub fn committed(&self) -> Result<Option<LogId>, LogError> {
        let wire: Option<LogIdWire> = self.read_meta("committed")?;
        Ok(wire.map(LogId::from))
    }

    /// Saves `committed` as the last committed log ID, or clears it.
    ///
    /// # Errors
    ///
    /// [`LogError`] if it can't be written.
    pub fn save_committed(&mut self, committed: Option<&LogId>) -> Result<(), LogError> {
        self.write_meta("committed", committed.map(LogIdWire::from).as_ref())
    }

    /// Returns the last purged log ID, if any.
    ///
    /// # Errors
    ///
    /// [`LogError`] if it can't be read or decoded.
    pub fn purged(&self) -> Result<Option<LogId>, LogError> {
        let wire: Option<LogIdWire> = self.read_meta("purged")?;
        Ok(wire.map(LogId::from))
    }

    /// Returns the last log ID: the last entry's, or the last purged one's when the log is
    /// empty.
    ///
    /// # Errors
    ///
    /// [`LogError`] if it can't be read or decoded.
    pub fn last_log_id(&self) -> Result<Option<LogId>, LogError> {
        let row: Option<(i64, i64, String)> = self
            .connection
            .query_row(
                "SELECT log_index, term, leader_node FROM log ORDER BY log_index DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|source| self.sqlite(source))?;
        match row {
            Some((index, term, leader)) => {
                let index = from_sql(index)?;
                Ok(Some(LogId::from(LogIdWire {
                    term: from_sql(term)?,
                    node: HexId(entry_raft_id(index, &leader)?),
                    index,
                })))
            }
            None => self.purged(),
        }
    }

    /// Appends `entries` in one transaction, and waits until all of them are on stable storage.
    ///
    /// # Errors
    ///
    /// [`LogError`] if an entry can't be encoded or written, including when its index is taken.
    /// Nothing is appended then.
    pub fn append_entries<'e>(
        &mut self,
        entries: impl IntoIterator<Item = &'e Entry>,
    ) -> Result<(), LogError> {
        let path = self.path.clone();
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| sqlite(&path, source))?;
        {
            let mut insert = transaction
                .prepare_cached(
                    "INSERT INTO log (log_index, term, leader_node, kind, payload) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )
                .map_err(|source| sqlite(&path, source))?;
            for entry in entries {
                let wire = EntryWire::from(entry);
                let payload = wire
                    .payload
                    .to_json()
                    .map_err(|source| LogError::CorruptEntry {
                        index: LogIndex::new(wire.index),
                        source,
                    })?;
                insert
                    .execute(params![
                        to_sql(wire.index)?,
                        to_sql(wire.term)?,
                        raft_id_text(wire.leader_node.0),
                        wire.kind.as_str(),
                        payload,
                    ])
                    .map_err(|source| sqlite(&path, source))?;
            }
        }
        transaction.commit().map_err(|source| sqlite(&path, source))
    }

    /// Deletes the entries from `index` on: a follower's uncommitted tail that conflicts with
    /// its leader's log.
    ///
    /// # Errors
    ///
    /// [`LogError`] if they can't be deleted.
    pub fn truncate_from(&mut self, index: u64) -> Result<(), LogError> {
        self.connection
            .execute(
                "DELETE FROM log WHERE log_index >= ?1",
                params![to_sql(index)?],
            )
            .map(drop)
            .map_err(|source| self.sqlite(source))
    }

    /// Deletes the entries up to and including `log_id`, and records it as purged, in one
    /// transaction.
    ///
    /// # Errors
    ///
    /// [`LogError`] if they can't be deleted or the record can't be written.
    pub fn purge_through(&mut self, log_id: &LogId) -> Result<(), LogError> {
        let path = self.path.clone();
        let wire = LogIdWire::from(log_id);
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| sqlite(&path, source))?;
        transaction
            .execute(
                "DELETE FROM log WHERE log_index <= ?1",
                params![to_sql(wire.index)?],
            )
            .map_err(|source| sqlite(&path, source))?;
        set_meta(&path, &transaction, "purged", Some(&wire))?;
        transaction.commit().map_err(|source| sqlite(&path, source))
    }

    /// Returns the entries whose indexes are in `range`, in log order.
    ///
    /// # Errors
    ///
    /// [`LogError`] if they can't be read, or one is corrupt.
    pub fn entries(&self, range: impl RangeBounds<u64>) -> Result<Vec<Entry>, LogError> {
        let low = match range.start_bound() {
            Bound::Included(&low) => low,
            Bound::Excluded(&low) => low.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let high = match range.end_bound() {
            Bound::Included(&high) => Some(high),
            Bound::Excluded(&0) => return Ok(Vec::new()),
            Bound::Excluded(&high) => Some(high - 1),
            Bound::Unbounded => None,
        };
        let high = high.map_or(Ok(i64::MAX), to_sql)?;
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT log_index, term, leader_node, kind, payload FROM log \
                 WHERE log_index >= ?1 AND log_index <= ?2 ORDER BY log_index",
            )
            .map_err(|source| self.sqlite(source))?;
        let rows = statement
            .query_map(params![to_sql(low)?, high], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(|source| self.sqlite(source))?;
        // The log holds every index after the purged one, with no gap: a missing row is damage,
        // and openraft must never be handed a log that skips an entry.
        let first = self
            .purged()?
            .map_or(0, |purged| purged.index.saturating_add(1));
        let mut expected = low.max(first);
        let mut entries = Vec::new();
        for row in rows {
            let (index, term, leader, kind, payload) = row.map_err(|source| self.sqlite(source))?;
            let entry = decode_entry(index, term, &leader, &kind, &payload)?;
            if entry.log_id.index != expected {
                return Err(LogError::Gap {
                    expected: LogIndex::new(expected),
                    found: LogIndex::new(entry.log_id.index),
                });
            }
            expected = expected.saturating_add(1);
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Stores a control snapshot of `state` at `meta`, and deletes all but the newest `retain`
    /// snapshots, in one transaction.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the snapshot has no last log ID, can't be encoded, or can't be written.
    pub fn save_control_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        state: &State,
        retain: NonZeroUsize,
    ) -> Result<(), LogError> {
        let path = self.path.clone();
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| sqlite(&path, source))?;
        insert_snapshot(&path, &transaction, meta, state)?;
        let retain = i64::try_from(retain.get()).unwrap_or(i64::MAX);
        transaction
            .execute(
                "DELETE FROM snapshots WHERE log_index NOT IN \
                 (SELECT log_index FROM snapshots ORDER BY log_index DESC LIMIT ?1)",
                params![retain],
            )
            .map_err(|source| sqlite(&path, source))?;
        transaction.commit().map_err(|source| sqlite(&path, source))
    }

    /// Returns every retained control snapshot, the oldest first, each checked against the
    /// genesis configuration.
    ///
    /// # Errors
    ///
    /// [`LogError`] if they can't be read, or one is corrupt.
    pub fn control_snapshots(&self) -> Result<Vec<ControlSnapshot>, LogError> {
        self.read_snapshots("ORDER BY log_index")
    }

    /// Returns the newest control snapshot, if any, checked against the genesis configuration.
    ///
    /// # Errors
    ///
    /// [`LogError`] if it can't be read, or is corrupt.
    pub fn latest_control_snapshot(&self) -> Result<Option<ControlSnapshot>, LogError> {
        Ok(self
            .read_snapshots("ORDER BY log_index DESC LIMIT 1")?
            .pop())
    }

    /// Returns the committed command entries after `index`, in log order: the control log's
    /// commands, without Raft's own blank and membership entries and without anything past the
    /// last committed log ID.
    ///
    /// # Errors
    ///
    /// [`LogError`] if they can't be read, or one is corrupt.
    pub fn committed_commands_after(
        &self,
        index: u64,
    ) -> Result<Vec<(LogId, ControlRequest)>, LogError> {
        let Some(committed) = self.committed()? else {
            return Ok(Vec::new());
        };
        if committed.index <= index {
            return Ok(Vec::new());
        }
        let entries = self.entries(index.saturating_add(1)..=committed.index)?;
        Ok(entries
            .into_iter()
            .filter_map(|entry| match entry.payload {
                openraft::EntryPayload::Normal(request) => Some((entry.log_id, request)),
                openraft::EntryPayload::Blank | openraft::EntryPayload::Membership(_) => None,
            })
            .collect())
    }

    /// Returns the commands the log still holds that the state machine applies, in log order,
    /// each with its index: Raft's own blank and membership entries, and the command entries the
    /// skip rules supersede, are left out (ADR-0040 point 2). Uncommitted entries are included.
    ///
    /// # Errors
    ///
    /// [`LogError`] if they can't be read, or one is corrupt or can't be decoded.
    pub fn commands(&self) -> Result<Vec<(LogIndex, Command)>, LogError> {
        let mut commands = Vec::new();
        for entry in self.entries(..)? {
            let openraft::EntryPayload::Normal(request) = &entry.payload else {
                continue;
            };
            let index = LogIndex::new(entry.log_id.index);
            match decide(&entry.log_id, request) {
                Decision::Apply(command) => commands.push((index, *command)),
                Decision::Superseded => {}
                Decision::Undecodable(source) => {
                    return Err(LogError::CorruptEntry {
                        index,
                        source: CodecError::Json {
                            what: "a command",
                            source,
                        },
                    });
                }
            }
        }
        Ok(commands)
    }

    /// Whether the store holds no log yet: no entry, nothing purged, and no snapshot, as on a
    /// node waiting to be added to a control group. A saved vote doesn't count: openraft saves
    /// the leader's vote before it installs the group's first snapshot on a joiner.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the store can't be read.
    pub fn is_fresh(&self) -> Result<bool, LogError> {
        let snapshots: i64 = self
            .connection
            .query_row("SELECT COUNT(*) FROM snapshots", [], |row| row.get(0))
            .map_err(|source| self.sqlite(source))?;
        Ok(snapshots == 0 && self.last_log_id()?.is_none())
    }

    /// Records `config` as the store's genesis configuration: on a fresh joiner, the one the
    /// group's first control snapshot carries, in place of its own configuration's.
    ///
    /// # Errors
    ///
    /// [`LogError`] if it can't be written.
    pub fn set_genesis(&mut self, config: ControlConfig) -> Result<(), LogError> {
        self.write_meta("genesis", Some(&config))?;
        self.genesis = config;
        Ok(())
    }

    fn read_snapshots(&self, order: &str) -> Result<Vec<ControlSnapshot>, LogError> {
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT log_index, term, leader_node, membership, state FROM snapshots {order}"
            ))
            .map_err(|source| self.sqlite(source))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(|source| self.sqlite(source))?;
        let mut snapshots = Vec::new();
        for row in rows {
            let (index, term, leader, membership, state) =
                row.map_err(|source| self.sqlite(source))?;
            let index = from_sql(index)?;
            let corrupt = |reason: String| LogError::CorruptSnapshot {
                index: LogIndex::new(index),
                reason,
            };
            let leader = decode_raft_id(&leader).map_err(|error| corrupt(error.to_string()))?;
            let membership: StoredMembershipWire =
                serde_json::from_str(&membership).map_err(|error| corrupt(error.to_string()))?;
            let state: State =
                serde_json::from_str(&state).map_err(|error| corrupt(error.to_string()))?;
            check_state(&state, &self.genesis).map_err(corrupt)?;
            let meta = SnapshotMeta {
                last_log_id: Some(LogId::from(LogIdWire {
                    term: from_sql(term)?,
                    node: HexId(leader),
                    index,
                })),
                last_membership: StoredMembership::try_from(membership)
                    .map_err(|error| corrupt(error.to_string()))?,
            };
            snapshots.push(ControlSnapshot { meta, state });
        }
        Ok(snapshots)
    }

    fn read_meta<T: serde::de::DeserializeOwned>(
        &self,
        key: &'static str,
    ) -> Result<Option<T>, LogError> {
        let text: Option<String> = self
            .connection
            .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|source| self.sqlite(source))?;
        text.map(|text| {
            serde_json::from_str(&text).map_err(|source| LogError::CorruptMeta {
                key,
                source: CodecError::Json { what: key, source },
            })
        })
        .transpose()
    }

    fn write_meta<T: serde::Serialize>(
        &mut self,
        key: &'static str,
        value: Option<&T>,
    ) -> Result<(), LogError> {
        let path = self.path.clone();
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| sqlite(&path, source))?;
        set_meta(&path, &transaction, key, value)?;
        transaction.commit().map_err(|source| sqlite(&path, source))
    }

    fn sqlite(&self, source: rusqlite::Error) -> LogError {
        sqlite(&self.path, source)
    }
}

fn sqlite(path: &Path, source: rusqlite::Error) -> LogError {
    LogError::Sqlite {
        path: path.to_owned(),
        source,
    }
}

fn connect(path: &Path) -> Result<Connection, LogError> {
    let connection = Connection::open(path).map_err(|source| sqlite(path, source))?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(|source| sqlite(path, source))?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|source| sqlite(path, source))?;
    Ok(connection)
}

fn has_table(connection: &Connection, table: &str) -> Result<bool, rusqlite::Error> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map(|found| found.is_some())
}

/// Returns `meta.format`, or `None` for a file without one: a new file.
fn read_format(connection: &Connection) -> Result<Option<String>, rusqlite::Error> {
    if !has_table(connection, "meta")? {
        return Ok(None);
    }
    connection
        .query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| {
            row.get(0)
        })
        .optional()
}

fn read_genesis(path: &Path, connection: &Connection) -> Result<ControlConfig, LogError> {
    let text: Option<String> = connection
        .query_row("SELECT value FROM meta WHERE key = 'genesis'", [], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|source| sqlite(path, source))?;
    let Some(text) = text else {
        return Err(LogError::CorruptGenesis {
            reason: "it is missing".into(),
        });
    };
    serde_json::from_str(&text).map_err(|error| LogError::CorruptGenesis {
        reason: error.to_string(),
    })
}

fn set_meta<T: serde::Serialize>(
    path: &Path,
    transaction: &Connection,
    key: &'static str,
    value: Option<&T>,
) -> Result<(), LogError> {
    match value {
        Some(value) => {
            let text = serde_json::to_string(value)
                .map_err(|source| LogError::Encode { what: key, source })?;
            transaction
                .execute(
                    "INSERT INTO meta (key, value) VALUES (?1, ?2) \
                     ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                    params![key, text],
                )
                .map_err(|source| sqlite(path, source))?;
        }
        None => {
            transaction
                .execute("DELETE FROM meta WHERE key = ?1", [key])
                .map_err(|source| sqlite(path, source))?;
        }
    }
    Ok(())
}

/// Creates an empty format-2 store in a new file.
fn create(path: &Path, connection: &Connection, config: ControlConfig) -> Result<(), LogError> {
    let genesis = serde_json::to_string(&config).map_err(|source| LogError::Encode {
        what: "the genesis configuration",
        source,
    })?;
    immediate(path, connection, |transaction| {
        // A file another opener initialized meanwhile is left as it is.
        if read_format(transaction)
            .map_err(|source| sqlite(path, source))?
            .is_some()
        {
            return Ok(());
        }
        for table in ["commands", "snapshots", "log"] {
            if has_table(transaction, table).map_err(|source| sqlite(path, source))? {
                return Err(LogError::UnsupportedFormat {
                    found: "none".into(),
                    supported: STORE_FORMAT,
                });
            }
        }
        transaction
            .execute_batch(SCHEMA)
            .map_err(|source| sqlite(path, source))?;
        transaction
            .execute(
                "INSERT INTO meta (key, value) VALUES ('format', ?1), ('genesis', ?2)",
                params![STORE_FORMAT.to_string(), genesis],
            )
            .map_err(|source| sqlite(path, source))?;
        Ok(())
    })
}

/// Runs `body` in a `BEGIN IMMEDIATE` transaction, committing if it succeeds and rolling back
/// otherwise.
fn immediate(
    path: &Path,
    connection: &Connection,
    body: impl FnOnce(&Connection) -> Result<(), LogError>,
) -> Result<(), LogError> {
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(|source| sqlite(path, source))?;
    match body(connection) {
        Ok(()) => connection
            .execute_batch("COMMIT")
            .map_err(|source| sqlite(path, source)),
        Err(error) => {
            // The transaction is abandoned either way; the original error matters more.
            let _rollback = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

/// Drops the empty format-1 `commands` table that 0.9's `CommandLog::open` leaves in a format-2
/// file, and refuses a non-empty one (ADR-0040 point 5).
fn drop_stray_commands(path: &Path, connection: &Connection) -> Result<(), LogError> {
    if !has_table(connection, "commands").map_err(|source| sqlite(path, source))? {
        return Ok(());
    }
    let rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .map_err(|source| sqlite(path, source))?;
    if rows > 0 {
        return Err(LogError::StrayTable {
            path: path.to_owned(),
            rows: u64::try_from(rows).unwrap_or(0),
        });
    }
    immediate(path, connection, |transaction| {
        transaction
            .execute_batch("DROP TABLE IF EXISTS commands")
            .map_err(|source| sqlite(path, source))
    })?;
    tracing::info!(path = %path.display(), "dropped the empty commands table an older release created");
    Ok(())
}

/// Migrates a format-1 file to format 2 (ADR-0040 point 8).
fn migrate(
    path: &Path,
    connection: &Connection,
    own: Option<(RaftId, ControlPeer)>,
) -> Result<(), LogError> {
    // 1. Recover with format 1's code, and 2. replay from genesis.
    let (recovered, commands, genesis) = {
        let log = CommandLog::open(path, ControlConfig::new())?;
        (log.recover()?, log.commands()?, log.genesis())
    };
    let snapshot = match recovered.snapshot {
        Some(index) => Some((index, format_one_snapshot(path, connection, index)?)),
        None => None,
    };
    check_replay(&recovered.state, snapshot, &commands, genesis)?;
    // 3. The node's own peer becomes the group's only voter.
    let Some((own_id, own_peer)) = own else {
        return Err(LogError::Migration {
            reason: "the node's raft id is needed".into(),
        });
    };
    let last = recovered.last_index.get();
    // 4. One transaction: rename format 1's tables, create format 2's, and write the first
    // snapshot.
    immediate(path, connection, |transaction| {
        // The checks above read the file before this transaction took its lock; nothing may have
        // changed it since.
        let format = read_format(transaction).map_err(|source| sqlite(path, source))?;
        let newest: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(log_index), 0) FROM commands",
                [],
                |row| row.get(0),
            )
            .map_err(|source| sqlite(path, source))?;
        if format.as_deref() != Some("1") || from_sql(newest)? != last {
            return Err(LogError::Migration {
                reason: "another process changed the file during migration".into(),
            });
        }
        transaction
            .execute_batch(
                "ALTER TABLE commands RENAME TO commands_v1;
                 ALTER TABLE snapshots RENAME TO snapshots_v1;",
            )
            .map_err(|source| sqlite(path, source))?;
        transaction
            .execute_batch(SCHEMA)
            .map_err(|source| sqlite(path, source))?;
        if last > 0 {
            let log_id = LogId::from(LogIdWire {
                term: 0,
                node: HexId(0),
                index: last,
            });
            let membership = Membership::new(
                vec![[own_id].into_iter().collect()],
                [(own_id, own_peer)]
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>(),
            )
            .map_err(|_| LogError::Migration {
                reason: "the node's own membership is invalid".into(),
            })?;
            let meta = SnapshotMeta {
                last_log_id: Some(log_id),
                last_membership: StoredMembership::new(Some(log_id), membership),
            };
            insert_snapshot(path, transaction, &meta, &recovered.state)?;
            let wire = LogIdWire::from(&log_id);
            set_meta(path, transaction, "committed", Some(&wire))?;
            set_meta(path, transaction, "purged", Some(&wire))?;
        }
        transaction
            .execute(
                "UPDATE meta SET value = ?1 WHERE key = 'format'",
                params![STORE_FORMAT.to_string()],
            )
            .map_err(|source| sqlite(path, source))?;
        Ok(())
    })?;
    // 5.
    tracing::info!(
        index = last,
        "migrated the control log from format 1 at index {last}"
    );
    Ok(())
}

/// Returns the state of format 1's snapshot at `index`, which recovery has already checked.
fn format_one_snapshot(
    path: &Path,
    connection: &Connection,
    index: LogIndex,
) -> Result<State, LogError> {
    let text: String = connection
        .query_row(
            "SELECT state FROM snapshots WHERE log_index = ?1",
            params![to_sql(index.get())?],
            |row| row.get(0),
        )
        .map_err(|source| sqlite(path, source))?;
    serde_json::from_str(&text).map_err(|error| LogError::CorruptSnapshot {
        index,
        reason: error.to_string(),
    })
}

/// Requires replaying every format-1 command from genesis to reach `snapshot`, the newest
/// snapshot, at its index, and `recovered`, the state recovery built from it, at the last index
/// (ADR-0040 point 8, step 2). Names the first index where the two replays differ. Format-1
/// commands replay with `apply` alone: format 2's skip rules don't apply to them.
fn check_replay(
    recovered: &State,
    snapshot: Option<(LogIndex, State)>,
    commands: &[(LogIndex, Command)],
    genesis: ControlConfig,
) -> Result<(), LogError> {
    let Some((snapshot_index, mut from_snapshot)) = snapshot else {
        // Recovery replayed from genesis itself.
        return Ok(());
    };
    let mismatch = |index: LogIndex| LogError::Migration {
        reason: format!(
            "the snapshot at {snapshot_index} doesn't match replaying the log from genesis; \
             the first difference is at {index}"
        ),
    };
    let mut from_genesis = State::new(genesis);
    let mut rest = commands.iter().peekable();
    while let Some((_, command)) = rest.next_if(|(index, _)| *index <= snapshot_index) {
        let _outcome = apply(&mut from_genesis, command.clone());
    }
    if from_snapshot != from_genesis {
        return Err(mismatch(snapshot_index));
    }
    let mut last = snapshot_index;
    for (index, command) in rest {
        let _outcome = apply(&mut from_genesis, command.clone());
        let _outcome = apply(&mut from_snapshot, command.clone());
        if from_snapshot != from_genesis {
            return Err(mismatch(*index));
        }
        last = *index;
    }
    if from_genesis != *recovered {
        return Err(mismatch(last));
    }
    Ok(())
}

fn insert_snapshot(
    path: &Path,
    transaction: &Connection,
    meta: &SnapshotMeta,
    state: &State,
) -> Result<(), LogError> {
    let Some(last) = meta.last_log_id.as_ref() else {
        return Err(LogError::Migration {
            reason: "a control snapshot needs a last log id".into(),
        });
    };
    let last = LogIdWire::from(last);
    let membership = serde_json::to_string(&StoredMembershipWire::from(&meta.last_membership))
        .map_err(|source| LogError::Encode {
            what: "a snapshot's membership",
            source,
        })?;
    let state = serde_json::to_string(state).map_err(|source| LogError::Encode {
        what: "a snapshot",
        source,
    })?;
    transaction
        .execute(
            "INSERT OR REPLACE INTO snapshots (log_index, term, leader_node, membership, state) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                to_sql(last.index)?,
                to_sql(last.term)?,
                raft_id_text(last.node.0),
                membership,
                state
            ],
        )
        .map_err(|source| sqlite(path, source))?;
    Ok(())
}

fn decode_entry(
    index: i64,
    term: i64,
    leader: &str,
    kind: &str,
    payload: &str,
) -> Result<Entry, LogError> {
    let index = from_sql(index)?;
    let corrupt = |source| LogError::CorruptEntry {
        index: LogIndex::new(index),
        source,
    };
    let leader_node = HexId(entry_raft_id(index, leader)?);
    let kind = EntryKind::from_stored(kind).ok_or_else(|| {
        corrupt(CodecError::Json {
            what: "an entry's kind",
            source: serde::de::Error::custom(format!("unknown kind '{kind}'")),
        })
    })?;
    let wire = EntryWire {
        index,
        term: from_sql(term)?,
        leader_node,
        kind,
        payload: PayloadWire::decode(kind, payload).map_err(corrupt)?,
    };
    wire.into_entry().map_err(corrupt)
}

fn entry_raft_id(index: u64, text: &str) -> Result<RaftId, LogError> {
    decode_raft_id(text).map_err(|source| LogError::CorruptEntry {
        index: LogIndex::new(index),
        source,
    })
}

fn to_sql(value: u64) -> Result<i64, LogError> {
    i64::try_from(value).map_err(|_| LogError::Exhausted)
}

fn from_sql(value: i64) -> Result<u64, LogError> {
    u64::try_from(value).map_err(|_| LogError::Exhausted)
}
