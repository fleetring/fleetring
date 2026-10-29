//! The control node's durable command log and snapshots, in one SQLite database (ADR-0017).
//!
//! # Format
//!
//! The database holds three tables:
//!
//! - `meta`: the format version and the *genesis* configuration, the [`ControlConfig`] the first
//!   state was created with. Replay always starts from it, so changing the configured timings
//!   later never changes what an existing log replays to.
//! - `commands`: one row per stamped [`Command`], keyed by its [`LogIndex`], from 1 with no gaps.
//! - `snapshots`: the [`State`] after the command at a log index. Only the newest is kept.
//!
//! Commands and states are stored as JSON text (`serde_json`). The control types already derive
//! serde, JSON keeps the log readable with the `sqlite3` shell, and its cost is irrelevant at the
//! control plane's command rate. The format version in `meta` guards future changes.
//!
//! Every write uses `synchronous=FULL` in WAL mode, so an append that returns has reached stable
//! storage. Outcomes are not stored: replay recomputes them.

use std::fmt;
use std::path::{Path, PathBuf};
use std::ptr;

use fleetring_control::{Command, ControlConfig, State, apply};
use rusqlite::{Connection, OptionalExtension, params};

/// The log format this build writes and reads.
const FORMAT_VERSION: u32 = 1;

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
    CREATE TABLE IF NOT EXISTS commands (log_index INTEGER PRIMARY KEY, command TEXT NOT NULL) STRICT;
    CREATE TABLE IF NOT EXISTS snapshots (log_index INTEGER PRIMARY KEY, state TEXT NOT NULL) STRICT;
";

/// The position of a command in the log. The first command is at index 1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(u64);

impl LogIndex {
    /// The index before the first command: an empty log.
    pub const ZERO: Self = Self(0);

    /// Returns the index numbered `value`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the index as an integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next index, or `None` if the counter is exhausted.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The error returned when the command log can't be read or written.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LogError {
    /// SQLite failed.
    #[error("control log {path} failed")]
    Sqlite {
        /// The log's database file.
        path: PathBuf,
        /// The SQLite error.
        source: rusqlite::Error,
    },
    /// The log was written by a newer, incompatible build.
    #[error("control log format {found} is not supported; this build reads format {supported}")]
    UnsupportedFormat {
        /// The format version in the file.
        found: String,
        /// The version this build reads.
        supported: u32,
    },
    /// A value couldn't be encoded.
    #[error("can't encode {what} for the control log")]
    Encode {
        /// What was being encoded.
        what: &'static str,
        /// The encoder's error.
        source: serde_json::Error,
    },
    /// The genesis configuration is missing or can't be decoded.
    #[error("control log genesis configuration is corrupt: {reason}")]
    CorruptGenesis {
        /// What is wrong with it.
        reason: String,
    },
    /// A logged command can't be decoded.
    #[error("control log command {index} is corrupt")]
    CorruptCommand {
        /// The command's index.
        index: LogIndex,
        /// The decoder's error.
        source: serde_json::Error,
    },
    /// A snapshot can't be decoded or breaks an invariant of control state.
    #[error("control snapshot at {index} is corrupt: {reason}")]
    CorruptSnapshot {
        /// The snapshot's log index.
        index: LogIndex,
        /// What is wrong with it.
        reason: String,
    },
    /// The log skips an index.
    #[error("control log is missing command {expected}; the next one is {found}")]
    Gap {
        /// The index that should come next.
        expected: LogIndex,
        /// The index found instead.
        found: LogIndex,
    },
    /// The log has no room for another command.
    #[error("control log index is exhausted")]
    Exhausted,
}

/// A state rebuilt from the log at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovered {
    /// The state after the last logged command.
    pub state: State,
    /// The index of the last logged command, or [`LogIndex::ZERO`] for an empty log.
    pub last_index: LogIndex,
    /// The snapshot recovery started from, if any.
    pub snapshot: Option<LogIndex>,
    /// How many commands were replayed after the snapshot.
    pub replayed: u64,
}

/// The control node's command log and snapshots, in one SQLite database file.
///
/// The log is blocking. In async code, call it from a dedicated thread, or inline under
/// simulation (ADR-0020).
#[derive(Debug)]
pub struct CommandLog {
    path: PathBuf,
    connection: Connection,
    genesis: ControlConfig,
}

impl CommandLog {
    /// Opens the log at `path`, creating it with `config` as its genesis configuration if it is
    /// new.
    ///
    /// An existing log keeps the genesis configuration it was created with; `config` is then
    /// ignored, and [`CommandLog::genesis`] returns the stored one.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the file can't be opened, was written by an incompatible build, or its
    /// genesis configuration is corrupt.
    pub fn open(path: impl Into<PathBuf>, config: ControlConfig) -> Result<Self, LogError> {
        let path = path.into();
        let sqlite = |source| LogError::Sqlite {
            path: path.clone(),
            source,
        };
        let connection = Connection::open(&path).map_err(sqlite)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sqlite)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(sqlite)?;
        connection.execute_batch(SCHEMA).map_err(sqlite)?;
        let genesis = init_meta(&connection, config).map_err(|error| match error {
            MetaError::Sqlite(source) => sqlite(source),
            MetaError::Other(error) => error,
        })?;
        Ok(Self {
            path,
            connection,
            genesis,
        })
    }

    /// Returns the log's database file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the configuration the log's first state was created with.
    #[must_use]
    pub fn genesis(&self) -> ControlConfig {
        self.genesis
    }

    /// Appends `command` at `index` and waits until it is on stable storage.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the command can't be encoded or written, including when `index` is
    /// already taken. After an error the command may or may not be durable.
    pub fn append(&mut self, index: LogIndex, command: &Command) -> Result<(), LogError> {
        self.append_all([(index, command)])
    }

    /// Appends `commands`, each at its index, in one transaction, and waits until all of them
    /// are on stable storage.
    ///
    /// The group costs one sync however many commands it holds, so a writer that logs everything
    /// queued behind a command as one group keeps up with a burst of commands on a disk whose
    /// syncs are slow, where logging them one at a time would fall ever further behind. Either
    /// every command in the group is logged or none is.
    ///
    /// # Errors
    ///
    /// [`LogError`] if a command can't be encoded or written, including when an index is already
    /// taken. Nothing from the group is logged then.
    pub fn append_all<'c>(
        &mut self,
        commands: impl IntoIterator<Item = (LogIndex, &'c Command)>,
    ) -> Result<(), LogError> {
        let path = self.path.clone();
        let sqlite = |source| LogError::Sqlite {
            path: path.clone(),
            source,
        };
        let transaction = self.connection.transaction().map_err(sqlite)?;
        {
            let mut insert = transaction
                .prepare_cached("INSERT INTO commands (log_index, command) VALUES (?1, ?2)")
                .map_err(sqlite)?;
            for (index, command) in commands {
                let text = serde_json::to_string(command).map_err(|source| LogError::Encode {
                    what: "a command",
                    source,
                })?;
                insert
                    .execute(params![to_sql(index)?, text])
                    .map_err(sqlite)?;
            }
        }
        transaction.commit().map_err(sqlite)
    }

    /// Stores `state` as the snapshot after the command at `index`, replacing older snapshots.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the state can't be encoded or written.
    pub fn save_snapshot(&mut self, index: LogIndex, state: &State) -> Result<(), LogError> {
        let text = serde_json::to_string(state).map_err(|source| LogError::Encode {
            what: "a snapshot",
            source,
        })?;
        let index = to_sql(index)?;
        let path = self.path.clone();
        let sqlite = |source| LogError::Sqlite {
            path: path.clone(),
            source,
        };
        let transaction = self.connection.transaction().map_err(sqlite)?;
        transaction
            .execute(
                "INSERT OR REPLACE INTO snapshots (log_index, state) VALUES (?1, ?2)",
                params![index, text],
            )
            .map_err(sqlite)?;
        transaction
            .execute("DELETE FROM snapshots WHERE log_index < ?1", params![index])
            .map_err(sqlite)?;
        transaction.commit().map_err(sqlite)
    }

    /// Returns every logged command in log order, checking that no index is missing.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the log can't be read, a command is corrupt, or an index is missing.
    pub fn commands(&self) -> Result<Vec<(LogIndex, Command)>, LogError> {
        let mut commands = Vec::new();
        self.for_each_command_after(LogIndex::ZERO, |index, command| {
            commands.push((index, command));
        })?;
        Ok(commands)
    }

    /// Rebuilds the state: loads the newest snapshot, checks it, and replays the commands after it.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the log can't be read, the snapshot or a command is corrupt, or an index
    /// is missing.
    pub fn recover(&self) -> Result<Recovered, LogError> {
        let snapshot = self.latest_snapshot()?;
        let (mut state, start) = match &snapshot {
            Some((index, state)) => (state.clone(), *index),
            None => (State::new(self.genesis), LogIndex::ZERO),
        };
        let mut last_index = start;
        let mut replayed = 0;
        self.for_each_command_after(start, |index, command| {
            // Rejections are part of the log's history; replay recomputes them and moves on.
            let _outcome = apply(&mut state, command);
            last_index = index;
            replayed += 1;
        })?;
        if let Some((index, _)) = &snapshot
            && start == last_index
            && !self.has_command(*index)?
        {
            return Err(LogError::CorruptSnapshot {
                index: *index,
                reason: "it is ahead of the log".into(),
            });
        }
        Ok(Recovered {
            state,
            last_index,
            snapshot: snapshot.map(|(index, _)| index),
            replayed,
        })
    }

    fn latest_snapshot(&self) -> Result<Option<(LogIndex, State)>, LogError> {
        let row: Option<(i64, String)> = self
            .connection
            .query_row(
                "SELECT log_index, state FROM snapshots ORDER BY log_index DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|source| self.sqlite(source))?;
        let Some((index, text)) = row else {
            return Ok(None);
        };
        let Ok(index) = u64::try_from(index).map(LogIndex) else {
            return Err(LogError::CorruptSnapshot {
                index: LogIndex::ZERO,
                reason: format!("negative log index {index}"),
            });
        };
        let state: State =
            serde_json::from_str(&text).map_err(|error| LogError::CorruptSnapshot {
                index,
                reason: error.to_string(),
            })?;
        check_state(&state, &self.genesis)
            .map_err(|reason| LogError::CorruptSnapshot { index, reason })?;
        Ok(Some((index, state)))
    }

    fn has_command(&self, index: LogIndex) -> Result<bool, LogError> {
        self.connection
            .query_row(
                "SELECT 1 FROM commands WHERE log_index = ?1",
                params![to_sql(index)?],
                |_| Ok(()),
            )
            .optional()
            .map(|row| row.is_some())
            .map_err(|source| self.sqlite(source))
    }

    fn for_each_command_after(
        &self,
        after: LogIndex,
        mut visit: impl FnMut(LogIndex, Command),
    ) -> Result<(), LogError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT log_index, command FROM commands WHERE log_index > ?1 ORDER BY log_index",
            )
            .map_err(|source| self.sqlite(source))?;
        let mut rows = statement
            .query(params![to_sql(after)?])
            .map_err(|source| self.sqlite(source))?;
        let mut expected = after.next().ok_or(LogError::Exhausted)?;
        while let Some(row) = rows.next().map_err(|source| self.sqlite(source))? {
            let index: i64 = row.get(0).map_err(|source| self.sqlite(source))?;
            let text: String = row.get(1).map_err(|source| self.sqlite(source))?;
            let index = LogIndex(u64::try_from(index).unwrap_or(0));
            if index != expected {
                return Err(LogError::Gap {
                    expected,
                    found: index,
                });
            }
            let command: Command = serde_json::from_str(&text)
                .map_err(|source| LogError::CorruptCommand { index, source })?;
            visit(index, command);
            expected = index.next().ok_or(LogError::Exhausted)?;
        }
        Ok(())
    }

    fn sqlite(&self, source: rusqlite::Error) -> LogError {
        LogError::Sqlite {
            path: self.path.clone(),
            source,
        }
    }
}

enum MetaError {
    Sqlite(rusqlite::Error),
    Other(LogError),
}

impl From<rusqlite::Error> for MetaError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

/// Records the format and genesis configuration in a new log, or checks and reads them from an
/// existing one.
fn init_meta(connection: &Connection, config: ControlConfig) -> Result<ControlConfig, MetaError> {
    let read = |key: &str| -> Result<Option<String>, rusqlite::Error> {
        connection
            .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()
    };
    match read("format")? {
        Some(found) if found == FORMAT_VERSION.to_string() => {}
        Some(found) => {
            return Err(MetaError::Other(LogError::UnsupportedFormat {
                found,
                supported: FORMAT_VERSION,
            }));
        }
        None => {
            let genesis = serde_json::to_string(&config).map_err(|source| {
                MetaError::Other(LogError::Encode {
                    what: "the genesis configuration",
                    source,
                })
            })?;
            connection.execute_batch("BEGIN IMMEDIATE")?;
            let written = connection
                .execute(
                    "INSERT INTO meta (key, value) VALUES ('format', ?1), ('genesis', ?2)",
                    params![FORMAT_VERSION.to_string(), genesis],
                )
                .and_then(|_| connection.execute_batch("COMMIT"));
            if let Err(error) = written {
                // The transaction is abandoned either way; the original error matters more.
                let _rollback = connection.execute_batch("ROLLBACK");
                return Err(error.into());
            }
            return Ok(config);
        }
    }
    let Some(text) = read("genesis")? else {
        return Err(MetaError::Other(LogError::CorruptGenesis {
            reason: "it is missing".into(),
        }));
    };
    serde_json::from_str(&text).map_err(|error| {
        MetaError::Other(LogError::CorruptGenesis {
            reason: error.to_string(),
        })
    })
}

fn to_sql(index: LogIndex) -> Result<i64, LogError> {
    i64::try_from(index.0).map_err(|_| LogError::Exhausted)
}

/// Checks the invariants that every state produced by `apply` keeps, so that a damaged snapshot
/// is refused instead of corrupting the control plane.
fn check_state(state: &State, genesis: &ControlConfig) -> Result<(), String> {
    if state.config() != genesis {
        return Err("its configuration differs from the log's genesis configuration".into());
    }
    let now = state.now();
    for member in state.members() {
        if !state
            .member(member.id())
            .is_some_and(|found| ptr::eq(found, member))
        {
            return Err(format!(
                "member '{}' is filed under another id",
                member.id()
            ));
        }
        if state.last_heartbeat(member.id()).is_some_and(|at| at > now) {
            return Err(format!(
                "member '{}' has a heartbeat after the state's time",
                member.id()
            ));
        }
    }
    if state.members().len() > 0 {
        if state.membership_epoch().get() == 0 {
            return Err("it has members at membership epoch 0".into());
        }
        state
            .ring()
            .map_err(|error| format!("its members don't form a ring: {error}"))?;
    }
    for database in state.databases() {
        let name = database.name();
        if !state
            .database(name)
            .is_some_and(|found| ptr::eq(found, database))
        {
            return Err(format!("database '{name}' is filed under another name"));
        }
        let epoch = database.epoch();
        if let Some(lease) = database.lease()
            && (lease.epoch().get() == 0 || lease.epoch() > epoch)
        {
            return Err(format!(
                "database '{name}' has a lease at epoch {} but epoch {epoch}",
                lease.epoch()
            ));
        }
        if let Some(promotion) = database.promotion() {
            if database.lease().is_none() {
                return Err(format!("database '{name}' has a promotion but no lease"));
            }
            if promotion.epoch() > epoch || promotion.started_at() > now {
                return Err(format!(
                    "database '{name}' has a promotion beyond its epoch or the state's time"
                ));
            }
        }
    }
    Ok(())
}
