//! The SQLite adapter: a frame source that ships WAL frames (ADR-0016, points 4 to 8).
//!
//! WAL details stay in this module and in `fleetring-wal`. Above it, positions are the 20-byte
//! encoding of [`fleetring_wal::Position`] and payloads are [`fleetring_wal::Batch`] payloads,
//! both opaque.
//!
//! Each checkpoint first records the position it covers in `data.db-checkpoint`, durably, so
//! that after a restart the adapter still knows which earlier-generation position the current
//! generation continues from, and refuses every other one (FR-032).
//!
//! Row changes for logical change streams are recorded by the `changes` module, in a change
//! table inside the database (FR-039, ADR-0024), and the positions that change sinks have been
//! delivered up to by the `marks` module, in a second table, so that they replicate with the data
//! (FR-041, ADR-0028). A fork's reset clears both and records its marker in a third, by the `fork`
//! module (ADR-0046, point 6). Names beginning with `_fleetring_`, in any ASCII case, are reserved
//! for them, and `_fleetring_cdc`, `_fleetring_sinks`, and `_fleetring_fork` are the tables: a
//! user statement that reads, writes, calls, creates, alters, or drops anything so named is refused
//! as a statement error, on both connections, so that neither the tables nor the recorder's
//! triggers can be read, forged, or broken from outside. Nor can a user statement create, rename
//! into, or put a trigger on anything in the reserved namespace: one that changes the namespace in
//! any way is refused and its transaction rolled back. The adapter, in turn, never adopts an
//! object there that it didn't create: it refuses to record changes or sink marks, or to read or
//! write a fork's marker, in a database that holds one (the `reserved` module).
//!
//! User statements are confined to the database they name. One that attaches or detaches a
//! database, loads an extension, or runs a pragma other than read-only introspection is refused
//! as a statement error, so it can't reach another database's files or change how this one is
//! journaled and checkpointed, nor create a temporary table, index, trigger, or view: temporary
//! objects live on one connection, which every SQL session and `Execute` share, and outside the
//! replicated log. The connections also run in SQLite's defensive mode.
//!
//! A user statement, and a schema read, stop once their call's [`Interrupt`] is raised (FR-078):
//! while one runs, SQLite's progress handler checks the interrupt every
//! [`INTERRUPT_CHECK_OPS`] virtual-machine instructions, and fails the statement with
//! `SQLITE_INTERRUPT` once it is raised. The handler is installed once per connection and checks
//! only the interrupt watched at the time. The write transaction the statement ran in then rolls
//! back, change records and all, as after any failed statement: nothing commits, so no batch
//! ships, though pages a large transaction spilled may sit in the WAL past its committed end until
//! the next commit overwrites them. The adapter's own statements around a write, its change
//! records and sink marks and its `COMMIT`, run with no interrupt watched, so a transaction whose
//! statements finished always commits.

mod changes;
mod follower;
mod fork;
mod marks;
mod page_size;
mod pins;
mod reserved;
mod schema;
mod session;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::{OsString, c_int};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Write as _};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use fleetring_wal::{Batch, ReadError, ReadFileError, read_committed_file};
use rusqlite::config::DbConfig;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{
    CachedStatement, Connection, ErrorCode, OpenFlags, OptionalExtension, ToSql, ffi, params,
};

pub use follower::SqliteFollower;

use self::changes::{RESERVED_PREFIX, Recorder, TRIGGER_PREFIX, Watched};
use self::fork::Outcome;
use self::marks::Marks;
use self::pins::Pins;
use self::reserved::{Reserved, is_reserved, mentions_reserved};
use self::session::{Control, OpenTransaction};
use crate::engine::{
    BatchTransaction, CaptureMode, CheckpointMarker, Database, DatabaseCapability, DatabaseEngine,
    DatabaseSnapshot, EngineCapability, EngineError, EngineKind, Executed, ForkMarker, ForkReset,
    FrameBatch, FrameFollower, FrameSource, FrameSourceEngine, Interrupt, LogicalChanges,
    LogicalSupport, OpenMode, PinnedSnapshot, Position, ReadLimit, RecordedTransaction, RowChange,
    Rows, Schema, SchemaRequest, SinkMarks, SnapshotFile, SqlSessions, Statement, Transaction,
    Value,
};

/// The database file inside a database's location. The WAL sits next to it.
const DATABASE_FILE: &str = "data.db";

/// The WAL file inside a database's location.
const WAL_FILE: &str = "data.db-wal";

/// The file inside a database's location that records where the last checkpoint ended.
const CHECKPOINT_FILE: &str = "data.db-checkpoint";

/// The smallest and largest page sizes SQLite supports.
const PAGE_SIZES: std::ops::RangeInclusive<u32> = 512..=65536;

/// How many SQLite virtual-machine instructions run between two checks of a running statement's
/// [`Interrupt`]. A check reads one atomic flag, so at a thousand instructions, a few
/// microseconds of work, it costs nothing measurable, and a statement stops within microseconds
/// of its interrupt.
const INTERRUPT_CHECK_OPS: c_int = 1_000;

/// Why SQL with a second statement after the first is refused: `Execute` and `Query` run one
/// statement each, and a batch of several waits for the router's sessions (FR-066). Nothing
/// runs, the first statement included.
const ONE_STATEMENT: &str = "a call runs one statement; send each in a call of its own";

/// The SQLite engine, with the page size it gives new databases.
///
/// Each database lives in its own directory as `data.db` and `data.db-wal`. The adapter keeps
/// one write connection and one read connection open for as long as the database is open, so
/// nothing else can checkpoint or restart the WAL.
///
/// Clones share the snapshots pinned through any of them, so a database reopened through a clone
/// still refuses to checkpoint under a snapshot that a follower is reading. Engines compare by
/// page size alone.
///
/// # Examples
///
/// ```
/// use fleetringd::engine::{
///     DatabaseEngine, Interrupt, OpenMode, SqliteEngine, Statement, Transaction,
/// };
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let dir = tempfile::tempdir()?;
/// let engine = SqliteEngine::new(4096)?;
/// let mut db = engine.open(dir.path(), OpenMode::Create)?;
/// db.execute(&Transaction::from(Statement::new("CREATE TABLE t (v)")), &Interrupt::new())?;
/// db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SqliteEngine {
    page_size: u32,
    pins: Arc<Pins>,
}

impl PartialEq for SqliteEngine {
    fn eq(&self, other: &Self) -> bool {
        self.page_size == other.page_size
    }
}

impl Eq for SqliteEngine {}

impl SqliteEngine {
    /// An engine that creates databases with `page_size`-byte pages.
    ///
    /// # Errors
    ///
    /// [`InvalidPageSizeError`] unless `page_size` is a power of two from 512 to 65536.
    pub fn new(page_size: u32) -> Result<Self, InvalidPageSizeError> {
        if !page_size.is_power_of_two() || !PAGE_SIZES.contains(&page_size) {
            return Err(InvalidPageSizeError { page_size });
        }
        Ok(Self {
            page_size,
            pins: Arc::default(),
        })
    }

    /// The page size new databases get.
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.page_size
    }
}

/// The error returned for a page size SQLite doesn't support.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("page size {page_size} is not a power of two from 512 to 65536")]
pub struct InvalidPageSizeError {
    /// The page size asked for.
    pub page_size: u32,
}

impl DatabaseEngine for SqliteEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Sqlite
    }

    fn capability(&self) -> EngineCapability<'_> {
        EngineCapability::FrameSource(self)
    }

    fn logical_support(&self) -> LogicalSupport {
        LogicalSupport::Supported
    }

    fn open(&self, location: &Path, mode: OpenMode) -> Result<Box<dyn Database>, EngineError> {
        let path = location.join(DATABASE_FILE);
        let exists = path.try_exists().map_err(storage)?;
        match (mode, exists) {
            (OpenMode::Existing, false) => return Err(EngineError::NotFound),
            (OpenMode::Create, true) => return Err(EngineError::AlreadyExists),
            (OpenMode::Create, false) => fs::create_dir_all(location).map_err(storage)?,
            (OpenMode::Existing, true) => {}
        }
        Ok(Box::new(SqliteDatabase::open(
            path,
            mode,
            self.page_size,
            Arc::clone(&self.pins),
        )?))
    }
}

impl FrameSourceEngine for SqliteEngine {
    fn open_follower(&self, location: &Path) -> Result<Box<dyn FrameFollower>, EngineError> {
        Ok(Box::new(SqliteFollower::open(location)?))
    }

    fn check_copy(&self, location: &Path) -> Result<(), EngineError> {
        page_size::check(&location.join(DATABASE_FILE), Some(self.page_size))
    }

    /// Each transaction is the frames up to and including a commit frame. The generation's WAL
    /// header, when the batch starts one, goes with the first; the payload's own fixed fields,
    /// which describe the batch rather than the log, belong to none.
    fn transactions(&self, batch: &FrameBatch) -> Result<Vec<BatchTransaction>, EngineError> {
        let layout = Batch::layout(batch.payload.as_bytes())
            .map_err(|error| EngineError::InvalidPayload(error.into()))?;
        let mut from = layout.log_at;
        Ok(layout
            .ends
            .into_iter()
            .map(|(to, end)| {
                let bytes = from..to;
                from = to;
                BatchTransaction {
                    end: encode_position(end),
                    bytes,
                }
            })
            .collect())
    }
}

/// An open SQLite primary: one write connection and one read-only connection.
#[derive(Debug)]
struct SqliteDatabase {
    path: PathBuf,
    writer: Connection,
    reader: Connection,
    /// The confinement of user statements on each connection.
    writing: Arc<Confinement>,
    reading: Arc<Confinement>,
    /// The engine's pinned snapshots, which hold off checkpoints.
    pins: Arc<Pins>,
    /// Where the last checkpoint ended: the only earlier-generation position the current
    /// generation is known to continue from.
    checkpointed: Option<fleetring_wal::Position>,
    /// The log's end as of the last read of it, which is the position the next transaction
    /// starts from. Kept for the change recorder.
    end: Option<fleetring_wal::Position>,
    /// The row-change recorder, once a logical change stream asked for one.
    recorder: Option<Recorder>,
    /// The sink marks staged for the next write transaction.
    marks: Marks,
    /// The reserved part of the write connection's schema, which user statements may not change.
    reserved: Reserved,
    /// The transaction a SQL session holds open on the writer, if one does (ADR-0037).
    transaction: Option<OpenTransaction>,
}

impl SqliteDatabase {
    fn open(
        path: PathBuf,
        mode: OpenMode,
        page_size: u32,
        pins: Arc<Pins>,
    ) -> Result<Self, EngineError> {
        let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        match mode {
            OpenMode::Create => flags |= OpenFlags::SQLITE_OPEN_CREATE,
            // Never beside a WAL of another page size (#154). The configured page size is for new
            // databases only, so an existing one may have another.
            OpenMode::Existing => page_size::check(&path, None)?,
        }
        let mut writer = Connection::open_with_flags(&path, flags).map_err(storage)?;
        defend(&writer)?;
        keep_wal_on_close(&writer)?;
        if mode == OpenMode::Create {
            writer
                .pragma_update(None, "page_size", page_size)
                .map_err(storage)?;
        }
        let journal: String = writer
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .map_err(storage)?;
        if !journal.eq_ignore_ascii_case("wal") {
            return Err(storage(io::Error::other(format!(
                "journal mode is {journal}, not wal"
            ))));
        }
        // `journal_size_limit = 0` trims the WAL file to the new generation's frames on the first
        // commit after a checkpoint, without ever deleting its header. Recursive triggers make
        // SQLite fire delete triggers for the rows an `INSERT OR REPLACE` replaces, so the change
        // recorder sees them.
        writer
            .execute_batch(
                "PRAGMA wal_autocheckpoint = 0;
                 PRAGMA synchronous = FULL;
                 PRAGMA journal_size_limit = 0;
                 PRAGMA recursive_triggers = ON;",
            )
            .map_err(storage)?;
        let writing = confine(&mut writer)?;
        let mut reader = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(storage)?;
        defend(&reader)?;
        keep_wal_on_close(&reader)?;
        reader
            .execute_batch("PRAGMA query_only = 1; PRAGMA wal_autocheckpoint = 0;")
            .map_err(storage)?;
        let reading = confine(&mut reader)?;
        let checkpointed = read_marker(&path)?;
        Ok(Self {
            path,
            writer,
            reader,
            writing,
            reading,
            pins,
            checkpointed,
            end: None,
            recorder: None,
            marks: Marks::default(),
            reserved: Reserved::default(),
            transaction: None,
        })
    }

    fn wal_path(&self) -> PathBuf {
        wal_path(&self.path)
    }

    fn run_statements(
        &mut self,
        transaction: &Transaction,
        interrupt: &Interrupt,
    ) -> Result<(), EngineError> {
        self.reserved.remember(&self.writer)?;
        for statement in transaction.statements() {
            {
                let _user = UserStatement::enter(&self.writing);
                let _interruptible = Interruptible::watch(&self.writing, interrupt);
                run(&self.writer, statement, &self.writing, interrupt)?;
            }
            // The authorizer sees a renamed table's old name only, so a rename into the reserved
            // namespace shows only in the schema it leaves behind.
            if self.reserved.changed(&self.writer)? {
                return Err(EngineError::Statement {
                    message: Refusal::Reserved.to_string(),
                });
            }
            if self.writing.defines_program.swap(false, Ordering::Relaxed) {
                refuse_programs_on_reserved_names(&self.writer)?;
            }
            if self.writer.is_autocommit() {
                return Err(EngineError::TransactionControl);
            }
            if let Some(recorder) = &mut self.recorder {
                recorder.after_statement(&self.writer, statement.sql())?;
            }
        }
        Ok(())
    }

    fn roll_back(&mut self) {
        self.reserved.forget();
        if let Some(recorder) = &mut self.recorder {
            recorder.rolled_back();
        }
        if self.writer.is_autocommit() {
            return;
        }
        if let Err(error) = self.writer.execute_batch("ROLLBACK") {
            tracing::error!(path = %self.path.display(), %error, "rollback failed");
        }
    }

    /// Fails if the reserved namespace holds an object the adapter didn't create. Called before
    /// the adapter records changes or reads or writes sink marks, whose statements run with the
    /// confinement clear and would otherwise adopt it, firing any trigger on it unconfined.
    fn refuse_foreign_objects(&mut self) -> Result<(), EngineError> {
        let Some(foreign) = self.reserved.foreign(&self.writer)? else {
            return Ok(());
        };
        tracing::warn!(
            path = %self.path.display(),
            kind = %foreign.kind,
            object = %foreign.name,
            reason = foreign.reason,
            "a foreign object in the reserved namespace stops change recording and sink marks"
        );
        Err(EngineError::Statement {
            message: foreign.to_string(),
        })
    }

    /// Reads every commit after `after`, as [`read_wal_file`] describes, and remembers where the
    /// log ends.
    fn read_wal(&mut self, after: Option<&Position>) -> Result<Option<Batch>, EngineError> {
        let batch = read_wal_file(&self.wal_path(), after, self.checkpointed)?;
        match (&batch, after) {
            (Some(batch), _) => self.end = Some(batch.end()),
            (None, None) => self.end = None,
            // Nothing after a valid position: the end is where it was.
            (None, Some(_)) => {}
        }
        Ok(batch)
    }
}

/// Reads every commit after `after` from the WAL at `wal`, whose last checkpoint ended at
/// `checkpointed`.
///
/// A batch that starts a new generation continues only from the end of the last checkpoint: any
/// other earlier-generation position is refused, because the frames between it and that end are
/// gone.
fn read_wal_file(
    wal: &Path,
    after: Option<&Position>,
    checkpointed: Option<fleetring_wal::Position>,
) -> Result<Option<Batch>, EngineError> {
    let decoded = after.map(decode_position).transpose()?;
    let batch = read_committed_file(wal, decoded).map_err(|error| match (error, after) {
        (
            ReadFileError::Read(
                ReadError::UnknownGeneration { .. }
                | ReadError::PastEnd { .. }
                | ReadError::NotCommitBoundary { .. },
            ),
            Some(position),
        ) => EngineError::InvalidPosition {
            position: position.clone(),
        },
        (other, _) => storage(other),
    })?;
    if let (Some(position), Some(batch)) = (after, &batch)
        && batch.header().is_some()
        && decoded != checkpointed
    {
        return Err(EngineError::InvalidPosition {
            position: position.clone(),
        });
    }
    Ok(batch)
}

/// The part of `batch` up to `until`, a transaction boundary inside it or its end, as the batch a
/// reader bounded by a position gets.
fn cut_at(batch: &Batch, until: &Position) -> Result<FrameBatch, EngineError> {
    let end = decode_position(until)?;
    if batch.end() == end {
        return Ok(frame_batch(batch));
    }
    let cut = batch.up_to(end).map_err(|_| EngineError::InvalidPosition {
        position: until.clone(),
    })?;
    Ok(frame_batch(&cut))
}

/// The batch a primary ships or a follower serves: `batch` with its positions and payload
/// encoded.
fn frame_batch(batch: &Batch) -> FrameBatch {
    FrameBatch {
        start: encode_position(batch.start()),
        end: encode_position(batch.end()),
        payload: batch.to_bytes().into(),
    }
}

impl Database for SqliteDatabase {
    fn execute(
        &mut self,
        transaction: &Transaction,
        interrupt: &Interrupt,
    ) -> Result<Executed, EngineError> {
        // A caller that has already gone, or run out of time, gets nothing started.
        interrupt.check()?;
        // The caller runs writes only while no session holds the writer, so this never happens.
        if !self.writer.is_autocommit() {
            return Err(EngineError::Statement {
                message: "a session's transaction holds the database's writer".to_owned(),
            });
        }
        self.writer
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(classify)?;
        // Before the adapter touches its own tables, which a foreign object would subvert.
        if (self.marks.is_staged() || self.recorder.is_some())
            && let Err(error) = self.refuse_foreign_objects()
        {
            self.roll_back();
            return Err(error);
        }
        // Staged sink marks go in first, ahead of any prune the recorder does, so that no copy
        // holds a prune without the marks that justify it. They come before the row count below
        // starts, so `rows_affected` never counts them.
        if let Err(error) = self.marks.flush(&self.writer) {
            self.roll_back();
            return Err(error);
        }
        let start = self.end;
        if let Some(recorder) = &mut self.recorder
            && let Err(error) = recorder.begin(&self.writer, start)
        {
            self.roll_back();
            return Err(error);
        }
        let before = self.writer.total_changes();
        if let Err(error) = self.run_statements(transaction, interrupt) {
            self.roll_back();
            return Err(error);
        }
        // The recorder's own rows aren't the transaction's.
        let recorded = match &self.recorder {
            Some(recorder) => match recorder.commit(&self.writer) {
                Ok(recorded) => recorded,
                Err(error) => {
                    self.roll_back();
                    return Err(error);
                }
            },
            None => 0,
        };
        if let Err(error) = self.writer.execute_batch("COMMIT") {
            self.roll_back();
            return Err(classify(error));
        }
        self.marks.committed();
        if let Some(recorder) = &mut self.recorder {
            recorder.committed();
        }
        Ok(Executed {
            rows_affected: self
                .writer
                .total_changes()
                .saturating_sub(before)
                .saturating_sub(recorded),
        })
    }

    fn query(&mut self, statement: &Statement, interrupt: &Interrupt) -> Result<Rows, EngineError> {
        interrupt.check()?;
        // Confined while it steps too: SQLite prepares statements of its own while stepping some,
        // such as a pragma table-valued function's pragma, and reprepares after a schema change.
        let _user = UserStatement::enter(&self.reading);
        let _interruptible = Interruptible::watch(&self.reading, interrupt);
        let classify = |error: rusqlite::Error| {
            interrupted(&error, interrupt).unwrap_or_else(|| self.reading.classify(error))
        };
        let mut prepared = prepare_user(&self.reader, statement.sql()).map_err(classify)?;
        let columns: Vec<String> = prepared.column_names().map(str::to_owned).collect();
        let width = columns.len();
        let mut rows = prepared
            .query(rusqlite::params_from_iter(
                statement.params().iter().map(Param),
            ))
            .map_err(classify)?;
        let max = statement.max_rows().map_or(usize::MAX, |max| {
            usize::try_from(max.get()).unwrap_or(usize::MAX)
        });
        let mut out = Vec::new();
        let mut truncated = false;
        while let Some(row) = rows.next().map_err(classify)? {
            // A row past the limit is only looked at, never read: it says there are more.
            if out.len() == max {
                truncated = true;
                break;
            }
            let values = (0..width)
                .map(|index| row.get_ref(index).map(value_from_sql))
                .collect::<Result<Vec<_>, _>>()
                .map_err(classify)?;
            out.push(values);
        }
        Ok(Rows {
            columns,
            rows: out,
            truncated,
        })
    }

    fn schema(
        &mut self,
        request: &SchemaRequest,
        interrupt: &Interrupt,
    ) -> Result<Schema, EngineError> {
        interrupt.check()?;
        // The adapter's own statements, on the read connection, with the user confinement off.
        // The scan budget bounds the counting; the interrupt bounds it in time as well.
        let _interruptible = Interruptible::watch(&self.reading, interrupt);
        schema::read(&self.reader, request).map_err(|error| stopped_by(interrupt, error))
    }

    fn snapshot(&mut self, destination: &Path) -> Result<DatabaseSnapshot, EngineError> {
        // Writes and checkpoints run only on this database's thread, so between calls the
        // database file is stable and the WAL's committed prefix is complete.
        let position = self.read_wal(None)?.map(|batch| batch.end());
        fs::create_dir_all(destination).map_err(storage)?;
        let target = destination.join(DATABASE_FILE);
        copy_prefix(&self.path, &target, None).map_err(storage)?;
        let target_wal = wal_path(&target);
        match position {
            Some(position) => {
                copy_prefix(&self.wal_path(), &target_wal, Some(position.offset()))
                    .map_err(storage)?;
            }
            None => remove_if_present(&target_wal).map_err(storage)?,
        }
        let marker = checkpoint_path(&self.path);
        if marker.try_exists().map_err(storage)? {
            copy_prefix(&marker, &checkpoint_path(&target), None).map_err(storage)?;
        }
        sync_dir(destination).map_err(storage)?;
        Ok(DatabaseSnapshot {
            position: position.map(encode_position),
        })
    }

    fn capability(&mut self) -> DatabaseCapability<'_> {
        DatabaseCapability::FrameSource(self)
    }

    fn logical_changes(&mut self) -> Option<&mut dyn LogicalChanges> {
        Some(self)
    }

    fn sink_marks(&mut self) -> Option<&mut dyn SinkMarks> {
        Some(self)
    }

    fn sql_sessions(&mut self) -> Option<&mut dyn SqlSessions> {
        Some(self)
    }

    fn fork_reset(&mut self, marker: &ForkMarker) -> Result<ForkReset, EngineError> {
        if !self.writer.is_autocommit() {
            return Err(EngineError::Statement {
                message: "a session's transaction holds the database's writer".to_owned(),
            });
        }
        // The reset starts where the log ends now, and the change record's floor goes there.
        let start = self.read_wal(None)?.map(|batch| batch.end());
        self.writer
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(classify)?;
        // The adapter's own tables only, and none subverted by an object it didn't create.
        let outcome = self
            .refuse_foreign_objects()
            .and_then(|()| fork::reset(&self.writer, marker, start));
        match outcome {
            Ok(Outcome::Written) => {}
            Ok(Outcome::Held) => {
                self.roll_back();
                return Ok(ForkReset::AlreadyDone);
            }
            Err(error) => {
                self.roll_back();
                return Err(error);
            }
        }
        if let Err(error) = self.writer.execute_batch("COMMIT") {
            self.roll_back();
            return Err(classify(error));
        }
        self.marks.discard_staged();
        let position = self
            .read_wal(None)?
            .map(|batch| encode_position(batch.end()));
        position
            .map(|position| ForkReset::Written { position })
            .ok_or_else(|| storage(io::Error::other("the fork reset committed to an empty log")))
    }

    fn fork_marker(&mut self) -> Result<Option<ForkMarker>, EngineError> {
        self.refuse_foreign_objects()?;
        // The read connection sees only committed transactions.
        fork::read(&self.reader)
    }

    fn close(self: Box<Self>) -> Result<(), EngineError> {
        let Self { reader, writer, .. } = *self;
        reader.close().map_err(|(_, error)| storage(error))?;
        writer.close().map_err(|(_, error)| storage(error))
    }
}

impl FrameSource for SqliteDatabase {
    fn position(&mut self) -> Result<Option<Position>, EngineError> {
        Ok(self
            .read_wal(None)?
            .map(|batch| encode_position(batch.end())))
    }

    fn committed_since(
        &mut self,
        after: Option<&Position>,
    ) -> Result<Option<FrameBatch>, EngineError> {
        Ok(self.read_wal(after)?.as_ref().map(frame_batch))
    }

    fn committed_between(
        &mut self,
        after: Option<&Position>,
        until: &Position,
    ) -> Result<Option<FrameBatch>, EngineError> {
        let Some(batch) = self.read_wal(after)? else {
            return Ok(None);
        };
        cut_at(&batch, until).map(Some)
    }

    fn checkpoint(&mut self) -> Result<CheckpointMarker, EngineError> {
        // A snapshot being read must see the database file and the WAL as they were pinned.
        if self.pins.is_pinned(&self.path) {
            return Err(EngineError::CheckpointBlocked);
        }
        // Nor while a session's transaction holds the writer.
        if !self.writer.is_autocommit() {
            return Err(EngineError::CheckpointBlocked);
        }
        let position = self.position()?.ok_or(EngineError::CheckpointBlocked)?;
        // Recorded first, so a restart never forgets where the next generation continues from.
        let end = decode_position(&position)?;
        write_marker(&self.path, end).map_err(storage)?;
        self.checkpointed = Some(end);
        // RESTART, not TRUNCATE: the WAL keeps a header, so its checkpoint sequence survives a
        // close and reopen, and the next generation still orders after this one.
        checkpoint(&self.writer, Checkpoint::Restart)?;
        Ok(CheckpointMarker { position })
    }

    fn pin_snapshot(&mut self) -> Result<PinnedSnapshot, EngineError> {
        // Writes and checkpoints run only on this database's thread, so the committed prefix read
        // here is complete. The pin then holds off checkpoints, the only thing that rewrites the
        // database file, until the snapshot is dropped.
        let batch = self.read_wal(None)?;
        let header = batch.as_ref().and_then(|batch| batch.header().copied());
        let pin = self.pins.pin(&self.path, header);
        let mut files = Vec::with_capacity(3);
        if let Some(marker) = open_if_present(&checkpoint_path(&self.path)).map_err(storage)? {
            let len = marker.metadata().map_err(storage)?.len();
            files.push(SnapshotFile::new(CHECKPOINT_FILE, marker, len));
        }
        let database = File::open(&self.path).map_err(storage)?;
        let len = database.metadata().map_err(storage)?.len();
        files.push(SnapshotFile::new(DATABASE_FILE, database, len));
        let position = batch.map(|batch| batch.end());
        if let Some(position) = position {
            // The WAL comes last: the pin confirms its header is unchanged once it has been read,
            // which shows no new generation overwrote the prefix meanwhile.
            let wal = File::open(self.wal_path()).map_err(storage)?;
            files.push(SnapshotFile::new(WAL_FILE, wal, position.offset()));
        }
        Ok(PinnedSnapshot::new(
            position.map(encode_position),
            files,
            pin,
        ))
    }

    fn pin_snapshot_at(&mut self, at: Option<&Position>) -> Result<PinnedSnapshot, EngineError> {
        // As in `pin_snapshot`, the committed prefix read here is complete, and the pin holds
        // off checkpoints until the snapshot is dropped.
        let batch = self.read_wal(None)?;
        let in_log = match (at, &batch) {
            (Some(at), Some(batch)) => match cut_at(batch, at) {
                Ok(_) => Some(decode_position(at)?),
                Err(_) => None,
            },
            _ => None,
        };
        let marker = self.checkpointed.map(encode_position);
        let at_log_start = at.is_none() || at == marker.as_ref();
        if in_log.is_none() && !at_log_start {
            let position = at.cloned().unwrap_or_default();
            return Err(EngineError::InvalidPosition { position });
        }
        let header = in_log
            .and(batch.as_ref())
            .and_then(|batch| batch.header().copied());
        let pin = self.pins.pin(&self.path, header);
        let mut files = Vec::with_capacity(3);
        if let Some(marker) = open_if_present(&checkpoint_path(&self.path)).map_err(storage)? {
            let len = marker.metadata().map_err(storage)?.len();
            files.push(SnapshotFile::new(CHECKPOINT_FILE, marker, len));
        }
        let database = File::open(&self.path).map_err(storage)?;
        let len = database.metadata().map_err(storage)?.len();
        files.push(SnapshotFile::new(DATABASE_FILE, database, len));
        if let Some(end) = in_log {
            // The WAL's prefix up to `at`, last, as `pin_snapshot` reads it.
            let wal = File::open(self.wal_path()).map_err(storage)?;
            files.push(SnapshotFile::new(WAL_FILE, wal, end.offset()));
        }
        // Without the WAL, the main file holds the log up to the last checkpoint.
        Ok(PinnedSnapshot::new(at.cloned(), files, pin))
    }
}

impl LogicalChanges for SqliteDatabase {
    fn start_recording(&mut self, mode: CaptureMode) -> Result<(), EngineError> {
        if let Some(recorder) = &self.recorder {
            if recorder.mode() != mode {
                tracing::debug!(path = %self.path.display(), recording = %recorder.mode(), asked = %mode, "row changes are already being recorded in another mode");
            }
            return Ok(());
        }
        self.refuse_foreign_objects()?;
        // The first recorded transaction starts where the log ends now.
        let _primed = self.read_wal(None)?;
        self.recorder = Some(Recorder::install(&self.writer, mode)?);
        Ok(())
    }

    fn changes_between(
        &mut self,
        after: Option<&Position>,
        until: &Position,
    ) -> Result<Vec<RowChange>, EngineError> {
        // A reader at the log's beginning holds everything since the last checkpoint, whose end
        // is the position those transactions started from.
        let checkpointed = self.checkpointed.map(fleetring_wal::Position::to_bytes);
        let lower = after
            .map(Position::as_bytes)
            .or(checkpointed.as_ref().map(<[u8; 20]>::as_slice));
        changes::changes_between(&self.reader, lower, until.as_bytes())
    }

    fn release(&mut self, before: &Position) -> Result<(), EngineError> {
        if let Some(recorder) = &mut self.recorder {
            recorder.release(before.as_bytes());
        }
        Ok(())
    }

    fn transactions_after(
        &mut self,
        after: Option<&Position>,
        until: &Position,
        limit: ReadLimit,
    ) -> Result<Vec<RecordedTransaction>, EngineError> {
        if let Some(after) = after {
            decode_position(after)?;
        }
        // As in `changes_between`: a reader at the log's beginning reads from the last
        // checkpoint's end.
        let checkpointed = self.checkpointed.map(fleetring_wal::Position::to_bytes);
        let lower = after
            .map(Position::as_bytes)
            .or(checkpointed.as_ref().map(<[u8; 20]>::as_slice))
            .unwrap_or_default();
        match changes::transactions_after(&self.reader, lower, until.as_bytes(), limit)? {
            Watched::Held(transactions) => Ok(transactions),
            Watched::Released { oldest } => Err(EngineError::ChangesReleased {
                position: Position::new(lower),
                oldest: oldest.map(Position::from),
            }),
        }
    }
}

impl SinkMarks for SqliteDatabase {
    fn marks(&mut self) -> Result<BTreeMap<String, Position>, EngineError> {
        self.refuse_foreign_objects()?;
        Ok(Marks::read(&self.writer)?
            .into_iter()
            .map(|(sink, position)| (sink, Position::from(position)))
            .collect())
    }

    fn stage(&mut self, sink: &str, delivered: &Position) {
        self.marks.stage(sink, delivered.as_bytes());
    }
}

/// A `wal_checkpoint` mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Checkpoint {
    Passive,
    Restart,
    Truncate,
}

/// Runs a checkpoint and checks that it copied every frame.
fn checkpoint(conn: &Connection, mode: Checkpoint) -> Result<(), EngineError> {
    let sql = match mode {
        Checkpoint::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
        Checkpoint::Restart => "PRAGMA wal_checkpoint(RESTART)",
        Checkpoint::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
    };
    let (busy, log, done): (i64, i64, i64) = conn
        .query_row(sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .map_err(storage)?;
    if busy != 0 || (mode != Checkpoint::Truncate && log != done) {
        return Err(EngineError::CheckpointBlocked);
    }
    Ok(())
}

/// The confinement of user statements on one connection.
#[derive(Debug, Default)]
struct Confinement {
    /// Whether the connection is preparing or running a user statement, which the confinement
    /// applies to.
    user: AtomicBool,
    /// Whether the user statement being prepared defines a trigger or a view, whose body SQLite
    /// authorizes only when it runs, so the adapter checks it afterwards.
    defines_program: AtomicBool,
    /// Why the user statement was refused, kept for its error.
    refused: Mutex<Option<Refusal>>,
    /// What the user statement being prepared does to a transaction, for a SQL session to read
    /// (ADR-0037, point 12). Never refused here: `Execute` refuses transaction control itself.
    control: Mutex<Option<Control>>,
    /// The interrupt the connection's statements stop on, while an [`Interruptible`] watches it.
    interrupt: Mutex<Option<Interrupt>>,
}

impl Confinement {
    /// Records the first reason a user statement is refused.
    fn refuse(&self, refusal: Refusal) {
        self.refused
            .lock()
            .expect("refusal lock poisoned")
            .get_or_insert(refusal);
    }

    /// Sorts an error from preparing or running a user statement, reporting a refusal with its
    /// reason. A refused statement fails on the refusal, whichever code SQLite reports it with:
    /// a refused function, for one, is a plain error rather than an authorization failure.
    fn classify(&self, error: rusqlite::Error) -> EngineError {
        let refusal = self.refused.lock().expect("refusal lock poisoned").take();
        match refusal {
            Some(refusal) => EngineError::Statement {
                message: refusal.to_string(),
            },
            None => classify(error),
        }
    }
}

/// Why a user statement was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Refusal {
    /// It names something beginning with `_fleetring_`.
    Reserved,
    /// It attaches or detaches a database.
    OtherDatabase,
    /// It loads an extension.
    Extension,
    /// It runs a pragma that isn't read-only introspection.
    Pragma(String),
    /// It asks for an action the adapter doesn't recognize, by SQLite's action code.
    Unrecognized(i32),
    /// It creates a temporary table, index, trigger, or view.
    Temporary,
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reserved => write!(
                f,
                "names beginning with `{RESERVED_PREFIX}` are reserved for fleetringd"
            ),
            Self::OtherDatabase => f.write_str(
                "statements can't attach or detach databases: each runs only on the database it names",
            ),
            Self::Extension => f.write_str("statements can't load extensions"),
            Self::Pragma(name) => write!(
                f,
                "pragma `{name}` isn't allowed: statements may run only read-only introspection pragmas"
            ),
            Self::Unrecognized(code) => {
                write!(f, "statements can't perform SQLite action {code}")
            }
            Self::Temporary => f.write_str(
                "statements can't create temporary tables, indexes, triggers, or views: they \
                 would live on a connection the database's every caller shares, and outside its \
                 replicated log",
            ),
        }
    }
}

/// Installs the confinement of user statements on `conn`. While a user statement is being
/// prepared or run, the authorizer refuses one that attaches or detaches a database, loads an
/// extension, runs a pragma other than read-only introspection, or names a reserved table,
/// index, trigger, view, or function, unless the access comes from one of the recorder's own
/// triggers.
///
/// SQLite authorizes a statement when it is prepared, so the confinement applies only around
/// user statements; the adapter's own statements prepare with it clear.
///
/// It also installs the progress handler that stops statements once the interrupt an
/// [`Interruptible`] put in the confinement is raised.
///
/// # Errors
///
/// [`EngineError::Storage`] if SQLite can't install the authorizer or the progress handler.
fn confine(conn: &mut Connection) -> Result<Arc<Confinement>, EngineError> {
    let confinement = Arc::new(Confinement::default());
    let watched = Arc::clone(&confinement);
    conn.progress_handler(
        INTERRUPT_CHECK_OPS,
        Some(move || {
            watched
                .interrupt
                .lock()
                .expect("interrupt lock poisoned")
                .as_ref()
                .is_some_and(|interrupt| interrupt.raised().is_some())
        }),
    )
    .map_err(storage)?;
    let seen = Arc::clone(&confinement);
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        if !seen.user.load(Ordering::Relaxed) {
            return Authorization::Allow;
        }
        if context
            .accessor
            .is_some_and(|accessor| accessor.starts_with(TRIGGER_PREFIX))
        {
            return Authorization::Allow;
        }
        if let Some(control) = control_of(&context.action) {
            *seen.control.lock().expect("control lock poisoned") = Some(control);
        }
        if matches!(
            context.action,
            AuthAction::CreateTrigger { .. }
                | AuthAction::CreateTempTrigger { .. }
                | AuthAction::CreateView { .. }
                | AuthAction::CreateTempView { .. }
        ) {
            seen.defines_program.store(true, Ordering::Relaxed);
        }
        match refusal(&context.action) {
            Some(refusal) => {
                seen.refuse(refusal);
                Authorization::Deny
            }
            None => Authorization::Allow,
        }
    }))
    .map_err(storage)?;
    Ok(confinement)
}

/// What `action` does to a transaction, if it is a transaction's or a savepoint's control. The
/// authorizer reports `COMMIT` and `END` as a transaction operation it has no name for.
fn control_of(action: &AuthAction<'_>) -> Option<Control> {
    match *action {
        AuthAction::Transaction { operation } => Some(match operation {
            TransactionOperation::Begin => Control::Begin,
            TransactionOperation::Rollback => Control::Rollback,
            _ => Control::Commit,
        }),
        AuthAction::Savepoint { operation, .. } => Some(match operation {
            TransactionOperation::Begin => Control::Savepoint,
            TransactionOperation::Release => Control::Release,
            _ => Control::RollbackTo,
        }),
        _ => None,
    }
}

/// Why a user statement may not perform `action`, if it may not.
fn refusal(action: &AuthAction<'_>) -> Option<Refusal> {
    match *action {
        // Another database's file, or any file at all, is outside the named database.
        AuthAction::Attach { .. } | AuthAction::Detach { .. } => Some(Refusal::OtherDatabase),
        // An `ATTACH` or `DETACH` whose argument isn't a literal arrives without it.
        AuthAction::Unknown {
            code: ffi::SQLITE_ATTACH | ffi::SQLITE_DETACH,
            ..
        } => Some(Refusal::OtherDatabase),
        // Fail closed on anything the authorizer can't name.
        AuthAction::Unknown { code, .. } => Some(Refusal::Unrecognized(code)),
        AuthAction::Function { function_name }
            if function_name.eq_ignore_ascii_case("load_extension") =>
        {
            Some(Refusal::Extension)
        }
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } if !pragma_allowed(pragma_name, pragma_value) => {
            Some(Refusal::Pragma(pragma_name.to_ascii_lowercase()))
        }
        _ if names_reserved(action) => Some(Refusal::Reserved),
        // A temporary object lives on one connection, which every session and `Execute` share:
        // a temporary table would shadow a main one for everyone, diverting their writes off
        // the log, unreplicated, and a session's own would be missing on the read connection.
        AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. } => Some(Refusal::Temporary),
        _ => None,
    }
}

/// Whether a user statement may run pragma `name`, with `value` as its argument if it has one.
///
/// Only read-only introspection is allowed. Every other pragma, including the read form of one
/// not listed here, is refused: the rest set how the database is journaled, synced, locked,
/// checkpointed, cached, or trusted, or write its header or schema directly, all of which the
/// adapter owns.
fn pragma_allowed(name: &str, value: Option<&str>) -> bool {
    match name.to_ascii_lowercase().as_str() {
        // Each takes an object's name, or nothing, and only reports on the schema; none has a
        // setter.
        "table_info" | "table_xinfo" | "index_list" | "index_info" | "index_xinfo"
        | "foreign_key_list" => true,
        // `integrity_check` and `quick_check` take a table or an error limit and only read pages.
        // `foreign_key_check` isn't here: it reads the parent tables of the table it checks
        // without the authorizer seeing them, so a user table declaring a foreign key to a
        // reserved table would make it report which of that table's rows exist.
        "integrity_check" | "quick_check" => true,
        // Each reads a header field. With a value, `user_version`, `application_id`, and
        // `schema_version` write the header, the last also invalidating the schema; `page_size`
        // would change the page size the adapter chose.
        "user_version" | "application_id" | "schema_version" | "page_size" => value.is_none(),
        // Each reads a count from the header and has no setter; a value means nothing, so it is
        // refused rather than ignored.
        "page_count" | "freelist_count" => value.is_none(),
        // Reads the connection's count of commits by other connections and has no setter. FTS5
        // runs it while a statement steps, to learn whether its cached index structure is stale,
        // so refusing it would refuse every later write to and search of a full-text table.
        "data_version" => value.is_none(),
        // Lists the options SQLite was built with and reads nothing of the database.
        "compile_options" => value.is_none(),
        _ => false,
    }
}

/// Fails if a trigger or view in the schema names something reserved, after a user statement
/// defined one. SQLite authorizes a trigger's or a view's body only when it runs, which would
/// then fail every statement that fires it; refusing the definition is clearer.
fn refuse_programs_on_reserved_names(conn: &Connection) -> Result<(), EngineError> {
    let pattern = format!("%{}%", RESERVED_PREFIX.replace('_', "\\_"));
    let offending: Option<String> = conn
        .query_row(
            "SELECT name FROM (
                SELECT type, name, sql FROM main.sqlite_master UNION ALL
                SELECT type, name, sql FROM temp.sqlite_master)
             WHERE type IN ('trigger', 'view')
               AND sql LIKE ?1 ESCAPE '\\' AND name NOT LIKE ?2 ESCAPE '\\' LIMIT 1",
            params![pattern, format!("{}%", TRIGGER_PREFIX.replace('_', "\\_"))],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    match offending {
        Some(name) => Err(EngineError::Statement {
            message: format!(
                "`{name}` refers to a name beginning with `{RESERVED_PREFIX}`, which is reserved for fleetringd"
            ),
        }),
        None => Ok(()),
    }
}

/// Whether `action` names something the adapter reserves.
fn names_reserved(action: &AuthAction<'_>) -> bool {
    match *action {
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::CreateTempIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropTempIndex {
            index_name,
            table_name,
        } => is_reserved(index_name) || is_reserved(table_name),
        AuthAction::CreateTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::CreateTempTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTempTrigger {
            trigger_name,
            table_name,
        } => is_reserved(trigger_name) || is_reserved(table_name),
        AuthAction::CreateView { view_name }
        | AuthAction::CreateTempView { view_name }
        | AuthAction::DropView { view_name }
        | AuthAction::DropTempView { view_name } => is_reserved(view_name),
        AuthAction::CreateTable { table_name }
        | AuthAction::CreateTempTable { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::DropTempTable { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Insert { table_name }
        | AuthAction::Read { table_name, .. }
        | AuthAction::Update { table_name, .. }
        | AuthAction::AlterTable { table_name, .. }
        | AuthAction::Analyze { table_name }
        | AuthAction::CreateVtable { table_name, .. }
        | AuthAction::DropVtable { table_name, .. } => is_reserved(table_name),
        AuthAction::Reindex { index_name } => is_reserved(index_name),
        AuthAction::Function { function_name } => is_reserved(function_name),
        _ => false,
    }
}

/// Marks a connection as preparing or running a user statement while it lives.
struct UserStatement<'c>(&'c Confinement);

impl<'c> UserStatement<'c> {
    fn enter(confinement: &'c Confinement) -> Self {
        confinement.user.store(true, Ordering::Relaxed);
        confinement.defines_program.store(false, Ordering::Relaxed);
        *confinement.refused.lock().expect("refusal lock poisoned") = None;
        *confinement.control.lock().expect("control lock poisoned") = None;
        Self(confinement)
    }
}

impl Drop for UserStatement<'_> {
    fn drop(&mut self) {
        self.0.user.store(false, Ordering::Relaxed);
    }
}

/// Turns on SQLite's defensive mode, which stops any statement from corrupting the database
/// through `writable_schema`, shadow-table or `sqlite_dbpage` writes, or `journal_mode = OFF`.
/// Extension loading stays off, as SQLite opens every connection.
fn defend(conn: &Connection) -> Result<(), EngineError> {
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)
        .map_err(storage)?;
    Ok(())
}

/// Stops the last connection to close from checkpointing and deleting the WAL.
fn keep_wal_on_close(conn: &Connection) -> Result<(), EngineError> {
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
        .map_err(storage)?;
    Ok(())
}

/// Stops the statements a connection runs once an [`Interrupt`] is raised, while it lives: it
/// installs the interrupt in its connection's [`Confinement`], which the progress handler that
/// [`confine`] installed checks, and takes it out again when dropped. With none installed, the
/// handler lets every statement run on.
///
/// The handler stays installed for the connection's life because installing one needs the
/// connection exclusively, which a statement prepared on it, and stepped while watched, holds
/// shared.
struct Interruptible<'c>(&'c Confinement);

impl<'c> Interruptible<'c> {
    fn watch(confinement: &'c Confinement, interrupt: &Interrupt) -> Self {
        *confinement
            .interrupt
            .lock()
            .expect("interrupt lock poisoned") = Some(interrupt.clone());
        Self(confinement)
    }
}

impl Drop for Interruptible<'_> {
    fn drop(&mut self) {
        *self.0.interrupt.lock().expect("interrupt lock poisoned") = None;
    }
}

/// The error for a statement `interrupt` stopped, if `error` is SQLite's report of one.
fn interrupted(error: &rusqlite::Error, interrupt: &Interrupt) -> Option<EngineError> {
    if error.sqlite_error_code() != Some(ErrorCode::OperationInterrupted) {
        return None;
    }
    interrupt.raised().map(EngineError::Interrupted)
}

/// `error`, or [`EngineError::Interrupted`] if it is a storage error that SQLite reported because
/// `interrupt` stopped one of the adapter's own statements.
fn stopped_by(interrupt: &Interrupt, error: EngineError) -> EngineError {
    let stopped = match &error {
        EngineError::Storage(source) => source
            .downcast_ref::<rusqlite::Error>()
            .and_then(|source| interrupted(source, interrupt)),
        _ => None,
    };
    stopped.unwrap_or(error)
}

/// Runs a user statement on `conn`, which `confinement` confines, until `interrupt` stops it.
fn run(
    conn: &Connection,
    statement: &Statement,
    confinement: &Confinement,
    interrupt: &Interrupt,
) -> Result<(), EngineError> {
    let classify = |error: rusqlite::Error| {
        interrupted(&error, interrupt).unwrap_or_else(|| confinement.classify(error))
    };
    let mut prepared = prepare_user(conn, statement.sql()).map_err(classify)?;
    let mut rows = prepared
        .query(rusqlite::params_from_iter(
            statement.params().iter().map(Param),
        ))
        .map_err(classify)?;
    while rows.next().map_err(classify)?.is_some() {}
    Ok(())
}

/// A user statement, prepared by [`prepare_user`].
enum UserPrepared<'conn> {
    /// Taken from, or added to, the connection's statement cache.
    Cached(CachedStatement<'conn>),
    /// Prepared afresh, and finalized when dropped.
    Fresh(rusqlite::Statement<'conn>),
}

impl<'conn> Deref for UserPrepared<'conn> {
    type Target = rusqlite::Statement<'conn>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Cached(statement) => statement,
            Self::Fresh(statement) => statement,
        }
    }
}

impl DerefMut for UserPrepared<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Cached(statement) => statement,
            Self::Fresh(statement) => statement,
        }
    }
}

/// Prepares a user statement on `conn`, with the confinement already entered.
///
/// SQLite authorizes a statement when it is prepared, not when it runs, and the adapter keeps its
/// own statements in the connection's statement cache, prepared with the confinement clear. A
/// user statement of exactly the same text taken from the cache would run the adapter's
/// statement, unauthorized: a write could then forge sink marks with the adapter's own upsert,
/// or a query read the change record. So a user statement that mentions the reserved prefix anywhere,
/// which every such statement of the adapter's does, is prepared afresh, and refused; the rest
/// share the cache.
fn prepare_user<'conn>(
    conn: &'conn Connection,
    sql: &str,
) -> rusqlite::Result<UserPrepared<'conn>> {
    if mentions_reserved(sql) {
        conn.prepare(sql).map(UserPrepared::Fresh)
    } else {
        conn.prepare_cached(sql).map(UserPrepared::Cached)
    }
}

/// A parameter bound by reference.
#[derive(Debug)]
struct Param<'a>(&'a Value);

impl ToSql for Param<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self.0 {
            Value::Null => ValueRef::Null,
            Value::Integer(value) => ValueRef::Integer(*value),
            Value::Real(value) => ValueRef::Real(*value),
            Value::Text(value) => ValueRef::Text(value.as_bytes()),
            Value::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

fn value_from_sql(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Integer(value),
        ValueRef::Real(value) => Value::Real(value),
        ValueRef::Text(value) => Value::Text(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::Blob(value.to_vec()),
    }
}

/// Sorts an error from running SQL into a statement error or a storage failure.
fn classify(error: rusqlite::Error) -> EngineError {
    if matches!(error, rusqlite::Error::MultipleStatement) {
        return EngineError::Statement {
            message: ONE_STATEMENT.to_owned(),
        };
    }
    let storage_failure = matches!(
        error.sqlite_error_code(),
        Some(
            ErrorCode::SystemIoFailure
                | ErrorCode::DatabaseCorrupt
                | ErrorCode::DiskFull
                | ErrorCode::CannotOpen
                | ErrorCode::NotADatabase
                | ErrorCode::OutOfMemory
                | ErrorCode::FileLockingProtocolFailed
        )
    );
    if storage_failure {
        storage(error)
    } else {
        EngineError::Statement {
            message: error.to_string(),
        }
    }
}

fn storage(error: impl Into<Box<dyn Error + Send + Sync>>) -> EngineError {
    EngineError::Storage(error.into())
}

fn encode_position(position: fleetring_wal::Position) -> Position {
    Position::new(position.to_bytes())
}

fn decode_position(position: &Position) -> Result<fleetring_wal::Position, EngineError> {
    fleetring_wal::Position::from_bytes(position.as_bytes()).map_err(|_| {
        EngineError::InvalidPosition {
            position: position.clone(),
        }
    })
}

fn wal_path(database: &Path) -> PathBuf {
    let mut path = OsString::from(database.as_os_str());
    path.push("-wal");
    PathBuf::from(path)
}

fn checkpoint_path(database: &Path) -> PathBuf {
    let mut path = OsString::from(database.as_os_str());
    path.push("-checkpoint");
    PathBuf::from(path)
}

/// Reads where the last checkpoint of the database at `database` ended, if one is recorded.
fn read_marker(database: &Path) -> Result<Option<fleetring_wal::Position>, EngineError> {
    let bytes = match fs::read(checkpoint_path(database)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    fleetring_wal::Position::from_bytes(&bytes)
        .map(Some)
        .map_err(storage)
}

/// Records durably that the last checkpoint of the database at `database` ended at `end`.
fn write_marker(database: &Path, end: fleetring_wal::Position) -> io::Result<()> {
    let target = checkpoint_path(database);
    let mut staged = OsString::from(target.as_os_str());
    staged.push(".tmp");
    let staged = PathBuf::from(staged);
    let mut file = File::create(&staged)?;
    file.write_all(&end.to_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(&staged, &target)?;
    match database.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => sync_dir(dir),
        _ => sync_dir(Path::new(".")),
    }
}

fn open_if_present(path: &Path) -> io::Result<Option<File>> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn shm_path(database: &Path) -> PathBuf {
    let mut path = OsString::from(database.as_os_str());
    path.push("-shm");
    PathBuf::from(path)
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Copies `source`, or its first `len` bytes, to `target` and fsyncs the copy.
fn copy_prefix(source: &Path, target: &Path, len: Option<u64>) -> io::Result<()> {
    let mut input = File::open(source)?;
    let mut output = File::create(target)?;
    match len {
        Some(len) => {
            let copied = io::copy(&mut (&mut input).take(len), &mut output)?;
            if copied != len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{} ended after {copied} of {len} bytes", source.display()),
                ));
            }
        }
        None => {
            io::copy(&mut input, &mut output)?;
        }
    }
    output.sync_all()
}

/// Makes new directory entries in `dir` durable. Only Unix can sync a directory.
fn sync_dir(dir: &Path) -> io::Result<()> {
    if cfg!(unix) {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}
