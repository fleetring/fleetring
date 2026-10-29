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
//! Row changes for logical change streams are recorded by the `changes` module, in a side table
//! inside the database (FR-039).

mod changes;
mod follower;
mod pins;

use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Read, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fleetring_wal::{Batch, ReadError, ReadFileError, read_committed_file};
use rusqlite::config::DbConfig;
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{Connection, ErrorCode, OpenFlags, ToSql};

pub use follower::SqliteFollower;

use self::changes::Recorder;
use self::pins::Pins;
use crate::engine::{
    CheckpointMarker, Database, DatabaseCapability, DatabaseEngine, EngineCapability, EngineError,
    EngineKind, Executed, FrameBatch, FrameFollower, FrameSource, FrameSourceEngine,
    LogicalChanges, LogicalSupport, OpenMode, PinnedSnapshot, Position, RowChange, Rows, Snapshot,
    SnapshotFile, Statement, Transaction, Value,
};

/// The database file inside a database's location. The WAL sits next to it.
const DATABASE_FILE: &str = "data.db";

/// The WAL file inside a database's location.
const WAL_FILE: &str = "data.db-wal";

/// The file inside a database's location that records where the last checkpoint ended.
const CHECKPOINT_FILE: &str = "data.db-checkpoint";

/// The smallest and largest page sizes SQLite supports.
const PAGE_SIZES: std::ops::RangeInclusive<u32> = 512..=65536;

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
/// use fleetringd::engine::{DatabaseEngine, OpenMode, SqliteEngine, Statement, Transaction};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let dir = tempfile::tempdir()?;
/// let engine = SqliteEngine::new(4096)?;
/// let mut db = engine.open(dir.path(), OpenMode::Create)?;
/// db.execute(&Transaction::from(Statement::new("CREATE TABLE t (v)")))?;
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
}

/// An open SQLite primary: one write connection and one read-only connection.
#[derive(Debug)]
struct SqliteDatabase {
    path: PathBuf,
    writer: Connection,
    reader: Connection,
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
}

impl SqliteDatabase {
    fn open(
        path: PathBuf,
        mode: OpenMode,
        page_size: u32,
        pins: Arc<Pins>,
    ) -> Result<Self, EngineError> {
        let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        if mode == OpenMode::Create {
            flags |= OpenFlags::SQLITE_OPEN_CREATE;
        }
        let writer = Connection::open_with_flags(&path, flags).map_err(storage)?;
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
        // commit after a checkpoint, without ever deleting its header.
        writer
            .execute_batch(
                "PRAGMA wal_autocheckpoint = 0;
                 PRAGMA synchronous = FULL;
                 PRAGMA journal_size_limit = 0;",
            )
            .map_err(storage)?;
        let reader = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(storage)?;
        keep_wal_on_close(&reader)?;
        reader
            .execute_batch("PRAGMA query_only = 1; PRAGMA wal_autocheckpoint = 0;")
            .map_err(storage)?;
        let checkpointed = read_marker(&path)?;
        Ok(Self {
            path,
            writer,
            reader,
            pins,
            checkpointed,
            end: None,
            recorder: None,
        })
    }

    fn wal_path(&self) -> PathBuf {
        wal_path(&self.path)
    }

    fn run_statements(&mut self, transaction: &Transaction) -> Result<(), EngineError> {
        for statement in transaction.statements() {
            run(&self.writer, statement)?;
            if self.writer.is_autocommit() {
                return Err(EngineError::TransactionControl);
            }
            if let Some(recorder) = &mut self.recorder {
                recorder.after_statement(&self.writer)?;
            }
        }
        Ok(())
    }

    fn roll_back(&self) {
        if self.writer.is_autocommit() {
            return;
        }
        if let Err(error) = self.writer.execute_batch("ROLLBACK") {
            tracing::error!(path = %self.path.display(), %error, "rollback failed");
        }
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
    fn execute(&mut self, transaction: &Transaction) -> Result<Executed, EngineError> {
        self.writer
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(classify)?;
        let start = self.end;
        if let Some(recorder) = &mut self.recorder
            && let Err(error) = recorder.begin(&self.writer, start)
        {
            self.roll_back();
            return Err(error);
        }
        let before = self.writer.total_changes();
        if let Err(error) = self.run_statements(transaction) {
            self.roll_back();
            return Err(error);
        }
        // The recorder's own rows aren't the transaction's.
        let recorded = match &self.recorder {
            Some(recorder) => match recorder.recorded(&self.writer, start) {
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
        Ok(Executed {
            rows_affected: self
                .writer
                .total_changes()
                .saturating_sub(before)
                .saturating_sub(recorded),
        })
    }

    fn query(&mut self, statement: &Statement) -> Result<Rows, EngineError> {
        let mut prepared = self
            .reader
            .prepare_cached(statement.sql())
            .map_err(classify)?;
        let columns: Vec<String> = prepared
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect();
        let width = columns.len();
        let mut rows = prepared
            .query(rusqlite::params_from_iter(
                statement.params().iter().map(Param),
            ))
            .map_err(classify)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(classify)? {
            let values = (0..width)
                .map(|index| row.get_ref(index).map(value_from_sql))
                .collect::<Result<Vec<_>, _>>()
                .map_err(classify)?;
            out.push(values);
        }
        Ok(Rows { columns, rows: out })
    }

    fn snapshot(&mut self, destination: &Path) -> Result<Snapshot, EngineError> {
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
        Ok(Snapshot {
            position: position.map(encode_position),
        })
    }

    fn capability(&mut self) -> DatabaseCapability<'_> {
        DatabaseCapability::FrameSource(self)
    }

    fn logical_changes(&mut self) -> Option<&mut dyn LogicalChanges> {
        Some(self)
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
        let end = decode_position(until)?;
        if batch.end() == end {
            return Ok(Some(frame_batch(&batch)));
        }
        let cut = batch.up_to(end).map_err(|_| EngineError::InvalidPosition {
            position: until.clone(),
        })?;
        Ok(Some(frame_batch(&cut)))
    }

    fn checkpoint(&mut self) -> Result<CheckpointMarker, EngineError> {
        // A snapshot being read must see the database file and the WAL as they were pinned.
        if self.pins.is_pinned(&self.path) {
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
}

impl LogicalChanges for SqliteDatabase {
    fn start_recording(&mut self) -> Result<(), EngineError> {
        if self.recorder.is_some() {
            return Ok(());
        }
        // The first recorded transaction starts where the log ends now.
        let _primed = self.read_wal(None)?;
        self.recorder = Some(Recorder::install(&self.writer)?);
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

/// Stops the last connection to close from checkpointing and deleting the WAL.
fn keep_wal_on_close(conn: &Connection) -> Result<(), EngineError> {
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
        .map_err(storage)?;
    Ok(())
}

fn run(conn: &Connection, statement: &Statement) -> Result<(), EngineError> {
    let mut prepared = conn.prepare_cached(statement.sql()).map_err(classify)?;
    let mut rows = prepared
        .query(rusqlite::params_from_iter(
            statement.params().iter().map(Param),
        ))
        .map_err(classify)?;
    while rows.next().map_err(classify)?.is_some() {}
    Ok(())
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
