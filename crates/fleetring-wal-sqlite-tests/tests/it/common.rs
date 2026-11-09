//! A real SQLite primary and a follower that mirrors its WAL.

use std::fs;
use std::path::{Path, PathBuf};

use fleetring_wal::{Batch, Mirror, read_committed_file};
use rusqlite::Connection;
use tempfile::TempDir;

/// A checkpoint mode for `PRAGMA wal_checkpoint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Checkpoint {
    /// Copies frames into the database and leaves the WAL file in place; the next write restarts
    /// the WAL over its old frames.
    Passive,
    /// Copies frames into the database and empties the WAL file.
    Truncate,
}

/// The path of a database's WAL file.
pub(crate) fn wal_path(db: &Path) -> PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push("-wal");
    PathBuf::from(path)
}

/// Opens a primary the way the adapter does: WAL mode, no automatic checkpoints, full sync.
pub(crate) fn open_primary(db: &Path, page_size: u32) -> Connection {
    let conn = Connection::open(db).expect("open primary");
    conn.execute_batch(&format!("PRAGMA page_size = {page_size};"))
        .expect("page size");
    let mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .expect("journal mode");
    assert_eq!(mode, "wal");
    conn.execute_batch(
        "PRAGMA wal_autocheckpoint = 0;
         PRAGMA synchronous = FULL;
         CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v BLOB NOT NULL);",
    )
    .expect("set up primary");
    conn
}

/// Runs a checkpoint and asserts that it completed.
pub(crate) fn checkpoint(conn: &Connection, mode: Checkpoint) {
    let sql = match mode {
        Checkpoint::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
        Checkpoint::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
    };
    let (busy, log, done): (i64, i64, i64) = conn
        .query_row(sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .expect("checkpoint");
    assert_eq!(busy, 0, "{mode:?} checkpoint was blocked");
    assert!(
        mode == Checkpoint::Truncate || log == done,
        "{mode:?} copied {done} of {log} frames"
    );
}

/// Every row of `t`, in key order.
pub(crate) fn rows(conn: &Connection) -> Vec<(i64, Vec<u8>)> {
    let mut stmt = conn
        .prepare("SELECT id, v FROM t ORDER BY id")
        .expect("prepare");
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows")
}

/// Runs `PRAGMA integrity_check` and returns its single row.
pub(crate) fn integrity(conn: &Connection) -> String {
    conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity check")
}

/// A primary database and a follower that mirrors its WAL into its own `-wal` file.
#[derive(Debug)]
pub(crate) struct Pair {
    _dir: TempDir,
    pub(crate) primary: Connection,
    pub(crate) primary_db: PathBuf,
    pub(crate) follower_db: PathBuf,
    pub(crate) mirror: Mirror,
}

impl Pair {
    /// Creates a primary with an empty table, and a follower from a snapshot of it.
    pub(crate) fn new(page_size: u32) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let primary_db = dir.path().join("primary.db");
        let follower_db = dir.path().join("follower.db");
        let primary = open_primary(&primary_db, page_size);
        checkpoint(&primary, Checkpoint::Truncate);
        fs::copy(&primary_db, &follower_db).expect("snapshot the primary");
        let mirror = Mirror::open(wal_path(&follower_db)).expect("open mirror");
        assert_eq!(mirror.position(), None);
        Self {
            _dir: dir,
            primary,
            primary_db,
            follower_db,
            mirror,
        }
    }

    /// The primary's WAL file.
    pub(crate) fn primary_wal(&self) -> PathBuf {
        wal_path(&self.primary_db)
    }

    /// Ships everything new on the primary to the follower as one payload, as the adapter does.
    /// Returns the batch shipped, if any.
    pub(crate) fn ship(&mut self) -> Option<Batch> {
        let batch =
            read_committed_file(self.primary_wal(), self.mirror.position()).expect("read")?;
        let batch = Batch::from_bytes(&batch.to_bytes()).expect("payload round-trips");
        if let Some(header) = batch.header()
            && self.mirror.header() != Some(header)
        {
            self.checkpoint_follower();
            self.mirror
                .start_generation(header)
                .expect("start generation");
        }
        let end = self.mirror.append(&batch).expect("append");
        assert_eq!(end, batch.end());
        Some(batch)
    }

    /// Opens the follower as a promotion does: drop any stale shared-memory index, then let
    /// SQLite recover the mirrored WAL.
    pub(crate) fn open_follower(&self) -> Connection {
        let mut shm = self.follower_db.as_os_str().to_owned();
        shm.push("-shm");
        match fs::remove_file(PathBuf::from(shm)) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => panic!("remove stale shm: {err}"),
        }
        Connection::open(&self.follower_db).expect("open follower")
    }

    /// Checkpoints the follower's mirrored WAL into its database.
    pub(crate) fn checkpoint_follower(&self) {
        let conn = self.open_follower();
        checkpoint(&conn, Checkpoint::Truncate);
        conn.close().expect("close follower");
    }
}
