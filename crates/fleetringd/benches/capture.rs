//! Change-capture overhead on the write path (FR-039, ADR-0024). Run with
//! `cargo bench -p fleetringd --bench capture`.
//!
//! Each case opens one SQLite database as the daemon does, with `synchronous = FULL`, and runs
//! the primary's write path on it: one transaction of 1 or 100 inserted rows, then the read of
//! the log's new frames that the actor does after every commit, and a checkpoint whenever the
//! WAL passes the daemon's default threshold. Capture is off, or on in one of the modes; a
//! recorded database releases every delivered batch at once, as a sink that keeps up would, so
//! the pruning of the change table is part of the cost. Numbers live in `docs/perf.md`.

use std::fmt;
use std::fs;
use std::hint::black_box;

use divan::Bencher;
use fleetringd::engine::{
    CaptureMode, Database, DatabaseCapability, DatabaseEngine, OpenMode, Position, SqliteEngine,
    Statement, Transaction, Value,
};
use tempfile::TempDir;

fn main() {
    divan::main();
}

/// Whether the database records row changes, and how much of each row.
#[derive(Debug, Clone, Copy)]
enum Capture {
    Off,
    On(CaptureMode),
}

/// One benchmark case: a capture setting and a transaction size.
#[derive(Debug, Clone, Copy)]
struct Case {
    capture: Capture,
    rows: usize,
}

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let capture = match self.capture {
            Capture::Off => "off",
            Capture::On(mode) => mode.as_str(),
        };
        write!(f, "{capture}, {} rows", self.rows)
    }
}

const CASES: &[Case] = &[
    Case {
        capture: Capture::Off,
        rows: 1,
    },
    Case {
        capture: Capture::On(CaptureMode::Id),
        rows: 1,
    },
    Case {
        capture: Capture::On(CaptureMode::After),
        rows: 1,
    },
    Case {
        capture: Capture::On(CaptureMode::Full),
        rows: 1,
    },
    Case {
        capture: Capture::Off,
        rows: 100,
    },
    Case {
        capture: Capture::On(CaptureMode::Id),
        rows: 100,
    },
    Case {
        capture: Capture::On(CaptureMode::After),
        rows: 100,
    },
    Case {
        capture: Capture::On(CaptureMode::Full),
        rows: 100,
    },
];

/// The page size the daemon gives new databases by default.
const PAGE_SIZE: u32 = 4096;

/// The WAL size at which the daemon checkpoints by default (`checkpoint_threshold_bytes`).
const CHECKPOINT_THRESHOLD: u64 = 4 * 1024 * 1024;

/// An open primary and the state the actor keeps around it.
struct Writer {
    dir: TempDir,
    db: Box<dyn Database>,
    last: Option<Position>,
    next_id: i64,
}

impl Writer {
    fn open(capture: Capture) -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let engine = SqliteEngine::new(PAGE_SIZE).expect("a valid page size");
        let mut db = engine
            .open(dir.path(), OpenMode::Create)
            .expect("the database opens");
        db.execute(&Transaction::from(Statement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, note BLOB)",
        )))
        .expect("the schema applies");
        if let Capture::On(mode) = capture {
            db.logical_changes()
                .expect("SQLite records row changes")
                .start_recording(mode)
                .expect("recording starts");
        }
        let mut writer = Self {
            dir,
            db,
            last: None,
            next_id: 0,
        };
        writer.capture();
        writer
    }

    /// Inserts `rows` rows in one transaction, then captures the batch and releases it, as the
    /// actor and a caught-up sink do.
    fn write(&mut self, rows: usize) {
        let statements: Vec<Statement> = (0..rows)
            .map(|_| {
                self.next_id += 1;
                Statement::with_params(
                    "INSERT INTO t (id, name, score, note) VALUES (?1, ?2, ?3, ?4)",
                    [
                        Value::Integer(self.next_id),
                        Value::Text(format!("row {}", self.next_id)),
                        Value::Real(0.5),
                        Value::Blob(vec![7; 16]),
                    ],
                )
            })
            .collect();
        self.db
            .execute(&Transaction::new(statements))
            .expect("the write commits");
        self.capture();
        if let (Some(last), Some(logical)) = (self.last.clone(), self.db.logical_changes()) {
            logical.release(&last).expect("the release is recorded");
        }
        let wal = fs::metadata(self.dir.path().join("data.db-wal")).map_or(0, |meta| meta.len());
        if wal >= CHECKPOINT_THRESHOLD {
            let DatabaseCapability::FrameSource(frames) = self.db.capability() else {
                panic!("SQLite is a frame source");
            };
            frames.checkpoint().expect("the checkpoint completes");
        }
    }

    /// Reads the log's new frames, as the actor does after every commit.
    fn capture(&mut self) {
        let DatabaseCapability::FrameSource(frames) = self.db.capability() else {
            panic!("SQLite is a frame source");
        };
        if let Some(batch) = frames
            .committed_since(self.last.as_ref())
            .expect("the log is readable")
        {
            self.last = Some(black_box(batch).end);
        }
    }
}

#[divan::bench(args = CASES, sample_count = 40, sample_size = 5)]
fn insert(bencher: Bencher, case: &Case) {
    let mut writer = Writer::open(case.capture);
    bencher.bench_local(|| writer.write(case.rows));
}
