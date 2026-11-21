//! A database written by sqlite-vec 0.1.9, checked in, so that an upgrade of the pinned release
//! is tested against the on-disk format of the one before it (ADR-0049, point 5).
//!
//! `tests/fixtures/vec0-v0.1.9.db` was written by `write_the_v0_1_9_fixture` below, which runs only
//! when asked (`cargo test -p fleetring-sqlite-vec -- --ignored`) and only with 0.1.9 registered:
//! once the pin moves, the fixture can't be made again, and it stays as it is.

use std::error::Error;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};

type TestResult = Result<(), Box<dyn Error>>;

/// The fixture, under this crate.
fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/vec0-v0.1.9.db")
}

/// Row `i`'s float vector, as JSON text, from integers alone.
fn embedding(i: i64) -> String {
    format!("[{}, {}, {}, 1]", i % 7, (i * 3) % 5, 10 - i % 10)
}

/// A text longer than the 16 bytes vec0 keeps inline for a text metadata value, for row `i`.
fn long_kind(i: i64) -> String {
    format!("a kind long enough to spill, number {}", i % 3)
}

/// A `vec0` table using every kind of column 0.1.9 has: a partition key, two vector columns of
/// different element types, integer and text metadata (short and long texts), and an auxiliary
/// column, in chunks of 8, holding 20 rows of which row 5 was deleted and row 6 updated.
#[test]
#[ignore = "writes the checked-in fixture; run only with sqlite-vec 0.1.9"]
fn write_the_v0_1_9_fixture() -> TestResult {
    let conn = Connection::open_in_memory()?;
    fleetring_sqlite_vec::register(&conn)?;
    assert_eq!(
        fleetring_sqlite_vec::VERSION,
        "v0.1.9",
        "the fixture is 0.1.9's"
    );
    conn.execute_batch(
        "PRAGMA page_size = 1024;
         CREATE VIRTUAL TABLE notes USING vec0(
           owner INTEGER PARTITION KEY,
           embedding float[4] distance_metric=cosine,
           tags int8[2],
           score INTEGER,
           kind TEXT,
           +body TEXT,
           chunk_size = 8
         );",
    )?;
    for i in 1..=20_i64 {
        let kind = if i % 2 == 0 {
            long_kind(i)
        } else {
            "short".to_owned()
        };
        conn.execute(
            "INSERT INTO notes (rowid, owner, embedding, tags, score, kind, body)
             VALUES (?1, ?2, ?3, vec_int8(?4), ?5, ?6, ?7)",
            params![
                i,
                i % 2,
                embedding(i),
                format!("[{}, {}]", i, -i),
                i * 10,
                kind,
                format!("note {i}")
            ],
        )?;
    }
    conn.execute_batch(
        "DELETE FROM notes WHERE rowid = 5;
         UPDATE notes SET embedding = '[6, 6, 6, 6]', body = 'note 6, updated' WHERE rowid = 6;",
    )?;
    let path = fixture();
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    conn.execute("VACUUM INTO ?1", params![path.to_string_lossy()])?;
    Ok(())
}

#[test]
fn a_table_written_by_v0_1_9_reads_and_takes_writes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("copy.db");
    std::fs::copy(fixture(), &path)?;
    let conn = Connection::open(&path)?;
    fleetring_sqlite_vec::register(&conn)?;
    assert_eq!(
        conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
        "ok"
    );
    let created: String = conn.query_row(
        "SELECT value FROM notes_info WHERE key = 'CREATE_VERSION'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(created, "v0.1.9");

    let count: i64 = conn.query_row("SELECT count(*) FROM notes", [], |row| row.get(0))?;
    assert_eq!(count, 19);
    // A k-nearest query within one partition, filtered by metadata, with the auxiliary column.
    let mut nearest = conn.prepare(
        "SELECT rowid, kind, body FROM notes
          WHERE embedding MATCH '[6, 6, 6, 6]' AND k = 3 AND owner = 0 AND score > 0
          ORDER BY distance",
    )?;
    let rows = nearest
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<Vec<(i64, String, String)>>>()?;
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        (6, long_kind(6), "note 6, updated".to_owned()),
        "{rows:?}"
    );
    let (tags, kind): (String, String) = conn.query_row(
        "SELECT vec_to_json(tags), kind FROM notes WHERE rowid = 12",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(tags, "[12,-12]");
    assert_eq!(kind, long_kind(12));

    // It takes writes into its existing chunks and new ones.
    conn.execute(
        "INSERT INTO notes (rowid, owner, embedding, tags, score, kind, body)
         VALUES (5, 1, '[1, 1, 1, 1]', vec_int8('[5, -5]'), 50, 'short', 'note 5, again')",
        [],
    )?;
    for i in 21..=30_i64 {
        conn.execute(
            "INSERT INTO notes (rowid, owner, embedding, tags, score, kind, body)
             VALUES (?1, 0, ?2, vec_int8('[0, 0]'), 0, 'short', 'new')",
            params![i, embedding(i)],
        )?;
    }
    let count: i64 = conn.query_row("SELECT count(*) FROM notes", [], |row| row.get(0))?;
    assert_eq!(count, 30);
    Ok(())
}
