//! #154 in the SQLite adapter: a copy or batch whose page size disagrees with its database
//! file's, or a copy with the configured one's, is refused before anything opens it.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use fleetringd::engine::{
    DatabaseCapability, DatabaseEngine, EngineCapability, EngineError, FrameSourceEngine,
    Interrupt, OpenMode, SqliteEngine, Statement, Transaction,
};

use crate::common::{PAGE_SIZE, TestResult};

fn engine(page_size: u32) -> SqliteEngine {
    SqliteEngine::new(page_size).expect("a valid page size")
}

fn frames(engine: &SqliteEngine) -> &dyn FrameSourceEngine {
    match engine.capability() {
        EngineCapability::FrameSource(frames) => frames,
        _ => unreachable!("SQLite is a frame source"),
    }
}

/// A database at `dir` with `page_size`-byte pages and a few committed writes in its WAL.
fn primary(dir: &Path, page_size: u32) -> TestResult {
    let mut db = engine(page_size).open(dir, OpenMode::Create)?;
    for sql in [
        "CREATE TABLE t (v)",
        "INSERT INTO t VALUES (randomblob(3000))",
        "INSERT INTO t VALUES (randomblob(3000))",
    ] {
        db.execute(&Transaction::from(Statement::new(sql)), &Interrupt::new())?;
    }
    db.close()?;
    Ok(())
}

/// The files at `dir`, by name, to show nothing touched them.
fn files(dir: &Path) -> Result<BTreeMap<String, Vec<u8>>, Box<dyn std::error::Error>> {
    let mut files = BTreeMap::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        files.insert(
            entry.file_name().to_string_lossy().into_owned(),
            fs::read(entry.path())?,
        );
    }
    Ok(files)
}

fn unsafe_reason<T>(result: Result<T, EngineError>) -> Result<String, Box<dyn std::error::Error>> {
    match result {
        Err(EngineError::UnsafeCopy { reason }) => Ok(reason),
        Err(other) => Err(format!("expected an unsafe copy, got {other:?}").into()),
        Ok(_) => Err("expected an unsafe copy, got success".into()),
    }
}

#[test]
fn a_copy_whose_wal_has_another_page_size_is_refused_and_never_opened() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (small, large) = (dir.path().join("small"), dir.path().join("large"));
    primary(&small, PAGE_SIZE)?;
    primary(&large, PAGE_SIZE * 2)?;
    // The database file of one, beside the WAL of the other.
    let copy = dir.path().join("copy");
    fs::create_dir_all(&copy)?;
    fs::copy(large.join("data.db"), copy.join("data.db"))?;
    fs::copy(small.join("data.db-wal"), copy.join("data.db-wal"))?;
    let before = files(&copy)?;

    let large_engine = engine(PAGE_SIZE * 2);
    let reason = unsafe_reason(frames(&large_engine).check_copy(&copy))?;
    assert_eq!(
        reason,
        format!(
            "the WAL's page size is {PAGE_SIZE} bytes, but the database file's page size is {}",
            PAGE_SIZE * 2
        )
    );
    // Nor does anything else open it: as a follower's copy, or as a primary's database.
    unsafe_reason(frames(&large_engine).open_follower(&copy))?;
    unsafe_reason(large_engine.open(&copy, OpenMode::Existing))?;
    assert_eq!(files(&copy)?, before, "nothing touched the copy");
    Ok(())
}

#[test]
fn a_copy_of_another_page_size_than_configured_is_refused_but_a_local_database_opens() -> TestResult
{
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("source");
    primary(&source, PAGE_SIZE)?;
    let other = engine(PAGE_SIZE * 2);
    let reason = unsafe_reason(frames(&other).check_copy(&source))?;
    assert_eq!(
        reason,
        format!(
            "the configured [engine] page_size is {} bytes, but the database file's page size \
             is {PAGE_SIZE}",
            PAGE_SIZE * 2
        )
    );
    // The engine that made it takes it as a copy.
    frames(&engine(PAGE_SIZE)).check_copy(&source)?;
    // The configured page size is for new databases: an existing one opens whatever it says.
    other.open(&source, OpenMode::Existing)?.close()?;
    Ok(())
}

#[test]
fn a_new_databases_copy_with_no_wal_passes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("source");
    let mut db = engine(PAGE_SIZE).open(&source, OpenMode::Create)?;
    let copy = dir.path().join("copy");
    db.snapshot(&copy)?;
    frames(&engine(PAGE_SIZE)).check_copy(&copy)?;
    frames(&engine(PAGE_SIZE)).open_follower(&copy)?;
    Ok(())
}

#[test]
fn a_wal_batch_of_another_page_size_is_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let small_engine = engine(PAGE_SIZE);
    let mut small = small_engine.open(&dir.path().join("small"), OpenMode::Create)?;
    small.execute(
        &Transaction::from(Statement::new("CREATE TABLE t (v)")),
        &Interrupt::new(),
    )?;
    let follower_dir = dir.path().join("follower");
    small.snapshot(&follower_dir)?;
    let mut follower = frames(&small_engine).open_follower(&follower_dir)?;
    let at = follower.position();

    // Every frame of another primary, whose pages are twice the size.
    let large_dir = dir.path().join("large");
    primary(&large_dir, PAGE_SIZE * 2)?;
    let mut large = engine(PAGE_SIZE * 2).open(&large_dir, OpenMode::Existing)?;
    let DatabaseCapability::FrameSource(large_frames) = large.capability() else {
        return Err("SQLite is a frame source".into());
    };
    let foreign = large_frames
        .committed_since(None)?
        .ok_or("the large primary's log")?;
    match follower.apply(&foreign) {
        Err(EngineError::InvalidPayload(error)) => assert_eq!(
            error.to_string(),
            format!(
                "the WAL batch's page size is {} bytes, but the database file's page size is \
                 {PAGE_SIZE}",
                PAGE_SIZE * 2
            )
        ),
        other => return Err(format!("expected the batch to be refused, got {other:?}").into()),
    }
    assert_eq!(follower.position(), at, "nothing was mirrored");

    // The copy still takes its own primary's batches.
    small.execute(
        &Transaction::from(Statement::new("INSERT INTO t VALUES (1)")),
        &Interrupt::new(),
    )?;
    let DatabaseCapability::FrameSource(small_frames) = small.capability() else {
        return Err("SQLite is a frame source".into());
    };
    let own = small_frames
        .committed_since(at.as_ref())?
        .ok_or("the write")?;
    assert_eq!(follower.apply(&own)?, own.end);
    Ok(())
}
