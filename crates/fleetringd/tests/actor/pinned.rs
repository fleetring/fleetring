//! FR-032 in the SQLite adapter: pinned snapshots that don't block writes, and the durable
//! checkpoint marker that refuses positions a restarted primary can't vouch for.

use std::fs;
use std::io::Read as _;
use std::path::Path;
use std::time::Duration;

use fleetring_wal::Position as WalPosition;
use fleetringd::DatabaseError;
use fleetringd::engine::{
    DatabaseEngine, EngineCapability, EngineError, Execution, FrameSourceEngine, PinnedSnapshot,
    Position, SqliteEngine, Transaction,
};

use crate::common::{Fixture, Kind, PAGE_SIZE, TestResult, config, name};

fn sqlite() -> Fixture {
    Fixture::new(
        Kind::Sqlite,
        config(Execution::Threads, 16, Duration::from_secs(3600)),
    )
}

/// Copies the pinned files into `dir`, as a transfer would.
fn copy(pinned: &PinnedSnapshot, dir: &Path) -> TestResult {
    fs::create_dir_all(dir)?;
    for file in pinned.files() {
        let mut content = vec![0; usize::try_from(file.len())?];
        (&mut file.file()).read_exact(&mut content)?;
        fs::write(dir.join(file.name()), content)?;
    }
    Ok(())
}

fn is_checkpoint_blocked(result: Result<impl std::fmt::Debug, DatabaseError>) -> bool {
    matches!(
        result,
        Err(DatabaseError::Engine(EngineError::CheckpointBlocked))
    )
}

#[tokio::test]
async fn a_pinned_snapshot_holds_off_checkpoints_but_not_writes() -> TestResult {
    let fx = sqlite();
    let db = name("pinned");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    for i in 0..20_i64 {
        handle
            .execute(Transaction::from(fx.put(&format!("before-{i}"), i)))
            .await?;
    }
    let marker = handle.checkpoint().await?;
    handle
        .execute(Transaction::from(fx.put("after-checkpoint", 0)))
        .await?;
    let pinned = handle.pin_snapshot().await?;
    assert_eq!(pinned.position(), handle.position().await?.as_ref());
    assert!(
        pinned
            .files()
            .iter()
            .any(|file| file.name() == "data.db-checkpoint"),
        "the snapshot carries the checkpoint marker"
    );

    for i in 0..20_i64 {
        handle
            .execute(Transaction::from(fx.put(&format!("during-{i}"), i)))
            .await?;
    }
    assert!(is_checkpoint_blocked(handle.checkpoint().await));
    let copied = fx.path("copy");
    copy(&pinned, &copied)?;
    pinned.confirm()?;
    let position = pinned.position().cloned();
    drop(pinned);

    // The copy opens as a follower at the pinned position and catches up from the log.
    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let EngineCapability::FrameSource(frames) = engine.capability() else {
        panic!("sqlite is a frame source");
    };
    let mut follower = frames.open_follower(&copied)?;
    assert_eq!(follower.position(), position);
    let rest = handle
        .committed_since(position)
        .await?
        .ok_or("writes during the pin")?;
    follower.apply(&rest)?;
    let marker_now = handle.checkpoint().await?;
    assert!(marker_now.position > marker.position);
    follower.checkpoint(&marker_now)?;
    drop(follower);
    assert!(
        fs::read(copied.join("data.db"))? == fs::read(fx.path("databases/pinned/data.db"))?,
        "the copy ends byte-identical to the primary"
    );
    Ok(())
}

#[tokio::test]
async fn a_snapshot_whose_log_restarts_under_it_is_refused() -> TestResult {
    let fx = sqlite();
    let db = name("restarting");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    handle.execute(Transaction::from(fx.put("a", 1))).await?;
    handle.checkpoint().await?;
    // The log is fully checkpointed but not restarted: the next write restarts it.
    let pinned = handle.pin_snapshot().await?;
    pinned.confirm()?;
    handle.execute(Transaction::from(fx.put("b", 2))).await?;
    let error = pinned
        .confirm()
        .expect_err("the log restarted under the snapshot");
    assert!(matches!(error, EngineError::Storage(_)), "{error:?}");
    Ok(())
}

#[tokio::test]
async fn a_reopened_primary_refuses_positions_behind_its_last_checkpoint() -> TestResult {
    let fx = sqlite();
    let db = name("marked");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    handle.execute(Transaction::from(fx.put("a", 1))).await?;
    let behind = handle.position().await?.ok_or("a position")?;
    handle.execute(Transaction::from(fx.put("b", 2))).await?;
    let marker = handle.checkpoint().await?;
    handle.execute(Transaction::from(fx.put("c", 3))).await?;
    drop(handle);
    fx.registry.close_all().await;
    assert!(fx.path("databases/marked/data.db-checkpoint").exists());

    let handle = fx.registry.open(&db).await?;
    let refused = handle.committed_since(Some(behind.clone())).await;
    assert!(
        matches!(
            &refused,
            Err(DatabaseError::Engine(EngineError::InvalidPosition { position })) if *position == behind
        ),
        "{refused:?}"
    );
    let next = handle
        .committed_since(Some(marker.position.clone()))
        .await?
        .ok_or("the new generation")?;
    assert!(
        next.start > marker.position,
        "the new generation continues from the marker"
    );
    Ok(())
}

/// The engine's follower operations.
fn frames(engine: &SqliteEngine) -> &dyn FrameSourceEngine {
    let EngineCapability::FrameSource(frames) = engine.capability() else {
        panic!("sqlite is a frame source");
    };
    frames
}

/// The rows of `kv` in the copy at `dir`, read from a scratch copy of it opened as a promoted
/// copy would be, so that SQLite's own checkpoint on close leaves `dir` as it was.
fn copied_rows(dir: &Path) -> Result<Vec<(String, i64)>, Box<dyn std::error::Error>> {
    let scratch = tempfile::tempdir()?;
    for name in ["data.db", "data.db-wal"] {
        if dir.join(name).exists() {
            fs::copy(dir.join(name), scratch.path().join(name))?;
        }
    }
    let conn = rusqlite::Connection::open(scratch.path().join("data.db"))?;
    let mut statement = conn.prepare("SELECT k, v FROM kv ORDER BY k")?;
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn pairs(rows: &[(&str, i64)]) -> Vec<(String, i64)> {
    rows.iter()
        .map(|(key, value)| ((*key).to_owned(), *value))
        .collect()
}

#[tokio::test]
async fn a_snapshot_pinned_at_an_earlier_commit_holds_the_log_up_to_it() -> TestResult {
    let fx = sqlite();
    let db = name("earlier");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    let mut ends = Vec::new();
    for (key, value) in [("a", 1), ("b", 2), ("c", 3)] {
        handle
            .execute(Transaction::from(fx.put(key, value)))
            .await?;
        ends.push(handle.position().await?.ok_or("a position")?);
    }
    let (p2, p3) = (ends[1].clone(), ends[2].clone());

    let pinned = handle.pin_snapshot_at(Some(p2.clone())).await?;
    assert_eq!(pinned.position(), Some(&p2));
    let copied = fx.path("at-p2");
    copy(&pinned, &copied)?;
    pinned.confirm()?;
    drop(pinned);

    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let follower = frames(&engine).open_follower(&copied)?;
    assert_eq!(follower.position(), Some(p2.clone()));
    drop(follower);
    assert_eq!(
        copied_rows(&copied)?,
        pairs(&[("a", 1), ("b", 2)]),
        "the copy holds the log up to p2 and nothing after it"
    );
    let mut follower = frames(&engine).open_follower(&copied)?;
    let rest = handle
        .committed_since(Some(p2))
        .await?
        .ok_or("the third write")?;
    assert_eq!(follower.apply(&rest)?, p3);
    drop(follower);
    assert_eq!(
        copied_rows(&copied)?,
        pairs(&[("a", 1), ("b", 2), ("c", 3)])
    );
    Ok(())
}

#[tokio::test]
async fn a_snapshot_pinned_at_the_log_beginning_is_the_main_file_alone() -> TestResult {
    let fx = sqlite();
    let db = name("beginning");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    handle.execute(Transaction::from(fx.put("a", 1))).await?;
    handle.checkpoint().await?;
    handle.execute(Transaction::from(fx.put("b", 2))).await?;

    let pinned = handle.pin_snapshot_at(None).await?;
    assert_eq!(pinned.position(), None);
    assert!(
        pinned
            .files()
            .iter()
            .all(|file| file.name() != "data.db-wal"),
        "no WAL: {:?}",
        pinned.files()
    );
    let copied = fx.path("at-beginning");
    copy(&pinned, &copied)?;
    pinned.confirm()?;
    drop(pinned);

    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let mut follower = frames(&engine).open_follower(&copied)?;
    assert_eq!(follower.position(), None);
    let log = handle
        .committed_since(None)
        .await?
        .ok_or("the current log")?;
    follower.apply(&log)?;
    assert_eq!(follower.position(), handle.position().await?);
    drop(follower);
    assert_eq!(copied_rows(&copied)?, pairs(&[("a", 1), ("b", 2)]));
    Ok(())
}

#[tokio::test]
async fn pinning_off_a_commit_boundary_or_before_the_checkpoint_is_refused() -> TestResult {
    let fx = sqlite();
    let db = name("refused");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    handle.execute(Transaction::from(fx.put("a", 1))).await?;
    let before = handle.position().await?.ok_or("a position")?;
    // A transaction of many frames, so that a frame boundary inside it isn't a commit.
    handle
        .execute(Transaction::new(
            (0..20).map(|i| fx.put(&format!("big-{i}"), vec![7_u8; 900])),
        ))
        .await?;
    let decoded = WalPosition::from_bytes(before.as_bytes())?;
    let frame = u64::from(PAGE_SIZE) + 24;
    let inside =
        Position::new(WalPosition::new(decoded.generation(), decoded.offset() + frame).to_bytes());
    let refused = handle.pin_snapshot_at(Some(inside.clone())).await;
    assert!(
        matches!(
            &refused,
            Err(DatabaseError::Engine(EngineError::InvalidPosition { position })) if *position == inside
        ),
        "{refused:?}"
    );

    let marker = handle.checkpoint().await?;
    handle.execute(Transaction::from(fx.put("b", 2))).await?;
    let refused = handle.pin_snapshot_at(Some(before.clone())).await;
    assert!(
        matches!(
            &refused,
            Err(DatabaseError::Engine(EngineError::InvalidPosition { position })) if *position == before
        ),
        "{refused:?}"
    );
    // Where the checkpoint ended is where the current log continues from, so it can be pinned.
    let pinned = handle
        .pin_snapshot_at(Some(marker.position.clone()))
        .await?;
    assert_eq!(pinned.position(), Some(&marker.position));
    Ok(())
}
