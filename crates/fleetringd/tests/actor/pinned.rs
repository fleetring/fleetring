//! FR-032 in the SQLite adapter: pinned snapshots that don't block writes, and the durable
//! checkpoint marker that refuses positions a restarted primary can't vouch for.

use std::fs;
use std::io::Read as _;
use std::path::Path;
use std::time::Duration;

use fleetringd::DatabaseError;
use fleetringd::engine::{
    DatabaseEngine, EngineCapability, EngineError, Execution, PinnedSnapshot, SqliteEngine,
    Transaction,
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
