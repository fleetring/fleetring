//! Restore: to the latest point, to a position, and to a time, each holding exactly the prefix
//! it names and verified against the chain; altered or missing objects refused as data loss;
//! and an offline restore that leaves a plain SQLite database.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::durability::{
    ArchiveKeys, Listing, MemoryStore, ObjectEntry, ObjectStore, PlanError, RestoreError,
    RestoreTarget, Restored, StoreError, StoreFuture, decode_segment, encode_segment,
    finish_offline, restore_into,
};
use fleetringd::engine::{Execution, Position};
use tokio::sync::watch;

use crate::common::{Node, TestResult, insert, name, settings, sqlite};

/// The values of `t` in the copy at `dir`, read from a scratch copy of its files so that SQLite's
/// own checkpoint on close leaves `dir` as it was, with `PRAGMA integrity_check`'s answer.
fn rows(dir: &Path) -> Result<(Vec<i64>, String), Box<dyn std::error::Error>> {
    let scratch = tempfile::tempdir()?;
    for file in ["data.db", "data.db-wal"] {
        if dir.join(file).exists() {
            std::fs::copy(dir.join(file), scratch.path().join(file))?;
        }
    }
    let conn = rusqlite::Connection::open(scratch.path().join("data.db"))?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    let mut statement = conn.prepare("SELECT value FROM t ORDER BY rowid")?;
    let values = statement
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<i64>, _>>()?;
    Ok((values, integrity))
}

async fn restore(
    node: &Node,
    database: &DatabaseName,
    target: &RestoreTarget,
    staging: &Path,
) -> Result<Restored, RestoreError> {
    restore_into(
        node.archive.store().as_ref(),
        &node.archive.keys(database),
        database,
        Arc::new(sqlite()),
        target,
        staging,
        Execution::Threads,
    )
    .await
}

async fn create_ledger(node: &Node) -> Result<DatabaseName, Box<dyn std::error::Error>> {
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    node.await_archived().await?;
    Ok(ledger)
}

#[tokio::test(flavor = "multi_thread")]
async fn restoring_latest_reproduces_the_primary() -> TestResult {
    let dir = tempfile::tempdir()?;
    let node = Node::start(
        &dir.path().join("node"),
        Arc::new(MemoryStore::new()),
        settings(),
    );
    let ledger = create_ledger(&node).await?;
    for value in 1..=30 {
        node.execute(&ledger, insert(value)).await?;
        if value % 7 == 0 {
            node.await_archived().await?;
        }
    }
    node.await_archived().await?;

    let staging = dir.path().join("restored");
    let restored = restore(&node, &ledger, &RestoreTarget::Latest, &staging).await?;
    let position = node.position(&ledger).await?;
    assert_eq!(restored.position, position);
    let verified = node.replication.verify(&ledger, position).await?;
    assert_eq!(
        restored.chain, verified.hash,
        "the chain the primary reports"
    );
    assert!(restored.segments > 0);
    let (values, integrity) = rows(&staging)?;
    assert_eq!(values, (1..=30).collect::<Vec<_>>());
    assert_eq!(integrity, "ok");

    // A staging directory that exists is refused, and left alone.
    let refused = restore(&node, &ledger, &RestoreTarget::Latest, &staging).await;
    assert!(matches!(refused, Err(RestoreError::Io(_))), "{refused:?}");
    assert!(staging.join("data.db").exists());
    node.stop().await;
    Ok(())
}

/// A store whose puts wait while it is held.
#[derive(Debug, Clone)]
struct GatedStore {
    inner: MemoryStore,
    open: watch::Sender<bool>,
}

impl GatedStore {
    fn new(inner: MemoryStore) -> Self {
        Self {
            inner,
            open: watch::Sender::new(true),
        }
    }

    fn hold(&self) {
        self.open.send_replace(false);
    }

    fn release(&self) {
        self.open.send_replace(true);
    }
}

impl ObjectStore for GatedStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> StoreFuture<'_, ()> {
        let key = key.to_owned();
        Box::pin(async move {
            let mut open = self.open.subscribe();
            open.wait_for(|open| *open)
                .await
                .map_err(|error| StoreError::Unavailable {
                    reason: error.to_string(),
                })?;
            self.inner.put(&key, bytes).await
        })
    }

    fn get(&self, key: &str) -> StoreFuture<'_, Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn list(&self, prefix: &str) -> StoreFuture<'_, Vec<ObjectEntry>> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> StoreFuture<'_, ()> {
        self.inner.delete(key)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn restoring_to_a_position_holds_exactly_the_writes_before_it() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = GatedStore::new(MemoryStore::new());
    let node = Node::start(
        &dir.path().join("node"),
        Arc::new(store.clone()),
        settings(),
    );
    let ledger = create_ledger(&node).await?;
    // The first write's segment waits in the store while the rest are written, so that those
    // reach the archive together. However the pump's reads fall, ten writes in at most two
    // deliveries leave some write ending strictly inside a segment.
    store.hold();
    let mut ends: Vec<Position> = Vec::new();
    for value in 1..=10 {
        node.execute(&ledger, insert(value)).await?;
        ends.push(node.position(&ledger).await?.ok_or("a position")?);
    }
    store.release();
    node.await_archived().await?;
    let listing = Listing::read(&store, &node.archive.keys(&ledger), &ledger).await?;
    let inside = |position: &Position| {
        listing.segments.iter().any(|segment| {
            segment.name.start.as_ref() < Some(position) && *position < segment.name.end
        })
    };
    // The cut: the first write after the first that ends strictly inside a listed segment.
    let cut = (1..ends.len())
        .find(|index| inside(&ends[*index]))
        .ok_or_else(|| format!("no write ends inside a segment: {listing:#?}"))?;
    let count = i64::try_from(cut)? + 1;

    for (count, end) in [(count, &ends[cut]), (1, &ends[0]), (10, &ends[9])] {
        let staging = dir.path().join(format!("at-{count}"));
        let restored = restore(
            &node,
            &ledger,
            &RestoreTarget::Position(end.clone()),
            &staging,
        )
        .await?;
        assert_eq!(restored.position.as_ref(), Some(end));
        let verified = node.replication.verify(&ledger, Some(end.clone())).await?;
        assert_eq!(restored.chain, verified.hash, "the chain at write {count}");
        let (values, integrity) = rows(&staging)?;
        assert_eq!(
            values,
            (1..=count).collect::<Vec<_>>(),
            "restored to write {count}"
        );
        assert_eq!(integrity, "ok");
    }

    // A position inside a transaction is refused with the boundaries around it.
    let cut_at = fleetring_wal::Position::from_bytes(ends[cut].as_bytes())?;
    let inside_one = fleetring_wal::Position::new(cut_at.generation(), cut_at.offset() - 8);
    let refused = restore(
        &node,
        &ledger,
        &RestoreTarget::Position(Position::new(inside_one.to_bytes())),
        &dir.path().join("refused"),
    )
    .await;
    assert!(
        matches!(&refused, Err(RestoreError::NotBoundary { before: Some(before), after: Some(after), .. }) if *before == ends[cut - 1] && *after == ends[cut]),
        "{refused:?}"
    );
    assert!(
        !dir.path().join("refused").exists(),
        "nothing is left behind"
    );
    // A position past everything archived is unreachable.
    let past = Position::new([0xff; 20]);
    let unreachable = restore(
        &node,
        &ledger,
        &RestoreTarget::Position(past),
        &dir.path().join("past"),
    )
    .await;
    assert!(
        matches!(&unreachable, Err(RestoreError::Plan(PlanError::Unreachable { furthest: Some(furthest) })) if *furthest == ends[9]),
        "{unreachable:?}"
    );
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn restoring_to_a_time_holds_no_segment_archived_after_it() -> TestResult {
    let dir = tempfile::tempdir()?;
    let node = Node::start(
        &dir.path().join("node"),
        Arc::new(MemoryStore::new()),
        settings(),
    );
    let ledger = create_ledger(&node).await?;
    for value in 1..=5 {
        node.execute(&ledger, insert(value)).await?;
    }
    node.await_archived().await?;
    let first_part = node.position(&ledger).await?;
    let time = SystemClock.now().as_millis();
    tokio::time::sleep(Duration::from_millis(20)).await;
    for value in 6..=10 {
        node.execute(&ledger, insert(value)).await?;
    }
    node.await_archived().await?;

    let staging = dir.path().join("at-time");
    let restored = restore(&node, &ledger, &RestoreTarget::Time(time), &staging).await?;
    assert_eq!(restored.position, first_part);
    assert!(restored.archived_at_ms.is_none_or(|at| at <= time));
    let (values, _) = rows(&staging)?;
    assert_eq!(values, (1..=5).collect::<Vec<_>>());
    let listing = node.listing(&ledger).await?;
    assert!(
        listing
            .segments
            .iter()
            .filter(|segment| Some(&segment.name.end) > first_part.as_ref())
            .all(|segment| segment.name.archived_at_ms > time),
        "every segment it left out was archived after the time"
    );
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_past_the_new_primarys_mark_leaves_later_points_restorable() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    let root = dir.path().join("node");
    let mut settings = settings();
    // A snapshot after every segment, and two kept: retention runs after every delivery.
    settings.snapshot_after_bytes = 1;
    settings.keep_snapshots = std::num::NonZeroU32::new(2).ok_or("nonzero")?;

    // The old primary archives three writes, each followed by a snapshot at its end. The mark
    // of the last delivery is staged but never written, since no write follows it.
    let node = Node::start(&root, Arc::new(store.clone()), settings);
    let ledger = create_ledger(&node).await?;
    for value in 1..=3 {
        node.execute(&ledger, insert(value)).await?;
        node.await_archived().await?;
    }
    let dead_end = node.position(&ledger).await?;
    node.stop().await;

    // The new primary takes two writes before its stream starts, so its first delivery runs
    // from the replicated mark, before the old primary's last snapshot, past that snapshot.
    let mut node = Node::without_streams(&root, Arc::new(store.clone()), settings);
    node.serve(&ledger, 2)?;
    node.execute(&ledger, insert(4)).await?;
    let fourth = node.position(&ledger).await?.ok_or("a position")?;
    node.execute(&ledger, insert(5)).await?;
    node.start_streams();
    node.await_archived().await?;

    let listing = node.listing(&ledger).await?;
    assert!(
        listing
            .snapshots
            .iter()
            .any(|snapshot| snapshot.position == dead_end),
        "the old primary's last snapshot is kept: {listing:#?}"
    );
    assert!(
        !listing
            .segments
            .iter()
            .any(|segment| segment.name.start == dead_end),
        "and leads nowhere: {listing:#?}"
    );
    // The fourth write lies between the dead end and the new primary's snapshot: only the
    // snapshot before the mark, and the new primary's segment from it, reach it.
    let staging = dir.path().join("fourth");
    let restored = restore(
        &node,
        &ledger,
        &RestoreTarget::Position(fourth.clone()),
        &staging,
    )
    .await?;
    assert_eq!(restored.position, Some(fourth));
    let (values, integrity) = rows(&staging)?;
    assert_eq!(values, [1, 2, 3, 4]);
    assert_eq!(integrity, "ok");
    node.stop().await;
    Ok(())
}

/// An archive of `ledger` with some segments, on a node that is stopped.
async fn archived()
-> Result<(tempfile::TempDir, MemoryStore, DatabaseName), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    let node = Node::start(
        &dir.path().join("node"),
        Arc::new(store.clone()),
        settings(),
    );
    let ledger = create_ledger(&node).await?;
    for value in 1..=6 {
        node.execute(&ledger, insert(value)).await?;
        node.await_archived().await?;
    }
    node.stop().await;
    Ok((dir, store, ledger))
}

async fn restore_from(
    store: &MemoryStore,
    ledger: &DatabaseName,
    staging: &Path,
) -> Result<Restored, RestoreError> {
    restore_into(
        store,
        &ArchiveKeys::new("fleetring", ledger),
        ledger,
        Arc::new(sqlite()),
        &RestoreTarget::Latest,
        staging,
        Execution::Threads,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tampered_segment_is_refused_as_data_loss() -> TestResult {
    let (dir, store, ledger) = archived().await?;
    let listing = Listing::read(&store, &ArchiveKeys::new("fleetring", &ledger), &ledger).await?;
    let segment = listing.segments.last().ok_or("a segment")?.key.clone();
    let original = store.get(&segment).await?.ok_or("the segment")?;

    // A flipped byte fails the segment's checksum.
    let mut flipped = original.clone();
    let middle = flipped.len() / 2;
    flipped[middle] ^= 0x01;
    store.put(&segment, flipped).await?;
    let staging = dir.path().join("restored");
    let refused = restore_from(&store, &ledger, &staging).await;
    assert!(
        matches!(&refused, Err(RestoreError::DataLoss { object, .. }) if *object == segment),
        "{refused:?}"
    );
    assert!(!staging.exists(), "nothing is installed");

    // A changed payload with a recomputed checksum fails the chain.
    let (header, payload) = decode_segment(&original)?;
    let mut payload = payload.to_vec();
    let last = payload.len() - 1;
    payload[last] ^= 0x01;
    store
        .put(&segment, encode_segment(&header, &payload))
        .await?;
    let refused = restore_from(&store, &ledger, &staging).await;
    assert!(
        matches!(&refused, Err(RestoreError::DataLoss { object, reason }) if *object == segment && reason.contains("hashes to")),
        "{refused:?}"
    );
    assert!(!staging.exists());

    store.put(&segment, original).await?;

    // A missing segment in the middle is a gap, which no target past it crosses.
    let middle = &listing.segments[listing.segments.len() / 2];
    let kept = store.get(&middle.key).await?.ok_or("the segment")?;
    store.delete(&middle.key).await?;
    let refused = restore_from(&store, &ledger, &staging).await;
    assert!(
        matches!(&refused, Err(RestoreError::Plan(PlanError::Unreachable { furthest })) if furthest.as_ref() == middle.name.start.as_ref()),
        "{refused:?}"
    );
    assert!(!staging.exists());
    store.put(&middle.key, kept).await?;
    restore_from(&store, &ledger, &staging).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_snapshot_chunk_is_refused_as_data_loss() -> TestResult {
    let (dir, store, ledger) = archived().await?;
    let listing = Listing::read(&store, &ArchiveKeys::new("fleetring", &ledger), &ledger).await?;
    let chunk = listing.snapshots[0]
        .chunks
        .first()
        .ok_or("a chunk")?
        .clone();
    let original = store.get(&chunk).await?.ok_or("the chunk")?;
    let staging = dir.path().join("restored");

    let mut altered = original.clone();
    altered[0] ^= 0x01;
    store.put(&chunk, altered).await?;
    let refused = restore_from(&store, &ledger, &staging).await;
    assert!(
        matches!(&refused, Err(RestoreError::DataLoss { object, .. }) if *object == chunk),
        "{refused:?}"
    );
    assert!(!staging.exists());

    store.delete(&chunk).await?;
    let refused = restore_from(&store, &ledger, &staging).await;
    assert!(
        matches!(&refused, Err(RestoreError::DataLoss { object, reason }) if *object == chunk && reason.contains("missing")),
        "{refused:?}"
    );
    assert!(!staging.exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_restore_writes_a_plain_sqlite_database() -> TestResult {
    let (dir, store, ledger) = archived().await?;
    let staging = dir.path().join("restored");
    restore_from(&store, &ledger, &staging).await?;
    finish_offline(&sqlite(), &staging)?;
    // The main file alone holds every row: a disaster recovery needs nothing else.
    let alone = dir.path().join("alone");
    std::fs::create_dir(&alone)?;
    std::fs::copy(staging.join("data.db"), alone.join("data.db"))?;
    let conn = rusqlite::Connection::open(alone.join("data.db"))?;
    let values = conn
        .prepare("SELECT value FROM t ORDER BY rowid")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<i64>, _>>()?;
    assert_eq!(values, (1..=6).collect::<Vec<_>>());
    Ok(())
}
