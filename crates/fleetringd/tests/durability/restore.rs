//! Restore: to the latest point, to a position, and to a time, each holding exactly the prefix
//! it names and verified against the chain; altered or missing objects refused as data loss;
//! and an offline restore that leaves a plain SQLite database.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::durability::{
    ArchiveKeys, Listing, MAX_METADATA_BYTES, MemoryStore, ObjectEntry, ObjectStore, PlanError,
    RestoreError, RestoreTarget, Restored, StoreError, StoreFuture, checksum_hex, decode_segment,
    encode_descriptor, encode_segment, finish_offline, restore_into,
};
use fleetringd::engine::{EngineKind, Execution, Position, SqliteEngine};
use tokio::sync::watch;

use crate::common::{
    Node, PAGE_SIZE, TestMetadata, TestResult, archived_metadata, insert, name, settings, sqlite,
};

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
    let listing = Listing::read(
        &store,
        &node.archive.keys(&ledger),
        &ledger,
        EngineKind::Sqlite,
    )
    .await?;
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
    let listing = Listing::read(
        &store,
        &ArchiveKeys::new("fleetring", &ledger),
        &ledger,
        EngineKind::Sqlite,
    )
    .await?;
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
async fn a_snapshot_another_engine_took_is_never_used() -> TestResult {
    let (dir, store, ledger) = archived().await?;
    let keys = ArchiveKeys::new("fleetring", &ledger);
    let listing = Listing::read(&store, &keys, &ledger, EngineKind::Sqlite).await?;
    let snapshot = listing.snapshots.first().ok_or("a snapshot")?;
    let mut descriptor = snapshot.descriptor.clone();
    descriptor.engine = "memory".to_owned();
    store
        .put(&snapshot.key, encode_descriptor(&descriptor))
        .await?;

    let listing = Listing::read(&store, &keys, &ledger, EngineKind::Sqlite).await?;
    assert!(listing.snapshots.is_empty(), "{listing:#?}");
    assert!(
        listing
            .damaged
            .iter()
            .any(|damaged| damaged.key == snapshot.key && damaged.reason.contains("memory")),
        "{listing:#?}"
    );
    let staging = dir.path().join("restored");
    let refused = restore_from(&store, &ledger, &staging).await;
    assert!(
        matches!(&refused, Err(RestoreError::Plan(PlanError::NoSnapshot))),
        "{refused:?}"
    );
    assert!(!staging.exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_snapshot_chunk_is_refused_as_data_loss() -> TestResult {
    let (dir, store, ledger) = archived().await?;
    let listing = Listing::read(
        &store,
        &ArchiveKeys::new("fleetring", &ledger),
        &ledger,
        EngineKind::Sqlite,
    )
    .await?;
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

/// Sets the page size in the packed snapshot's `data.db` header to twice what it was, leaving
/// the WAL's alone. The stream is each file as a big-endian `u16` name length, the name, a
/// big-endian `u64` content length, and the content.
fn double_database_page_size(stream: &mut [u8]) -> Option<()> {
    let mut at = 0;
    while at < stream.len() {
        let name_len = usize::from(u16::from_be_bytes(stream.get(at..at + 2)?.try_into().ok()?));
        let is_database = stream.get(at + 2..at + 2 + name_len)? == b"data.db";
        at += 2 + name_len;
        let len =
            usize::try_from(u64::from_be_bytes(stream.get(at..at + 8)?.try_into().ok()?)).ok()?;
        at += 8;
        if is_database {
            let field = stream.get_mut(at + 16..at + 18)?;
            let size = u16::from_be_bytes([field[0], field[1]]);
            field.copy_from_slice(&size.checked_mul(2)?.to_be_bytes());
            return Some(());
        }
        at += len;
    }
    None
}

/// Rewrites the archived snapshot so its database file's header names twice the page size its
/// WAL does, with every checksum recomputed: the damage checksums can't see, as whoever can write
/// the bucket would leave it. Returns the snapshot's key.
async fn forge_page_size_mismatch(
    store: &MemoryStore,
    ledger: &DatabaseName,
) -> Result<String, Box<dyn std::error::Error>> {
    let keys = ArchiveKeys::new("fleetring", ledger);
    let listing = Listing::read(store, &keys, ledger, EngineKind::Sqlite).await?;
    let snapshot = listing.snapshots.first().ok_or("a snapshot")?;
    let mut chunks = Vec::new();
    for key in &snapshot.chunks {
        chunks.push(store.get(key).await?.ok_or("a chunk")?);
    }
    let mut stream = chunks.concat();
    double_database_page_size(&mut stream).ok_or("a database file in the snapshot")?;
    let mut descriptor = snapshot.descriptor.clone();
    descriptor.checksum = checksum_hex(&stream);
    let mut rest = stream.as_slice();
    for (index, (key, old)) in snapshot.chunks.iter().zip(&chunks).enumerate() {
        let (chunk, tail) = rest.split_at(old.len());
        rest = tail;
        descriptor.chunk_checksums[index] = checksum_hex(chunk);
        store.put(key, chunk.to_vec()).await?;
    }
    store
        .put(&snapshot.key, encode_descriptor(&descriptor))
        .await?;
    Ok(snapshot.key.clone())
}

/// An archive whose snapshot holds a database file of one page size beside a WAL of another is
/// refused before anything opens the copy, and nothing is left behind (#154).
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_whose_wal_and_database_disagree_is_refused_before_anything_opens_it()
-> TestResult {
    let (dir, store, ledger) = archived().await?;
    let snapshot = forge_page_size_mismatch(&store, &ledger).await?;
    let staging = dir.path().join("restored");
    let refused = restore_from(&store, &ledger, &staging).await;
    assert!(
        matches!(
            &refused,
            Err(RestoreError::DataLoss { object, reason })
                if *object == snapshot && reason.contains("the archive's snapshot is refused")
                    && reason.contains("page size")
        ),
        "{refused:?}"
    );
    assert!(!staging.exists(), "nothing is left behind");
    Ok(())
}

/// The configured `[engine] page_size` is for new databases: an engine configured for another
/// page size than the archive's still restores it (#154).
#[tokio::test(flavor = "multi_thread")]
async fn an_engine_configured_for_another_page_size_still_restores_an_archive() -> TestResult {
    let (dir, store, ledger) = archived().await?;
    let staging = dir.path().join("restored");
    restore_into(
        &store,
        &ArchiveKeys::new("fleetring", &ledger),
        &ledger,
        Arc::new(SqliteEngine::new(PAGE_SIZE * 2)?),
        &RestoreTarget::Latest,
        &staging,
        Execution::Threads,
    )
    .await?;
    let (values, integrity) = rows(&staging)?;
    assert_eq!(values, [1, 2, 3, 4, 5, 6]);
    assert_eq!(integrity, "ok");
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

/// A node archiving `ledger` with metadata to `store`, which holds the values `1..=3` once
/// archived.
async fn archived_with_metadata(
    dir: &Path,
    store: &MemoryStore,
) -> Result<(Node, DatabaseName), Box<dyn std::error::Error>> {
    let ledger = name("ledger");
    let metadata = TestMetadata::new();
    metadata.set(&ledger, archived_metadata("ledger"));
    let node = Node::start_with_metadata(
        &dir.join("node"),
        Arc::new(store.clone()),
        settings(),
        metadata,
    );
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    for value in 1..=3 {
        node.execute(&ledger, insert(value)).await?;
    }
    node.await_archived().await?;
    Ok((node, ledger))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restore_returns_the_archived_metadata_as_the_archives_claim() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    let (node, ledger) = archived_with_metadata(dir.path(), &store).await?;

    let staging = dir.path().join("restored");
    let restored = restore(&node, &ledger, &RestoreTarget::Latest, &staging).await?;
    // Everything the object said comes back, the author, the creator, and the expiry included:
    // they are the archive's claims, and the caller decides what to do with each. The daemon's
    // client sets the description without an author and never copies the expiry.
    assert_eq!(restored.metadata, Some(archived_metadata("ledger")));
    assert_eq!(restored.metadata_skipped, None);
    assert_eq!(rows(&staging)?.0, [1, 2, 3]);
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_archive_without_metadata_restores_exactly_as_before() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    // An archive a release without metadata wrote: no `.metadata/` objects at all.
    let node = Node::start(
        &dir.path().join("node"),
        Arc::new(store.clone()),
        settings(),
    );
    let ledger = create_ledger(&node).await?;
    for value in 1..=3 {
        node.execute(&ledger, insert(value)).await?;
    }
    node.await_archived().await?;
    let keys = store.list("").await?;
    assert!(
        keys.iter().all(|object| !object.key.contains(".metadata")),
        "{keys:#?}"
    );

    let staging = dir.path().join("restored");
    let restored = restore(&node, &ledger, &RestoreTarget::Latest, &staging).await?;
    assert_eq!(restored.metadata, None);
    assert_eq!(restored.metadata_skipped, None);
    assert_eq!(rows(&staging)?.0, [1, 2, 3]);
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_metadata_object_that_fails_its_checksum_is_ignored() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    let (node, ledger) = archived_with_metadata(dir.path(), &store).await?;
    let listing = node.listing(&ledger).await?;
    let entry = listing.snapshots[0].metadata.clone().ok_or("an object")?;

    // Flip a byte, keeping the name: the data restores, the metadata is skipped and reported.
    let mut bytes = store.get(&entry.key).await?.ok_or("the object")?;
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    store.put(&entry.key, bytes).await?;
    let staging = dir.path().join("restored");
    let restored = restore(&node, &ledger, &RestoreTarget::Latest, &staging).await?;
    assert_eq!(restored.metadata, None);
    let reason = restored.metadata_skipped.ok_or("a reason")?;
    assert!(reason.contains("checksum"), "{reason}");
    assert_eq!(rows(&staging)?.0, [1, 2, 3], "the data restored");

    // A truncated object fails the same way.
    store.put(&entry.key, vec![b'{']).await?;
    let staging = dir.path().join("restored-again");
    let restored = restore(&node, &ledger, &RestoreTarget::Latest, &staging).await?;
    assert_eq!(restored.metadata, None);
    assert!(restored.metadata_skipped.is_some());
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_that_checks_out_but_cant_be_used_never_fails_a_data_restore() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    let (node, ledger) = archived_with_metadata(dir.path(), &store).await?;
    let keys = node.archive.keys(&ledger);
    let listing = node.listing(&ledger).await?;
    let snapshot = &listing.snapshots[0];
    let old = snapshot.metadata.clone().ok_or("an object")?;

    let genuine = serde_json::to_value(archived_metadata("ledger"))?;
    let with = |field: &str, value: serde_json::Value| {
        let mut object = genuine.clone();
        object[field] = value;
        object.to_string().into_bytes()
    };
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("not JSON", b"{ not json".to_vec()),
        ("an unknown field", with("surprise", serde_json::json!(1))),
        ("a newer format", with("format", serde_json::json!(2))),
        (
            "another database's",
            with("database", serde_json::json!("other")),
        ),
        (
            "an oversized description",
            with("description", serde_json::json!("x".repeat(2048))),
        ),
        (
            "too many labels",
            with(
                "labels",
                serde_json::Value::Object(
                    (0..40)
                        .map(|n| (format!("k{n}"), serde_json::json!("v")))
                        .collect(),
                ),
            ),
        ),
        ("an oversized object", vec![b' '; MAX_METADATA_BYTES + 1]),
    ];
    store.delete(&old.key).await?;
    for (index, (what, bytes)) in cases.into_iter().enumerate() {
        // The name carries the bytes' own checksum, so only the content is at fault.
        let key = keys.metadata(&snapshot.id, &checksum_hex(&bytes));
        store.put(&key, bytes).await?;
        let staging = dir.path().join(format!("restored-{index}"));
        let restored = restore(&node, &ledger, &RestoreTarget::Latest, &staging).await?;
        assert_eq!(restored.metadata, None, "{what}");
        assert!(restored.metadata_skipped.is_some(), "{what}");
        assert_eq!(rows(&staging)?.0, [1, 2, 3], "{what}: the data restored");
        store.delete(&key).await?;
    }

    // A snapshot whose object is gone restores without metadata, and nothing was skipped.
    let staging = dir.path().join("restored-gone");
    let restored = restore(&node, &ledger, &RestoreTarget::Latest, &staging).await?;
    assert_eq!((restored.metadata, restored.metadata_skipped), (None, None));
    assert_eq!(rows(&staging)?.0, [1, 2, 3]);
    node.stop().await;
    Ok(())
}
