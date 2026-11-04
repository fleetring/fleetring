//! The archive sink: a base snapshot, then one segment per delivery, chained without gaps;
//! snapshots when due, with retention; a failing store that delays the archive and never a
//! write; and the configuration's rules.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use fleetring_control::DatabaseName;
use fleetringd::Config;
use fleetringd::clock::SystemClock;
use fleetringd::durability::{
    Archive, ArchiveConfig, ArchiveError, ArchiveParts, DirectoryStore, FaultyStore, Listing,
    MemoryStore, ObjectStore, RestoreTarget, decode_segment, plan,
};
use fleetringd::engine::{EngineKind, Execution, MemoryEngine};
use fleetringd::replication::ChainHash;

use crate::common::{Node, TestResult, insert, name, settings, start_replication};

/// Checks that `listing` holds one path, from a snapshot through every segment, that ends at
/// `end`: each segment starts where the one before ends.
fn assert_one_path(listing: &Listing, end: Option<&fleetringd::engine::Position>) {
    let mut segments = listing.segments.clone();
    segments.sort_by(|a, b| a.name.start.cmp(&b.name.start));
    // Without segments, the snapshot itself is at the end.
    let first = segments
        .first()
        .map_or_else(|| end.cloned(), |segment| segment.name.start.clone());
    assert!(
        listing
            .snapshots
            .iter()
            .any(|snapshot| snapshot.position == first),
        "the path begins at a snapshot: {listing:#?}"
    );
    if segments.is_empty() {
        return;
    }
    for pair in segments.windows(2) {
        assert_eq!(
            pair[1].name.start.as_ref(),
            Some(&pair[0].name.end),
            "segments chain start to end"
        );
    }
    assert_eq!(segments.last().map(|segment| &segment.name.end), end);
}

async fn create_ledger(node: &Node) -> Result<DatabaseName, Box<dyn std::error::Error>> {
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    Ok(ledger)
}

/// A physical change of `ledger` at epoch 1, as a pump hands the archive's sink one.
fn change(
    ledger: &DatabaseName,
    start: Option<fleetringd::engine::Position>,
    batch: fleetringd::engine::FrameBatch,
    chain_start: Option<ChainHash>,
    chain_end: ChainHash,
) -> fleetringd::changes::Change {
    fleetringd::changes::Change {
        database: ledger.clone(),
        epoch: fleetring_control::LeaseEpoch::new(1),
        start,
        position: batch.end,
        kind: fleetringd::changes::ChangeKind::Physical(batch.payload),
        chain_start,
        chain_end: Some(chain_end),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chain_the_primary_cant_check_that_differs_takes_a_new_base_snapshot() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    let node = Node::without_streams(
        &dir.path().join("node"),
        Arc::new(store.clone()),
        settings(),
    );
    let ledger = create_ledger(&node).await?;
    let handle = node.registry().open(&ledger).await?;
    let first = handle.committed_since(None).await?.ok_or("the schema")?;
    let schema_end = first.end.clone();
    node.execute(&ledger, insert(1)).await?;
    let second = handle
        .committed_since(Some(schema_end.clone()))
        .await?
        .ok_or("the write")?;
    let end = second.end.clone();
    let primary = node
        .replication
        .verify(&ledger, Some(end.clone()))
        .await?
        .hash;

    // The archive's base holds a chain other than the primary's, as one derived on another
    // lineage would.
    let other = ChainHash::from_bytes(&[7; ChainHash::LEN]).ok_or("a hash")?;
    let sink = node.archive.sink().sink;
    sink.publish(change(&ledger, None, first, None, other))
        .await?;
    // The primary's chain doesn't hold the next delivery's start, so the archive can't check
    // its own hash there, and its batch doesn't continue to the primary's: a new base, not a
    // delivery refused for good.
    sink.publish(change(&ledger, Some(schema_end), second, None, primary))
        .await?;
    let listing = node.listing(&ledger).await?;
    assert!(listing.segments.is_empty(), "{listing:#?}");
    let newest = listing
        .snapshots
        .iter()
        .max_by(|a, b| a.position.cmp(&b.position))
        .ok_or("a snapshot")?;
    assert_eq!(newest.position.as_ref(), Some(&end));
    assert_eq!(newest.chain, primary);
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_archive_takes_a_base_snapshot_then_uploads_one_segment_per_delivery() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = Arc::new(DirectoryStore::new(dir.path().join("store")));
    let node = Node::start(&dir.path().join("node"), store, settings());
    let ledger = create_ledger(&node).await?;
    // The first delivery has no path in the archive, so a snapshot at its end holds it.
    node.await_archived().await?;
    for value in 1..=5 {
        node.execute(&ledger, insert(value)).await?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    node.await_archived().await?;

    let listing = node.listing(&ledger).await?;
    assert_eq!(listing.snapshots.len(), 1, "{listing:#?}");
    assert!(listing.incomplete.is_empty() && listing.damaged.is_empty());
    assert!(!listing.segments.is_empty());
    assert_one_path(&listing, node.position(&ledger).await?.as_ref());
    // The objects are files under the directory store's root, by key.
    let snapshot = &listing.snapshots[0];
    assert!(
        dir.path().join("store").join(&snapshot.key).is_file(),
        "{}",
        snapshot.key
    );
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn segments_chain_from_the_snapshot_without_gaps() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    let node = Node::start(
        &dir.path().join("node"),
        Arc::new(store.clone()),
        settings(),
    );
    let ledger = create_ledger(&node).await?;
    for value in 1..=8 {
        node.execute(&ledger, insert(value)).await?;
        node.await_archived().await?;
    }
    let listing = node.listing(&ledger).await?;
    let mut segments = listing.segments.clone();
    segments.sort_by(|a, b| a.name.start.cmp(&b.name.start));
    let mut chain = listing.snapshots[0].chain;
    for segment in &segments {
        let bytes = store.get(&segment.key).await?.ok_or("the segment")?;
        let (header, _) = decode_segment(&bytes)?;
        assert_eq!(
            header.chain_start.parse::<ChainHash>()?,
            chain,
            "{}",
            segment.key
        );
        assert_eq!(header.database, "ledger");
        assert_eq!(header.engine, "sqlite");
        chain = header.chain_end.parse()?;
    }
    // The chain the archive ends at is the one the primary reports there.
    let position = node.position(&ledger).await?;
    let verified = node.replication.verify(&ledger, position).await?;
    assert_eq!(chain, verified.hash);
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_due_snapshot_is_taken_at_the_archives_position_and_retention_keeps_the_newest()
-> TestResult {
    let dir = tempfile::tempdir()?;
    let mut settings = settings();
    settings.snapshot_after_bytes = 1;
    settings.keep_snapshots = NonZeroU32::new(2).ok_or("nonzero")?;
    let node = Node::start(
        &dir.path().join("node"),
        Arc::new(MemoryStore::new()),
        settings,
    );
    let ledger = create_ledger(&node).await?;
    for value in 1..=6 {
        node.execute(&ledger, insert(value)).await?;
        node.await_archived().await?;
    }
    let listing = node.listing(&ledger).await?;
    let mut positions: Vec<_> = listing
        .snapshots
        .iter()
        .map(|snapshot| snapshot.position.clone())
        .collect();
    positions.sort();
    assert_eq!(positions.len(), 2, "{listing:#?}");
    // Each segment brings a snapshot at its end, the archive's position.
    assert_eq!(positions[1], node.position(&ledger).await?);
    assert!(
        listing
            .segments
            .iter()
            .all(|segment| Some(&segment.name.end) > positions[0].as_ref()),
        "no segment ends at or before the older kept snapshot: {listing:#?}"
    );
    assert!(listing.incomplete.is_empty());
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_store_delays_the_archive_but_never_a_write() -> TestResult {
    let dir = tempfile::tempdir()?;
    let inner = MemoryStore::new();
    let faulty = FaultyStore::new(
        Arc::new(inner.clone()),
        NonZeroU32::new(2).ok_or("nonzero")?,
    );
    let node = Node::start(&dir.path().join("node"), Arc::new(faulty), settings());
    let ledger = create_ledger(&node).await?;
    for value in 1..=12 {
        // Each write is acknowledged whatever the store does.
        node.execute(&ledger, insert(value)).await?;
        if value % 3 == 0 {
            node.await_archived().await?;
        }
    }
    node.await_archived().await?;
    let listing = Listing::read(
        &inner,
        &node.archive.keys(&ledger),
        &ledger,
        EngineKind::Sqlite,
    )
    .await?;
    assert_one_path(&listing, node.position(&ledger).await?.as_ref());
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_primary_continues_the_archive_from_the_mark_without_a_snapshot() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = MemoryStore::new();
    let root = dir.path().join("node");
    let node = Node::start(&root, Arc::new(store.clone()), settings());
    let ledger = create_ledger(&node).await?;
    for value in 1..=5 {
        node.execute(&ledger, insert(value)).await?;
        node.await_archived().await?;
    }
    // One more write records the last delivery's mark in the database.
    node.execute(&ledger, insert(6)).await?;
    node.await_archived().await?;
    node.stop().await;

    // A new primary at the next epoch, with an archive that knows nothing yet: its stream starts
    // at the mark the database holds, and the archive finds the segment that ends there.
    let node = Node::start(&root, Arc::new(store.clone()), settings());
    node.serve(&ledger, 2)?;
    for value in 7..=10 {
        node.execute(&ledger, insert(value)).await?;
    }
    node.await_archived().await?;
    let listing = node.listing(&ledger).await?;
    assert_eq!(
        listing.snapshots.len(),
        1,
        "no new base snapshot: {listing:#?}"
    );
    let second = fleetring_control::LeaseEpoch::new(2);
    let first_of_second = listing
        .segments
        .iter()
        .filter(|segment| segment.name.epoch == second)
        .min_by(|a, b| a.name.start.cmp(&b.name.start))
        .ok_or("the new primary archives")?;
    // It continues where an earlier segment ends: at the mark, repeating what the old primary
    // archived after it.
    assert!(
        listing
            .segments
            .iter()
            .any(|segment| Some(&segment.name.end) == first_of_second.name.start.as_ref()),
        "{listing:#?}"
    );
    // A restore walks through both epochs, preferring the new one where they overlap.
    let plan = plan(&listing, &RestoreTarget::Latest)?;
    assert_eq!(plan.position, node.position(&ledger).await?);
    assert_eq!(
        plan.segments.last().map(|segment| segment.name.epoch),
        Some(second)
    );
    node.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_archive_on_a_self_replicating_engine_is_refused_at_start() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (membership, _manual) = fleetringd::Membership::manual();
    let replication = start_replication(dir.path(), MemoryEngine::new(), membership);
    let refused = Archive::new(
        &ArchiveConfig::directory("backups", [every()], dir.path().join("store")),
        Arc::new(MemoryStore::new()),
        ArchiveParts {
            replication,
            engine: Arc::new(MemoryEngine::new()),
            clock: Arc::new(SystemClock),
            execution: Execution::Threads,
        },
    );
    let Err(error) = refused else {
        panic!("an archive on the memory engine was built");
    };
    assert!(
        matches!(&error, ArchiveError::SelfReplicating { engine, .. } if engine.to_string() == "memory"),
        "{error:?}"
    );
    assert!(error.to_string().contains("memory"), "{error}");
    Ok(())
}

fn every() -> fleetringd::changes::DatabaseMatcher {
    "*".parse().expect("a star")
}

/// A data node's configuration with `tables` appended.
fn config(tables: &str) -> Result<Config, fleetringd::ConfigError> {
    Config::from_toml(&format!(
        "data_dir = \"/tmp/fleetring-test\"\nlisten = \"127.0.0.1:0\"\ninsecure_plaintext = true\n\n[node]\nid = \"data-1\"\n{tables}"
    ))
}

#[test]
fn archive_config_is_validated() {
    let directory = "[[archive]]\nname = \"backups\"\ndatabases = [\"*\"]\nstore = \"directory\"\npath = \"/mnt/backups\"\n";
    let parsed = config(directory).expect("a directory archive");
    assert_eq!(parsed.archive.len(), 1);
    assert_eq!(parsed.archive[0].prefix, "fleetring");
    assert_eq!(
        parsed.archive[0].segment_interval_ms,
        NonZeroU64::new(10_000).expect("nonzero")
    );
    let s3 = "[[archive]]\nname = \"s3\"\ndatabases = [\"*\"]\nstore = \"s3\"\nbucket = \"b\"\nregion = \"eu-west-1\"\n";
    let parsed = config(s3).expect("an s3 archive");
    let settings = parsed.archive[0].s3_settings().expect("s3 settings");
    assert_eq!(settings.endpoint, "https://s3.eu-west-1.amazonaws.com");

    let refused = [
        (
            "a sink and an archive sharing a name",
            format!(
                "[[changes.sink]]\nname = \"backups\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = \"/x\"\n\n{directory}"
            ),
            "unique",
        ),
        (
            "two archives sharing a name",
            format!("{directory}\n{directory}"),
            "unique",
        ),
        (
            "s3 without a bucket",
            "[[archive]]\nname = \"s3\"\ndatabases = [\"*\"]\nstore = \"s3\"\nregion = \"r\"\n"
                .to_owned(),
            "bucket",
        ),
        (
            "plain http without allow_http",
            format!("{s3}endpoint = \"http://127.0.0.1:9000\"\n"),
            "allow_http",
        ),
        (
            "a directory without a path",
            "[[archive]]\nname = \"d\"\ndatabases = [\"*\"]\nstore = \"directory\"\n".to_owned(),
            "path",
        ),
        (
            "a file credential without a file",
            format!("{s3}credentials = \"file\"\n"),
            "credentials_file",
        ),
        (
            "no databases",
            "[[archive]]\nname = \"d\"\ndatabases = []\nstore = \"directory\"\npath = \"/x\"\n"
                .to_owned(),
            "databases",
        ),
        (
            "keep_snapshots = 0",
            format!("{directory}keep_snapshots = 0\n"),
            "keep_snapshots",
        ),
        (
            "a prefix with a trailing slash",
            format!("{directory}prefix = \"a/\"\n"),
            "prefix",
        ),
    ];
    for (what, text, mentions) in refused {
        let error = config(&text).expect_err(what).to_string();
        assert!(error.contains(mentions), "{what}: {error}");
    }
    assert!(
        config(&format!(
            "{s3}endpoint = \"http://127.0.0.1:9000\"\nallow_http = true\n"
        ))
        .is_ok()
    );
}
