//! FR-032: followers catch up from checksummed snapshots, while writes go on, and a transfer that
//! fails never leaves a partial database in place.

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring_control::LeaseEpoch;
use fleetring_proto::v1::replication_server::Replication as ReplicationRpc;
use fleetring_proto::v1::{
    FenceRequest, FenceResponse, FetchRequest, FetchResponse, PromoteRequest, PromoteResponse,
    ReplicateMessage, SnapshotChunk, SnapshotRequest,
};
use fleetringd::DatabaseError;
use fleetringd::engine::{Statement, Transaction, Value};
use fleetringd::replication::{PrimaryRole, ReplicationService, ReplicationSettings, SeedError};
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::common::{
    Cluster, HANG_GUARD, TestResult, Wrap, checkpoint, eventually, guarded, id, name, put, routes,
    settings, sqlite, unhurried,
};

/// The size the CI run loads before adding a follower.
const CI_MEGABYTES: usize = 4;

/// Bytes per row of the load, and rows per load transaction.
const ROW_BYTES: usize = 64 * 1024;
const ROWS_PER_TRANSACTION: usize = 16;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_added_during_writes_catches_up_and_ends_byte_identical() -> TestResult {
    added_follower_catches_up(CI_MEGABYTES).await
}

/// The ticket's full size. Slow in a debug build; run it with `--release -- --ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "loads 100 MB; run in release"]
async fn a_follower_added_to_a_100_mb_database_catches_up() -> TestResult {
    added_follower_catches_up(100).await
}

/// Loads `megabytes` into a database with two replicas, then grows its replica set to three while
/// a writer keeps writing. The added follower seeds itself from a snapshot, catches up, stays
/// current, and ends byte-identical to the primary once both checkpoint.
async fn added_follower_catches_up(megabytes: usize) -> TestResult {
    let settings = ReplicationSettings {
        // Generous, so a large transfer in a slow build isn't cut short.
        open_timeout: Duration::from_secs(600),
        ..unhurried()
    };
    let cluster = Cluster::start(&["a", "b", "c"], sqlite(), settings, BTreeMap::new()).await?;
    let db = name("grown");
    let pair = cluster.node("a").replica_set(&db, 2);
    let [primary, first] = &pair[..] else {
        return Err(format!("two replicas expected, got {pair:?}").into());
    };
    let added = ["a", "b", "c"]
        .into_iter()
        .find(|node| !pair.iter().any(|member| member == node))
        .ok_or("a third node")?;
    cluster.create(primary, &db, 2).await?;

    let registry = cluster.node(primary).registry().clone();
    let transactions = megabytes * 1024 * 1024 / (ROW_BYTES * ROWS_PER_TRANSACTION);
    for t in 0..transactions {
        let rows = (0..ROWS_PER_TRANSACTION).map(|r| {
            let k = i64::try_from(t * ROWS_PER_TRANSACTION + r).expect("small");
            put(k, ROW_BYTES).statements()[0].clone()
        });
        registry.execute(&db, Transaction::new(rows)).await?;
    }
    cluster.await_caught_up(primary, &db).await?;

    // Grow the replica set while a writer keeps writing.
    let writer = {
        let registry = registry.clone();
        let db = db.clone();
        tokio::spawn(async move {
            let mut acknowledged = 0_u32;
            for i in 0..300_i64 {
                let k = 1_000_000 + i;
                let len = usize::try_from(1 + (i * 37) % 3_000).expect("small");
                if tolerate(registry.execute(&db, put(k, len)).await)? {
                    acknowledged += 1;
                }
                if i % 10 == 0 {
                    let delete = Transaction::from(Statement::with_params(
                        "DELETE FROM kv WHERE k = ?1",
                        [Value::Integer(i)],
                    ));
                    tolerate(registry.execute(&db, delete).await)?;
                }
            }
            Ok::<_, DatabaseError>(acknowledged)
        })
    };
    let replication = &cluster.node(primary).replication;
    replication.serve_primary(db.clone(), crate::common::role(3))?;
    let acknowledged = guarded("the writer", writer).await???;
    assert!(
        acknowledged > 0,
        "writes went on while the follower was added"
    );
    cluster.await_caught_up(primary, &db).await?;
    let status = replication.primary_status(&db).ok_or("not primary")?;
    assert_eq!(status.followers.len(), 2, "{status:?}");
    let seeded = cluster
        .node(added)
        .replication
        .follower_status(&db)
        .ok_or("the added node follows")?;
    assert!(seeded.snapshots >= 1, "{added} seeded from a snapshot");

    // It stays current.
    for i in 0..20_i64 {
        registry.execute(&db, put(2_000_000 + i, 900)).await?;
    }
    cluster.await_caught_up(primary, &db).await?;

    let marker = checkpoint(replication, &db).await?;
    for follower in [first.as_str(), added] {
        let follower = &cluster.node(follower).replication;
        eventually("the follower checkpointing at the marker", || {
            follower
                .follower_status(&db)
                .is_some_and(|status| status.checkpointed.as_ref() == Some(&marker.position))
        })
        .await?;
    }
    let file = |node: &str| fs::read(cluster.node(node).database_dir("grown").join("data.db"));
    let expected = file(primary)?;
    assert!(
        expected.len() >= megabytes * 1024 * 1024,
        "the database holds the load"
    );
    for follower in [first.as_str(), added] {
        let copy = file(follower)?;
        assert_eq!(copy.len(), expected.len(), "{follower}'s database size");
        assert!(
            copy == expected,
            "{follower}'s database differs from the primary's"
        );
    }
    Ok(())
}

/// Whether a write was acknowledged. A write that committed on the primary but wasn't
/// acknowledged, as happens while streams reopen, isn't an error here.
fn tolerate<T>(written: Result<T, DatabaseError>) -> Result<bool, DatabaseError> {
    match written {
        Ok(_) => Ok(true),
        Err(DatabaseError::Replication(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

/// What the tampering proxy does to one `Snapshot` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Passes the first chunk, then fails the stream.
    Cut,
    /// Flips one bit of file content in the first chunk.
    Corrupt,
    /// Waits for the test's release before passing anything.
    Hold,
}

/// A primary's `Replication` service that tampers with its snapshot transfers.
#[derive(Debug, Clone)]
struct Tamper {
    inner: ReplicationService,
    faults: Arc<Mutex<VecDeque<Fault>>>,
    calls: Arc<AtomicUsize>,
    release: watch::Receiver<bool>,
}

impl Tamper {
    fn wrap(
        faults: Arc<Mutex<VecDeque<Fault>>>,
        calls: Arc<AtomicUsize>,
        release: watch::Receiver<bool>,
    ) -> Wrap {
        Box::new(move |replication| {
            routes(Self {
                inner: replication.service(),
                faults,
                calls,
                release,
            })
        })
    }
}

#[tonic::async_trait]
impl ReplicationRpc for Tamper {
    type ReplicateStream = <ReplicationService as ReplicationRpc>::ReplicateStream;
    type SnapshotStream = <ReplicationService as ReplicationRpc>::SnapshotStream;

    async fn replicate(
        &self,
        request: Request<Streaming<ReplicateMessage>>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        self.inner.replicate(request).await
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        self.inner.fence(request).await
    }

    async fn fetch(
        &self,
        request: Request<FetchRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        self.inner.fetch(request).await
    }

    async fn promote(
        &self,
        request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        self.inner.promote(request).await
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let fault = self.faults.lock().expect("faults lock").pop_front();
        let mut chunks = self.inner.snapshot(request).await?.into_inner();
        let Some(fault) = fault else {
            return Ok(Response::new(chunks));
        };
        let mut release = self.release.clone();
        let (sender, receiver) = mpsc::channel::<Result<SnapshotChunk, Status>>(4);
        tokio::spawn(async move {
            if fault == Fault::Hold && release.wait_for(|on| *on).await.is_err() {
                return;
            }
            let mut tampered = false;
            while let Some(mut chunk) = chunks.next().await {
                if let Ok(data) = &mut chunk
                    && !data.data.is_empty()
                    && !tampered
                {
                    tampered = true;
                    match fault {
                        Fault::Cut => {
                            let _ = sender.send(chunk).await;
                            let _ = sender.send(Err(Status::unavailable("cut"))).await;
                            return;
                        }
                        Fault::Corrupt => {
                            // The last byte is file content, so only the checksum can tell.
                            if let Some(last) = data.data.last_mut() {
                                *last ^= 1;
                            }
                        }
                        Fault::Hold => {}
                    }
                }
                if sender.send(chunk).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

/// The staging directories a node's transfers left under `databases`.
fn staged(databases: &Path) -> Vec<String> {
    fs::read_dir(databases)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|entry| entry.file_name().into_string().ok())
                .filter(|name| name.starts_with('.'))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_transfers_restart_cleanly_and_never_leave_a_partial_copy() -> TestResult {
    let faults = Arc::new(Mutex::new(VecDeque::from([
        Fault::Cut,
        Fault::Corrupt,
        Fault::Hold,
    ])));
    let calls = Arc::new(AtomicUsize::new(0));
    let (release, released) = watch::channel(false);
    let wraps = BTreeMap::from([(
        "a".to_owned(),
        Tamper::wrap(Arc::clone(&faults), Arc::clone(&calls), released),
    )]);
    let cluster = Cluster::start(&["a", "b"], sqlite(), settings(), wraps).await?;
    let db = name("tampered");
    let follower = cluster.node("b");
    let databases = follower.root.join("databases");

    // The first transfer is cut, the second corrupted; while the third is held, the follower has
    // no copy at all, and the failed transfers' staging directories are gone.
    let created = cluster.create("a", &db, 2);
    let checked = async {
        eventually("the third transfer", || calls.load(Ordering::SeqCst) >= 3).await?;
        assert!(
            !follower.database_dir("tampered").exists(),
            "no partial copy is in place"
        );
        let left = staged(&databases);
        assert!(left.len() <= 1, "only the held transfer stages: {left:?}");
        release.send_replace(true);
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (created, checked) = tokio::join!(created, checked);
    checked?;
    created?;
    let status = follower.replication.follower_status(&db).ok_or("follows")?;
    assert_eq!(status.snapshots, 1, "only the verified transfer installed");
    assert!(staged(&databases).is_empty(), "{:?}", staged(&databases));

    // Seeding the copy again through a corrupted transfer fails and keeps the old copy.
    cluster
        .node("a")
        .registry()
        .execute(&db, put(1, 4_000))
        .await?;
    cluster.await_caught_up("a", &db).await?;
    let before = fs::read(follower.database_dir("tampered").join("data.db"))?;
    let wal_before = fs::read(follower.database_dir("tampered").join("data.db-wal"))?;
    faults
        .lock()
        .expect("faults lock")
        .push_back(Fault::Corrupt);
    let refused = follower
        .replication
        .seed(&db, &id("a"), LeaseEpoch::new(1))
        .await;
    assert!(
        matches!(&refused, Err(SeedError::Failed { reason, .. }) if reason.contains("checksum")),
        "{refused:?}"
    );
    assert_eq!(
        fs::read(follower.database_dir("tampered").join("data.db"))?,
        before
    );
    assert_eq!(
        fs::read(follower.database_dir("tampered").join("data.db-wal"))?,
        wal_before
    );
    assert!(staged(&databases).is_empty(), "{:?}", staged(&databases));

    // A clean transfer seeds it, and replication carries on from the snapshot's position.
    let seeded = follower
        .replication
        .seed(&db, &id("a"), LeaseEpoch::new(1))
        .await?;
    let primary = &cluster.node("a").replication;
    assert_eq!(
        seeded,
        primary.primary_status(&db).ok_or("primary")?.committed
    );
    // Seeding ended the follower's stream; the primary reopens it from the new copy.
    assert!(primary.await_writable(&db, HANG_GUARD).await);
    cluster
        .node("a")
        .registry()
        .execute(&db, put(2, 4_000))
        .await?;
    cluster.await_caught_up("a", &db).await?;
    Ok(())
}

/// A follower whose log came from an earlier epoch and runs past where the new epoch's log
/// began, in a newer generation than that point, can't cut its tail back there: the frames up to
/// its last checkpoint are in its database file. The primary re-seeds it instead, once, and
/// replication goes on from the snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_diverges_behind_its_last_checkpoint_is_reseeded() -> TestResult {
    let cluster = Cluster::start(&["a", "b"], sqlite(), settings(), BTreeMap::new()).await?;
    let db = name("diverged");
    cluster.create("a", &db, 2).await?;
    let primary = &cluster.node("a").replication;
    let registry = cluster.node("a").registry();
    let follower = &cluster.node("b").replication;

    registry.execute(&db, put(1, 1_000)).await?;
    cluster.await_caught_up("a", &db).await?;
    let start = primary.primary_status(&db).ok_or("primary")?.committed;
    registry.execute(&db, put(2, 1_000)).await?;
    cluster.await_caught_up("a", &db).await?;
    primary.checkpoint(&db).await?;
    // The follower takes the next generation, and with it the checkpoint, at epoch 1.
    registry.execute(&db, put(3, 1_000)).await?;
    cluster.await_caught_up("a", &db).await?;
    let before = follower.follower_status(&db).ok_or("follows")?.snapshots;

    // Epoch 2's log began back at `start`, before that checkpoint.
    let epoch = PrimaryRole {
        epoch: LeaseEpoch::new(2),
        ..crate::common::role(2)
    };
    primary.lead(&db, epoch, start).await?;
    assert!(primary.await_writable(&db, HANG_GUARD).await);
    registry.execute(&db, put(4, 1_000)).await?;
    cluster.await_caught_up("a", &db).await?;
    let status = follower.follower_status(&db).ok_or("follows")?;
    assert_eq!(status.snapshots, before + 1, "re-seeded once: {status:?}");
    assert_eq!(status.epoch, Some(LeaseEpoch::new(2)));

    // The re-seeded copy is epoch 2's log, so the streams serving again reopens, which carry
    // the same start, neither cut it back nor re-seed it.
    primary.serve_primary(db.clone(), epoch)?;
    assert!(primary.await_writable(&db, HANG_GUARD).await);
    registry.execute(&db, put(5, 1_000)).await?;
    cluster.await_caught_up("a", &db).await?;
    let status = follower.follower_status(&db).ok_or("follows")?;
    assert_eq!(
        status.snapshots,
        before + 1,
        "not re-seeded again: {status:?}"
    );
    Ok(())
}
