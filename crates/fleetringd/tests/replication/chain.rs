//! FR-044: every replica's hash chain agrees at every common position, a batch whose hash
//! doesn't continue a follower's chain is refused and the follower re-seeded, and a byte changed
//! in a follower's log after it was applied is caught when the log is verified.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use fleetring::ReplicationFactor;
use fleetring_control::{Acks, DatabaseName, Durability, LeaseEpoch};
use fleetring_proto::v1::replicate_message::Message;
use fleetring_proto::v1::{Batch, ReplicateMessage, ReplicateOpen, Reseed};
use fleetringd::engine::{DatabaseEngine, EngineCapability, Position};
use fleetringd::replication::{ChainHash, PRIMARY_METADATA_KEY, PrimaryRole, ReplicationSettings};
use fleetringd::transport::{Peers, TransportConfig, replication_client};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::metadata::MetadataValue;

use crate::common::{
    Fleet, TestResult, checkpoint, eventually, guarded, name, put, settings, sqlite,
};

/// Every node's chain over `db`, by node.
fn chains(fleet: &Fleet, db: &DatabaseName) -> BTreeMap<String, BTreeMap<Position, ChainHash>> {
    fleet
        .nodes
        .iter()
        .filter_map(|node| {
            let chain = node.replication.chain_history(db)?;
            Some((node.id.to_string(), chain))
        })
        .collect()
}

/// Asserts that every chain in `chains` holds the same hash at every position any two share,
/// and returns the positions they all share.
fn assert_agree(chains: &BTreeMap<String, BTreeMap<Position, ChainHash>>) -> Vec<Position> {
    let mut common: Option<Vec<Position>> = None;
    for (node, chain) in chains {
        for (other, theirs) in chains {
            for (position, hash) in chain {
                if let Some(held) = theirs.get(position) {
                    assert_eq!(held, hash, "{node} and {other} differ at {position}");
                }
            }
        }
        let positions: Vec<Position> = chain.keys().cloned().collect();
        common = Some(match common {
            None => positions,
            Some(common) => common
                .into_iter()
                .filter(|p| positions.contains(p))
                .collect(),
        });
    }
    common.unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replica_reports_the_same_chain_hash_at_every_common_position() -> TestResult {
    let settings = ReplicationSettings {
        checkpoint_threshold: 16 * 1024,
        ..settings()
    };
    let fleet = Fleet::start(&["a", "b", "c"], sqlite(), settings, BTreeMap::new()).await?;
    let db = name("chained");
    fleet.create("a", &db, 3).await?;
    let registry = fleet.node("a").registry();
    for i in 0..40_i64 {
        registry.execute(&db, put(i, 700)).await?;
    }
    fleet.await_caught_up("a", &db).await?;
    let primary = &fleet.node("a").replication;
    assert!(
        primary
            .primary_status(&db)
            .ok_or("primary")?
            .checkpointed
            .is_some(),
        "the writes crossed a checkpoint"
    );
    let marker = checkpoint(primary, &db).await?;
    for follower in ["b", "c"] {
        let replication = &fleet.node(follower).replication;
        eventually("the follower checkpointing", || {
            replication
                .follower_status(&db)
                .is_some_and(|status| status.checkpointed.as_ref() == Some(&marker.position))
        })
        .await?;
    }
    for i in 40..60_i64 {
        registry.execute(&db, put(i, 300)).await?;
    }
    fleet.await_caught_up("a", &db).await?;

    let held = chains(&fleet, &db);
    assert_eq!(held.len(), 3, "every node holds a chain: {held:?}");
    let common = assert_agree(&held);
    assert!(common.len() >= 20, "{} common positions", common.len());

    // Each replica recomputes the same hashes from its log on disk, at the latest position and
    // at the checkpoint's.
    let mut latest = Vec::new();
    for node in ["a", "b", "c"] {
        let verified = guarded(
            "verifying the latest position",
            fleet.node(node).replication.verify(&db, None),
        )
        .await?
        .map_err(|status| format!("{node}: {status}"))?;
        assert_eq!(
            verified.position,
            primary.primary_status(&db).ok_or("primary")?.committed
        );
        latest.push(verified.hash);
        let at_marker = fleet
            .node(node)
            .replication
            .verify(&db, Some(marker.position.clone()))
            .await?;
        assert_eq!(
            Some(at_marker.hash),
            held["a"].get(&marker.position).copied(),
            "{node} at the checkpoint"
        );
    }
    assert!(latest.iter().all(|hash| *hash == latest[0]), "{latest:?}");

    // A position no replica holds a hash for is reported as such.
    let unknown = fleet
        .node("b")
        .replication
        .verify(&db, Some(Position::new([0xff; 20])))
        .await
        .expect_err("an unknown position");
    assert!(
        matches!(unknown.code(), Code::NotFound | Code::FailedPrecondition),
        "{unknown:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_derived_from_where_the_log_begins_agrees_with_verify() -> TestResult {
    let fleet = Fleet::start(&["a"], sqlite(), settings(), BTreeMap::new()).await?;
    let db = name("derived");
    fleet.create("a", &db, 1).await?;
    let node = fleet.node("a");
    for i in 0..3_i64 {
        node.registry().execute(&db, put(i, 100)).await?;
    }
    // Without its record, a primary that serves the database again derives its chain from where
    // the log's generation begins, past the generation's header.
    node.replication.stop_primary(&db);
    match fs::remove_file(node.root.join("replication/epochs/derived/chain")) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        _ => {}
    }
    node.replication.serve_primary(
        db.clone(),
        PrimaryRole::new(ReplicationFactor::ONE, LeaseEpoch::new(2)),
    )?;
    node.registry().execute(&db, put(3, 100)).await?;
    // The chain is rebuilt on a task of its own, and a primary with no follower chains a batch it
    // publishes only when a checkpoint or a snapshot reaches for it, so the chain may stop short
    // of the write, or reach it, depending on which the rebuild read first. Either way it is
    // derived from the log's start, so it is compared with `verify` at the last position it
    // holds, once it holds more than its anchor.
    eventually("the derived chain", || {
        node.replication
            .chain_history(&db)
            .is_some_and(|chain| chain.len() > 1)
    })
    .await?;
    let chain = node
        .replication
        .chain_history(&db)
        .ok_or("the chain is built")?;
    let (tip, held) = chain.last_key_value().ok_or("a position")?;
    let verified = guarded("verifying", async {
        loop {
            match node.replication.verify(&db, Some(tip.clone())).await {
                Err(status) if status.code() == Code::Unavailable => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                verified => return verified,
            }
        }
    })
    .await??;
    assert_eq!(
        verified.position.as_ref(),
        Some(tip),
        "verified where asked"
    );
    assert_eq!(
        *held, verified.hash,
        "the chain holds what the log reads back to"
    );
    Ok(())
}

/// Where `node` keeps `db`'s mirrored log.
fn wal_path(fleet: &Fleet, node: &str, db: &str) -> PathBuf {
    fleet.node(node).database_dir(db).join("data.db-wal")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_byte_changed_after_apply_is_caught_when_the_log_is_verified() -> TestResult {
    let fleet = Fleet::start(&["a", "b", "c"], sqlite(), settings(), BTreeMap::new()).await?;
    let db = name("tampered");
    fleet.create("a", &db, 3).await?;
    let registry = fleet.node("a").registry();
    for i in 0..5_i64 {
        registry.execute(&db, put(i, 200)).await?;
    }
    fleet.await_caught_up("a", &db).await?;
    let agreed = fleet.node("c").replication.verify(&db, None).await?;

    // Flip a byte of page content in the last frame of b's log, after b applied and
    // acknowledged it.
    let wal = wal_path(&fleet, "b", "tampered");
    let mut bytes = fs::read(&wal)?;
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    fs::write(&wal, &bytes)?;

    let refused = fleet
        .node("b")
        .replication
        .verify(&db, agreed.position.clone())
        .await
        .expect_err("the changed log doesn't verify");
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
    assert!(
        refused.message().contains("can't be read back"),
        "{refused:?}"
    );
    for node in ["a", "c"] {
        let verified = fleet.node(node).replication.verify(&db, None).await?;
        assert_eq!(verified.hash, agreed.hash, "{node} still agrees");
    }
    Ok(())
}

/// Opens a `Replicate` stream to `address` as if from primary `primary`, and sends `open`.
async fn open_stream(
    address: &str,
    primary: &str,
    open: ReplicateOpen,
) -> Result<
    (
        mpsc::Sender<ReplicateMessage>,
        tonic::Streaming<fleetring_proto::v1::ReplicateAck>,
    ),
    Box<dyn std::error::Error>,
> {
    let peers = Peers::new(TransportConfig::default());
    let mut client = replication_client(peers.channel(address.parse()?));
    let (sender, receiver) = mpsc::channel(8);
    sender
        .send(ReplicateMessage {
            message: Some(Message::Open(open)),
        })
        .await?;
    let mut request = tonic::Request::new(ReceiverStream::new(receiver));
    request
        .metadata_mut()
        .insert(PRIMARY_METADATA_KEY, MetadataValue::try_from(primary)?);
    let acks = client.replicate(request).await?.into_inner();
    Ok((sender, acks))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_whose_hash_differs_is_refused_and_the_follower_reseeded() -> TestResult {
    // The test's own stream takes the follower over; a slow retry keeps the primary's stream
    // from taking it back before the exchange is done.
    let settings = ReplicationSettings {
        retry_interval: std::time::Duration::from_secs(2),
        ..settings()
    };
    let fleet = Fleet::start(&["a", "b"], sqlite(), settings, BTreeMap::new()).await?;
    let db = name("forged");
    // Acknowledged by the primary alone, so a write commits while the test holds the follower.
    let two = ReplicationFactor::new(2).ok_or("nonzero")?;
    let role = PrimaryRole::new(two, LeaseEpoch::new(1))
        .with_durability(Durability::new(Acks::Local, two));
    fleet.create_as("a", &db, role).await?;
    let a = fleet.node("a");
    let b = fleet.node("b");
    a.registry().execute(&db, put(1, 100)).await?;
    fleet.await_caught_up("a", &db).await?;
    let before = b.replication.follower_status(&db).ok_or("follows")?;
    let at = before.position.clone().ok_or("a position")?;

    // Commit a batch the follower hasn't seen, by taking its stream over first, so that the
    // primary's own stream doesn't ship it.
    let (sender, mut acks) = open_stream(
        &fleet.address("b"),
        "a",
        ReplicateOpen {
            database: db.to_string(),
            epoch: 1,
            epoch_start: None,
            epoch_start_chain: None,
        },
    )
    .await?;
    let first = guarded("the first acknowledgement", acks.message())
        .await??
        .ok_or("no acknowledgement")?;
    assert_eq!(first.position, at.as_bytes());
    a.registry().execute(&db, put(2, 100)).await?;
    let handle = a.registry().open(&db).await?;
    let batch = handle
        .committed_since(Some(at.clone()))
        .await?
        .ok_or("a new batch")?;
    let engine = sqlite();
    let EngineCapability::FrameSource(frames) = engine.capability() else {
        return Err("SQLite is a frame source".into());
    };
    let held = b
        .replication
        .chain_history(&db)
        .and_then(|chain| chain.get(&at).copied())
        .ok_or("the follower's hash at its position")?;
    let right = ChainHash::chain(frames, held, Some(&at), &batch)?;
    let mut wrong = *right.as_bytes();
    wrong[0] ^= 0x80;

    let shipped = |chain: Vec<u8>| ReplicateMessage {
        message: Some(Message::Batch(Batch {
            epoch: 1,
            start: at.as_bytes().to_vec(),
            end: batch.end.as_bytes().to_vec(),
            payload: batch.payload.as_bytes().to_vec(),
            chain: Some(chain),
        })),
    };
    sender.send(shipped(wrong.to_vec())).await?;
    let reply = guarded("the reply to the forged hash", acks.message())
        .await??
        .ok_or("a reply")?;
    assert!(reply.needs_snapshot, "{reply:?}");
    assert_eq!(reply.position, at.as_bytes(), "nothing was applied");
    let status = b.replication.follower_status(&db).ok_or("follows")?;
    assert_eq!(status.position, Some(at.clone()));
    assert_eq!(status.snapshots, before.snapshots);

    // Until re-seeded the copy takes nothing, as after any divergence; a re-seed from the
    // primary brings it to the primary's position with the primary's chain, and the primary's
    // own stream then carries on from there.
    sender.send(shipped(right.as_bytes().to_vec())).await?;
    sender
        .send(ReplicateMessage {
            message: Some(Message::Reseed(Reseed { epoch: 1 })),
        })
        .await?;
    let reseeded = guarded("the re-seed's acknowledgement", acks.message())
        .await??
        .ok_or("an acknowledgement")?;
    assert!(!reseeded.needs_snapshot);
    assert_eq!(reseeded.position, batch.end.as_bytes());
    drop(sender);
    let status = b.replication.follower_status(&db).ok_or("follows")?;
    assert_eq!(status.snapshots, before.snapshots + 1, "{status:?}");

    assert!(
        a.replication
            .await_writable(&db, crate::common::HANG_GUARD)
            .await
    );
    a.registry().execute(&db, put(3, 100)).await?;
    fleet.await_caught_up("a", &db).await?;
    let held = chains(&fleet, &db);
    let common = assert_agree(&held);
    assert!(common.contains(&batch.end), "{common:?}");
    let status = b.replication.follower_status(&db).ok_or("follows")?;
    assert_eq!(
        status.snapshots,
        before.snapshots + 1,
        "not re-seeded again"
    );
    Ok(())
}
