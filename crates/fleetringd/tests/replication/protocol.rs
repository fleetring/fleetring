//! The follower's checks: epochs, continuity, and engines that replicate themselves.

use std::collections::BTreeMap;

use fleetring_proto::v1::replicate_message::Message;
use fleetring_proto::v1::{Batch, ReplicateAck, ReplicateMessage, ReplicateOpen};
use fleetringd::engine::{EngineKind, MemoryEngine, Statement, Transaction};
use fleetringd::replication::ServePrimaryError;
use fleetringd::transport::{Peers, TransportConfig, replication_client};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

use crate::common::{Fleet, TestResult, guarded, name, role, settings, sqlite};

fn open(database: &str, epoch: u64) -> ReplicateMessage {
    ReplicateMessage {
        message: Some(Message::Open(ReplicateOpen {
            database: database.to_owned(),
            epoch,
            epoch_start: None,
        })),
    }
}

/// Opens a `Replicate` stream to `address` and sends `messages`, then returns the status it ends
/// with, after the acknowledgements before it.
async fn replicate(
    address: &str,
    messages: Vec<ReplicateMessage>,
) -> Result<(Vec<Vec<u8>>, tonic::Status), Box<dyn std::error::Error>> {
    let peers = Peers::new(TransportConfig::default());
    let mut client = replication_client(peers.channel(address.parse()?));
    let (sender, receiver) = mpsc::channel(8);
    for message in messages {
        sender.send(message).await?;
    }
    let mut acks = match client.replicate(ReceiverStream::new(receiver)).await {
        Ok(response) => response.into_inner(),
        Err(status) => return Ok((Vec::new(), status)),
    };
    let mut positions = Vec::new();
    loop {
        match guarded("an acknowledgement", acks.message()).await? {
            Ok(Some(ack)) => positions.push(ack.position),
            Ok(None) => return Err("the stream ended without an error".into()),
            Err(status) => return Ok((positions, status)),
        }
    }
}

/// As [`replicate`], but tries again while the follower ended the stream because a newer one
/// replaced it, which says nothing about the messages sent.
///
/// A stream that opens at a current epoch takes the follower's slot, so the primary's own stream
/// is ended and reopens after the retry interval. On a slow host that can happen before the
/// follower has read the test's later messages, and the primary's stream then replaces the test's
/// in turn. Only a stream the follower got to judge says what the test asks about.
async fn replicate_until_judged(
    address: &str,
    messages: &[ReplicateMessage],
) -> Result<(Vec<Vec<u8>>, tonic::Status), Box<dyn std::error::Error>> {
    guarded("a stream the follower judged", async {
        loop {
            let (acks, status) = replicate(address, messages.to_vec()).await?;
            if status.code() != Code::Aborted {
                return Ok::<_, Box<dyn std::error::Error>>((acks, status));
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_refuse_older_epochs_and_batches_that_skip_ahead() -> TestResult {
    let fleet = Fleet::start(&["a", "b"], sqlite(), settings(), BTreeMap::new()).await?;
    let db = name("fenced");
    fleet.create("a", &db, 2).await?;
    let follower = fleet.address("b");
    let status = fleet
        .node("b")
        .replication
        .follower_status(&db)
        .ok_or("not following")?;
    assert_eq!(status.epoch.map(|epoch| epoch.get()), Some(1));

    let (acks, status) = replicate(&follower, vec![open("fenced", 0)]).await?;
    assert!(acks.is_empty());
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(status.message().contains("older"), "{status:?}");

    let gap = ReplicateMessage {
        message: Some(Message::Batch(Batch {
            epoch: 1,
            start: vec![0xff; 20],
            end: vec![0xff; 21],
            payload: Vec::new(),
            chain: None,
        })),
    };
    let (acks, status) = replicate_until_judged(&follower, &[open("fenced", 1), gap]).await?;
    assert_eq!(acks.len(), 1, "the copy's position comes first");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(status.message().contains("batch starts at"), "{status:?}");

    // The primary's own stream, which the test's streams replaced, reopens and goes on.
    let registry = fleet.node("a").registry();
    guarded("a write after the stream reopens", async {
        while registry
            .execute(&db, crate::common::put(5, 5))
            .await
            .is_err()
        {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    fleet.await_caught_up("a", &db).await
}

/// A batch that starts at `at`, where the copy is, but doesn't continue its log.
fn foreign(at: &[u8], end: u8) -> ReplicateMessage {
    ReplicateMessage {
        message: Some(Message::Batch(Batch {
            epoch: 1,
            start: at.to_vec(),
            end: vec![end; 20],
            payload: vec![0xa5; 64],
            chain: None,
        })),
    }
}

/// Opens a stream to the follower at `address`, sends it two foreign batches from `at` and ends
/// it, and returns every acknowledgement it answered with, or the status it ended the stream with.
async fn send_foreign_batches(
    address: &str,
    at: &[u8],
) -> Result<Result<Vec<ReplicateAck>, tonic::Status>, Box<dyn std::error::Error>> {
    let peers = Peers::new(TransportConfig::default());
    let mut client = replication_client(peers.channel(address.parse()?));
    let (sender, receiver) = mpsc::channel(4);
    for message in [
        open("unfollowable", 1),
        foreign(at, 0xfe),
        foreign(at, 0xff),
    ] {
        sender.send(message).await?;
    }
    drop(sender);
    let mut acks = match client.replicate(ReceiverStream::new(receiver)).await {
        Ok(response) => response.into_inner(),
        Err(status) => return Ok(Err(status)),
    };
    let mut answered = Vec::new();
    loop {
        match guarded("an acknowledgement", acks.message()).await? {
            Ok(Some(ack)) => answered.push(ack),
            Ok(None) => return Ok(Ok(answered)),
            Err(status) => return Ok(Err(status)),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_whose_copy_the_frames_dont_continue_asks_for_a_snapshot() -> TestResult {
    let fleet = Fleet::start(&["a", "b"], sqlite(), settings(), BTreeMap::new()).await?;
    let db = name("unfollowable");
    fleet.create("a", &db, 2).await?;
    let follower = &fleet.node("b").replication;
    let copy = follower
        .follower_status(&db)
        .and_then(|status| status.position)
        .ok_or("no copy")?
        .into_bytes();

    // As in `replicate_until_judged`, the primary's stream can take the follower's slot back
    // before the follower has read the test's batches, which ends the test's stream `Aborted`.
    let address = fleet.address("b");
    let answered = guarded("a stream the follower judged", async {
        loop {
            match send_foreign_batches(&address, &copy).await? {
                Err(status) if status.code() == Code::Aborted => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                answered => return Ok::<_, Box<dyn std::error::Error>>(answered),
            }
        }
    })
    .await??;
    let acks = answered?;

    // The copy's position, then a request for a snapshot there. Until it is re-seeded, the copy
    // takes no frames and acknowledges none, so the second batch gets no answer.
    let [first, reply] = &acks[..] else {
        return Err(format!("two acknowledgements expected: {acks:?}").into());
    };
    assert!(!first.needs_snapshot, "{first:?}");
    assert_eq!(first.position, copy);
    assert!(reply.needs_snapshot, "{reply:?}");
    assert_eq!(reply.position, copy);
    let status = follower.follower_status(&db).ok_or("not following")?;
    assert_eq!(
        status.position.map(|position| position.into_bytes()),
        Some(copy)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_replicating_engines_are_refused_cleanly() -> TestResult {
    let fleet = Fleet::start(
        &["a", "b"],
        MemoryEngine::new(),
        settings(),
        BTreeMap::new(),
    )
    .await?;
    let db = name("memory");
    let a = fleet.node("a");
    assert_eq!(
        a.replication.serve_primary(db.clone(), role(2)),
        Err(ServePrimaryError::SelfReplicating {
            engine: EngineKind::Memory
        })
    );
    // Its writes never reach the sink, so they complete as the engine replicates them.
    a.registry().create(&db).await?;
    a.registry()
        .execute(
            &db,
            Transaction::from(Statement::with_params("put", ["k".into(), 1.into()])),
        )
        .await?;
    let (acks, status) = replicate(&fleet.address("b"), vec![open("memory", 1)]).await?;
    assert!(acks.is_empty());
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(
        status.message().contains("replicate themselves"),
        "{status:?}"
    );
    Ok(())
}
