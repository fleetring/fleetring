//! The follower's checks: epochs, continuity, and engines that replicate themselves.

use std::collections::BTreeMap;

use fleetring_proto::v1::replicate_message::Message;
use fleetring_proto::v1::{Batch, ReplicateMessage, ReplicateOpen};
use fleetringd::engine::{EngineKind, MemoryEngine, Statement, Transaction};
use fleetringd::replication::ServePrimaryError;
use fleetringd::transport::{Peers, TransportConfig, replication_client};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

use crate::common::{Cluster, TestResult, guarded, name, role, settings, sqlite};

fn open(database: &str, epoch: u64) -> ReplicateMessage {
    ReplicateMessage {
        message: Some(Message::Open(ReplicateOpen {
            database: database.to_owned(),
            epoch,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_refuse_older_epochs_and_batches_that_skip_ahead() -> TestResult {
    let cluster = Cluster::start(&["a", "b"], sqlite(), settings(), BTreeMap::new()).await?;
    let db = name("fenced");
    cluster.create("a", &db, 2).await?;
    let follower = cluster.address("b");
    let status = cluster
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
        })),
    };
    let (acks, status) = replicate(&follower, vec![open("fenced", 1), gap]).await?;
    assert_eq!(acks.len(), 1, "the copy's position comes first");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(status.message().contains("batch starts at"), "{status:?}");

    // The primary's own stream, which the test's streams replaced, reopens and goes on.
    let registry = cluster.node("a").registry();
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
    cluster.await_caught_up("a", &db).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_replicating_engines_are_refused_cleanly() -> TestResult {
    let cluster = Cluster::start(
        &["a", "b"],
        MemoryEngine::new(),
        settings(),
        BTreeMap::new(),
    )
    .await?;
    let db = name("memory");
    let a = cluster.node("a");
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
    let (acks, status) = replicate(&cluster.address("b"), vec![open("memory", 1)]).await?;
    assert!(acks.is_empty());
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(
        status.message().contains("replicate themselves"),
        "{status:?}"
    );
    Ok(())
}
