//! The `WatchLeases` stream: the leases a node holds, as its replication reports them (FR-042).
//!
//! Each stream runs a task of its own that reads the node's [`LeaseWatch`], sends the leases it
//! holds as the first message, and from then on one event per change, derived by comparing each
//! new set of leases with the last one sent. Updates wait in a bounded buffer for the
//! subscriber; a subscriber that lets it fill gets `RESOURCE_EXHAUSTED` and the stream ends, so
//! it opens the stream again and starts from a fresh snapshot rather than from a backlog that
//! would grow without bound.

use std::pin::Pin;

use fleetring::NodeId;
use fleetring_control::DatabaseName;
use fleetring_proto::v1::LeaseUpdate;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::data::convert::{lease_event_to_proto, lease_snapshot_to_proto};
use crate::replication::{LeaseWatch, lease_events};

/// The stream `WatchLeases` returns.
pub(crate) type LeaseStream = Pin<Box<dyn Stream<Item = Result<LeaseUpdate, Status>> + Send>>;

/// How many updates a stream holds for a subscriber that hasn't read them yet. A subscriber
/// further behind than this is cut off with `RESOURCE_EXHAUSTED`.
pub(crate) const STREAM_BUFFER: usize = 256;

/// A stream of `node`'s leases from `watch`: a snapshot, then events, with room for `buffer`
/// unread updates. Only databases `visible` accepts appear, in the snapshot and in events, so a
/// principal sees the leases on the databases it may read.
pub(crate) fn lease_stream(
    node: NodeId,
    watch: LeaseWatch,
    buffer: usize,
    visible: impl Fn(&DatabaseName) -> bool + Send + Sync + 'static,
) -> LeaseStream {
    let (updates, stream) = mpsc::channel(buffer.max(1));
    tokio::spawn(feed(node, watch, updates, visible));
    Box::pin(ReceiverStream::new(stream))
}

async fn feed(
    node: NodeId,
    mut watch: LeaseWatch,
    updates: mpsc::Sender<Result<LeaseUpdate, Status>>,
    visible: impl Fn(&DatabaseName) -> bool + Send + Sync,
) {
    let held_visible = |watch: &mut LeaseWatch| {
        let mut held = watch.held();
        held.retain(|database, _| visible(database));
        held
    };
    let mut held = held_visible(&mut watch);
    let snapshot = lease_snapshot_to_proto(&node, &held);
    if updates.send(Ok(snapshot)).await.is_err() {
        return;
    }
    loop {
        let changed = tokio::select! {
            () = updates.closed() => return,
            changed = watch.changed() => changed,
        };
        if !changed {
            return;
        }
        let now = held_visible(&mut watch);
        for event in lease_events(&held, &now) {
            match updates.try_send(Ok(lease_event_to_proto(&event))) {
                Ok(()) => {}
                Err(TrySendError::Closed(_)) => return,
                Err(TrySendError::Full(_)) => {
                    let behind = Status::resource_exhausted(format!(
                        "the lease watcher fell more than {} updates behind; open the stream \
                         again for a fresh snapshot",
                        updates.max_capacity()
                    ));
                    // The buffer is full, so this waits for the subscriber to read one update,
                    // or for it to go away; either way the stream ends here.
                    let _ = updates.send(Err(behind)).await;
                    return;
                }
            }
        }
        held = now;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use fleetring::ReplicationFactor;
    use fleetring_control::{DatabaseName, LeaseEpoch};
    use fleetring_proto::v1::{LeaseEventKind, lease_update::Update};
    use tokio_stream::StreamExt as _;
    use tonic::Code;

    use super::*;
    use crate::engine::{EngineConfig, Execution, SqliteEngine};
    use crate::replication::{
        EpochHolders, PrimaryRole, Replication, ReplicationParts, ReplicationSettings,
    };
    use crate::transport::Peers;
    use crate::{Membership, Registry};

    fn replication(dir: &std::path::Path) -> Replication {
        let engine = SqliteEngine::new(512).expect("a valid page size");
        let parts = ReplicationParts {
            node: "a".parse().expect("id"),
            engine: Arc::new(engine.clone()),
            membership: Membership::detached(),
            peers: Arc::new(Peers::with_network(crate::Network::default())),
            addresses: Arc::new(BTreeMap::new()),
            authority: Arc::new(BTreeMap::<DatabaseName, EpochHolders>::new()),
            clock: Arc::new(crate::clock::SystemClock),
            execution: Execution::Inline,
            state_dir: dir.join("state"),
            settings: ReplicationSettings::new(),
        };
        Replication::start(parts, |sink| {
            Registry::new(engine, sink, dir.join("databases"), EngineConfig::new())
        })
    }

    fn name(text: &str) -> DatabaseName {
        text.parse().expect("name")
    }

    fn role(epoch: u64) -> PrimaryRole {
        PrimaryRole::new(ReplicationFactor::ONE, LeaseEpoch::new(epoch))
    }

    async fn next(stream: &mut LeaseStream) -> Result<Update, Status> {
        let update = stream
            .next()
            .await
            .expect("the stream has an update")?
            .update
            .expect("an update has a kind");
        Ok(update)
    }

    #[tokio::test]
    async fn the_stream_starts_with_a_snapshot_and_follows_with_events_in_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let replication = replication(dir.path());
        replication
            .serve_primary(name("held"), role(3))
            .expect("serves");
        let mut stream = lease_stream("a".parse().expect("id"), replication.leases(), 8, |_| true);

        let Ok(Update::Snapshot(snapshot)) = next(&mut stream).await else {
            panic!("the first update is a snapshot");
        };
        assert_eq!(snapshot.node_id, "a");
        assert_eq!(
            snapshot
                .leases
                .iter()
                .map(|lease| (lease.database.as_str(), lease.epoch))
                .collect::<Vec<_>>(),
            [("held", 3)]
        );

        replication
            .serve_primary(name("gained"), role(1))
            .expect("serves");
        let Ok(Update::Event(event)) = next(&mut stream).await else {
            panic!("a gained lease is an event");
        };
        assert_eq!(
            (event.database.as_str(), event.epoch, event.kind),
            ("gained", 1, i32::from(LeaseEventKind::Gained))
        );

        // Serving at a new epoch ends the old one first.
        replication
            .serve_primary(name("held"), role(4))
            .expect("serves");
        let Ok(Update::Event(lost)) = next(&mut stream).await else {
            panic!("the old epoch is lost");
        };
        let Ok(Update::Event(gained)) = next(&mut stream).await else {
            panic!("the new epoch is gained");
        };
        assert_eq!(
            (lost.epoch, lost.kind, gained.epoch, gained.kind),
            (
                3,
                i32::from(LeaseEventKind::Lost),
                4,
                i32::from(LeaseEventKind::Gained)
            )
        );

        replication.stop_primary(&name("held"));
        let Ok(Update::Event(lost)) = next(&mut stream).await else {
            panic!("a stopped primary is lost");
        };
        assert_eq!(
            (lost.epoch, lost.kind),
            (4, i32::from(LeaseEventKind::Lost))
        );
        replication.stop();
    }

    #[tokio::test]
    async fn a_subscriber_that_falls_behind_is_cut_off_and_a_new_stream_starts_fresh() {
        let dir = tempfile::tempdir().expect("tempdir");
        let replication = replication(dir.path());
        let mut stream = lease_stream("a".parse().expect("id"), replication.leases(), 2, |_| true);
        let Ok(Update::Snapshot(snapshot)) = next(&mut stream).await else {
            panic!("the first update is a snapshot");
        };
        assert!(snapshot.leases.is_empty());

        // Each change is handed to the stream's task before the next one, and nothing reads
        // the stream meanwhile: the third event finds the buffer full.
        for epoch in 1..=4 {
            replication
                .serve_primary(name("db"), role(epoch))
                .expect("serves");
            tokio::task::yield_now().await;
        }
        let mut delivered = Vec::new();
        let ended = loop {
            match next(&mut stream).await {
                Ok(Update::Event(event)) => delivered.push((event.epoch, event.kind)),
                Ok(Update::Snapshot(_)) => panic!("only one snapshot per stream"),
                Err(status) => break status,
            }
        };
        assert_eq!(ended.code(), Code::ResourceExhausted, "{ended}");
        assert_eq!(
            delivered,
            [
                (1, i32::from(LeaseEventKind::Gained)),
                (1, i32::from(LeaseEventKind::Lost)),
            ]
        );
        assert!(
            stream.next().await.is_none(),
            "the stream ends after the error"
        );

        let mut fresh = lease_stream("a".parse().expect("id"), replication.leases(), 2, |_| true);
        let Ok(Update::Snapshot(snapshot)) = next(&mut fresh).await else {
            panic!("a new stream starts with a snapshot");
        };
        assert_eq!(
            snapshot
                .leases
                .iter()
                .map(|lease| lease.epoch)
                .collect::<Vec<_>>(),
            [4],
            "the snapshot holds the current epoch, with no stale event before it"
        );
        replication.stop();
    }

    #[tokio::test]
    async fn the_stream_ends_when_replication_stops_or_the_subscriber_leaves() {
        let dir = tempfile::tempdir().expect("tempdir");
        let replication = replication(dir.path());
        let mut stream = lease_stream("a".parse().expect("id"), replication.leases(), 2, |_| true);
        let Ok(Update::Snapshot(_)) = next(&mut stream).await else {
            panic!("the first update is a snapshot");
        };
        drop(replication);
        assert!(stream.next().await.is_none(), "replication is gone");
    }
}
