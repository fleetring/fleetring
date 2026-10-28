//! A follower's `Replication` service behind a switchboard: a test can hold its
//! acknowledgements back, or cut its streams, and sees every acknowledgement it lets through.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use fleetring_proto::v1::replication_server::Replication as ReplicationRpc;
use fleetring_proto::v1::{
    FenceRequest, FenceResponse, ReplicateAck, ReplicateMessage, SnapshotRequest,
};
use fleetringd::engine::Position;
use fleetringd::replication::ReplicationService;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Something that happened, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event {
    /// A follower's acknowledgement reached the primary's side of the proxy.
    Acked { node: String, position: Position },
    /// A write returned to its client.
    Written,
}

/// The shared, ordered record of events.
#[derive(Debug, Clone, Default)]
pub(crate) struct Log(Arc<Mutex<Vec<Event>>>);

impl Log {
    pub(crate) fn push(&self, event: Event) {
        self.0.lock().expect("log lock").push(event);
    }

    pub(crate) fn events(&self) -> Vec<Event> {
        self.0.lock().expect("log lock").clone()
    }
}

/// The test's levers on one follower.
#[derive(Debug, Clone)]
pub(crate) struct Switches {
    /// While false, acknowledgements after a stream's first are held back.
    pub(crate) pass_acks: watch::Sender<bool>,
    /// While true, streams fail and new ones are refused.
    pub(crate) cut: watch::Sender<bool>,
}

impl Switches {
    pub(crate) fn new() -> Self {
        Self {
            pass_acks: watch::Sender::new(true),
            cut: watch::Sender::new(false),
        }
    }
}

/// The proxy around one follower's service.
#[derive(Debug, Clone)]
pub(crate) struct Proxy {
    pub(crate) node: String,
    pub(crate) inner: ReplicationService,
    pub(crate) switches: Switches,
    pub(crate) log: Log,
}

#[tonic::async_trait]
impl ReplicationRpc for Proxy {
    type ReplicateStream = BoxStream<ReplicateAck>;
    type SnapshotStream = <ReplicationService as ReplicationRpc>::SnapshotStream;

    async fn replicate(
        &self,
        request: Request<Streaming<ReplicateMessage>>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        if *self.switches.cut.borrow() {
            return Err(Status::unavailable("cut"));
        }
        let mut acks = self.inner.replicate(request).await?.into_inner();
        let mut pass = self.switches.pass_acks.subscribe();
        let mut cut = self.switches.cut.subscribe();
        let log = self.log.clone();
        let node = self.node.clone();
        let (sender, receiver) = mpsc::channel(64);
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let ack = tokio::select! {
                    () = until(&mut cut) => {
                        let _ = sender.send(Err(Status::unavailable("cut"))).await;
                        return;
                    }
                    ack = acks.next() => match ack {
                        Some(ack) => ack,
                        None => return,
                    },
                };
                if !first {
                    tokio::select! {
                        () = until(&mut cut) => {
                            let _ = sender.send(Err(Status::unavailable("cut"))).await;
                            return;
                        }
                        () = until(&mut pass) => {}
                    }
                }
                first = false;
                if let Ok(ack) = &ack {
                    log.push(Event::Acked {
                        node: node.clone(),
                        position: Position::from(ack.position.clone()),
                    });
                }
                if sender.send(ack).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        self.inner.fence(request).await
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        self.inner.snapshot(request).await
    }
}

/// Waits until `switch` is on.
async fn until(switch: &mut watch::Receiver<bool>) {
    // A dropped sender leaves the switch where it was, so wait forever.
    if switch.wait_for(|on| *on).await.is_err() {
        std::future::pending::<()>().await;
    }
}
