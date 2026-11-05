//! Drop notices (ADR-0042): the control node that dropped or expired a database tells each node
//! that may hold a copy of it, through that node's `Replication.DropCopy`, so that the node
//! deletes its copy.
//!
//! A data node never deletes a copy because control state doesn't know its database: an empty,
//! older, or foreign control state says the same. The notice is the positive signal instead:
//! only a control node that applied the drop sends one. The receiving node still checks with
//! control state before it deletes anything.
//!
//! Notices are best effort and kept nowhere. A node that is down or unreachable for every
//! attempt gets none, and keeps its copy as an orphan for an operator to remove; tombstones in
//! control state (#105) will let it learn of the drop later.

use std::sync::Arc;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, LeaseEpoch, State};
use fleetring_proto::v1::DropCopyRequest;
use tonic::Code;

use crate::data::unknown_call;
use crate::environment::Network;
use crate::transport::{Peers, replication_client};

/// How many times a node is told of one drop while it can't be reached.
const ATTEMPTS: u32 = 5;

/// How long one notice may take.
const TIMEOUT: Duration = Duration::from_secs(5);

/// How long the control node waits before telling an unreachable node again.
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Sends drop notices to data nodes. Cloning is cheap and shares the connections.
#[derive(Debug, Clone)]
pub(crate) struct DropNotices {
    peers: Arc<Peers>,
}

/// A node that may hold a copy of a dropped database, and where it listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Recipient {
    node: NodeId,
    address: String,
}

impl DropNotices {
    /// Notices that reach data nodes over `network`.
    pub(crate) fn new(network: Network) -> Self {
        Self {
            peers: Arc::new(Peers::with_network(network)),
        }
    }

    /// The members of `state`, the state before the drop, that may hold a copy of `database`:
    /// its replica set, and every other member that reported a copy, each with its address.
    pub(crate) fn recipients(state: &State, database: &DatabaseName) -> Vec<Recipient> {
        let replicas = state.replica_set(database).unwrap_or_default();
        state
            .members()
            .filter(|member| {
                replicas.contains(member.id()) || state.report(member.id(), database).is_some()
            })
            .filter_map(|member| {
                Some(Recipient {
                    node: member.id().clone(),
                    address: member.address()?.to_owned(),
                })
            })
            .collect()
    }

    /// Tells each of `recipients` that `database` was dropped at `epoch`, each in a task of its
    /// own, retrying a node that can't be reached a few times.
    pub(crate) fn send(
        &self,
        recipients: Vec<Recipient>,
        database: &DatabaseName,
        epoch: LeaseEpoch,
    ) {
        for recipient in recipients {
            let peers = Arc::clone(&self.peers);
            let request = DropCopyRequest {
                database: database.to_string(),
                epoch: epoch.get(),
            };
            tokio::spawn(async move {
                tell(&peers, &recipient, request).await;
            });
        }
    }
}

/// Tells `recipient` of the drop in `request`, up to [`ATTEMPTS`] times while it can't be
/// reached.
async fn tell(peers: &Peers, recipient: &Recipient, request: DropCopyRequest) {
    let Recipient { node, address } = recipient;
    for attempt in 1..=ATTEMPTS {
        if attempt > 1 {
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
        let Ok(channel) = peers.channel_to(address) else {
            continue;
        };
        let sent = tokio::time::timeout(
            TIMEOUT,
            replication_client(channel).drop_copy(request.clone()),
        )
        .await;
        match sent {
            Ok(Ok(_)) => {
                tracing::debug!(%node, database = %request.database, epoch = request.epoch, "told a replica of a drop");
                return;
            }
            Ok(Err(status)) if unknown_call(&status) => {
                tracing::debug!(%node, database = %request.database, "the replica runs a release without drop notices; it learns of the drop by itself");
                return;
            }
            Ok(Err(status))
                if matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded) =>
            {
                peers.forget(address);
            }
            Ok(Err(status)) => {
                tracing::info!(%node, database = %request.database, %status, "a replica refused a drop notice");
                return;
            }
            Err(_) => peers.forget(address),
        }
    }
    tracing::info!(%node, database = %request.database, epoch = request.epoch, "can't tell a replica of a drop; it keeps its copy as an orphan");
}
