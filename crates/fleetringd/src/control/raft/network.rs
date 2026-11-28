//! The control group's network: openraft's messages to the other members, as FleetRing's JSON in
//! `RaftPayload`s over the `Raft` gRPC service (ADR-0040 point 3).
//!
//! - Every request carries this node's software and its clock's reading, and every reply's are
//!   recorded in [`GroupSoftware`] under the member it came from (ADR-0040 points 4 and 6).
//! - openraft hands the network a whole snapshot, which goes as `InstallSnapshot` chunks of at
//!   most 1 MiB, in order.
//! - A dial failure, a call that outlasts openraft's deadline, or `UNAVAILABLE` is
//!   `Unreachable`, and the connection is forgotten, so the next call dials again. Any other
//!   status, or a reply that doesn't decode, is a network error.

use std::cmp::Ordering;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use fleetring_proto::v1::{RaftPayload, raft_client::RaftClient};
use openraft::OptionalSend;
use openraft::errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::transport::Channel;
use tonic::{Code, Status};

use crate::auth::{Binding, refused_binding};
use crate::clock::Clock;
use crate::control::raft::codec::{
    AppendEntries, AppendEntriesReply, InstallSnapshotReply, ProbeReply, SNAPSHOT_CHUNK_BYTES,
    TransferLeaderReply, TransferLeaderWire, VoteReply, VoteRequestWire, snapshot_chunks,
};
use crate::control::raft::group::GroupSoftware;
use crate::control::raft::state_machine::{ControlSnapshotOf, SnapshotData};
use crate::control::raft::{ControlPeer, ControlTypes, RaftId, Vote, raft_id_text};
use crate::environment::Network;
use crate::software::{software_from_proto, software_to_proto};
use crate::tls::Peer;
use crate::transport::raft_client;

/// How long one `InstallSnapshot` chunk, 1 MiB of snapshot, may take to send and answer: openraft's
/// `install_snapshot_timeout`, which this network applies per chunk. Ten seconds is about
/// 1 Mbit/s; the last chunk gets [`INSTALL_ROOM`] times as long, since its answer waits for the
/// receiver to install the whole snapshot.
pub(crate) const SNAPSHOT_CHUNK_TIMEOUT: Duration = Duration::from_secs(10);

/// How many chunk timeouts the last chunk of a snapshot gets.
const INSTALL_ROOM: u32 = 6;

/// What a `Probe` asks, which names nothing yet: the reply says who answers.
#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize)]
pub(crate) struct ProbeRequest {}

/// The control group's network factory: a connection per member, dialled over the node's
/// network with its identity.
#[derive(Debug, Clone)]
pub(crate) struct ControlNetwork {
    network: Network,
    group: GroupSoftware,
    clock: Arc<dyn Clock>,
}

/// Why a `Probe` failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProbeError {
    /// The node knows no `Raft` service: a release before the control group, whose boundary
    /// refuses a path its table doesn't list with `PERMISSION_DENIED`.
    #[error("the node knows no Raft service")]
    NoControlGroup,
    /// The node knows the `Raft` service but doesn't serve it: it doesn't run the control role.
    #[error("the node doesn't serve the Raft service")]
    NotControl,
    /// The node refused the call: it belongs to a control group this node isn't a member of.
    #[error("the node refused the probe: {0}")]
    Refused(String),
    /// The node can't be reached.
    #[error("the node can't be reached: {0}")]
    Unreachable(String),
    /// The node answered something this build can't read.
    #[error("the node's answer can't be read: {0}")]
    Invalid(String),
}

impl ControlNetwork {
    /// Returns the network that dials members over `network`, stamping each request with
    /// `clock` and the software `group` reports for this node, and recording what members
    /// report in `group`.
    pub(crate) fn new(network: Network, group: GroupSoftware, clock: Arc<dyn Clock>) -> Self {
        Self {
            network,
            group,
            clock,
        }
    }

    /// A payload holding `json`, with this node's software and clock.
    pub(crate) fn payload(&self, json: Vec<u8>) -> RaftPayload {
        payload(&self.group, self.clock.as_ref(), json)
    }

    /// Records what `reply`, from `from`, says of its sender.
    fn heard(&self, from: RaftId, reply: &RaftPayload) {
        heard(&self.group, self.clock.as_ref(), from, reply);
    }

    /// Asks the node at `address` what it would bring to a control group, waiting at most
    /// `limit`.
    ///
    /// # Errors
    ///
    /// [`ProbeError`] if the node has no `Raft` service, refuses, can't be reached, or answers
    /// something unreadable.
    pub(crate) async fn probe(
        &self,
        address: &str,
        limit: Duration,
    ) -> Result<ProbeReply, ProbeError> {
        let channel = self
            .network
            .channel(address)
            .map_err(|error| ProbeError::Unreachable(error.to_string()))?;
        let json = serde_json::to_vec(&ProbeRequest {})
            .map_err(|error| ProbeError::Invalid(error.to_string()))?;
        let mut client = raft_client(channel);
        let reply = match tokio::time::timeout(limit, client.probe(self.payload(json))).await {
            Err(_elapsed) => {
                return Err(ProbeError::Unreachable(format!(
                    "no answer within {} ms",
                    limit.as_millis()
                )));
            }
            Ok(Err(status)) => {
                return Err(match status.code() {
                    Code::Unimplemented => ProbeError::NotControl,
                    Code::PermissionDenied
                        if refused_binding(&status) == Some(Binding::ControlPeer.as_str()) =>
                    {
                        ProbeError::Refused(status.message().to_owned())
                    }
                    Code::PermissionDenied => ProbeError::NoControlGroup,
                    Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => {
                        ProbeError::Unreachable(status.message().to_owned())
                    }
                    _ => ProbeError::Invalid(status.message().to_owned()),
                });
            }
            Ok(Ok(reply)) => reply.into_inner(),
        };
        serde_json::from_slice(&reply.json).map_err(|error| ProbeError::Invalid(error.to_string()))
    }

    /// The node or principal the certificate of the server at `address` names, waiting at most
    /// `limit`; `None` in plaintext, where nothing names it. A probe's reply names the node only
    /// as the node says, so the leader checks the certificate as well.
    ///
    /// # Errors
    ///
    /// [`ProbeError::Unreachable`] if the server can't be reached or its handshake fails.
    pub(crate) async fn certified(
        &self,
        address: &str,
        limit: Duration,
    ) -> Result<Option<Peer>, ProbeError> {
        match tokio::time::timeout(limit, self.network.server_peer(address)).await {
            Err(_elapsed) => Err(ProbeError::Unreachable(format!(
                "no handshake within {} ms",
                limit.as_millis()
            ))),
            Ok(Err(error)) => Err(ProbeError::Unreachable(error.to_string())),
            Ok(Ok(peer)) => Ok(peer),
        }
    }
}

/// A payload holding `json`, with `group`'s software for this node and `clock`'s reading.
pub(crate) fn payload(group: &GroupSoftware, clock: &dyn Clock, json: Vec<u8>) -> RaftPayload {
    RaftPayload {
        json,
        sender: Some(software_to_proto(group.own_software())),
        sent_at_ms: clock.now().as_millis(),
    }
}

/// Records in `group` the software and clock a payload from the member `from` carries; a payload
/// without software, or with software this build can't read, records nothing.
pub(crate) fn heard(group: &GroupSoftware, clock: &dyn Clock, from: RaftId, payload: &RaftPayload) {
    let Some(sender) = payload.sender.clone() else {
        return;
    };
    match software_from_proto(sender) {
        Ok(software) => group.record(from, software, payload.sent_at_ms, clock.now().as_millis()),
        Err(status) => tracing::debug!(
            member = %raft_id_text(from),
            reason = status.message(),
            "a control peer's message carried software this node can't read",
        ),
    }
}

impl RaftNetworkFactory<ControlTypes> for ControlNetwork {
    type Network = PeerConnection;

    async fn new_client(&mut self, target: RaftId, node: &ControlPeer) -> Self::Network {
        PeerConnection {
            target,
            address: node.address.clone(),
            network: self.clone(),
            client: None,
        }
    }
}

/// Which `Raft` call a message goes on.
#[derive(Debug, Clone, Copy)]
enum Method {
    AppendEntries,
    Vote,
    InstallSnapshot,
    TransferLeader,
}

/// A connection to one member of the control group.
#[derive(Debug)]
pub(crate) struct PeerConnection {
    target: RaftId,
    address: String,
    network: ControlNetwork,
    /// The client, dialled lazily and forgotten when the member can't be reached.
    client: Option<RaftClient<Channel>>,
}

impl PeerConnection {
    fn unreachable(&mut self, why: &str) -> RPCError<ControlTypes> {
        self.client = None;
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
            "control peer {} at {} can't be reached: {why}",
            raft_id_text(self.target),
            self.address
        ))))
    }

    /// Sends `request` on `method`, waiting at most `limit`, and decodes the reply.
    async fn call<Request, Reply>(
        &mut self,
        method: Method,
        request: &Request,
        limit: Duration,
    ) -> Result<Reply, RPCError<ControlTypes>>
    where
        Request: Serialize,
        Reply: DeserializeOwned,
    {
        let json = serde_json::to_vec(request)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let payload = self.network.payload(json);
        let mut client = match self.client.take() {
            Some(client) => client,
            None => match self.network.network.channel(&self.address) {
                Ok(channel) => raft_client(channel),
                Err(error) => return Err(self.unreachable(&error.to_string())),
            },
        };
        let call = async {
            match method {
                Method::AppendEntries => client.append_entries(payload).await,
                Method::Vote => client.vote(payload).await,
                Method::InstallSnapshot => client.install_snapshot(payload).await,
                Method::TransferLeader => client.transfer_leader(payload).await,
            }
        };
        let answered = tokio::time::timeout(limit, call).await;
        let reply = match answered {
            Err(_elapsed) => {
                return Err(self.unreachable(&format!("no answer within {} ms", limit.as_millis())));
            }
            Ok(Err(status)) if unreachable(&status) => {
                return Err(self.unreachable(status.message()));
            }
            Ok(Err(status)) => {
                self.client = Some(client);
                return Err(RPCError::Network(NetworkError::from_string(format!(
                    "control peer {} answered {:?}: {}",
                    raft_id_text(self.target),
                    status.code(),
                    status.message()
                ))));
            }
            Ok(Ok(reply)) => reply.into_inner(),
        };
        self.client = Some(client);
        self.network.heard(self.target, &reply);
        serde_json::from_slice(&reply.json)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))
    }
}

/// Whether `status` says the member can't be reached now, rather than that it refused.
fn unreachable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown
    )
}

impl RaftNetworkV2<ControlTypes> for PeerConnection {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlTypes>,
        option: RPCOption,
    ) -> Result<AppendResult<ControlTypes>, RPCError<ControlTypes>> {
        let reply: AppendEntriesReply = self
            .call(
                Method::AppendEntries,
                &AppendEntries::from(&rpc),
                option.hard_ttl(),
            )
            .await?;
        Ok(AppendResult::from(reply))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<ControlTypes>, RPCError<ControlTypes>> {
        let reply: VoteReply = self
            .call(
                Method::Vote,
                &VoteRequestWire::from(&rpc),
                option.hard_ttl(),
            )
            .await?;
        Ok(VoteResponse::from(reply))
    }

    async fn transfer_leader(
        &mut self,
        rpc: TransferLeaderRequest<ControlTypes>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<ControlTypes>, RPCError<ControlTypes>> {
        let reply: TransferLeaderReply = self
            .call(
                Method::TransferLeader,
                &TransferLeaderWire::from(&rpc),
                option.hard_ttl(),
            )
            .await?;
        Ok(TransferLeaderResponse::from(reply))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: ControlSnapshotOf,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ControlTypes>, StreamingError<ControlTypes>> {
        let chunks = snapshot_chunks(
            &vote,
            &snapshot.meta,
            &snapshot.snapshot,
            SNAPSHOT_CHUNK_BYTES,
        );
        let limit = option.hard_ttl();
        let send = async {
            let mut answered = SnapshotResponse::new(vote);
            for chunk in chunks {
                let limit = if chunk.done {
                    limit.saturating_mul(INSTALL_ROOM)
                } else {
                    limit
                };
                let reply: InstallSnapshotReply =
                    self.call(Method::InstallSnapshot, &chunk, limit).await?;
                answered = SnapshotResponse::from(reply);
                // A member that holds a higher vote takes no more of it; openraft steps down
                // when it sees the vote.
                if answered.vote.partial_cmp(&vote) == Some(Ordering::Greater) {
                    break;
                }
            }
            Ok::<_, RPCError<ControlTypes>>(answered)
        };
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            sent = send => sent.map_err(StreamingError::from),
        }
    }
}
