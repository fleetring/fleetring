//! The `Raft` gRPC service: a control node's end of its group's network (ADR-0035, ADR-0040).
//!
//! Every call is bound to `control peer`: the caller's node certificate must name a voter or
//! learner of this node's control group, as its membership stands, or, while this node belongs to
//! no group yet, any node of the fleet, so that a fresh joiner accepts its first leader. A call
//! that fails is refused with `PERMISSION_DENIED` and audited (ADR-0031). The service records the
//! caller's software and clock, decodes the payload through `codec` (`INVALID_ARGUMENT` if it
//! doesn't decode), reassembles snapshot chunks, and answers with its own software and clock.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use fleetring_control::State;
use fleetring_proto::v1::RaftPayload;
use fleetring_proto::v1::raft_server::Raft;
use openraft::errors::RaftError;
use openraft::rt::watch::WatchReceiver;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::{Request, Response, Status};

use crate::auth::{BindingDenied, Unbound};
use crate::clock::Clock;
use crate::control::ControlHandle;
use crate::control::format1::check_state;
use crate::control::raft::codec::{
    AppendEntries, AppendEntriesReply, AssembledSnapshot, InstallSnapshotChunk,
    InstallSnapshotReply, ProbeReply, SnapshotAssembly, VoteReply, VoteRequestWire, VoteWire,
};
use crate::control::raft::group::GroupSoftware;
use crate::control::raft::network::{ProbeRequest, heard, payload};
use crate::control::raft::proposer::ControlRaft;
use crate::control::raft::state_machine::ControlSnapshotOf;
use crate::control::raft::{RaftId, raft_id, raft_id_text};
use crate::tls::Peer;

/// The control node's `Raft` service.
#[derive(Debug, Clone)]
pub(crate) struct RaftService {
    handle: ControlHandle,
    group: GroupSoftware,
    clock: Arc<dyn Clock>,
    /// The snapshot each member is sending, by the member's Raft ID.
    assemblies: Arc<Mutex<BTreeMap<RaftId, SnapshotAssembly>>>,
}

impl RaftService {
    /// Returns the service of the control node behind `handle`, which records what members report
    /// in `group` and stamps replies with `clock`.
    pub(crate) fn new(handle: ControlHandle, group: GroupSoftware, clock: Arc<dyn Clock>) -> Self {
        Self {
            handle,
            group,
            clock,
            assemblies: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Checks `request`'s `control peer` binding, and records what its payload says of its
    /// sender. Returns the caller's Raft ID, if its connection names a node.
    fn admit(
        &self,
        request: &Request<RaftPayload>,
        method: &'static str,
    ) -> Result<Option<RaftId>, Status> {
        let caller = match Peer::of(request) {
            Peer::Node(node) => {
                let members = self.handle.group_peers();
                if !members.is_empty() && !members.iter().any(|peer| peer.node_id == node.as_str())
                {
                    return Err(
                        BindingDenied::new(method, node, Unbound::NotControlPeer).into_status()
                    );
                }
                Some(raft_id(node.as_str()))
            }
            // Only `insecure_plaintext` and the simulation run without certificates.
            Peer::Plaintext => None,
            Peer::Principal(name) => {
                return Err(Status::permission_denied(format!(
                    "{method} is for control nodes; '{name}' is a principal"
                )));
            }
        };
        if let Some(caller) = caller {
            heard(&self.group, self.clock.as_ref(), caller, request.get_ref());
        }
        Ok(caller)
    }

    fn raft(&self) -> Result<&ControlRaft, Status> {
        self.handle.raft_group().ok_or_else(|| {
            Status::unavailable(
                "this control node stopped at an entry it can't decode, and runs no control group",
            )
        })
    }

    fn reply(&self, value: &impl Serialize) -> Result<Response<RaftPayload>, Status> {
        let json = serde_json::to_vec(value)
            .map_err(|error| Status::internal(format!("can't encode the reply: {error}")))?;
        Ok(Response::new(payload(
            &self.group,
            self.clock.as_ref(),
            json,
        )))
    }
}

/// Decodes `request`'s payload as `T`.
fn decode<T: DeserializeOwned>(request: &RaftPayload, what: &str) -> Result<T, Status> {
    serde_json::from_slice(&request.json)
        .map_err(|error| Status::invalid_argument(format!("can't decode {what}: {error}")))
}

/// The status a stopped group answers with.
fn stopped<E: std::fmt::Display>(error: &E) -> Status {
    Status::unavailable(format!("the control group has stopped: {error}"))
}

/// Refuses a message whose leader or candidate isn't the node its caller's certificate names: a
/// member speaks only for itself. A caller without a certificate names nobody.
fn require_speaker(caller: Option<RaftId>, speaker: RaftId, method: &str) -> Result<(), Status> {
    match caller {
        Some(caller) if caller != speaker => Err(Status::permission_denied(format!(
            "{method} speaks for control node {}, not for the caller {}",
            raft_id_text(speaker),
            raft_id_text(caller)
        ))),
        Some(_) | None => Ok(()),
    }
}

impl RaftService {
    /// Checks a reassembled snapshot before openraft sees it, since one openraft can't install
    /// ends its core: its last entry is no later than the vote that sent it, and its data is a
    /// control state consistent with itself and, unless this node holds no log yet, with this
    /// node's genesis configuration.
    fn check_snapshot(&self, whole: &AssembledSnapshot) -> Result<(), Status> {
        if whole
            .meta
            .last_log_id
            .is_some_and(|log_id| log_id.leader_id.term > whole.vote.leader_id.term)
        {
            return Err(Status::invalid_argument(
                "the snapshot's last entry is later than the vote that sent it",
            ));
        }
        let state: State = serde_json::from_slice(&whole.data).map_err(|error| {
            Status::invalid_argument(format!("the snapshot's state doesn't decode: {error}"))
        })?;
        let genesis = if self.handle.holds_log() {
            *self.handle.state().config()
        } else {
            *state.config()
        };
        check_state(&state, &genesis).map_err(|reason| {
            Status::invalid_argument(format!("the snapshot's state is refused: {reason}"))
        })
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn append_entries(
        &self,
        request: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let caller = self.admit(&request, "AppendEntries")?;
        let wire: AppendEntries = decode(request.get_ref(), "AppendEntries")?;
        require_speaker(caller, wire.vote.node.0, "AppendEntries")?;
        let rpc = openraft::raft::AppendEntriesRequest::try_from(wire)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self.raft()?.append_entries(rpc).await {
            Ok(result) => result,
            Err(RaftError::Fatal(fatal)) => return Err(stopped(&fatal)),
            Err(RaftError::APIError(never)) => match never {},
        };
        self.reply(&AppendEntriesReply::from(&result))
    }

    async fn vote(&self, request: Request<RaftPayload>) -> Result<Response<RaftPayload>, Status> {
        let caller = self.admit(&request, "Vote")?;
        let wire: VoteRequestWire = decode(request.get_ref(), "Vote")?;
        require_speaker(caller, wire.vote.node.0, "Vote")?;
        let response = match self.raft()?.vote(wire.into()).await {
            Ok(response) => response,
            Err(RaftError::Fatal(fatal)) => return Err(stopped(&fatal)),
            Err(RaftError::APIError(never)) => match never {},
        };
        self.reply(&VoteReply::from(&response))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let caller = self.admit(&request, "InstallSnapshot")?;
        let chunk: InstallSnapshotChunk = decode(request.get_ref(), "InstallSnapshot")?;
        require_speaker(caller, chunk.vote.node.0, "InstallSnapshot")?;
        // Without a certificate, the sender is the vote's node.
        let sender = caller.unwrap_or(chunk.vote.node.0);
        let raft = self.raft()?;
        let assembled = {
            let mut assemblies = self
                .assemblies
                .lock()
                .expect("snapshot assembly lock poisoned");
            let assembly = assemblies.entry(sender).or_default();
            match assembly.push(chunk) {
                Ok(Some(whole)) => {
                    assemblies.remove(&sender);
                    Some(whole)
                }
                Ok(None) => None,
                Err(error) => {
                    assemblies.remove(&sender);
                    return Err(Status::invalid_argument(error.to_string()));
                }
            }
        };
        let Some(whole) = assembled else {
            // More chunks follow: answer with this node's vote, so a leader whose vote is behind
            // learns it before it sends the rest.
            let vote = raft.metrics().borrow_watched().vote;
            return self.reply(&InstallSnapshotReply {
                vote: VoteWire::from(&vote),
            });
        };
        self.check_snapshot(&whole)?;
        let snapshot = ControlSnapshotOf {
            meta: whole.meta,
            snapshot: whole.data,
        };
        let response = raft
            .install_full_snapshot(whole.vote, snapshot)
            .await
            .map_err(|fatal| stopped(&fatal))?;
        self.reply(&InstallSnapshotReply::from(&response))
    }

    async fn probe(&self, request: Request<RaftPayload>) -> Result<Response<RaftPayload>, Status> {
        self.admit(&request, "Probe")?;
        let _asked: ProbeRequest = decode(request.get_ref(), "Probe")?;
        let reply = ProbeReply {
            node_id: self.handle.own().node_id.clone(),
            software: self.group.own_software().clone(),
            has_raft_state: self.handle.has_raft_state(),
            genesis: Some(*self.handle.state().config()),
        };
        self.reply(&reply)
    }
}
