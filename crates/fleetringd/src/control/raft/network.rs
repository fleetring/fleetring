//! The control group's network: for now, none. A one-voter group never sends a Raft message, so
//! every peer is unreachable until the `Raft` service carries them.

use std::future::Future;
use std::io;

use openraft::OptionalSend;
use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};

use crate::control::raft::state_machine::{ControlSnapshotOf, SnapshotData};
use crate::control::raft::{ControlPeer, ControlTypes, RaftId, Vote};
use crate::environment::Network;

/// The network factory of a control group whose peers can't be reached: every call is answered
/// `Unreachable`.
#[derive(Debug, Clone)]
pub(crate) struct NoPeers {
    /// The network the `Raft` service will dial peers over.
    #[expect(dead_code, reason = "the Raft service dials peers over it from PR 4")]
    network: Network,
}

impl NoPeers {
    /// Returns the factory for a node whose peers will be dialled over `network`.
    pub(crate) fn new(network: Network) -> Self {
        Self { network }
    }
}

/// A connection to a peer that can't be reached.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Unconnected {
    target: RaftId,
}

impl Unconnected {
    fn unreachable(self) -> Unreachable<ControlTypes> {
        Unreachable::new(&io::Error::other(format!(
            "control peer {} can't be reached: this build carries no Raft messages",
            crate::control::raft::raft_id_text(self.target)
        )))
    }
}

impl RaftNetworkFactory<ControlTypes> for NoPeers {
    type Network = Unconnected;

    async fn new_client(&mut self, target: RaftId, _node: &ControlPeer) -> Self::Network {
        Unconnected { target }
    }
}

impl RaftNetworkV2<ControlTypes> for Unconnected {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<ControlTypes>,
        _option: RPCOption,
    ) -> Result<AppendResult<ControlTypes>, RPCError<ControlTypes>> {
        Err(RPCError::Unreachable(self.unreachable()))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<ControlTypes>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlTypes>, RPCError<ControlTypes>> {
        Err(RPCError::Unreachable(self.unreachable()))
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote,
        _snapshot: ControlSnapshotOf,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<ControlTypes>, StreamingError<ControlTypes>> {
        Err(StreamingError::Unreachable(self.unreachable()))
    }
}
