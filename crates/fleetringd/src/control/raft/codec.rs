//! FleetRing's encodings of every openraft value that is stored or sent (ADR-0040 point 3).
//!
//! openraft is built without `serde`, so its types reach neither `control.db` nor the wire:
//! each has a FleetRing struct here, converted with `From` or `TryFrom` both ways. Raft IDs are
//! 16 lowercase hex digits. No struct denies unknown fields, so a reader ignores fields a newer
//! release adds. Golden vectors in `tests/fixtures/raft-wire-v1/` pin every form; they are
//! append-only (ADR-0008).
//!
//! A log entry is stored as its `kind` and its `payload`:
//!
//! - `command`: `{"screened": {"term": <u64>, "node": "<hex>"}, "command": <Command>}`, where
//!   `<Command>` is byte for byte the JSON format 1 stores;
//! - `membership`: `{"configs": [["<hex>", …], …], "nodes": {"<hex>": {"node_id", "address"}}}`,
//!   one config, or two during joint consensus;
//! - `blank`: `{}`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use fleetring_control::Software;
use openraft::EntryPayload;
use openraft::impls::leader_id_adv::LeaderId;
use openraft::raft::{AppendError, AppendResult, ConflictHint, SnapshotResponse};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use crate::control::raft::id::{parse_raft_id, raft_id_text};
use crate::control::raft::{
    ControlPeer, ControlRequest, ControlTypes, Entry, LeaderRef, LogId, Membership, RaftId,
    SnapshotMeta, StoredMembership, Vote,
};

/// The most snapshot bytes one `InstallSnapshot` chunk carries: 1 MiB (ADR-0040 point 3).
pub const SNAPSHOT_CHUNK_BYTES: usize = 1 << 20;

/// The error returned when a stored or received value can't be decoded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The JSON doesn't hold the value expected.
    #[error("can't decode {what}")]
    Json {
        /// What was being decoded.
        what: &'static str,
        /// The decoder's error.
        source: serde_json::Error,
    },
    /// A Raft ID isn't 16 lowercase hex digits.
    #[error("raft id '{text}' is not 16 lowercase hex digits")]
    RaftId {
        /// The text found.
        text: String,
    },
    /// A membership names a voter it has no node for.
    #[error("membership names voter {} without its node", raft_id_text(*.voter))]
    Membership {
        /// The voter without a node.
        voter: RaftId,
    },
    /// An `InstallSnapshot` chunk doesn't continue the snapshot being received.
    #[error("snapshot chunk at offset {offset} doesn't follow the {received} bytes received")]
    Chunk {
        /// The chunk's offset.
        offset: u64,
        /// The bytes received so far.
        received: u64,
    },
}

impl CodecError {
    fn json(what: &'static str) -> impl FnOnce(serde_json::Error) -> Self {
        move |source| Self::Json { what, source }
    }
}

/// A Raft ID as text: 16 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HexId(pub RaftId);

impl Serialize for HexId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&raft_id_text(self.0))
    }
}

impl<'de> Deserialize<'de> for HexId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        parse_raft_id(&text)
            .map(Self)
            .map_err(|error| serde::de::Error::custom(error.to_string()))
    }
}

/// Parses a Raft ID as stored in a text column, such as `log.leader_node`.
///
/// # Errors
///
/// [`CodecError::RaftId`] unless `text` is 16 lowercase hex digits.
pub fn decode_raft_id(text: &str) -> Result<RaftId, CodecError> {
    parse_raft_id(text).map_err(|_| CodecError::RaftId { text: text.into() })
}

/// A log ID: the term and node of the leader that appended the entry, and its index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogIdWire {
    /// The control group's term.
    pub term: u64,
    /// The leader's Raft ID.
    pub node: HexId,
    /// The entry's index.
    pub index: u64,
}

impl From<&LogId> for LogIdWire {
    fn from(log_id: &LogId) -> Self {
        Self {
            term: log_id.leader_id.term,
            node: HexId(log_id.leader_id.node_id),
            index: log_id.index,
        }
    }
}

impl From<LogIdWire> for LogId {
    fn from(wire: LogIdWire) -> Self {
        LogId::new(leader(wire.term, wire.node.0), wire.index)
    }
}

fn leader(term: u64, node: RaftId) -> LeaderId<u64, RaftId> {
    LeaderId {
        term,
        node_id: node,
    }
}

fn wire_log_id(log_id: Option<&LogId>) -> Option<LogIdWire> {
    log_id.map(LogIdWire::from)
}

/// A leader: the control group's term and the leader's Raft ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderWire {
    /// The control group's term.
    pub term: u64,
    /// The leader's Raft ID.
    pub node: HexId,
}

impl From<LeaderRef> for LeaderWire {
    fn from(leader: LeaderRef) -> Self {
        Self {
            term: leader.term,
            node: HexId(leader.node),
        }
    }
}

impl From<LeaderWire> for LeaderRef {
    fn from(wire: LeaderWire) -> Self {
        Self {
            term: wire.term,
            node: wire.node.0,
        }
    }
}

/// A vote: the term and candidate or leader voted for, and whether a quorum granted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteWire {
    /// The control group's term.
    pub term: u64,
    /// The Raft ID voted for.
    pub node: HexId,
    /// Whether a quorum granted the vote.
    pub committed: bool,
}

impl From<&Vote> for VoteWire {
    fn from(vote: &Vote) -> Self {
        Self {
            term: vote.leader_id.term,
            node: HexId(vote.leader_id.node_id),
            committed: vote.committed,
        }
    }
}

impl From<VoteWire> for Vote {
    fn from(wire: VoteWire) -> Self {
        let mut vote = Vote::new(wire.term, wire.node.0);
        vote.committed = wire.committed;
        vote
    }
}

/// A control group member's peer: its fleet node ID and the address its peers dial.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerWire {
    /// The fleet node ID.
    pub node_id: String,
    /// The address peers dial.
    pub address: String,
}

impl From<&ControlPeer> for PeerWire {
    fn from(peer: &ControlPeer) -> Self {
        Self {
            node_id: peer.node_id.clone(),
            address: peer.address.clone(),
        }
    }
}

impl From<PeerWire> for ControlPeer {
    fn from(wire: PeerWire) -> Self {
        Self::new(wire.node_id, wire.address)
    }
}

/// A membership: its voter configs, one or two during joint consensus, and every member's peer;
/// a member in `nodes` and in no config is a learner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipWire {
    /// The voter configs.
    pub configs: Vec<Vec<HexId>>,
    /// Every member's peer, voters and learners.
    pub nodes: BTreeMap<HexId, PeerWire>,
}

impl From<&Membership> for MembershipWire {
    fn from(membership: &Membership) -> Self {
        Self {
            configs: membership
                .get_joint_config()
                .iter()
                .map(|config| config.iter().copied().map(HexId).collect())
                .collect(),
            nodes: membership
                .nodes()
                .map(|(id, peer)| (HexId(*id), PeerWire::from(peer)))
                .collect(),
        }
    }
}

impl TryFrom<MembershipWire> for Membership {
    type Error = CodecError;

    fn try_from(wire: MembershipWire) -> Result<Self, CodecError> {
        let configs: Vec<BTreeSet<RaftId>> = wire
            .configs
            .into_iter()
            .map(|config| config.into_iter().map(|id| id.0).collect())
            .collect();
        let nodes: BTreeMap<RaftId, ControlPeer> = wire
            .nodes
            .into_iter()
            .map(|(id, peer)| (id.0, ControlPeer::from(peer)))
            .collect();
        Membership::new(configs, nodes).map_err(|error| CodecError::Membership {
            voter: error.node_id,
        })
    }
}

/// A membership as a snapshot stores it: [`MembershipWire`]'s fields, and the log ID of the entry
/// that holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMembershipWire {
    /// The log ID of the entry that holds the membership; none before the first.
    pub log_id: Option<LogIdWire>,
    /// The membership.
    #[serde(flatten)]
    pub membership: MembershipWire,
}

impl From<&StoredMembership> for StoredMembershipWire {
    fn from(stored: &StoredMembership) -> Self {
        Self {
            log_id: wire_log_id(stored.log_id().as_ref()),
            membership: MembershipWire::from(stored.membership()),
        }
    }
}

impl TryFrom<StoredMembershipWire> for StoredMembership {
    type Error = CodecError;

    fn try_from(wire: StoredMembershipWire) -> Result<Self, CodecError> {
        Ok(Self::new(
            wire.log_id.map(LogId::from),
            Membership::try_from(wire.membership)?,
        ))
    }
}

/// What an entry holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    /// A control command.
    Command,
    /// A membership, openraft's own.
    Membership,
    /// A blank entry, which a new leader appends; openraft's own.
    Blank,
}

impl EntryKind {
    /// Returns the kind as `control.db` stores it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Membership => "membership",
            Self::Blank => "blank",
        }
    }

    /// Parses a kind as `control.db` stores it.
    #[must_use]
    pub fn from_stored(text: &str) -> Option<Self> {
        match text {
            "command" => Some(Self::Command),
            "membership" => Some(Self::Membership),
            "blank" => Some(Self::Blank),
            _ => None,
        }
    }
}

impl fmt::Display for EntryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A command entry's payload: the leader that screened the command, and the command's JSON as
/// format 1 stores it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandPayload {
    /// The leader the command was screened under.
    pub screened: LeaderWire,
    /// The command, byte for byte as format 1 stores it.
    pub command: Box<RawValue>,
}

/// A blank entry's payload, `{}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlankPayload {}

/// An entry's payload; its kind travels beside it.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum PayloadWire {
    /// A command entry's payload.
    Command(CommandPayload),
    /// A membership entry's payload.
    Membership(MembershipWire),
    /// A blank entry's payload.
    Blank(BlankPayload),
}

impl PayloadWire {
    /// Returns the payload's kind.
    #[must_use]
    pub const fn kind(&self) -> EntryKind {
        match self {
            Self::Command(_) => EntryKind::Command,
            Self::Membership(_) => EntryKind::Membership,
            Self::Blank(_) => EntryKind::Blank,
        }
    }

    /// Decodes a payload of `kind` from its JSON.
    ///
    /// # Errors
    ///
    /// [`CodecError::Json`] if the JSON isn't a payload of `kind`.
    pub fn decode(kind: EntryKind, json: &str) -> Result<Self, CodecError> {
        Ok(match kind {
            EntryKind::Command => Self::Command(
                serde_json::from_str(json)
                    .map_err(CodecError::json("a command entry's payload"))?,
            ),
            EntryKind::Membership => Self::Membership(
                serde_json::from_str(json)
                    .map_err(CodecError::json("a membership entry's payload"))?,
            ),
            EntryKind::Blank => Self::Blank(
                serde_json::from_str(json).map_err(CodecError::json("a blank entry's payload"))?,
            ),
        })
    }

    /// Returns the payload as compact JSON, as `control.db` stores it.
    ///
    /// # Errors
    ///
    /// [`CodecError::Json`] if it can't be encoded.
    pub fn to_json(&self) -> Result<String, CodecError> {
        serde_json::to_string(self).map_err(CodecError::json("an entry's payload"))
    }
}

impl From<&EntryPayload<ControlRequest, Membership>> for PayloadWire {
    fn from(payload: &EntryPayload<ControlRequest, Membership>) -> Self {
        match payload {
            EntryPayload::Normal(request) => Self::Command(CommandPayload {
                screened: request.screened.into(),
                command: request.command_json().to_owned(),
            }),
            EntryPayload::Membership(membership) => {
                Self::Membership(MembershipWire::from(membership))
            }
            EntryPayload::Blank => Self::Blank(BlankPayload {}),
        }
    }
}

impl TryFrom<PayloadWire> for EntryPayload<ControlRequest, Membership> {
    type Error = CodecError;

    fn try_from(wire: PayloadWire) -> Result<Self, CodecError> {
        Ok(match wire {
            PayloadWire::Command(payload) => Self::Normal(ControlRequest::from_json(
                payload.screened.into(),
                payload.command,
            )),
            PayloadWire::Membership(membership) => {
                Self::Membership(Membership::try_from(membership)?)
            }
            PayloadWire::Blank(BlankPayload {}) => Self::Blank,
        })
    }
}

/// A log entry: its log ID, its kind, and its payload. `control.db` stores the same fields in
/// the columns of `log`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "EntryRaw")]
pub struct EntryWire {
    /// The entry's index.
    pub index: u64,
    /// The control group's term when the entry was appended.
    pub term: u64,
    /// The Raft ID of the leader that appended it.
    pub leader_node: HexId,
    /// What the entry holds.
    pub kind: EntryKind,
    /// The payload, of `kind`.
    pub payload: PayloadWire,
}

/// An entry as read, before its payload is decoded by its kind.
#[derive(Deserialize)]
struct EntryRaw {
    index: u64,
    term: u64,
    leader_node: HexId,
    kind: EntryKind,
    payload: Box<RawValue>,
}

impl TryFrom<EntryRaw> for EntryWire {
    type Error = CodecError;

    fn try_from(raw: EntryRaw) -> Result<Self, CodecError> {
        Ok(Self {
            index: raw.index,
            term: raw.term,
            leader_node: raw.leader_node,
            kind: raw.kind,
            payload: PayloadWire::decode(raw.kind, raw.payload.get())?,
        })
    }
}

impl EntryWire {
    /// Returns the entry's log ID.
    #[must_use]
    pub const fn log_id(&self) -> LogIdWire {
        LogIdWire {
            term: self.term,
            node: self.leader_node,
            index: self.index,
        }
    }
}

impl From<&Entry> for EntryWire {
    fn from(entry: &Entry) -> Self {
        let payload = PayloadWire::from(&entry.payload);
        Self {
            index: entry.log_id.index,
            term: entry.log_id.leader_id.term,
            leader_node: HexId(entry.log_id.leader_id.node_id),
            kind: payload.kind(),
            payload,
        }
    }
}

impl EntryWire {
    /// Converts the entry to openraft's.
    ///
    /// An inherent method rather than `TryFrom`: `Entry` is an associated type of openraft's,
    /// for which coherence can't rule out core's blanket `TryFrom`.
    ///
    /// # Errors
    ///
    /// [`CodecError::Membership`] if a membership names a voter without its node.
    pub fn into_entry(self) -> Result<Entry, CodecError> {
        Ok(Entry {
            log_id: LogId::from(self.log_id()),
            payload: EntryPayload::try_from(self.payload)?,
        })
    }
}

/// A snapshot's metadata: the last log ID it includes and the membership it holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetaWire {
    /// The last log ID the snapshot includes; none for an empty one.
    pub last_log_id: Option<LogIdWire>,
    /// The membership the snapshot holds.
    pub last_membership: StoredMembershipWire,
}

impl From<&SnapshotMeta> for SnapshotMetaWire {
    fn from(meta: &SnapshotMeta) -> Self {
        Self {
            last_log_id: wire_log_id(meta.last_log_id.as_ref()),
            last_membership: StoredMembershipWire::from(&meta.last_membership),
        }
    }
}

impl TryFrom<SnapshotMetaWire> for SnapshotMeta {
    type Error = CodecError;

    fn try_from(wire: SnapshotMetaWire) -> Result<Self, CodecError> {
        Ok(SnapshotMeta {
            last_log_id: wire.last_log_id.map(LogId::from),
            last_membership: StoredMembership::try_from(wire.last_membership)?,
        })
    }
}

/// `AppendEntries`: the leader's vote, the log ID before the entries, the entries, and the
/// leader's committed log ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries {
    /// The leader's vote.
    pub vote: VoteWire,
    /// The log ID before the first entry.
    pub prev_log_id: Option<LogIdWire>,
    /// The entries, in log order.
    pub entries: Vec<EntryWire>,
    /// The leader's committed log ID.
    pub leader_commit: Option<LogIdWire>,
}

impl From<&openraft::raft::AppendEntriesRequest<ControlTypes>> for AppendEntries {
    fn from(request: &openraft::raft::AppendEntriesRequest<ControlTypes>) -> Self {
        Self {
            vote: VoteWire::from(&request.vote),
            prev_log_id: wire_log_id(request.prev_log_id.as_ref()),
            entries: request.entries.iter().map(EntryWire::from).collect(),
            leader_commit: wire_log_id(request.leader_commit.as_ref()),
        }
    }
}

impl TryFrom<AppendEntries> for openraft::raft::AppendEntriesRequest<ControlTypes> {
    type Error = CodecError;

    fn try_from(wire: AppendEntries) -> Result<Self, CodecError> {
        Ok(Self {
            vote: Vote::from(wire.vote),
            prev_log_id: wire.prev_log_id.map(LogId::from),
            entries: wire
                .entries
                .into_iter()
                .map(EntryWire::into_entry)
                .collect::<Result<_, _>>()?,
            leader_commit: wire.leader_commit.map(LogId::from),
        })
    }
}

/// The reply to `AppendEntries`.
///
/// A follower that appended only a prefix of the entries acknowledges the last one it appended:
/// a partial success is a `success` whose `acknowledged` is below the request's last entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum AppendEntriesReply {
    /// The entries up to `acknowledged` match the leader's log.
    Success {
        /// The last log ID acknowledged; none for an empty log.
        acknowledged: Option<LogIdWire>,
    },
    /// The follower's log doesn't hold the request's `prev_log_id`.
    Conflict {
        /// The request's `prev_log_id`.
        log_id: LogIdWire,
        /// Log IDs the follower holds, to help the leader find where their logs agree.
        hint: Option<Vec<LogIdWire>>,
    },
    /// The follower holds a higher vote than the leader's.
    HigherVote {
        /// The follower's vote.
        vote: VoteWire,
    },
}

impl From<&AppendResult<ControlTypes>> for AppendEntriesReply {
    fn from(result: &AppendResult<ControlTypes>) -> Self {
        match result {
            Ok(acknowledged) => Self::Success {
                acknowledged: wire_log_id(acknowledged.as_ref()),
            },
            Err(AppendError::Conflict { log_id, hint }) => Self::Conflict {
                log_id: LogIdWire::from(log_id),
                hint: hint
                    .as_ref()
                    .map(|hint| hint.log_ids.iter().map(LogIdWire::from).collect()),
            },
            Err(AppendError::HigherVote(vote)) => Self::HigherVote {
                vote: VoteWire::from(vote),
            },
        }
    }
}

impl From<AppendEntriesReply> for AppendResult<ControlTypes> {
    fn from(wire: AppendEntriesReply) -> Self {
        match wire {
            AppendEntriesReply::Success { acknowledged } => Ok(acknowledged.map(LogId::from)),
            AppendEntriesReply::Conflict { log_id, hint } => Err(AppendError::Conflict {
                log_id: LogId::from(log_id),
                hint: hint.map(|log_ids| {
                    ConflictHint::new(log_ids.into_iter().map(LogId::from).collect())
                }),
            }),
            AppendEntriesReply::HigherVote { vote } => {
                Err(AppendError::HigherVote(Vote::from(vote)))
            }
        }
    }
}

/// `Vote`: a candidate asks for a vote, or for a pre-vote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRequestWire {
    /// The vote the candidate asks for.
    pub vote: VoteWire,
    /// The candidate's last log ID.
    pub last_log_id: Option<LogIdWire>,
    /// The vote of the leader that hands leadership to the candidate, if one does.
    #[serde(default)]
    pub leadership_transfer_from: Option<VoteWire>,
}

impl From<&openraft::raft::VoteRequest<ControlTypes>> for VoteRequestWire {
    fn from(request: &openraft::raft::VoteRequest<ControlTypes>) -> Self {
        Self {
            vote: VoteWire::from(&request.vote),
            last_log_id: wire_log_id(request.last_log_id.as_ref()),
            leadership_transfer_from: request
                .leadership_transfer_from
                .as_ref()
                .map(VoteWire::from),
        }
    }
}

impl From<VoteRequestWire> for openraft::raft::VoteRequest<ControlTypes> {
    fn from(wire: VoteRequestWire) -> Self {
        Self {
            vote: Vote::from(wire.vote),
            last_log_id: wire.last_log_id.map(LogId::from),
            leadership_transfer_from: wire.leadership_transfer_from.map(Vote::from),
        }
    }
}

/// The reply to `Vote`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteReply {
    /// The voter's vote after the request.
    pub vote: VoteWire,
    /// Whether the voter granted the vote.
    pub vote_granted: bool,
    /// The voter's last log ID.
    pub last_log_id: Option<LogIdWire>,
}

impl From<&openraft::raft::VoteResponse<ControlTypes>> for VoteReply {
    fn from(response: &openraft::raft::VoteResponse<ControlTypes>) -> Self {
        Self {
            vote: VoteWire::from(&response.vote),
            vote_granted: response.vote_granted,
            last_log_id: wire_log_id(response.last_log_id.as_ref()),
        }
    }
}

impl From<VoteReply> for openraft::raft::VoteResponse<ControlTypes> {
    fn from(wire: VoteReply) -> Self {
        Self {
            vote: Vote::from(wire.vote),
            vote_granted: wire.vote_granted,
            last_log_id: wire.last_log_id.map(LogId::from),
        }
    }
}

/// One chunk of a whole snapshot: openraft hands the network the whole snapshot, which it sends
/// in chunks of at most [`SNAPSHOT_CHUNK_BYTES`]; the receiver reassembles them with
/// [`SnapshotAssembly`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotChunk {
    /// The leader's vote.
    pub vote: VoteWire,
    /// The snapshot's metadata.
    pub meta: SnapshotMetaWire,
    /// Where the chunk's bytes start in the snapshot's data.
    pub offset: u64,
    /// The chunk's bytes, in standard base64.
    pub data: String,
    /// Whether this is the last chunk.
    pub done: bool,
}

/// The reply to `InstallSnapshot`: the receiver's vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    /// The receiver's vote.
    pub vote: VoteWire,
}

impl From<&SnapshotResponse<ControlTypes>> for InstallSnapshotReply {
    fn from(response: &SnapshotResponse<ControlTypes>) -> Self {
        Self {
            vote: VoteWire::from(&response.vote),
        }
    }
}

impl From<InstallSnapshotReply> for SnapshotResponse<ControlTypes> {
    fn from(wire: InstallSnapshotReply) -> Self {
        Self::new(Vote::from(wire.vote))
    }
}

/// The reply to `Probe`: who the node is, what it runs, and whether its store holds Raft state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeReply {
    /// The node's fleet node ID.
    pub node_id: String,
    /// The node's software.
    pub software: Software,
    /// Whether the node's store holds a vote, a log, or a snapshot.
    pub has_raft_state: bool,
}

/// Splits a whole snapshot into `InstallSnapshot` chunks of at most `chunk_bytes` bytes each.
///
/// An empty snapshot is one empty chunk. A `chunk_bytes` of 0 is taken as 1.
#[must_use]
pub fn snapshot_chunks(
    vote: &Vote,
    meta: &SnapshotMeta,
    data: &[u8],
    chunk_bytes: usize,
) -> Vec<InstallSnapshotChunk> {
    let vote = VoteWire::from(vote);
    let meta = SnapshotMetaWire::from(meta);
    let pieces: Vec<&[u8]> = if data.is_empty() {
        vec![&[]]
    } else {
        data.chunks(chunk_bytes.max(1)).collect()
    };
    let last = pieces.len() - 1;
    let mut offset = 0_u64;
    pieces
        .into_iter()
        .enumerate()
        .map(|(position, piece)| {
            let chunk = InstallSnapshotChunk {
                vote,
                meta: meta.clone(),
                offset,
                data: BASE64.encode(piece),
                done: position == last,
            };
            offset += u64::try_from(piece.len()).unwrap_or(u64::MAX);
            chunk
        })
        .collect()
}

/// A snapshot whose chunks have all arrived.
#[derive(Debug, Clone)]
pub struct AssembledSnapshot {
    /// The vote of the leader that sent it.
    pub vote: Vote,
    /// The snapshot's metadata.
    pub meta: SnapshotMeta,
    /// The snapshot's data.
    pub data: Vec<u8>,
}

/// Reassembles a whole snapshot from its `InstallSnapshot` chunks, in order.
#[derive(Debug, Default)]
pub struct SnapshotAssembly {
    data: Vec<u8>,
}

impl SnapshotAssembly {
    /// Returns an empty assembly.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `chunk`, and returns the whole snapshot once its last chunk has arrived.
    ///
    /// # Errors
    ///
    /// [`CodecError::Chunk`] if the chunk doesn't start where the bytes received so far end,
    /// [`CodecError::Json`] if its data isn't base64, and any error decoding its metadata.
    pub fn push(
        &mut self,
        chunk: InstallSnapshotChunk,
    ) -> Result<Option<AssembledSnapshot>, CodecError> {
        let received = u64::try_from(self.data.len()).unwrap_or(u64::MAX);
        if chunk.offset != received {
            return Err(CodecError::Chunk {
                offset: chunk.offset,
                received,
            });
        }
        let bytes = BASE64
            .decode(chunk.data.as_bytes())
            .map_err(|error| CodecError::Json {
                what: "a snapshot chunk's data",
                source: serde::de::Error::custom(error),
            })?;
        self.data.extend_from_slice(&bytes);
        if !chunk.done {
            return Ok(None);
        }
        Ok(Some(AssembledSnapshot {
            vote: Vote::from(chunk.vote),
            meta: SnapshotMeta::try_from(chunk.meta)?,
            data: std::mem::take(&mut self.data),
        }))
    }
}

#[cfg(test)]
mod tests;
