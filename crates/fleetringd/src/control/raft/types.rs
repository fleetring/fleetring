//! The values the control group's Raft carries: its peers, its requests, and its responses.

use std::fmt;
use std::sync::Arc;

use fleetring_control::{Command, Outcome, State};
use serde_json::value::RawValue;

use crate::control::raft::RaftId;
use crate::control::raft::id::raft_id_text;

/// A member of the control group as openraft knows it: its fleet node ID and the address its
/// peers dial.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControlPeer {
    /// The fleet node ID; its XXH3-64 hash is the member's Raft ID.
    pub node_id: String,
    /// The address peers dial, `host:port`.
    pub address: String,
}

impl ControlPeer {
    /// Returns the peer `node_id` at `address`.
    #[must_use]
    pub fn new(node_id: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            address: address.into(),
        }
    }
}

impl fmt::Display for ControlPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.node_id, self.address)
    }
}

/// The leader a command was screened under: the control group's term and the leader's Raft ID
/// (ADR-0040 point 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaderRef {
    /// The control group's term.
    pub term: u64,
    /// The leader's Raft ID.
    pub node: RaftId,
}

impl fmt::Display for LeaderRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.term, raft_id_text(self.node))
    }
}

/// A command proposed to the control group: the command as JSON, stored and sent exactly as
/// format 1 stores it, and the leader that screened it.
///
/// The proposer builds it with [`ControlRequest::new`] and keeps the decoded command beside the
/// JSON; a request read back from the log or the wire holds only the JSON, so a member never
/// decodes a command it won't apply.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ControlRequest {
    /// The leader the command was screened under.
    pub screened: LeaderRef,
    command: Box<RawValue>,
    decoded: Option<Arc<Command>>,
}

impl ControlRequest {
    /// Returns a request for `command`, screened under `screened`.
    ///
    /// # Errors
    ///
    /// The encoder's error if `command` can't be encoded as JSON.
    pub fn new(screened: LeaderRef, command: Command) -> Result<Self, serde_json::Error> {
        let text = serde_json::to_string(&command)?;
        Ok(Self {
            screened,
            command: RawValue::from_string(text)?,
            decoded: Some(Arc::new(command)),
        })
    }

    /// Returns a request holding `command`'s JSON as it was stored or sent, undecoded.
    #[must_use]
    pub fn from_json(screened: LeaderRef, command: Box<RawValue>) -> Self {
        Self {
            screened,
            command,
            decoded: None,
        }
    }

    /// Returns the command's JSON, byte for byte as format 1 stores it.
    #[must_use]
    pub fn command_json(&self) -> &RawValue {
        &self.command
    }

    /// Returns the command the proposer decoded, if this request was built from one.
    #[must_use]
    pub fn command(&self) -> Option<&Command> {
        self.decoded.as_deref()
    }

    /// Decodes the command from its JSON.
    ///
    /// # Errors
    ///
    /// The decoder's error if the JSON isn't a command this build reads.
    pub fn decode(&self) -> Result<Command, serde_json::Error> {
        serde_json::from_str(self.command.get())
    }
}

/// Two requests are equal when they were screened under the same leader and hold the same
/// JSON; whether a decoded command travels beside it doesn't count.
impl PartialEq for ControlRequest {
    fn eq(&self, other: &Self) -> bool {
        self.screened == other.screened && self.command.get() == other.command.get()
    }
}

impl Eq for ControlRequest {}

impl fmt::Display for ControlRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "command screened under {}", self.screened)
    }
}

/// What the control group's state machine answers for an entry.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ControlResponse {
    /// The command was applied.
    Applied {
        /// What `apply` decided.
        outcome: Outcome,
        /// The state after the command.
        state: Arc<State>,
    },
    /// The command was skipped: it was screened under another leader than the one that
    /// appended it, or its `recorded_by` isn't that leader. Nothing changed.
    Superseded,
    /// A blank or membership entry, which is openraft's own.
    Raft,
}
