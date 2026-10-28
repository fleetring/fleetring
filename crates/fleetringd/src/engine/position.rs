//! Engine-defined log positions and replication payloads, opaque above the adapter.

use std::fmt;

use fleetring::NodeId;

/// A place in a database's log, as opaque bytes that only the engine interprets (ADR-0016).
///
/// Engines encode positions so that comparing the bytes lexicographically gives log order, for
/// example as fixed-width big-endian integers. Everything above the adapter (quorums, leases,
/// control state, and the protocol) compares positions with [`Ord`] and never decodes them.
///
/// # Examples
///
/// ```
/// use fleetringd::engine::Position;
///
/// let earlier = Position::new([0, 0, 0, 7]);
/// let later = Position::new([0, 0, 1, 0]);
/// assert!(earlier < later);
/// ```
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position(Box<[u8]>);

impl Position {
    /// Returns the position encoded as `bytes`.
    #[must_use]
    pub fn new(bytes: impl Into<Box<[u8]>>) -> Self {
        Self(bytes.into())
    }

    /// The position's bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the position and returns its bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_vec()
    }
}

impl From<Vec<u8>> for Position {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes.into_boxed_slice())
    }
}

impl From<Position> for fleetring_control::Position {
    fn from(position: Position) -> Self {
        Self::new(position.into_bytes())
    }
}

impl From<fleetring_control::Position> for Position {
    fn from(position: fleetring_control::Position) -> Self {
        Self::new(position.as_bytes())
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Position({self})")
    }
}

/// A replication payload: opaque bytes that one engine produces and the same engine applies.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Payload(Vec<u8>);

impl Payload {
    /// Returns the payload holding `bytes`.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// The payload's bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the payload and returns its bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// The payload's length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the payload holds no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Payload({} bytes)", self.0.len())
    }
}

/// One or more committed transactions from a frame-source engine, ready to ship to followers.
///
/// A follower holding exactly [`start`](Self::start) applies the payload and is then at
/// [`end`](Self::end).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameBatch {
    /// The position the batch continues from.
    pub start: Position,
    /// The position just past the batch's last transaction.
    pub end: Position,
    /// The engine's encoding of the transactions.
    pub payload: Payload,
}

/// Where a frame-source primary checkpointed its log.
///
/// A follower checkpoints its own copy once it holds the log up to [`position`](Self::position)
/// (ADR-0016, point 7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMarker {
    /// The end of the log the checkpoint covered.
    pub position: Position,
}

/// A consistent copy of a database, and where its log continues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The log position the copy holds everything up to, or `None` for an empty log.
    pub position: Option<Position>,
}

/// How far one replica of a self-replicating engine has got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaPosition {
    /// The node holding the replica.
    pub node: NodeId,
    /// The last position the replica holds, or `None` if it holds nothing yet.
    pub position: Option<Position>,
}
