//! Databases in control state: names, engines, log positions, epochs, leases, and promotions.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use fleetring::{NodeId, ReplicationFactor};
use serde::{Deserialize, Serialize};

use crate::Timestamp;

/// The longest database name, in bytes.
const MAX_NAME_LEN: usize = 255;

/// The name of a database: its identity in control state and its placement key on the ring.
///
/// A name is 1 to 255 bytes of UTF-8 without control characters.
///
/// # Examples
///
/// ```
/// use fleetring_control::DatabaseName;
///
/// let name: DatabaseName = "tenant:acme".parse()?;
/// assert_eq!(name.as_str(), "tenant:acme");
/// assert!("".parse::<DatabaseName>().is_err());
/// # Ok::<(), fleetring_control::ParseDatabaseNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DatabaseName(Box<str>);

impl DatabaseName {
    /// Returns the name as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DatabaseName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for DatabaseName {
    type Err = ParseDatabaseNameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::try_from(name.to_owned())
    }
}

impl TryFrom<&str> for DatabaseName {
    type Error = ParseDatabaseNameError;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        name.parse()
    }
}

impl TryFrom<String> for DatabaseName {
    type Error = ParseDatabaseNameError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.is_empty() {
            return Err(ParseDatabaseNameError::Empty);
        }
        if name.len() > MAX_NAME_LEN {
            return Err(ParseDatabaseNameError::TooLong {
                len: name.len(),
                max: MAX_NAME_LEN,
            });
        }
        if name.chars().any(char::is_control) {
            return Err(ParseDatabaseNameError::ControlCharacter);
        }
        Ok(Self(name.into_boxed_str()))
    }
}

impl From<DatabaseName> for String {
    fn from(name: DatabaseName) -> Self {
        name.0.into_string()
    }
}

impl AsRef<str> for DatabaseName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Why a string is not a valid [`DatabaseName`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseDatabaseNameError {
    /// The name is empty.
    #[error("database name is empty")]
    Empty,

    /// The name is longer than the maximum.
    #[error("database name is {len} bytes; the maximum is {max}")]
    TooLong {
        /// The name's length in bytes.
        len: usize,
        /// The maximum length in bytes.
        max: usize,
    },

    /// The name contains a control character.
    #[error("database name contains a control character")]
    ControlCharacter,
}

/// The storage engine a database runs on (ADR-0021).
///
/// Every database records its engine so that failover and routing can read it and so that a
/// later migration can change it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Engine {
    /// SQLite, replicated by shipping WAL frames (ADR-0016).
    Sqlite,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sqlite => "sqlite",
        })
    }
}

/// A log position, as opaque bytes defined by the database's engine (ADR-0016).
///
/// Engines encode positions so that comparing the bytes lexicographically gives log order, for
/// example as fixed-width big-endian integers. Control state compares positions only that way and
/// never interprets them.
///
/// # Examples
///
/// ```
/// use fleetring_control::Position;
///
/// let earlier = Position::new([0, 0, 0, 7]);
/// let later = Position::new([0, 0, 1, 0]);
/// assert!(earlier < later);
/// assert_eq!(later.as_bytes(), &[0, 0, 1, 0]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Position(Vec<u8>);

impl Position {
    /// Returns the position encoded as `bytes`.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the position's bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Position {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

/// A database's lease epoch: the fencing token that names one primary (ADR-0018).
///
/// Epoch 0 means no lease has ever been issued. Each new holder gets a higher epoch than any
/// issued before it, and followers refuse batches from lower epochs.
///
/// # Examples
///
/// ```
/// use fleetring_control::LeaseEpoch;
///
/// assert_eq!(LeaseEpoch::default(), LeaseEpoch::ZERO);
/// assert!(LeaseEpoch::new(2) > LeaseEpoch::new(1));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseEpoch(u64);

impl LeaseEpoch {
    /// The epoch before any lease is issued.
    pub const ZERO: Self = Self(0);

    /// Returns the epoch numbered `value`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the epoch as an integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next epoch, or `None` if the counter is exhausted.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for LeaseEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The right of one node to accept writes for a database at one epoch, until an expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    holder: NodeId,
    epoch: LeaseEpoch,
    expires_at: Timestamp,
}

impl Lease {
    pub(crate) fn new(holder: NodeId, epoch: LeaseEpoch, expires_at: Timestamp) -> Self {
        Self {
            holder,
            epoch,
            expires_at,
        }
    }

    /// Returns the node that holds the lease.
    #[must_use]
    pub fn holder(&self) -> &NodeId {
        &self.holder
    }

    /// Returns the lease's epoch.
    #[must_use]
    pub fn epoch(&self) -> LeaseEpoch {
        self.epoch
    }

    /// Returns when the lease expires unless it is renewed.
    #[must_use]
    pub fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) fn set_expires_at(&mut self, expires_at: Timestamp) {
        self.expires_at = expires_at;
    }
}

/// A failover in progress: a candidate, the new epoch, and the members fenced at it so far.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Promotion {
    epoch: LeaseEpoch,
    candidate: NodeId,
    started_at: Timestamp,
    fenced: BTreeMap<NodeId, Position>,
}

impl Promotion {
    pub(crate) fn new(epoch: LeaseEpoch, candidate: NodeId, started_at: Timestamp) -> Self {
        Self {
            epoch,
            candidate,
            started_at,
            fenced: BTreeMap::new(),
        }
    }

    /// Returns the epoch the candidate will hold the lease at.
    #[must_use]
    pub fn epoch(&self) -> LeaseEpoch {
        self.epoch
    }

    /// Returns the node being promoted.
    #[must_use]
    pub fn candidate(&self) -> &NodeId {
        &self.candidate
    }

    /// Returns when the promotion started.
    #[must_use]
    pub fn started_at(&self) -> Timestamp {
        self.started_at
    }

    /// Returns the members fenced at the new epoch, with the positions they reported, in ID
    /// order.
    pub fn fenced(&self) -> impl ExactSizeIterator<Item = (&NodeId, &Position)> {
        self.fenced.iter()
    }

    /// Returns the position `id` reported when it was fenced, or `None` if it isn't fenced yet.
    #[must_use]
    pub fn fenced_position(&self, id: &NodeId) -> Option<&Position> {
        self.fenced.get(id)
    }

    pub(crate) fn record_fenced(&mut self, id: NodeId, position: Position) {
        self.fenced.insert(id, position);
    }
}

/// A database in control state: what it is, and who may write to it.
///
/// Where the database should live is not stored: the ring computes it from the membership
/// (ADR-0017, point 6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Database {
    name: DatabaseName,
    replication_factor: ReplicationFactor,
    engine: Engine,
    epoch: LeaseEpoch,
    lease: Option<Lease>,
    promotion: Option<Promotion>,
}

impl Database {
    pub(crate) fn new(
        name: DatabaseName,
        replication_factor: ReplicationFactor,
        engine: Engine,
    ) -> Self {
        Self {
            name,
            replication_factor,
            engine,
            epoch: LeaseEpoch::ZERO,
            lease: None,
            promotion: None,
        }
    }

    /// Returns the database's name.
    #[must_use]
    pub fn name(&self) -> &DatabaseName {
        &self.name
    }

    /// Returns how many replicas the database should have.
    #[must_use]
    pub fn replication_factor(&self) -> ReplicationFactor {
        self.replication_factor
    }

    /// Returns the engine the database runs on.
    #[must_use]
    pub fn engine(&self) -> Engine {
        self.engine
    }

    /// Returns the highest epoch issued for the database, by a lease or a started promotion.
    #[must_use]
    pub fn epoch(&self) -> LeaseEpoch {
        self.epoch
    }

    /// Returns the current lease, or `None` if none has been granted yet.
    ///
    /// During a promotion this is still the old lease; it is replaced when the promotion
    /// completes.
    #[must_use]
    pub fn lease(&self) -> Option<&Lease> {
        self.lease.as_ref()
    }

    /// Returns the promotion in progress, if any.
    #[must_use]
    pub fn promotion(&self) -> Option<&Promotion> {
        self.promotion.as_ref()
    }

    pub(crate) fn set_epoch(&mut self, epoch: LeaseEpoch) {
        self.epoch = epoch;
    }

    pub(crate) fn lease_mut(&mut self) -> Option<&mut Lease> {
        self.lease.as_mut()
    }

    pub(crate) fn set_lease(&mut self, lease: Lease) {
        self.lease = Some(lease);
    }

    pub(crate) fn promotion_mut(&mut self) -> Option<&mut Promotion> {
        self.promotion.as_mut()
    }

    pub(crate) fn set_promotion(&mut self, promotion: Option<Promotion>) {
        self.promotion = promotion;
    }
}

/// A replica set's size and its write and promotion quorums (ADR-0018).
///
/// R is the replica set's size, W = ⌊R/2⌋ + 1, and Q = R − W + 1, so W + Q > R: every write
/// acknowledged by W members reaches at least one of any Q fenced members.
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
/// use fleetring_control::Quorums;
///
/// let three = Quorums::new(ReplicationFactor::new(3).expect("3 is nonzero"));
/// assert_eq!((three.replicas(), three.write(), three.promotion()), (3, 2, 2));
/// let two = Quorums::new(ReplicationFactor::new(2).expect("2 is nonzero"));
/// assert_eq!((two.write(), two.promotion()), (2, 1));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quorums {
    replicas: u8,
    write: u8,
    promotion: u8,
}

impl Quorums {
    /// Returns the quorums for a replica set of `size` members.
    #[must_use]
    pub const fn new(size: ReplicationFactor) -> Self {
        let replicas = size.get();
        let write = replicas / 2 + 1;
        Self {
            replicas,
            write,
            promotion: replicas - write + 1,
        }
    }

    /// Returns R, the replica set's size.
    #[must_use]
    pub const fn replicas(&self) -> u8 {
        self.replicas
    }

    /// Returns W, the number of members, the primary included, that must store a write before
    /// it is acknowledged.
    #[must_use]
    pub const fn write(&self) -> u8 {
        self.write
    }

    /// Returns Q, the number of members, the candidate included, that must be fenced before a
    /// promotion completes.
    #[must_use]
    pub const fn promotion(&self) -> u8 {
        self.promotion
    }
}
