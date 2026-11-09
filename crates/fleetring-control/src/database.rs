//! Databases in control state: names, engines, log positions, epochs, leases, and promotions.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use fleetring::{NodeId, ReplicationFactor};
use serde::{Deserialize, Serialize};

use crate::fork::Fork;
use crate::{Metadata, Timestamp};

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

impl Engine {
    /// Returns whether a database on this engine can be forked: its adapter is a frame source
    /// that pins snapshots and runs the conditional fork reset (ADR-0046, point 9).
    #[must_use]
    pub const fn can_fork(self) -> bool {
        match self {
            Self::Sqlite => true,
        }
    }
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sqlite => "sqlite",
        })
    }
}

/// How many replicas must store a write before it is acknowledged (ADR-0023).
///
/// The mode fixes W, the write quorum, and Q, the promotion quorum, for a replica set of R
/// members; see [`Quorums::with_acks`].
///
/// # Examples
///
/// ```
/// use fleetring_control::Acks;
///
/// let acks: Acks = "all".parse()?;
/// assert_eq!(acks, Acks::All);
/// assert_eq!(acks.to_string(), "all");
/// assert_eq!(Acks::default(), Acks::Quorum);
/// # Ok::<(), fleetring_control::ParseAcksError>(())
/// ```
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Acks {
    /// A majority: W = ⌊R/2⌋ + 1 and Q = R − W + 1. No acknowledged write is lost while a
    /// majority survives (ADR-0018).
    #[default]
    Quorum,
    /// Every replica: W = R and Q = 1. Any one replica down blocks writes.
    All,
    /// The primary alone: W = 1, acknowledged after its own fsync. A promotion fences a majority
    /// and adopts the most advanced log among them, so a failover may lose acknowledged writes.
    Local,
}

impl Acks {
    /// Returns whether a write acknowledged in this mode survives any failover that ADR-0018
    /// tolerates: `false` only for [`Acks::Local`].
    #[must_use]
    pub const fn acknowledged_writes_survive(self) -> bool {
        !matches!(self, Self::Local)
    }

    /// Returns the mode's name, as [`Display`](fmt::Display) and [`FromStr`] use it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quorum => "quorum",
            Self::All => "all",
            Self::Local => "local",
        }
    }
}

impl fmt::Display for Acks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Acks {
    type Err = ParseAcksError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "quorum" => Ok(Self::Quorum),
            "all" => Ok(Self::All),
            "local" => Ok(Self::Local),
            other => Err(ParseAcksError {
                text: other.to_owned(),
            }),
        }
    }
}

/// Why a string is not an [`Acks`] mode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown acks mode '{text}': expected quorum, all, or local")]
pub struct ParseAcksError {
    text: String,
}

/// A database's durability settings: its [`Acks`] mode and its floor on live replicas
/// (ADR-0023).
///
/// `min_replicas` counts the primary. A write that arrives while fewer replicas are reachable is
/// refused before anything commits. It must satisfy W ≤ `min_replicas` ≤ R, where W is the
/// mode's write quorum for R replicas; the default is W.
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
/// use fleetring_control::{Acks, Durability};
///
/// let three = ReplicationFactor::new(3).ok_or("zero")?;
/// let quorum = Durability::new(Acks::Quorum, three);
/// assert_eq!(quorum.min_replicas(), 2);
/// let strict = Durability::with_min_replicas(Acks::Quorum, 3, three)?;
/// assert_eq!(strict.min_replicas(), 3);
/// assert!(Durability::with_min_replicas(Acks::All, 2, three).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Durability {
    acks: Acks,
    min_replicas: u8,
}

impl Durability {
    /// The settings for `acks` on a database with `replication_factor` replicas, with the floor
    /// at W.
    #[must_use]
    pub const fn new(acks: Acks, replication_factor: ReplicationFactor) -> Self {
        Self {
            acks,
            min_replicas: Quorums::with_acks(replication_factor, acks).write(),
        }
    }

    /// The default settings: [`Acks::Quorum`] with the floor at W.
    #[must_use]
    pub const fn quorum(replication_factor: ReplicationFactor) -> Self {
        Self::new(Acks::Quorum, replication_factor)
    }

    /// The settings for `acks` with the floor at `min_replicas`.
    ///
    /// # Errors
    ///
    /// [`DurabilityError`] if `min_replicas` is below W or above `replication_factor`.
    pub fn with_min_replicas(
        acks: Acks,
        min_replicas: u8,
        replication_factor: ReplicationFactor,
    ) -> Result<Self, DurabilityError> {
        let durability = Self { acks, min_replicas };
        durability.check(replication_factor)?;
        Ok(durability)
    }

    /// Returns the acknowledgement mode.
    #[must_use]
    pub const fn acks(&self) -> Acks {
        self.acks
    }

    /// Returns how many replicas, the primary included, must be reachable for a write to be
    /// accepted.
    #[must_use]
    pub const fn min_replicas(&self) -> u8 {
        self.min_replicas
    }

    /// Returns the quorums for a replica set of `size` members in this mode.
    #[must_use]
    pub const fn quorums(&self, size: ReplicationFactor) -> Quorums {
        Quorums::with_acks(size, self.acks)
    }

    /// Checks that the floor fits a database with `replication_factor` replicas.
    ///
    /// # Errors
    ///
    /// [`DurabilityError`] if `min_replicas` is below W or above `replication_factor`.
    pub fn check(&self, replication_factor: ReplicationFactor) -> Result<(), DurabilityError> {
        let write = self.quorums(replication_factor).write();
        if self.min_replicas < write {
            return Err(DurabilityError::BelowWriteQuorum {
                min_replicas: self.min_replicas,
                write,
            });
        }
        if self.min_replicas > replication_factor.get() {
            return Err(DurabilityError::AboveReplicationFactor {
                min_replicas: self.min_replicas,
                replication_factor: replication_factor.get(),
            });
        }
        Ok(())
    }
}

impl fmt::Display for Durability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "acks={} min_replicas={}", self.acks, self.min_replicas)
    }
}

/// Why a [`Durability`] doesn't fit a database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DurabilityError {
    /// The floor is below the write quorum, so a write could be acknowledged by replicas it was
    /// not allowed to wait for.
    #[error("min_replicas {min_replicas} is below the write quorum {write}")]
    BelowWriteQuorum {
        /// The floor asked for.
        min_replicas: u8,
        /// W for the mode and the replication factor.
        write: u8,
    },
    /// The floor is above the replication factor, so no write could ever be accepted.
    #[error("min_replicas {min_replicas} is above the replication factor {replication_factor}")]
    AboveReplicationFactor {
        /// The floor asked for.
        min_replicas: u8,
        /// R.
        replication_factor: u8,
    },
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

/// A database's incarnation: which life of its name a database is (ADR-0046, point 1).
///
/// The control node that proposes a creation draws its incarnation as 64 random bits and carries
/// it in the command, as it carries the time, so `apply` stays pure and a replay gives the same
/// incarnations. No rule rests on incarnations growing or never repeating: fencing compares a
/// fork's whole identity. Incarnations are stored once the `fork` fleet feature is enabled; every
/// database created before then, and every record written before incarnations existed, has
/// [`Incarnation::LEGACY`], 0. Every fork has a nonzero incarnation.
///
/// Text and JSON write an incarnation as 16 lowercase hex digits, since a 64-bit integer doesn't
/// survive every JSON reader.
///
/// # Examples
///
/// ```
/// use fleetring_control::Incarnation;
///
/// assert_eq!(Incarnation::default(), Incarnation::LEGACY);
/// assert_eq!(Incarnation::new(0xAB).to_string(), "00000000000000ab");
/// assert_eq!("00000000000000ab".parse(), Ok(Incarnation::new(0xAB)));
/// assert!("ab".parse::<Incarnation>().is_err());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Incarnation(u64);

impl Incarnation {
    /// The incarnation of every database created before incarnations were stored.
    pub const LEGACY: Self = Self(0);

    /// Returns the incarnation with the value `value`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the incarnation as an integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns whether this is [`Incarnation::LEGACY`].
    #[must_use]
    pub const fn is_legacy(&self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Incarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for Incarnation {
    type Err = ParseIncarnationError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let valid = text.len() == 16
            && text
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if !valid {
            return Err(ParseIncarnationError {
                text: text.to_owned(),
            });
        }
        u64::from_str_radix(text, 16)
            .map(Self)
            .map_err(|_| ParseIncarnationError {
                text: text.to_owned(),
            })
    }
}

impl TryFrom<String> for Incarnation {
    type Error = ParseIncarnationError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl From<Incarnation> for String {
    fn from(incarnation: Incarnation) -> Self {
        incarnation.to_string()
    }
}

/// Why a string is not an [`Incarnation`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("'{text}' isn't an incarnation: expected 16 lowercase hex digits")]
pub struct ParseIncarnationError {
    text: String,
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
    // Absent from states and logs written before leases could be transferred.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    handed_over_by: Option<NodeId>,
}

impl Promotion {
    pub(crate) fn new(epoch: LeaseEpoch, candidate: NodeId, started_at: Timestamp) -> Self {
        Self {
            epoch,
            candidate,
            started_at,
            fenced: BTreeMap::new(),
            handed_over_by: None,
        }
    }

    /// Records that the promotion is a transfer from `holder`, which is draining.
    #[must_use]
    pub(crate) fn handed_over_by(mut self, holder: NodeId) -> Self {
        self.handed_over_by = Some(holder);
        self
    }

    /// Returns the draining holder that handed the lease over, if the promotion is a transfer
    /// rather than a failover after an expiry.
    #[must_use]
    pub fn transferred_from(&self) -> Option<&NodeId> {
        self.handed_over_by.as_ref()
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
///
/// The durability settings are fixed at creation and change only through a `ChangeDurability`
/// command, which takes effect when the next lease is granted (ADR-0023). Records written before
/// the settings existed load as [`Acks::Quorum`] with the floor at W.
///
/// A database may carry [`Metadata`] (ADR-0041). One without any is stored exactly as before
/// metadata existed.
///
/// A database has an [`Incarnation`], and a database made by a fork carries its [`Fork`] record
/// (ADR-0046). A database without a fork record is an ordinary database, fully seeded and ready;
/// one with the legacy incarnation and no fork record is stored exactly as before forks existed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "DatabaseRecord", into = "DatabaseRecord")]
pub struct Database {
    name: DatabaseName,
    replication_factor: ReplicationFactor,
    engine: Engine,
    durability: Durability,
    pending_durability: Option<Durability>,
    epoch: LeaseEpoch,
    lease: Option<Lease>,
    promotion: Option<Promotion>,
    metadata: Option<Metadata>,
    incarnation: Incarnation,
    fork: Option<Fork>,
}

/// A database as it is stored: the durability fields are optional, so records written before
/// they existed load with the defaults, and the metadata, the incarnation, and the fork record
/// come last and only when they say something, so a record without them is written byte for byte
/// as before they existed.
#[derive(Serialize, Deserialize)]
struct DatabaseRecord {
    name: DatabaseName,
    replication_factor: ReplicationFactor,
    engine: Engine,
    #[serde(default)]
    acks: Acks,
    #[serde(default)]
    min_replicas: Option<u8>,
    #[serde(default)]
    pending_durability: Option<Durability>,
    epoch: LeaseEpoch,
    lease: Option<Lease>,
    promotion: Option<Promotion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<Metadata>,
    #[serde(default, skip_serializing_if = "Incarnation::is_legacy")]
    incarnation: Incarnation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fork: Option<Fork>,
}

impl From<DatabaseRecord> for Database {
    fn from(record: DatabaseRecord) -> Self {
        let min_replicas = record.min_replicas.unwrap_or_else(|| {
            Durability::new(record.acks, record.replication_factor).min_replicas
        });
        Self {
            name: record.name,
            replication_factor: record.replication_factor,
            engine: record.engine,
            durability: Durability {
                acks: record.acks,
                min_replicas,
            },
            pending_durability: record.pending_durability,
            epoch: record.epoch,
            lease: record.lease,
            promotion: record.promotion,
            metadata: record.metadata,
            incarnation: record.incarnation,
            fork: record.fork,
        }
    }
}

impl From<Database> for DatabaseRecord {
    fn from(database: Database) -> Self {
        Self {
            name: database.name,
            replication_factor: database.replication_factor,
            engine: database.engine,
            acks: database.durability.acks,
            min_replicas: Some(database.durability.min_replicas),
            pending_durability: database.pending_durability,
            epoch: database.epoch,
            lease: database.lease,
            promotion: database.promotion,
            metadata: database.metadata,
            incarnation: database.incarnation,
            fork: database.fork,
        }
    }
}

impl Database {
    pub(crate) fn new(
        name: DatabaseName,
        replication_factor: ReplicationFactor,
        engine: Engine,
        durability: Durability,
        metadata: Option<Metadata>,
    ) -> Self {
        Self {
            name,
            replication_factor,
            engine,
            durability,
            pending_durability: None,
            epoch: LeaseEpoch::ZERO,
            lease: None,
            promotion: None,
            metadata,
            incarnation: Incarnation::LEGACY,
            fork: None,
        }
    }

    /// Gives the database `incarnation`.
    #[must_use]
    pub(crate) fn with_incarnation(mut self, incarnation: Incarnation) -> Self {
        self.incarnation = incarnation;
        self
    }

    /// Records that the database is made by `fork`.
    #[must_use]
    pub(crate) fn with_fork(mut self, fork: Fork) -> Self {
        self.fork = Some(fork);
        self
    }

    /// Returns which of the databases ever given this name the database is:
    /// [`Incarnation::LEGACY`] for one created before incarnations were assigned.
    #[must_use]
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Returns the fork that made the database, or `None` for an ordinary database.
    #[must_use]
    pub fn fork(&self) -> Option<&Fork> {
        self.fork.as_ref()
    }

    pub(crate) fn fork_mut(&mut self) -> Option<&mut Fork> {
        self.fork.as_mut()
    }

    /// Returns whether a quorum of the database's replicas holds its data: always for an
    /// ordinary database, and for a fork once it is
    /// [`Seeded`](crate::fork::ForkState::Seeded) or [`Ready`](crate::fork::ForkState::Ready).
    /// Only a seeded database may take a lease (ADR-0046, point 3.9).
    #[must_use]
    pub fn is_seeded(&self) -> bool {
        self.fork.as_ref().is_none_or(Fork::is_seeded)
    }

    /// Returns whether clients may use the database: always for an ordinary database, and for a
    /// fork once its reset is committed and recorded, [`Ready`](crate::fork::ForkState::Ready)
    /// (ADR-0046, point 6).
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.fork.as_ref().is_none_or(Fork::is_ready)
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

    /// Returns the durability settings in effect: those the current lease was granted under.
    #[must_use]
    pub fn durability(&self) -> Durability {
        self.durability
    }

    /// Returns the durability settings a `ChangeDurability` command recorded, which take effect
    /// when the next lease is granted, if any are pending.
    #[must_use]
    pub fn pending_durability(&self) -> Option<Durability> {
        self.pending_durability
    }

    pub(crate) fn set_pending_durability(&mut self, durability: Durability) {
        self.pending_durability = Some(durability);
    }

    /// Makes any pending durability settings the ones in effect, as a lease grant does.
    pub(crate) fn apply_pending_durability(&mut self) {
        if let Some(durability) = self.pending_durability.take() {
            self.durability = durability;
        }
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

    /// Removes the lease and any promotion, keeping the epoch, so that the next lease is a
    /// first lease at a higher epoch: a fork that seeds again after losing its copy.
    pub(crate) fn clear_lease(&mut self) {
        self.lease = None;
        self.promotion = None;
    }

    pub(crate) fn promotion_mut(&mut self) -> Option<&mut Promotion> {
        self.promotion.as_mut()
    }

    pub(crate) fn set_promotion(&mut self, promotion: Option<Promotion>) {
        self.promotion = promotion;
    }

    /// Returns the database's metadata, or `None` if it was created without any and never
    /// updated.
    #[must_use]
    pub fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }

    pub(crate) fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = Some(metadata);
    }
}

/// A replica set's size and its write and promotion quorums (ADR-0018, ADR-0023).
///
/// R is the replica set's size. In [`Acks::Quorum`], W = ⌊R/2⌋ + 1 and Q = R − W + 1; in
/// [`Acks::All`], W = R and Q = 1. Either way W + Q > R: every write acknowledged by W members
/// reaches at least one of any Q fenced members. [`Acks::Local`] gives that up: W = 1 and Q is
/// a majority, so a failover may lose acknowledged writes.
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
/// use fleetring_control::{Acks, Quorums};
///
/// let three = Quorums::new(ReplicationFactor::new(3).expect("3 is nonzero"));
/// assert_eq!((three.replicas(), three.write(), three.promotion()), (3, 2, 2));
/// let two = Quorums::new(ReplicationFactor::new(2).expect("2 is nonzero"));
/// assert_eq!((two.write(), two.promotion()), (2, 1));
/// let all = Quorums::with_acks(ReplicationFactor::new(3).expect("3 is nonzero"), Acks::All);
/// assert_eq!((all.write(), all.promotion()), (3, 1));
/// let local = Quorums::with_acks(ReplicationFactor::new(3).expect("3 is nonzero"), Acks::Local);
/// assert_eq!((local.write(), local.promotion()), (1, 2));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quorums {
    replicas: u8,
    write: u8,
    promotion: u8,
}

impl Quorums {
    /// Returns the quorums for a replica set of `size` members in [`Acks::Quorum`].
    #[must_use]
    pub const fn new(size: ReplicationFactor) -> Self {
        Self::with_acks(size, Acks::Quorum)
    }

    /// Returns the quorums for a replica set of `size` members in `acks`.
    #[must_use]
    pub const fn with_acks(size: ReplicationFactor, acks: Acks) -> Self {
        let replicas = size.get();
        let majority = replicas / 2 + 1;
        let (write, promotion) = match acks {
            Acks::Quorum => (majority, replicas - majority + 1),
            Acks::All => (replicas, 1),
            Acks::Local => (1, majority),
        };
        Self {
            replicas,
            write,
            promotion,
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
