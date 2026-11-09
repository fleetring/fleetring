//! The FleetRing control plane's state machine (ADR-0017).
//!
//! Every change to control state is a command, applied in order by one pure function. The crate
//! does no I/O and reads no clock: each command carries the time the control node stamped on it,
//! so replaying a command log anywhere gives the same state. That is what lets the control plane
//! move from one node to Raft without changing its logic.
//!
//! - [`State`] holds the members with their placement attributes and liveness, the membership
//!   epoch, and every database with its engine, epoch, lease, any promotion in progress, and any
//!   metadata.
//! - [`Command`] is a stamped [`Action`]: join, leave, heartbeat, create or drop a database,
//!   change its durability or its metadata, expire it, acquire or renew a lease, start a
//!   promotion, record a fenced member, complete a promotion, tick, drain or undrain a member,
//!   transfer a lease, or revoke a bearer token.
//! - [`apply`] checks a command against the rules of ADR-0017 and ADR-0018 and returns an
//!   [`Outcome`]: what changed, or a [`Rejection`] that left the state untouched.
//!
//! # Rules
//!
//! - **Time** comes only from command stamps. A stamp earlier than the last accepted one counts as
//!   the last accepted one.
//! - **Liveness:** a member is down once the time passes its last heartbeat plus the heartbeat
//!   timeout. Down members stay in the ring; liveness only filters lease and promotion candidates.
//! - **Membership epoch** grows on every change to the members or their placement attributes.
//! - **Replica sets** are the ring's replica sets over all members. With R the replica set's size,
//!   W and Q come from the database's [`Acks`] mode (ADR-0023): a majority by default, W = R and
//!   Q = 1 for `all`, and W = 1 with a majority Q for `local`, which may lose acknowledged writes
//!   on failover. A database also records `min_replicas`, the floor on reachable replicas its
//!   primary refuses writes below.
//! - **Durability changes** are recorded and take effect when the next lease is granted, so a
//!   primary's guarantee never changes under it.
//! - **Epochs** only grow. A database's first lease is granted at epoch 1; every later holder
//!   comes from a promotion, which may start only after the current lease has expired plus the
//!   failover margin, issues the next epoch, and completes only once Q replica-set members, the
//!   candidate included, are fenced at it.
//! - **Candidates** are live replica-set members that aren't draining. A promotion picks the one
//!   that reported the most advanced log: the highest log epoch (the epoch of the primary that
//!   wrote its last frames), then the highest position, preferring the earliest in the ring's
//!   preference order on ties. [`State::promotions_due`] lists the databases a promotion may
//!   start for.
//! - **Draining** members get no new lease and are never candidates. A draining holder may hand
//!   a lease over before it expires: a transfer starts the same promotion at the next epoch, with
//!   the holder's chosen target as the candidate, or else the most advanced other candidate.
//! - **Dropping** a database removes it with its lease, any promotion, and every report of it.
//!   Every later command that names it is rejected as an unknown database, so a node that still
//!   serves it stops once its next renewal is refused.
//! - **Ticks** change only the time, so liveness and lease expiry advance without other traffic.
//! - **Positions** are opaque bytes compared lexicographically (ADR-0016).
//! - **Software:** each member records the [`Software`] it runs; one that reports none runs
//!   [`Software::baseline`], release 0.3.0. A join is refused when its release line is more than
//!   one line from any member's, or when it lacks an enabled feature (ADR-0038).
//! - **Features** gate commands that not every member can read or act on: such a command is
//!   accepted only once every member supports each [`Feature`] it needs, and never by a fleet
//!   without members; the first one accepted enables its features for good.
//! - **Revocations** list token and key IDs, never credentials, each until a time after which it
//!   is forgotten; ticks and revocations forget the entries the time has passed (ADR-0025).
//! - **Metadata** (ADR-0041): a database may carry a [`Description`], [`Labels`], its creator
//!   and creation time, and an expiry, in a [`Metadata`] record. A creation records it from a
//!   [`MetadataSpec`]; afterwards, each update is a [`MetadataChange`] patch that changes only
//!   what it names, so concurrent updates to different fields both land and, for one field, the
//!   later in the log wins. Every accepted update grows the metadata's version, and an update
//!   may require a version first. The creator and each description's author are recorded as the
//!   command gives them; nothing is decided by them.
//! - **Expiry** is judged only at a command's time. An expiry must be later than the time of the
//!   command that sets it, and an `ExpireDatabase` drops a database, as `DropDatabase` does,
//!   only if its expiry is at or before the command's time; an expiry extended since wins.
//!   [`State::expired_databases`] lists the databases due.
//! - **Incarnations** (ADR-0046, point 1): each database has an [`Incarnation`], so a name
//!   dropped and created again names a different database. They are assigned from a counter
//!   that only grows, starting once the `fork` feature is enabled; until then every database
//!   has the legacy incarnation 0 and control state is stored exactly as before.
//! - **Forks** (ADR-0046, [`fork`]): a database may be made as a copy of another. It takes no
//!   lease until a write quorum of its replicas holds the copy, its first lease only on one of
//!   them, and every command that drives it is fenced by its fork identity. Each fork is made by
//!   an operation whose ID the client chooses; a resend of the same request returns the same
//!   operation, and the operation's record is kept for a retention after its outcome.
//!
//! # Examples
//!
//! ```
//! use fleetring::ReplicationFactor;
//! use fleetring_control::{
//!     apply, Action, Command, ControlConfig, Engine, Member, Rejection, State, Timestamp,
//! };
//!
//! let mut state = State::new(ControlConfig::new());
//! let at = Timestamp::from_millis(0);
//! for id in ["a", "b", "c"] {
//!     apply(&mut state, Command::new(at, Action::Join { member: Member::new(id.parse()?) }))?;
//! }
//! let db = "tenant:acme".parse()?;
//! apply(&mut state, Command::new(at, Action::CreateDatabase {
//!     name: db,
//!     replication_factor: ReplicationFactor::new(3).ok_or("zero")?,
//!     engine: Engine::Sqlite,
//!     durability: None,
//!     metadata: None,
//!     incarnation: fleetring_control::Incarnation::LEGACY,
//! }))?;
//! let quorums = state.quorums(&"tenant:acme".parse()?)?;
//! assert_eq!((quorums.write(), quorums.promotion()), (2, 2));
//!
//! // A rejected command leaves the state as it was.
//! let before = state.clone();
//! let again = apply(&mut state, Command::new(at, Action::Leave { id: "z".parse()? }));
//! assert!(matches!(again, Err(Rejection::UnknownMember { .. })));
//! assert_eq!(state, before);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
#![deny(clippy::float_arithmetic)]

mod apply;
mod command;
mod config;
mod database;
pub mod fork;
mod member;
mod metadata;
mod revocation;
mod software;
mod state;
mod time;

pub use apply::{Applied, JoinKind, Outcome, Rejection, apply};
pub use command::{Action, Command, UnknownReports};
pub use config::ControlConfig;
pub use database::{
    Acks, Database, DatabaseName, Durability, DurabilityError, Engine, Incarnation, Lease,
    LeaseEpoch, ParseAcksError, ParseDatabaseNameError, ParseIncarnationError, Position, Promotion,
    Quorums,
};
pub use member::{HostedReplica, Liveness, Member, ReplicaRole, Report};
pub use metadata::{
    Description, DescriptionChange, ExpiryChange, FORBIDDEN_FORMAT_CHARACTERS, LabelKey,
    LabelValue, Labels, MAX_DESCRIPTION_BYTES, MAX_LABELS, Metadata, MetadataChange, MetadataSpec,
    ParseDescriptionError, ParseLabelError, ParsePrincipalNameError, PrincipalName,
    RESERVED_LABEL_DOMAIN, parse_label,
};
pub use revocation::{KeyId, ParseCredentialIdError, Revocations, Revoked, TokenId};
pub use software::{Feature, ParseFeatureError, ParseReleaseError, Release, Software};
pub use state::{State, membership_ring};
pub use time::{Millis, Timestamp};

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<Acks>();
    assert_debug_send_sync::<Action>();
    assert_debug_send_sync::<Applied>();
    assert_debug_send_sync::<Command>();
    assert_debug_send_sync::<ControlConfig>();
    assert_debug_send_sync::<Database>();
    assert_debug_send_sync::<DatabaseName>();
    assert_debug_send_sync::<Description>();
    assert_debug_send_sync::<DescriptionChange>();
    assert_debug_send_sync::<Durability>();
    assert_debug_send_sync::<DurabilityError>();
    assert_debug_send_sync::<Engine>();
    assert_debug_send_sync::<ExpiryChange>();
    assert_debug_send_sync::<Feature>();
    assert_debug_send_sync::<HostedReplica>();
    assert_debug_send_sync::<Incarnation>();
    assert_debug_send_sync::<fork::CancelCause>();
    assert_debug_send_sync::<fork::ChainHash>();
    assert_debug_send_sync::<fork::CopyLossReason>();
    assert_debug_send_sync::<fork::CopyLost>();
    assert_debug_send_sync::<fork::Fork>();
    assert_debug_send_sync::<fork::ForkExpiry>();
    assert_debug_send_sync::<fork::ForkFailure>();
    assert_debug_send_sync::<fork::ForkIdentity>();
    assert_debug_send_sync::<fork::ForkIntent>();
    assert_debug_send_sync::<fork::IntentField>();
    assert_debug_send_sync::<fork::ForkLimits>();
    assert_debug_send_sync::<fork::ForkState>();
    assert_debug_send_sync::<fork::OperationId>();
    assert_debug_send_sync::<fork::OperationOutcome>();
    assert_debug_send_sync::<fork::OperationRecord>();
    assert_debug_send_sync::<fork::ParseOperationIdError>();
    assert_debug_send_sync::<fork::ResetReport>();
    assert_debug_send_sync::<fork::RetryCause>();
    assert_debug_send_sync::<fork::SeedFailure>();
    assert_debug_send_sync::<fork::SeedFailureReason>();
    assert_debug_send_sync::<fork::StagedCopy>();
    assert_debug_send_sync::<JoinKind>();
    assert_debug_send_sync::<KeyId>();
    assert_debug_send_sync::<LabelKey>();
    assert_debug_send_sync::<LabelValue>();
    assert_debug_send_sync::<Labels>();
    assert_debug_send_sync::<Lease>();
    assert_debug_send_sync::<LeaseEpoch>();
    assert_debug_send_sync::<Liveness>();
    assert_debug_send_sync::<Member>();
    assert_debug_send_sync::<Metadata>();
    assert_debug_send_sync::<MetadataChange>();
    assert_debug_send_sync::<MetadataSpec>();
    assert_debug_send_sync::<Millis>();
    assert_debug_send_sync::<Outcome>();
    assert_debug_send_sync::<ParseAcksError>();
    assert_debug_send_sync::<ParseCredentialIdError>();
    assert_debug_send_sync::<ParseDatabaseNameError>();
    assert_debug_send_sync::<ParseDescriptionError>();
    assert_debug_send_sync::<ParseFeatureError>();
    assert_debug_send_sync::<ParseIncarnationError>();
    assert_debug_send_sync::<ParseLabelError>();
    assert_debug_send_sync::<ParsePrincipalNameError>();
    assert_debug_send_sync::<ParseReleaseError>();
    assert_debug_send_sync::<Position>();
    assert_debug_send_sync::<PrincipalName>();
    assert_debug_send_sync::<Promotion>();
    assert_debug_send_sync::<Quorums>();
    assert_debug_send_sync::<Rejection>();
    assert_debug_send_sync::<Release>();
    assert_debug_send_sync::<Report>();
    assert_debug_send_sync::<Revocations>();
    assert_debug_send_sync::<Revoked>();
    assert_debug_send_sync::<ReplicaRole>();
    assert_debug_send_sync::<Software>();
    assert_debug_send_sync::<State>();
    assert_debug_send_sync::<Timestamp>();
    assert_debug_send_sync::<TokenId>();
};
