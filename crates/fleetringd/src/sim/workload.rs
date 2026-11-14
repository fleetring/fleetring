//! Workloads: clients that issue operations and record which ones were acknowledged.
//!
//! A [`Workload`] decides what a client does next; the harness runs the operation, records it in
//! the [`History`] and the trace, and tells the workload how it ended. Control operations go to
//! the control plane; [`Operation::Create`], [`Operation::Write`], and [`Operation::Read`] go
//! through the data plane's `Database` service, as the command line's do.
//!
//! The history records each operation twice, as the Jepsen-style checkers in
//! [`history`](crate::sim::history) need it: once when the client sends it and once when the
//! outcome arrives. An outcome is `ok` ([`Outcome::Acknowledged`]: the effect is durable),
//! `fail` ([`Outcome::Rejected`]: definitely not applied), or `info` ([`Outcome::Indeterminate`]
//! or never completed: unknown).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::delegation::DelegationId;
use fleetring_control::fork::{ForkIdentity, OperationId as RequestId};
use fleetring_control::{
    Action, DatabaseName, Feature, HostedReplica, Incarnation, KeyId, LeaseEpoch, Millis,
    PrincipalName, Revoked, Timestamp, TokenId,
};
use tonic::Code;

use crate::auth::BearerToken;
use crate::tls::Principal;

use crate::replication::FaultPoint;
use crate::sim::SimRng;
use crate::sim::check::ReplicaRows;
use crate::sim::history::DurabilityMode;

/// One operation a client sends to the fleet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operation {
    /// Joins `node` to the fleet, with default placement attributes.
    Join {
        /// The node.
        node: NodeId,
    },
    /// Sends a heartbeat for `node`, reporting no databases.
    Heartbeat {
        /// The node.
        node: NodeId,
    },
    /// Joins `node` reporting this build's software with `features` besides, as a node of a
    /// later release would: so that a scenario of nodes without data, such as FR-087's, can
    /// enable a fleet feature this build doesn't report yet, `fork` among them.
    JoinWith {
        /// The node.
        node: NodeId,
        /// The fleet features it reports beyond this build's.
        features: Vec<Feature>,
    },
    /// Joins `node` reporting this build's software without `features`, as a node of the
    /// previous release would: so that a scenario can run a fleet with a member that lacks a
    /// fleet feature, such as `handover` (ADR-0031, amendment 2). The node joins with no data.
    JoinWithout {
        /// The node.
        node: NodeId,
        /// The fleet features it doesn't report.
        features: Vec<Feature>,
    },
    /// Sends a heartbeat for `node` that reports `replicas`, with what each says about a fork
    /// of its name (staged copies, seed failures, lost copies, and reset writes; ADR-0046, point
    /// 4.5), and the free bytes on its volume: what a data node that seeds forks reports, sent
    /// without one. The heartbeat replaces everything `node` reported before, as any does.
    ///
    /// A scenario delays a report by sending it later, and duplicates it by sending it again;
    /// a report for another fork identity is sent the same way, and must count for nothing.
    Report {
        /// The node.
        node: NodeId,
        /// The databases it hosts, as it reports them.
        replicas: Vec<HostedReplica>,
        /// The free bytes on its data volume, if it says.
        disk_free_bytes: Option<u64>,
    },
    /// Forks `source` as `target` with `replication_factor` replicas (0 for the fleet's
    /// default) under `operation`, through the control plane's `ForkDatabase`, as
    /// `db fork --detach` does. Sent again with the same operation, it is a resend, and answers
    /// with the same operation.
    Fork {
        /// The database copied.
        source: DatabaseName,
        /// The fork.
        target: DatabaseName,
        /// The operation's ID.
        operation: fleetring_control::fork::OperationId,
        /// How many replicas the fork should have.
        replication_factor: u32,
        /// The source incarnation the caller requires, or `None` for the current one.
        expect_source_incarnation: Option<Incarnation>,
    },
    /// Drops `database` through the control plane, as `db drop` does: a fork that isn't ready
    /// is cancelled (ADR-0046, point 3.8).
    Drop {
        /// The database.
        database: DatabaseName,
    },
    /// Submits `action` to the control node in process, as its own fork driver would propose
    /// it: how a scenario delivers a delayed proposal, such as one for a fork identity that was
    /// dropped since (ADR-0046, point 12, row 18).
    Propose {
        /// The proposal.
        action: Box<Action>,
    },
    /// Sends a seeding `Snapshot` request to `to` with `as_node`'s node certificate, as a fork's
    /// seeder or replica would (ADR-0046, points 4.1 and 4.4), and records whether `to` served
    /// it or refused it: how a scenario delivers a request of a dropped fork identity.
    SeedingProbe {
        /// The node asked.
        to: NodeId,
        /// The node the request comes from.
        as_node: NodeId,
        /// What it asks for.
        request: SeedingProbe,
    },
    /// Adds `bytes` bytes of padding to `database` in one write, in a table of its own, so that
    /// its copy streams in several chunks. The history checker ignores it.
    Pad {
        /// The database.
        database: DatabaseName,
        /// How many bytes.
        bytes: u32,
    },
    /// Watches `database`'s changes as a live tail, as `db watch` without a position does, for a
    /// moment, and records the values of the [`WRITES_TABLE`] rows it saw inserted.
    Watch {
        /// The database.
        database: DatabaseName,
    },
    /// Removes `node`, as an operator on the control host would: it is submitted to the control
    /// node in process, not through the `Leave` RPC a node calls when it shuts down.
    Leave {
        /// The node.
        node: NodeId,
    },
    /// Marks `node` as draining, as `fleetringd node drain` does: the node hands every lease it
    /// holds to a follower and gets no new one. Submitted to the control node in process.
    Drain {
        /// The node.
        node: NodeId,
    },
    /// Creates a SQLite database.
    CreateDatabase {
        /// The database.
        database: DatabaseName,
        /// How many replicas it should have.
        replication_factor: u32,
    },
    /// Asks where a database lives.
    Locate {
        /// The database.
        database: DatabaseName,
    },
    /// Takes a database's first lease for `node`.
    AcquireLease {
        /// The database.
        database: DatabaseName,
        /// The node that asks to be primary.
        node: NodeId,
    },
    /// Renews `node`'s lease at `epoch`.
    RenewLease {
        /// The database.
        database: DatabaseName,
        /// The lease holder.
        node: NodeId,
        /// The lease's epoch.
        epoch: LeaseEpoch,
    },
    /// Creates a SQLite database through the data plane, as `fleetringd db create` does: the
    /// control plane places it, and the first live member of its replica set takes the lease and
    /// opens it. Then creates its [`WRITES_TABLE`]. A database that already exists counts as
    /// created once its table is there, so the operation can be repeated until it succeeds.
    Create {
        /// The database.
        database: DatabaseName,
        /// How many replicas it should have.
        replication_factor: u32,
        /// How its writes are acknowledged (ADR-0023), with the floor at the mode's write
        /// quorum.
        durability: DurabilityMode,
    },
    /// Creates a SQLite database through the data plane with an expiry, as `db create
    /// --expires-in` does: it expires `expires_in` after the control node takes the creation
    /// (ADR-0041, point 4). An expiring database may be gone by the end of a run, so the check
    /// that acknowledged creations survive leaves it out. A database that already exists counts
    /// as created.
    CreateExpiring {
        /// The database.
        database: DatabaseName,
        /// How many replicas it should have.
        replication_factor: u32,
        /// How long after its creation it expires.
        expires_in: Duration,
    },
    /// Inserts `value` into the database's [`WRITES_TABLE`] on its primary, through the data
    /// plane, and waits for the write quorum to hold it.
    Write {
        /// The database.
        database: DatabaseName,
        /// The value, unique within the run.
        value: i64,
    },
    /// Reads the database's [`WRITES_TABLE`] on its primary, through the data plane's `Query`:
    /// every value, or whether one value is there.
    Read {
        /// The database.
        database: DatabaseName,
        /// The one value to look for, or `None` to read every value.
        value: Option<i64>,
    },
    /// Opens a `Replicate` stream to `target` for `database` at `epoch`, as an impostor holding
    /// a node certificate would to take a database over (FR-050). The client's certificate must
    /// be a node's ([`FleetBuilder::impostor`](crate::sim::FleetBuilder::impostor)).
    ImpostorReplicate {
        /// The database.
        database: DatabaseName,
        /// The node the stream goes to.
        target: NodeId,
        /// The epoch the stream claims.
        epoch: LeaseEpoch,
    },
    /// Fences `target`'s replica of `database` at `epoch` for `candidate`.
    ImpostorFence {
        /// The database.
        database: DatabaseName,
        /// The node fenced.
        target: NodeId,
        /// The epoch the fence claims.
        epoch: LeaseEpoch,
        /// The candidate the fence names: the impostor's own node, as its certificate requires.
        candidate: NodeId,
    },
    /// Asks `target` for a snapshot of `database` at the epoch `target` serves it at, as an
    /// impostor reading a database it doesn't host would.
    ImpostorSnapshot {
        /// The database.
        database: DatabaseName,
        /// The node asked.
        target: NodeId,
        /// The epoch asked for.
        epoch: LeaseEpoch,
    },
    /// Asks the control plane for a delegated agent's token (ADR-0047, point 5), on a
    /// connection whose certificate names `certificate`, carrying `token`. Sent once per call:
    /// a resend is another `Delegate` with the same `operation`.
    Delegate {
        /// The principal the connection's certificate names: the caller or an ancestor.
        certificate: Principal,
        /// The caller's token.
        token: BearerToken,
        /// The operation ID the caller chose.
        operation: RequestId,
        /// The child's last segment.
        segment: String,
        /// The scopes asked for; none for the defaults.
        scopes: Vec<String>,
        /// The lifetime asked for, if one is.
        lifetime: Option<Millis>,
    },
    /// Creates `database` with one replica on a connection whose certificate names
    /// `certificate`, carrying `token`: an agent using a token, which the checker holds to the
    /// token's scopes.
    CreateAs {
        /// The principal the connection's certificate names.
        certificate: Principal,
        /// The token the call carries.
        token: BearerToken,
        /// The database.
        database: DatabaseName,
    },
    /// Adds `revoked` to the denylist until `until`, as an operator does with `fleet`.
    Revoke {
        /// What is revoked.
        revoked: Revoked,
        /// When the entry may be forgotten.
        until: Timestamp,
    },
    /// Revokes `principal` and every name below it, as an operator does with `fleet`
    /// (ADR-0047, point 11).
    RevokePrincipal {
        /// The principal.
        principal: PrincipalName,
    },
    /// Calls `Control.RevokeToken` under the ancestor rule (ADR-0047, point 11.3), on a
    /// connection whose certificate names `certificate`, carrying `token`, naming a principal,
    /// a token ID, or both. Sent once per call.
    RevokeAs {
        /// The principal the connection's certificate names: the caller or an ancestor.
        certificate: Principal,
        /// The caller's token.
        token: BearerToken,
        /// The principal revoked, if any.
        principal: Option<PrincipalName>,
        /// The token ID revoked, if any.
        token_id: Option<TokenId>,
    },
    /// Reads `database` on its primary with a token, on `certificate`'s certificate: the data
    /// node judges the token against the denylist it heard.
    QueryAs {
        /// The principal the connection's certificate names.
        certificate: Principal,
        /// The token the call carries.
        token: BearerToken,
        /// The database.
        database: DatabaseName,
    },
}

impl Operation {
    /// Returns the database a data-plane operation acts on, or `None` for a control operation.
    #[must_use]
    pub fn database(&self) -> Option<&DatabaseName> {
        match self {
            Self::Create { database, .. }
            | Self::CreateExpiring { database, .. }
            | Self::Write { database, .. }
            | Self::Read { database, .. }
            | Self::CreateDatabase { database, .. }
            | Self::Locate { database }
            | Self::AcquireLease { database, .. }
            | Self::RenewLease { database, .. }
            | Self::ImpostorReplicate { database, .. }
            | Self::ImpostorFence { database, .. }
            | Self::ImpostorSnapshot { database, .. } => Some(database),
            Self::Fork { target, .. }
            | Self::Drop { database: target }
            | Self::Pad {
                database: target, ..
            }
            | Self::Watch { database: target } => Some(target),
            Self::Join { .. }
            | Self::JoinWith { .. }
            | Self::JoinWithout { .. }
            | Self::Report { .. }
            | Self::Heartbeat { .. }
            | Self::Leave { .. }
            | Self::Drain { .. }
            | Self::Propose { .. }
            | Self::SeedingProbe { .. }
            | Self::Delegate { .. }
            | Self::CreateAs { .. }
            | Self::Revoke { .. }
            | Self::RevokePrincipal { .. }
            | Self::RevokeAs { .. }
            | Self::QueryAs { .. } => None,
        }
    }
}

/// The table [`Operation::Create`] makes and [`Operation::Write`] inserts into: one `value`
/// column, one row per acknowledged write.
pub const WRITES_TABLE: &str = "writes";

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Join { node } => write!(f, "join {node}"),
            Self::Heartbeat { node } => write!(f, "heartbeat {node}"),
            Self::JoinWith { node, features } => {
                write!(f, "join {node} with")?;
                for feature in features {
                    write!(f, " {feature}")?;
                }
                Ok(())
            }
            Self::JoinWithout { node, features } => {
                write!(f, "join {node} without")?;
                for feature in features {
                    write!(f, " {feature}")?;
                }
                Ok(())
            }
            Self::Report {
                node,
                replicas,
                disk_free_bytes,
            } => {
                write!(f, "report {} databases from {node}", replicas.len())?;
                if let Some(free) = disk_free_bytes {
                    write!(f, ", {free} bytes free")?;
                }
                Ok(())
            }
            Self::Fork {
                source,
                target,
                operation,
                replication_factor,
                expect_source_incarnation,
            } => {
                write!(
                    f,
                    "fork {source} as {target} rf {replication_factor} operation {operation}"
                )?;
                if let Some(incarnation) = expect_source_incarnation {
                    write!(f, " expecting {incarnation}")?;
                }
                Ok(())
            }
            Self::Drop { database } => write!(f, "drop {database}"),
            Self::Propose { action } => write!(f, "propose {action:?}"),
            Self::SeedingProbe {
                to,
                as_node,
                request,
            } => write!(f, "seeding probe to {to} as {as_node}: {request}"),
            Self::Pad { database, bytes } => write!(f, "pad {database} with {bytes} bytes"),
            Self::Watch { database } => write!(f, "watch {database}"),
            Self::Leave { node } => write!(f, "leave {node}"),
            Self::Drain { node } => write!(f, "drain {node}"),
            Self::CreateDatabase {
                database,
                replication_factor,
            } => write!(f, "create {database} rf {replication_factor}"),
            Self::Locate { database } => write!(f, "locate {database}"),
            Self::AcquireLease { database, node } => write!(f, "acquire {database} for {node}"),
            Self::RenewLease {
                database,
                node,
                epoch,
            } => write!(f, "renew {database} for {node} at epoch {}", epoch.get()),
            Self::Create {
                database,
                replication_factor,
                durability,
            } => write!(
                f,
                "open {database} rf {replication_factor} acks {durability}"
            ),
            Self::CreateExpiring {
                database,
                replication_factor,
                expires_in,
            } => write!(
                f,
                "open {database} rf {replication_factor} expiring in {} ms",
                expires_in.as_millis()
            ),
            Self::Write { database, value } => write!(f, "write {value} to {database}"),
            Self::Read {
                database,
                value: Some(value),
            } => write!(f, "read {value} from {database}"),
            Self::Read {
                database,
                value: None,
            } => write!(f, "read {database}"),
            Self::ImpostorReplicate {
                database,
                target,
                epoch,
            } => write!(
                f,
                "impostor stream of {database} to {target} at epoch {}",
                epoch.get()
            ),
            Self::ImpostorFence {
                database,
                target,
                epoch,
                candidate,
            } => write!(
                f,
                "impostor fence of {database} on {target} at epoch {} for {candidate}",
                epoch.get()
            ),
            Self::ImpostorSnapshot {
                database,
                target,
                epoch,
            } => write!(
                f,
                "impostor snapshot of {database} from {target} at epoch {}",
                epoch.get()
            ),
            // Never the token: a trace is printed when a seed fails.
            Self::Delegate {
                certificate,
                operation,
                segment,
                scopes,
                lifetime,
                ..
            } => {
                write!(
                    f,
                    "delegate {segment} as {certificate}'s token, operation {operation}"
                )?;
                if !scopes.is_empty() {
                    write!(f, " scopes {}", scopes.join(","))?;
                }
                if let Some(lifetime) = lifetime {
                    write!(f, " for {} ms", lifetime.get())?;
                }
                Ok(())
            }
            Self::CreateAs {
                certificate,
                database,
                ..
            } => write!(
                f,
                "create {database} with a token, on {certificate}'s certificate"
            ),
            Self::Revoke { revoked, until } => {
                write!(f, "revoke {revoked} until {}", until.as_millis())
            }
            Self::RevokePrincipal { principal } => write!(f, "revoke principal {principal}"),
            Self::RevokeAs {
                certificate,
                principal,
                token_id,
                ..
            } => {
                write!(f, "revoke as {certificate}'s token")?;
                if let Some(principal) = principal {
                    write!(f, " principal {principal}")?;
                }
                if let Some(token_id) = token_id {
                    write!(f, " token {token_id}")?;
                }
                Ok(())
            }
            Self::QueryAs {
                certificate,
                database,
                ..
            } => write!(
                f,
                "query {database} with a token, on {certificate}'s certificate"
            ),
        }
    }
}

/// How an operation ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// The fleet acknowledged it: the effect is durable and must never be lost.
    Acknowledged(Ack),
    /// The fleet refused it; it had no effect.
    Rejected {
        /// The gRPC status code.
        code: Code,
        /// The status message.
        message: String,
    },
    /// No answer arrived: the operation may or may not have taken effect.
    Indeterminate {
        /// The status code of the failure, or `None` if the call timed out.
        code: Option<Code>,
    },
}

impl Outcome {
    /// Returns the acknowledgement, if the operation was acknowledged.
    #[must_use]
    pub fn ack(&self) -> Option<&Ack> {
        match self {
            Self::Acknowledged(ack) => Some(ack),
            _ => None,
        }
    }

    /// Returns the outcome's class: what the checkers may assume about the operation's effect.
    #[must_use]
    pub fn class(&self) -> OutcomeClass {
        match self {
            Self::Acknowledged(_) => OutcomeClass::Ok,
            Self::Rejected { .. } => OutcomeClass::Fail,
            Self::Indeterminate { .. } => OutcomeClass::Info,
        }
    }

    pub(crate) fn from_status(status: &tonic::Status) -> Self {
        match status.code() {
            Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Cancelled
            | Code::Unknown
            | Code::Internal
            | Code::Aborted
            | Code::ResourceExhausted => Self::Indeterminate {
                code: Some(status.code()),
            },
            code => Self::Rejected {
                code,
                message: status.message().to_owned(),
            },
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acknowledged(ack) => write!(f, "ok {ack}"),
            Self::Rejected { code, message } => write!(f, "rejected {code:?}: {message}"),
            Self::Indeterminate { code: Some(code) } => write!(f, "indeterminate {code:?}"),
            Self::Indeterminate { code: None } => f.write_str("indeterminate timeout"),
        }
    }
}

/// What a checker may assume about an operation's effect, from how it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutcomeClass {
    /// The operation was acknowledged: its effect is durable and must never be lost.
    Ok,
    /// The operation was refused: it definitely had no effect.
    Fail,
    /// The operation got no answer, or an answer that leaves its effect unknown: it may or may
    /// not have taken effect. An operation still in flight is `info` too.
    Info,
}

impl fmt::Display for OutcomeClass {
    /// Honors a width, so excerpt columns line up.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(match self {
            Self::Ok => "ok",
            Self::Fail => "fail",
            Self::Info => "info",
        })
    }
}

/// What an acknowledged operation returned.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Ack {
    /// The node is a member, at this membership epoch.
    Joined {
        /// The membership epoch after the join.
        membership_epoch: u64,
    },
    /// The heartbeat was recorded.
    HeartbeatRecorded,
    /// The node was removed.
    Left,
    /// The node is draining.
    Draining,
    /// The database was created.
    Created,
    /// Where the database lives.
    Located {
        /// The database's epoch.
        epoch: LeaseEpoch,
        /// Its lease holder, if it has one.
        primary: Option<NodeId>,
    },
    /// A lease was granted or renewed.
    Lease {
        /// The lease's epoch.
        epoch: LeaseEpoch,
        /// When it expires, in control-plane milliseconds.
        expires_at_ms: u64,
    },
    /// The database exists, with a primary that serves it.
    Opened {
        /// The primary.
        primary: NodeId,
        /// The primary's lease epoch.
        epoch: LeaseEpoch,
        /// The durability mode the control plane reports for it, which the history checker
        /// holds it to.
        durability: DurabilityMode,
    },
    /// A write committed and a write quorum stored it: it must never be lost.
    Written {
        /// How many replicas, the primary included, held it when it was acknowledged: W or
        /// more.
        acknowledged_by: u32,
        /// R, the size the database's replica set should have.
        replicas: u32,
        /// Whether fewer than R of the database's replicas were reachable.
        under_replicated: bool,
        /// The primary that acknowledged it, if it said.
        primary: Option<NodeId>,
        /// The epoch it acknowledged it at, or 0 if it didn't say.
        epoch: LeaseEpoch,
    },
    /// What a read found in the database's [`WRITES_TABLE`]: every value it holds, or, for a
    /// read of one value, that value if it is there.
    Read {
        /// The values found, in order.
        values: BTreeSet<i64>,
    },
    /// The node answered an impostor's node call by refusing it: the answer, not an effect.
    Refused {
        /// The status code.
        code: Code,
        /// The binding the refusal's `fleetring-binding` metadata names, if it names one.
        binding: Option<String>,
    },
    /// The node served an impostor's node call, which a node bound to placement never does.
    Served,
    /// The control plane accepted a fork, or answered a resend with the operation's status.
    Forked {
        /// The fork's incarnation.
        incarnation: Incarnation,
    },
    /// The database was dropped.
    Dropped,
    /// The control node took a proposal: applied it, or refused it with a rejection that left
    /// its state as it was.
    Proposed {
        /// Whether `apply` accepted it.
        accepted: bool,
    },
    /// The padding was written.
    Padded,
    /// What a watch saw: the values of the rows it saw inserted into the [`WRITES_TABLE`].
    Watched {
        /// The values, in the order seen.
        values: Vec<i64>,
    },
    /// The control plane delegated a token, or signed a recorded one again for a resend.
    Delegated {
        /// The child's token.
        token: BearerToken,
        /// Its delegation ID.
        delegation: DelegationId,
        /// The child's principal.
        principal: String,
        /// Its scopes, in canonical form.
        scopes: Vec<String>,
        /// When it expires.
        expires_at: Timestamp,
        /// The issuer key that signed this copy.
        issuer_key: KeyId,
    },
    /// The entry is on the denylist.
    Revoked,
    /// The query was answered.
    Queried,
}

impl fmt::Display for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Joined { membership_epoch } => write!(f, "joined at epoch {membership_epoch}"),
            Self::HeartbeatRecorded => f.write_str("recorded"),
            Self::Left => f.write_str("left"),
            Self::Draining => f.write_str("draining"),
            Self::Created => f.write_str("created"),
            Self::Located { epoch, primary } => match primary {
                Some(primary) => write!(f, "epoch {} primary {primary}", epoch.get()),
                None => write!(f, "epoch {} no primary", epoch.get()),
            },
            Self::Lease {
                epoch,
                expires_at_ms,
            } => write!(f, "lease epoch {} until {expires_at_ms}", epoch.get()),
            Self::Opened {
                primary,
                epoch,
                durability,
            } => write!(
                f,
                "opened on {primary} at epoch {} acks {durability}",
                epoch.get()
            ),
            Self::Written {
                acknowledged_by,
                replicas,
                under_replicated,
                primary,
                epoch,
            } => {
                write!(f, "written to {acknowledged_by} of {replicas}")?;
                if *under_replicated {
                    f.write_str(", under-replicated")?;
                }
                if let Some(primary) = primary {
                    write!(f, " by {primary} at epoch {}", epoch.get())?;
                }
                Ok(())
            }
            Self::Refused {
                code,
                binding: Some(binding),
            } => write!(f, "refused {code:?}, binding {binding}"),
            Self::Refused {
                code,
                binding: None,
            } => write!(f, "refused {code:?}"),
            Self::Served => f.write_str("served"),
            Self::Forked { incarnation } => write!(f, "forked at incarnation {incarnation}"),
            Self::Dropped => f.write_str("dropped"),
            Self::Proposed { accepted: true } => f.write_str("proposal accepted"),
            Self::Proposed { accepted: false } => f.write_str("proposal refused"),
            Self::Padded => f.write_str("padded"),
            Self::Watched { values } => write!(f, "watched {} inserts", values.len()),
            Self::Delegated {
                delegation,
                principal,
                issuer_key,
                ..
            } => write!(
                f,
                "delegated {principal} as {delegation}, signed by {issuer_key}"
            ),
            Self::Revoked => f.write_str("revoked"),
            Self::Queried => f.write_str("queried"),
            Self::Read { values } => {
                const SHOWN: usize = 6;
                write!(f, "read {} values", values.len())?;
                if values.len() <= SHOWN {
                    write!(f, " {values:?}")
                } else {
                    let first: Vec<&i64> = values.iter().take(SHOWN).collect();
                    write!(f, " {first:?}…")
                }
            }
        }
    }
}

/// Identifies one operation in a [`History`]: the order its client sent it in, across clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationId(u64);

impl OperationId {
    /// Returns the operation's number, counted from 1 in the order operations were sent.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// One finished operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    id: OperationId,
    client: String,
    invoked_at: Duration,
    completed_at: Duration,
    operation: Operation,
    outcome: Outcome,
}

impl Record {
    /// Returns the operation's ID.
    #[must_use]
    pub fn id(&self) -> OperationId {
        self.id
    }

    /// Returns the client that sent the operation.
    #[must_use]
    pub fn client(&self) -> &str {
        &self.client
    }

    /// Returns the simulated time the operation was sent.
    #[must_use]
    pub fn invoked_at(&self) -> Duration {
        self.invoked_at
    }

    /// Returns the simulated time the outcome arrived.
    #[must_use]
    pub fn completed_at(&self) -> Duration {
        self.completed_at
    }

    /// Returns the operation.
    #[must_use]
    pub fn operation(&self) -> &Operation {
        &self.operation
    }

    /// Returns how it ended.
    #[must_use]
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// Returns what the checkers may assume about the operation's effect.
    #[must_use]
    pub fn class(&self) -> OutcomeClass {
        self.outcome.class()
    }
}

/// An operation a client sent and hasn't heard back about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pending {
    client: String,
    invoked_at: Duration,
    operation: Operation,
}

impl Pending {
    /// Returns the client that sent the operation.
    #[must_use]
    pub fn client(&self) -> &str {
        &self.client
    }

    /// Returns the simulated time the operation was sent.
    #[must_use]
    pub fn invoked_at(&self) -> Duration {
        self.invoked_at
    }

    /// Returns the operation.
    #[must_use]
    pub fn operation(&self) -> &Operation {
        &self.operation
    }
}

/// Which end of an operation a [`HistoryEvent`] marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase<'h> {
    /// The client sent the operation.
    Invoke,
    /// The outcome arrived.
    Complete(&'h Outcome),
}

/// One end of an operation, as [`History::events`] lists them: Jepsen's `:invoke` and `:ok`,
/// `:fail`, or `:info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryEvent<'h> {
    at: Duration,
    client: &'h str,
    id: OperationId,
    operation: &'h Operation,
    phase: Phase<'h>,
}

impl<'h> HistoryEvent<'h> {
    /// Returns the simulated time of the event.
    #[must_use]
    pub fn at(&self) -> Duration {
        self.at
    }

    /// Returns the client that sent the operation.
    #[must_use]
    pub fn client(&self) -> &'h str {
        self.client
    }

    /// Returns the operation's ID.
    #[must_use]
    pub fn id(&self) -> OperationId {
        self.id
    }

    /// Returns the operation.
    #[must_use]
    pub fn operation(&self) -> &'h Operation {
        self.operation
    }

    /// Returns which end of the operation this is.
    #[must_use]
    pub fn phase(&self) -> Phase<'h> {
        self.phase
    }
}

impl fmt::Display for HistoryEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:>8}ms {} {:>6} ",
            self.at.as_millis(),
            self.id,
            self.client
        )?;
        match self.phase {
            Phase::Invoke => write!(f, "invoke {}", self.operation),
            Phase::Complete(outcome) => {
                write!(f, "{:<6} {} -> {outcome}", outcome.class(), self.operation)
            }
        }
    }
}

/// How a lease watcher learned that a node holds, or held, a lease (FR-042).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LeaseSeen {
    /// The lease was in the snapshot a `WatchLeases` stream opened with.
    Held,
    /// The stream reported the lease gained.
    Gained,
    /// The stream reported the lease lost; the epoch is the one that ended.
    Lost,
}

impl fmt::Display for LeaseSeen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Held => "held",
            Self::Gained => "gained",
            Self::Lost => "lost",
        })
    }
}

/// One thing a `WatchLeases` subscriber learned about the leases a node holds (FR-042).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseObservation {
    node: String,
    at: Duration,
    database: DatabaseName,
    epoch: LeaseEpoch,
    seen: LeaseSeen,
}

impl LeaseObservation {
    /// What the watcher of `node` learned at `at`.
    #[must_use]
    pub fn new(
        node: impl Into<String>,
        at: Duration,
        database: DatabaseName,
        epoch: LeaseEpoch,
        seen: LeaseSeen,
    ) -> Self {
        Self {
            node: node.into(),
            at,
            database,
            epoch,
            seen,
        }
    }

    /// Returns the host whose leases were watched.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// Returns the simulated time the watcher learned it.
    #[must_use]
    pub fn at(&self) -> Duration {
        self.at
    }

    /// Returns the database.
    #[must_use]
    pub fn database(&self) -> &DatabaseName {
        &self.database
    }

    /// Returns the lease's epoch.
    #[must_use]
    pub fn epoch(&self) -> LeaseEpoch {
        self.epoch
    }

    /// Returns how the watcher learned it.
    #[must_use]
    pub fn seen(&self) -> LeaseSeen {
        self.seen
    }

    /// Whether the observation says the node holds, or held, the lease: a snapshot entry or a
    /// gain, as opposed to a loss.
    #[must_use]
    pub fn is_holding(&self) -> bool {
        matches!(self.seen, LeaseSeen::Held | LeaseSeen::Gained)
    }
}

impl fmt::Display for LeaseObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:>8}ms {} {} {} epoch {}",
            self.at.as_millis(),
            self.node,
            self.seen,
            self.database,
            self.epoch.get()
        )
    }
}

/// Every operation clients sent: the finished ones in completion order, and the ones still in
/// flight; and everything the lease watchers learned, in the order they learned it.
///
/// The harness fills it as clients run; a test builds one by hand with [`History::begin`] and
/// [`History::complete`], or [`History::record`], to exercise the checkers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    next_id: u64,
    records: Vec<Record>,
    pending: BTreeMap<OperationId, Pending>,
    leases: Vec<LeaseObservation>,
    fork_passes: Vec<ForkPass>,
    fork_transitions: Vec<ForkTransition>,
    fork_markers: Vec<ForkMarkerSeen>,
    fenced: Vec<FencedSeen>,
    clock_shifts: Vec<(Duration, u64)>,
}

impl History {
    /// Records that `client` sent `operation` at `at`, and returns the ID to complete it with.
    pub fn begin(
        &mut self,
        client: impl Into<String>,
        at: Duration,
        operation: Operation,
    ) -> OperationId {
        self.next_id += 1;
        let id = OperationId(self.next_id);
        self.pending.insert(
            id,
            Pending {
                client: client.into(),
                invoked_at: at,
                operation,
            },
        );
        id
    }

    /// Records that the operation `id` ended with `outcome` at `at`.
    ///
    /// # Panics
    ///
    /// If `id` isn't in flight: it was never begun, or it was completed already.
    pub fn complete(&mut self, id: OperationId, at: Duration, outcome: Outcome) {
        let pending = self
            .pending
            .remove(&id)
            .unwrap_or_else(|| panic!("operation {id} isn't in flight"));
        self.records.push(Record {
            id,
            client: pending.client,
            invoked_at: pending.invoked_at,
            completed_at: at,
            operation: pending.operation,
            outcome,
        });
    }

    /// Records a finished operation in one step, and returns its ID.
    pub fn record(
        &mut self,
        client: impl Into<String>,
        invoked_at: Duration,
        completed_at: Duration,
        operation: Operation,
        outcome: Outcome,
    ) -> OperationId {
        let id = self.begin(client, invoked_at, operation);
        self.complete(id, completed_at, outcome);
        id
    }

    /// Returns every finished operation, in completion order.
    #[must_use]
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Returns the finished operation with ID `id`, if it has finished.
    #[must_use]
    pub fn record_by_id(&self, id: OperationId) -> Option<&Record> {
        self.records.iter().find(|record| record.id == id)
    }

    /// Returns every operation still in flight, with its ID, in the order they were sent.
    pub fn pending(&self) -> impl Iterator<Item = (OperationId, &Pending)> {
        self.pending.iter().map(|(id, pending)| (*id, pending))
    }

    /// Returns both ends of every operation, in time order. At one time, invocations come
    /// before completions, and earlier operations first.
    #[must_use]
    pub fn events(&self) -> Vec<HistoryEvent<'_>> {
        let mut events: Vec<HistoryEvent<'_>> = Vec::with_capacity(2 * self.records.len());
        for record in &self.records {
            events.push(HistoryEvent {
                at: record.invoked_at,
                client: &record.client,
                id: record.id,
                operation: &record.operation,
                phase: Phase::Invoke,
            });
            events.push(HistoryEvent {
                at: record.completed_at,
                client: &record.client,
                id: record.id,
                operation: &record.operation,
                phase: Phase::Complete(&record.outcome),
            });
        }
        for (id, pending) in &self.pending {
            events.push(HistoryEvent {
                at: pending.invoked_at,
                client: &pending.client,
                id: *id,
                operation: &pending.operation,
                phase: Phase::Invoke,
            });
        }
        events.sort_by_key(|event| {
            (
                event.at,
                matches!(event.phase, Phase::Complete(_)),
                event.id,
            )
        });
        events
    }

    /// Returns the acknowledged operations with their acknowledgements, in completion order.
    pub fn acknowledged(&self) -> impl Iterator<Item = (&Operation, &Ack)> {
        self.records
            .iter()
            .filter_map(|record| Some((&record.operation, record.outcome.ack()?)))
    }

    /// Returns every acknowledged write, as its database and value, in completion order. Each
    /// must be found on the database after any fault.
    pub fn acknowledged_writes(&self) -> impl Iterator<Item = (&DatabaseName, i64)> {
        self.acknowledged()
            .filter_map(|(operation, ack)| match (operation, ack) {
                (Operation::Write { database, value }, Ack::Written { .. }) => {
                    Some((database, *value))
                }
                _ => None,
            })
    }

    /// Records what a lease watcher learned.
    pub fn observe_lease(&mut self, observation: LeaseObservation) {
        self.leases.push(observation);
    }

    /// Returns everything the lease watchers learned, in the order they learned it.
    #[must_use]
    pub fn lease_observations(&self) -> &[LeaseObservation] {
        &self.leases
    }

    /// Records that a data host's fork work passed a fault hook.
    pub fn observe_fork_pass(&mut self, pass: ForkPass) {
        self.fork_passes.push(pass);
    }

    /// Returns every pass of a data host's fork work through a fault hook the fleet saw, in the
    /// order it saw them.
    #[must_use]
    pub fn fork_passes(&self) -> &[ForkPass] {
        &self.fork_passes
    }

    /// Records that every host's clock was set ahead, at `at`, to `total` milliseconds ahead of
    /// simulated time in all.
    pub fn observe_clock_shift(&mut self, at: Duration, total: u64) {
        self.clock_shifts.push((at, total));
    }

    /// Returns each time the hosts' clocks were set ahead, with how far ahead they were then.
    #[must_use]
    pub fn clock_shifts(&self) -> &[(Duration, u64)] {
        &self.clock_shifts
    }

    /// Records that a fork's phase changed in control state.
    pub fn observe_fork_transition(&mut self, transition: ForkTransition) {
        self.fork_transitions.push(transition);
    }

    /// Returns every change of a fork's phase the fleet saw in control state, in order.
    #[must_use]
    pub fn fork_transitions(&self) -> &[ForkTransition] {
        &self.fork_transitions
    }

    /// Records that a promotion started in control state, or that a database's replica set
    /// changed there.
    pub fn observe_fenced(&mut self, seen: FencedSeen) {
        self.fenced.push(seen);
    }

    /// Returns every promotion the fleet saw start in control state, and every change of a
    /// replica set it saw, in order.
    #[must_use]
    pub fn fenced(&self) -> &[FencedSeen] {
        &self.fenced
    }

    /// Records that a holder's copy of a fork was first seen holding the fork's marker on disk.
    pub fn observe_fork_marker(&mut self, seen: ForkMarkerSeen) {
        self.fork_markers.push(seen);
    }

    /// Returns, for each holder of each fork, when the fleet first saw its copy on disk hold the
    /// fork's marker, in order.
    #[must_use]
    pub fn fork_markers(&self) -> &[ForkMarkerSeen] {
        &self.fork_markers
    }

    /// The databases a client asked to drop during the run, whatever the answer.
    #[must_use]
    pub fn dropped(&self) -> BTreeSet<DatabaseName> {
        self.records
            .iter()
            .map(|record| &record.operation)
            .chain(self.pending.values().map(|pending| &pending.operation))
            .filter_map(|operation| match operation {
                Operation::Drop { database } => Some(database.clone()),
                _ => None,
            })
            .collect()
    }

    /// This history without any operation on the databases in `names`: for the checkers that
    /// assume a name means one database for the whole run, when a scenario drops some.
    #[must_use]
    pub fn without_databases(&self, names: &BTreeSet<DatabaseName>) -> Self {
        let kept = |operation: &Operation| {
            operation
                .database()
                .is_none_or(|name| !names.contains(name))
        };
        Self {
            next_id: self.next_id,
            records: self
                .records
                .iter()
                .filter(|record| kept(&record.operation))
                .cloned()
                .collect(),
            pending: self
                .pending
                .iter()
                .filter(|(_, pending)| kept(&pending.operation))
                .map(|(id, pending)| (*id, pending.clone()))
                .collect(),
            leases: self.leases.clone(),
            fork_passes: self.fork_passes.clone(),
            fork_transitions: self.fork_transitions.clone(),
            fork_markers: self.fork_markers.clone(),
            fenced: self
                .fenced
                .iter()
                .filter(|seen| !names.contains(&seen.database))
                .cloned()
                .collect(),
            clock_shifts: self.clock_shifts.clone(),
        }
    }

    /// Returns how many operations finished.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns whether no operation finished.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// What a client does next.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Step {
    /// Send an operation and wait for its outcome.
    Run(Operation),
    /// Wait for a while.
    Sleep(Duration),
    /// Stop: the workload is finished.
    Done,
}

/// Decides what a simulated client does.
///
/// The harness calls [`Workload::next`] for each step, with a generator forked from the run's
/// seed, and [`Workload::observe`] with each outcome. A workload must take randomness only from
/// that generator, so a seed replays it.
pub trait Workload: fmt::Debug + 'static {
    /// Returns the client's next step.
    fn next(&mut self, rng: &mut SimRng) -> Step;

    /// Learns how an operation ended. The default ignores it.
    fn observe(&mut self, operation: &Operation, outcome: &Outcome) {
        let _ = (operation, outcome);
    }

    /// Whether the workload runs until the simulation ends rather than finishing on its own.
    /// [`Fleet::await_workloads`](crate::sim::Fleet::await_workloads) doesn't wait for such
    /// a workload. The default is `false`.
    fn is_endless(&self) -> bool {
        false
    }
}

/// How long a script waits before it repeats an operation that got no answer.
const SCRIPT_RETRY: Duration = Duration::from_millis(20);

/// Runs a fixed list of operations in order, repeating each one until it gets an answer.
///
/// An operation counts as answered once it is acknowledged or rejected; `ALREADY_EXISTS` after a
/// lost reply is such an answer.
#[derive(Debug, Clone, Default)]
pub struct Script {
    pending: VecDeque<Operation>,
    waiting: bool,
}

impl Script {
    /// A script of `operations`.
    #[must_use]
    pub fn new(operations: impl IntoIterator<Item = Operation>) -> Self {
        Self {
            pending: operations.into_iter().collect(),
            waiting: false,
        }
    }
}

impl Workload for Script {
    fn next(&mut self, _rng: &mut SimRng) -> Step {
        if self.waiting {
            self.waiting = false;
            return Step::Sleep(SCRIPT_RETRY);
        }
        match self.pending.front() {
            Some(operation) => Step::Run(operation.clone()),
            None => Step::Done,
        }
    }

    fn observe(&mut self, _operation: &Operation, outcome: &Outcome) {
        if matches!(outcome, Outcome::Indeterminate { .. }) {
            self.waiting = true;
        } else {
            self.pending.pop_front();
        }
    }
}

/// How often an idle operator checks its queue.
const OPERATOR_POLL: Duration = Duration::from_millis(10);

/// Operations a test queues while the simulation runs, as an operator would type them.
///
/// It never finishes. Each queued operation runs once, whatever its outcome. Clones share the
/// queue, so the test keeps one and gives another to [`Fleet::client`](crate::sim::Fleet::client).
#[derive(Debug, Clone, Default)]
pub struct Operator {
    queue: Arc<Mutex<VecDeque<Operation>>>,
}

impl Operator {
    /// An operator with nothing queued.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues `operation`.
    ///
    /// # Panics
    ///
    /// If the queue's lock was poisoned by a panic in another thread.
    pub fn push(&self, operation: Operation) {
        self.queue
            .lock()
            .expect("operator queue lock poisoned")
            .push_back(operation);
    }

    /// Returns whether every queued operation has been sent.
    ///
    /// # Panics
    ///
    /// If the queue's lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.queue
            .lock()
            .expect("operator queue lock poisoned")
            .is_empty()
    }
}

impl Workload for Operator {
    fn next(&mut self, _rng: &mut SimRng) -> Step {
        let next = self
            .queue
            .lock()
            .expect("operator queue lock poisoned")
            .pop_front();
        next.map_or(Step::Sleep(OPERATOR_POLL), Step::Run)
    }

    fn is_endless(&self) -> bool {
        true
    }
}

/// The longest pause between two operations of [`CreateAndAcquire`], in milliseconds.
const MAX_PAUSE_MS: u64 = 60;

/// Creates databases, takes each one's first lease for a random node, and renews leases.
///
/// Every database is created with the given replication factor, so with as many data nodes as
/// replicas, any live node may take the lease. An acquire that gets no answer, or is refused,
/// is followed by a `Locate` that learns whether the lease was granted after all. The workload
/// finishes once every database is leased and the renewals are spent.
#[derive(Debug, Clone)]
pub struct CreateAndAcquire {
    nodes: Vec<NodeId>,
    replication_factor: u32,
    databases: Vec<(DatabaseName, Progress)>,
    renewals: u32,
    pause: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Progress {
    Planned,
    Created,
    Uncertain,
    Leased { node: NodeId, epoch: LeaseEpoch },
}

impl CreateAndAcquire {
    /// Creates `databases` databases named `db-0`, `db-1`, …, with `replication_factor`
    /// replicas, leases them to nodes among `nodes`, and then renews leases `renewals` times.
    ///
    /// # Panics
    ///
    /// If `db-N` isn't a valid database name, which it always is.
    #[must_use]
    pub fn new(
        nodes: impl IntoIterator<Item = NodeId>,
        databases: u32,
        replication_factor: u32,
        renewals: u32,
    ) -> Self {
        Self {
            nodes: nodes.into_iter().collect(),
            replication_factor,
            databases: (0..databases)
                .map(|k| {
                    let name = format!("db-{k}");
                    let name = name
                        .parse()
                        .unwrap_or_else(|error| panic!("'{name}' is a valid name: {error}"));
                    (name, Progress::Planned)
                })
                .collect(),
            renewals,
            pause: false,
        }
    }

    fn progress(&mut self, database: &DatabaseName) -> Option<&mut Progress> {
        self.databases
            .iter_mut()
            .find(|(name, _)| name == database)
            .map(|(_, progress)| progress)
    }
}

impl Workload for CreateAndAcquire {
    fn next(&mut self, rng: &mut SimRng) -> Step {
        if self.pause {
            self.pause = false;
            return Step::Sleep(Duration::from_millis(10 + rng.below(MAX_PAUSE_MS - 10)));
        }
        let planned = self
            .databases
            .iter()
            .find(|(_, progress)| *progress == Progress::Planned)
            .map(|(database, _)| database.clone());
        let open: Vec<&(DatabaseName, Progress)> = self
            .databases
            .iter()
            .filter(|(_, progress)| matches!(progress, Progress::Created | Progress::Uncertain))
            .collect();
        let leased: Vec<&(DatabaseName, Progress)> = self
            .databases
            .iter()
            .filter(|(_, progress)| matches!(progress, Progress::Leased { .. }))
            .collect();
        let renewable = self.renewals > 0 && !leased.is_empty();

        let operation = match planned {
            // Creates go in order, interleaved with work on databases already created.
            Some(database) if open.is_empty() || rng.chance(1, 2) => Operation::CreateDatabase {
                database,
                replication_factor: self.replication_factor,
            },
            _ if !open.is_empty() && (!renewable || rng.chance(2, 3)) => {
                let (database, progress) = open[rng.index(open.len())];
                match (progress, self.nodes.get(rng.index(self.nodes.len()))) {
                    (Progress::Created, Some(node)) => Operation::AcquireLease {
                        database: database.clone(),
                        node: node.clone(),
                    },
                    _ => Operation::Locate {
                        database: database.clone(),
                    },
                }
            }
            _ if renewable => {
                self.renewals -= 1;
                match leased[rng.index(leased.len())] {
                    (database, Progress::Leased { node, epoch }) => Operation::RenewLease {
                        database: database.clone(),
                        node: node.clone(),
                        epoch: *epoch,
                    },
                    (database, _) => Operation::Locate {
                        database: database.clone(),
                    },
                }
            }
            Some(database) => Operation::CreateDatabase {
                database,
                replication_factor: self.replication_factor,
            },
            None => return Step::Done,
        };
        self.pause = true;
        Step::Run(operation)
    }

    fn observe(&mut self, operation: &Operation, outcome: &Outcome) {
        match (operation, outcome) {
            (Operation::CreateDatabase { database, .. }, Outcome::Acknowledged(_))
            | (
                Operation::CreateDatabase { database, .. },
                Outcome::Rejected {
                    code: Code::AlreadyExists,
                    ..
                },
            ) => {
                if let Some(progress) = self.progress(database) {
                    *progress = Progress::Created;
                }
            }
            (
                Operation::AcquireLease { database, node },
                Outcome::Acknowledged(Ack::Lease { epoch, .. }),
            ) => {
                let leased = Progress::Leased {
                    node: node.clone(),
                    epoch: *epoch,
                };
                if let Some(progress) = self.progress(database) {
                    *progress = leased;
                }
            }
            (Operation::AcquireLease { database, .. }, _) => {
                if let Some(progress) = self.progress(database) {
                    *progress = Progress::Uncertain;
                }
            }
            (
                Operation::Locate { database },
                Outcome::Acknowledged(Ack::Located { epoch, primary }),
            ) => {
                let next = match primary {
                    Some(node) => Progress::Leased {
                        node: node.clone(),
                        epoch: *epoch,
                    },
                    None => Progress::Created,
                };
                if let Some(progress) = self.progress(database) {
                    *progress = next;
                }
            }
            _ => {}
        }
    }
}

/// The longest pause between two operations of [`CreateAndWrite`], in milliseconds.
const MAX_WRITE_PAUSE_MS: u64 = 30;

/// Creates databases through the data plane, then writes to them and reads them back.
///
/// It creates every database first, repeating each [`Operation::Create`] until it is
/// acknowledged, then sends the given number of [`Operation::Write`]s, each to a random database
/// and with the next value: 0, 1, 2, …. A write is sent once, whatever its outcome, so the
/// history tells which values must survive. One operation in four between the writes is an
/// [`Operation::Read`] instead: of a random database's every value, or of the last value the
/// workload saw acknowledged, so the history has the reads the checkers want. After the last
/// write it reads every database once more, then finishes.
///
/// Every database is created in [`DurabilityMode::Quorum`] unless [`with_durability`] or
/// [`with_durability_for`] says otherwise.
///
/// [`with_durability`]: CreateAndWrite::with_durability
/// [`with_durability_for`]: CreateAndWrite::with_durability_for
#[derive(Debug, Clone)]
pub struct CreateAndWrite {
    replication_factor: u32,
    databases: Vec<(DatabaseName, bool)>,
    durability: Vec<DurabilityMode>,
    writes: u32,
    next_value: i64,
    last_acknowledged: Option<(DatabaseName, i64)>,
    final_reads: Vec<DatabaseName>,
    pause: bool,
}

impl CreateAndWrite {
    /// Creates `databases` databases named `db-0`, `db-1`, …, with `replication_factor`
    /// replicas, then sends `writes` writes, with reads between them.
    ///
    /// # Panics
    ///
    /// If `db-N` isn't a valid database name, which it always is.
    #[must_use]
    pub fn new(databases: u32, replication_factor: u32, writes: u32) -> Self {
        let databases: Vec<(DatabaseName, bool)> = (0..databases)
            .map(|k| {
                let name = format!("db-{k}");
                let name = name
                    .parse()
                    .unwrap_or_else(|error| panic!("'{name}' is a valid name: {error}"));
                (name, false)
            })
            .collect();
        Self {
            replication_factor,
            final_reads: databases
                .iter()
                .rev()
                .map(|(name, _)| name.clone())
                .collect(),
            durability: vec![DurabilityMode::Quorum; databases.len()],
            databases,
            writes,
            next_value: 0,
            last_acknowledged: None,
            pause: false,
        }
    }

    /// Creates every database in `durability`.
    #[must_use]
    pub fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = vec![durability; self.databases.len()];
        self
    }

    /// Creates `db-{index}` in `durability`. An index past the last database changes nothing.
    #[must_use]
    pub fn with_durability_for(mut self, index: usize, durability: DurabilityMode) -> Self {
        if let Some(mode) = self.durability.get_mut(index) {
            *mode = durability;
        }
        self
    }

    /// Returns the durability each database is created in, in name order.
    pub fn durabilities(&self) -> impl Iterator<Item = (&DatabaseName, DurabilityMode)> {
        self.databases
            .iter()
            .zip(&self.durability)
            .map(|((name, _), mode)| (name, *mode))
    }

    fn read(&self, rng: &mut SimRng) -> Operation {
        match &self.last_acknowledged {
            Some((database, value)) if rng.chance(1, 3) => Operation::Read {
                database: database.clone(),
                value: Some(*value),
            },
            _ => {
                let (database, _) = &self.databases[rng.index(self.databases.len())];
                Operation::Read {
                    database: database.clone(),
                    value: None,
                }
            }
        }
    }
}

impl Workload for CreateAndWrite {
    fn next(&mut self, rng: &mut SimRng) -> Step {
        if self.pause {
            self.pause = false;
            return Step::Sleep(Duration::from_millis(1 + rng.below(MAX_WRITE_PAUSE_MS)));
        }
        self.pause = true;
        let pending = self
            .databases
            .iter()
            .zip(&self.durability)
            .find(|((_, created), _)| !created);
        if let Some(((database, _), durability)) = pending {
            return Step::Run(Operation::Create {
                database: database.clone(),
                replication_factor: self.replication_factor,
                durability: *durability,
            });
        }
        if self.databases.is_empty() {
            return Step::Done;
        }
        if self.writes == 0 {
            return match self.final_reads.pop() {
                Some(database) => Step::Run(Operation::Read {
                    database,
                    value: None,
                }),
                None => Step::Done,
            };
        }
        if self.next_value > 0 && rng.chance(1, 4) {
            return Step::Run(self.read(rng));
        }
        self.writes -= 1;
        let (database, _) = &self.databases[rng.index(self.databases.len())];
        let value = self.next_value;
        self.next_value += 1;
        Step::Run(Operation::Write {
            database: database.clone(),
            value,
        })
    }

    fn observe(&mut self, operation: &Operation, outcome: &Outcome) {
        match (operation, outcome) {
            (Operation::Create { database, .. }, Outcome::Acknowledged(_)) => {
                if let Some((_, created)) =
                    self.databases.iter_mut().find(|(name, _)| name == database)
                {
                    *created = true;
                }
            }
            (Operation::Write { database, value }, Outcome::Acknowledged(_)) => {
                self.last_acknowledged = Some((database.clone(), *value));
            }
            _ => {}
        }
    }
}

/// What a [`Operation::SeedingProbe`] asks for: a fork's copy, from the source's primary, or,
/// `staged`, from the attempt's seeder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedingProbe {
    /// The fork identity the request names.
    pub fork: ForkIdentity,
    /// The attempt it names.
    pub attempt: u32,
    /// The database it asks for: the source, or the fork for a staged copy.
    pub database: DatabaseName,
    /// The source incarnation it names.
    pub source_incarnation: Incarnation,
    /// Whether it asks a seeder for its staged copy.
    pub staged: bool,
}

impl fmt::Display for SeedingProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.staged { "staged copy" } else { "source" };
        write!(
            f,
            "{kind} {} for {} attempt {}",
            self.database, self.fork, self.attempt
        )
    }
}

/// A data host's fork work passing a fault hook, as the fleet saw it after a step: the hooks
/// count passes, so `count` passes happened by `at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkPass {
    /// When the fleet saw it.
    pub at: Duration,
    /// The data host.
    pub host: String,
    /// The hook.
    pub point: FaultPoint,
    /// The fork operation the work belonged to, if any.
    pub operation: Option<fleetring_control::fork::OperationId>,
    /// How many passes the host's hooks had counted by then, for the point and operation.
    pub count: u64,
}

/// A fork's phase in control state, as [`ForkTransition`] records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ForkPhaseKind {
    /// Seeding.
    Seeding,
    /// Seeded: its holders are its replica set.
    Seeded,
    /// Ready, its holders kept as its replica set.
    ReadyKept,
    /// Ready, its holders released: it follows placement.
    Released,
    /// Failed.
    Failed,
}

/// A change of a fork's phase in control state, as the fleet saw it after a step, with its
/// replica set then and every data host's rows of it, read from disk then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkTransition {
    /// When the fleet saw it.
    pub at: Duration,
    /// The fork.
    pub identity: ForkIdentity,
    /// The phase it moved to.
    pub phase: ForkPhaseKind,
    /// Its replica set in that phase: the holders, or placement once released.
    pub replica_set: Vec<NodeId>,
    /// Each data host's rows of the fork's writes table then.
    pub rows: ReplicaRows,
}

/// A promotion, or a change of a database's replica set, as the fleet first saw it in control
/// state after a step: the replica set a promotion fences a quorum of from then on, the write
/// quorum over that set, and every data host's rows of the database, read from disk then
/// (FR-102).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencedSeen {
    /// When the fleet saw it.
    pub at: Duration,
    /// The database.
    pub database: DatabaseName,
    /// The epoch a promotion promotes to; `None` for a change of the replica set.
    pub promotion: Option<LeaseEpoch>,
    /// The replica set control state fences: the set `RecordFenced` counts.
    pub replica_set: Vec<NodeId>,
    /// The write quorum W over that set: a quorum Q = R - W + 1 of it intersects every write
    /// held by W of its members.
    pub write_quorum: usize,
    /// Each data host's rows of the database's writes table then.
    pub rows: ReplicaRows,
}

/// A holder's copy of a fork on disk holding the fork's marker, its reset write, as the fleet
/// first saw it after a step (FR-087): read from the copy's files, not from what any node says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkMarkerSeen {
    /// When the fleet saw it.
    pub at: Duration,
    /// The fork.
    pub identity: ForkIdentity,
    /// The holder.
    pub host: String,
}

/// How long a [`Traffic`] client waits between operations, at most, in milliseconds.
const TRAFFIC_PAUSE_MS: u64 = 25;

/// What a [`Traffic`] workload does, shared with its [`TrafficHandle`].
#[derive(Debug, Default)]
struct TrafficState {
    /// Databases still to create through the data plane, with their replication factors.
    creating: VecDeque<(DatabaseName, u32)>,
    /// The databases it writes to and reads, in turn.
    targets: Vec<DatabaseName>,
    next_target: usize,
    next_value: i64,
    stopped: bool,
    pause: bool,
    /// The values acknowledged so far, by database.
    acknowledged: BTreeMap<DatabaseName, usize>,
}

/// Writes continuously to a changing set of databases, with reads between: FR-087's workload,
/// which writes to a fork's source, forks it, and keeps writing to both.
///
/// It creates the databases it is given through the data plane first, then writes the next
/// value, unique across every database, to each target in turn, one operation in four a read of
/// every value instead. A write is sent once, whatever its outcome. A [`TrafficHandle`] adds and
/// removes targets while it runs, and stops it.
#[derive(Debug, Clone, Default)]
pub struct Traffic {
    state: Arc<Mutex<TrafficState>>,
}

/// Steers a running [`Traffic`] workload.
#[derive(Debug, Clone)]
pub struct TrafficHandle {
    state: Arc<Mutex<TrafficState>>,
}

impl Traffic {
    /// Traffic that creates each of `databases` with its replication factor, then writes to
    /// them, its values counting from `first_value`. Returns the handle that steers it too.
    #[must_use]
    pub fn new(
        databases: impl IntoIterator<Item = (DatabaseName, u32)>,
        first_value: i64,
    ) -> (Self, TrafficHandle) {
        let creating: VecDeque<(DatabaseName, u32)> = databases.into_iter().collect();
        let state = Arc::new(Mutex::new(TrafficState {
            targets: creating.iter().map(|(name, _)| name.clone()).collect(),
            creating,
            next_value: first_value,
            ..TrafficState::default()
        }));
        (
            Self {
                state: Arc::clone(&state),
            },
            TrafficHandle { state },
        )
    }
}

impl TrafficHandle {
    fn lock(&self) -> std::sync::MutexGuard<'_, TrafficState> {
        self.state.lock().expect("traffic lock poisoned")
    }

    /// Writes to and reads `database` too from now on.
    pub fn add(&self, database: DatabaseName) {
        let mut state = self.lock();
        if !state.targets.contains(&database) {
            state.targets.push(database);
        }
    }

    /// Stops writing to and reading `database`.
    pub fn remove(&self, database: &DatabaseName) {
        self.lock().targets.retain(|name| name != database);
    }

    /// Creates `database` through the data plane, then writes to it too.
    pub fn create(&self, database: DatabaseName, replication_factor: u32) {
        let mut state = self.lock();
        state
            .creating
            .push_back((database.clone(), replication_factor));
        if !state.targets.contains(&database) {
            state.targets.push(database);
        }
    }

    /// Counts values from `next` on.
    pub fn count_from(&self, next: i64) {
        self.lock().next_value = next;
    }

    /// Stops the workload after the operation in flight.
    pub fn stop(&self) {
        self.lock().stopped = true;
    }

    /// Whether every database it was given has been created.
    #[must_use]
    pub fn created(&self) -> bool {
        self.lock().creating.is_empty()
    }

    /// How many writes to `database` were acknowledged so far.
    #[must_use]
    pub fn acknowledged(&self, database: &DatabaseName) -> usize {
        self.lock().acknowledged.get(database).copied().unwrap_or(0)
    }
}

impl Workload for Traffic {
    fn next(&mut self, rng: &mut SimRng) -> Step {
        let mut state = self.state.lock().expect("traffic lock poisoned");
        if state.stopped {
            return Step::Done;
        }
        if state.pause {
            state.pause = false;
            return Step::Sleep(Duration::from_millis(5 + rng.below(TRAFFIC_PAUSE_MS - 5)));
        }
        state.pause = true;
        if let Some((database, replication_factor)) = state.creating.front().cloned() {
            return Step::Run(Operation::Create {
                database,
                replication_factor,
                durability: DurabilityMode::Quorum,
            });
        }
        if state.targets.is_empty() {
            return Step::Sleep(Duration::from_millis(TRAFFIC_PAUSE_MS));
        }
        let index = state.next_target % state.targets.len();
        state.next_target = state.next_target.wrapping_add(1);
        let database = state.targets[index].clone();
        if rng.chance(1, 4) {
            return Step::Run(Operation::Read {
                database,
                value: None,
            });
        }
        let value = state.next_value;
        state.next_value += 1;
        Step::Run(Operation::Write { database, value })
    }

    fn observe(&mut self, operation: &Operation, outcome: &Outcome) {
        let mut state = self.state.lock().expect("traffic lock poisoned");
        match (operation, outcome) {
            (Operation::Create { database, .. }, Outcome::Acknowledged(_)) => {
                state.creating.retain(|(name, _)| name != database);
            }
            (Operation::Write { database, .. }, Outcome::Acknowledged(_)) => {
                *state.acknowledged.entry(database.clone()).or_default() += 1;
            }
            _ => {}
        }
    }
}

/// Delegates `children` agents from one token, one after another, and uses each child's token
/// twice: to create a database within its prefix, and one outside it, which every node must
/// refuse (ADR-0047, invariant I1).
///
/// A delegation that gets no answer, or `UNAVAILABLE`, is sent again under the same operation
/// ID, as `agent delegate` resends, until it is answered: the control plane must record it once
/// and answer each resend with the same delegation (I7). Any other refusal moves on to the next
/// child.
#[derive(Debug, Clone)]
pub struct Delegations {
    certificate: Principal,
    token: BearerToken,
    /// Distinguishes this workload's operation IDs from another's.
    salt: u8,
    children: u32,
    pause: Duration,
    next: u32,
    step: DelegationStep,
    waiting: bool,
}

/// Where a [`Delegations`] workload is with its current child.
#[derive(Debug, Clone)]
enum DelegationStep {
    /// Asking for the child.
    Delegate,
    /// Using the child's token within its prefix.
    Inside {
        token: BearerToken,
        database: DatabaseName,
    },
    /// Using the child's token outside its prefix.
    Outside {
        token: BearerToken,
        database: DatabaseName,
    },
    /// Pausing before the next child.
    Pause,
}

impl Delegations {
    /// A workload that delegates `children` agents, `<salt>c0` to `<salt>c<children - 1>`
    /// below the token's principal, on a connection whose certificate names `certificate`,
    /// carrying `token`, pausing `pause` after each.
    #[must_use]
    pub fn new(
        certificate: Principal,
        token: BearerToken,
        salt: u8,
        children: u32,
        pause: Duration,
    ) -> Self {
        Self {
            certificate,
            token,
            salt,
            children,
            pause,
            next: 0,
            step: DelegationStep::Delegate,
            waiting: false,
        }
    }

    /// The operation ID of the delegation of child `child`: the same for every resend.
    fn operation(&self, child: u32) -> RequestId {
        let mut bytes = [self.salt; 16];
        bytes[12..].copy_from_slice(&child.to_be_bytes());
        RequestId::from_bytes(bytes)
    }

    fn segment(&self, child: u32) -> String {
        format!("s{}c{child}", self.salt)
    }

    fn advance(&mut self) {
        self.step = DelegationStep::Pause;
    }
}

impl Workload for Delegations {
    fn next(&mut self, _rng: &mut SimRng) -> Step {
        if self.waiting {
            self.waiting = false;
            return Step::Sleep(SCRIPT_RETRY);
        }
        if self.next >= self.children {
            return Step::Done;
        }
        match &self.step {
            DelegationStep::Delegate => Step::Run(Operation::Delegate {
                certificate: self.certificate.clone(),
                token: self.token.clone(),
                operation: self.operation(self.next),
                segment: self.segment(self.next),
                scopes: Vec::new(),
                lifetime: None,
            }),
            DelegationStep::Inside { token, database }
            | DelegationStep::Outside { token, database } => Step::Run(Operation::CreateAs {
                certificate: self.certificate.clone(),
                token: token.clone(),
                database: database.clone(),
            }),
            DelegationStep::Pause => {
                self.next += 1;
                self.step = DelegationStep::Delegate;
                Step::Sleep(self.pause)
            }
        }
    }

    fn observe(&mut self, operation: &Operation, outcome: &Outcome) {
        let retry = match outcome {
            Outcome::Indeterminate { .. } => true,
            Outcome::Rejected { code, .. } => *code == Code::Unavailable,
            Outcome::Acknowledged(_) => false,
        };
        match (operation, &self.step) {
            (Operation::Delegate { .. }, DelegationStep::Delegate) => {
                if retry {
                    self.waiting = true;
                    return;
                }
                let Some(Ack::Delegated {
                    token, principal, ..
                }) = outcome.ack()
                else {
                    self.advance();
                    return;
                };
                match format!("{principal}-db").parse() {
                    Ok(database) => {
                        self.step = DelegationStep::Inside {
                            token: token.clone(),
                            database,
                        };
                    }
                    Err(_) => self.advance(),
                }
            }
            (Operation::CreateAs { .. }, DelegationStep::Inside { token, .. }) => {
                if matches!(outcome, Outcome::Indeterminate { .. }) {
                    self.waiting = true;
                    return;
                }
                let outside = format!("agent-43-s{}x{}", self.salt, self.next).parse();
                match outside {
                    Ok(database) => {
                        self.step = DelegationStep::Outside {
                            token: token.clone(),
                            database,
                        };
                    }
                    Err(_) => self.advance(),
                }
            }
            (Operation::CreateAs { .. }, DelegationStep::Outside { .. }) => {
                if matches!(outcome, Outcome::Indeterminate { .. }) {
                    self.waiting = true;
                    return;
                }
                self.advance();
            }
            _ => {}
        }
    }
}
