//! Invariants checked during and after a run (ADR-0020, point 4).
//!
//! Each check is a function of what it needs: control state, a log on disk, the published
//! views, or the [`History`]. [`Fleet`](crate::sim::Fleet) runs them at the points where
//! they hold:
//!
//! - [`ring_converged`] once faults heal ([`Fleet::await_ring`](crate::sim::Fleet::await_ring));
//! - [`control_log_replays`] and [`acknowledged_control_survives`] whenever the control host is
//!   crashed or stopped, and at the end of a run;
//! - [`one_primary_per_epoch`] at every check and at the end;
//! - the history checker ([`history::check`](crate::sim::history::check)), which includes
//!   [`acknowledged_writes_replicated`], whenever the test asks
//!   ([`Fleet::check_history`](crate::sim::Fleet::check_history)), once replicas converge,
//!   and at the end, and [`replicas_converged`] once the network heals
//!   ([`Fleet::await_replicas_converged`](crate::sim::Fleet::await_replicas_converged));
//! - [`change_stream_consistent`] and [`change_streams_resume`] at the end, for every file sink a
//!   host is configured with
//!   ([`Fleet::check_change_streams`](crate::sim::Fleet::check_change_streams));
//! - [`lease_events_match_control`] at the end, over what the lease watchers learned and the
//!   leases the control log granted (FR-042).
//! - [`chains_agree`] once replicas converge and at the end, over the hash chains of every
//!   running replica the primary counts as following its log
//!   ([`Fleet::check_chains`](crate::sim::Fleet::check_chains)).
//! - [`copies_kept_until_dropped`] at the end, over the data hosts' disks, the control log, and
//!   what the lease watchers saw (ADR-0042).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{Action, Command, ControlConfig, DatabaseName, LeaseEpoch, State, apply};

use crate::MembershipView;
use crate::changes::{ChangeRecord, RecordContent, unhex};
use crate::control::CommandLog;
use crate::engine::{Position, Value};
use crate::replication::ChainHash;
use crate::sim::workload::{Ack, History, Operation, OutcomeClass, Record, WRITES_TABLE};

/// An invariant the checker enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Invariant {
    /// After faults heal, every live data node publishes the control plane's ring.
    RingConverged,
    /// Replaying the control log reproduces the control state.
    ControlLogReplays,
    /// No acknowledged join, create, or lease is lost, across control node crashes too.
    AcknowledgedControlSurvives,
    /// No two nodes were granted a database's lease at the same epoch.
    OnePrimaryPerEpoch,
    /// Every acknowledged write is on at least W of its database's replicas.
    AcknowledgedWritesReplicated,
    /// Every acknowledged write is in every read that began after it was acknowledged.
    AcknowledgedWritesRead,
    /// A refused write is on no replica and in no read.
    NoPhantomWrites,
    /// A read sees every write its own client had acknowledged when the read began.
    ReadYourWrites,
    /// A client never sees an acknowledged write disappear that one of its earlier reads saw.
    MonotonicReads,
    /// Once the network heals, every replica of a database holds the same rows.
    ReplicasConverged,
    /// A change sink's file holds the database's writes in commit order, with no gap and no
    /// write that was refused or never made (FR-039).
    ChangeStreamConsistent,
    /// After a failover, a change stream continues at a mark the old primary replicated, and
    /// repeats no more than the bound allows (FR-041, ADR-0028).
    ChangeStreamResumes,
    /// What the lease watchers learned matches the leases control granted (FR-042): a node
    /// reports holding a database only at an epoch control leased to it, no two nodes report
    /// holding one epoch, each node's events for a database arrive in epoch order, and every
    /// epoch a node acknowledged writes under was reported by that node.
    LeaseEventsMatchControl,
    /// Every replica that follows a database's log holds the same hash chain at every position
    /// they have in common (FR-044).
    ChainAgrees,
    /// Every node call an impostor makes on a database it doesn't host is refused for its
    /// binding, or can't be checked and waits; none is served (FR-050).
    NodeCallsBound,
    /// A copy restored from an archive holds a prefix of the database's committed writes, in
    /// commit order, with no write that was refused or never made, and, restored to the latest
    /// point, every write acknowledged before the archive caught up (FR-049).
    ArchiveRestoresAcknowledgedPrefix,
    /// No node deletes a copy of a database the control log never dropped or expired: every
    /// node that served such a database still holds a copy of it (ADR-0042).
    CopiesKeptUntilDropped,
}

impl fmt::Display for Invariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RingConverged => "ring converged",
            Self::ControlLogReplays => "control log replays",
            Self::AcknowledgedControlSurvives => "acknowledged control operations survive",
            Self::OnePrimaryPerEpoch => "one primary per epoch",
            Self::AcknowledgedWritesReplicated => "acknowledged writes are on a write quorum",
            Self::AcknowledgedWritesRead => "acknowledged writes are in every later read",
            Self::NoPhantomWrites => "no phantom writes",
            Self::ReadYourWrites => "read your writes",
            Self::MonotonicReads => "monotonic reads",
            Self::ReplicasConverged => "replicas converged",
            Self::ChangeStreamConsistent => "change stream consistent",
            Self::ChangeStreamResumes => "change stream resumes at a replicated mark",
            Self::LeaseEventsMatchControl => "lease events match control",
            Self::ChainAgrees => "hash chains agree",
            Self::NodeCallsBound => "node calls bound to placement",
            Self::ArchiveRestoresAcknowledgedPrefix => "archive restores an acknowledged prefix",
            Self::CopiesKeptUntilDropped => "copies kept until dropped",
        })
    }
}

/// A broken invariant, what broke it, and, from the history checker, the history around it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    invariant: Invariant,
    detail: String,
    excerpt: Vec<String>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invariant violated ({}): {}",
            self.invariant, self.detail
        )?;
        if !self.excerpt.is_empty() {
            f.write_str("\n  history around it (> marks the operations involved):")?;
            for line in &self.excerpt {
                write!(f, "\n  {line}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for Violation {}

impl Violation {
    pub(crate) fn new(invariant: Invariant, detail: impl Into<String>) -> Self {
        Self {
            invariant,
            detail: detail.into(),
            excerpt: Vec::new(),
        }
    }

    /// Attaches the lines of history around the violation.
    #[must_use]
    pub(crate) fn with_excerpt(mut self, excerpt: Vec<String>) -> Self {
        self.excerpt = excerpt;
        self
    }

    /// Returns the history around the violation, one event per line, if the check had one.
    #[must_use]
    pub fn excerpt(&self) -> &[String] {
        &self.excerpt
    }

    /// Returns the invariant that broke.
    #[must_use]
    pub fn invariant(&self) -> Invariant {
        self.invariant
    }

    /// Returns what broke it.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Checks that every node in `views` is a live member of `state`, publishes its membership epoch
/// and ring fingerprint, and agrees with it on every member's liveness.
///
/// # Errors
///
/// [`Invariant::RingConverged`] naming the first node that disagrees.
pub fn ring_converged<'a>(
    state: &State,
    views: impl IntoIterator<Item = (&'a str, &'a MembershipView)>,
) -> Result<(), Violation> {
    let fingerprint = state.ring().ok().map(|ring| ring.fingerprint());
    let violation = |detail: String| Violation::new(Invariant::RingConverged, detail);
    for (node, view) in views {
        let live = node
            .parse::<NodeId>()
            .ok()
            .and_then(|id| state.liveness(&id, state.now()));
        if live != Some(fleetring_control::Liveness::Live) {
            return Err(violation(format!(
                "control sees {node} as {live:?}, not a live member"
            )));
        }
        if view.epoch() != state.membership_epoch() || view.fingerprint() != fingerprint {
            return Err(violation(format!(
                "{node} publishes epoch {} ring {:?}, control has epoch {} ring {:?}",
                view.epoch().get(),
                view.fingerprint().map(|f| f.to_string()),
                state.membership_epoch().get(),
                fingerprint.map(|f| f.to_string()),
            )));
        }
        for member in state.members() {
            let theirs = view.liveness(member.id());
            let ours = state.liveness(member.id(), state.now());
            if theirs != ours {
                return Err(violation(format!(
                    "{node} sees {} as {theirs:?}, control sees {ours:?}",
                    member.id()
                )));
            }
        }
    }
    Ok(())
}

/// Who held each database's lease at each epoch: what control granted, by database and epoch.
pub type LeaseHolders = BTreeMap<(DatabaseName, LeaseEpoch), NodeId>;

/// Checks that no node deleted a copy of a database the control log never dropped or expired
/// (ADR-0042): every node its lease watcher in `history` saw serving such a database still holds
/// a copy of it, as `has_copy` finds on its disk. A node deletes a copy only after it saw the
/// database's drop, so a copy that goes missing without one was deleted by absence.
///
/// # Errors
///
/// [`Invariant::CopiesKeptUntilDropped`] naming the first node whose copy is gone.
pub fn copies_kept_until_dropped(
    log: &ControlLog,
    history: &History,
    has_copy: impl Fn(&str, &DatabaseName) -> bool,
) -> Result<(), Violation> {
    let dropped: BTreeSet<&DatabaseName> = log
        .commands()
        .iter()
        .filter_map(|command| match &command.action {
            Action::DropDatabase { name } | Action::ExpireDatabase { name } => Some(name),
            _ => None,
        })
        .collect();
    let served: BTreeSet<(&str, &DatabaseName)> = history
        .lease_observations()
        .iter()
        .filter(|observation| observation.is_holding())
        .map(|observation| (observation.node(), observation.database()))
        .collect();
    for (node, database) in served {
        if dropped.contains(database) || has_copy(node, database) {
            continue;
        }
        return Err(Violation::new(
            Invariant::CopiesKeptUntilDropped,
            format!(
                "{node} served {database} and no longer has a copy of it, though the control log \
                 never dropped it"
            ),
        ));
    }
    Ok(())
}

/// What [`control_log_replays`] read from a control log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlLog {
    state: State,
    commands: Vec<Command>,
    holders: LeaseHolders,
}

impl ControlLog {
    /// Returns the state recovery builds.
    #[must_use]
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Returns every logged command, in log order.
    #[must_use]
    pub fn commands(&self) -> &[Command] {
        &self.commands
    }

    /// Returns the holder of each database's lease at each epoch the log ever granted one at.
    #[must_use]
    pub fn lease_holders(&self) -> &LeaseHolders {
        &self.holders
    }

    /// Returns the recovered state, consuming the record.
    #[must_use]
    pub fn into_state(self) -> State {
        self.state
    }
}

/// Opens the control log at `path` and checks that recovery (snapshot plus tail) equals a replay
/// of every logged command from genesis.
///
/// Call it only while no control node has the log open.
///
/// # Errors
///
/// [`Invariant::ControlLogReplays`] if the log can't be read or the two states differ.
pub fn control_log_replays(path: &Path) -> Result<ControlLog, Violation> {
    let violation = |detail: String| Violation::new(Invariant::ControlLogReplays, detail);
    if !path.exists() {
        return Err(violation(format!("no control log at {}", path.display())));
    }
    let log = CommandLog::open(path, ControlConfig::new())
        .map_err(|error| violation(format!("can't open the log: {error}")))?;
    let recovered = log
        .recover()
        .map_err(|error| violation(format!("can't recover the log: {error}")))?;
    let commands: Vec<Command> = log
        .commands()
        .map_err(|error| violation(format!("can't read the log: {error}")))?
        .into_iter()
        .map(|(_, command)| command)
        .collect();
    let mut replayed = State::new(log.genesis());
    let mut holders = LeaseHolders::new();
    for command in &commands {
        // A rejected command changes nothing, in recovery as in the replay.
        let _outcome = apply(&mut replayed, command.clone());
        record_holders(&replayed, &mut holders);
    }
    if recovered.state != replayed {
        return Err(violation(format!(
            "recovery after {} commands differs from a replay from genesis",
            recovered.last_index.get()
        )));
    }
    Ok(ControlLog {
        state: recovered.state,
        commands,
        holders,
    })
}

/// Records every lease `state` holds now in `holders`. A lease's epoch names one holder for good,
/// so a holder already recorded for an epoch stays.
fn record_holders(state: &State, holders: &mut LeaseHolders) {
    for database in state.databases() {
        if let Some(lease) = database.lease() {
            holders
                .entry((database.name().clone(), lease.epoch()))
                .or_insert_with(|| lease.holder().clone());
        }
    }
}

/// Checks that `state` holds everything `history` says was acknowledged: every joined member
/// that wasn't later asked to leave, every created database, and every granted or renewed lease,
/// unless a later epoch replaced it. A database created with an expiry
/// ([`Operation::CreateExpiring`]) may be gone, dropped once its expiry passed.
///
/// # Errors
///
/// [`Invariant::AcknowledgedControlSurvives`] naming the first lost acknowledgement.
pub fn acknowledged_control_survives(state: &State, history: &History) -> Result<(), Violation> {
    let violation = |detail: String| Violation::new(Invariant::AcknowledgedControlSurvives, detail);
    let leaving: Vec<&NodeId> = history
        .records()
        .iter()
        .filter_map(|record| match record.operation() {
            Operation::Leave { node } => Some(node),
            _ => None,
        })
        .collect();
    for (operation, ack) in history.acknowledged() {
        match (operation, ack) {
            (Operation::Join { node } | Operation::JoinWith { node, .. }, Ack::Joined { .. }) => {
                if state.member(node).is_none() && !leaving.contains(&node) {
                    return Err(violation(format!("acknowledged member {node} is lost")));
                }
            }
            (Operation::CreateDatabase { database, .. }, Ack::Created)
            | (Operation::Create { database, .. }, Ack::Opened { .. }) => {
                if state.database(database).is_none() {
                    return Err(violation(format!(
                        "acknowledged database {database} is lost"
                    )));
                }
            }
            (
                Operation::AcquireLease { database, node }
                | Operation::RenewLease { database, node, .. },
                Ack::Lease {
                    epoch,
                    expires_at_ms,
                },
            ) => check_lease(state, database, node, *epoch, *expires_at_ms).map_err(violation)?,
            _ => {}
        }
    }
    Ok(())
}

fn check_lease(
    state: &State,
    database: &DatabaseName,
    node: &NodeId,
    epoch: LeaseEpoch,
    expires_at_ms: u64,
) -> Result<(), String> {
    let lost = || {
        format!(
            "acknowledged lease on {database} for {node} at epoch {} is lost",
            epoch.get()
        )
    };
    let db = state.database(database).ok_or_else(lost)?;
    if db.epoch() < epoch {
        return Err(lost());
    }
    match db.lease() {
        Some(lease) if lease.epoch() == epoch => {
            if lease.holder() != node || lease.expires_at().as_millis() < expires_at_ms {
                return Err(format!(
                    "{}: control holds it for {} until {}, acknowledged until {expires_at_ms}",
                    lost(),
                    lease.holder(),
                    lease.expires_at().as_millis()
                ));
            }
            Ok(())
        }
        Some(lease) if lease.epoch() > epoch => Ok(()),
        _ => Err(lost()),
    }
}

/// Checks that no two nodes were acknowledged as a database's lease holder at the same epoch, and
/// that at most one node acknowledged writes to a database as its primary in any epoch.
///
/// Lease holders come from granted and renewed leases and from data-plane creations; primaries
/// from the node and epoch each acknowledged write reports.
///
/// # Errors
///
/// [`Invariant::OnePrimaryPerEpoch`] naming the database, the epoch, and both holders.
pub fn one_primary_per_epoch(history: &History) -> Result<(), Violation> {
    match conflicting_primaries(history) {
        Some(conflict) => Err(conflict.violation()),
        None => Ok(()),
    }
}

/// Two acknowledgements that name different nodes as a database's primary at one epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrimaryConflict<'h> {
    pub(crate) database: &'h DatabaseName,
    pub(crate) epoch: LeaseEpoch,
    pub(crate) first: &'h Record,
    pub(crate) second: &'h Record,
    pub(crate) first_node: &'h NodeId,
    pub(crate) second_node: &'h NodeId,
}

impl PrimaryConflict<'_> {
    pub(crate) fn violation(&self) -> Violation {
        Violation::new(
            Invariant::OnePrimaryPerEpoch,
            format!(
                "{} has two primaries at epoch {}: {} and {}",
                self.database,
                self.epoch.get(),
                self.first_node,
                self.second_node
            ),
        )
    }
}

/// Finds the first pair of acknowledgements that name different primaries at one epoch.
pub(crate) fn conflicting_primaries(history: &History) -> Option<PrimaryConflict<'_>> {
    let mut holders: BTreeMap<(&DatabaseName, LeaseEpoch), (&NodeId, &Record)> = BTreeMap::new();
    for record in history.records() {
        let Some(ack) = record.outcome().ack() else {
            continue;
        };
        let (database, node, epoch) = match (record.operation(), ack) {
            (
                Operation::Write { database, .. },
                Ack::Written {
                    primary: Some(node),
                    epoch,
                    ..
                },
            ) => (database, node, *epoch),
            (
                Operation::AcquireLease { database, node }
                | Operation::RenewLease { database, node, .. },
                Ack::Lease { epoch, .. },
            )
            | (
                Operation::Create { database, .. },
                Ack::Opened {
                    primary: node,
                    epoch,
                    ..
                },
            ) => (database, node, *epoch),
            _ => continue,
        };
        let (holder, first) = *holders.entry((database, epoch)).or_insert((node, record));
        if holder != node {
            return Some(PrimaryConflict {
                database,
                epoch,
                first,
                second: record,
                first_node: holder,
                second_node: node,
            });
        }
    }
    None
}

/// Checks what the lease watchers learned ([`History::lease_observations`]) against the leases
/// control granted, `holders` (FR-042):
///
/// - a node reports holding a database, in a snapshot or as gained, only at an epoch control
///   leased to that node;
/// - no two nodes report holding a database at one epoch;
/// - each node's events for a database arrive in epoch order;
/// - every epoch a node acknowledged writes under, as the history records them, was reported
///   held or gained by that node, since a node reports a lease before it acknowledges a write
///   under it.
///
/// # Errors
///
/// [`Invariant::LeaseEventsMatchControl`] naming the first observation that breaks it.
pub fn lease_events_match_control(
    history: &History,
    holders: &LeaseHolders,
) -> Result<(), Violation> {
    let violation = |detail: String| Violation::new(Invariant::LeaseEventsMatchControl, detail);
    let observations = history.lease_observations();
    let mut reported: BTreeMap<(&DatabaseName, LeaseEpoch), &str> = BTreeMap::new();
    let mut last_epoch: BTreeMap<(&str, &DatabaseName), LeaseEpoch> = BTreeMap::new();
    for observation in observations {
        let key = (observation.database(), observation.epoch());
        if observation.is_holding() {
            match holders.get(&(observation.database().clone(), observation.epoch())) {
                Some(holder) if holder.as_str() == observation.node() => {}
                Some(holder) => {
                    return Err(violation(format!(
                        "{observation}: control leased that epoch to {holder}"
                    )));
                }
                None => {
                    return Err(violation(format!(
                        "{observation}: control never leased that epoch"
                    )));
                }
            }
            match reported.get(&key) {
                Some(other) if *other != observation.node() => {
                    return Err(violation(format!(
                        "{observation}: {other} reported holding it too"
                    )));
                }
                _ => {
                    reported.insert(key, observation.node());
                }
            }
        }
        let order = (observation.node(), observation.database());
        if let Some(last) = last_epoch.get(&order)
            && observation.epoch() < *last
        {
            return Err(violation(format!(
                "{observation}: after epoch {} on the same stream",
                last.get()
            )));
        }
        last_epoch.insert(order, observation.epoch());
    }
    for record in history.records() {
        let (
            Operation::Write { database, .. },
            Some(Ack::Written {
                primary: Some(primary),
                epoch,
                ..
            }),
        ) = (record.operation(), record.outcome().ack())
        else {
            continue;
        };
        let covered = observations.iter().any(|observation| {
            observation.is_holding()
                && observation.node() == primary.as_str()
                && observation.database() == database
                && observation.epoch() == *epoch
        });
        if !covered {
            return Err(violation(format!(
                "{primary} acknowledged write {} to {database} at epoch {}, but its watcher never saw it hold that lease",
                record.id(),
                epoch.get()
            )));
        }
    }
    Ok(())
}

/// The rows each replica of a database holds, by host.
pub type ReplicaRows = BTreeMap<String, BTreeSet<i64>>;

/// The hash chain each replica of a database holds, by host: the hash at every position the
/// replica still keeps one for.
pub type ReplicaChains = BTreeMap<String, BTreeMap<Position, ChainHash>>;

/// Checks that every replica in `chains` holds the same hash at every position any two of them
/// have in common.
///
/// # Errors
///
/// [`Invariant::ChainAgrees`] naming the two replicas and the first position they differ at.
pub fn chains_agree(database: &DatabaseName, chains: &ReplicaChains) -> Result<(), Violation> {
    let mut all = chains.iter();
    while let Some((host, chain)) = all.next() {
        for (other_host, other) in all.clone() {
            let differs = chain.iter().find_map(|(position, hash)| {
                other
                    .get(position)
                    .filter(|theirs| *theirs != hash)
                    .map(|theirs| (position, hash, theirs))
            });
            if let Some((position, hash, theirs)) = differs {
                return Err(Violation::new(
                    Invariant::ChainAgrees,
                    format!(
                        "{database}: at {position}, {host} holds {hash} but {other_host} holds {theirs}"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Checks that every impostor operation in `history` ([`Operation::ImpostorReplicate`],
/// [`Operation::ImpostorFence`], [`Operation::ImpostorSnapshot`]) ended refused with
/// `PERMISSION_DENIED` naming a binding, or with `UNAVAILABLE` because the check couldn't be
/// made yet, and that none was served (FR-050).
///
/// # Errors
///
/// [`Invariant::NodeCallsBound`] naming the first impostor operation that ended otherwise.
pub fn impostor_refused(history: &History) -> Result<(), Violation> {
    for record in history.records() {
        let impostor = matches!(
            record.operation(),
            Operation::ImpostorReplicate { .. }
                | Operation::ImpostorFence { .. }
                | Operation::ImpostorSnapshot { .. }
        );
        if !impostor {
            continue;
        }
        let bound = match record.outcome().ack() {
            Some(Ack::Refused {
                code: tonic::Code::PermissionDenied,
                binding: Some(_),
            })
            | Some(Ack::Refused {
                code: tonic::Code::Unavailable,
                ..
            }) => true,
            _ => matches!(
                record.outcome(),
                crate::sim::Outcome::Indeterminate {
                    code: Some(tonic::Code::Unavailable)
                }
            ),
        };
        if !bound {
            return Err(Violation::new(
                Invariant::NodeCallsBound,
                format!(
                    "{} {} by {} ended {}",
                    record.id(),
                    record.operation(),
                    record.client(),
                    record.outcome()
                ),
            ));
        }
    }
    Ok(())
}

/// Checks that every write to `database` that `history` says was acknowledged is held by at least
/// `required` of `replicas`.
///
/// # Errors
///
/// [`Invariant::AcknowledgedWritesReplicated`] naming the first write held by too few replicas,
/// and the replicas that hold it.
pub fn acknowledged_writes_replicated(
    history: &History,
    database: &DatabaseName,
    replicas: &ReplicaRows,
    required: usize,
) -> Result<(), Violation> {
    for (written, value) in history.acknowledged_writes() {
        if written != database {
            continue;
        }
        let holders: Vec<&str> = replicas
            .iter()
            .filter(|(_, rows)| rows.contains(&value))
            .map(|(host, _)| host.as_str())
            .collect();
        if holders.len() < required {
            return Err(Violation::new(
                Invariant::AcknowledgedWritesReplicated,
                format!(
                    "acknowledged write {value} to {database} is on {holders:?}, fewer than {required} replicas"
                ),
            ));
        }
    }
    Ok(())
}

/// Checks that every replica of `database` holds the same rows.
///
/// # Errors
///
/// [`Invariant::ReplicasConverged`] naming two replicas that differ.
pub fn replicas_converged(
    database: &DatabaseName,
    replicas: &ReplicaRows,
) -> Result<(), Violation> {
    let mut all = replicas.iter();
    let Some((first, rows)) = all.next() else {
        return Ok(());
    };
    for (host, other) in all {
        if other != rows {
            let only_first: Vec<&i64> = rows.difference(other).take(5).collect();
            let only_other: Vec<&i64> = other.difference(rows).take(5).collect();
            return Err(Violation::new(
                Invariant::ReplicasConverged,
                format!(
                    "{database}: {first} has {} rows, {host} has {}; only on {first}: {only_first:?}, only on {host}: {only_other:?}",
                    rows.len(),
                    other.len()
                ),
            ));
        }
    }
    Ok(())
}

/// How many records a failover may make a change stream repeat: what the old primary delivered
/// after the newest mark that reached the write quorum. With writes flowing that is the last
/// delivery, and the one before it when the next write was already running as the mark was staged
/// (ADR-0028, point 6).
pub const REPEATS_PER_FAILOVER: usize = 2;

/// Checks the lines a file sink on `host` wrote for `database`, in file order, against
/// `history`: the records' positions never go backwards, each record continues the one before
/// it or repeats delivered positions (a duplicate, which at-least-once allows), and a record may
/// start from the log's beginning only when the pump started or restarted there. A record that
/// starts elsewhere than where the host left off is a resume, and is accepted when its start is
/// a replicated mark: the end of some record, of any host, at an earlier epoch. `marks` holds the
/// `(epoch, end)` of every record of the sink across hosts. Every row is an insert into the writes
/// table of a value some write in the history made, and never one the history says was refused.
///
/// # Errors
///
/// [`Invariant::ChangeStreamConsistent`] naming the first record that breaks it.
pub fn change_stream_consistent(
    history: &History,
    database: &DatabaseName,
    host: &str,
    records: &[ChangeRecord],
    marks: &BTreeSet<(u64, Position)>,
) -> Result<(), Violation> {
    let violation = |detail: String| {
        Violation::new(
            Invariant::ChangeStreamConsistent,
            format!("{host}'s stream of {database}: {detail}"),
        )
    };
    // Whether some attempt at each value may have taken effect: `false` only if every attempt
    // was refused.
    let mut written: BTreeMap<i64, bool> = BTreeMap::new();
    for record in history.records() {
        if let Operation::Write {
            database: target,
            value,
        } = record.operation()
            && target == database
        {
            let made = record.class() != OutcomeClass::Fail;
            let entry = written.entry(*value).or_insert(made);
            *entry |= made;
        }
    }
    let mut delivered: Option<Position> = None;
    let mut epoch = 0;
    for (index, record) in records.iter().enumerate() {
        let position = unhex(&record.position)
            .map(Position::from)
            .map_err(|_| violation(format!("record {index} has a position that isn't hex")))?;
        let start = record
            .start
            .as_deref()
            .map(unhex)
            .transpose()
            .map_err(|_| violation(format!("record {index} has a start that isn't hex")))?
            .map(Position::from);
        if record.database != database.as_str() {
            return Err(violation(format!(
                "record {index} is for {}",
                record.database
            )));
        }
        if record.epoch < epoch {
            return Err(violation(format!(
                "record {index} is from epoch {}, after epoch {epoch}",
                record.epoch
            )));
        }
        epoch = record.epoch;
        match (&delivered, &start) {
            // The first record, or a restart from the log's beginning.
            (_, None) => {}
            // A duplicate of what was delivered already.
            (Some(delivered), Some(_)) if position <= *delivered => {}
            // A resume at a mark an earlier epoch's primary replicated.
            (_, Some(start))
                if marks
                    .iter()
                    .any(|(at, end)| *at < record.epoch && end == start) => {}
            (Some(delivered), Some(start)) if start > delivered => {
                return Err(violation(format!(
                    "record {index} starts at {start}, past the last delivered position {delivered}: a gap"
                )));
            }
            (None, Some(start)) => {
                return Err(violation(format!(
                    "record {index}, the first, starts at {start} instead of the log's beginning"
                )));
            }
            (Some(_), Some(_)) => {}
        }
        if delivered.as_ref() > Some(&position) {
            return Err(violation(format!(
                "record {index} ends at {position}, before the last delivered position"
            )));
        }
        delivered = Some(position.clone());
        let RecordContent::Logical { rows } = &record.content else {
            return Err(violation(format!("record {index} isn't a logical change")));
        };
        for row in rows {
            if row.table != WRITES_TABLE {
                continue;
            }
            let value = row
                .values()
                .iter()
                .find(|column| column.name == "value")
                .and_then(|column| match column.value {
                    Value::Integer(value) => Some(value),
                    _ => None,
                });
            let Some(value) = value else {
                return Err(violation(format!(
                    "record {index} has a {} row without an integer value: {row:?}",
                    row.operation
                )));
            };
            match written.get(&value) {
                Some(true) => {}
                Some(false) => {
                    return Err(violation(format!(
                        "record {index} delivers write {value}, which was refused"
                    )));
                }
                None => {
                    return Err(violation(format!(
                        "record {index} delivers write {value}, which no client made"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Checks, across every host's file sink stream of `database` (each a host and its records, in
/// file order), that a failover continues the stream where the old primary's replicated mark left
/// it. Taking the streams by epoch, the first record each host wrote at an epoch after the first
/// starts at a position some earlier epoch delivered up to, never at the log's beginning, since
/// something was delivered before; and the records of earlier epochs that end after that start,
/// the repeats, number at most `repeats_per_failover`. A record is counted once however often a
/// restarted pump delivered it again.
///
/// # Errors
///
/// [`Invariant::ChangeStreamResumes`] naming the epoch whose first record breaks it.
pub fn change_streams_resume(
    database: &DatabaseName,
    streams: &[(String, Vec<ChangeRecord>)],
    repeats_per_failover: usize,
) -> Result<(), Violation> {
    let violation = |detail: String| {
        Violation::new(
            Invariant::ChangeStreamResumes,
            format!("{database}'s stream: {detail}"),
        )
    };
    // Every record as (epoch, start, end, host), with its positions decoded.
    let mut all: Vec<(u64, Option<Position>, Position, &str)> = Vec::new();
    for (host, records) in streams {
        for record in records {
            let end = unhex(&record.position)
                .map(Position::from)
                .map_err(|_| violation(format!("{host} has a position that isn't hex")))?;
            let start = record
                .start
                .as_deref()
                .map(unhex)
                .transpose()
                .map_err(|_| violation(format!("{host} has a start that isn't hex")))?
                .map(Position::from);
            all.push((record.epoch, start, end, host));
        }
    }
    let epochs: BTreeSet<u64> = all.iter().map(|(epoch, ..)| *epoch).collect();
    for epoch in epochs.iter().skip(1) {
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for (_, start, _, host) in all.iter().filter(|(at, ..)| at == epoch) {
            if !seen.insert(host) {
                continue;
            }
            let earlier = || all.iter().filter(|(at, ..)| at < epoch);
            let Some(start) = start else {
                return Err(violation(format!(
                    "{host}'s first record at epoch {epoch} starts from the log's beginning, \
                     though an earlier epoch delivered"
                )));
            };
            if !earlier().any(|(_, _, end, _)| end == start) {
                return Err(violation(format!(
                    "{host}'s first record at epoch {epoch} starts at {start}, which no earlier \
                     epoch delivered up to"
                )));
            }
            let repeats: BTreeSet<(u64, &Position)> = earlier()
                .filter(|(_, _, end, _)| end > start)
                .map(|(at, _, end, _)| (*at, end))
                .collect();
            if repeats.len() > repeats_per_failover {
                return Err(violation(format!(
                    "{host}'s resume at epoch {epoch} from {start} repeats {} records, more than \
                     the {repeats_per_failover} a failover may",
                    repeats.len()
                )));
            }
        }
    }
    Ok(())
}

/// Checks a copy restored from an archive: its writes-table values, `restored`, in commit order,
/// are the first values of `committed`, the database's values in commit order at the end of the
/// run; each was written by some write the history records and not refused by all of its
/// attempts; and, with `complete_before`, every write acknowledged at or before that time is
/// among them.
///
/// The restore verified every batch against the hash chain, so a prefix of the committed log is
/// what it must hold; this checks the rows say so too.
///
/// # Errors
///
/// [`Invariant::ArchiveRestoresAcknowledgedPrefix`] naming the first value that breaks it.
pub fn archive_restores_prefix(
    history: &History,
    database: &DatabaseName,
    restored: &[i64],
    committed: &[i64],
    complete_before: Option<Duration>,
) -> Result<(), Violation> {
    let violation = |detail: String| {
        Violation::new(
            Invariant::ArchiveRestoresAcknowledgedPrefix,
            format!("the copy of {database} restored from its archive: {detail}"),
        )
    };
    if let Some((index, (got, want))) = restored
        .iter()
        .zip(committed)
        .enumerate()
        .find(|(_, (got, want))| got != want)
    {
        return Err(violation(format!(
            "row {index} holds {got} where the database committed {want}"
        )));
    }
    if restored.len() > committed.len() {
        return Err(violation(format!(
            "it holds {} rows, more than the {} the database committed",
            restored.len(),
            committed.len()
        )));
    }
    let mut made: BTreeMap<i64, bool> = BTreeMap::new();
    let mut acknowledged = Vec::new();
    for record in history.records() {
        if let Operation::Write {
            database: target,
            value,
        } = record.operation()
            && target == database
        {
            *made.entry(*value).or_insert(false) |= record.class() != OutcomeClass::Fail;
            if record.class() == OutcomeClass::Ok
                && complete_before.is_some_and(|before| record.completed_at() <= before)
            {
                acknowledged.push(*value);
            }
        }
    }
    for value in restored {
        match made.get(value) {
            None => return Err(violation(format!("it holds {value}, which no write made"))),
            Some(false) => {
                return Err(violation(format!(
                    "it holds {value}, which every attempt to write was refused"
                )));
            }
            Some(true) => {}
        }
    }
    let held: BTreeSet<i64> = restored.iter().copied().collect();
    if let Some(missing) = acknowledged.iter().find(|value| !held.contains(value)) {
        return Err(violation(format!(
            "it lacks {missing}, which was acknowledged before the archive caught up"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fleetring::ReplicationFactor;
    use fleetring_control::{Action, Command, Member, Millis, Timestamp};

    use super::*;
    use crate::sim::workload::{LeaseObservation, LeaseSeen, Outcome};

    fn acknowledged(operation: Operation, ack: Ack) -> (Operation, Outcome) {
        (operation, Outcome::Acknowledged(ack))
    }

    trait Push {
        fn push(&mut self, record: (Operation, Outcome));
    }

    impl Push for History {
        fn push(&mut self, (operation, outcome): (Operation, Outcome)) {
            self.record("client", Duration::ZERO, Duration::ZERO, operation, outcome);
        }
    }

    fn lease(database: &str, node: &str, epoch: u64, expires_at_ms: u64) -> (Operation, Outcome) {
        acknowledged(
            Operation::AcquireLease {
                database: database.parse().expect("name"),
                node: node.parse().expect("id"),
            },
            Ack::Lease {
                epoch: LeaseEpoch::new(epoch),
                expires_at_ms,
            },
        )
    }

    fn state_with_lease() -> State {
        let mut state = State::new(ControlConfig::new().with_lease_duration(Millis::new(100)));
        let at = Timestamp::from_millis(10);
        let actions = [
            Action::Join {
                member: Member::new("a".parse().expect("id")),
            },
            Action::CreateDatabase {
                name: "db".parse().expect("name"),
                replication_factor: ReplicationFactor::new(1).expect("one"),
                engine: fleetring_control::Engine::Sqlite,
                durability: None,
                metadata: None,
                incarnation: fleetring_control::Incarnation::LEGACY,
            },
            Action::AcquireLease {
                database: "db".parse().expect("name"),
                node: "a".parse().expect("id"),
            },
        ];
        for action in actions {
            apply(&mut state, Command::new(at, action)).expect("accepted");
        }
        state
    }

    #[test]
    fn a_lost_acknowledgement_is_a_violation() {
        let state = state_with_lease();
        let mut history = History::default();
        history.push(lease("db", "a", 1, 110));
        assert_eq!(acknowledged_control_survives(&state, &history), Ok(()));

        // A renewal the state doesn't reflect was lost.
        history.push(lease("db", "a", 1, 500));
        let violation = acknowledged_control_survives(&state, &history).expect_err("lost renewal");
        assert_eq!(
            violation.invariant(),
            Invariant::AcknowledgedControlSurvives
        );

        let mut history = History::default();
        history.push(acknowledged(
            Operation::CreateDatabase {
                database: "other".parse().expect("name"),
                replication_factor: 1,
            },
            Ack::Created,
        ));
        assert!(acknowledged_control_survives(&state, &history).is_err());
    }

    fn written(value: i64) -> (Operation, Outcome) {
        acknowledged(
            Operation::Write {
                database: "db".parse().expect("name"),
                value,
            },
            Ack::Written {
                acknowledged_by: 2,
                replicas: 3,
                under_replicated: false,
                primary: Some("a".parse().expect("id")),
                epoch: LeaseEpoch::new(1),
            },
        )
    }

    fn written_by(value: i64, primary: &str, epoch: u64) -> (Operation, Outcome) {
        acknowledged(
            Operation::Write {
                database: "db".parse().expect("name"),
                value,
            },
            Ack::Written {
                acknowledged_by: 2,
                replicas: 3,
                under_replicated: false,
                primary: Some(primary.parse().expect("id")),
                epoch: LeaseEpoch::new(epoch),
            },
        )
    }

    #[test]
    fn two_nodes_acknowledging_writes_at_one_epoch_is_a_violation() {
        let mut history = History::default();
        history.push(lease("db", "a", 1, 10));
        history.push(written_by(1, "a", 1));
        history.push(written_by(2, "b", 2));
        history.push(written_by(3, "b", 2));
        assert_eq!(one_primary_per_epoch(&history), Ok(()));
        history.push(written_by(4, "c", 2));
        let violation = one_primary_per_epoch(&history).expect_err("two primaries");
        assert!(violation.detail().contains("b and c"), "{violation}");
        let mut history = History::default();
        history.push(lease("db", "a", 1, 10));
        history.push(written_by(1, "b", 1));
        assert!(
            one_primary_per_epoch(&history).is_err(),
            "a lease and a write disagree"
        );
    }

    #[test]
    fn an_acknowledged_write_on_too_few_replicas_is_a_violation() {
        let db: DatabaseName = "db".parse().expect("name");
        let mut history = History::default();
        history.push(written(1));
        history.push(written(2));
        let mut replicas = ReplicaRows::new();
        replicas.insert("a".into(), BTreeSet::from([1, 2, 3]));
        replicas.insert("b".into(), BTreeSet::from([1, 2]));
        replicas.insert("c".into(), BTreeSet::from([1]));
        assert_eq!(
            acknowledged_writes_replicated(&history, &db, &replicas, 2),
            Ok(())
        );
        let violation =
            acknowledged_writes_replicated(&history, &db, &replicas, 3).expect_err("too few");
        assert_eq!(
            violation.invariant(),
            Invariant::AcknowledgedWritesReplicated
        );
        assert!(violation.detail().contains("write 2"), "{violation}");
        let violation = replicas_converged(&db, &replicas).expect_err("they differ");
        assert_eq!(violation.invariant(), Invariant::ReplicasConverged);
    }

    fn seen(node: &str, at: u64, epoch: u64, seen: LeaseSeen) -> LeaseObservation {
        LeaseObservation::new(
            node,
            Duration::from_millis(at),
            "db".parse().expect("name"),
            LeaseEpoch::new(epoch),
            seen,
        )
    }

    #[test]
    fn lease_observations_are_held_to_the_leases_control_granted() {
        let db: DatabaseName = "db".parse().expect("name");
        let holders = LeaseHolders::from([
            ((db.clone(), LeaseEpoch::new(1)), "a".parse().expect("id")),
            ((db.clone(), LeaseEpoch::new(2)), "b".parse().expect("id")),
        ]);
        let mut history = History::default();
        history.observe_lease(seen("a", 1, 1, LeaseSeen::Held));
        history.observe_lease(seen("a", 5, 1, LeaseSeen::Lost));
        history.observe_lease(seen("b", 6, 2, LeaseSeen::Gained));
        history.push(written_by(1, "a", 1));
        history.push(written_by(2, "b", 2));
        assert_eq!(lease_events_match_control(&history, &holders), Ok(()));

        let mut wrong_holder = history.clone();
        wrong_holder.observe_lease(seen("c", 7, 2, LeaseSeen::Gained));
        let violation =
            lease_events_match_control(&wrong_holder, &holders).expect_err("c never held it");
        assert_eq!(violation.invariant(), Invariant::LeaseEventsMatchControl);
        assert!(
            violation.detail().contains("leased that epoch to b"),
            "{violation}"
        );

        let mut unknown_epoch = history.clone();
        unknown_epoch.observe_lease(seen("b", 8, 3, LeaseSeen::Gained));
        let violation =
            lease_events_match_control(&unknown_epoch, &holders).expect_err("no epoch 3");
        assert!(violation.detail().contains("never leased"), "{violation}");

        let mut out_of_order = history.clone();
        out_of_order.observe_lease(seen("b", 9, 1, LeaseSeen::Lost));
        let violation =
            lease_events_match_control(&out_of_order, &holders).expect_err("epoch 1 after 2");
        assert!(violation.detail().contains("after epoch 2"), "{violation}");

        let mut unseen_write = History::default();
        unseen_write.observe_lease(seen("a", 1, 1, LeaseSeen::Held));
        unseen_write.push(written_by(3, "b", 2));
        let violation = lease_events_match_control(&unseen_write, &holders)
            .expect_err("b's watcher saw nothing");
        assert!(
            violation.detail().contains("never saw it hold"),
            "{violation}"
        );
    }

    #[test]
    fn chains_that_differ_at_a_common_position_are_a_violation() {
        let db: DatabaseName = "db".parse().expect("name");
        let hash = |byte: u8| ChainHash::from_bytes(&[byte; ChainHash::LEN]).expect("32 bytes");
        let mut chains = ReplicaChains::new();
        chains.insert(
            "a".into(),
            BTreeMap::from([(Position::new([1]), hash(1)), (Position::new([2]), hash(2))]),
        );
        chains.insert("b".into(), BTreeMap::from([(Position::new([1]), hash(1))]));
        chains.insert(
            "c".into(),
            BTreeMap::from([(Position::new([2]), hash(2)), (Position::new([3]), hash(3))]),
        );
        assert_eq!(chains_agree(&db, &chains), Ok(()));
        chains.insert("d".into(), BTreeMap::from([(Position::new([2]), hash(9))]));
        let violation = chains_agree(&db, &chains).expect_err("they differ");
        assert_eq!(violation.invariant(), Invariant::ChainAgrees);
        assert!(violation.detail().contains("a holds"), "{violation}");
        assert!(violation.detail().contains("d holds"), "{violation}");
    }

    #[test]
    fn two_holders_at_one_epoch_is_a_violation() {
        let mut history = History::default();
        history.push(lease("db", "a", 1, 10));
        history.push(lease("db", "a", 1, 20));
        history.push(lease("db", "b", 2, 30));
        assert_eq!(one_primary_per_epoch(&history), Ok(()));
        history.push(lease("db", "c", 2, 40));
        let violation = one_primary_per_epoch(&history).expect_err("two primaries");
        assert_eq!(violation.invariant(), Invariant::OnePrimaryPerEpoch);
        assert!(violation.detail().contains("b and c"), "{violation}");
    }

    fn delivered(epoch: u64, start: Option<&str>, end: &str) -> ChangeRecord {
        ChangeRecord {
            database: "db".to_owned(),
            epoch,
            start: start.map(str::to_owned),
            position: end.to_owned(),
            content: RecordContent::Logical { rows: Vec::new() },
        }
    }

    /// The `(epoch, end)` of every record, as the fleet collects them.
    fn marks_of(streams: &[(String, Vec<ChangeRecord>)]) -> BTreeSet<(u64, Position)> {
        streams
            .iter()
            .flat_map(|(_, records)| records)
            .map(|record| {
                let end = unhex(&record.position).expect("hex");
                (record.epoch, Position::from(end))
            })
            .collect()
    }

    fn failed_over(resumed: ChangeRecord) -> Vec<(String, Vec<ChangeRecord>)> {
        vec![
            (
                "a".to_owned(),
                vec![
                    delivered(1, None, "01"),
                    delivered(1, Some("01"), "02"),
                    delivered(1, Some("02"), "03"),
                ],
            ),
            ("b".to_owned(), vec![resumed]),
        ]
    }

    fn consistent(streams: &[(String, Vec<ChangeRecord>)], host: usize) -> Result<(), Violation> {
        let db: DatabaseName = "db".parse().expect("name");
        let (name, records) = &streams[host];
        change_stream_consistent(&History::default(), &db, name, records, &marks_of(streams))
    }

    #[test]
    fn a_stream_resumed_at_a_mark_passes() {
        let db: DatabaseName = "db".parse().expect("name");
        let streams = failed_over(delivered(2, Some("02"), "04"));
        assert_eq!(consistent(&streams, 0), Ok(()));
        assert_eq!(consistent(&streams, 1), Ok(()));
        // The record at 03 is the one repeat.
        assert_eq!(change_streams_resume(&db, &streams, 1), Ok(()));
        assert_eq!(
            change_streams_resume(&db, &streams, REPEATS_PER_FAILOVER),
            Ok(())
        );
    }

    #[test]
    fn a_resume_past_every_delivered_position_is_a_gap() {
        let db: DatabaseName = "db".parse().expect("name");
        let streams = failed_over(delivered(2, Some("09"), "0a"));
        let violation = consistent(&streams, 1).expect_err("nothing delivered up to 09");
        assert_eq!(violation.invariant(), Invariant::ChangeStreamConsistent);
        let violation = change_streams_resume(&db, &streams, REPEATS_PER_FAILOVER)
            .expect_err("09 is no earlier epoch's mark");
        assert_eq!(violation.invariant(), Invariant::ChangeStreamResumes);
    }

    #[test]
    fn more_repeats_than_the_bound_fail() {
        let db: DatabaseName = "db".parse().expect("name");
        // Epoch 2 resumes at 01: the records ending at 02 and 03 are repeated, and a third
        // delivery at epoch 1 is one too many.
        let mut streams = failed_over(delivered(2, Some("01"), "04"));
        assert_eq!(
            change_streams_resume(&db, &streams, REPEATS_PER_FAILOVER),
            Ok(())
        );
        streams[0].1.push(delivered(1, Some("03"), "03aa"));
        let violation =
            change_streams_resume(&db, &streams, REPEATS_PER_FAILOVER).expect_err("three repeats");
        assert!(violation.detail().contains("repeats 3"), "{violation}");
        assert_eq!(change_streams_resume(&db, &streams, 3), Ok(()));
        // A pump that delivered the same record twice on one epoch repeats it once.
        streams[0].1.pop();
        streams[0].1.push(delivered(1, Some("01"), "02"));
        assert_eq!(
            change_streams_resume(&db, &streams, REPEATS_PER_FAILOVER),
            Ok(())
        );
    }

    #[test]
    fn a_first_record_from_the_beginning_after_deliveries_fails() {
        let db: DatabaseName = "db".parse().expect("name");
        let streams = failed_over(delivered(2, None, "04"));
        // Per host, starting from the log's beginning is always allowed: a pump may reset.
        assert_eq!(consistent(&streams, 1), Ok(()));
        let violation = change_streams_resume(&db, &streams, REPEATS_PER_FAILOVER)
            .expect_err("a new epoch restarted from the beginning");
        assert!(
            violation.detail().contains("log's beginning"),
            "{violation}"
        );
        // With nothing delivered before, the first epoch starts from the beginning.
        let first = vec![("a".to_owned(), vec![delivered(1, None, "01")])];
        assert_eq!(
            change_streams_resume(&db, &first, REPEATS_PER_FAILOVER),
            Ok(())
        );
    }

    #[test]
    fn a_restored_copy_must_be_an_acknowledged_prefix_of_the_committed_log() {
        let db: DatabaseName = "db".parse().expect("name");
        let mut history = History::default();
        for value in 1..=4 {
            history.push(written(value));
        }
        history.push((
            Operation::Write {
                database: db.clone(),
                value: 9,
            },
            Outcome::Rejected {
                code: tonic::Code::Unavailable,
                message: "refused".to_owned(),
            },
        ));
        let committed = [1, 2, 3, 4];
        let check = |restored: &[i64], committed: &[i64], before| {
            archive_restores_prefix(&history, &db, restored, committed, before)
        };
        assert_eq!(check(&[1, 2], &committed, None), Ok(()));
        assert_eq!(check(&committed, &committed, Some(Duration::ZERO)), Ok(()));
        let violated = |result: Result<(), Violation>| {
            result.is_err_and(|violation| {
                violation.invariant() == Invariant::ArchiveRestoresAcknowledgedPrefix
            })
        };
        assert!(violated(check(&[1, 3], &committed, None)), "a gap");
        assert!(violated(check(&[2, 1], &committed, None)), "out of order");
        assert!(
            violated(check(&[1, 2, 3, 4, 5], &committed, None)),
            "past the log"
        );
        assert!(violated(check(&[9], &[9], None)), "a refused write");
        assert!(violated(check(&[7], &[7], None)), "a write never made");
        assert!(
            violated(check(&[1, 2], &committed, Some(Duration::ZERO))),
            "acknowledged writes missing from the latest point"
        );
    }
}
