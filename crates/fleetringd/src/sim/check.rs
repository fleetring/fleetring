//! Invariants checked during and after a run (ADR-0020, point 4).
//!
//! Each check is a function of what it needs: control state, a log on disk, the published
//! views, or the [`History`]. [`Cluster`](crate::sim::Cluster) runs them at the points where
//! they hold:
//!
//! - [`ring_converged`] once faults heal ([`Cluster::await_ring`](crate::sim::Cluster::await_ring));
//! - [`control_log_replays`] and [`acknowledged_control_survives`] whenever the control host is
//!   crashed or stopped, and at the end of a run;
//! - [`one_primary_per_epoch`] at every check and at the end;
//! - the history checker ([`history::check`](crate::sim::history::check)), which includes
//!   [`acknowledged_writes_replicated`], whenever the test asks
//!   ([`Cluster::check_history`](crate::sim::Cluster::check_history)), once replicas converge,
//!   and at the end, and [`replicas_converged`] once the network heals
//!   ([`Cluster::await_replicas_converged`](crate::sim::Cluster::await_replicas_converged)).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use fleetring::NodeId;
use fleetring_control::{Command, ControlConfig, DatabaseName, LeaseEpoch, State, apply};

use crate::MembershipView;
use crate::control::CommandLog;
use crate::sim::workload::{Ack, History, Operation, Record};

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

/// What [`control_log_replays`] read from a control log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlLog {
    state: State,
    commands: Vec<Command>,
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
    for command in &commands {
        // A rejected command changes nothing, in recovery as in the replay.
        let _outcome = apply(&mut replayed, command.clone());
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
    })
}

/// Checks that `state` holds everything `history` says was acknowledged: every joined member
/// that wasn't later asked to leave, every created database, and every granted or renewed lease,
/// unless a later epoch replaced it.
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
            (Operation::Join { node }, Ack::Joined { .. }) => {
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

/// The rows each replica of a database holds, by host.
pub type ReplicaRows = BTreeMap<String, BTreeSet<i64>>;

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fleetring::ReplicationFactor;
    use fleetring_control::{Action, Command, Member, Millis, Timestamp};

    use super::*;
    use crate::sim::workload::Outcome;

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
}
