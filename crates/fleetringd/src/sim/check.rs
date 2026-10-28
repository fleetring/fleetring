//! Invariants checked during and after a run (ADR-0020, point 4).
//!
//! Each check is a function of what it needs: control state, a log on disk, the published
//! views, or the [`History`]. [`Cluster`](crate::sim::Cluster) runs them at the points where
//! they hold:
//!
//! - [`ring_converged`] once faults heal ([`Cluster::await_ring`](crate::sim::Cluster::await_ring));
//! - [`control_log_replays`] and [`acknowledged_control_survives`] whenever the control host is
//!   crashed or stopped, and at the end of a run;
//! - [`one_primary_per_epoch`] at every check and at the end.
//!
//! # Extension points
//!
//! - **No acknowledged write is lost** (FR-030): once [`Operation`] carries database writes, add
//!   a check that every acknowledged write is present on the database's primary after faults
//!   heal, and on a quorum of its replicas, and call it from
//!   [`Cluster::check`](crate::sim::Cluster::check) and [`Cluster::finish`](crate::sim::Cluster::finish).
//! - **At most one primary per epoch** (FR-031): [`one_primary_per_epoch`] checks the leases
//!   clients were granted. Once data nodes report their roles in heartbeats and accept writes,
//!   extend it with every node that acknowledged a write as primary, keyed by database and epoch.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use fleetring::NodeId;
use fleetring_control::{Command, ControlConfig, DatabaseName, LeaseEpoch, State, apply};

use crate::MembershipView;
use crate::control::CommandLog;
use crate::sim::workload::{Ack, History, Operation};

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
}

impl fmt::Display for Invariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RingConverged => "ring converged",
            Self::ControlLogReplays => "control log replays",
            Self::AcknowledgedControlSurvives => "acknowledged control operations survive",
            Self::OnePrimaryPerEpoch => "one primary per epoch",
        })
    }
}

/// A broken invariant, and what broke it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invariant violated ({invariant}): {detail}")]
pub struct Violation {
    invariant: Invariant,
    detail: String,
}

impl Violation {
    pub(crate) fn new(invariant: Invariant, detail: impl Into<String>) -> Self {
        Self {
            invariant,
            detail: detail.into(),
        }
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
            (Operation::CreateDatabase { database, .. }, Ack::Created) => {
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

/// Checks that no two nodes were acknowledged as a database's lease holder at the same epoch.
///
/// # Errors
///
/// [`Invariant::OnePrimaryPerEpoch`] naming the database, the epoch, and both holders.
pub fn one_primary_per_epoch(history: &History) -> Result<(), Violation> {
    let mut holders: BTreeMap<(&DatabaseName, LeaseEpoch), &NodeId> = BTreeMap::new();
    for (operation, ack) in history.acknowledged() {
        let (database, node, epoch) = match (operation, ack) {
            (
                Operation::AcquireLease { database, node }
                | Operation::RenewLease { database, node, .. },
                Ack::Lease { epoch, .. },
            ) => (database, node, *epoch),
            _ => continue,
        };
        let holder = *holders.entry((database, epoch)).or_insert(node);
        if holder != node {
            return Err(Violation::new(
                Invariant::OnePrimaryPerEpoch,
                format!(
                    "{database} has two primaries at epoch {}: {holder} and {node}",
                    epoch.get()
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
    use crate::sim::workload::{Outcome, Record};

    fn acknowledged(operation: Operation, ack: Ack) -> Record {
        Record::new(
            "client",
            Duration::ZERO,
            Duration::ZERO,
            operation,
            Outcome::Acknowledged(ack),
        )
    }

    fn lease(database: &str, node: &str, epoch: u64, expires_at_ms: u64) -> Record {
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
