//! The fork properties of the history checker: ADR-0046 point 12's invariants (a) to (h), the
//! frozen replica set and its hand-over (i), and disk admission, checked over a whole run (FR-087).
//!
//! [`check`] reads the control log, replaying it command by command with `apply`; the history,
//! with what the fleet saw of each data host's fork work at its fault hooks and of each fork's
//! phase in control state; and every data host's copies on disk, staged ones included. It never
//! asks the code under test to judge itself: the quorums, the identities, and the order of events
//! are worked out here, from what happened.
//!
//! - **(a) Fencing.** No lease on a fork before it is seeded; a seeded fork's first lease only to
//!   a holder whose report showed the staged copy it completed with; completion only on a
//!   completion quorum C = max(⌊R/2⌋ + 1, W, `min_replicas`) of holders that reported it;
//!   readiness only once a bootstrap quorum W_boot of its holders' copies on disk hold its
//!   marker; and no client write, read, or watch acknowledged on a fork before its reset
//!   committed at W_boot: before W_boot of its holders' copies on disk held the marker, as the
//!   fleet read them after each step, nor before its primary's hooks show the quorum commit.
//! - **(b) Acknowledged rows only.** A fork's rows from its source are a prefix of the source's
//!   surviving history, in order, and none was refused.
//! - **(c) One source incarnation.** No copy of a fork, staged or final, holds a row written to
//!   another incarnation of its source.
//! - **(d) Identities apart.** No fork command `apply` accepts names another identity than the
//!   current one; completion and readiness count only reports of the command's identity, and
//!   readiness only its holders' reports; no
//!   seeding request of a dropped identity is served; and no fork holds another fork's rows.
//! - **(e) Independence.** No source holds a row written to its fork, and no fork a source row
//!   after its own.
//! - **(f) Operations.** One operation ID names one fork identity while its record lasts; a
//!   resubmission is answered with that identity only from its requester with the same request;
//!   and a terminal outcome never changes.
//! - **(g) Termination.** A fork leaves `seeding` by its deadline, and ends as its scenario
//!   expects.
//! - **(h) One reset.** No fork-reset write runs once a client write to the fork was acknowledged;
//!   every copy of a ready fork that holds its writes, or any marker, holds exactly its own
//!   marker; no watch of a fork sees a change it inherited; and every watch sees every write to
//!   the fork acknowledged before it began: the change history recorded after readiness survives.
//! - **(i) Frozen replica set.** While a fork is seeded or ready with its holders kept, its
//!   replica set in control state is its recorded holders and its lease stays with one; and every
//!   write acknowledged before it became ready, or before its holders were released, is on a
//!   write quorum of the replica set it moved to. The replica set is worked out here: the
//!   holders, or once they are released the ring's placement over the fleet's members, computed
//!   with the placement crate, never asked of control state ([`expected_replica_set`]).
//! - **Disk admission.** No node holds more concurrent forks' copies than its free disk admits.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use fleetring::{NodeId, Ring};
use fleetring_control::fork::{Fork, ForkIdentity, ForkState, OperationId};
use fleetring_control::{
    Acks, Action, Command, Database, DatabaseName, Incarnation, Report, State, apply,
};

use crate::sim::check::{Invariant, Violation};
use crate::sim::history::excerpt;
use crate::sim::workload::{
    Ack, ForkPhaseKind, ForkTransition, History, Operation, OutcomeClass, Record, SeedingProbe,
};

/// How long a watch may take to start following a database's changes after it is asked for,
/// and a change to reach it after its write is acknowledged: a few of the simulated network's
/// longest delays.
const WATCH_SETTLED: Duration = Duration::from_millis(100);

/// How long past a seeding fork's deadline the driver may take to fail it: a few ticks.
const DEADLINE_SLACK_MS: u64 = 3_000;

/// How a scenario expects a fork operation to end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Expected {
    /// Ready.
    Ready,
    /// Failed.
    Failed,
    /// Failed for this reason.
    FailedWith(fleetring_control::fork::ForkFailure),
    /// Cancelled, by a drop or an expiry.
    Cancelled,
    /// Seeded and never ready: unusable but safe, as invariant (g) allows while its quorums are
    /// unreachable.
    Seeded,
    /// Ready, failed, or cancelled.
    Terminal,
}

/// What a scenario expects of its forks, beyond the invariants every run holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ForkExpectations {
    outcomes: BTreeMap<OperationId, Expected>,
    copy_budgets: BTreeMap<NodeId, usize>,
    principals: BTreeMap<String, String>,
}

impl ForkExpectations {
    /// Expects the fork operation `operation` to end as `expected`.
    pub fn expect(&mut self, operation: OperationId, expected: Expected) {
        self.outcomes.insert(operation, expected);
    }

    /// Lets `node` hold at most `copies` forks' copies whose seeding overlapped, as its free
    /// disk admits (ADR-0046, point 7.1).
    pub fn limit_copies(&mut self, node: NodeId, copies: usize) {
        self.copy_budgets.insert(node, copies);
    }

    /// Records that client `client` acts as `principal`.
    pub(crate) fn principal(&mut self, client: &str, principal: &str) {
        self.principals
            .insert(client.to_owned(), principal.to_owned());
    }

    fn principal_of<'a>(&'a self, client: &'a str) -> &'a str {
        self.principals
            .get(client)
            .map_or("workload", String::as_str)
    }
}

/// A copy of a database on one data host's disk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CopyOnDisk {
    /// The values of its writes table, in commit order.
    pub values: Vec<i64>,
    /// Its fork markers, as `(fork incarnation, operation)`.
    pub markers: Vec<(u64, Vec<u8>)>,
}

/// Every data host's copies of the databases forks involve.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ForkDisk {
    /// Each host's copy of each database, by database and host.
    pub copies: BTreeMap<(DatabaseName, String), CopyOnDisk>,
    /// Each host's staged copies: the host, the fork's name, its `incarnation-operation`
    /// directory, and the copy's values in commit order.
    pub staged: Vec<(String, DatabaseName, String, Vec<i64>)>,
    /// Every data host of the fleet: the hosts whose disks were read.
    pub hosts: BTreeSet<String>,
}

/// How each control time maps to simulated time: the fleet's clock shifts, in order.
#[derive(Debug, Clone, Copy)]
struct Clock<'h> {
    shifts: &'h [(Duration, u64)],
}

impl Clock<'_> {
    /// The simulated time a host's clock read `control_ms` at.
    fn sim(&self, control_ms: u64) -> Duration {
        let mut shift = 0;
        for (at, total) in self.shifts {
            if millis(*at).saturating_add(*total) <= control_ms {
                shift = *total;
            }
        }
        Duration::from_millis(control_ms.saturating_sub(shift))
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// One life of a fork identity, or of a database name, in simulated time.
#[derive(Debug, Clone)]
struct Life {
    identity: ForkIdentity,
    source: DatabaseName,
    source_incarnation: Incarnation,
    /// Its write quorum W and bootstrap quorum W_boot, from its replication factor and
    /// durability when it was asked for: neither changes before it is ready.
    write: usize,
    bootstrap: usize,
    start: Duration,
    end: Option<Duration>,
}

impl Life {
    fn contains(&self, record: &Record) -> bool {
        record.invoked_at() >= self.start && self.end.is_none_or(|end| record.completed_at() <= end)
    }

    /// Whether `record` was sent while the fork lived. An orphan copy of the fork may hold such
    /// a write however late, or whether, its answer came.
    fn sent_during(&self, record: &Record) -> bool {
        record.invoked_at() >= self.start && self.end.is_none_or(|end| record.invoked_at() < end)
    }

    /// The fork marker its reset write leaves in its copies, as [`CopyOnDisk::markers`] holds it.
    fn marker(&self) -> (u64, Vec<u8>) {
        (
            self.identity.incarnation.get(),
            self.identity.operation.to_bytes().to_vec(),
        )
    }
}

/// The fork of `name` that `copy` belongs to by its markers: the one its only marker names.
/// `None` for a copy without a marker, or with markers that name no one fork of the name.
fn marked_life<'l>(
    learned: &'l Learned,
    name: &DatabaseName,
    copy: &CopyOnDisk,
) -> Option<&'l Life> {
    let [marker] = copy.markers.as_slice() else {
        return None;
    };
    learned
        .forks
        .iter()
        .find(|life| life.identity.name == *name && life.marker() == *marker)
}

/// Which of a fork's writes a copy checked against it may hold as its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Own {
    /// Those sent and answered while it lived: a live or staged copy of it.
    Answered,
    /// Those sent while it lived: an orphan copy of it, kept when its database went, which
    /// holds what its primary applied before then, answered or not.
    Sent,
}

/// One life of a database name: its incarnation, in simulated time.
#[derive(Debug, Clone)]
struct NameLife {
    name: DatabaseName,
    incarnation: Incarnation,
    start: Duration,
    end: Option<Duration>,
}

/// What a replay of the control log learned for the data checks.
#[derive(Debug, Default)]
struct Learned {
    forks: Vec<Life>,
    names: Vec<NameLife>,
    /// Each fork identity's completions: the holders.
    completions: BTreeMap<ForkIdentity, BTreeSet<NodeId>>,
    /// Each operation's record over time: the incarnations it named, in order.
    record_incarnations: BTreeMap<OperationId, Vec<(Duration, Option<Incarnation>)>>,
}

/// The write quorum W, the bootstrap quorum W_boot, and the completion quorum C of `db`, worked
/// out from its replication factor and durability (ADR-0046, point 3).
fn quorums(db: &Database) -> (usize, usize, usize) {
    let replicas = usize::from(db.replication_factor().get());
    let majority = replicas / 2 + 1;
    let durability = db.durability();
    let write = match durability.acks() {
        Acks::Local => 1,
        Acks::All => replicas,
        _ => majority,
    };
    let bootstrap = majority.max(write);
    (
        write,
        bootstrap,
        bootstrap.max(usize::from(durability.min_replicas())),
    )
}

fn identity_of(db: &Database) -> Option<ForkIdentity> {
    db.fork()
        .map(|fork| ForkIdentity::new(db.name().clone(), db.incarnation(), fork.operation()))
}

/// The replica set ADR-0046 gives `db`, worked out here rather than asked of control state: a
/// seeded fork's recorded holders, and a ready one's while it keeps them (point 3.10, invariant
/// (i)); the replica set a membership change kept, while control state records one (ADR-0031,
/// amendment 2); otherwise the ring's placement over the fleet's members with its replication
/// factor, computed with the placement crate itself.
#[must_use]
pub fn expected_replica_set(state: &State, db: &Database) -> Vec<NodeId> {
    match db.fork().map(Fork::state) {
        Some(
            ForkState::Seeded { holders, .. }
            | ForkState::Ready {
                holders: Some(holders),
                ..
            },
        ) => holders.iter().cloned().collect(),
        // A replica set a membership change kept is the set promotions fence until it is
        // released (ADR-0031, amendment 2).
        _ => db
            .kept_replicas()
            .map_or_else(|| placement(state, db), <[NodeId]>::to_vec),
    }
}

/// Where the ring over `state`'s members places `db`, in preference order: none if the fleet
/// has no members.
fn placement(state: &State, db: &Database) -> Vec<NodeId> {
    let nodes = state.members().map(|member| {
        let mut node = fleetring::Node::new(member.id().clone()).with_weight(member.weight());
        if let Some(zone) = member.zone() {
            node = node.with_zone(zone.clone());
        }
        if let Some(host) = member.host() {
            node = node.with_host(host.clone());
        }
        node
    });
    Ring::builder()
        .epoch(state.membership_epoch())
        .nodes(nodes)
        .build()
        .map(|ring| {
            ring.replicas_n(db.name().as_str(), db.replication_factor())
                .to_ids()
        })
        .unwrap_or_default()
}

fn violation(invariant: Invariant, detail: String) -> Violation {
    Violation::new(invariant, detail)
}

/// Checks every fork property over a run; see the [module documentation](self).
///
/// # Errors
///
/// The first [`Violation`], of the first property that fails.
pub fn check(
    genesis: &State,
    commands: &[Command],
    history: &History,
    disk: &ForkDisk,
    expectations: &ForkExpectations,
) -> Result<(), Violation> {
    let clock = Clock {
        shifts: history.clock_shifts(),
    };
    let (learned, last) = replay(genesis, commands, clock)?;
    let fork_writes = fork_writes(history, &learned);
    let markers = marker_quorums(history, &learned, disk);
    readiness_on_disk(history, &learned, &markers)?;
    fencing_of_clients(history, &learned, &markers)?;
    rows(history, &learned, &last, disk)?;
    probes(history, &learned)?;
    operations(history, &learned, expectations)?;
    reset_once(history, &learned, &last, disk, &fork_writes)?;
    handover(history, &learned, &last, disk, &fork_writes)?;
    // What the scenario expects comes last, after the invariants that say why it went wrong.
    copy_budgets(&learned, expectations)?;
    outcomes(&last, expectations)?;
    Ok(())
}

/// Replays `commands` from `genesis`, checking (a), (d), (f), (g), (i), and disk admission per
/// command, and returns what the data checks need, with the last state.
fn replay(
    genesis: &State,
    commands: &[Command],
    clock: Clock<'_>,
) -> Result<(Learned, State), Violation> {
    let mut state = genesis.clone();
    let mut learned = Learned::default();
    // Each seeding fork's deadline as last seen, and each operation's terminal outcome.
    let mut terminal: BTreeMap<OperationId, String> = BTreeMap::new();
    let mut last_at = 0;
    for command in commands {
        let at = command.at.as_millis();
        last_at = at;
        let action = &command.action;
        let name = affected(action);
        let before = name.and_then(|name| state.database(name)).cloned();
        let current = before.as_ref().and_then(identity_of);
        let reports: BTreeMap<NodeId, Report> = before
            .as_ref()
            .map(|db| {
                state
                    .members()
                    .filter_map(|member| {
                        Some((
                            member.id().clone(),
                            state.report(member.id(), db.name())?.clone(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let accepted = apply(&mut state, command.clone()).is_ok();
        let after = name.and_then(|name| state.database(name)).cloned();
        if accepted {
            if let Some(named) = action.fork_identity()
                && current.as_ref() != Some(named)
            {
                return Err(violation(
                    Invariant::ForkIdentitiesApart,
                    format!(
                        "apply accepted {action:?} for fork {named} at {at} ms while the current fork of the name is {current:?}"
                    ),
                ));
            }
            fork_command(action, at, before.as_ref(), &reports, &mut learned)?;
            lease_rules(action, at, before.as_ref(), after.as_ref(), &reports)?;
        }
        lives(
            action,
            accepted,
            at,
            before.as_ref(),
            after.as_ref(),
            clock,
            &mut learned,
        );
        frozen(&state, at)?;
        // Seeding ends by its deadline.
        if let Some(db) = &before
            && let Some(ForkState::Seeding { deadline, .. }) = db.fork().map(|fork| fork.state())
            && at > deadline.as_millis().saturating_add(DEADLINE_SLACK_MS)
            && after
                .as_ref()
                .and_then(Database::fork)
                .is_some_and(|fork| matches!(fork.state(), ForkState::Seeding { deadline: still, .. } if still == deadline))
        {
            return Err(violation(
                Invariant::ForkTermination,
                format!(
                    "fork {} is still seeding at {at} ms, past its deadline at {} ms",
                    db.name(),
                    deadline.as_millis()
                ),
            ));
        }
        operation_records(&state, at, clock, &mut terminal, &mut learned)?;
    }
    // A fork still seeding at the end of the log is past its deadline only if the log went on
    // long enough after it.
    for db in state.databases() {
        if let Some(ForkState::Seeding { deadline, .. }) = db.fork().map(|fork| fork.state())
            && last_at > deadline.as_millis().saturating_add(DEADLINE_SLACK_MS)
        {
            return Err(violation(
                Invariant::ForkTermination,
                format!(
                    "fork {} is still seeding at the end of the log, past its deadline at {} ms",
                    db.name(),
                    deadline.as_millis()
                ),
            ));
        }
    }
    Ok((learned, state))
}

/// (i): every seeded fork's replica set, and every ready one's whose holders are kept, is its
/// recorded holders in control state, whatever placement says, and its lease is a holder's,
/// whatever moved it. Checked after every command, since a membership change moves placement.
fn frozen(state: &State, at: u64) -> Result<(), Violation> {
    for db in state.databases() {
        let Some(
            ForkState::Seeded { holders, .. }
            | ForkState::Ready {
                holders: Some(holders),
                ..
            },
        ) = db.fork().map(Fork::state)
        else {
            continue;
        };
        let replicas: BTreeSet<NodeId> = state
            .replica_set(db.name())
            .unwrap_or_default()
            .into_iter()
            .collect();
        if replicas != *holders {
            return Err(violation(
                Invariant::ForkFrozenReplicaSet,
                format!(
                    "control state gives fork {} the replica set {replicas:?} at {at} ms, not its recorded holders {holders:?}; placement says {:?}",
                    db.name(),
                    placement(state, db)
                ),
            ));
        }
        if let Some(lease) = db.lease()
            && !holders.contains(lease.holder())
        {
            return Err(violation(
                Invariant::ForkFrozenReplicaSet,
                format!(
                    "{} holds the lease of fork {} at {at} ms, but isn't among its holders {holders:?}",
                    lease.holder(),
                    db.name()
                ),
            ));
        }
    }
    Ok(())
}

/// The database name an action touches, for the replay's before-and-after views.
fn affected(action: &Action) -> Option<&DatabaseName> {
    match action {
        Action::ForkDatabase { intent, .. } => Some(&intent.name),
        Action::AcquireLease { database, .. } => Some(database),
        Action::DropDatabase { name } | Action::ExpireDatabase { name } => Some(name),
        Action::CreateDatabase { name, .. } => Some(name),
        other => other.fork_identity().map(|identity| &identity.name),
    }
}

/// The checks of (a) and (d) on an accepted fork command.
fn fork_command(
    action: &Action,
    at: u64,
    before: Option<&Database>,
    reports: &BTreeMap<NodeId, Report>,
    learned: &mut Learned,
) -> Result<(), Violation> {
    match action {
        Action::CompleteFork {
            fork,
            attempt,
            at: position,
            chain_hash,
            holders,
        } => {
            let Some(db) = before else {
                return Ok(());
            };
            let (_, _, completion) = quorums(db);
            if holders.len() < completion {
                return Err(violation(
                    Invariant::ForkFencing,
                    format!(
                        "fork {fork} completed at {at} ms on {} holders {holders:?}, fewer than its completion quorum of {completion}",
                        holders.len()
                    ),
                ));
            }
            for holder in holders {
                let shown = reports.get(holder).is_some_and(|report| {
                    report.log_epoch().get() == 0
                        && report.staged().is_some_and(|staged| {
                            staged.fork() == fork
                                && staged.attempt() == *attempt
                                && staged.position() == position
                                && staged.chain_hash() == chain_hash
                        })
                });
                if !shown {
                    return Err(violation(
                        Invariant::ForkIdentitiesApart,
                        format!(
                            "fork {fork} completed at {at} ms counting {holder}, whose report {:?} doesn't show that staged copy",
                            reports.get(holder).map(Report::staged)
                        ),
                    ));
                }
            }
            learned
                .completions
                .entry(fork.clone())
                .or_default()
                .extend(holders.iter().cloned());
        }
        Action::MarkForkReady {
            fork,
            epoch,
            reset_position,
        } => {
            let Some(db) = before else {
                return Ok(());
            };
            let (_, bootstrap, _) = quorums(db);
            // Only the holders' reports count: they are the replica set until the fork is ready.
            let holders = match db.fork().map(Fork::state) {
                Some(ForkState::Seeded { holders, .. }) => holders.clone(),
                _ => BTreeSet::new(),
            };
            let committed = reports
                .iter()
                .filter(|(node, _)| holders.contains(*node))
                .map(|(_, report)| report)
                .filter(|report| {
                    report.reset().is_some_and(|reset| {
                        reset.fork() == fork
                            && reset.epoch() == *epoch
                            && reset.position() == reset_position
                            && reset.is_committed()
                    })
                })
                .count();
            if committed < bootstrap {
                return Err(violation(
                    Invariant::ForkIdentitiesApart,
                    format!(
                        "fork {fork} was marked ready at {at} ms on {committed} of its holders' {holders:?} reports of its reset, fewer than its bootstrap quorum of {bootstrap}"
                    ),
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

/// (a): a lease on a fork only once it is seeded, and its first only to a holder that reported
/// the staged copy it completed with.
fn lease_rules(
    action: &Action,
    at: u64,
    before: Option<&Database>,
    after: Option<&Database>,
    reports: &BTreeMap<NodeId, Report>,
) -> Result<(), Violation> {
    let Action::AcquireLease { database, node } = action else {
        return Ok(());
    };
    let (Some(before), Some(after)) = (before, after) else {
        return Ok(());
    };
    let Some(fork) = before.fork() else {
        return Ok(());
    };
    if after.lease().is_none_or(|lease| lease.holder() != node) {
        return Ok(());
    }
    match fork.state() {
        ForkState::Seeding { .. } | ForkState::Failed { .. } => Err(violation(
            Invariant::ForkFencing,
            format!("{node} took a lease of fork {database} at {at} ms before it was seeded"),
        )),
        ForkState::Seeded {
            attempt,
            at: position,
            chain_hash,
            holders,
            ..
        } => {
            let shown = holders.contains(node)
                && reports.get(node).is_some_and(|report| {
                    report.log_epoch().get() == 0
                        && report.staged().is_some_and(|staged| {
                            staged.attempt() == *attempt
                                && staged.position() == position
                                && staged.chain_hash() == chain_hash
                                && identity_of(before).as_ref() == Some(staged.fork())
                        })
                });
            if shown {
                Ok(())
            } else {
                Err(violation(
                    Invariant::ForkFencing,
                    format!(
                        "{node} took the first lease of seeded fork {database} at {at} ms without being a holder {holders:?} that reports its staged copy"
                    ),
                ))
            }
        }
        _ => Ok(()),
    }
}

/// Follows the lives of fork identities and of database names through the log.
fn lives(
    action: &Action,
    accepted: bool,
    at: u64,
    before: Option<&Database>,
    after: Option<&Database>,
    clock: Clock<'_>,
    learned: &mut Learned,
) {
    if !accepted {
        return;
    }
    let now = clock.sim(at);
    let end = |learned: &mut Learned, name: &DatabaseName| {
        for life in learned
            .forks
            .iter_mut()
            .filter(|life| life.identity.name == *name)
        {
            life.end.get_or_insert(now);
        }
        for life in learned.names.iter_mut().filter(|life| life.name == *name) {
            life.end.get_or_insert(now);
        }
    };
    match action {
        Action::DropDatabase { name } | Action::ExpireDatabase { name } => end(learned, name),
        Action::CreateDatabase { .. } | Action::ForkDatabase { .. } => {
            let Some(db) = after else {
                return;
            };
            let fresh = before.is_none_or(|old| old.incarnation() != db.incarnation());
            if !fresh {
                return;
            }
            let name = db.name().clone();
            end(learned, &name);
            learned.names.push(NameLife {
                name,
                incarnation: db.incarnation(),
                start: now,
                end: None,
            });
            if let (Some(identity), Some(fork)) = (identity_of(db), db.fork()) {
                let (write, bootstrap, _) = quorums(db);
                learned.forks.push(Life {
                    identity,
                    source: fork.source().clone(),
                    source_incarnation: fork.source_incarnation(),
                    write,
                    bootstrap,
                    start: now,
                    end: None,
                });
            }
        }
        _ => {}
    }
}

/// (f): an operation's record names one incarnation while it lasts, and its terminal outcome
/// never changes.
fn operation_records(
    state: &State,
    at: u64,
    clock: Clock<'_>,
    terminal: &mut BTreeMap<OperationId, String>,
    learned: &mut Learned,
) -> Result<(), Violation> {
    let now = clock.sim(at);
    let mut seen = BTreeSet::new();
    let live = state.fork_operations().filter(|(operation, _)| {
        state
            .fork_operation(operation, fleetring_control::Timestamp::from_millis(at))
            .is_some()
    });
    for (operation, record) in live {
        seen.insert(*operation);
        let history = learned.record_incarnations.entry(*operation).or_default();
        match history.last() {
            Some((_, Some(incarnation))) if *incarnation != record.target_incarnation() => {
                return Err(violation(
                    Invariant::ForkOperations,
                    format!(
                        "operation {operation}'s record named fork incarnation {incarnation}, then {} at {at} ms without being forgotten",
                        record.target_incarnation()
                    ),
                ));
            }
            Some((_, Some(_))) => {}
            _ => history.push((now, Some(record.target_incarnation()))),
        }
        if record.outcome().is_terminal() {
            let outcome = format!("{:?}", record.outcome());
            match terminal.get(operation) {
                Some(earlier) if *earlier != outcome => {
                    return Err(violation(
                        Invariant::ForkOperations,
                        format!(
                            "operation {operation}'s terminal outcome changed from {earlier} to {outcome} at {at} ms"
                        ),
                    ));
                }
                Some(_) => {}
                None => {
                    terminal.insert(*operation, outcome);
                }
            }
        }
    }
    for (operation, history) in &mut learned.record_incarnations {
        if !seen.contains(operation) && history.last().is_some_and(|(_, inc)| inc.is_some()) {
            history.push((now, None));
            terminal.remove(operation);
        }
    }
    Ok(())
}

/// (g): every fork operation the scenario expects something of ended so.
fn outcomes(state: &State, expectations: &ForkExpectations) -> Result<(), Violation> {
    use fleetring_control::fork::OperationOutcome;
    for (operation, expected) in &expectations.outcomes {
        let outcome = state
            .fork_operations()
            .find(|(id, _)| *id == operation)
            .map(|(_, record)| record.outcome());
        let met = match (expected, outcome) {
            (Expected::Ready, Some(OperationOutcome::Ready { .. }))
            | (Expected::Failed, Some(OperationOutcome::Failed { .. }))
            | (Expected::Cancelled, Some(OperationOutcome::Cancelled { .. }))
            | (Expected::Seeded, Some(OperationOutcome::Seeded)) => true,
            (Expected::FailedWith(want), Some(OperationOutcome::Failed { reason })) => {
                reason == want
            }
            (Expected::Terminal, Some(outcome)) => outcome.is_terminal(),
            _ => false,
        };
        if !met {
            return Err(violation(
                Invariant::ForkTermination,
                format!("operation {operation} ended {outcome:?}, not {expected:?}"),
            ));
        }
    }
    Ok(())
}

/// Disk admission: no node held more concurrently seeding forks' copies than its free disk
/// admits.
fn copy_budgets(learned: &Learned, expectations: &ForkExpectations) -> Result<(), Violation> {
    for (node, budget) in &expectations.copy_budgets {
        let held: Vec<&ForkIdentity> = learned
            .completions
            .iter()
            .filter(|(_, holders)| holders.contains(node))
            .map(|(identity, _)| identity)
            .collect();
        if held.len() > *budget {
            return Err(violation(
                Invariant::ForkDiskAdmission,
                format!(
                    "{node} holds copies of {} forks seeded at once, {held:?}, though its free disk admits {budget}",
                    held.len()
                ),
            ));
        }
    }
    Ok(())
}

/// The life of the fork identity whose name `record` acts on, if `record` falls in it.
fn life_of<'l>(learned: &'l Learned, name: &DatabaseName, record: &Record) -> Option<&'l Life> {
    learned
        .forks
        .iter()
        .find(|life| life.identity.name == *name && life.contains(record))
}

/// The acknowledged writes to each fork identity, by its operation: `(value, acknowledged at)`.
fn fork_writes(
    history: &History,
    learned: &Learned,
) -> BTreeMap<OperationId, Vec<(i64, Duration)>> {
    let mut writes: BTreeMap<OperationId, Vec<(i64, Duration)>> = BTreeMap::new();
    for record in history.records() {
        if let (Operation::Write { database, value }, Some(Ack::Written { .. })) =
            (record.operation(), record.outcome().ack())
            && let Some(life) = life_of(learned, database, record)
        {
            writes
                .entry(life.identity.operation)
                .or_default()
                .push((*value, record.completed_at()));
        }
    }
    writes
}

/// When each fork operation's work first passed `point` on any host, by the hooks.
fn first_pass(
    history: &History,
    point: crate::replication::FaultPoint,
) -> BTreeMap<OperationId, Duration> {
    let mut first = BTreeMap::new();
    for pass in history.fork_passes() {
        if pass.point == point
            && let Some(operation) = pass.operation
        {
            first.entry(operation).or_insert(pass.at);
        }
    }
    first
}

/// When a bootstrap quorum of each fork identity's holders' copies on disk first held its
/// marker, as the fleet read them after each step: the W_boot-th holder's first sighting, or
/// `None` if none was seen. Only forks seeded on the fleet's data hosts are judged on disk: a
/// fork driven by reports alone, with no data host among its holders, has no copies to read.
fn marker_quorums(
    history: &History,
    learned: &Learned,
    disk: &ForkDisk,
) -> BTreeMap<ForkIdentity, Option<Duration>> {
    let mut seen: BTreeMap<&ForkIdentity, Vec<Duration>> = BTreeMap::new();
    for marker in history.fork_markers() {
        seen.entry(&marker.identity).or_default().push(marker.at);
    }
    let on_data_hosts = |identity: &ForkIdentity| {
        history.fork_transitions().iter().any(|transition| {
            transition.identity == *identity
                && transition
                    .replica_set
                    .iter()
                    .any(|node| disk.hosts.contains(node.as_str()))
        })
    };
    let mut quorums = BTreeMap::new();
    for life in learned
        .forks
        .iter()
        .filter(|life| on_data_hosts(&life.identity))
    {
        let mut times = seen.remove(&life.identity).unwrap_or_default();
        times.sort_unstable();
        let at = life
            .bootstrap
            .checked_sub(1)
            .and_then(|index| times.get(index))
            .copied();
        quorums.insert(life.identity.clone(), at);
    }
    quorums
}

/// (a): a fork becomes ready only once a bootstrap quorum of its holders' copies on disk hold
/// its marker, whatever the replicas reported.
fn readiness_on_disk(
    history: &History,
    learned: &Learned,
    markers: &BTreeMap<ForkIdentity, Option<Duration>>,
) -> Result<(), Violation> {
    let mut judged = BTreeSet::new();
    for ForkTransition {
        at,
        identity,
        phase,
        ..
    } in history.fork_transitions()
    {
        let Some(quorum) = markers.get(identity) else {
            continue;
        };
        if !matches!(phase, ForkPhaseKind::ReadyKept | ForkPhaseKind::Released)
            || !judged.insert(identity)
        {
            continue;
        }
        let bootstrap = learned
            .forks
            .iter()
            .find(|life| life.identity == *identity)
            .map_or(0, |life| life.bootstrap);
        let held = history
            .fork_markers()
            .iter()
            .filter(|marker| marker.identity == *identity && marker.at <= *at)
            .map(|marker| marker.host.as_str())
            .collect::<BTreeSet<_>>();
        if quorum.is_none_or(|quorum| quorum > *at) {
            return Err(violation(
                Invariant::ForkFencing,
                format!(
                    "fork {identity} became ready at {} ms while only {held:?} of its holders' copies on disk held its marker, fewer than its bootstrap quorum of {bootstrap}",
                    at.as_millis()
                ),
            ));
        }
    }
    Ok(())
}

/// (a): no client data call acknowledged on a fork before its reset committed at its bootstrap
/// quorum: before a bootstrap quorum of its holders' copies on disk held its marker, nor, with
/// the hooks, before its primary passed its quorum commit.
fn fencing_of_clients(
    history: &History,
    learned: &Learned,
    markers: &BTreeMap<ForkIdentity, Option<Duration>>,
) -> Result<(), Violation> {
    let opened = cfg!(feature = "fault-hooks")
        .then(|| first_pass(history, crate::replication::FaultPoint::AfterQuorumCommit));
    for record in history.records() {
        let served = matches!(
            (record.operation(), record.outcome().ack()),
            (Operation::Write { .. }, Some(Ack::Written { .. }))
                | (Operation::Read { .. }, Some(Ack::Read { .. }))
                | (Operation::Watch { .. }, Some(Ack::Watched { .. }))
        );
        let Some(database) = record.operation().database() else {
            continue;
        };
        if !served {
            continue;
        }
        let Some(life) = life_of(learned, database, record) else {
            continue;
        };
        if let Some(on_disk) = markers.get(&life.identity)
            && on_disk.is_none_or(|on_disk| record.completed_at() < on_disk)
        {
            return Err(violation(
                Invariant::ForkFencing,
                format!(
                    "{} on fork {} was served at {} ms, before a bootstrap quorum of its holders' copies on disk held its marker ({on_disk:?})",
                    record.operation(),
                    life.identity,
                    record.completed_at().as_millis()
                ),
            )
            .with_excerpt(excerpt(history, &[record.id()])));
        }
        let open = opened
            .as_ref()
            .map(|opened| opened.get(&life.identity.operation));
        if let Some(open) = open
            && open.is_none_or(|open| record.completed_at() < *open)
        {
            return Err(violation(
                Invariant::ForkFencing,
                format!(
                    "{} on fork {} was served at {} ms, before its primary saw its reset committed at its bootstrap quorum ({open:?})",
                    record.operation(),
                    life.identity,
                    record.completed_at().as_millis()
                ),
            )
            .with_excerpt(excerpt(history, &[record.id()])));
        }
    }
    Ok(())
}

/// Where each written value went: the database and the name's life, by value.
fn origins(history: &History) -> BTreeMap<i64, (&DatabaseName, &Record)> {
    let mut origins = BTreeMap::new();
    for record in history.records() {
        if let Operation::Write { database, value } = record.operation() {
            origins.insert(*value, (database, record));
        }
    }
    origins
}

/// The incarnation of `name` a write `record` went to, if it fell inside one life of the name.
fn incarnation_of(learned: &Learned, name: &DatabaseName, record: &Record) -> Option<Incarnation> {
    learned
        .names
        .iter()
        .find(|life| {
            life.name == *name
                && record.invoked_at() >= life.start
                && life.end.is_none_or(|end| record.completed_at() <= end)
        })
        .map(|life| life.incarnation)
}

/// The copy of `name` that counts as its surviving history: its lease holder's at the end, or
/// else the longest.
fn surviving<'d>(state: &State, disk: &'d ForkDisk, name: &DatabaseName) -> Option<&'d CopyOnDisk> {
    let holder = state
        .database(name)
        .and_then(Database::lease)
        .map(|lease| lease.holder().to_string());
    if let Some(holder) = holder
        && let Some(copy) = disk.copies.get(&(name.clone(), holder))
    {
        return Some(copy);
    }
    disk.copies
        .iter()
        .filter(|((database, _), _)| database == name)
        .map(|(_, copy)| copy)
        .max_by_key(|copy| copy.values.len())
}

/// (b), (c), (d), and (e) over the copies on disk.
fn rows(
    history: &History,
    learned: &Learned,
    state: &State,
    disk: &ForkDisk,
) -> Result<(), Violation> {
    let origins = origins(history);
    for life in &learned.forks {
        let current = state
            .database(&life.identity.name)
            .and_then(identity_of)
            .is_some_and(|identity| identity == life.identity);
        // A copy whose marker names another fork of the name is an orphan of that fork, kept
        // when it was dropped: it is checked against that fork, never against this one.
        let mut copies: Vec<(String, &[i64], Own)> = Vec::new();
        for ((name, host), copy) in &disk.copies {
            if *name != life.identity.name {
                continue;
            }
            match marked_life(learned, name, copy) {
                Some(marked) if marked.identity != life.identity => {}
                Some(_) if !current => {
                    copies.push((format!("{host}'s orphan copy"), &copy.values, Own::Sent));
                }
                _ if current => {
                    copies.push((format!("{host}'s copy"), &copy.values, Own::Answered));
                }
                _ => {}
            }
        }
        let directory = format!("{}-{}", life.identity.incarnation, life.identity.operation);
        for (host, name, dir, values) in &disk.staged {
            if *name == life.identity.name && *dir == directory {
                copies.push((format!("{host}'s staged copy"), values, Own::Answered));
            }
        }
        let source = state
            .database(&life.source)
            .filter(|db| db.incarnation() == life.source_incarnation)
            .and_then(|_| surviving(state, disk, &life.source));
        for (whose, values, window) in copies {
            let mut from_source = Vec::new();
            let mut own = false;
            for value in values {
                let Some((database, record)) = origins.get(value) else {
                    continue;
                };
                let ours = match window {
                    Own::Answered => life.contains(record),
                    Own::Sent => life.sent_during(record),
                };
                if **database == life.identity.name && ours {
                    own = true;
                    continue;
                }
                if **database != life.source {
                    return Err(violation(
                        Invariant::ForkIdentitiesApart,
                        format!(
                            "{whose} of fork {} holds value {value}, written to {database}, neither the fork nor its source",
                            life.identity
                        ),
                    )
                    .with_excerpt(excerpt(history, &[record.id()])));
                }
                let incarnation = incarnation_of(learned, &life.source, record);
                if incarnation.is_some_and(|incarnation| incarnation != life.source_incarnation) {
                    return Err(violation(
                        Invariant::ForkSourceIncarnation,
                        format!(
                            "{whose} of fork {} holds value {value}, written to {} incarnation {incarnation:?}, not {}",
                            life.identity, life.source, life.source_incarnation
                        ),
                    )
                    .with_excerpt(excerpt(history, &[record.id()])));
                }
                if record.class() == OutcomeClass::Fail {
                    return Err(violation(
                        Invariant::ForkAcknowledgedRows,
                        format!(
                            "{whose} of fork {} holds value {value}, a write to {} that was refused",
                            life.identity, life.source
                        ),
                    )
                    .with_excerpt(excerpt(history, &[record.id()])));
                }
                if own {
                    return Err(violation(
                        Invariant::ForkIndependence,
                        format!(
                            "{whose} of fork {} holds source value {value} after a value written to the fork",
                            life.identity
                        ),
                    )
                    .with_excerpt(excerpt(history, &[record.id()])));
                }
                if incarnation.is_some() {
                    from_source.push(*value);
                }
            }
            if let Some(source) = source {
                let prefix: Vec<i64> = source
                    .values
                    .iter()
                    .copied()
                    .filter(|value| {
                        origins.get(value).is_some_and(|(_, record)| {
                            incarnation_of(learned, &life.source, record).is_some()
                        })
                    })
                    .take(from_source.len())
                    .collect();
                if prefix != from_source {
                    let first = from_source
                        .iter()
                        .zip(&prefix)
                        .position(|(ours, theirs)| ours != theirs)
                        .unwrap_or(prefix.len().min(from_source.len()));
                    return Err(violation(
                        Invariant::ForkAcknowledgedRows,
                        format!(
                            "{whose} of fork {} holds source rows that aren't a prefix of {}'s surviving history: they part at row {first}, the fork's {:?} against the source's {:?}",
                            life.identity,
                            life.source,
                            from_source.get(first),
                            prefix.get(first)
                        ),
                    ));
                }
            }
        }
        // Independence the other way: no copy of the source holds a value written to the fork.
        for ((name, host), copy) in &disk.copies {
            if *name != life.source {
                continue;
            }
            if let Some(value) = copy.values.iter().find(|value| {
                origins.get(value).is_some_and(|(database, record)| {
                    **database == life.identity.name && life.contains(record)
                })
            }) {
                return Err(violation(
                    Invariant::ForkIndependence,
                    format!(
                        "{host}'s copy of {} holds value {value}, written to its fork {}",
                        life.source, life.identity
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Whether `probe` named a fork identity that was gone, or never current, when it was sent.
fn stale(learned: &Learned, probe: &SeedingProbe, record: &Record) -> bool {
    learned
        .forks
        .iter()
        .find(|life| life.identity == probe.fork)
        .is_none_or(|life| life.end.is_some_and(|end| end <= record.invoked_at()))
}

/// (d): no seeding request of a dropped fork identity is served.
fn probes(history: &History, learned: &Learned) -> Result<(), Violation> {
    for record in history.records() {
        let Operation::SeedingProbe { to, request, .. } = record.operation() else {
            continue;
        };
        if matches!(record.outcome().ack(), Some(Ack::Served)) && stale(learned, request, record) {
            return Err(violation(
                Invariant::ForkIdentitiesApart,
                format!("{to} served a seeding request of the dropped fork identity: {request}"),
            )
            .with_excerpt(excerpt(history, &[record.id()])));
        }
    }
    Ok(())
}

/// (f): a resubmission is answered with the operation's fork only from its requester with the
/// same request, while the operation's record lasts.
fn operations(
    history: &History,
    learned: &Learned,
    expectations: &ForkExpectations,
) -> Result<(), Violation> {
    // The first acknowledged request of each operation since its record last began.
    let mut first: BTreeMap<OperationId, (&Record, Incarnation)> = BTreeMap::new();
    for record in history.records() {
        let (
            Operation::Fork {
                operation,
                source,
                target,
                replication_factor,
                expect_source_incarnation,
            },
            Some(Ack::Forked { incarnation }),
        ) = (record.operation(), record.outcome().ack())
        else {
            continue;
        };
        // The incarnation the record named when the answer came.
        let named = learned
            .record_incarnations
            .get(operation)
            .and_then(|records| {
                records
                    .iter()
                    .rev()
                    .find(|(at, _)| *at <= record.completed_at())
                    .and_then(|(_, incarnation)| *incarnation)
            });
        if let Some(named) = named
            && named != *incarnation
        {
            return Err(violation(
                Invariant::ForkOperations,
                format!(
                    "a resubmission of operation {operation} was answered with fork incarnation {incarnation}, while its record names {named}"
                ),
            )
            .with_excerpt(excerpt(history, &[record.id()])));
        }
        match first.get(operation) {
            Some((earlier, earlier_incarnation)) if *earlier_incarnation == *incarnation => {
                let Operation::Fork {
                    source: first_source,
                    target: first_target,
                    replication_factor: first_factor,
                    expect_source_incarnation: first_expect,
                    ..
                } = earlier.operation()
                else {
                    continue;
                };
                let same_intent = first_source == source
                    && first_target == target
                    && first_factor == replication_factor
                    && first_expect == expect_source_incarnation;
                let same_requester = expectations.principal_of(earlier.client())
                    == expectations.principal_of(record.client());
                if !same_intent || !same_requester {
                    return Err(violation(
                        Invariant::ForkOperations,
                        format!(
                            "operation {operation} answered {} with its fork, though it {}",
                            record.operation(),
                            if same_requester {
                                "asks for something else"
                            } else {
                                "comes from another principal"
                            }
                        ),
                    )
                    .with_excerpt(excerpt(history, &[earlier.id(), record.id()])));
                }
            }
            _ => {
                first.insert(*operation, (record, *incarnation));
            }
        }
    }
    Ok(())
}

/// (h): no reset runs once a client write to the fork was acknowledged; every copy of a ready
/// fork that holds its writes holds exactly its own marker; and no watch sees an inherited change.
fn reset_once(
    history: &History,
    learned: &Learned,
    state: &State,
    disk: &ForkDisk,
    fork_writes: &BTreeMap<OperationId, Vec<(i64, Duration)>>,
) -> Result<(), Violation> {
    if cfg!(feature = "fault-hooks") {
        for pass in history.fork_passes() {
            if pass.point != crate::replication::FaultPoint::AfterLocalCommit {
                continue;
            }
            let Some(operation) = pass.operation else {
                continue;
            };
            let earlier = fork_writes
                .get(&operation)
                .and_then(|writes| writes.iter().find(|(_, at)| *at < pass.at));
            if let Some((value, at)) = earlier {
                return Err(violation(
                    Invariant::ForkResetOnce,
                    format!(
                        "{} ran the fork-reset write of operation {operation} at {} ms, after write {value} to the fork was acknowledged at {} ms",
                        pass.host,
                        pass.at.as_millis(),
                        at.as_millis()
                    ),
                ));
            }
        }
    }
    for life in &learned.forks {
        let ready = state
            .database(&life.identity.name)
            .filter(|db| identity_of(db).as_ref() == Some(&life.identity))
            .and_then(Database::fork)
            .is_some_and(|fork| matches!(fork.state(), ForkState::Ready { .. }));
        if !ready {
            continue;
        }
        let ours: BTreeSet<i64> = fork_writes
            .get(&life.identity.operation)
            .map(|writes| writes.iter().map(|(value, _)| *value).collect())
            .unwrap_or_default();
        let marker = life.marker();
        for ((name, host), copy) in &disk.copies {
            let holds_ours = copy.values.iter().any(|value| ours.contains(value));
            // An orphan of another fork of the name, holding none of this one's writes, is
            // checked against its own fork.
            let orphan = marked_life(learned, name, copy)
                .is_some_and(|marked| marked.identity != life.identity);
            if *name != life.identity.name || (!holds_ours && (copy.markers.is_empty() || orphan)) {
                continue;
            }
            if copy.markers != [marker.clone()] {
                return Err(violation(
                    Invariant::ForkResetOnce,
                    format!(
                        "{host}'s copy of ready fork {} holds the markers {:?}, not exactly its own",
                        life.identity, copy.markers
                    ),
                ));
            }
        }
    }
    let origins = origins(history);
    for record in history.records() {
        let (Operation::Watch { database }, Some(Ack::Watched { values })) =
            (record.operation(), record.outcome().ack())
        else {
            continue;
        };
        let Some(life) = life_of(learned, database, record) else {
            continue;
        };
        if let Some(value) = values.iter().find(|value| {
            origins
                .get(value)
                .is_some_and(|(written_to, _)| **written_to != life.identity.name)
        }) {
            return Err(violation(
                Invariant::ForkResetOnce,
                format!(
                    "a watch of fork {} saw value {value} inserted, a change it inherited from {}",
                    life.identity, life.source
                ),
            )
            .with_excerpt(excerpt(history, &[record.id()])));
        }
        // A watch is a live tail: it sees every write to the fork made while it watched, none
        // missed and an empty watch only if there were none.
        let seen: BTreeSet<i64> = values.iter().copied().collect();
        let watched = |write: &Record| {
            write.invoked_at() >= record.invoked_at() + WATCH_SETTLED
                && write.completed_at() + WATCH_SETTLED <= record.completed_at()
        };
        if let Some(write) = history.records().iter().find(|write| {
            matches!(
                (write.operation(), write.outcome().ack()),
                (Operation::Write { database: to, value }, Some(Ack::Written { .. }))
                    if to == database && !seen.contains(value)
            ) && watched(write)
                && life.contains(write)
        }) {
            return Err(violation(
                Invariant::ForkResetOnce,
                format!(
                    "a watch of fork {} from {} to {} ms saw {} changes, and not {}, acknowledged while it watched",
                    life.identity,
                    record.invoked_at().as_millis(),
                    record.completed_at().as_millis(),
                    values.len(),
                    write.operation()
                ),
            )
            .with_excerpt(excerpt(history, &[record.id(), write.id()])));
        }
    }
    Ok(())
}

/// (i): every write acknowledged before a fork became ready, or before its holders were
/// released, is on a write quorum of the replica set it moved to, and every acknowledged write
/// of a ready fork is on one of its final replica set: each set worked out here
/// ([`expected_replica_set`]), the fleet's for its transitions.
fn handover(
    history: &History,
    learned: &Learned,
    state: &State,
    disk: &ForkDisk,
    fork_writes: &BTreeMap<OperationId, Vec<(i64, Duration)>>,
) -> Result<(), Violation> {
    for transition in history.fork_transitions() {
        if !matches!(
            transition.phase,
            ForkPhaseKind::ReadyKept | ForkPhaseKind::Released
        ) {
            continue;
        }
        let Some(write) = learned
            .forks
            .iter()
            .find(|life| life.identity == transition.identity)
            .map(|life| life.write)
        else {
            continue;
        };
        let Some(writes) = fork_writes.get(&transition.identity.operation) else {
            continue;
        };
        for (value, at) in writes.iter().filter(|(_, at)| *at < transition.at) {
            let holders: Vec<&NodeId> = transition
                .replica_set
                .iter()
                .filter(|node| {
                    transition
                        .rows
                        .get(node.as_str())
                        .is_some_and(|rows| rows.contains(value))
                })
                .collect();
            if holders.len() < write {
                return Err(violation(
                    Invariant::ForkFrozenReplicaSet,
                    format!(
                        "write {value} to fork {}, acknowledged at {} ms, was on {holders:?} of its replica set {:?} when it became {:?} at {} ms: fewer than its write quorum of {write}",
                        transition.identity,
                        at.as_millis(),
                        transition.replica_set,
                        transition.phase,
                        transition.at.as_millis()
                    ),
                ));
            }
        }
    }
    for life in &learned.forks {
        let Some(db) = state
            .database(&life.identity.name)
            .filter(|db| identity_of(db).as_ref() == Some(&life.identity))
        else {
            continue;
        };
        if !db
            .fork()
            .is_some_and(fleetring_control::fork::Fork::is_ready)
        {
            continue;
        }
        let (write, _, _) = quorums(db);
        let replicas = expected_replica_set(state, db);
        for (value, _) in fork_writes
            .get(&life.identity.operation)
            .into_iter()
            .flatten()
        {
            let holders = replicas
                .iter()
                .filter(|node| {
                    disk.copies
                        .get(&(life.identity.name.clone(), node.to_string()))
                        .is_some_and(|copy| copy.values.contains(value))
                })
                .count();
            if holders < write {
                return Err(violation(
                    Invariant::ForkFrozenReplicaSet,
                    format!(
                        "acknowledged write {value} to fork {} is on {holders} of its replica set {replicas:?} at the end, fewer than its write quorum of {write}",
                        life.identity
                    ),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn life(
        name: &DatabaseName,
        incarnation: u64,
        operation: u8,
        start: u64,
        end: Option<u64>,
    ) -> Life {
        Life {
            identity: ForkIdentity::new(
                name.clone(),
                Incarnation::new(incarnation),
                OperationId::from_bytes([operation; 16]),
            ),
            source: "src".parse().expect("a name"),
            source_incarnation: Incarnation::new(0),
            write: 2,
            bootstrap: 2,
            start: Duration::from_millis(start),
            end: end.map(Duration::from_millis),
        }
    }

    fn copy(markers: Vec<(u64, Vec<u8>)>) -> CopyOnDisk {
        CopyOnDisk {
            values: Vec::new(),
            markers,
        }
    }

    #[test]
    fn a_copy_belongs_to_the_one_fork_its_marker_names() {
        let dst: DatabaseName = "dst".parse().expect("a name");
        let other: DatabaseName = "other".parse().expect("a name");
        let dropped = life(&dst, 1, 1, 0, Some(1_000));
        let again = life(&dst, 2, 2, 1_500, None);
        let elsewhere = life(&other, 4, 4, 0, None);
        let learned = Learned {
            forks: vec![dropped.clone(), again.clone(), elsewhere],
            ..Learned::default()
        };
        let owner =
            |markers| marked_life(&learned, &dst, &copy(markers)).map(|life| &life.identity);
        assert_eq!(
            owner(vec![dropped.marker()]),
            Some(&dropped.identity),
            "an orphan of the dropped fork"
        );
        assert_eq!(owner(vec![again.marker()]), Some(&again.identity));
        assert_eq!(owner(Vec::new()), None, "no marker names no fork");
        assert_eq!(
            owner(vec![dropped.marker(), again.marker()]),
            None,
            "two markers name no one fork, and are checked as the current fork's"
        );
        assert_eq!(
            owner(vec![(3, vec![3; 16])]),
            None,
            "a marker of no known fork"
        );
        assert_eq!(
            marked_life(&learned, &other, &copy(vec![dropped.marker()])).map(|life| &life.identity),
            None,
            "a marker of another name's fork"
        );
    }
}
