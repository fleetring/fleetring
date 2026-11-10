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
//!   completion quorum C = max(⌊R/2⌋ + 1, W, `min_replicas`) of holders that reported it; and no
//!   client write, read, or watch acknowledged on a fork before its reset committed at its
//!   bootstrap quorum, as its primary's hooks show.
//! - **(b) Acknowledged rows only.** A fork's rows from its source are a prefix of the source's
//!   surviving history, in order, and none was refused.
//! - **(c) One source incarnation.** No copy of a fork, staged or final, holds a row written to
//!   another incarnation of its source.
//! - **(d) Identities apart.** No fork command `apply` accepts names another identity than the
//!   current one; completion and readiness count only reports of the command's identity; no
//!   seeding request of a dropped identity is served; and no fork holds another fork's rows.
//! - **(e) Independence.** No source holds a row written to its fork, and no fork a source row
//!   after its own.
//! - **(f) Operations.** One operation ID names one fork identity while its record lasts; a
//!   resubmission is answered with that identity only from its requester with the same request;
//!   and a terminal outcome never changes.
//! - **(g) Termination.** A fork leaves `seeding` by its deadline, and ends as its scenario
//!   expects.
//! - **(h) One reset.** No fork-reset write runs once a client write to the fork was acknowledged;
//!   every copy of a ready fork that holds its writes holds exactly its own marker; and no watch
//!   of a fork sees a change it inherited.
//! - **(i) Frozen replica set.** While a fork is seeded or ready with its holders kept, its lease
//!   stays with a holder; and every write acknowledged before it became ready, or before its
//!   holders were released, is on a write quorum of the replica set it moved to.
//! - **Disk admission.** No node holds more concurrent forks' copies than its free disk admits.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::fork::{ForkIdentity, ForkState, OperationId};
use fleetring_control::{
    Acks, Action, Command, Database, DatabaseName, Incarnation, Report, State, apply,
};

use crate::sim::check::{Invariant, Violation};
use crate::sim::history::excerpt;
use crate::sim::workload::{
    Ack, ForkPhaseKind, History, Operation, OutcomeClass, Record, SeedingProbe,
};

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
    start: Duration,
    end: Option<Duration>,
}

impl Life {
    fn contains(&self, record: &Record) -> bool {
        record.invoked_at() >= self.start && self.end.is_none_or(|end| record.completed_at() <= end)
    }
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
    let (learned, last) = replay(genesis, commands, clock, expectations)?;
    let fork_writes = fork_writes(history, &learned);
    fencing_of_clients(history, &learned)?;
    rows(history, &learned, &last, disk)?;
    probes(history, &learned)?;
    operations(history, &learned, expectations)?;
    reset_once(history, &learned, &last, disk, &fork_writes)?;
    handover(history, &learned, &last, disk, &fork_writes)?;
    Ok(())
}

/// Replays `commands` from `genesis`, checking (a), (d), (f), (g), (i), and disk admission per
/// command, and returns what the data checks need, with the last state.
fn replay(
    genesis: &State,
    commands: &[Command],
    clock: Clock<'_>,
    expectations: &ForkExpectations,
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
        // The frozen replica set: a seeded fork's lease, or a ready one's whose holders are
        // kept, is a holder's, whatever moved it.
        if let Some(db) = &after
            && let Some(fork) = db.fork()
            && let Some(lease) = db.lease()
        {
            let holders = match fork.state() {
                ForkState::Seeded { holders, .. }
                | ForkState::Ready {
                    holders: Some(holders),
                    ..
                } => Some(holders),
                _ => None,
            };
            if let Some(holders) = holders
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
    outcomes(&state, expectations)?;
    copy_budgets(&learned, expectations)?;
    Ok((learned, state))
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
            let committed = reports
                .values()
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
                        "fork {fork} was marked ready at {at} ms on {committed} reports of its reset, fewer than its bootstrap quorum of {bootstrap}"
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
                learned.forks.push(Life {
                    identity,
                    source: fork.source().clone(),
                    source_incarnation: fork.source_incarnation(),
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

/// (a): no client data call acknowledged on a fork before its reset committed at its bootstrap
/// quorum, as the first primary's hooks show.
fn fencing_of_clients(history: &History, learned: &Learned) -> Result<(), Violation> {
    if !cfg!(feature = "fault-hooks") {
        return Ok(());
    }
    let opened = first_pass(history, crate::replication::FaultPoint::AfterQuorumCommit);
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
        let open = opened.get(&life.identity.operation);
        if open.is_none_or(|open| record.completed_at() < *open) {
            return Err(violation(
                Invariant::ForkFencing,
                format!(
                    "{} on fork {} was served at {} ms, before its reset committed at its bootstrap quorum ({open:?})",
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
fn origins<'h>(history: &'h History) -> BTreeMap<i64, (&'h DatabaseName, &'h Record)> {
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
        let mut copies: Vec<(String, &[i64])> = Vec::new();
        if current {
            for ((name, host), copy) in &disk.copies {
                if *name == life.identity.name {
                    copies.push((format!("{host}'s copy"), &copy.values));
                }
            }
        }
        let directory = format!("{}-{}", life.identity.incarnation, life.identity.operation);
        for (host, name, dir, values) in &disk.staged {
            if *name == life.identity.name && *dir == directory {
                copies.push((format!("{host}'s staged copy"), values));
            }
        }
        let source = state
            .database(&life.source)
            .filter(|db| db.incarnation() == life.source_incarnation)
            .and_then(|_| surviving(state, disk, &life.source));
        for (whose, values) in copies {
            let mut from_source = Vec::new();
            let mut own = false;
            for value in values {
                let Some((database, record)) = origins.get(value) else {
                    continue;
                };
                if **database == life.identity.name && life.contains(record) {
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
        let marker = (
            life.identity.incarnation.get(),
            life.identity.operation.to_bytes().to_vec(),
        );
        for ((name, host), copy) in &disk.copies {
            if *name != life.identity.name || !copy.values.iter().any(|value| ours.contains(value))
            {
                continue;
            }
            if copy.markers != [marker.clone()] {
                return Err(violation(
                    Invariant::ForkResetOnce,
                    format!(
                        "{host}'s copy of ready fork {} holds its writes and the markers {:?}, not exactly its own",
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
    }
    Ok(())
}

/// (i): every write acknowledged before a fork became ready, or before its holders were
/// released, is on a write quorum of the replica set it moved to, and every acknowledged write
/// of a ready fork is on one of its final replica set.
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
        let Some(db) = state.database(&transition.identity.name) else {
            continue;
        };
        let (write, _, _) = quorums(db);
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
        let Ok(replicas) = state.replica_set(&life.identity.name) else {
            continue;
        };
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
