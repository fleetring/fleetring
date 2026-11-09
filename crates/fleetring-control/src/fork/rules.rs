//! The checks `apply` and the fork driver share, so that the driver proposes only what `apply`
//! would accept.

use std::collections::BTreeSet;

use fleetring::{NodeId, ReplicationFactor};

use crate::fork::{ChainHash, Fork, ForkIdentity, ForkState, ResetReport, StagedCopy};
use crate::{
    Acks, Database, DatabaseName, Durability, Incarnation, LeaseEpoch, Position, Quorums,
    Rejection, Report, State, Timestamp,
};

/// The headroom a replica must keep beyond a fork's copy, as control-plane admission counts it:
/// 1 GiB, the floor of ADR-0046's `max(1 GiB, 5% of the volume)`. Heartbeats carry free bytes
/// but not the volume's size, so the control plane checks the floor and the data node, which
/// knows its volume, checks the rest (ADR-0046, point 7).
pub(crate) const ADMISSION_HEADROOM: u64 = 1 << 30;

/// Returns the database and the fork that `identity` names, if it is the current one: the
/// database `identity.name` exists, has `identity.incarnation`, and was made by
/// `identity.operation`.
///
/// # Errors
///
/// [`Rejection::StaleForkIdentity`] otherwise: the name is unknown, or names an ordinary
/// database, another incarnation, or a fork made by another operation.
pub(crate) fn current_fork<'s>(
    state: &'s State,
    identity: &ForkIdentity,
) -> Result<(&'s Database, &'s Fork), Rejection> {
    state
        .database(&identity.name)
        .filter(|db| db.incarnation() == identity.incarnation)
        .and_then(|db| Some((db, db.fork()?)))
        .filter(|(_, fork)| fork.operation() == identity.operation)
        .ok_or_else(|| Rejection::StaleForkIdentity {
            fork: identity.clone(),
        })
}

/// Returns the identity of `db`'s fork.
pub(crate) fn identity_of(db: &Database, fork: &Fork) -> ForkIdentity {
    ForkIdentity::new(db.name().clone(), db.incarnation(), fork.operation())
}

/// Returns the bootstrap quorum of a fork with `replication_factor` replicas and `durability`:
/// W_boot = max(⌊R/2⌋ + 1, W), the acknowledgements its reset needs (ADR-0046, point 3). Under
/// `acks = local` it is a majority, so W_boot + Q > R holds for a fork's first writes.
pub(crate) fn bootstrap_quorum(
    replication_factor: ReplicationFactor,
    durability: Durability,
) -> u8 {
    let majority = Quorums::with_acks(replication_factor, Acks::Quorum).write();
    majority.max(durability.quorums(replication_factor).write())
}

/// Returns the completion quorum of a fork with `replication_factor` replicas and
/// `durability`: C = max(⌊R/2⌋ + 1, W, `min_replicas`), the holders its completion needs
/// (ADR-0046, point 3).
pub(crate) fn completion_quorum(
    replication_factor: ReplicationFactor,
    durability: Durability,
) -> u8 {
    bootstrap_quorum(replication_factor, durability).max(durability.min_replicas())
}

/// Returns the completion quorum of `db`, a fork, under the durability it was made with: it
/// can't change before the fork is ready.
pub(crate) fn completion_quorum_of(db: &Database) -> usize {
    usize::from(completion_quorum(db.replication_factor(), db.durability()))
}

/// Returns the next seeder among `replicas` at `now` (ADR-0046, point 3.3): the first live one,
/// in preference order, that is neither passed over as a seeder nor disk-refused; or, if every
/// live one that isn't disk-refused was passed over, the first of those. Passing over is a
/// preference, not a ban.
pub(crate) fn next_seeder(
    state: &State,
    replicas: &[NodeId],
    seeder_excluded: &BTreeSet<NodeId>,
    disk_refused: &BTreeSet<NodeId>,
    now: Timestamp,
) -> Option<NodeId> {
    let mut eligible = replicas
        .iter()
        .filter(|id| !disk_refused.contains(*id) && state.is_live(id, now));
    let first = eligible.clone().find(|id| !seeder_excluded.contains(*id));
    first.or_else(|| eligible.next()).cloned()
}

/// Checks that a fork's source still exists at the incarnation the fork copies.
///
/// # Errors
///
/// [`Rejection::SourceIncarnationMismatch`] naming the source's current incarnation, or none if
/// it was dropped.
pub(crate) fn check_source(state: &State, fork: &Fork) -> Result<(), Rejection> {
    let current = state.database(fork.source()).map(Database::incarnation);
    if current == Some(fork.source_incarnation()) {
        return Ok(());
    }
    Err(Rejection::SourceIncarnationMismatch {
        source_database: fork.source().clone(),
        expected: fork.source_incarnation(),
        current,
    })
}

/// A staged copy, as a completion claims or a seeded fork recorded it.
pub(crate) struct Claim<'c> {
    pub(crate) identity: &'c ForkIdentity,
    pub(crate) attempt: u32,
    pub(crate) at: &'c Position,
    pub(crate) chain_hash: &'c ChainHash,
    pub(crate) source_incarnation: Incarnation,
}

impl Claim<'_> {
    /// Returns whether `report` shows exactly this staged copy, at log epoch 0 (ADR-0046, point
    /// 4.5): a report claiming another log epoch for a staged copy doesn't count.
    pub(crate) fn shown_by(&self, report: &Report) -> bool {
        report.log_epoch() == LeaseEpoch::ZERO
            && report.staged().is_some_and(|staged| self.matches(staged))
    }

    fn matches(&self, staged: &StagedCopy) -> bool {
        staged.fork() == self.identity
            && staged.attempt() == self.attempt
            && staged.position() == self.at
            && staged.chain_hash() == self.chain_hash
            && staged.source_incarnation() == self.source_incarnation
    }
}

/// Checks that `holders` may complete a fork: at least `need` of them, each a member of
/// `replicas` that didn't refuse on disk and whose latest report is exactly the claimed staged
/// copy at log epoch 0.
///
/// # Errors
///
/// [`Rejection::NotEnoughHolders`], [`Rejection::UnknownMember`],
/// [`Rejection::NotInReplicaSet`], [`Rejection::DiskRefusedHolder`], or
/// [`Rejection::HolderReportMismatch`].
pub(crate) fn check_holders(
    state: &State,
    claim: &Claim<'_>,
    holders: &BTreeSet<NodeId>,
    replicas: &[NodeId],
    disk_refused: &BTreeSet<NodeId>,
    need: usize,
) -> Result<(), Rejection> {
    let have = holders.len();
    if have < need {
        return Err(Rejection::NotEnoughHolders {
            have,
            need: u8::try_from(need).unwrap_or(u8::MAX),
        });
    }
    let database = &claim.identity.name;
    for holder in holders {
        if state.member(holder).is_none() {
            return Err(Rejection::UnknownMember { id: holder.clone() });
        }
        if !replicas.contains(holder) {
            return Err(Rejection::NotInReplicaSet {
                id: holder.clone(),
                database: database.clone(),
            });
        }
        if disk_refused.contains(holder) {
            return Err(Rejection::DiskRefusedHolder {
                id: holder.clone(),
                database: database.clone(),
            });
        }
        let shown = state
            .report(holder, database)
            .is_some_and(|report| claim.shown_by(report));
        if !shown {
            return Err(Rejection::HolderReportMismatch {
                id: holder.clone(),
                database: database.clone(),
            });
        }
    }
    Ok(())
}

/// Returns whether `report` shows exactly the staged copy a seeded fork completed with, at log
/// epoch 0.
pub(crate) fn shows_seeded_copy(db: &Database, fork: &Fork, report: &Report) -> bool {
    let ForkState::Seeded {
        attempt,
        at,
        chain_hash,
        ..
    } = fork.state()
    else {
        return false;
    };
    let identity = identity_of(db, fork);
    Claim {
        identity: &identity,
        attempt: *attempt,
        at,
        chain_hash,
        source_incarnation: fork.source_incarnation(),
    }
    .shown_by(report)
}

/// Returns how many members of the fork's replica set last reported the reset of `identity`
/// committed at `epoch` and `position`.
pub(crate) fn resets_reported(
    state: &State,
    identity: &ForkIdentity,
    epoch: LeaseEpoch,
    position: &Position,
) -> usize {
    let Ok(replicas) = state.replica_set(&identity.name) else {
        return 0;
    };
    let wanted = ResetReport::committed(identity.clone(), epoch, position.clone());
    replicas
        .iter()
        .filter(|replica| {
            state
                .report(replica, &identity.name)
                .and_then(Report::reset)
                .is_some_and(|reset| *reset == wanted)
        })
        .count()
}

/// Returns how a promotion ranks `report` of `db`, or `None` if it doesn't make its member a
/// candidate (ADR-0046, points 4.5 and 6.2).
///
/// - A report showing a staged copy is a candidate only if it is exactly the copy a seeded fork
///   completed with, at log epoch 0, and then ranks at `(0, at)`: below any replica holding
///   later writes, such as the committed reset.
/// - While a fork is seeded, a report without a staged copy is a candidate only if its log comes
///   from a lease issued since the completion: a log epoch above `lease_from` and no higher
///   than the database's epoch. A copy opened in an earlier attempt, or an undeleted copy of an
///   earlier database of the same name, is none, so it never outranks the staged copies.
pub(crate) fn candidate_rank<'r>(
    db: &Database,
    report: &'r Report,
) -> Option<(LeaseEpoch, &'r Position)> {
    let fork = db.fork();
    if let Some(staged) = report.staged() {
        let fork = fork?;
        return shows_seeded_copy(db, fork, report)
            .then_some((LeaseEpoch::ZERO, staged.position()));
    }
    let since_completion = |lease_from: LeaseEpoch| {
        report.log_epoch() > lease_from && report.log_epoch() <= db.epoch()
    };
    match fork.map(Fork::state) {
        Some(ForkState::Seeded { lease_from, .. }) if !since_completion(*lease_from) => None,
        _ => Some((report.log_epoch(), report.position())),
    }
}

/// Returns the most bytes any member reported for `database`'s copy, or 0 if none did.
pub(crate) fn reported_size(state: &State, database: &DatabaseName) -> u64 {
    state
        .members
        .values()
        .filter_map(|entry| entry.reports.get(database)?.size_bytes())
        .max()
        .unwrap_or(0)
}

/// What admitting a copy of `bytes` reserves: its size plus a tenth (ADR-0046, point 7).
fn reservation(bytes: u64) -> u128 {
    u128::from(bytes) + u128::from(bytes / 10)
}

/// Returns how many bytes `node` has promised to seeding forks it hasn't staged yet: the
/// reservation of each seeding fork whose replica set holds it, that it didn't refuse, and
/// whose current attempt it hasn't reported a staged copy for.
fn pending_seeds(state: &State, node: &NodeId) -> u128 {
    state
        .databases()
        .filter_map(|db| {
            let fork = db.fork()?;
            let ForkState::Seeding {
                attempt,
                disk_refused,
                ..
            } = fork.state()
            else {
                return None;
            };
            if disk_refused.contains(node) || !state.replica_set(db.name()).ok()?.contains(node) {
                return None;
            }
            let identity = identity_of(db, fork);
            let staged = state
                .report(node, db.name())
                .and_then(Report::staged)
                .is_some_and(|staged| *staged.fork() == identity && staged.attempt() == *attempt);
            (!staged).then(|| reservation(fork.source_bytes()))
        })
        .sum()
}

/// Returns how many of `replicas` have room for a copy of `bytes`, as far as their heartbeats
/// say: free bytes, less what seeding forks have reserved there, of at least the copy's
/// reservation plus [`ADMISSION_HEADROOM`]. A member that hasn't reported its free bytes counts
/// as having room, and so does every member when the copy's size is unknown: the data node's
/// own check decides (ADR-0046, point 7.3).
pub(crate) fn admitting(state: &State, replicas: &[NodeId], bytes: u64) -> usize {
    if bytes == 0 {
        return replicas.len();
    }
    replicas
        .iter()
        .filter(|node| {
            state.disk_free_bytes(node).is_none_or(|free| {
                u128::from(free)
                    >= pending_seeds(state, node)
                        + reservation(bytes)
                        + u128::from(ADMISSION_HEADROOM)
            })
        })
        .count()
}
