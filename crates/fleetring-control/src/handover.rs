//! Replica-set hand-over on membership changes (ADR-0031, amendment 2; FR-102).
//!
//! A membership change moves a database's replica set in control state at once, but its primary
//! learns of the change only when it next reads control state. Until then it counts the old set
//! toward each write, so a promotion that fenced the new set could miss a write the primary
//! acknowledged. So a membership change never moves the replica set promotions fence: a leased
//! database the change moves keeps the set it moved off ([`Database::kept_replicas`]) while its
//! primary counts both write quorums, the kept set's and placement's, toward every write, and
//! [`Action::ReleaseKeptReplicas`] moves it onto placement once the primary reports that
//! placement's write quorum holds its log. This is the hand-over a ready fork's holders get
//! (ADR-0046, point 3.10, as built), for every database.

use std::collections::BTreeSet;

use fleetring::{Epoch, NodeId, Ring};
use serde::{Deserialize, Serialize};

use crate::fork::ForkState;
use crate::{
    Action, Applied, Database, DatabaseName, Feature, Incarnation, LeaseEpoch, Outcome, Rejection,
    State,
};

/// A database's primary reports that placement's write quorum holds its whole log, and that it
/// counts both the kept replica set's and placement's write quorums toward every write from now
/// on (ADR-0031, amendment 2). [`Action::ReleaseKeptReplicas`] needs this report from the lease
/// holder at the current epoch, about placement at the current membership epoch, before the
/// database's replica set leaves the set a membership change kept.
///
/// # Examples
///
/// ```
/// use fleetring::Epoch;
/// use fleetring_control::{Incarnation, LeaseEpoch, ReplicasCaughtUp};
///
/// let caught_up = ReplicasCaughtUp::new(Incarnation::LEGACY, LeaseEpoch::new(2), Epoch::new(7));
/// assert_eq!(caught_up.epoch(), LeaseEpoch::new(2));
/// assert_eq!(caught_up.membership_epoch(), Epoch::new(7));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicasCaughtUp {
    incarnation: Incarnation,
    epoch: LeaseEpoch,
    membership_epoch: Epoch,
}

impl ReplicasCaughtUp {
    /// The primary of the database's `incarnation` at `epoch` has the write quorum of
    /// placement, as the ring at `membership_epoch` places the database, holding its log.
    #[must_use]
    pub fn new(incarnation: Incarnation, epoch: LeaseEpoch, membership_epoch: Epoch) -> Self {
        Self {
            incarnation,
            epoch,
            membership_epoch,
        }
    }

    /// Returns the incarnation of the database the report is about.
    #[must_use]
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Returns the lease epoch of the primary that reports it.
    #[must_use]
    pub fn epoch(&self) -> LeaseEpoch {
        self.epoch
    }

    /// Returns the membership epoch of the ring whose placement the primary checked.
    #[must_use]
    pub fn membership_epoch(&self) -> Epoch {
        self.membership_epoch
    }
}

/// Whether a membership change may keep `db`'s replica set: it has had a lease, so its primary
/// may have acknowledged writes on it, and it isn't a fork whose recorded holders are its
/// replica set, which a membership change doesn't move (ADR-0046, point 3.10).
fn may_keep(db: &Database) -> bool {
    let frozen = db.fork().is_some_and(|fork| {
        matches!(
            fork.state(),
            ForkState::Seeded { .. }
                | ForkState::Ready {
                    holders: Some(_),
                    ..
                }
        )
    });
    db.lease().is_some() && !frozen
}

/// After a membership change, keeps the replica set each database had under `before`, the ring
/// before the change, where the change moved its placement off it, and stops keeping one
/// placement has come back to: the replica set promotions fence changes only by a release, never
/// by a membership change. Joins and leaves change no database, so whether a database may keep
/// its set reads the same before and after.
///
/// It keeps a set only once [`Feature::Handover`] is enabled, which only the logged
/// [`Action::EnableHandover`] does: what the members' recorded software supports must not
/// decide it, since a binary that knows the feature reads it in software a control node that
/// didn't know it recorded, and would replay that control node's log to other replica sets.
/// Until then a membership change moves replica sets at once, as before.
pub(crate) fn keep_moved(state: &mut State, before: &Ring) {
    if !state.is_enabled(Feature::Handover)
        || crate::mutation::Mutation::MoveKeptReplicaSet.is_active()
    {
        return;
    }
    // A leave of the last member leaves no ring: nothing is placed, and every set is kept.
    let ring = state.ring().ok();
    for db in state.databases.values_mut().filter(|db| may_keep(db)) {
        let name = db.name().as_str();
        let factor = db.replication_factor();
        let fenced = db.kept_replicas().map_or_else(
            || before.replicas_n(name, factor).to_ids(),
            <[NodeId]>::to_vec,
        );
        let placed = ring
            .as_ref()
            .map_or_else(Vec::new, |ring| ring.replicas_n(name, factor).to_ids());
        let kept = (!same_members(&fenced, &placed)).then_some(fenced);
        if kept.is_some() || db.kept_replicas().is_some() {
            db.set_kept_replicas(kept);
        }
    }
}

/// Whether `a` and `b` name the same nodes, in any order.
fn same_members(a: &[NodeId], b: &[NodeId]) -> bool {
    a.iter().collect::<BTreeSet<_>>() == b.iter().collect::<BTreeSet<_>>()
}

/// Applies `ReleaseKeptReplicas` (ADR-0031, amendment 2).
pub(crate) fn release_kept_replicas(
    state: &mut State,
    database: DatabaseName,
    incarnation: Incarnation,
    epoch: LeaseEpoch,
) -> Outcome {
    let db = state.database_or_reject(&database)?;
    let not_releasable = || Rejection::KeptReplicasNotReleasable {
        database: database.clone(),
    };
    if db.incarnation() != incarnation || db.kept_replicas().is_none() {
        return Err(not_releasable());
    }
    // The report must come from the primary serving now: a promotion in progress, or a lease
    // at another epoch, may have a primary that still counts the kept set alone.
    if db.promotion().is_some() && db.epoch() == epoch {
        return Err(Rejection::PromotionInProgress {
            database: database.clone(),
        });
    }
    let Some(lease) = db
        .lease()
        .filter(|lease| lease.epoch() == epoch && db.epoch() == epoch && db.promotion().is_none())
    else {
        return Err(Rejection::StaleEpoch {
            database: database.clone(),
            given: epoch,
            current: db.epoch(),
        });
    };
    if !reported(state, db, lease.holder(), epoch) {
        return Err(not_releasable());
    }
    if let Some(db) = state.databases.get_mut(&database) {
        db.set_kept_replicas(None);
    }
    Ok(Applied::KeptReplicasReleased { database, epoch })
}

/// Whether `holder`, the lease holder of `db` at `epoch`, last reported that placement's write
/// quorum holds its log, about placement now: a ring that changed since may place the database
/// on nodes that don't hold its log.
fn reported(state: &State, db: &Database, holder: &NodeId, epoch: LeaseEpoch) -> bool {
    state
        .report(holder, db.name())
        .and_then(crate::Report::replicas_caught_up)
        .is_some_and(|caught_up| {
            caught_up.incarnation() == db.incarnation()
                && caught_up.epoch() == epoch
                && caught_up.membership_epoch() == state.membership_epoch()
        })
}

impl State {
    /// Returns a [`Action::ReleaseKeptReplicas`] for every database whose replica set a
    /// membership change kept, once the primary serving it now reports that placement's write
    /// quorum, at the current membership epoch, holds its log, in name order (ADR-0031,
    /// amendment 2).
    ///
    /// The control node asks this on its timer and submits each; `apply` checks each one again.
    #[must_use]
    pub fn releases_due(&self) -> Vec<Action> {
        self.databases()
            .filter(|db| db.kept_replicas().is_some())
            .filter_map(|db| {
                let lease = db
                    .lease()
                    .filter(|lease| lease.epoch() == db.epoch() && db.promotion().is_none())?;
                let epoch = lease.epoch();
                reported(self, db, lease.holder(), epoch).then(|| Action::ReleaseKeptReplicas {
                    database: db.name().clone(),
                    incarnation: db.incarnation(),
                    epoch,
                })
            })
            .collect()
    }

    /// Whether the control node should log [`Action::EnableHandover`] before `action`: the
    /// feature isn't enabled, every member, live or down, supports it, and `action` is a join or
    /// a leave that would move the placement of a database that may keep its replica set
    /// (ADR-0031, amendment 2). So the feature is enabled on its first use, as a command's
    /// feature is (ADR-0038, point 4), and a fleet that never moved a leased database can still
    /// roll back. It looks at the databases only while the feature isn't enabled.
    #[must_use]
    pub fn handover_due(&self, action: &Action) -> bool {
        if self.is_enabled(Feature::Handover) || self.check_feature(Feature::Handover).is_err() {
            return false;
        }
        let members: Vec<&crate::Member> = self.members().collect();
        let after: Vec<&crate::Member> = match action {
            // A joining member that lacks the feature: the join itself is refused once the
            // feature is enabled, so enabling it here would close rollback for a join that never
            // happens. The join goes through as a mixed fleet's change instead.
            Action::Join { member } if !member.running().supports(Feature::Handover) => {
                return false;
            }
            Action::Join { member } => members
                .iter()
                .copied()
                .filter(|known| known.id() != member.id())
                .chain(std::iter::once(member))
                .collect(),
            Action::Leave { id } => members
                .iter()
                .copied()
                .filter(|known| known.id() != id)
                .collect(),
            _ => return false,
        };
        let epoch = self.membership_epoch();
        let (Ok(before), Ok(after)) = (
            crate::membership_ring(members, epoch),
            crate::membership_ring(after, epoch),
        ) else {
            return false;
        };
        self.databases().filter(|db| may_keep(db)).any(|db| {
            let name = db.name().as_str();
            let factor = db.replication_factor();
            !same_members(
                &before.replicas_n(name, factor).to_ids(),
                &after.replicas_n(name, factor).to_ids(),
            )
        })
    }

    /// Returns where placement puts `database` now: the ring's replica set over all members,
    /// which differs from [`State::replica_set`] while a fork's recorded holders or a set a
    /// membership change kept are its replica set.
    ///
    /// # Errors
    ///
    /// Fails as [`State::replica_set`] does.
    pub fn placed_replica_set(&self, database: &DatabaseName) -> Result<Vec<NodeId>, Rejection> {
        let db = self.database_or_reject(database)?;
        self.placed_replicas(db)
    }
}
