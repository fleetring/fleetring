//! What a primary counts toward a write while a membership change moves its database's replica
//! set (ADR-0031, amendment 2; FR-102).
//!
//! Control state keeps the replica set a membership change moved a leased database off as the
//! set promotions fence, until the primary reports that placement's write quorum holds its log.
//! So a primary counts the write quorums of the replica sets control state last showed it: the
//! set promotions fence and placement, both read from one placement at one membership epoch. A
//! ring newer than that placement only makes the primary read control state again; until it
//! has, it goes on counting the sets it has, which is safe because the set promotions fence
//! changes only by a release, and a release needs this primary's own report about the placement
//! it counts.
//!
//! A newer placement replaces an older one, and never the other way round: a primary that
//! reported placement at one membership epoch never goes back to counting the sets of an earlier
//! one. Two placements at the same membership epoch differ only in whether the kept set was
//! released yet, and counting either covers the set promotions fence.
//!
//! A control node that predates showing the sets (0.7) shows none. A primary under one counts
//! placement by its ring, but never a ring newer than the last `Locate` that answered without
//! sets: it pins the placement of the ring it had when it asked ([`Counting::Ring`]), and counts
//! both the pinned placement and its current ring's until a `Locate` asked under the current
//! ring answers. So a control node upgraded to show the sets, whose first membership change
//! keeps the pinned set, never finds a primary that counted the new placement alone. An answer
//! without sets replaces sets control state showed only if no placement with sets was followed
//! since the question was asked: an answer from before an upgrade never undoes one from after
//! it, and a control node rolled back is followed once it answers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

use fleetring::{Epoch, NodeId};
use fleetring_control::{DatabaseName, Incarnation};
use fleetring_proto::v1 as proto;
use tonic::Status;

/// The replica sets of a database as one placement from control state shows them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSets {
    /// The membership epoch of the control state the placement was read from.
    pub membership_epoch: Epoch,
    /// The database's incarnation then.
    pub incarnation: Incarnation,
    /// The replica set promotions fence: the set a membership change kept, while it keeps one.
    pub fenced: Vec<NodeId>,
    /// Where the ring at `membership_epoch` places the database.
    pub placed: Vec<NodeId>,
}

impl ReplicaSets {
    /// The replica sets `placement` shows, or `None` from a control node that predates them.
    ///
    /// # Errors
    ///
    /// `INTERNAL` for an invalid node ID.
    pub fn from_placement(placement: &proto::Placement) -> Result<Option<Self>, Status> {
        let Some(membership_epoch) = placement.membership_epoch else {
            return Ok(None);
        };
        let node = |text: &String| -> Result<NodeId, Status> {
            text.parse().map_err(|error| {
                Status::internal(format!("the placement has an invalid node id: {error}"))
            })
        };
        let fenced = placement
            .replicas
            .iter()
            .map(|replica| node(&replica.node_id))
            .collect::<Result<Vec<_>, _>>()?;
        let placed = placement
            .placed_replicas
            .iter()
            .map(node)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(Self {
            membership_epoch: Epoch::new(membership_epoch),
            incarnation: Incarnation::new(placement.incarnation),
            fenced,
            placed,
        }))
    }

    /// Whether the set promotions fence isn't placement: a membership change kept it, or a
    /// ready fork keeps its holders, so a write needs the write quorums of both.
    #[must_use]
    pub fn hands_over(&self) -> bool {
        !same_members(&self.fenced, &self.placed)
    }
}

/// Whether `a` and `b` name the same nodes, in any order.
pub(super) fn same_members(a: &[NodeId], b: &[NodeId]) -> bool {
    a.iter().collect::<BTreeSet<_>>() == b.iter().collect::<BTreeSet<_>>()
}

/// What a database's primary on this node counts toward each write, besides a fork's recorded
/// holders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Counting {
    /// The replica sets one placement from control state showed.
    Shown(ReplicaSets),
    /// Placement by this node's ring, under a control node that shows no replica sets: `placed`
    /// is where the ring at `ring_epoch` places the database, and the primary counts it beside
    /// its current ring's placement. `ring_epoch` is `None` until a `Locate` asked under a
    /// known ring has answered without sets, as when the primary has just started serving.
    Ring {
        /// The epoch of the ring `placed` comes from, once confirmed.
        ring_epoch: Option<Epoch>,
        /// Where that ring places the database.
        placed: Vec<NodeId>,
    },
}

/// Whether a primary should read its database's placement from control state again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Staleness {
    /// It counts the newest placement it can know of.
    Fresh,
    /// Control state keeps the old replica set: read every few retry intervals, to learn of the
    /// release.
    Periodic,
    /// Its ring is newer than what it counts, or what it counts was never confirmed: read now.
    Now,
}

#[derive(Debug, Clone)]
struct Entry {
    counting: Counting,
    /// Grows with every placement with sets followed, so that an answer without sets to a
    /// question asked before one is ignored.
    generation: u64,
}

/// The replica sets each database's primary on this node counts, as control state last showed
/// them.
#[derive(Debug, Default)]
pub(super) struct Counted {
    entries: Mutex<BTreeMap<DatabaseName, Entry>>,
}

impl Counted {
    fn lock(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, Entry>> {
        self.entries.lock().expect("replica sets lock poisoned")
    }

    /// Follows `sets` for `name`, unless they are older than the sets already followed: a
    /// placement read at an earlier membership epoch of the same incarnation. Sets of another
    /// incarnation than the ones followed replace them only from the placement a primary is
    /// about to serve from ([`Origin::Serving`]): a placement read for a primary already serving
    /// one incarnation never makes it count the sets of a database made again under its name.
    /// Returns whether it followed them.
    pub(super) fn follow(&self, name: &DatabaseName, sets: ReplicaSets, origin: Origin) -> bool {
        let mut entries = self.lock();
        let generation = entries.get(name).map_or(0, |entry| entry.generation) + 1;
        let newer = match entries.get(name).map(|entry| &entry.counting) {
            Some(Counting::Shown(current)) if current.incarnation != sets.incarnation => {
                origin == Origin::Serving
            }
            Some(Counting::Shown(current)) => current.membership_epoch <= sets.membership_epoch,
            Some(Counting::Ring { .. }) | None => true,
        };
        if newer {
            entries.insert(
                name.clone(),
                Entry {
                    counting: Counting::Shown(sets),
                    generation,
                },
            );
        }
        newer
    }

    /// Starts counting `placed`, placement by this node's ring, for a primary that has just
    /// started serving under a control node that showed no sets, unless this node counts
    /// something for `name` already: sets control state showed, which only an answer to a
    /// question asked after them replaces ([`follow_ring`](Self::follow_ring)), or a pin, which
    /// a newer ring must not overwrite unconfirmed. The placement is confirmed by the next read.
    pub(super) fn seed_ring(&self, name: &DatabaseName, placed: Vec<NodeId>) {
        let mut entries = self.lock();
        if entries.contains_key(name) {
            return;
        }
        let generation = 0;
        entries.insert(
            name.clone(),
            Entry {
                counting: Counting::Ring {
                    ring_epoch: None,
                    placed,
                },
                generation,
            },
        );
    }

    /// Counts `placed`, where this node's ring at `ring_epoch` places `name`, after a `Locate`
    /// asked under that ring answered without sets, unless sets were followed since `asked`,
    /// the generation when it was asked. Returns whether it counts them, and whether that
    /// replaced sets control state had shown.
    pub(super) fn follow_ring(
        &self,
        name: &DatabaseName,
        ring_epoch: Epoch,
        placed: Vec<NodeId>,
        asked: u64,
    ) -> RingFollowed {
        let mut entries = self.lock();
        let current = entries.get(name);
        if current.map_or(0, |entry| entry.generation) != asked {
            return RingFollowed::Ignored;
        }
        let replaced = matches!(
            current.map(|entry| &entry.counting),
            Some(Counting::Shown(_))
        );
        entries.insert(
            name.clone(),
            Entry {
                counting: Counting::Ring {
                    ring_epoch: Some(ring_epoch),
                    placed,
                },
                generation: asked,
            },
        );
        if replaced {
            RingFollowed::ReplacedSets
        } else {
            RingFollowed::Followed
        }
    }

    /// The generation of `name`'s entry: what a question asked now passes to
    /// [`follow_ring`](Self::follow_ring).
    pub(super) fn generation(&self, name: &DatabaseName) -> u64 {
        self.lock().get(name).map_or(0, |entry| entry.generation)
    }

    /// What `name`'s primary counts, if control state has answered about it.
    pub(super) fn get(&self, name: &DatabaseName) -> Option<Counting> {
        self.lock().get(name).map(|entry| entry.counting.clone())
    }

    /// The replica sets control state last showed for `name`, if it showed any.
    pub(super) fn shown(&self, name: &DatabaseName) -> Option<ReplicaSets> {
        match self.get(name)? {
            Counting::Shown(sets) => Some(sets),
            Counting::Ring { .. } => None,
        }
    }

    /// Whether `name`'s primary should read its placement again, with this node's ring at
    /// `ring_epoch`. Never for a database it counts nothing for.
    pub(super) fn staleness(&self, name: &DatabaseName, ring_epoch: Epoch) -> Staleness {
        match self.get(name) {
            None => Staleness::Fresh,
            Some(Counting::Shown(sets)) if ring_epoch > sets.membership_epoch => Staleness::Now,
            Some(Counting::Shown(sets)) if sets.hands_over() => Staleness::Periodic,
            Some(Counting::Shown(_)) => Staleness::Fresh,
            Some(Counting::Ring {
                ring_epoch: Some(pinned),
                ..
            }) if pinned >= ring_epoch => Staleness::Fresh,
            Some(Counting::Ring { .. }) => Staleness::Now,
        }
    }

    /// Forgets `name`'s replica sets, as when the database is dropped.
    pub(super) fn forget(&self, name: &DatabaseName) {
        self.lock().remove(name);
    }
}

/// Where a placement [`Counted::follow`] is given comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Origin {
    /// The placement a primary, or a promotion's candidate, is about to serve from: its
    /// incarnation is the one served from then on.
    Serving,
    /// A placement read for a primary already serving, or by a node that follows a fork.
    Asked,
}

/// What [`Counted::follow_ring`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RingFollowed {
    /// Sets were followed since the question was asked; nothing changed.
    Ignored,
    /// The primary counts placement by the ring.
    Followed,
    /// The primary counts placement by the ring, where it counted sets control state showed: the
    /// control node no longer shows them, as after a rollback.
    ReplacedSets,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sets(membership_epoch: u64, fenced: &[&str], placed: &[&str]) -> ReplicaSets {
        ReplicaSets {
            membership_epoch: Epoch::new(membership_epoch),
            incarnation: Incarnation::LEGACY,
            fenced: nodes(fenced),
            placed: nodes(placed),
        }
    }

    fn nodes(ids: &[&str]) -> Vec<NodeId> {
        ids.iter().map(|id| id.parse().expect("id")).collect()
    }

    fn shown(counted: &Counted, name: &DatabaseName) -> Option<ReplicaSets> {
        counted.shown(name)
    }

    #[test]
    fn a_placement_never_replaces_a_newer_one() {
        let name: DatabaseName = "db".parse().expect("name");
        let counted = Counted::default();
        let kept = sets(5, &["a", "b", "c"], &["a", "b", "x"]);
        assert!(kept.hands_over());
        assert!(counted.follow(&name, kept.clone(), Origin::Serving));
        // An older placement changes nothing.
        let older = sets(4, &["a", "b", "c"], &["a", "b", "c"]);
        assert!(!counted.follow(&name, older, Origin::Asked));
        assert_eq!(shown(&counted, &name), Some(kept));
        // The release, at the same membership epoch, is followed.
        let released = sets(5, &["a", "b", "x"], &["a", "b", "x"]);
        assert!(!released.hands_over());
        assert!(counted.follow(&name, released.clone(), Origin::Asked));
        assert_eq!(shown(&counted, &name), Some(released.clone()));
        counted.forget(&name);
        assert_eq!(counted.get(&name), None);
    }

    #[test]
    fn only_the_served_incarnation_is_followed() {
        let name: DatabaseName = "db".parse().expect("name");
        let counted = Counted::default();
        let served = sets(3, &["a", "b"], &["a", "b"]);
        assert!(counted.follow(&name, served.clone(), Origin::Serving));
        // A database made again under the name: a read for the primary serving the first
        // changes nothing, even at a later membership epoch.
        let mut again = sets(9, &["c"], &["c"]);
        again.incarnation = Incarnation::new(9);
        assert!(!counted.follow(&name, again.clone(), Origin::Asked));
        assert_eq!(shown(&counted, &name), Some(served));
        // A primary that serves the new one follows it.
        assert!(counted.follow(&name, again.clone(), Origin::Serving));
        assert_eq!(shown(&counted, &name), Some(again));
    }

    #[test]
    fn an_answer_without_sets_never_undoes_sets_followed_after_it_was_asked() {
        let name: DatabaseName = "db".parse().expect("name");
        let counted = Counted::default();
        // A primary that started under a control node showing no sets pins its ring's placement,
        // unconfirmed, and reads at once.
        counted.seed_ring(&name, nodes(&["a", "b", "c"]));
        assert_eq!(counted.staleness(&name, Epoch::new(3)), Staleness::Now);
        let asked = counted.generation(&name);
        assert_eq!(
            counted.follow_ring(&name, Epoch::new(3), nodes(&["a", "b", "c"]), asked),
            RingFollowed::Followed
        );
        assert_eq!(counted.staleness(&name, Epoch::new(3)), Staleness::Fresh);
        // A newer ring makes it ask again.
        assert_eq!(counted.staleness(&name, Epoch::new(4)), Staleness::Now);
        let asked = counted.generation(&name);
        // The control node is upgraded meanwhile, and another path follows its sets.
        let kept = sets(4, &["a", "b", "c"], &["a", "b", "x"]);
        assert!(counted.follow(&name, kept.clone(), Origin::Asked));
        // The answer from before the upgrade changes nothing.
        assert_eq!(
            counted.follow_ring(&name, Epoch::new(4), nodes(&["a", "b", "x"]), asked),
            RingFollowed::Ignored
        );
        assert_eq!(shown(&counted, &name), Some(kept));
        // Nor does a serving path's placement without sets.
        counted.seed_ring(&name, nodes(&["a", "b", "x"]));
        assert!(shown(&counted, &name).is_some());
        // A control node rolled back answers a question asked after the sets: followed.
        let asked = counted.generation(&name);
        assert_eq!(
            counted.follow_ring(&name, Epoch::new(5), nodes(&["a", "b", "x"]), asked),
            RingFollowed::ReplacedSets
        );
        assert_eq!(shown(&counted, &name), None);
    }

    #[test]
    fn a_placement_from_an_older_control_node_shows_no_sets() {
        let placement = proto::Placement {
            replicas: vec![proto::Replica {
                node_id: "a".to_owned(),
                ..proto::Replica::default()
            }],
            ..proto::Placement::default()
        };
        assert_eq!(
            ReplicaSets::from_placement(&placement).expect("valid"),
            None
        );
        let placement = proto::Placement {
            placed_replicas: vec!["x".to_owned()],
            membership_epoch: Some(7),
            ..placement
        };
        let shown = ReplicaSets::from_placement(&placement)
            .expect("valid")
            .expect("shown");
        assert_eq!(shown, sets(7, &["a"], &["x"]));
    }
}
