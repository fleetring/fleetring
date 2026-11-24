//! Tombstones: what control state remembers of a database it dropped or expired (ADR-0050).
//!
//! A data node deletes its copy of a database only on a tombstone that names the copy: the same
//! random incarnation, at an epoch at least as high as the copy's highest. Control state that
//! merely doesn't know a database says nothing, since an empty, restored, or foreign control
//! state says the same. A tombstone is kept for a bounded window, then forgotten by a stamped
//! [`Action::Tick`](crate::Action::Tick). A node that comes back after that finds none, and
//! keeps its copy as an orphan.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::fork::OperationId;
use crate::{DatabaseName, Incarnation, LeaseEpoch, Millis, Timestamp};

/// How long a tombstone is kept by default: seven days, longer than a node is usually down for
/// repair (ADR-0050, owner decision 1).
pub const DEFAULT_TOMBSTONE_WINDOW: Millis = Millis::new(7 * 24 * 60 * 60 * 1_000);

/// The most tombstones kept for one name. A name dropped and created again more often than this
/// within the window forgets its oldest first, and a node that missed those drops keeps an orphan.
pub const MAX_TOMBSTONES_PER_NAME: usize = 16;

/// The most tombstones control state keeps. Past it the oldest is forgotten first, which leaves
/// a node that missed that drop an orphan and never deletes anything.
pub const MAX_TOMBSTONES: usize = 32_768;

/// Why a database has a tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TombstoneCause {
    /// `db drop`.
    Dropped,
    /// Its expiry passed.
    Expired,
}

/// The record of one dropped or expired database.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Incarnation, LeaseEpoch, Timestamp, Tombstone, TombstoneCause};
///
/// let tombstone = Tombstone::new(
///     Incarnation::new(7),
///     LeaseEpoch::new(3),
///     None,
///     TombstoneCause::Dropped,
///     Timestamp::from_millis(10),
///     Timestamp::from_millis(20),
/// );
/// assert!(tombstone.names_copy(Some(Incarnation::new(7)), LeaseEpoch::new(2)));
/// assert!(!tombstone.names_copy(Some(Incarnation::new(7)), LeaseEpoch::new(4)));
/// assert!(!tombstone.names_copy(Some(Incarnation::new(8)), LeaseEpoch::new(3)));
/// assert!(!tombstone.names_copy(None, LeaseEpoch::new(3)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    incarnation: Incarnation,
    epoch: LeaseEpoch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operation: Option<OperationId>,
    cause: TombstoneCause,
    dropped_at: Timestamp,
    kept_until: Timestamp,
}

impl Tombstone {
    /// Returns the tombstone of the database of `incarnation`, dropped at `epoch`, and of its
    /// fork `operation` if it was a fork, kept from `dropped_at` until `kept_until`.
    #[must_use]
    pub const fn new(
        incarnation: Incarnation,
        epoch: LeaseEpoch,
        operation: Option<OperationId>,
        cause: TombstoneCause,
        dropped_at: Timestamp,
        kept_until: Timestamp,
    ) -> Self {
        Self {
            incarnation,
            epoch,
            operation,
            cause,
            dropped_at,
            kept_until,
        }
    }

    /// Returns the dropped database's incarnation.
    #[must_use]
    pub const fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Returns the epoch the database was at when it was dropped.
    #[must_use]
    pub const fn epoch(&self) -> LeaseEpoch {
        self.epoch
    }

    /// Returns the operation that made the database, if it was a fork.
    #[must_use]
    pub const fn operation(&self) -> Option<OperationId> {
        self.operation
    }

    /// Returns why the database has a tombstone.
    #[must_use]
    pub const fn cause(&self) -> TombstoneCause {
        self.cause
    }

    /// Returns when the database was dropped.
    #[must_use]
    pub const fn dropped_at(&self) -> Timestamp {
        self.dropped_at
    }

    /// Returns when the tombstone may be forgotten.
    #[must_use]
    pub const fn kept_until(&self) -> Timestamp {
        self.kept_until
    }

    /// Returns whether this tombstone names a copy that records `incarnation` and has seen at
    /// most `highest`: the one rule by which a data node deletes a copy (ADR-0050, point 2).
    ///
    /// A copy that records no incarnation is never named, whatever its epoch: a database created
    /// again under the name restarts its epochs, so an epoch alone can't say whose copy it is.
    #[must_use]
    pub fn names_copy(&self, incarnation: Option<Incarnation>, highest: LeaseEpoch) -> bool {
        tombstone_names_copy(self.incarnation, self.epoch, incarnation, highest)
    }
}

/// Returns whether a tombstone of the database of `named`, dropped at `epoch`, names a copy that
/// records `incarnation` and has seen at most `highest`: the one rule by which a data node
/// deletes a copy (ADR-0050, point 2). The legacy incarnation names nothing, since every database
/// of a name made before incarnations shares it.
#[must_use]
pub fn tombstone_names_copy(
    named: Incarnation,
    epoch: LeaseEpoch,
    incarnation: Option<Incarnation>,
    highest: LeaseEpoch,
) -> bool {
    !named.is_legacy() && incarnation == Some(named) && highest <= epoch
}

/// Every tombstone control state keeps, by name, each name's oldest first, and the window new ones
/// are kept for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstones {
    /// How long a new tombstone is kept. Absent until the first command that sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    window: Option<Millis>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    by_name: BTreeMap<DatabaseName, Vec<Tombstone>>,
}

impl Tombstones {
    /// Whether nothing is recorded: no window and no tombstone, as in a state from before the
    /// feature, which then serializes exactly as it did.
    pub(crate) fn is_unset(&self) -> bool {
        self.window.is_none() && self.by_name.is_empty()
    }

    /// Returns how long a new tombstone is kept, once a command has set it.
    #[must_use]
    pub const fn window(&self) -> Option<Millis> {
        self.window
    }

    pub(crate) fn set_window(&mut self, window: Millis) {
        self.window = Some(window);
    }

    /// Returns `name`'s tombstones, oldest first.
    #[must_use]
    pub fn of(&self, name: &DatabaseName) -> &[Tombstone] {
        self.by_name.get(name).map_or(&[], Vec::as_slice)
    }

    /// Returns whether any tombstone names `incarnation`, under any name.
    pub(crate) fn holds_incarnation(&self, incarnation: Incarnation) -> bool {
        self.iter()
            .any(|(_, tombstone)| tombstone.incarnation == incarnation)
    }

    /// Returns every tombstone with its name, in name order and each name's oldest first.
    pub fn iter(&self) -> impl Iterator<Item = (&DatabaseName, &Tombstone)> {
        self.by_name
            .iter()
            .flat_map(|(name, kept)| kept.iter().map(move |tombstone| (name, tombstone)))
    }

    /// Returns how many tombstones are kept.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_name.values().map(Vec::len).sum()
    }

    /// Returns whether no tombstone is kept.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// Returns whether a [`Tick`](crate::Action::Tick) stamped `at` would forget a tombstone.
    #[must_use]
    pub fn purge_due(&self, at: Timestamp) -> bool {
        self.iter().any(|(_, tombstone)| tombstone.kept_until <= at)
    }

    /// Records `tombstone` for `name`, forgetting the oldest past either bound.
    pub(crate) fn record(&mut self, name: &DatabaseName, tombstone: Tombstone) {
        let kept = self.by_name.entry(name.clone()).or_default();
        kept.push(tombstone);
        if kept.len() > MAX_TOMBSTONES_PER_NAME {
            kept.remove(0);
        }
        if self.len() > MAX_TOMBSTONES {
            self.forget_oldest();
        }
    }

    /// Forgets the tombstone dropped earliest, the first name's on a tie, so that every replay
    /// forgets the same one. Each name's list is oldest first, so only the heads are compared.
    fn forget_oldest(&mut self) {
        let oldest = self
            .by_name
            .iter()
            .filter_map(|(name, kept)| kept.first().map(|first| (first.dropped_at, name)))
            .min()
            .map(|(_, name)| name.clone());
        if let Some(name) = oldest
            && let Some(kept) = self.by_name.get_mut(&name)
        {
            kept.remove(0);
            if kept.is_empty() {
                self.by_name.remove(&name);
            }
        }
    }

    /// Forgets every tombstone whose `kept_until` is at or before `now`.
    pub(crate) fn purge_through(&mut self, now: Timestamp) {
        self.by_name.retain(|_, kept| {
            kept.retain(|tombstone| tombstone.kept_until > now);
            !kept.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tombstone(incarnation: u64, dropped_at: u64) -> Tombstone {
        Tombstone::new(
            Incarnation::new(incarnation),
            LeaseEpoch::new(1),
            None,
            TombstoneCause::Dropped,
            Timestamp::from_millis(dropped_at),
            Timestamp::from_millis(dropped_at + 100),
        )
    }

    fn name(text: &str) -> DatabaseName {
        text.parse().expect("a database name")
    }

    #[test]
    fn a_name_keeps_its_newest_tombstones() {
        let mut tombstones = Tombstones::default();
        let db = name("scratch");
        let extra = 3;
        let total = u64::try_from(MAX_TOMBSTONES_PER_NAME + extra).expect("fits");
        for index in 1..=total {
            tombstones.record(&db, tombstone(index, index));
        }
        let kept = tombstones.of(&db);
        assert_eq!(kept.len(), MAX_TOMBSTONES_PER_NAME);
        assert_eq!(
            kept.first().map(Tombstone::incarnation),
            Some(Incarnation::new(u64::try_from(extra).expect("fits") + 1))
        );
    }

    #[test]
    fn the_fleet_wide_bound_forgets_the_oldest_drop() {
        let mut tombstones = Tombstones::default();
        for index in 0..MAX_TOMBSTONES {
            let at = u64::try_from(index).expect("fits") + 10;
            tombstones.record(&name(&format!("db-{index}")), tombstone(at, at));
        }
        tombstones.record(&name("last"), tombstone(1, 1_000_000));
        assert_eq!(tombstones.len(), MAX_TOMBSTONES);
        assert!(tombstones.of(&name("db-0")).is_empty(), "the oldest went");
        assert_eq!(tombstones.of(&name("last")).len(), 1);
    }

    #[test]
    fn purging_forgets_only_what_the_time_has_passed() {
        let mut tombstones = Tombstones::default();
        let db = name("scratch");
        tombstones.record(&db, tombstone(1, 0));
        tombstones.record(&db, tombstone(2, 50));
        assert!(!tombstones.purge_due(Timestamp::from_millis(99)));
        assert!(tombstones.purge_due(Timestamp::from_millis(100)));
        tombstones.purge_through(Timestamp::from_millis(100));
        assert_eq!(tombstones.of(&db).len(), 1);
        tombstones.purge_through(Timestamp::from_millis(150));
        assert!(tombstones.is_empty());
    }
}
