//! Diffs between cells layers: which keys move to another cell, and which change only inside
//! their cell (ADR-0012).

use std::borrow::Borrow;
use std::fmt;
use std::iter::FusedIterator;

use crate::{CellPlacement, Cells, Node, Placement};

impl<P: Placement> Cells<P> {
    /// Returns the keys whose placement differs between this layer and `target`, lazily.
    ///
    /// The diff walks `keys` in order and places each key on both layers. It yields a
    /// [`CellChange`] for every key whose cell path or replica set differs, and skips every other
    /// key. Each change says whether the key moves to another cell, which copies all of its data
    /// across cells, or changes only inside its cell, as a [`Ring::diff`](crate::Ring::diff)
    /// change does. Each key costs one [`replicas`](Self::replicas) lookup on each layer. Call
    /// [`CellsDiff::summarize`] to count the changes instead.
    ///
    /// Cell weights are explicit, so changes inside a cell never move keys across cells: adding,
    /// removing, or reweighting a node inside a cell yields in-cell changes only, and changing a
    /// cell's weight in the selector ring yields cross-cell moves into or out of that cell only.
    ///
    /// # Examples
    ///
    /// Grow one cell's share of the hashed keys:
    ///
    /// ```
    /// use fleetring::{Cells, Node, Ring, Weight};
    ///
    /// let ring = |seed: u64, ids: [&str; 3]| {
    ///     Ring::builder()
    ///         .seed(seed)
    ///         .nodes(ids.map(|id| Node::new(id.parse().expect("valid id"))))
    ///         .build()
    /// };
    /// let selector = |big_weight: u32| -> Result<Ring, Box<dyn std::error::Error>> {
    ///     Ok(Ring::builder()
    ///         .seed(100)
    ///         .node(Node::new("a".parse()?))
    ///         .node(Node::new("b".parse()?).with_weight(Weight::new(big_weight).ok_or("zero")?))
    ///         .build()?)
    /// };
    /// let build = |big_weight: u32| -> Result<Cells, Box<dyn std::error::Error>> {
    ///     Ok(Cells::builder()
    ///         .ring_selector(selector(big_weight)?)
    ///         .cell("a".parse()?, ring(1, ["a1", "a2", "a3"])?)
    ///         .cell("b".parse()?, ring(2, ["b1", "b2", "b3"])?)
    ///         .build()?)
    /// };
    /// let (current, target) = (build(1)?, build(3)?);
    ///
    /// let keys: Vec<String> = (0..1_000).map(|i| format!("tenant:{i}")).collect();
    /// for change in current.diff(&target, &keys) {
    ///     // Growing cell b only moves keys from a to b, and moves all of their replicas.
    ///     assert!(change.is_cross_cell());
    ///     assert_eq!(change.to().cell().map(|cell| cell.as_str()), Some("b"));
    ///     assert_eq!(change.added().count(), 3);
    /// }
    /// let summary = current.diff(&target, &keys).summarize();
    /// assert_eq!(summary.cross_cell_moves(), summary.keys_changed());
    /// assert_eq!(summary.in_cell_changes(), 0);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn diff<'a, I>(&'a self, target: &'a Cells<P>, keys: I) -> CellsDiff<'a, P, I::IntoIter>
    where
        I: IntoIterator,
        I::Item: AsRef<[u8]>,
    {
        CellsDiff {
            current: self,
            target,
            keys: keys.into_iter(),
        }
    }
}

/// A lazy iterator over the keys whose placement differs between two cells layers.
///
/// Created by [`Cells::diff`]. It yields one [`CellChange`] per changed key, in key order, and
/// skips unchanged keys. [`summarize`](Self::summarize) counts the changes instead of yielding
/// them.
#[must_use = "a diff is lazy and does nothing until iterated or summarized"]
pub struct CellsDiff<'a, P: Placement, I> {
    current: &'a Cells<P>,
    target: &'a Cells<P>,
    keys: I,
}

impl<P, I> CellsDiff<'_, P, I>
where
    P: Placement,
    I: Iterator,
    I::Item: AsRef<[u8]>,
{
    /// Consumes the diff and counts every key it examines and every change it finds.
    ///
    /// Only the keys the diff has not yet examined are counted, so summarizing a fresh diff
    /// covers every key.
    #[must_use]
    pub fn summarize(self) -> CellMovementSummary {
        let Self {
            current,
            target,
            keys,
        } = self;
        let mut summary = CellMovementSummary::default();
        for key in keys {
            summary.keys_examined += 1;
            if let Some(change) = CellChange::between(current, target, key) {
                summary.record(&change);
            }
        }
        summary
    }
}

impl<'a, P, I> Iterator for CellsDiff<'a, P, I>
where
    P: Placement,
    I: Iterator,
    I::Item: AsRef<[u8]>,
{
    type Item = CellChange<'a, I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let (current, target) = (self.current, self.target);
        self.keys
            .find_map(|key| CellChange::between(current, target, key))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Any number of the remaining keys may be unchanged.
        (0, self.keys.size_hint().1)
    }
}

impl<P, I> FusedIterator for CellsDiff<'_, P, I>
where
    P: Placement,
    I: FusedIterator,
    I::Item: AsRef<[u8]>,
{
}

// Not derived: the derives would require `P: Clone` and `P: Debug`, but the diff only borrows
// the layers.
impl<P: Placement, I: Clone> Clone for CellsDiff<'_, P, I> {
    fn clone(&self) -> Self {
        Self {
            current: self.current,
            target: self.target,
            keys: self.keys.clone(),
        }
    }
}

impl<P: Placement, I: fmt::Debug> fmt::Debug for CellsDiff<'_, P, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CellsDiff")
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

/// One key whose placement differs between two cells layers, as [`Cells::diff`] yields it.
///
/// [`from`](Self::from) is the key's placement on the current layer and [`to`](Self::to) its
/// placement on the target layer. They always differ, in their cell paths, their replica sets, or
/// both. A change is one of two kinds:
///
/// - A **cross-cell move** ([`is_cross_cell`](Self::is_cross_cell)): the key's cell path
///   changes, so all of its data moves to another cell, every replica is
///   [added](Self::added) in the new cell and [removed](Self::removed) from the old one, and the
///   lease moves with it.
/// - An **in-cell change**: the key stays in its cell, and its replica set changes inside the
///   cell's ring, exactly as a [`Change`](crate::Change) from [`Ring::diff`](crate::Ring::diff)
///   does.
///
/// To migrate a key, copy its data to every added node, hand off the lease if the
/// [primary changed](Self::primary_changed), then drop the data from every removed node, so it
/// never has fewer replicas than it started with.
///
/// Equality compares the keys, and both placements' paths and replica IDs, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellChange<'a, K> {
    key: K,
    from: CellPlacement<'a>,
    to: CellPlacement<'a>,
}

impl<'a, K> CellChange<'a, K> {
    /// Returns the change for `key` between two layers, or `None` if its placement is the same
    /// on both.
    fn between<P: Placement>(current: &'a Cells<P>, target: &'a Cells<P>, key: K) -> Option<Self>
    where
        K: AsRef<[u8]>,
    {
        let from = current.replicas(key.as_ref());
        let to = target.replicas(key.as_ref());
        (from != to).then_some(Self { key, from, to })
    }

    /// Returns the key, as the diff's input yielded it.
    #[must_use]
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Returns the key, consuming the change.
    #[must_use]
    pub fn into_key(self) -> K {
        self.key
    }

    /// Returns the key's placement on the current layer: where its data lives now.
    #[must_use]
    pub fn from(&self) -> &CellPlacement<'a> {
        &self.from
    }

    /// Returns the key's placement on the target layer: where its data lives once the migration
    /// finishes.
    #[must_use]
    pub fn to(&self) -> &CellPlacement<'a> {
        &self.to
    }

    /// Returns whether the key moves to another cell: whether its cell paths differ.
    ///
    /// When false, the key changes only inside its cell.
    #[must_use]
    pub fn is_cross_cell(&self) -> bool {
        self.from.path() != self.to.path()
    }

    /// Returns whether the preferred primary changes: it is in another cell, or it is another
    /// node of the same cell.
    ///
    /// A primary change means a leadership handoff, so a single-leader system moves the key's
    /// lease.
    #[must_use]
    pub fn primary_changed(&self) -> bool {
        self.is_cross_cell()
            || self.from.replicas().primary().id() != self.to.replicas().primary().id()
    }

    /// Returns the nodes that gain a replica, in the target placement's preference order.
    ///
    /// After a cross-cell move, that is every target replica. Within a cell, it is the target
    /// replicas whose IDs aren't among the current ones, as in [`Change::added`].
    ///
    /// [`Change::added`]: crate::Change::added
    pub fn added(&self) -> impl Iterator<Item = &'a Node> {
        missing_from(&self.to, &self.from, self.is_cross_cell())
    }

    /// Returns the nodes that lose their replica, in the current placement's preference order.
    ///
    /// After a cross-cell move, that is every current replica. Within a cell, it is the current
    /// replicas whose IDs aren't among the target ones, as in [`Change::removed`].
    ///
    /// [`Change::removed`]: crate::Change::removed
    pub fn removed(&self) -> impl Iterator<Item = &'a Node> {
        missing_from(&self.from, &self.to, self.is_cross_cell())
    }

    /// Returns whether the change only reorders the replicas: the key stays in its cell, on the
    /// same nodes, in a different order.
    ///
    /// A reorder moves no data. It can still move the lease, when the primary is among the
    /// reordered nodes.
    #[must_use]
    pub fn is_reorder_only(&self) -> bool {
        self.added().next().is_none() && self.removed().next().is_none()
    }
}

/// Returns the replicas of `set`, in preference order, that `other` lacks: all of them when the
/// placements are in different cells, otherwise those whose IDs aren't in `other`.
fn missing_from<'set, 'a>(
    set: &'set CellPlacement<'a>,
    other: &'set CellPlacement<'a>,
    cross_cell: bool,
) -> impl Iterator<Item = &'a Node> {
    let replicas = set.replicas();
    // Iterate by position: `ReplicaSet::get` returns nodes borrowed for the layer's lifetime,
    // while `ReplicaSet::iter` borrows them only for as long as it borrows the set.
    (0..replicas.len())
        .filter_map(move |position| replicas.get(position))
        .filter(move |node| cross_cell || !other.replicas().contains(node.id()))
}

/// How much data a cells layer change moves: keys that move across cells, keys that change only
/// inside their cell, and lease handoffs.
///
/// Build one with [`CellsDiff::summarize`], or with [`from_changes`](Self::from_changes) when
/// you keep the changes to act on them. Every changed key is either a cross-cell move or an
/// in-cell change, so the two counts add up to [`keys_changed`](Self::keys_changed).
///
/// # Examples
///
/// ```
/// use fleetring::{Cells, Directory, Node, Ring};
///
/// let ring = |seed: u64, ids: &[&str]| {
///     Ring::builder()
///         .seed(seed)
///         .nodes(ids.iter().map(|id| Node::new(id.parse().expect("valid id"))))
///         .build()
/// };
/// let build = |main: Ring| -> Result<Cells, Box<dyn std::error::Error>> {
///     Ok(Cells::builder()
///         .directory(Directory::new().prefix("vip:", "vip".parse()?).catch_all("main".parse()?))
///         .cell("main".parse()?, main)
///         .cell("vip".parse()?, ring(2, &["v1", "v2", "v3"])?)
///         .build()?)
/// };
/// let current = build(ring(1, &["m1", "m2", "m3"])?)?;
/// let target = build(ring(1, &["m1", "m2", "m3", "m4"])?)?;
///
/// let keys: Vec<String> = (0..100).map(|i| format!("tenant:{i}")).collect();
/// let summary = current.diff(&target, &keys).summarize();
/// // A new node in the main cell never moves a key to another cell.
/// assert_eq!(summary.keys_examined(), 100);
/// assert_eq!(summary.cross_cell_moves(), 0);
/// assert_eq!(summary.in_cell_changes(), summary.keys_changed());
/// println!("{summary}");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CellMovementSummary {
    keys_examined: u64,
    keys_changed: u64,
    cross_cell_moves: u64,
    in_cell_changes: u64,
    primary_changes: u64,
}

impl CellMovementSummary {
    /// Summarizes `changes`, which came from diffing `keys_examined` keys.
    ///
    /// Use this when you keep the changes to act on them; otherwise [`CellsDiff::summarize`] is
    /// simpler and counts the examined keys itself. `changes` may yield changes or references to
    /// them. A summary never reports more changed keys than examined ones, so a `keys_examined`
    /// below the number of changes is raised to it.
    #[must_use]
    pub fn from_changes<'a, K, C>(changes: C, keys_examined: u64) -> Self
    where
        C: IntoIterator,
        C::Item: Borrow<CellChange<'a, K>>,
    {
        let mut summary = Self::default();
        for change in changes {
            summary.record(change.borrow());
        }
        summary.keys_examined = keys_examined.max(summary.keys_changed);
        summary
    }

    /// Returns how many keys were diffed, changed or not.
    #[must_use]
    pub fn keys_examined(&self) -> u64 {
        self.keys_examined
    }

    /// Returns how many keys changed their placement: their cell path, their replica set, or
    /// both.
    #[must_use]
    pub fn keys_changed(&self) -> u64 {
        self.keys_changed
    }

    /// Returns how many keys move to another cell, each copying all of its data across cells.
    #[must_use]
    pub fn cross_cell_moves(&self) -> u64 {
        self.cross_cell_moves
    }

    /// Returns how many keys stay in their cell and change their replica set inside it.
    #[must_use]
    pub fn in_cell_changes(&self) -> u64 {
        self.in_cell_changes
    }

    /// Returns how many keys change their preferred primary, each a lease handoff.
    #[must_use]
    pub fn primary_changes(&self) -> u64 {
        self.primary_changes
    }

    /// Counts one change.
    fn record<K>(&mut self, change: &CellChange<'_, K>) {
        self.keys_changed += 1;
        if change.is_cross_cell() {
            self.cross_cell_moves += 1;
        } else {
            self.in_cell_changes += 1;
        }
        if change.primary_changed() {
            self.primary_changes += 1;
        }
    }
}

impl fmt::Display for CellMovementSummary {
    /// Formats the summary as an aligned plain-text table, such as:
    ///
    /// ```text
    /// keys examined     1000
    /// keys changed       257  25.70%
    /// cross-cell moves   250  25.00%
    /// in-cell changes      7   0.70%
    /// primary changes    253  25.30%
    /// ```
    ///
    /// Shares are of the examined keys, rounded to two decimals. The output has no trailing
    /// newline.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let share = |count| Some(percent(count, self.keys_examined));
        let rows = [
            ("keys examined", self.keys_examined, None),
            ("keys changed", self.keys_changed, share(self.keys_changed)),
            (
                "cross-cell moves",
                self.cross_cell_moves,
                share(self.cross_cell_moves),
            ),
            (
                "in-cell changes",
                self.in_cell_changes,
                share(self.in_cell_changes),
            ),
            (
                "primary changes",
                self.primary_changes,
                share(self.primary_changes),
            ),
        ]
        .map(|(label, count, share)| (label, count.to_string(), share));
        let label_width = width(rows.iter().map(|(label, ..)| *label));
        let count_width = width(rows.iter().map(|(_, count, _)| count.as_str()));
        let share_width = width(rows.iter().filter_map(|(.., share)| share.as_deref()));
        for (index, (label, count, share)) in rows.iter().enumerate() {
            if index > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{label:<label_width$}  {count:>count_width$}")?;
            if let Some(share) = share {
                write!(f, "  {share:>share_width$}")?;
            }
        }
        Ok(())
    }
}

/// Formats `part / whole` as a percentage with two decimals, rounding half up. An empty whole
/// shows as 0.00%.
fn percent(part: u64, whole: u64) -> String {
    // Integer arithmetic only (ADR-0003). In u128, `part * 10_000` cannot overflow.
    let basis_points = match u128::from(whole) {
        0 => 0,
        whole => (u128::from(part) * 10_000 + whole / 2) / whole,
    };
    format!("{}.{:02}%", basis_points / 100, basis_points % 100)
}

/// Returns the length of the longest cell, for padding a column.
fn width<'cell>(cells: impl Iterator<Item = &'cell str>) -> usize {
    cells.map(str::len).max().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_summary_displays_aligned_counts_and_shares() {
        let summary = CellMovementSummary {
            keys_examined: 1_000,
            keys_changed: 257,
            cross_cell_moves: 250,
            in_cell_changes: 7,
            primary_changes: 253,
        };
        assert_eq!(
            summary.to_string(),
            "keys examined     1000\n\
             keys changed       257  25.70%\n\
             cross-cell moves   250  25.00%\n\
             in-cell changes      7   0.70%\n\
             primary changes    253  25.30%"
        );
    }

    #[test]
    fn an_empty_summary_shows_zero_shares() {
        assert_eq!(
            CellMovementSummary::default().to_string(),
            "keys examined     0\n\
             keys changed      0  0.00%\n\
             cross-cell moves  0  0.00%\n\
             in-cell changes   0  0.00%\n\
             primary changes   0  0.00%"
        );
    }
}
