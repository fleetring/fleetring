//! Ring diffs and movement summaries for migration planning (ADR-0001, ADR-0006).

use std::array;
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::iter::{self, FusedIterator};
use std::num::NonZeroU32;

use crate::{Node, NodeId, ReplicaSet, Ring, StableHasher};

/// The prefix of the synthetic keys that [`Ring::estimate_movement`] diffs, followed by the
/// sample's index. The keys are not part of the placement spec: changing them changes estimates,
/// never placement.
const SAMPLE_KEY_PREFIX: &str = "fleetring-movement-sample-";

impl<H: StableHasher> Ring<H> {
    /// Returns the keys whose replica sets differ between this ring and `target`, lazily.
    ///
    /// The diff walks `keys` in order and looks up each key on both rings. It yields a
    /// [`Change`] for every key whose replica set differs, in its members or in their order,
    /// and skips every other key. It pulls keys only as it is iterated, and each key costs one
    /// [`replicas`](Self::replicas) lookup on each ring. Call [`Diff::summarize`] to count the
    /// changes instead.
    ///
    /// This plans a two-ring migration (ADR-0001): build the target ring, diff the current ring
    /// against it over every key, then work through the changes. The rings may differ in
    /// anything that affects placement, such as membership, weights, labels, the replication
    /// factor, or the seed. Adding or removing one node swaps at most one replica of each
    /// key (ADR-0006), so each of its changes adds at most one node and removes at most one.
    ///
    /// # Examples
    ///
    /// Plan adding a node:
    ///
    /// ```
    /// use fleetring::{Epoch, Node, Ring};
    ///
    /// let current = Ring::builder()
    ///     .node(Node::new("node-1".parse()?))
    ///     .node(Node::new("node-2".parse()?))
    ///     .node(Node::new("node-3".parse()?))
    ///     .epoch(Epoch::new(1))
    ///     .build()?;
    /// let target = current
    ///     .to_builder()
    ///     .node(Node::new("node-4".parse()?))
    ///     .epoch(Epoch::new(2))
    ///     .build()?;
    ///
    /// let keys = ["agent:alice", "agent:bob", "tenant:acme", "tenant:globex"];
    /// for change in current.diff(&target, keys) {
    ///     // Adding a node only ever copies data to that node.
    ///     let added: Vec<&str> = change.added().map(|node| node.id().as_str()).collect();
    ///     assert_eq!(added, ["node-4"]);
    ///
    ///     // Copy first, hand off the lease, and drop last: no key is ever short of replicas.
    ///     for node in change.added() {
    ///         println!("copy {} to {}", change.key(), node.id());
    ///     }
    ///     if change.primary_changed() {
    ///         let (old, new) = (change.from().primary().id(), change.to().primary().id());
    ///         println!("hand the lease of {} from {old} to {new}", change.key());
    ///     }
    ///     for node in change.removed() {
    ///         println!("drop {} from {}", change.key(), node.id());
    ///     }
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn diff<'a, I>(&'a self, target: &'a Ring<H>, keys: I) -> Diff<'a, H, I::IntoIter>
    where
        I: IntoIterator,
        I::Item: AsRef<[u8]>,
    {
        Diff {
            current: self,
            target,
            keys: keys.into_iter(),
        }
    }

    /// Estimates how much data moves from this ring to `target`, for dry runs without a list of
    /// keys.
    ///
    /// Diffs `samples` synthetic keys, `fleetring-movement-sample-0` through
    /// `fleetring-movement-sample-{samples - 1}`, and summarizes the result. The counts describe
    /// those keys, not yours: read them as fractions of
    /// [`keys_examined`](MovementSummary::keys_examined), which equals `samples`. The estimate is
    /// deterministic, but the sample keys are not part of the placement spec, so a later release
    /// may estimate differently.
    ///
    /// Each estimated fraction has a standard error of at most `0.5 / √samples`: 100,000 samples
    /// give ±0.16 percentage points. Each sample costs one [`replicas`](Self::replicas) lookup on
    /// each ring.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroU32;
    ///
    /// use fleetring::{Node, Ring};
    ///
    /// let mut builder = Ring::builder();
    /// for i in 1..=10 {
    ///     builder = builder.node(Node::new(format!("node-{i}").parse()?));
    /// }
    /// let current = builder.build()?;
    /// let target = current.to_builder().node(Node::new("node-11".parse()?)).build()?;
    ///
    /// let samples = NonZeroU32::new(10_000).ok_or("zero samples")?;
    /// let estimate = current.estimate_movement(&target, samples);
    /// assert_eq!(estimate.keys_examined(), 10_000);
    /// // With 3 replicas each, an 11th equal node takes a replica of about 3/11 of the keys.
    /// let percent = estimate.keys_changed() * 100 / estimate.keys_examined();
    /// assert!((25..=30).contains(&percent), "{percent}%");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn estimate_movement(&self, target: &Ring<H>, samples: NonZeroU32) -> MovementSummary {
        let keys = (0..samples.get()).map(|i| format!("{SAMPLE_KEY_PREFIX}{i}"));
        self.diff(target, keys).summarize()
    }
}

/// A lazy iterator over the keys whose replica sets differ between two rings.
///
/// Created by [`Ring::diff`]. It yields one [`Change`] per changed key, in key order, and skips
/// unchanged keys. [`summarize`](Self::summarize) counts the changes instead of yielding them.
///
/// # Examples
///
/// Size a migration before starting it:
///
/// ```
/// use fleetring::{Node, Ring};
///
/// let current = Ring::builder()
///     .node(Node::new("laptop".parse()?))
///     .node(Node::new("nas".parse()?))
///     .node(Node::new("vps".parse()?))
///     .build()?;
/// let target = current.to_builder().remove_node("laptop").build()?;
///
/// let keys: Vec<String> = (0..1_000).map(|i| format!("agent:{i}")).collect();
/// let summary = current.diff(&target, &keys).summarize();
/// // With 3 replicas on 3 nodes, every key has a replica on the laptop.
/// assert_eq!(summary.keys_changed(), 1_000);
/// assert_eq!(summary.replicas_removed().get("laptop"), Some(&1_000));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
#[must_use = "a diff is lazy and does nothing until iterated or summarized"]
pub struct Diff<'ring, H, I> {
    current: &'ring Ring<H>,
    target: &'ring Ring<H>,
    keys: I,
}

impl<H, I> Diff<'_, H, I>
where
    H: StableHasher,
    I: Iterator,
    I::Item: AsRef<[u8]>,
{
    /// Consumes the diff and counts every key it examines and every change it finds.
    ///
    /// Only the keys the diff has not yet examined are counted, so summarizing a fresh diff
    /// covers every key.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let current = Ring::builder().node(Node::new("old".parse()?)).build()?;
    /// let target = Ring::builder().node(Node::new("new".parse()?)).build()?;
    ///
    /// let summary = current.diff(&target, ["agent:1", "agent:2"]).summarize();
    /// assert_eq!(summary.keys_examined(), 2);
    /// assert_eq!(summary.keys_changed(), 2);
    /// assert_eq!(summary.primary_changes(), 2);
    /// assert_eq!(summary.replicas_added().get("new"), Some(&2));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn summarize(self) -> MovementSummary {
        let Self {
            current,
            target,
            keys,
        } = self;
        let mut summary = MovementSummary::default();
        for key in keys {
            summary.keys_examined += 1;
            if let Some(change) = Change::between(current, target, key) {
                summary.record(&change);
            }
        }
        summary
    }
}

impl<'ring, H, I> Iterator for Diff<'ring, H, I>
where
    H: StableHasher,
    I: Iterator,
    I::Item: AsRef<[u8]>,
{
    type Item = Change<'ring, I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let (current, target) = (self.current, self.target);
        self.keys
            .find_map(|key| Change::between(current, target, key))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Any number of the remaining keys may be unchanged.
        (0, self.keys.size_hint().1)
    }
}

impl<H, I> FusedIterator for Diff<'_, H, I>
where
    H: StableHasher,
    I: FusedIterator,
    I::Item: AsRef<[u8]>,
{
}

// Not derived: the derive would require `H: Clone`, but the diff only borrows the rings.
impl<H, I: Clone> Clone for Diff<'_, H, I> {
    fn clone(&self) -> Self {
        Self {
            current: self.current,
            target: self.target,
            keys: self.keys.clone(),
        }
    }
}

/// One key whose replica set differs between two rings, as [`Ring::diff`] yields it.
///
/// [`from`](Self::from) is the key's replica set on the current ring and [`to`](Self::to) is its
/// replica set on the target ring. They always differ, in their members, their order, or both.
///
/// To migrate a key's data, copy it to every [`added`](Self::added) node, then drop it from
/// every [`removed`](Self::removed) node, so it never has fewer replicas than it started with.
/// When [`primary_changed`](Self::primary_changed) is true, the lease must also move: hand it to
/// the new primary once that node has caught up, and before dropping the old primary's copy.
///
/// Equality compares the keys and the node IDs of both replica sets, in order.
///
/// # Examples
///
/// Drain a node:
///
/// ```
/// use fleetring::{Node, Ring};
///
/// let current = Ring::builder()
///     .node(Node::new("laptop".parse()?))
///     .node(Node::new("nas".parse()?))
///     .node(Node::new("vps".parse()?))
///     .build()?;
/// let target = current.to_builder().remove_node("laptop").build()?;
///
/// for change in current.diff(&target, ["agent:1", "agent:2", "agent:3"]) {
///     // Draining a node moves only the keys it held, and copies them nowhere: two nodes
///     // are left for three replicas.
///     assert!(change.from().contains("laptop"));
///     assert_eq!(change.added().count(), 0);
///     let removed: Vec<&str> = change.removed().map(|node| node.id().as_str()).collect();
///     assert_eq!(removed, ["laptop"]);
///     assert!(change.to().is_under_replicated());
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change<'ring, K> {
    key: K,
    from: ReplicaSet<'ring>,
    to: ReplicaSet<'ring>,
}

impl<'ring, K> Change<'ring, K> {
    /// Returns the change for `key` between two rings, or `None` if the key's replica set is the
    /// same on both.
    fn between<H: StableHasher>(
        current: &'ring Ring<H>,
        target: &'ring Ring<H>,
        key: K,
    ) -> Option<Self>
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

    /// Returns the key's replica set on the current ring: where the key's data lives now.
    #[must_use]
    pub fn from(&self) -> &ReplicaSet<'ring> {
        &self.from
    }

    /// Returns the key's replica set on the target ring: where the key's data lives once the
    /// migration finishes.
    #[must_use]
    pub fn to(&self) -> &ReplicaSet<'ring> {
        &self.to
    }

    /// Returns the nodes that gain a replica, in the target ring's preference order.
    ///
    /// These are the nodes of [`to`](Self::to) whose IDs are not in [`from`](Self::from). Nodes
    /// are compared by ID, so a node that keeps its replica while its weight or labels change is
    /// neither added nor removed. Copy the key's data to each added node before dropping it from
    /// any [`removed`](Self::removed) node.
    pub fn added(&self) -> impl Iterator<Item = &'ring Node> {
        missing_from(&self.to, &self.from)
    }

    /// Returns the nodes that lose their replica, in the current ring's preference order.
    ///
    /// These are the nodes of [`from`](Self::from) whose IDs are not in [`to`](Self::to). Drop
    /// the data from them only after every [`added`](Self::added) node holds a copy and,
    /// if the [primary changed](Self::primary_changed), after the lease has moved.
    pub fn removed(&self) -> impl Iterator<Item = &'ring Node> {
        missing_from(&self.from, &self.to)
    }

    /// Returns whether the preferred primary differs between the two rings.
    ///
    /// A primary change means a leadership handoff: a single-leader system moves the key's lease
    /// from the old primary to the new one. The new primary may already hold a replica, in which
    /// case no data moves for it.
    #[must_use]
    pub fn primary_changed(&self) -> bool {
        self.from.primary().id() != self.to.primary().id()
    }

    /// Returns whether the change only reorders the replicas: the same nodes hold the key
    /// on both rings, in a different order.
    ///
    /// A reorder moves no data. It can still move the lease, when the primary is among the
    /// reordered nodes.
    #[must_use]
    pub fn is_reorder_only(&self) -> bool {
        self.added().next().is_none() && self.removed().next().is_none()
    }
}

/// Returns the nodes of `set`, in preference order, whose IDs are not in `other`.
fn missing_from<'set, 'ring>(
    set: &'set ReplicaSet<'ring>,
    other: &'set ReplicaSet<'ring>,
) -> impl Iterator<Item = &'ring Node> {
    // Iterate by position: `ReplicaSet::get` returns nodes borrowed for the ring's lifetime,
    // while `ReplicaSet::iter` borrows them only for as long as it borrows the set.
    (0..set.len())
        .filter_map(move |position| set.get(position))
        .filter(move |node| !other.contains(node.id()))
}

/// How much data a ring change moves: changed keys, lease handoffs, and replicas gained and lost
/// per node.
///
/// Build one with [`Diff::summarize`], or with [`from_changes`](Self::from_changes) when you keep
/// the changes to act on them. [`Ring::estimate_movement`] estimates one when you have no list of
/// keys. The per-node counts are [`BTreeMap`]s keyed by [`NodeId`], so iterating them and
/// displaying the summary always list nodes in the same order.
///
/// # Examples
///
/// ```
/// use fleetring::{Node, Ring};
///
/// let current = Ring::builder()
///     .node(Node::new("node-1".parse()?))
///     .node(Node::new("node-2".parse()?))
///     .build()?;
/// let target = current.to_builder().node(Node::new("node-3".parse()?)).build()?;
///
/// let keys = ["agent:alice", "agent:bob", "tenant:acme"];
/// let summary = current.diff(&target, keys).summarize();
/// // Two nodes held 2 of 3 replicas, so the third node gets a copy of every key.
/// assert_eq!(summary.keys_examined(), 3);
/// assert_eq!(summary.keys_changed(), 3);
/// assert_eq!(summary.replicas_added().get("node-3"), Some(&3));
/// assert!(summary.replicas_removed().is_empty());
/// println!("{summary}");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MovementSummary {
    keys_examined: u64,
    keys_changed: u64,
    primary_changes: u64,
    reorder_only_changes: u64,
    replicas_added: BTreeMap<NodeId, u64>,
    replicas_removed: BTreeMap<NodeId, u64>,
}

impl MovementSummary {
    /// Summarizes `changes`, which came from diffing `keys_examined` keys.
    ///
    /// Use this when you keep the changes to act on them; otherwise [`Diff::summarize`] is
    /// simpler and counts the examined keys itself. `changes` may yield changes or references to
    /// them. A summary never reports more changed keys than examined ones, so a `keys_examined`
    /// below the number of changes is raised to it.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{MovementSummary, Node, Ring};
    ///
    /// let current = Ring::builder()
    ///     .node(Node::new("node-1".parse()?))
    ///     .node(Node::new("node-2".parse()?))
    ///     .build()?;
    /// let target = current.to_builder().node(Node::new("node-3".parse()?)).build()?;
    /// let keys = ["agent:alice", "agent:bob", "tenant:acme"];
    ///
    /// let changes: Vec<_> = current.diff(&target, keys).collect();
    /// let summary = MovementSummary::from_changes(&changes, 3);
    /// assert_eq!(summary, current.diff(&target, keys).summarize());
    /// // The changes are still there to act on.
    /// assert_eq!(changes.len(), 3);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn from_changes<'ring, K, C>(changes: C, keys_examined: u64) -> Self
    where
        C: IntoIterator,
        C::Item: Borrow<Change<'ring, K>>,
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

    /// Returns how many keys changed their replica set, in members or order.
    #[must_use]
    pub fn keys_changed(&self) -> u64 {
        self.keys_changed
    }

    /// Returns how many keys changed their preferred primary, each a lease handoff.
    #[must_use]
    pub fn primary_changes(&self) -> u64 {
        self.primary_changes
    }

    /// Returns how many keys kept the same nodes in a different order, which moves no data.
    #[must_use]
    pub fn reorder_only_changes(&self) -> u64 {
        self.reorder_only_changes
    }

    /// Returns how many replicas each node gains: the keys to copy to it.
    ///
    /// Nodes that gain nothing are absent.
    #[must_use]
    pub fn replicas_added(&self) -> &BTreeMap<NodeId, u64> {
        &self.replicas_added
    }

    /// Returns how many replicas each node loses: the keys to drop from it.
    ///
    /// Nodes that lose nothing are absent.
    #[must_use]
    pub fn replicas_removed(&self) -> &BTreeMap<NodeId, u64> {
        &self.replicas_removed
    }

    /// Counts one change.
    fn record<K>(&mut self, change: &Change<'_, K>) {
        self.keys_changed += 1;
        if change.primary_changed() {
            self.primary_changes += 1;
        }
        if change.is_reorder_only() {
            self.reorder_only_changes += 1;
        }
        for node in change.added() {
            increment(&mut self.replicas_added, node.id());
        }
        for node in change.removed() {
            increment(&mut self.replicas_removed, node.id());
        }
    }

    /// Writes the key counts, each with its share of the examined keys.
    fn write_totals(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let share = |count| Some(percent(count, self.keys_examined));
        let rows = [
            ("keys examined", self.keys_examined, None),
            ("keys changed", self.keys_changed, share(self.keys_changed)),
            (
                "primary changes",
                self.primary_changes,
                share(self.primary_changes),
            ),
            (
                "reorder-only changes",
                self.reorder_only_changes,
                share(self.reorder_only_changes),
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

    /// Writes a blank line and a table of the replicas each node gains and loses, or nothing if
    /// no replica moves.
    fn write_nodes(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ids: BTreeSet<&NodeId> = self
            .replicas_added
            .keys()
            .chain(self.replicas_removed.keys())
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let count = |counts: &BTreeMap<NodeId, u64>, id: &NodeId| {
            counts.get(id).copied().unwrap_or(0).to_string()
        };
        let header = ["node", "replicas added", "replicas removed"].map(String::from);
        let rows: Vec<[String; 3]> = iter::once(header)
            .chain(ids.into_iter().map(|id| {
                [
                    id.to_string(),
                    count(&self.replicas_added, id),
                    count(&self.replicas_removed, id),
                ]
            }))
            .collect();
        let widths: [usize; 3] =
            array::from_fn(|column| width(rows.iter().map(|row| row[column].as_str())));
        let [node_width, added_width, removed_width] = widths;
        f.write_str("\n")?;
        for [node, added, removed] in &rows {
            write!(
                f,
                "\n{node:<node_width$}  {added:>added_width$}  {removed:>removed_width$}"
            )?;
        }
        Ok(())
    }
}

impl fmt::Display for MovementSummary {
    /// Formats the summary as an aligned plain-text table, such as:
    ///
    /// ```text
    /// keys examined         24
    /// keys changed          22  91.67%
    /// primary changes        5  20.83%
    /// reorder-only changes   2   8.33%
    ///
    /// node      replicas added  replicas removed
    /// laptop-1               0                 9
    /// laptop-2               0                11
    /// vps                   20                 0
    /// ```
    ///
    /// Shares are of the examined keys, rounded to two decimals. The node table lists every node
    /// that gains or loses replicas, in [`NodeId`] order, and is left out when no replica moves.
    /// The output has no trailing newline.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_totals(f)?;
        self.write_nodes(f)
    }
}

/// Adds 1 to `id`'s count, cloning the ID only the first time it is counted.
fn increment(counts: &mut BTreeMap<NodeId, u64>, id: &NodeId) {
    if let Some(count) = counts.get_mut(id) {
        *count += 1;
    } else {
        counts.insert(id.clone(), 1);
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
