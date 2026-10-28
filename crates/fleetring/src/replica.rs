//! Replica sets and replication factors (ADR-0006).

use std::fmt;
use std::iter::FusedIterator;
use std::num::NonZeroU8;
use std::slice;

use crate::select::Picks;
use crate::{Node, NodeId};

/// The nodes a key is placed on, best first: the preferred primary, then its followers.
///
/// A replica set is never empty. Its first node is the *preferred* primary: a single-leader
/// system confirms it with a lease or an election of its own, or fails over to a follower. When
/// the ring has fewer eligible nodes than the replication factor, the set is shorter and
/// [`is_under_replicated`] returns true.
///
/// Equality compares the node IDs in order and ignores the replication factor. The set displays
/// as its IDs in order, such as `[node-1, node-2]`.
///
/// # Examples
///
/// ```
/// use fleetring::{Node, Ring};
///
/// let ring = Ring::builder()
///     .node(Node::new("node-1".parse()?).with_zone("eu-west-1a".parse()?))
///     .node(Node::new("node-2".parse()?).with_zone("eu-west-1b".parse()?))
///     .build()?;
/// let replicas = ring.replicas("tenant:acme");
/// // Two nodes can't hold the default three replicas.
/// assert_eq!(replicas.len(), 2);
/// assert!(replicas.is_under_replicated());
/// let follower = replicas.followers().next().ok_or("no follower")?;
/// assert_ne!(replicas.primary().zone(), follower.zone());
/// println!("writes go to {}; replicas: {replicas}", replicas.primary().id());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// [`is_under_replicated`]: Self::is_under_replicated
#[derive(Clone)]
pub struct ReplicaSet<'ring> {
    nodes: &'ring [Node],
    picks: Picks,
    target: ReplicationFactor,
}

impl<'ring> ReplicaSet<'ring> {
    /// Wraps picked node indices, or returns `None` if nothing was picked.
    pub(crate) fn new(
        nodes: &'ring [Node],
        picks: Picks,
        target: ReplicationFactor,
    ) -> Option<Self> {
        (!picks.is_empty()).then_some(Self {
            nodes,
            picks,
            target,
        })
    }

    /// Returns the preferred primary.
    #[must_use]
    pub fn primary(&self) -> &'ring Node {
        &self.nodes[self.picks[0]]
    }

    /// Returns the followers in preference order, without the primary.
    #[must_use]
    pub fn followers(&self) -> ReplicaIter<'_> {
        ReplicaIter {
            nodes: self.nodes,
            picks: self.picks[1..].iter(),
        }
    }

    /// Returns every replica in preference order, primary first.
    #[must_use]
    pub fn iter(&self) -> ReplicaIter<'_> {
        ReplicaIter {
            nodes: self.nodes,
            picks: self.picks.iter(),
        }
    }

    /// Returns the replica at `position`, where 0 is the primary.
    #[must_use]
    pub fn get(&self, position: usize) -> Option<&'ring Node> {
        self.picks.get(position).map(|&index| &self.nodes[index])
    }

    /// Returns the number of replicas, which is at least 1.
    #[must_use]
    #[expect(clippy::len_without_is_empty, reason = "a replica set is never empty")]
    pub fn len(&self) -> usize {
        self.picks.len()
    }

    /// Returns the position of the node with ID `id`, where 0 is the primary.
    #[must_use]
    pub fn position(&self, id: impl AsRef<str>) -> Option<usize> {
        let id = id.as_ref();
        self.iter().position(|node| node.id().as_str() == id)
    }

    /// Returns whether the node with ID `id` holds a replica.
    #[must_use]
    pub fn contains(&self, id: impl AsRef<str>) -> bool {
        self.position(id).is_some()
    }

    /// Returns the replication factor the set was selected for.
    #[must_use]
    pub fn replication_factor(&self) -> ReplicationFactor {
        self.target
    }

    /// Returns whether the set has fewer replicas than its replication factor, because the
    /// ring has too few eligible nodes.
    #[must_use]
    pub fn is_under_replicated(&self) -> bool {
        self.picks.len() < usize::from(self.target.get())
    }

    /// Returns the replicas' IDs in preference order.
    #[must_use]
    pub fn to_ids(&self) -> Vec<NodeId> {
        self.iter().map(|node| node.id().clone()).collect()
    }
}

impl PartialEq for ReplicaSet<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().map(Node::id).eq(other.iter().map(Node::id))
    }
}

impl Eq for ReplicaSet<'_> {}

impl fmt::Debug for ReplicaSet<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicaSet")
            .field("nodes", &self.to_ids())
            .field("replication_factor", &self.target)
            .finish()
    }
}

impl fmt::Display for ReplicaSet<'_> {
    /// Formats the replica IDs as `[primary, follower, …]`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (position, node) in self.iter().enumerate() {
            if position > 0 {
                f.write_str(", ")?;
            }
            f.write_str(node.id().as_str())?;
        }
        f.write_str("]")
    }
}

impl<'a> IntoIterator for &'a ReplicaSet<'_> {
    type Item = &'a Node;
    type IntoIter = ReplicaIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An iterator over the nodes of a [`ReplicaSet`], in preference order.
#[derive(Debug, Clone)]
pub struct ReplicaIter<'a> {
    nodes: &'a [Node],
    picks: slice::Iter<'a, usize>,
}

impl<'a> Iterator for ReplicaIter<'a> {
    type Item = &'a Node;

    fn next(&mut self) -> Option<Self::Item> {
        self.picks.next().map(|&index| &self.nodes[index])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.picks.size_hint()
    }
}

impl DoubleEndedIterator for ReplicaIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.picks.next_back().map(|&index| &self.nodes[index])
    }
}

impl ExactSizeIterator for ReplicaIter<'_> {}

impl FusedIterator for ReplicaIter<'_> {}

/// An iterator over a key's full preference list: every eligible node, best first.
///
/// The first R nodes are the key's replica set. Later nodes are where traffic or data goes
/// when earlier ones are unavailable.
#[derive(Debug, Clone)]
pub struct Preference<'ring> {
    nodes: &'ring [Node],
    order: std::vec::IntoIter<usize>,
}

impl<'ring> Preference<'ring> {
    pub(crate) fn new(nodes: &'ring [Node], order: Vec<usize>) -> Self {
        Self {
            nodes,
            order: order.into_iter(),
        }
    }
}

impl<'ring> Iterator for Preference<'ring> {
    type Item = &'ring Node;

    fn next(&mut self) -> Option<Self::Item> {
        self.order.next().map(|index| &self.nodes[index])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.order.size_hint()
    }
}

impl DoubleEndedIterator for Preference<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.order.next_back().map(|index| &self.nodes[index])
    }
}

impl ExactSizeIterator for Preference<'_> {}

impl FusedIterator for Preference<'_> {}

/// How many replicas each key gets, counting the primary.
///
/// The default is 3. A ring with fewer eligible nodes than this still places every key,
/// on as many nodes as it can.
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
///
/// assert_eq!(ReplicationFactor::default().get(), 3);
/// assert_eq!(ReplicationFactor::new(5).map(ReplicationFactor::get), Some(5));
/// assert_eq!(ReplicationFactor::new(0), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicationFactor(NonZeroU8);

impl ReplicationFactor {
    /// A single copy: the primary only.
    pub const ONE: Self = Self(NonZeroU8::MIN);

    const DEFAULT: Self = Self(NonZeroU8::new(3).expect("3 is nonzero"));

    /// Returns a replication factor of `value`, or `None` if `value` is zero.
    #[must_use]
    pub const fn new(value: u8) -> Option<Self> {
        match NonZeroU8::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the replication factor as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0.get()
    }
}

impl Default for ReplicationFactor {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<NonZeroU8> for ReplicationFactor {
    fn from(value: NonZeroU8) -> Self {
        Self(value)
    }
}

impl From<ReplicationFactor> for u8 {
    fn from(factor: ReplicationFactor) -> Self {
        factor.get()
    }
}

impl fmt::Display for ReplicationFactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
