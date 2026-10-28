//! Rings and the builder that validates them (ADR-0001, ADR-0005, ADR-0006).

use std::collections::BTreeMap;
use std::fmt;

use crate::{Epoch, Host, Node, NodeId, ReplicationFactor, StableHasher, Strategy, Xxh3, Zone};

use self::precomputed::Precomputed;

/// The most nodes a ring can hold.
///
/// Placement refers to nodes, zones, and hosts by `u32` index. The cap also keeps the sum of all
/// weights below 2⁶⁴.
const MAX_NODES: usize = u32::MAX as usize;

/// An immutable set of nodes and the settings that place keys on them.
///
/// Build a ring with [`Ring::builder`]. [`RingBuilder::build`] validates every input, so a ring
/// always has at least one node, no two of its nodes share an ID, and each of its host labels
/// belongs to exactly one zone. A ring never changes once built. To change membership, derive a
/// new ring with [`to_builder`](Self::to_builder) and give it a newer [`Epoch`].
///
/// A ring keeps its nodes in canonical order, sorted by [`NodeId`], so it depends only on its set
/// of nodes, never on the order they were added in.
///
/// `H` is the [`StableHasher`] behind every placement decision. The default, [`Xxh3`], suits
/// almost every fleet. A `Ring<Xxh3>` is `Send + Sync`, so threads can share one ring.
///
/// # Examples
///
/// Build a three-node ring that spans two zones:
///
/// ```
/// use fleetring::{Node, Ring, Weight};
///
/// let ring = Ring::builder()
///     .node(Node::new("laptop-1".parse()?).with_zone("home".parse()?))
///     .node(Node::new("laptop-2".parse()?).with_zone("home".parse()?))
///     .node(
///         Node::new("nas".parse()?)
///             .with_zone("garage".parse()?)
///             .with_weight(Weight::new(4).ok_or("weight is zero")?),
///     )
///     .build()?;
/// assert_eq!(ring.node_count(), 3);
/// assert_eq!(ring.zone_count(), 2);
/// assert_eq!(ring.node("nas").map(|nas| nas.weight().get()), Some(4));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// [`build`](RingBuilder::build) rejects invalid input with a [`BuildRingError`]:
///
/// ```
/// use fleetring::{BuildRingError, Node, Ring};
///
/// let nas = Node::new("nas".parse()?);
/// let result = Ring::builder().node(nas.clone()).node(nas).build();
/// assert_eq!(
///     result.err(),
///     Some(BuildRingError::DuplicateNode { id: "nas".parse()? })
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
pub struct Ring<H = Xxh3> {
    hasher: H,
    seed: u64,
    strategy: Strategy,
    replication_factor: ReplicationFactor,
    epoch: Epoch,
    /// The nodes in canonical order: sorted by ID, with no duplicates.
    nodes: Box<[Node]>,
    precomputed: Precomputed,
}

impl Ring {
    /// Returns a builder for a ring that uses the default [`Xxh3`] hasher.
    ///
    /// This is the same as [`RingBuilder::new`]. Call [`RingBuilder::hasher`] to use another
    /// hasher.
    pub fn builder() -> RingBuilder {
        RingBuilder::new()
    }
}

impl<H> Ring<H> {
    /// Returns the node with ID `id`, or `None` if the ring has no such node.
    ///
    /// Takes O(log n) time for n nodes.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let ring = Ring::builder().node(Node::new("nas".parse()?)).build()?;
    /// assert_eq!(ring.node("nas").map(|nas| nas.id().as_str()), Some("nas"));
    /// assert!(ring.node("laptop-1").is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn node(&self, id: impl AsRef<str>) -> Option<&Node> {
        let id = id.as_ref();
        self.nodes
            .binary_search_by(|node| node.id().as_str().cmp(id))
            .ok()
            .map(|index| &self.nodes[index])
    }

    /// Returns the nodes in canonical order: sorted by ID, comparing IDs byte by byte.
    ///
    /// The order depends only on the set of nodes, never on the order they were added in. Byte
    /// order puts `node-10` before `node-2`. A node's position can change when membership does,
    /// so look nodes up by ID rather than by position.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .node(Node::new("node-2".parse()?))
    ///     .node(Node::new("node-10".parse()?))
    ///     .node(Node::new("node-1".parse()?))
    ///     .build()?;
    /// let ids: Vec<&str> = ring.nodes().iter().map(|node| node.id().as_str()).collect();
    /// assert_eq!(ids, ["node-1", "node-10", "node-2"]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Returns the number of nodes, which is always at least 1.
    ///
    /// Rings have no `len` method because a ring's length could mean more than one thing.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the number of distinct zones, which is always at least 1.
    ///
    /// Nodes without a zone share the default zone, which counts as one zone.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .node(Node::new("laptop-1".parse()?).with_zone("home".parse()?))
    ///     .node(Node::new("laptop-2".parse()?).with_zone("home".parse()?))
    ///     .node(Node::new("vps".parse()?))
    ///     .build()?;
    /// assert_eq!(ring.zone_count(), 2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn zone_count(&self) -> usize {
        self.precomputed.zone_count()
    }

    /// Returns how many replicas each key gets, counting the primary.
    #[must_use]
    pub fn replication_factor(&self) -> ReplicationFactor {
        self.replication_factor
    }

    /// Returns the strategy that ranks the nodes for each key.
    #[must_use]
    pub fn strategy(&self) -> Strategy {
        self.strategy
    }

    /// Returns the seed for every hash the ring computes.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the ring's version, as assigned by the control plane.
    ///
    /// The epoch never affects placement.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Returns the hasher.
    #[must_use]
    pub fn hasher(&self) -> &H {
        &self.hasher
    }

    /// Returns a builder pre-filled with this ring's settings and nodes.
    ///
    /// This is how membership changes: add, remove, or replace nodes on the builder, then build
    /// the next ring. The builder keeps this ring's epoch, so set a newer one for the next ring.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Epoch, Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .node(Node::new("laptop-1".parse()?))
    ///     .node(Node::new("nas".parse()?))
    ///     .epoch(Epoch::new(7))
    ///     .build()?;
    /// let next = ring
    ///     .to_builder()
    ///     .node(Node::new("laptop-2".parse()?))
    ///     .epoch(Epoch::new(8))
    ///     .build()?;
    /// assert_eq!(next.node_count(), 3);
    /// assert!(next.epoch() > ring.epoch());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn to_builder(&self) -> RingBuilder<H>
    where
        H: Clone,
    {
        RingBuilder {
            hasher: self.hasher.clone(),
            seed: self.seed,
            strategy: self.strategy,
            replication_factor: self.replication_factor,
            epoch: self.epoch,
            nodes: self.nodes.to_vec(),
        }
    }
}

impl<H: fmt::Debug> fmt::Debug for Ring<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ring")
            .field("hasher", &self.hasher)
            .field("seed", &self.seed)
            .field("strategy", &self.strategy)
            .field("replication_factor", &self.replication_factor)
            .field("epoch", &self.epoch)
            .field("node_count", &self.node_count())
            .field("zone_count", &self.zone_count())
            .finish_non_exhaustive()
    }
}

/// Collects a ring's settings and nodes, then validates them all at once.
///
/// Start from [`Ring::builder`], [`RingBuilder::new`], or an existing ring's
/// [`to_builder`](Ring::to_builder). The setters never fail: [`build`](Self::build) performs
/// every check.
///
/// # Examples
///
/// ```
/// use fleetring::{Epoch, Node, ReplicationFactor, RingBuilder, Strategy};
///
/// let ring = RingBuilder::new()
///     .seed(7)
///     .strategy(Strategy::Rendezvous)
///     .replication_factor(ReplicationFactor::new(2).ok_or("zero replicas")?)
///     .epoch(Epoch::new(1))
///     .nodes([Node::new("vps".parse()?), Node::new("nas".parse()?)])
///     .node(Node::new("laptop".parse()?))
///     .build()?;
/// assert_eq!(ring.seed(), 7);
/// assert_eq!(ring.replication_factor().get(), 2);
/// let ids: Vec<&str> = ring.nodes().iter().map(|node| node.id().as_str()).collect();
/// assert_eq!(ids, ["laptop", "nas", "vps"]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
#[must_use = "a builder does nothing until `build` is called"]
pub struct RingBuilder<H = Xxh3> {
    hasher: H,
    seed: u64,
    strategy: Strategy,
    replication_factor: ReplicationFactor,
    epoch: Epoch,
    nodes: Vec<Node>,
}

impl RingBuilder {
    /// Creates a builder with no nodes and the default settings.
    ///
    /// The defaults are the [`Xxh3`] hasher, seed 0, [`Strategy::Rendezvous`], a replication
    /// factor of 3, and epoch 0.
    pub fn new() -> Self {
        Self {
            hasher: Xxh3,
            seed: 0,
            strategy: Strategy::default(),
            replication_factor: ReplicationFactor::default(),
            epoch: Epoch::default(),
            nodes: Vec::new(),
        }
    }
}

impl Default for RingBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl<H> RingBuilder<H> {
    /// Replaces the hasher, keeping every other setting and every node.
    ///
    /// The hasher is part of the placement function, so rings with different hashers place keys
    /// differently. [`StableHasher`] describes what an implementation must guarantee.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring, StableHasher, Xxh3};
    ///
    /// // A boxed hasher lets a program choose the algorithm at run time.
    /// let hasher: Box<dyn StableHasher + Send + Sync> = Box::new(Xxh3);
    /// let ring = Ring::builder()
    ///     .hasher(hasher)
    ///     .node(Node::new("nas".parse()?))
    ///     .build()?;
    /// assert_eq!(ring.hasher().algorithm(), "xxh3-64");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn hasher<H2: StableHasher>(self, hasher: H2) -> RingBuilder<H2> {
        RingBuilder {
            hasher,
            seed: self.seed,
            strategy: self.strategy,
            replication_factor: self.replication_factor,
            epoch: self.epoch,
            nodes: self.nodes,
        }
    }

    /// Sets the seed for every hash the ring computes. The default is 0.
    ///
    /// Rings that differ only in seed place keys independently of each other, so changing the
    /// seed moves almost every key.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Sets the placement strategy. The default is [`Strategy::Rendezvous`].
    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Sets how many replicas each key gets, counting the primary. The default is 3.
    ///
    /// A ring with fewer nodes than this still places every key, on as many nodes as it
    /// can.
    pub fn replication_factor(mut self, replication_factor: ReplicationFactor) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// Sets the ring's version. The default is epoch 0.
    ///
    /// The control plane assigns epochs, and peers compare them to tell which ring is newer. The
    /// epoch never affects placement.
    pub fn epoch(mut self, epoch: Epoch) -> Self {
        self.epoch = epoch;
        self
    }

    /// Adds a node.
    ///
    /// Nodes can be added in any order, because [`build`](Self::build) sorts them by ID. Adding
    /// a second node with the same ID makes `build` fail, so to replace a node, call
    /// [`remove_node`](Self::remove_node) first.
    pub fn node(mut self, node: Node) -> Self {
        self.nodes.push(node);
        self
    }

    /// Adds every node in `nodes`.
    pub fn nodes(mut self, nodes: impl IntoIterator<Item = Node>) -> Self {
        self.nodes.extend(nodes);
        self
    }

    /// Removes every node whose ID is `id`, if there is one.
    ///
    /// # Examples
    ///
    /// Drain a node by building the next ring without it:
    ///
    /// ```
    /// use fleetring::{Epoch, Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .node(Node::new("laptop-1".parse()?))
    ///     .node(Node::new("nas".parse()?))
    ///     .build()?;
    /// let next = ring
    ///     .to_builder()
    ///     .remove_node("laptop-1")
    ///     .epoch(Epoch::new(1))
    ///     .build()?;
    /// assert!(next.node("laptop-1").is_none());
    /// assert_eq!(next.node_count(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn remove_node(mut self, id: impl AsRef<str>) -> Self {
        let id = id.as_ref();
        self.nodes.retain(|node| node.id().as_str() != id);
        self
    }
}

impl<H: StableHasher> RingBuilder<H> {
    /// Validates the settings and nodes, then builds the ring.
    ///
    /// Sorts the nodes into canonical order and hashes each node's ID once, so lookups don't
    /// repeat that work. Takes O(n log n) time for n nodes.
    ///
    /// # Errors
    ///
    /// Builds nothing and returns an error if:
    ///
    /// - there are no nodes ([`BuildRingError::NoNodes`]);
    /// - there are more than `u32::MAX` nodes ([`BuildRingError::TooManyNodes`]);
    /// - two nodes share an ID ([`BuildRingError::DuplicateNode`]);
    /// - a host label appears in more than one zone ([`BuildRingError::HostInMultipleZones`]).
    /// - a [`Strategy::Vnodes`] ring would need more than 2²² tokens
    ///   ([`BuildRingError::TooManyTokens`]).
    ///
    /// The checks run in that order, and each walks the nodes in canonical order, so the same
    /// nodes always produce the same error, whatever order they were added in.
    pub fn build(self) -> Result<Ring<H>, BuildRingError> {
        let Self {
            hasher,
            seed,
            strategy,
            replication_factor,
            epoch,
            mut nodes,
        } = self;
        check_node_count(nodes.len())?;
        nodes.sort_unstable_by(|a, b| a.id().cmp(b.id()));
        if let Some(id) = first_duplicate(&nodes) {
            return Err(BuildRingError::DuplicateNode { id: id.clone() });
        }
        check_hosts(&nodes)?;
        let precomputed = Precomputed::new(&nodes, &hasher, seed, strategy)?;
        Ok(Ring {
            hasher,
            seed,
            strategy,
            replication_factor,
            epoch,
            nodes: nodes.into_boxed_slice(),
            precomputed,
        })
    }
}

/// The error returned when [`RingBuilder::build`] rejects its input.
///
/// `build` reports the first problem it finds. It checks in a fixed order and walks the nodes in
/// canonical order, so the same nodes always produce the same error, whatever order they were
/// added in.
///
/// # Examples
///
/// ```
/// use fleetring::{BuildRingError, Node, Ring};
///
/// assert_eq!(Ring::builder().build().err(), Some(BuildRingError::NoNodes));
///
/// // A host is one machine, so all of its nodes must share a zone.
/// let result = Ring::builder()
///     .node(
///         Node::new("disk-1".parse()?)
///             .with_host("nas".parse()?)
///             .with_zone("garage".parse()?),
///     )
///     .node(Node::new("disk-2".parse()?).with_host("nas".parse()?))
///     .build();
/// let error = result.err().ok_or("the ring should be rejected")?;
/// assert_eq!(
///     error.to_string(),
///     "host 'nas' is in zone 'garage' and in the default zone; a host must be in exactly one zone"
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BuildRingError {
    /// The builder has no nodes. A ring needs at least one.
    #[error("ring has no nodes")]
    NoNodes,

    /// Two or more nodes share an ID.
    #[error("node id '{id}' appears more than once")]
    DuplicateNode {
        /// The shared ID. If several IDs are shared, this is the smallest.
        id: NodeId,
    },

    /// A host label appears in more than one zone.
    ///
    /// A host is one machine, so all of its nodes must be in the same zone.
    #[error(
        "host '{host}' is in {} and in {}; a host must be in exactly one zone",
        ZoneName(.first.as_ref()),
        ZoneName(.second.as_ref())
    )]
    HostInMultipleZones {
        /// The host label.
        host: Host,
        /// The zone of the host's first node in canonical order. `None` is the default zone.
        first: Option<Zone>,
        /// The zone of the host's first node, in canonical order, that is not in `first`.
        /// `None` is the default zone.
        second: Option<Zone>,
    },

    /// The builder has more nodes than a ring can hold.
    ///
    /// Placement numbers nodes with `u32` indices, so a ring holds at most `u32::MAX` nodes.
    #[error("ring has {count} nodes; the maximum is {max}")]
    TooManyNodes {
        /// The number of nodes in the builder.
        count: usize,
        /// The most nodes a ring can hold.
        max: usize,
    },

    /// A [`Strategy::Vnodes`] ring would need more tokens than a ring can hold.
    ///
    /// Each node needs `weight × tokens_per_weight` tokens, and a ring holds at most 2²². Lower
    /// the weights or `tokens_per_weight`.
    #[error("ring needs {total} tokens; the maximum is {max}")]
    TooManyTokens {
        /// The tokens the nodes would need, saturating at `u64::MAX`.
        total: u64,
        /// The most tokens a ring can hold.
        max: u64,
    },
}

/// Displays a zone label, or the default zone for `None`.
struct ZoneName<'a>(Option<&'a Zone>);

impl fmt::Display for ZoneName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(zone) => write!(f, "zone '{zone}'"),
            None => f.write_str("the default zone"),
        }
    }
}

/// Checks that a ring of `count` nodes is neither empty nor too large for `u32` indices.
fn check_node_count(count: usize) -> Result<(), BuildRingError> {
    if count == 0 {
        return Err(BuildRingError::NoNodes);
    }
    if u32::try_from(count).is_err() {
        return Err(BuildRingError::TooManyNodes {
            count,
            max: MAX_NODES,
        });
    }
    Ok(())
}

/// Returns the smallest ID that appears more than once in `nodes`, which is sorted by ID.
fn first_duplicate(nodes: &[Node]) -> Option<&NodeId> {
    nodes
        .windows(2)
        .find(|pair| pair[0].id() == pair[1].id())
        .map(|pair| pair[0].id())
}

/// Checks that each host label in `nodes` is in exactly one zone.
///
/// Reports the first node, in the order of `nodes`, whose zone differs from the zone of the
/// first node with the same host.
fn check_hosts(nodes: &[Node]) -> Result<(), BuildRingError> {
    let mut zone_of_host: BTreeMap<&Host, Option<&Zone>> = BTreeMap::new();
    for node in nodes {
        let Some(host) = node.host() else {
            continue;
        };
        let first = *zone_of_host.entry(host).or_insert(node.zone());
        if first != node.zone() {
            return Err(BuildRingError::HostInMultipleZones {
                host: host.clone(),
                first: first.cloned(),
                second: node.zone().cloned(),
            });
        }
    }
    Ok(())
}

/// Placement data derived from a ring's nodes at build time, and the crate-internal accessors
/// that placement reads it through.
mod precomputed {
    use super::{BuildRingError, MAX_NODES, Ring};
    use crate::select::{Member, Members};
    use crate::strategy::StrategyIndex;
    use crate::vnodes::{MAX_TOKENS, TokenTable, token_count};
    use crate::{Node, StableHasher, Strategy};

    /// Placement data derived from a ring's validated, canonically ordered nodes.
    #[derive(Debug, Clone)]
    pub(super) struct Precomputed {
        /// The strategy's lookup structures, such as a vnode ring's token table.
        index: StrategyIndex,
        /// One entry per node, in canonical order.
        entries: Box<[Member]>,
        zone_count: usize,
        /// Distinct host labels, plus one host for each unlabeled node.
        host_count: usize,
        /// The sum of all weights. [`MAX_NODES`] keeps it from overflowing.
        total_weight: u64,
        /// Whether every node has the same weight, which lets rendezvous rank by raw draws alone.
        uniform_weights: bool,
    }

    impl Precomputed {
        /// Derives the placement data for `nodes`, which `build` has validated and sorted.
        pub(super) fn new(
            nodes: &[Node],
            hasher: &impl StableHasher,
            seed: u64,
            strategy: Strategy,
        ) -> Result<Self, BuildRingError> {
            let zones = distinct(nodes.iter().map(Node::zone));
            let hosts = distinct(nodes.iter().filter_map(Node::host));
            let mut entries = Vec::with_capacity(nodes.len());
            let mut unlabeled_hosts = 0;
            for node in nodes {
                let host = match node.host() {
                    Some(host) => position(&hosts, &host),
                    // An unlabeled node is a host of its own, numbered after the labeled hosts.
                    None => {
                        let host = hosts.len() + unlabeled_hosts;
                        unlabeled_hosts += 1;
                        host
                    }
                };
                entries.push(Member {
                    node_hash: hasher.hash(node.id().as_str().as_bytes(), seed),
                    weight: node.weight().get(),
                    zone: to_index(position(&zones, &node.zone())),
                    host: to_index(host),
                });
            }
            let mut precomputed = Self {
                index: StrategyIndex::Rendezvous,
                entries: entries.into_boxed_slice(),
                zone_count: zones.len(),
                host_count: hosts.len() + unlabeled_hosts,
                total_weight: nodes
                    .iter()
                    .map(|node| u64::from(node.weight().get()))
                    .sum(),
                uniform_weights: nodes
                    .windows(2)
                    .all(|pair| pair[0].weight() == pair[1].weight()),
            };
            precomputed.index = match strategy {
                Strategy::Rendezvous => StrategyIndex::Rendezvous,
                Strategy::Vnodes(params) => {
                    let members = precomputed.members();
                    let per_weight = params.tokens_per_weight().get();
                    let total = token_count(&members, per_weight);
                    if total > MAX_TOKENS {
                        return Err(BuildRingError::TooManyTokens {
                            total,
                            max: MAX_TOKENS,
                        });
                    }
                    StrategyIndex::Vnodes(TokenTable::build(&members, hasher, seed, per_weight))
                }
            };
            Ok(precomputed)
        }

        fn members(&self) -> Members<'_> {
            Members {
                entries: &self.entries,
                zone_count: self.zone_count,
                host_count: self.host_count,
                uniform_weights: self.uniform_weights,
            }
        }

        pub(super) fn zone_count(&self) -> usize {
            self.zone_count
        }
    }

    // Placement inputs by canonical node index: an `index` is a position in `Ring::nodes`, and
    // every method that takes one panics if it is out of bounds.
    impl<H> Ring<H> {
        /// Hashes `key` with the ring's hasher and seed.
        pub(crate) fn key_hash(&self, key: &[u8]) -> u64
        where
            H: StableHasher,
        {
            self.hasher.hash(key, self.seed)
        }

        /// Returns the per-node placement inputs that strategies and selection read.
        pub(crate) fn members(&self) -> Members<'_> {
            self.precomputed.members()
        }

        /// Returns the strategy's lookup structures.
        pub(crate) fn strategy_index(&self) -> &StrategyIndex {
            &self.precomputed.index
        }

        /// Returns the hash of node `index`'s ID under the ring's seed.
        #[cfg(test)]
        pub(crate) fn node_hash(&self, index: usize) -> u64 {
            self.precomputed.entries[index].node_hash
        }

        /// Returns node `index`'s zone, as an index below [`zone_count`](Self::zone_count).
        ///
        /// Zones are numbered in sorted order, starting with the default zone if any node is in
        /// it.
        #[cfg(test)]
        pub(crate) fn zone_of(&self, index: usize) -> u32 {
            self.precomputed.entries[index].zone
        }

        /// Returns node `index`'s host, as an index below [`host_count`](Self::host_count).
        ///
        /// Two nodes share a host index exactly when they share a host label.
        #[cfg(test)]
        pub(crate) fn host_of(&self, index: usize) -> u32 {
            self.precomputed.entries[index].host
        }

        /// Returns node `index`'s weight.
        #[cfg(test)]
        pub(crate) fn weight_of(&self, index: usize) -> u32 {
            self.precomputed.entries[index].weight
        }

        /// Returns whether every node has the same weight.
        #[cfg(test)]
        pub(crate) fn uniform_weights(&self) -> bool {
            self.precomputed.uniform_weights
        }

        /// Returns the sum of all weights.
        pub(crate) fn total_weight(&self) -> u64 {
            self.precomputed.total_weight
        }

        /// Returns the number of distinct hosts, counting each unlabeled node as a host of its
        /// own.
        #[cfg(test)]
        pub(crate) fn host_count(&self) -> usize {
            self.precomputed.host_count
        }
    }

    /// Returns the distinct `values` in sorted order.
    fn distinct<T: Ord>(values: impl Iterator<Item = T>) -> Vec<T> {
        let mut values: Vec<T> = values.collect();
        values.sort_unstable();
        values.dedup();
        values
    }

    /// Returns the position of `value` in `sorted`, which contains it.
    fn position<T: Ord>(sorted: &[T], value: &T) -> usize {
        // `value` is always present, so the search returns `Ok`. `Err` would be an insertion
        // point.
        match sorted.binary_search(value) {
            Ok(position) | Err(position) => position,
        }
    }

    /// Converts a position among a ring's nodes, zones, or hosts to a `u32` index.
    fn to_index(position: usize) -> u32 {
        // `build` caps rings at `MAX_NODES` nodes, and a ring never has more zones or hosts than
        // nodes.
        u32::try_from(position).unwrap_or_else(|_| {
            unreachable!("position {position} exceeds MAX_NODES ({MAX_NODES}) in a validated ring")
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::{Just, ProptestConfig, any, prop_assert_eq, proptest};
    use proptest::strategy::Strategy as _;

    use super::*;
    use crate::Weight;
    use crate::test_util::TableHasher;

    fn node(id: &str) -> Node {
        Node::new(id.parse().expect("test ids are valid"))
    }

    fn zoned(id: &str, zone: &str) -> Node {
        node(id).with_zone(zone.parse().expect("test zones are valid"))
    }

    fn hosted(id: &str, host: &str) -> Node {
        node(id).with_host(host.parse().expect("test hosts are valid"))
    }

    fn weighted(id: &str, weight: u32) -> Node {
        node(id).with_weight(Weight::new(weight).expect("test weights are nonzero"))
    }

    fn build(nodes: impl IntoIterator<Item = Node>) -> Ring {
        Ring::builder()
            .nodes(nodes)
            .build()
            .expect("test rings are valid")
    }

    /// A node's precomputed placement inputs: its hash, weight, zone, and host.
    type Inputs = (u64, u32, u32, u32);

    /// Everything a ring precomputes, read through the crate-internal accessors.
    fn internals<H>(ring: &Ring<H>) -> (&[Node], Vec<Inputs>, usize, usize, u64, bool) {
        let inputs = (0..ring.node_count())
            .map(|index| {
                (
                    ring.node_hash(index),
                    ring.weight_of(index),
                    ring.zone_of(index),
                    ring.host_of(index),
                )
            })
            .collect();
        (
            ring.nodes(),
            inputs,
            ring.zone_count(),
            ring.host_count(),
            ring.total_weight(),
            ring.uniform_weights(),
        )
    }

    fn zones_of<H>(ring: &Ring<H>) -> Vec<u32> {
        (0..ring.node_count())
            .map(|index| ring.zone_of(index))
            .collect()
    }

    fn hosts_of<H>(ring: &Ring<H>) -> Vec<u32> {
        (0..ring.node_count())
            .map(|index| ring.host_of(index))
            .collect()
    }

    #[test]
    fn node_hashes_are_the_ids_hashed_under_the_seed() {
        for seed in [0, 1, 0x9e37_79b9_7f4a_7c15] {
            let ring = Ring::builder()
                .seed(seed)
                .nodes(["nas", "laptop-1", "vps"].map(node))
                .build()
                .expect("valid ring");
            for (index, node) in ring.nodes().iter().enumerate() {
                let want = Xxh3.hash(node.id().as_str().as_bytes(), seed);
                assert_eq!(
                    ring.node_hash(index),
                    want,
                    "{} under seed {seed}",
                    node.id()
                );
            }
        }
    }

    #[test]
    fn rings_hash_with_their_own_hasher_and_seed() {
        let hasher = TableHasher::new().with("b", 7).with("agent:1", 99);
        let ring = Ring::builder()
            .hasher(hasher)
            .seed(5)
            .nodes(["b", "a"].map(node))
            .build()
            .expect("valid ring");
        assert_eq!(ring.node_hash(0), Xxh3.hash(b"a", 5));
        assert_eq!(ring.node_hash(1), 7);
        assert_eq!(ring.key_hash(b"agent:1"), 99);
        assert_eq!(ring.key_hash(b"agent:2"), Xxh3.hash(b"agent:2", 5));
    }

    #[test]
    fn zones_are_numbered_in_sorted_order_after_the_default_zone() {
        let ring = build([
            zoned("a", "z2"),
            node("b"),
            zoned("c", "z1"),
            zoned("d", "z2"),
        ]);
        assert_eq!(zones_of(&ring), [2, 0, 1, 2]);
        assert_eq!(ring.zone_count(), 3);

        let ring = build([zoned("a", "z2"), zoned("b", "z1")]);
        assert_eq!(zones_of(&ring), [1, 0], "no default zone, so z1 is zone 0");
        assert_eq!(ring.zone_count(), 2);

        let ring = build(["a", "b"].map(node));
        assert_eq!(zones_of(&ring), [0, 0]);
        assert_eq!(ring.zone_count(), 1);
    }

    #[test]
    fn nodes_share_a_host_index_exactly_when_they_share_a_label() {
        let ring = build([
            hosted("a", "h2"),
            node("b"),
            hosted("c", "h1"),
            hosted("d", "h2"),
            node("e"),
        ]);
        // Labels first, in sorted order; then one host per unlabeled node, in canonical order.
        assert_eq!(hosts_of(&ring), [1, 2, 0, 1, 3]);
        assert_eq!(ring.host_count(), 4);
    }

    #[test]
    fn every_unlabeled_node_is_a_host_of_its_own() {
        let ring = build(["c", "a", "b"].map(node));
        assert_eq!(hosts_of(&ring), [0, 1, 2]);
        assert_eq!(ring.host_count(), 3);
    }

    #[test]
    fn uniform_weights_requires_every_weight_to_match() {
        assert!(build([weighted("a", 9)]).uniform_weights());
        assert!(build([weighted("a", 5), weighted("b", 5)]).uniform_weights());
        assert!(!build([weighted("a", 5), weighted("b", 5), weighted("c", 6)]).uniform_weights());
        assert!(!build([weighted("a", 1), weighted("b", u32::MAX)]).uniform_weights());
    }

    #[test]
    fn weights_follow_canonical_order_and_sum_without_overflow() {
        let ring = build([
            weighted("c", 3),
            weighted("a", u32::MAX),
            weighted("b", u32::MAX),
        ]);
        let weights: Vec<u32> = (0..3).map(|index| ring.weight_of(index)).collect();
        assert_eq!(weights, [u32::MAX, u32::MAX, 3]);
        assert_eq!(ring.total_weight(), 2 * u64::from(u32::MAX) + 3);
        assert_eq!(build(["a", "b"].map(node)).total_weight(), 2);
    }

    #[test]
    fn node_counts_must_be_positive() {
        assert_eq!(check_node_count(0), Err(BuildRingError::NoNodes));
        assert_eq!(check_node_count(1), Ok(()));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn node_counts_must_fit_u32_indices() {
        assert_eq!(check_node_count(MAX_NODES), Ok(()));
        let count = MAX_NODES + 1;
        assert_eq!(
            check_node_count(count),
            Err(BuildRingError::TooManyNodes {
                count,
                max: MAX_NODES
            })
        );
    }

    /// Nodes that always build: distinct IDs, with host `hN` always in zone number `N % 3`.
    fn valid_nodes() -> impl proptest::strategy::Strategy<Value = Vec<Node>> {
        let member = (
            1..=3u32,
            0..3u8,
            proptest::option::of(0..4u8),
            any::<bool>(),
        );
        proptest::collection::btree_map("[a-z0-9]{1,4}", member, 1..16).prop_map(|members| {
            members
                .into_iter()
                .map(|(id, (weight, zone, host, heavy))| {
                    let zone = host.map_or(zone, |host| host % 3);
                    let weight = if heavy { u32::MAX } else { weight };
                    let mut node = weighted(&id, weight);
                    if zone > 0 {
                        node = node.with_zone(format!("z{zone}").parse().expect("valid zone"));
                    }
                    if let Some(host) = host {
                        node = node.with_host(format!("h{host}").parse().expect("valid host"));
                    }
                    node
                })
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn internals_do_not_depend_on_insertion_order(
            (nodes, shuffled) in valid_nodes()
                .prop_flat_map(|nodes| (Just(nodes.clone()), Just(nodes).prop_shuffle())),
            seed in any::<u64>(),
        ) {
            let ring = Ring::builder().seed(seed).nodes(nodes).build().expect("valid nodes");
            let other = Ring::builder().seed(seed).nodes(shuffled).build().expect("valid nodes");
            prop_assert_eq!(internals(&ring), internals(&other));
        }
    }
}
