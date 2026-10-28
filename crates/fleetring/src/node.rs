//! Ring members and their weights (ADR-0005, ADR-0006).

use std::fmt;
use std::num::NonZeroU32;

use crate::{Host, NodeId, Zone};

/// The relative capacity of a node.
///
/// Only ratios matter: under the default rendezvous strategy, a node of weight 2 is home to
/// twice as many keys as a node of weight 1. The vnode strategy gives each node
/// `weight × tokens_per_weight` tokens, so keep vnode weights small. A weight is never zero; to
/// drain a node, build a ring without it.
///
/// # Examples
///
/// ```
/// use fleetring::Weight;
///
/// assert_eq!(Weight::new(4).map(Weight::get), Some(4));
/// assert_eq!(Weight::new(0), None);
/// assert_eq!(Weight::default(), Weight::ONE);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Weight(NonZeroU32);

impl Weight {
    /// A weight of 1, the default.
    pub const ONE: Self = Self(NonZeroU32::MIN);

    /// Returns a weight of `value`, or `None` if `value` is zero.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the weight as an integer.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Default for Weight {
    fn default() -> Self {
        Self::ONE
    }
}

impl From<NonZeroU32> for Weight {
    fn from(value: NonZeroU32) -> Self {
        Self(value)
    }
}

impl From<Weight> for NonZeroU32 {
    fn from(weight: Weight) -> Self {
        weight.0
    }
}

impl From<Weight> for u32 {
    fn from(weight: Weight) -> Self {
        weight.get()
    }
}

impl fmt::Display for Weight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A member of a ring: a stable identity, a weight, and topology labels.
///
/// A new node has weight 1, belongs to the default zone, and is its own host.
///
/// # Examples
///
/// ```
/// use fleetring::{Node, Weight};
///
/// let node = Node::new("nas".parse()?)
///     .with_weight(Weight::new(4).expect("4 is nonzero"))
///     .with_zone("garage".parse()?);
/// assert_eq!(node.id().as_str(), "nas");
/// assert_eq!(node.weight().get(), 4);
/// assert_eq!(node.zone().map(|zone| zone.as_str()), Some("garage"));
/// assert_eq!(node.host(), None);
/// # Ok::<(), fleetring::ParseLabelError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    id: NodeId,
    weight: Weight,
    zone: Option<Zone>,
    host: Option<Host>,
}

impl Node {
    /// Creates a node of weight 1 in the default zone, on a host of its own.
    #[must_use]
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            weight: Weight::ONE,
            zone: None,
            host: None,
        }
    }

    /// Sets the node's weight.
    #[must_use]
    pub fn with_weight(mut self, weight: Weight) -> Self {
        self.weight = weight;
        self
    }

    /// Places the node in `zone`.
    #[must_use]
    pub fn with_zone(mut self, zone: Zone) -> Self {
        self.zone = Some(zone);
        self
    }

    /// Places the node on `host`, which it may share with other nodes in the same zone.
    #[must_use]
    pub fn with_host(mut self, host: Host) -> Self {
        self.host = Some(host);
        self
    }

    /// Returns the node's stable identity.
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }

    /// Returns the node's weight.
    #[must_use]
    pub fn weight(&self) -> Weight {
        self.weight
    }

    /// Returns the node's zone, or `None` if it is in the default zone.
    #[must_use]
    pub fn zone(&self) -> Option<&Zone> {
        self.zone.as_ref()
    }

    /// Returns the node's host label, or `None` if the node is its own host.
    #[must_use]
    pub fn host(&self) -> Option<&Host> {
        self.host.as_ref()
    }
}
