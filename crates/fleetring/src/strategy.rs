//! Placement strategies (ADR-0004).

use std::num::NonZeroU32;

use crate::vnodes::TokenTable;

/// The algorithm that ranks a ring's nodes for each key.
///
/// For every key, a strategy produces a total order over the ring's nodes, and replica selection
/// walks that order. Every strategy keeps membership changes minimal: adding a node only inserts
/// it into each key's order, removing a node only deletes it, and reweighting a node only moves
/// that node. The strategy is part of the placement function, so switching strategies moves
/// data.
///
/// Later releases may add strategies, so matches on this enum need a wildcard arm.
///
/// # Examples
///
/// ```
/// use fleetring::{Node, Ring, Strategy};
///
/// let ring = Ring::builder()
///     .strategy(Strategy::Rendezvous)
///     .node(Node::new("nas".parse()?))
///     .build()?;
/// assert_eq!(ring.strategy(), Strategy::default());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Strategy {
    /// Weighted rendezvous hashing, also called highest random weight. This is the default.
    ///
    /// For each key, every node takes a pseudo-random draw from its own hash and the key's hash.
    /// Each draw is scaled by the node's weight, so a node ranks first for exactly its share of
    /// the total weight, and the nodes are ranked by their scaled draws. The scaling uses integer
    /// arithmetic only, so every platform and language computes the same order.
    ///
    /// Rendezvous needs no memory beyond the node list, and a node's draws don't depend on the
    /// other nodes, which is what keeps membership changes minimal. A lookup costs one hash per
    /// node.
    #[default]
    Rendezvous,

    /// A weighted virtual-node ring: consistent hashing as in Dynamo and Cassandra.
    ///
    /// Each node places `weight × tokens_per_weight` tokens on a circle, and a key's order walks
    /// clockwise from the key's hash, taking each node at its first token. A lookup costs a
    /// binary search instead of one hash per node, which suits very large fleets. Balance is
    /// approximate: a node's share is off by about 1/√tokens (±6% at 256 tokens), where
    /// rendezvous is exact.
    ///
    /// Token counts grow with weight and a ring holds at most 2²² tokens, so keep weights
    /// small, in relative units.
    Vnodes(VnodeParams),
}

/// Parameters for [`Strategy::Vnodes`].
///
/// # Examples
///
/// ```
/// use std::num::NonZeroU32;
///
/// use fleetring::{Node, Ring, Strategy, VnodeParams};
///
/// let params = VnodeParams::new(NonZeroU32::new(64).ok_or("zero")?);
/// let ring = Ring::builder()
///     .strategy(Strategy::Vnodes(params))
///     .nodes(["a", "b", "c"].map(|id| Node::new(id.parse().expect("valid id"))))
///     .build()?;
/// assert_eq!(ring.strategy(), Strategy::Vnodes(params));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VnodeParams {
    tokens_per_weight: NonZeroU32,
}

impl VnodeParams {
    /// The default number of tokens per unit of weight. It balances nodes to about ±6%.
    pub const DEFAULT_TOKENS_PER_WEIGHT: NonZeroU32 = NonZeroU32::new(256).expect("256 is nonzero");

    /// Returns parameters that give each node `weight × tokens_per_weight` tokens.
    #[must_use]
    pub const fn new(tokens_per_weight: NonZeroU32) -> Self {
        Self { tokens_per_weight }
    }

    /// Returns the number of tokens per unit of weight.
    #[must_use]
    pub const fn tokens_per_weight(self) -> NonZeroU32 {
        self.tokens_per_weight
    }
}

impl Default for VnodeParams {
    fn default() -> Self {
        Self::new(Self::DEFAULT_TOKENS_PER_WEIGHT)
    }
}

/// Each strategy's lookup structures, built with the ring.
#[derive(Debug, Clone)]
pub(crate) enum StrategyIndex {
    Rendezvous,
    Vnodes(TokenTable),
}
