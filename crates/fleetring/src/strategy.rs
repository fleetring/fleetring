//! Placement strategies (ADR-0004).

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
}
