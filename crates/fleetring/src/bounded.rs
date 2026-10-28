//! Bounded loads: placement that keeps every node near its fair share of the load (ADR-0003,
//! ADR-0006, ADR-0007).
//!
//! Home placement depends only on the ring and the key, so it balances key counts but not
//! the size or traffic behind each key. [`BoundedLoads`] corrects for that when a key is created or
//! re-placed. Given every node's current load, in a unit the caller chooses, it skips the nodes
//! that would end up above their weighted share of the total plus some [`Headroom`]. This is
//! consistent hashing with bounded loads (Mirrokni, Thorup, and Zadimoghaddam, SODA 2018) over
//! each key's preference list, so replicas still spread across zones and hosts.
//!
//! The result depends on the loads, so clients can't recompute it from the ring. The daemon
//! records every placement, and clients follow a redirect for each key placed away from
//! home (ADR-0001).
//!
//! # Examples
//!
//! For example, a FleetRing daemon places a new database by the bytes each node stores:
//!
//! ```
//! use fleetring::{BoundedLoads, Headroom, Node, Ring};
//!
//! let ring = Ring::builder()
//!     .node(Node::new("laptop-1".parse()?).with_zone("home".parse()?))
//!     .node(Node::new("laptop-2".parse()?).with_zone("home".parse()?))
//!     .node(Node::new("nas".parse()?).with_zone("home".parse()?))
//!     .node(Node::new("vps-1".parse()?).with_zone("cloud".parse()?))
//!     .node(Node::new("vps-2".parse()?).with_zone("cloud".parse()?))
//!     .build()?;
//! let policy = BoundedLoads::new(Headroom::from_percent(25));
//!
//! // Bytes stored per node, counting every replica, as the control db reports them. A few huge
//! // tenants live on the NAS.
//! const GIB: u64 = 1 << 30;
//! let mut stored = [
//!     ("laptop-1", 20 * GIB),
//!     ("laptop-2", 24 * GIB),
//!     ("nas", 400 * GIB),
//!     ("vps-1", 18 * GIB),
//!     ("vps-2", 22 * GIB),
//! ];
//!
//! // A new agent database, expected to grow to 4 GiB.
//! let key = "agent:sam-researcher";
//! let size = 4 * GIB;
//! let view = policy.load_view(&ring, stored)?;
//! let placed = view.replicas(key, size);
//! assert!(!placed.is_saturated());
//! assert!(!placed.replicas().contains("nas"), "the NAS is far above its share");
//!
//! // Clients compute home placement, so the daemon records an override whenever the policy
//! // placed the database elsewhere: here, exactly when its home replica set includes the NAS.
//! let needs_override = !placed.is_home();
//! assert_eq!(needs_override, ring.replicas(key).contains("nas"));
//!
//! // The next placement must see this one, so count its replicas in a fresh snapshot.
//! for (id, bytes) in &mut stored {
//!     if placed.replicas().contains(*id) {
//!         *bytes += size;
//!     }
//! }
//! let next = policy.load_view(&ring, stored)?;
//! assert_eq!(next.total_load(), view.total_load() + 3 * u128::from(size));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![warn(clippy::arithmetic_side_effects)]

use std::fmt;
use std::mem;

use crate::select::Picks;
use crate::{Node, ReplicaSet, Ring, StableHasher, Xxh3};

/// Headroom is a percentage, so the capacity formula works in hundredths.
const PERCENT: u128 = 100;

/// How far above its fair share of the load a node may go, as an integer percentage ε.
///
/// A node's fair share is the total load times the node's share of the ring's weight. With
/// headroom ε, a node has room for a new key only if its load afterwards stays within
/// `⌈(1 + ε/100) × fair share⌉`, so `Headroom::from_percent(25)` caps every node at 1.25 times
/// its fair share. Less headroom balances more tightly but places more keys away from home.
///
/// The default is 25%. Any value is valid, including zero, which caps each node at its fair
/// share rounded up. Headroom is a `u16` so that the capacity arithmetic can't overflow.
///
/// # Examples
///
/// ```
/// use fleetring::Headroom;
///
/// let headroom = Headroom::from_percent(10);
/// assert_eq!(headroom.percent(), 10);
/// assert_eq!(Headroom::default(), Headroom::from_percent(25));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Headroom(u16);

impl Headroom {
    const DEFAULT: Self = Self::from_percent(25);

    /// Returns a headroom of `value` percent.
    #[must_use]
    pub const fn from_percent(value: u16) -> Self {
        Self(value)
    }

    /// Returns the headroom as a percentage.
    #[must_use]
    pub const fn percent(self) -> u16 {
        self.0
    }
}

impl Default for Headroom {
    /// Returns a headroom of 25%.
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Consistent hashing with bounded loads: a placement policy that keeps each node near its
/// weighted share of the load (ADR-0007).
///
/// Home placement ([`Ring::replicas`]) balances key counts, but not the size or
/// traffic behind each key. This policy places a new key on its replica set among the nodes with
/// room for it, where a node has room if its load afterwards stays within its fair share plus
/// the [`Headroom`]. Loads are in any unit that fits a `u64`, such as bytes, queries per second,
/// or key counts, and count every replica a node holds.
///
/// Apply the policy when a key is created or re-placed, never to route requests: the
/// result depends on loads, so clients can't recompute it. Record each placement, and an
/// override whenever it isn't [home placement](BoundedPlacement::is_home). Place keys one
/// at a time with a fresh [`LoadView`] each, so every placement sees the loads the previous ones
/// left.
///
/// The default policy has 25% headroom.
///
/// # Examples
///
/// ```
/// use fleetring::{BoundedLoads, Headroom, Node, Ring};
///
/// let ring = Ring::builder()
///     .nodes(["a", "b", "c", "d"].map(|id| Node::new(id.parse().expect("valid id"))))
///     .build()?;
/// let policy = BoundedLoads::new(Headroom::from_percent(25));
/// // Key counts: `d` already holds far more than its quarter.
/// let view = policy.load_view(&ring, [("a", 10), ("b", 12), ("c", 11), ("d", 40)])?;
/// let placed = view.replicas("tenant:acme", 1);
/// assert!(!placed.replicas().contains("d"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct BoundedLoads {
    headroom: Headroom,
}

impl BoundedLoads {
    /// Creates the policy, letting each node go `headroom` above its fair share.
    #[must_use]
    pub const fn new(headroom: Headroom) -> Self {
        Self { headroom }
    }

    /// Returns how far above its fair share each node may go.
    #[must_use]
    pub const fn headroom(&self) -> Headroom {
        self.headroom
    }

    /// Takes a snapshot of every node's load, bound to `ring`.
    ///
    /// `loads` pairs node IDs with their current loads, in any order, and nodes it leaves out
    /// have load 0. The view borrows the ring, so it can't place keys on a different ring.
    /// Takes O(m log n) time for m loads on n nodes.
    ///
    /// # Errors
    ///
    /// Builds nothing and returns an error at the first ID in `loads` that:
    ///
    /// - names no node of `ring` ([`LoadViewError::UnknownNode`]);
    /// - appeared earlier in `loads` ([`LoadViewError::DuplicateNode`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{BoundedLoads, Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .nodes(["laptop", "nas", "vps"].map(|id| Node::new(id.parse().expect("valid id"))))
    ///     .build()?;
    /// let view = BoundedLoads::default().load_view(&ring, [("nas", 300), ("laptop", 20)])?;
    /// assert_eq!(view.load("nas"), Some(300));
    /// assert_eq!(view.load("vps"), Some(0));
    /// assert_eq!(view.load("phone"), None);
    /// assert_eq!(view.total_load(), 320);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn load_view<'ring, H: StableHasher, K: AsRef<str>>(
        &self,
        ring: &'ring Ring<H>,
        loads: impl IntoIterator<Item = (K, u64)>,
    ) -> Result<LoadView<'ring, H>, LoadViewError> {
        let nodes = ring.nodes();
        let mut per_node = vec![0; nodes.len()];
        let mut listed = vec![false; nodes.len()];
        for (id, load) in loads {
            let id = id.as_ref();
            let Some(index) = position(nodes, id) else {
                return Err(LoadViewError::UnknownNode { id: id.to_owned() });
            };
            if mem::replace(&mut listed[index], true) {
                return Err(LoadViewError::DuplicateNode { id: id.to_owned() });
            }
            per_node[index] = load;
        }
        // At most u32::MAX loads below 2⁶⁴ each, so the sum stays below 2⁹⁶.
        let total = per_node.iter().copied().map(u128::from).sum();
        Ok(LoadView {
            ring,
            headroom: self.headroom,
            loads: per_node,
            total,
        })
    }
}

/// A snapshot of every node's load, bound to one ring, for placing new keys.
///
/// Build one with [`BoundedLoads::load_view`]. The view borrows its ring, so it only places
/// keys on the ring its loads describe. It never changes: placing a key doesn't count
/// the new replicas, so after recording a placement, take a fresh view for the next one.
pub struct LoadView<'ring, H = Xxh3> {
    ring: &'ring Ring<H>,
    headroom: Headroom,
    /// Each node's load, in canonical order.
    loads: Vec<u64>,
    /// The sum of `loads`.
    total: u128,
}

impl<'ring, H> LoadView<'ring, H> {
    /// Returns the ring the loads describe.
    #[must_use]
    pub fn ring(&self) -> &'ring Ring<H> {
        self.ring
    }

    /// Returns node `id`'s load, or `None` if the ring has no such node.
    ///
    /// Nodes that the loads left out have load 0.
    #[must_use]
    pub fn load(&self, id: impl AsRef<str>) -> Option<u64> {
        position(self.ring.nodes(), id.as_ref()).map(|index| self.loads[index])
    }

    /// Returns the sum of every node's load, which can exceed `u64::MAX`.
    #[must_use]
    pub fn total_load(&self) -> u128 {
        self.total
    }

    /// Returns the most load node `id` may hold after placing a key with load `item_load`, or
    /// `None` if the ring has no such node.
    ///
    /// The capacity is `⌈(100 + ε) · (total + r · item_load) · w / (100 · W)⌉`: the node's fair
    /// share of the total once all `r` replicas are placed, plus the headroom ε percent. Here
    /// `total` is [`total_load`](Self::total_load), `r` is the ring's replication factor, `w` is
    /// the node's weight, and `W` is the ring's total weight. The node has room for the key
    /// if its load plus `item_load` is at most this. The arithmetic is exact and can't overflow
    /// for any loads, weights, or headroom.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{BoundedLoads, Headroom, Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .nodes(["a", "b", "c", "d"].map(|id| Node::new(id.parse().expect("valid id"))))
    ///     .build()?;
    /// let view = BoundedLoads::new(Headroom::from_percent(25)).load_view(&ring, [("a", 100)])?;
    /// // Three replicas of 10 raise the total to 130, so each node's fair share is 32.5 and its
    /// // capacity is ⌈1.25 × 32.5⌉ = 41.
    /// assert_eq!(view.capacity("b", 10), Some(41));
    /// assert_eq!(view.capacity("e", 10), None);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn capacity(&self, id: impl AsRef<str>, item_load: u64) -> Option<u128> {
        let node = self.ring.node(id)?;
        Some(self.capacities(item_load).of(node.weight().get()))
    }

    /// Returns every node's capacity for placing a key with load `item_load`.
    fn capacities(&self, item_load: u64) -> Capacities {
        Capacities::new(
            self.total,
            item_load,
            self.replicas_placed(),
            self.headroom,
            self.ring.total_weight(),
        )
    }

    /// Returns how many replicas each placement creates: the replication factor, or the node
    /// count when the ring is smaller (ADR-0007).
    fn replicas_placed(&self) -> u8 {
        let factor = self.ring.replication_factor().get();
        u8::try_from(self.ring.node_count()).map_or(factor, |nodes| factor.min(nodes))
    }

    /// Returns whether node `index` can take a key with load `item_load` and stay within its
    /// capacity.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "a load and an item below 2⁶⁴ each sum below 2⁶⁵"
    )]
    fn has_room(&self, index: usize, item_load: u64, capacities: &Capacities) -> bool {
        let weight = self.ring.nodes()[index].weight().get();
        u128::from(self.loads[index]) + u128::from(item_load) <= capacities.of(weight)
    }
}

impl<'ring, H: StableHasher> LoadView<'ring, H> {
    /// Places `key`, which carries `item_load`: its replica set among the nodes with room
    /// for it.
    ///
    /// A node has room when its load plus `item_load` is at most its
    /// [`capacity`](Self::capacity). The replicas are selected exactly as
    /// [`Ring::replicas_where`] selects them among the nodes with room, so zones and hosts spread
    /// as they do in failover. If too few nodes have room, the remaining slots go to the best
    /// other nodes of the key's unfiltered [preference list](Ring::preference), and the
    /// placement [is saturated](BoundedPlacement::is_saturated). Either way, the placement has
    /// as many replicas as the key's home replica set.
    ///
    /// The view doesn't change. After recording the placement, add `item_load` to the load of
    /// each replica's node, then take a fresh view for the next placement. The result depends
    /// only on the ring, the key, the loads, the headroom, and `item_load`, and is the same on
    /// every platform. It costs about two [`Ring::replicas_where`] calls: one for the nodes with
    /// room and one for home placement.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{BoundedLoads, Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .nodes(["a", "b", "c", "d"].map(|id| Node::new(id.parse().expect("valid id"))))
    ///     .build()?;
    /// // Key counts: `a` holds far more than its quarter.
    /// let loads = [("a", 90), ("b", 30), ("c", 30), ("d", 30)];
    /// let view = BoundedLoads::default().load_view(&ring, loads)?;
    /// let placed = view.replicas("tenant:acme", 1);
    /// assert_eq!(placed.replicas().len(), 3);
    /// assert!(!placed.replicas().contains("a"));
    /// assert!(!placed.is_saturated());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn replicas(&self, key: impl AsRef<[u8]>, item_load: u64) -> BoundedPlacement<'ring> {
        let ring = self.ring;
        let factor = ring.replication_factor();
        let key_hash = ring.key_hash(key.as_ref());
        let capacities = self.capacities(item_load);
        let home = ring
            .replicas_hashed_where(key_hash, factor, |_| true)
            .expect("a ring always has at least one node");
        let with_room = ring.replicas_hashed_where(key_hash, factor, |index| {
            self.has_room(index, item_load, &capacities)
        });
        let (replicas, saturated) = match with_room {
            Some(replicas) if replicas.len() == home.len() => (replicas, false),
            partial => (saturate(ring.nodes(), partial, &home), true),
        };
        let is_home = replicas == home;
        BoundedPlacement {
            replicas,
            saturated,
            home: is_home,
        }
    }
}

impl<H> Clone for LoadView<'_, H> {
    fn clone(&self) -> Self {
        Self {
            ring: self.ring,
            headroom: self.headroom,
            loads: self.loads.clone(),
            total: self.total,
        }
    }
}

impl<H: fmt::Debug> fmt::Debug for LoadView<'_, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadView")
            .field("ring", self.ring)
            .field("headroom", &self.headroom)
            .field("total_load", &self.total)
            .finish_non_exhaustive()
    }
}

/// Where [`LoadView::replicas`] places a new key, and how that compares with home
/// placement.
///
/// Equality compares the replicas' node IDs in order, then both flags.
///
/// # Examples
///
/// A key too heavy for any node still gets placed:
///
/// ```
/// use fleetring::{BoundedLoads, Node, Ring};
///
/// let ring = Ring::builder()
///     .nodes(["a", "b", "c", "d"].map(|id| Node::new(id.parse().expect("valid id"))))
///     .build()?;
/// let loads = [("a", 10), ("b", 10), ("c", 10), ("d", 10)];
/// let view = BoundedLoads::default().load_view(&ring, loads)?;
///
/// // No node has room for something 25 times the size of everything stored so far, so the
/// // key goes home and the placement reports saturation.
/// let placed = view.replicas("tenant:huge", 1_000);
/// assert!(placed.is_saturated());
/// assert!(placed.is_home());
/// assert_eq!(placed.replicas(), &ring.replicas("tenant:huge"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedPlacement<'ring> {
    replicas: ReplicaSet<'ring>,
    saturated: bool,
    home: bool,
}

impl<'ring> BoundedPlacement<'ring> {
    /// Returns the replicas, preferred primary first.
    #[must_use]
    pub fn replicas(&self) -> &ReplicaSet<'ring> {
        &self.replicas
    }

    /// Returns the replicas, consuming the placement.
    #[must_use]
    pub fn into_replicas(self) -> ReplicaSet<'ring> {
        self.replicas
    }

    /// Returns whether some replica went to a node without room for the key.
    ///
    /// This happens when fewer nodes have room than the replica set needs, usually because the
    /// key is heavy compared with the total load. The policy never fails: the remaining
    /// slots go to the best other nodes of the key's preference list, which are then over their
    /// capacity.
    #[must_use]
    pub fn is_saturated(&self) -> bool {
        self.saturated
    }

    /// Returns whether the replicas equal the key's home replica set: the same nodes, in the
    /// same order.
    ///
    /// Clients compute home placement themselves, so when this returns false, the daemon must
    /// record an override for the key (ADR-0001). A saturated placement can still be home.
    #[must_use]
    pub fn is_home(&self) -> bool {
        self.home
    }
}

/// The error returned when [`BoundedLoads::load_view`] rejects its loads.
///
/// `load_view` reports the first bad ID in the order the loads list them.
///
/// # Examples
///
/// ```
/// use fleetring::{BoundedLoads, LoadViewError, Node, Ring};
///
/// let ring = Ring::builder().node(Node::new("nas".parse()?)).build()?;
/// let error = BoundedLoads::default()
///     .load_view(&ring, [("nas", 7), ("vps", 3)])
///     .err()
///     .ok_or("the loads should be rejected")?;
/// assert_eq!(error, LoadViewError::UnknownNode { id: "vps".to_owned() });
/// assert_eq!(error.to_string(), "node id 'vps' is not in the ring");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LoadViewError {
    /// An ID in the loads names no node of the ring.
    #[error("node id '{}' is not in the ring", .id.escape_debug())]
    UnknownNode {
        /// The ID as given, which need not be a valid [`NodeId`](crate::NodeId). The error
        /// message escapes it.
        id: String,
    },

    /// A node appears more than once in the loads.
    #[error("node id '{id}' appears more than once in the loads")]
    DuplicateNode {
        /// The node's ID.
        id: String,
    },
}

/// Every node's capacity for one placement, sharing the division that doesn't depend on the
/// node.
///
/// Node *i*'s capacity is `⌈(100 + ε) · q · wᵢ / (100 · W)⌉`, where `q = total + r · item` is
/// the total load once the placement's `r` replicas land. The numerator can reach about 2¹⁴⁶,
/// too much for `u128`, so it is never formed. With `f = (100 + ε) · wᵢ` and `d = 100 · W`,
/// splitting `q` into `⌊q / d⌋ · d + (q mod d)` gives exactly the same value, because the first
/// part contributes a whole multiple of `f`:
///
/// `⌈f · q / d⌉ = ⌊q / d⌋ · f + ⌈(q mod d) · f / d⌉`
///
/// No step overflows `u128`. A ring has at most 2³² − 1 nodes, loads and items are below 2⁶⁴,
/// weights below 2³², ε below 2¹⁶, and r below 2⁸, so:
///
/// - `q < 2³² · 2⁶⁴ + 2⁸ · 2⁶⁴ < 2⁹⁷`;
/// - `f < 2¹⁷ · 2³² = 2⁴⁹`, because `100 + ε < 2¹⁷`;
/// - `100 ≤ d < 2⁷ · 2⁶⁴ = 2⁷¹`, because `1 ≤ W < 2⁶⁴`;
/// - `(q mod d) · f < d · f < 2¹²⁰`, so the rounded-up part is at most `f`;
/// - `⌊q / d⌋ · f ≤ q · (100 + ε) / 100 < 2⁹⁷ · 2¹⁰ = 2¹⁰⁷`, because `wᵢ ≤ W`;
/// - so every capacity is below `2¹⁰⁷ + 2⁴⁹`.
#[derive(Debug, Clone, Copy)]
struct Capacities {
    /// `⌊q / d⌋`.
    quotient: u128,
    /// `q mod d`.
    remainder: u128,
    /// `d = 100 · W`.
    divisor: u128,
    /// `100 + ε`.
    scale: u128,
}

impl Capacities {
    /// Prepares the capacities for placing `replicas` copies of a key with load `item_load` on a
    /// ring whose loads sum to `total` and whose weights sum to `total_weight`.
    ///
    /// The inputs must come from a ring: `total` is a sum of at most 2³² − 1 loads, and
    /// `total_weight` is the sum of every node's weight, so it is at least 1 and at least any one
    /// node's weight.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "q < 2⁹⁷, d < 2⁷¹, and d ≥ 100; see the type's docs"
    )]
    fn new(
        total: u128,
        item_load: u64,
        replicas: u8,
        headroom: Headroom,
        total_weight: u64,
    ) -> Self {
        let q = total + u128::from(replicas) * u128::from(item_load);
        let d = PERCENT * u128::from(total_weight);
        Self {
            quotient: q / d,
            remainder: q % d,
            divisor: d,
            scale: PERCENT + u128::from(headroom.percent()),
        }
    }

    /// Returns the capacity of a node of weight `weight`, one of the ring's own nodes.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "every term and the sum stay below 2¹²⁰; see the type's docs"
    )]
    fn of(&self, weight: u32) -> u128 {
        let f = self.scale * u128::from(weight);
        self.quotient * f + (self.remainder * f).div_ceil(self.divisor)
    }
}

/// Completes a placement for which too few nodes had room. Keeps the replicas with room, in
/// order, then fills the remaining slots with the best other nodes of the key's unfiltered
/// preference list.
///
/// Those nodes all come from `home`, which is the preference list's first `home.len()` entries:
/// at most `with_room.len()` of them are already taken, which leaves enough to fill every slot.
fn saturate<'ring>(
    nodes: &'ring [Node],
    with_room: Option<ReplicaSet<'ring>>,
    home: &ReplicaSet<'ring>,
) -> ReplicaSet<'ring> {
    let mut picks: Picks = with_room
        .iter()
        .flatten()
        .map(|node| index_of(nodes, node))
        .collect();
    for node in home {
        if picks.len() == home.len() {
            break;
        }
        let index = index_of(nodes, node);
        if !picks.contains(&index) {
            picks.push(index);
        }
    }
    ReplicaSet::new(nodes, picks, home.replication_factor())
        .expect("home placement is never empty, so neither is a saturated one")
}

/// Returns the canonical index of the node with ID `id`, or `None` if `nodes` has none.
fn position(nodes: &[Node], id: &str) -> Option<usize> {
    nodes
        .binary_search_by(|node| node.id().as_str().cmp(id))
        .ok()
}

/// Returns the canonical index of `node`, one of the ring's own nodes.
fn index_of(nodes: &[Node], node: &Node) -> usize {
    position(nodes, node.id().as_str()).expect("replica sets hold the ring's own nodes")
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute on values small enough for u128, as each test explains"
)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// The largest total load a ring can have: `u32::MAX` nodes holding `u64::MAX` each.
    fn max_total() -> u128 {
        u128::from(u32::MAX) * u128::from(u64::MAX)
    }

    /// The largest total weight a ring can have: `u32::MAX` nodes of weight `u32::MAX`.
    fn max_total_weight() -> u64 {
        u64::from(u32::MAX) * u64::from(u32::MAX)
    }

    fn capacity(
        total: u128,
        item_load: u64,
        replicas: u8,
        percent: u16,
        weight: u32,
        total_weight: u64,
    ) -> u128 {
        Capacities::new(
            total,
            item_load,
            replicas,
            Headroom::from_percent(percent),
            total_weight,
        )
        .of(weight)
    }

    /// The slow reference: whether `c = ⌈n / d⌉` by the ceiling's definition, which needs no
    /// division. That is `(c − 1) · d < n ≤ c · d`, or `c = 0` when `n = 0`. The caller keeps
    /// `n + d` below 2¹²⁸, so no product overflows.
    fn is_ceiling(c: u128, n: u128, d: u128) -> bool {
        if n == 0 {
            c == 0
        } else {
            c >= 1 && (c - 1) * d < n && n <= c * d
        }
    }

    /// Checks `capacity` against the formula's exact value, computed directly. The caller keeps
    /// the numerator `(100 + ε) · (total + r · item) · w` below 2¹²⁰.
    fn matches_formula(
        total: u128,
        item_load: u64,
        replicas: u8,
        percent: u16,
        weight: u32,
        total_weight: u64,
    ) -> bool {
        let numerator = (100 + u128::from(percent))
            * (total + u128::from(replicas) * u128::from(item_load))
            * u128::from(weight);
        let denominator = 100 * u128::from(total_weight);
        let c = capacity(total, item_load, replicas, percent, weight, total_weight);
        is_ceiling(c, numerator, denominator)
    }

    #[test]
    fn capacity_is_the_exact_ceiling_on_a_small_grid() {
        for total in 0..=40 {
            for item_load in 0..=5 {
                for replicas in [1, 2, 3, 7] {
                    for percent in [0, 1, 25, 50, 99, 100, 250] {
                        for total_weight in 1..=9 {
                            for weight in 1..=u32::try_from(total_weight).expect("small") {
                                assert!(
                                    matches_formula(
                                        total,
                                        item_load,
                                        replicas,
                                        percent,
                                        weight,
                                        total_weight
                                    ),
                                    "total {total}, item {item_load}, r {replicas}, \
                                     ε {percent}%, w {weight} of {total_weight}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn capacity_is_exact_at_the_largest_inputs() {
        // u32::MAX nodes of weight u32::MAX holding u64::MAX each, placing 255 replicas of
        // u64::MAX with 65535% headroom. Here q = u64::MAX · (u32::MAX + 255) and w / W =
        // 1 / u32::MAX, so the capacity is ⌈u64::MAX · (u32::MAX + 255) · 65635 / (100 ·
        // u32::MAX)⌉, whose numerator is below 2¹¹⁴.
        let got = capacity(
            max_total(),
            u64::MAX,
            u8::MAX,
            u16::MAX,
            u32::MAX,
            max_total_weight(),
        );
        let numerator = u128::from(u64::MAX) * (u128::from(u32::MAX) + 255) * 65_635;
        let want = numerator.div_ceil(100 * u128::from(u32::MAX));
        assert_eq!(got, want);
    }

    /// A weight and a total weight at least as large, mixing both extremes in.
    fn weights() -> impl Strategy<Value = (u32, u64)> {
        prop_oneof![
            Just((u32::MAX, max_total_weight())),
            Just((u32::MAX, u64::from(u32::MAX))),
            Just((1, max_total_weight())),
            (1..=u32::MAX)
                .prop_flat_map(|weight| (Just(weight), u64::from(weight)..=max_total_weight())),
        ]
    }

    proptest! {
        #[test]
        fn capacity_is_the_exact_ceiling(
            total in 0..1u128 << 60,
            item_load in 0..1u64 << 40,
            replicas in 1..=u8::MAX,
            percent in any::<u16>(),
            (weight, total_weight) in (1..=u32::MAX)
                .prop_flat_map(|weight| (Just(weight), u64::from(weight)..1 << 40)),
        ) {
            // The numerator stays below 2¹⁷ · 2⁶¹ · 2³² = 2¹¹⁰.
            prop_assert!(matches_formula(
                total, item_load, replicas, percent, weight, total_weight
            ));
        }

        // In debug builds, which panic on overflow, this proves the arithmetic never overflows.
        #[test]
        fn capacity_never_overflows_at_the_extremes(
            total in prop_oneof![Just(0), Just(max_total()), 0..=max_total()],
            item_load in prop_oneof![Just(0), Just(u64::MAX), any::<u64>()],
            replicas in prop_oneof![Just(1), Just(u8::MAX), 1..=u8::MAX],
            percent in prop_oneof![Just(0), Just(u16::MAX), any::<u16>()],
            (weight, total_weight) in weights(),
        ) {
            let c = capacity(total, item_load, replicas, percent, weight, total_weight);
            // No node's capacity exceeds the whole post-placement total plus headroom, rounded
            // up, and a node holding all the weight gets exactly that. q · (100 + ε) < 2¹¹⁴.
            let q = total + u128::from(replicas) * u128::from(item_load);
            let whole = (q * (100 + u128::from(percent))).div_ceil(100);
            prop_assert!(c <= whole, "{c} > {whole}");
            if u64::from(weight) == total_weight {
                prop_assert_eq!(c, whole);
            }
            // More load, more weight, or more headroom never lowers a capacity.
            if total < max_total() {
                let more =
                    capacity(total + 1, item_load, replicas, percent, weight, total_weight);
                prop_assert!(more >= c);
            }
            if u64::from(weight) < total_weight && weight < u32::MAX {
                let heavier =
                    capacity(total, item_load, replicas, percent, weight + 1, total_weight);
                prop_assert!(heavier >= c);
            }
            if percent < u16::MAX {
                let roomier =
                    capacity(total, item_load, replicas, percent + 1, weight, total_weight);
                prop_assert!(roomier >= c);
            }
        }

        #[test]
        fn capacity_is_exact_when_the_division_is(
            (weight, total_weight) in weights(),
            percent in prop_oneof![Just(u16::MAX), any::<u16>()],
            multiple in any::<u128>(),
        ) {
            // q = k · 100 · W for k up to max_total / (100 · W), so the capacity is exactly
            // k · (100 + ε) · w, which is below 2¹⁰⁷ like every capacity.
            let d = 100 * u128::from(total_weight);
            let k = multiple % (max_total() / d + 1);
            let c = capacity(k * d, 0, 1, percent, weight, total_weight);
            prop_assert_eq!(c, k * (100 + u128::from(percent)) * u128::from(weight));
        }
    }
}
