//! Key lookups: home placement and preference lists (ADR-0001, ADR-0006).

use crate::rendezvous::Rendezvous;
use crate::select::{self, CandidateOrder, Picks};
use crate::strategy::StrategyIndex;
use crate::vnodes::VnodeWalk;
use crate::{Node, Preference, ReplicaSet, ReplicationFactor, Ring, StableHasher};

impl<H: StableHasher> Ring<H> {
    /// Returns the key's home replica set at the ring's replication factor.
    ///
    /// The first node is the preferred primary. Replicas spread across zones first, then
    /// hosts. If the ring has fewer nodes than the replication factor, the set is shorter and
    /// [`ReplicaSet::is_under_replicated`] returns true.
    ///
    /// Keys are hashed exactly as given: no normalization or case folding.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .node(Node::new("laptop-1".parse()?).with_zone("home".parse()?))
    ///     .node(Node::new("laptop-2".parse()?).with_zone("home".parse()?))
    ///     .node(Node::new("vps".parse()?).with_zone("cloud".parse()?))
    ///     .build()?;
    ///
    /// let replicas = ring.replicas("agent:sam-researcher");
    /// assert_eq!(replicas.len(), 3);
    /// // The first two replicas always cover both zones.
    /// let zones: Vec<_> = replicas.iter().take(2).map(|node| node.zone()).collect();
    /// assert_ne!(zones[0], zones[1]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn replicas(&self, key: impl AsRef<[u8]>) -> ReplicaSet<'_> {
        self.replicas_n(key, self.replication_factor())
    }

    /// Returns the key's home replica set at replication factor `factor`.
    ///
    /// The set for `factor` is always a prefix of the set for a larger factor, so changing a
    /// key's replication factor never reshuffles its existing replicas.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, ReplicationFactor, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .nodes(["a", "b", "c", "d"].map(|id| Node::new(id.parse().expect("valid id"))))
    ///     .build()?;
    /// let two = ReplicationFactor::new(2).ok_or("zero")?;
    /// let three = ReplicationFactor::new(3).ok_or("zero")?;
    /// let short = ring.replicas_n("tenant:acme", two).to_ids();
    /// let long = ring.replicas_n("tenant:acme", three).to_ids();
    /// assert_eq!(short[..], long[..2]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn replicas_n(&self, key: impl AsRef<[u8]>, factor: ReplicationFactor) -> ReplicaSet<'_> {
        let picks = self.select(key.as_ref(), usize::from(factor.get()), |_| true);
        ReplicaSet::new(self.nodes(), picks, factor).expect("a ring always has at least one node")
    }

    /// Returns the key's replica set among the nodes `eligible` accepts, or `None` if it
    /// rejects them all.
    ///
    /// Use this for failover: filtering out down nodes gives exactly the replica set of a ring
    /// built without them, so every router picks the same fallback, and zones stay diverse.
    /// Filtering never changes membership, so it never moves data.
    ///
    /// `eligible` may be called up to three times per node and must answer consistently
    /// during the call.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .nodes(["a", "b", "c", "d"].map(|id| Node::new(id.parse().expect("valid id"))))
    ///     .build()?;
    /// let home = ring.replicas("agent:7");
    /// let down = home.primary().id().clone();
    /// let failover = ring.replicas_where("agent:7", |node| node.id() != &down).ok_or("no nodes")?;
    /// assert!(!failover.contains(&down));
    /// // The surviving home replicas keep their order; the next candidate fills the gap.
    /// assert_eq!(failover.get(0), home.get(1));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn replicas_where<F>(
        &self,
        key: impl AsRef<[u8]>,
        mut eligible: F,
    ) -> Option<ReplicaSet<'_>>
    where
        F: FnMut(&Node) -> bool,
    {
        let nodes = self.nodes();
        self.replicas_hashed_where(
            self.key_hash(key.as_ref()),
            self.replication_factor(),
            |index| eligible(&nodes[index]),
        )
    }

    /// Returns the key's preferred primary: the first node of its replica set.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let ring = Ring::builder().node(Node::new("solo".parse()?)).build()?;
    /// assert_eq!(ring.primary("agent:1").id().as_str(), "solo");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn primary(&self, key: impl AsRef<[u8]>) -> &Node {
        let picks = self.select(key.as_ref(), 1, |_| true);
        &self.nodes()[picks[0]]
    }

    /// Returns the key's full preference list: every node, best first.
    ///
    /// The first R nodes are [`replicas`](Self::replicas). Later nodes are where traffic or
    /// data goes when earlier ones are unavailable. Unlike `replicas`, this allocates.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .nodes(["a", "b", "c", "d"].map(|id| Node::new(id.parse().expect("valid id"))))
    ///     .build()?;
    /// assert_eq!(ring.preference("room:42").len(), 4);
    /// // The replica set is the list's first R nodes.
    /// let replicas = ring.replicas("room:42");
    /// assert!(ring.preference("room:42").take(3).eq(&replicas));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn preference(&self, key: impl AsRef<[u8]>) -> Preference<'_> {
        self.preference_where(key, |_| true)
    }

    /// Returns the key's preference list among the nodes `eligible` accepts.
    ///
    /// Filtering happens before zone and host spreading, exactly as in
    /// [`replicas_where`](Self::replicas_where). `eligible` is called once per node.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring};
    ///
    /// let ring = Ring::builder()
    ///     .nodes(["a", "b", "c", "d"].map(|id| Node::new(id.parse().expect("valid id"))))
    ///     .build()?;
    /// let all: Vec<&Node> = ring.preference("room:42").collect();
    /// let down = all[0].id();
    /// let live: Vec<&Node> = ring.preference_where("room:42", |node| node.id() != down).collect();
    /// // Without zones or hosts to spread over, filtering only skips the rejected nodes.
    /// assert_eq!(live, all[1..]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn preference_where<F>(&self, key: impl AsRef<[u8]>, mut eligible: F) -> Preference<'_>
    where
        F: FnMut(&Node) -> bool,
    {
        let nodes = self.nodes();
        let key_hash = self.key_hash(key.as_ref());
        let members = self.members();
        let eligible = |index: usize| eligible(&nodes[index]);
        let order = match self.strategy_index() {
            StrategyIndex::Rendezvous => {
                Rendezvous::new(members, self.hasher(), self.seed(), key_hash).order(eligible)
            }
            StrategyIndex::Vnodes(table) => {
                VnodeWalk::new(members, table, key_hash).order(eligible)
            }
        };
        Preference::new(nodes, select::preference(&members, &order))
    }

    /// Returns a hashed key's replica set among the nodes `eligible` accepts, by canonical
    /// index, or `None` if it rejects them all. Policies that keep per-node state in canonical
    /// order (bounded loads) use this to skip ID lookups.
    pub(crate) fn replicas_hashed_where<F: FnMut(usize) -> bool>(
        &self,
        key_hash: u64,
        factor: ReplicationFactor,
        eligible: F,
    ) -> Option<ReplicaSet<'_>> {
        let picks = self.select_hashed(key_hash, usize::from(factor.get()), eligible);
        ReplicaSet::new(self.nodes(), picks, factor)
    }

    /// Picks up to `r` replicas for `key` among nodes `eligible` accepts.
    fn select<F: FnMut(usize) -> bool>(&self, key: &[u8], r: usize, eligible: F) -> Picks {
        self.select_hashed(self.key_hash(key), r, eligible)
    }

    /// Picks up to `r` replicas for a hashed key among nodes `eligible` accepts.
    fn select_hashed<F: FnMut(usize) -> bool>(
        &self,
        key_hash: u64,
        r: usize,
        eligible: F,
    ) -> Picks {
        let members = self.members();
        match self.strategy_index() {
            StrategyIndex::Rendezvous => select::select(
                &Rendezvous::new(members, self.hasher(), self.seed(), key_hash),
                r,
                eligible,
            ),
            StrategyIndex::Vnodes(table) => {
                select::select(&VnodeWalk::new(members, table, key_hash), r, eligible)
            }
        }
    }
}
