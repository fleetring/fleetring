//! Cells: nested placement that composes rings (ADR-0012).
//!
//! A [`Cells`] layer places a key in two steps. Its selector picks one of its cells, and the cell
//! places the key. A cell is a [`Ring`] or another `Cells` layer, so layers nest to any depth:
//! `Cells<Ring>` is cell → nodes, and `Cells<Cells<Ring>>` is region → cell → nodes. All of a
//! key's replicas come from one ring, the *leaf ring* at the end of the key's path, which spreads
//! them across its own zones and hosts. Layers partition keys; zone and host labels spread
//! replicas (ADR-0006).
//!
//! # Fingerprint encoding
//!
//! A layer's fingerprint is XXH3-64, seed 0, of the layer's canonical encoding. Every integer is
//! little-endian, and a *string* is its byte length as a `u32`, then its bytes. The fields, in
//! order:
//!
//! 1. layer tag: the 4 ASCII bytes `CELL`;
//! 2. mode: a `u8`, 0 for Within mode, the only mode;
//! 3. exact rules: their count as a `u32`, then each rule sorted by key (comparing bytes), as
//!    the key and then the cell ID, both strings;
//! 4. prefix rules: the same, sorted by prefix;
//! 5. selector ring: a `u8` flag, 0 when absent and 1 when present, followed when present by the
//!    ring's [`Fingerprint`] as a `u64`;
//! 6. cells: their count as a `u32`, then each cell sorted by ID, as the ID (a string) and then
//!    the cell's fingerprint as a `u64`: a ring's own fingerprint, or a nested layer's, computed
//!    the same way.
//!
//! Placement spec v1 defines the same encoding for ports, in its composition section.

use std::fmt;

use smallvec::SmallVec;

use crate::{
    CellId, Directory, Fingerprint, Node, NodeId, Preference, ReplicaSet, Ring, StableHasher, Xxh3,
};

/// The cells on a key's path, top to leaf. Paths up to four layers deep don't allocate.
type Path<'a> = SmallVec<[&'a CellId; 4]>;

/// The first field of a layer's canonical encoding, which no ring encoding starts with.
const LAYER_TAG: &[u8; 4] = b"CELL";

/// The canonical encoding's mode for Within mode: all of a key's replicas live in one cell.
const MODE_WITHIN: u8 = 0;

/// The XXH3-64 seed of a layer's fingerprint, the same as a ring's.
const FINGERPRINT_SEED: u64 = 0;

/// A placement that a [`Cells`] layer can hold as a cell: a [`Ring`], or another [`Cells`] layer.
///
/// The trait is what lets layers nest: `Cells<P>` holds cells of any type `P` that implements it,
/// and implements it itself. It also lets code work with either kind of placement, for example a
/// router that starts with one ring and later grows into cells.
///
/// The trait is sealed: only [`Ring`] and [`Cells`] implement it, because placement spec v1
/// defines exactly these two kinds of layer, and a layer's fingerprint covers its cells'
/// fingerprints.
///
/// # Examples
///
/// ```
/// use fleetring::{CellPlacement, Cells, Directory, Node, Placement, Ring};
///
/// /// Describes where a key lives, whatever the placement's shape.
/// fn locate<P: Placement>(placement: &P, key: &str) -> String {
///     let placed: CellPlacement<'_> = placement.replicas_for(key.as_bytes());
///     placed.to_string()
/// }
///
/// let ring = Ring::builder().node(Node::new("solo".parse()?)).build()?;
/// assert_eq!(locate(&ring, "tenant:acme"), "[solo]");
///
/// let cells = Cells::builder()
///     .directory(Directory::new().catch_all("main".parse()?))
///     .cell("main".parse()?, ring)
///     .build()?;
/// assert_eq!(locate(&cells, "tenant:acme"), "main [solo]");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait Placement: sealed::Sealed {
    /// The hasher of the rings that place keys.
    type Hasher: StableHasher;

    /// Returns the key's replica set and the path of cells that led to it.
    ///
    /// For a [`Ring`], the path is empty and the replica set is [`Ring::replicas`]. For a
    /// [`Cells`] layer, this is [`Cells::replicas`].
    #[must_use]
    fn replicas_for(&self, key: &[u8]) -> CellPlacement<'_> {
        let mut path = Path::new();
        let ring = self.leaf(key, &mut |cell| path.push(cell));
        CellPlacement {
            path,
            replicas: ring.replicas(key),
        }
    }

    /// Computes the fingerprint, which identifies the placement function.
    ///
    /// For a [`Ring`], this is [`Ring::fingerprint`]; for a [`Cells`] layer,
    /// [`Cells::fingerprint`].
    #[must_use]
    fn fingerprint(&self) -> Fingerprint;

    /// Follows the key's path to its leaf ring, calling `visit` with each cell on the way, top
    /// first. A ring is its own leaf.
    #[doc(hidden)]
    fn leaf<'a>(&'a self, key: &[u8], visit: &mut dyn FnMut(&'a CellId)) -> &'a Ring<Self::Hasher>;

    /// Returns whether any ring in the placement, at any depth, hashes with `seed`.
    #[doc(hidden)]
    fn hashes_with_seed(&self, seed: u64) -> bool;
}

mod sealed {
    /// Keeps [`Placement`](super::Placement) implemented by this crate's rings and cells only.
    pub trait Sealed {}

    impl<H> Sealed for crate::Ring<H> {}

    impl<P: super::Placement> Sealed for super::Cells<P> {}
}

impl<H: StableHasher> Placement for Ring<H> {
    type Hasher = H;

    fn fingerprint(&self) -> Fingerprint {
        Ring::fingerprint(self)
    }

    fn leaf<'a>(&'a self, _key: &[u8], _visit: &mut dyn FnMut(&'a CellId)) -> &'a Ring<H> {
        self
    }

    fn hashes_with_seed(&self, seed: u64) -> bool {
        self.seed() == seed
    }
}

/// Where a key lives: the cells on its path, top to leaf, and its replica set on the leaf ring.
///
/// [`Cells::replicas`] returns one. The path names one cell per layer, for example `["eu",
/// "eu-3"]` for a region layer whose cells are cell layers. That is enough to route a request to
/// the right cell and to explain the decision. The replica set comes from the leaf ring alone, so
/// all of a key's replicas live in one cell.
///
/// Equality compares the paths' cell IDs and the replicas' node IDs, in order. A placement
/// displays as its path joined with `/`, then its replica set, such as `eu/eu-3 [n1, n2, n3]`.
///
/// # Examples
///
/// ```
/// use fleetring::{Cells, Directory, Node, Ring};
///
/// let ring = Ring::builder()
///     .node(Node::new("n1".parse()?))
///     .node(Node::new("n2".parse()?))
///     .build()?;
/// let cells = Cells::builder()
///     .directory(Directory::new().catch_all("eu".parse()?))
///     .cell("eu".parse()?, ring)
///     .build()?;
///
/// let placed = cells.replicas("tenant:acme");
/// let path: Vec<&str> = placed.path().iter().map(|cell| cell.as_str()).collect();
/// assert_eq!(path, ["eu"]);
/// assert_eq!(placed.cell().map(|cell| cell.as_str()), Some("eu"));
/// assert_eq!(placed.replicas().len(), 2);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct CellPlacement<'a> {
    path: Path<'a>,
    replicas: ReplicaSet<'a>,
}

impl<'a> CellPlacement<'a> {
    /// Returns the cells on the key's path, one per layer, from the top layer to the leaf.
    ///
    /// The path is empty only for a placement that a bare [`Ring`] made through
    /// [`Placement::replicas_for`].
    #[must_use]
    pub fn path(&self) -> &[&'a CellId] {
        &self.path
    }

    /// Returns the leaf cell: the last cell on the path, whose ring holds the replicas.
    ///
    /// Returns `None` only for a placement that a bare [`Ring`] made through
    /// [`Placement::replicas_for`]. Leaf IDs are unique within their layer, not across layers,
    /// so compare whole [paths](Self::path) when layers reuse IDs.
    #[must_use]
    pub fn cell(&self) -> Option<&'a CellId> {
        self.path.last().copied()
    }

    /// Returns the key's replica set on the leaf ring: the preferred primary, then its followers.
    #[must_use]
    pub fn replicas(&self) -> &ReplicaSet<'a> {
        &self.replicas
    }

    /// Returns the replica set, consuming the placement.
    #[must_use]
    pub fn into_replicas(self) -> ReplicaSet<'a> {
        self.replicas
    }
}

impl fmt::Debug for CellPlacement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path: Vec<&str> = self.path.iter().map(|cell| cell.as_str()).collect();
        f.debug_struct("CellPlacement")
            .field("path", &path)
            .field("replicas", &self.replicas)
            .finish()
    }
}

impl fmt::Display for CellPlacement<'_> {
    /// Formats the placement as its path joined with `/`, a space, and then its replica set,
    /// such as `eu/eu-3 [n1, n2, n3]`. A placement with an empty path is just its replica set.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (position, cell) in self.path.iter().enumerate() {
            if position > 0 {
                f.write_str("/")?;
            }
            f.write_str(cell.as_str())?;
        }
        if !self.path.is_empty() {
            f.write_str(" ")?;
        }
        fmt::Display::fmt(&self.replicas, f)
    }
}

/// A layer of nested placement: a selector that picks a cell for each key, and the cells, each a
/// [`Ring`] or another `Cells` layer.
///
/// Many systems place keys in two layers: a directory or a ring picks the cell, such as a region,
/// a cell, a tenant pool, or a jurisdiction, and the cell's own ring picks the nodes. Cells keep
/// residency rules enforceable, keep a bad deploy or an overload inside one cell, and turn one
/// lookup over N nodes into two lookups over about √N each.
///
/// # The selector
///
/// The selector is a [`Directory`], a selector ring, or both, and it picks a cell for a key in
/// this order:
///
/// 1. **Directory rules first.** An exact rule for the key wins, then the longest matching
///    prefix rule.
/// 2. **Then the selector ring**, for keys that no rule matches. It is an ordinary [`Ring`] whose
///    node IDs are the cell IDs, and the key's cell is its [preferred primary](Ring::primary). A
///    node's weight is its cell's share of these keys. Weights are explicit, never derived from
///    a cell's contents, so adding, removing, or reweighting a node inside a cell never moves a
///    key to another cell.
///
/// Every key must reach a cell, so a layer without a selector ring needs a directory with a
/// catch-all rule, and a layer with one must not have a catch-all, which would hide the ring.
///
/// # Placement
///
/// All of a key's replicas live in its cell (Within mode). The leaf ring at the end of the key's
/// path picks them exactly as [`Ring::replicas`] does, spread across that ring's zones and hosts,
/// at that ring's replication factor. Filters such as liveness apply inside the cell and never
/// change which cell a key maps to: failing over to another cell would break residency and
/// blast-radius isolation.
///
/// # Seeds
///
/// Every layer must hash keys independently of the layers above it. If a cell's ring hashed keys
/// with its selector ring's seed, the keys reaching the cell would be correlated with their
/// positions in the cell's ring, and a vnode cell would see a skewed slice of its circle.
/// [`build`](CellsBuilder::build) therefore rejects a layer whose selector ring's seed is the seed
/// of any ring inside one of its cells, at any depth.
///
/// Distinct seeds are easy to keep with a rule: derive each ring's seed from where it sits, for
/// example XXH3-64, seed 0, of the slash-separated path of the cell that holds it (`"eu/eu-3"`
/// for the ring of cell `eu-3` inside cell `eu`, `"eu"` for cell `eu`'s selector ring, and `""`
/// for the top selector ring). Any language can compute it, and distinct paths give distinct
/// seeds.
///
/// # Examples
///
/// EU tenants stay in the EU cell by a residency rule, and every other key is hashed to a cell by
/// the selector ring:
///
/// ```
/// use fleetring::{Cells, Directory, Node, Ring, Weight};
///
/// // Each cell is a zone-aware ring with a seed of its own.
/// let eu = Ring::builder()
///     .seed(1)
///     .node(Node::new("eu-1".parse()?).with_zone("eu-west-1a".parse()?))
///     .node(Node::new("eu-2".parse()?).with_zone("eu-west-1b".parse()?))
///     .node(Node::new("eu-3".parse()?).with_zone("eu-west-1c".parse()?))
///     .build()?;
/// let us = Ring::builder()
///     .seed(2)
///     .node(Node::new("us-1".parse()?).with_zone("us-east-1a".parse()?))
///     .node(Node::new("us-2".parse()?).with_zone("us-east-1b".parse()?))
///     .node(Node::new("us-3".parse()?).with_zone("us-east-1c".parse()?))
///     .build()?;
/// // The selector ring's nodes are the cells, weighted by their shares of the hashed keys.
/// let selector = Ring::builder()
///     .seed(7)
///     .node(Node::new("eu".parse()?))
///     .node(Node::new("us".parse()?).with_weight(Weight::new(2).ok_or("zero weight")?))
///     .build()?;
/// let fleet = Cells::builder()
///     .directory(Directory::new().prefix("eu:", "eu".parse()?))
///     .ring_selector(selector)
///     .cell("eu".parse()?, eu)
///     .cell("us".parse()?, us)
///     .build()?;
///
/// let placed = fleet.replicas("eu:tenant:acme");
/// assert_eq!(placed.cell().map(|cell| cell.as_str()), Some("eu"));
/// assert_eq!(placed.replicas().len(), 3);
/// assert!(placed.replicas().iter().all(|node| node.id().as_str().starts_with("eu-")));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Cells<P: Placement = Ring> {
    directory: Directory,
    ring: Option<Ring<P::Hasher>>,
    /// The cells in canonical order: sorted by ID, with no duplicates.
    cells: Box<[(CellId, P)]>,
}

impl<P: Placement> Cells<P> {
    /// Returns an empty builder.
    ///
    /// This is the same as [`CellsBuilder::new`]. The cell type `P` is usually inferred from the
    /// first [`cell`](CellsBuilder::cell).
    pub fn builder() -> CellsBuilder<P> {
        CellsBuilder::new()
    }

    /// Returns the key's cell path and its replica set on the leaf ring.
    ///
    /// The selector picks a cell, and so on down the layers, until a cell is a ring: the leaf
    /// ring. Its [`replicas`](Ring::replicas) are the key's replicas, so they all live in one
    /// cell and spread across that ring's zones and hosts. Keys are hashed exactly as given, and
    /// each layer hashes them with its own seed.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Cells, Directory, Node, Ring};
    ///
    /// let ring = |seed: u64, ids: [&str; 3]| {
    ///     Ring::builder()
    ///         .seed(seed)
    ///         .nodes(ids.map(|id| Node::new(id.parse().expect("valid id"))))
    ///         .build()
    /// };
    /// let fleet = Cells::builder()
    ///     .directory(Directory::new().prefix("vip:", "vip".parse()?).catch_all("main".parse()?))
    ///     .cell("main".parse()?, ring(1, ["m1", "m2", "m3"])?)
    ///     .cell("vip".parse()?, ring(2, ["v1", "v2", "v3"])?)
    ///     .build()?;
    ///
    /// let placed = fleet.replicas("vip:tenant:acme");
    /// assert_eq!(placed.cell().map(|cell| cell.as_str()), Some("vip"));
    /// let vip = fleet.cell("vip").ok_or("no vip cell")?;
    /// assert_eq!(placed.replicas(), &vip.replicas("vip:tenant:acme"));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn replicas(&self, key: impl AsRef<[u8]>) -> CellPlacement<'_> {
        let key = key.as_ref();
        let (path, ring) = self.locate(key);
        CellPlacement {
            path,
            replicas: ring.replicas(key),
        }
    }

    /// Returns the key's placement among the nodes `eligible` accepts, or `None` if it rejects
    /// every node of the key's cell.
    ///
    /// The filter applies inside the key's cell only, exactly as [`Ring::replicas_where`] applies
    /// it, so zones stay diverse and every router picks the same fallback. It never changes which
    /// cell the key maps to: when every node of the cell is down, the key has no replicas rather
    /// than moving to another cell, which would break residency and blast-radius isolation.
    ///
    /// `eligible` may be called up to three times per node of the leaf ring and must answer
    /// consistently during the call.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Cells, Directory, Node, Ring};
    ///
    /// let ring = |seed: u64, ids: [&str; 2]| {
    ///     Ring::builder()
    ///         .seed(seed)
    ///         .nodes(ids.map(|id| Node::new(id.parse().expect("valid id"))))
    ///         .build()
    /// };
    /// let fleet = Cells::builder()
    ///     .directory(Directory::new().prefix("eu:", "eu".parse()?).catch_all("us".parse()?))
    ///     .cell("eu".parse()?, ring(1, ["eu-1", "eu-2"])?)
    ///     .cell("us".parse()?, ring(2, ["us-1", "us-2"])?)
    ///     .build()?;
    ///
    /// // With eu-1 down, the key fails over inside the EU cell.
    /// let placed = fleet
    ///     .replicas_where("eu:tenant:acme", |node| node.id().as_str() != "eu-1")
    ///     .ok_or("every EU node is down")?;
    /// assert_eq!(placed.replicas().to_string(), "[eu-2]");
    ///
    /// // With the whole EU cell down, the key never moves to the US cell.
    /// let none = fleet.replicas_where("eu:tenant:acme", |node| node.id().as_str().starts_with("us-"));
    /// assert!(none.is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn replicas_where<F>(&self, key: impl AsRef<[u8]>, eligible: F) -> Option<CellPlacement<'_>>
    where
        F: FnMut(&Node) -> bool,
    {
        let key = key.as_ref();
        let (path, ring) = self.locate(key);
        ring.replicas_where(key, eligible)
            .map(|replicas| CellPlacement { path, replicas })
    }

    /// Returns the key's preferred primary: the first node of its replica set, from the leaf
    /// ring's [`primary`](Ring::primary).
    #[must_use]
    pub fn primary(&self, key: impl AsRef<[u8]>) -> &Node {
        let key = key.as_ref();
        self.leaf(key, &mut |_| {}).primary(key)
    }

    /// Returns the key's full preference list on the leaf ring: every node of the key's cell,
    /// best first.
    ///
    /// The first R nodes are the key's [`replicas`](Self::replicas), and later ones are where
    /// traffic or data goes when earlier ones are unavailable. The list never leaves the cell.
    /// Like [`Ring::preference`], this allocates.
    #[must_use]
    pub fn preference(&self, key: impl AsRef<[u8]>) -> Preference<'_> {
        let key = key.as_ref();
        self.leaf(key, &mut |_| {}).preference(key)
    }

    /// Returns the key's leaf cell: the last cell on its path, whose ring holds its replicas.
    ///
    /// For a `Cells<Ring>`, that is the cell this layer's selector picks. For nested layers, it
    /// is the innermost cell; [`replicas`](Self::replicas) returns the whole path. Only the
    /// selectors run, so this costs no lookup on the leaf ring.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Cells, Directory, Node, Ring};
    ///
    /// let ring = Ring::builder().seed(1).node(Node::new("n1".parse()?)).build()?;
    /// let fleet = Cells::builder()
    ///     .directory(Directory::new().catch_all("main".parse()?))
    ///     .cell("main".parse()?, ring)
    ///     .build()?;
    /// assert_eq!(fleet.cell_of("tenant:acme").as_str(), "main");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn cell_of(&self, key: impl AsRef<[u8]>) -> &CellId {
        let key = key.as_ref();
        let (top, cell) = self.select(key);
        let mut leaf = top;
        cell.leaf(key, &mut |inner| leaf = inner);
        leaf
    }

    /// Returns the cell with ID `id`, or `None` if the layer has no such cell.
    ///
    /// Takes O(log n) time for n cells.
    #[must_use]
    pub fn cell(&self, id: impl AsRef<str>) -> Option<&P> {
        let id = id.as_ref();
        self.cells
            .binary_search_by(|(cell, _)| cell.as_str().cmp(id))
            .ok()
            .map(|index| &self.cells[index].1)
    }

    /// Returns the cells in canonical order: sorted by ID, comparing IDs byte by byte.
    pub fn cells(&self) -> impl ExactSizeIterator<Item = (&CellId, &P)> {
        self.cells.iter().map(|(id, cell)| (id, cell))
    }

    /// Returns the number of cells, which is always at least 1.
    #[must_use]
    pub fn cell_count(&self) -> usize {
        self.cells.len()
    }

    /// Returns the directory, which is empty when the layer selects by ring alone.
    #[must_use]
    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    /// Returns the selector ring, whose nodes are the cells, or `None` when the layer selects by
    /// directory alone.
    #[must_use]
    pub fn ring_selector(&self) -> Option<&Ring<P::Hasher>> {
        self.ring.as_ref()
    }

    /// Returns a builder pre-filled with this layer's selector and cells.
    ///
    /// This is how a layer changes: replace a cell with [`remove_cell`](CellsBuilder::remove_cell)
    /// and [`cell`](CellsBuilder::cell), or replace the selector, then build the next layer.
    ///
    /// # Examples
    ///
    /// Add a node to one cell:
    ///
    /// ```
    /// use fleetring::{Cells, Directory, Node, Ring};
    ///
    /// let ring = Ring::builder().seed(1).node(Node::new("n1".parse()?)).build()?;
    /// let fleet = Cells::builder()
    ///     .directory(Directory::new().catch_all("main".parse()?))
    ///     .cell("main".parse()?, ring)
    ///     .build()?;
    ///
    /// let grown = fleet
    ///     .cell("main")
    ///     .ok_or("no main cell")?
    ///     .to_builder()
    ///     .node(Node::new("n2".parse()?))
    ///     .build()?;
    /// let next = fleet.to_builder().remove_cell("main").cell("main".parse()?, grown).build()?;
    /// assert_eq!(next.cell("main").map(|ring| ring.node_count()), Some(2));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn to_builder(&self) -> CellsBuilder<P>
    where
        P: Clone,
        P::Hasher: Clone,
    {
        CellsBuilder {
            directory: self.directory.clone(),
            ring: self.ring.clone(),
            cells: self.cells.to_vec(),
        }
    }

    /// Computes the layer's fingerprint, which identifies its placement function.
    ///
    /// The fingerprint is XXH3-64, seed 0, over a canonical encoding of the layer: its directory
    /// rules, the [fingerprint](Ring::fingerprint) of its selector ring, and every cell's ID and
    /// fingerprint, in ID order. It therefore changes when any layer changes, at any depth,
    /// including the cells' weights, which the selector ring's fingerprint covers. Epochs never
    /// affect placement, so they never affect the fingerprint.
    ///
    /// The fingerprint is computed on every call: it fingerprints every ring in the layer, each
    /// costing about 64 lookups, so compute it once and keep it.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Cells, Directory, Epoch, Node, Ring};
    ///
    /// let ring = Ring::builder().seed(1).node(Node::new("n1".parse()?)).build()?;
    /// let build = |ring: Ring, directory: Directory| -> Result<Cells, Box<dyn std::error::Error>> {
    ///     Ok(Cells::builder().directory(directory).cell("main".parse()?, ring).build()?)
    /// };
    /// let catch_all = Directory::new().catch_all("main".parse()?);
    /// let fleet = build(ring.clone(), catch_all.clone())?;
    ///
    /// // Epochs never change the fingerprint.
    /// let later = build(ring.to_builder().epoch(Epoch::new(9)).build()?, catch_all.clone())?;
    /// assert_eq!(later.fingerprint(), fleet.fingerprint());
    ///
    /// // A new rule does, even when it sends keys where they already go.
    /// let pinned = build(ring, catch_all.exact("tenant:acme", "main".parse()?))?;
    /// assert_ne!(pinned.fingerprint(), fleet.fingerprint());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Panics
    ///
    /// Panics in the cases where [`Ring::fingerprint`] does, or if a directory rule is longer
    /// than `u32::MAX` bytes, which the encoding can't represent.
    #[must_use]
    pub fn fingerprint(&self) -> Fingerprint {
        let mut out = Encoder::default();
        out.put_raw(LAYER_TAG);
        out.put_u8(MODE_WITHIN);
        out.put_rules(self.directory.exact_rules());
        out.put_rules(self.directory.prefix_rules());
        match &self.ring {
            None => out.put_u8(0),
            Some(ring) => {
                out.put_u8(1);
                out.put_u64(ring.fingerprint().get());
            }
        }
        out.put_len(self.cells.len());
        for (id, cell) in &self.cells {
            out.put_bytes(id.as_str().as_bytes());
            out.put_u64(cell.fingerprint().get());
        }
        Fingerprint::new(Xxh3.hash(&out.bytes, FINGERPRINT_SEED))
    }

    /// Returns the cell this layer's selector picks for `key`: directory rules first, then the
    /// selector ring.
    fn select(&self, key: &[u8]) -> (&CellId, &P) {
        let target = match self.directory.lookup(key) {
            Some(cell) => cell.as_str(),
            None => self
                .ring
                .as_ref()
                .expect("build() gives every layer without a catch-all rule a selector ring")
                .primary(key)
                .id()
                .as_str(),
        };
        let index = self
            .cells
            .binary_search_by(|(id, _)| id.as_str().cmp(target))
            .expect("build() checks that every selector target names a cell");
        let (id, cell) = &self.cells[index];
        (id, cell)
    }

    /// Follows `key`'s path to its leaf ring, collecting the cells on the way.
    fn locate(&self, key: &[u8]) -> (Path<'_>, &Ring<P::Hasher>) {
        let mut path = Path::new();
        let ring = self.leaf(key, &mut |cell| path.push(cell));
        (path, ring)
    }
}

impl<P: Placement> Placement for Cells<P> {
    type Hasher = P::Hasher;

    fn fingerprint(&self) -> Fingerprint {
        Cells::fingerprint(self)
    }

    fn leaf<'a>(&'a self, key: &[u8], visit: &mut dyn FnMut(&'a CellId)) -> &'a Ring<Self::Hasher> {
        let (id, cell) = self.select(key);
        visit(id);
        cell.leaf(key, visit)
    }

    fn hashes_with_seed(&self, seed: u64) -> bool {
        self.ring.as_ref().is_some_and(|ring| ring.seed() == seed)
            || self
                .cells
                .iter()
                .any(|(_, cell)| cell.hashes_with_seed(seed))
    }
}

// Not derived: the derive would require `P: Clone` but not `P::Hasher: Clone`, which the selector
// ring needs.
impl<P> Clone for Cells<P>
where
    P: Placement + Clone,
    P::Hasher: Clone,
{
    fn clone(&self) -> Self {
        Self {
            directory: self.directory.clone(),
            ring: self.ring.clone(),
            cells: self.cells.clone(),
        }
    }
}

impl<P> fmt::Debug for Cells<P>
where
    P: Placement + fmt::Debug,
    P::Hasher: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cells")
            .field("directory", &self.directory)
            .field("ring_selector", &self.ring)
            .field("cells", &DebugCells(&self.cells))
            .finish()
    }
}

/// Debug-formats cells as a map from cell ID to cell.
struct DebugCells<'a, P>(&'a [(CellId, P)]);

impl<P: fmt::Debug> fmt::Debug for DebugCells<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.iter().map(|(id, cell)| (id.as_str(), cell)))
            .finish()
    }
}

/// Collects a layer's selector and cells, then validates them all at once.
///
/// Start from [`Cells::builder`], [`CellsBuilder::new`], or an existing layer's
/// [`to_builder`](Cells::to_builder). The setters never fail: [`build`](Self::build) performs
/// every check.
///
/// # Examples
///
/// A selector ring alone, with cells weighted 1 and 3:
///
/// ```
/// use fleetring::{CellsBuilder, Node, Ring, Weight};
///
/// let ring = |seed: u64, id: &str| {
///     Ring::builder()
///         .seed(seed)
///         .node(Node::new(id.parse().expect("valid id")))
///         .build()
/// };
/// let selector = Ring::builder()
///     .seed(100)
///     .node(Node::new("small".parse()?))
///     .node(Node::new("large".parse()?).with_weight(Weight::new(3).ok_or("zero weight")?))
///     .build()?;
/// let cells = CellsBuilder::new()
///     .ring_selector(selector)
///     .cell("small".parse()?, ring(1, "s1")?)
///     .cell("large".parse()?, ring(2, "l1")?)
///     .build()?;
/// assert_eq!(cells.cell_count(), 2);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[must_use = "a builder does nothing until `build` is called"]
pub struct CellsBuilder<P: Placement = Ring> {
    directory: Directory,
    ring: Option<Ring<P::Hasher>>,
    cells: Vec<(CellId, P)>,
}

impl<P: Placement> CellsBuilder<P> {
    /// Creates a builder with no directory rules, no selector ring, and no cells.
    pub fn new() -> Self {
        Self {
            directory: Directory::new(),
            ring: None,
            cells: Vec::new(),
        }
    }

    /// Sets the directory, replacing any earlier one.
    ///
    /// Directory rules run before the selector ring. Without a selector ring, the directory needs
    /// a [catch-all](Directory::catch_all) rule.
    pub fn directory(mut self, directory: Directory) -> Self {
        self.directory = directory;
        self
    }

    /// Sets the selector ring, replacing any earlier one.
    ///
    /// The ring's node IDs name the layer's cells, and their weights are the cells' shares of
    /// the keys that no directory rule matches. Only the ring's preferred primary picks the cell,
    /// so its replication factor, zones, and hosts don't affect placement, though its fingerprint
    /// covers them. Its seed must differ from the seed of every ring inside the cells.
    pub fn ring_selector(mut self, ring: Ring<P::Hasher>) -> Self {
        self.ring = Some(ring);
        self
    }

    /// Adds a cell: a ring, or a nested layer.
    ///
    /// Cells can be added in any order, because [`build`](Self::build) sorts them by ID. Adding a
    /// second cell with the same ID makes `build` fail, so to replace a cell, call
    /// [`remove_cell`](Self::remove_cell) first.
    pub fn cell(mut self, id: CellId, placement: P) -> Self {
        self.cells.push((id, placement));
        self
    }

    /// Adds every `(id, placement)` pair in `cells`.
    pub fn cells(mut self, cells: impl IntoIterator<Item = (CellId, P)>) -> Self {
        self.cells.extend(cells);
        self
    }

    /// Removes every cell whose ID is `id`, if there is one.
    pub fn remove_cell(mut self, id: impl AsRef<str>) -> Self {
        let id = id.as_ref();
        self.cells.retain(|(cell, _)| cell.as_str() != id);
        self
    }

    /// Validates the selector and cells, then builds the layer.
    ///
    /// Sorts the cells into canonical order. Takes O((n + r) log n) time for n cells and r
    /// directory rules and selector ring nodes, plus one visit to every ring inside the cells to
    /// check seeds.
    ///
    /// # Errors
    ///
    /// Builds nothing and returns an error if, checking in this order:
    ///
    /// - there are no cells ([`BuildCellsError::NoCells`]);
    /// - two cells share an ID ([`BuildCellsError::DuplicateCell`]);
    /// - there is no selector ring and the directory has no catch-all rule, so some keys would
    ///   reach no cell ([`BuildCellsError::NoFallback`]);
    /// - there is a selector ring and the directory has a catch-all rule, so the ring would never
    ///   be used ([`BuildCellsError::ShadowedRing`]);
    /// - a directory rule names a cell the layer doesn't have
    ///   ([`BuildCellsError::UnknownDirectoryTarget`]);
    /// - a selector ring node names a cell the layer doesn't have
    ///   ([`BuildCellsError::UnknownRingTarget`]);
    /// - a cell is named by no directory rule and no selector ring node
    ///   ([`BuildCellsError::UnreachableCell`]);
    /// - a ring inside a cell, at any depth, has the selector ring's seed
    ///   ([`BuildCellsError::SharedSeed`]).
    ///
    /// Rules are checked exact rules first, then prefix rules, each sorted by bytes; nodes and
    /// cells are checked in canonical order. The same input therefore always produces the same
    /// error, whatever order it was added in.
    pub fn build(self) -> Result<Cells<P>, BuildCellsError> {
        let Self {
            directory,
            ring,
            mut cells,
        } = self;
        if cells.is_empty() {
            return Err(BuildCellsError::NoCells);
        }
        cells.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        if let Some(pair) = cells.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(BuildCellsError::DuplicateCell {
                id: pair[0].0.clone(),
            });
        }
        match (&ring, directory.has_catch_all()) {
            (None, false) => return Err(BuildCellsError::NoFallback),
            (Some(_), true) => return Err(BuildCellsError::ShadowedRing),
            _ => {}
        }
        let position = |id: &str| {
            cells
                .binary_search_by(|(cell, _)| cell.as_str().cmp(id))
                .ok()
        };
        let mut reachable = vec![false; cells.len()];
        for cell in directory.targets() {
            let Some(index) = position(cell.as_str()) else {
                return Err(BuildCellsError::UnknownDirectoryTarget { cell: cell.clone() });
            };
            reachable[index] = true;
        }
        for node in ring.iter().flat_map(Ring::nodes) {
            let Some(index) = position(node.id().as_str()) else {
                return Err(BuildCellsError::UnknownRingTarget {
                    node: node.id().clone(),
                });
            };
            reachable[index] = true;
        }
        if let Some(index) = reachable.iter().position(|&reached| !reached) {
            return Err(BuildCellsError::UnreachableCell {
                id: cells[index].0.clone(),
            });
        }
        if let Some(seed) = ring.as_ref().map(Ring::seed)
            && let Some((id, _)) = cells.iter().find(|(_, cell)| cell.hashes_with_seed(seed))
        {
            return Err(BuildCellsError::SharedSeed {
                cell: id.clone(),
                seed,
            });
        }
        Ok(Cells {
            directory,
            ring,
            cells: cells.into_boxed_slice(),
        })
    }
}

impl<P: Placement> Default for CellsBuilder<P> {
    fn default() -> Self {
        Self::new()
    }
}

// Not derived, for the same reason as `Clone for Cells`.
impl<P> Clone for CellsBuilder<P>
where
    P: Placement + Clone,
    P::Hasher: Clone,
{
    fn clone(&self) -> Self {
        Self {
            directory: self.directory.clone(),
            ring: self.ring.clone(),
            cells: self.cells.clone(),
        }
    }
}

impl<P> fmt::Debug for CellsBuilder<P>
where
    P: Placement + fmt::Debug,
    P::Hasher: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CellsBuilder")
            .field("directory", &self.directory)
            .field("ring_selector", &self.ring)
            .field("cells", &DebugCells(&self.cells))
            .finish()
    }
}

/// The error returned when [`CellsBuilder::build`] rejects its input.
///
/// `build` reports the first problem it finds. It checks in a fixed order and walks rules, nodes,
/// and cells in sorted order, so the same input always produces the same error, whatever order it
/// was added in.
///
/// # Examples
///
/// ```
/// use fleetring::{BuildCellsError, Cells, Node, Ring};
///
/// // A cell's ring must not hash with its selector ring's seed.
/// let cell = Ring::builder().seed(7).node(Node::new("n1".parse()?)).build()?;
/// let selector = Ring::builder().seed(7).node(Node::new("eu".parse()?)).build()?;
/// let error = Cells::builder()
///     .ring_selector(selector)
///     .cell("eu".parse()?, cell)
///     .build()
///     .err()
///     .ok_or("the shared seed should be rejected")?;
/// assert_eq!(error, BuildCellsError::SharedSeed { cell: "eu".parse()?, seed: 7 });
/// assert_eq!(
///     error.to_string(),
///     "cell 'eu' holds a ring with seed 7, the selector ring's seed; each layer must hash keys with its own seed"
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BuildCellsError {
    /// The builder has no cells. A layer needs at least one.
    #[error("cells layer has no cells")]
    NoCells,

    /// Two or more cells share an ID.
    #[error("cell id '{id}' appears more than once")]
    DuplicateCell {
        /// The shared ID. If several IDs are shared, this is the smallest.
        id: CellId,
    },

    /// Some keys would reach no cell: there is no selector ring, and the directory has no
    /// catch-all rule.
    ///
    /// Add a [catch-all](Directory::catch_all) rule or a selector ring.
    #[error(
        "keys that match no directory rule reach no cell; add a catch-all rule or a selector ring"
    )]
    NoFallback,

    /// The directory's catch-all rule matches every key, so the selector ring would never be
    /// used.
    ///
    /// Remove the catch-all rule or the selector ring.
    #[error("the directory's catch-all rule matches every key, so the selector ring is never used")]
    ShadowedRing,

    /// A directory rule sends keys to a cell the layer doesn't have.
    #[error("a directory rule names cell '{cell}', which the layer doesn't have")]
    UnknownDirectoryTarget {
        /// The rule's cell ID. If several rules name unknown cells, this is the first rule's:
        /// exact rules first, then prefix rules, each sorted by bytes.
        cell: CellId,
    },

    /// A selector ring node names a cell the layer doesn't have.
    #[error("selector ring node '{node}' names a cell the layer doesn't have")]
    UnknownRingTarget {
        /// The node's ID. If several nodes name unknown cells, this is the smallest.
        node: NodeId,
    },

    /// A cell can't be reached: no directory rule and no selector ring node names it.
    #[error("cell '{id}' is unreachable: no directory rule or selector ring node names it")]
    UnreachableCell {
        /// The cell's ID. If several cells are unreachable, this is the smallest.
        id: CellId,
    },

    /// A ring inside a cell hashes keys with the selector ring's seed.
    ///
    /// The ring is the cell itself, or for a nested layer, any selector ring or cell ring inside
    /// it, at any depth. Keys that reach the cell would be correlated with their positions in
    /// that ring, so every layer needs its own seed. [`Cells`] recommends a way to derive them.
    #[error(
        "cell '{cell}' holds a ring with seed {seed}, the selector ring's seed; each layer must hash keys with its own seed"
    )]
    SharedSeed {
        /// The cell that holds the ring. If several cells do, this is the smallest.
        cell: CellId,
        /// The shared seed.
        seed: u64,
    },
}

/// Writes the integers and strings of a layer's canonical encoding.
#[derive(Debug, Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn put_raw(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    /// Writes a length or count as a `u32`.
    fn put_len(&mut self, len: usize) {
        let len = u32::try_from(len).unwrap_or_else(|_| {
            panic!("{len} exceeds u32::MAX, the largest length or count the encoding holds")
        });
        self.bytes.extend_from_slice(&len.to_le_bytes());
    }

    /// Writes `bytes` as a string: its length, then the bytes.
    fn put_bytes(&mut self, bytes: &[u8]) {
        self.put_len(bytes.len());
        self.put_raw(bytes);
    }

    /// Writes rules as their count, then each rule's key or prefix and cell ID.
    fn put_rules<'r>(&mut self, rules: impl ExactSizeIterator<Item = (&'r [u8], &'r CellId)>) {
        self.put_len(rules.len());
        for (bytes, cell) in rules {
            self.put_bytes(bytes);
            self.put_bytes(cell.as_str().as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(seed: u64, ids: &[&str]) -> Ring {
        Ring::builder()
            .seed(seed)
            .nodes(
                ids.iter()
                    .map(|id| Node::new(id.parse().expect("test ids are valid"))),
            )
            .build()
            .expect("test rings are valid")
    }

    fn cell(id: &str) -> CellId {
        id.parse().expect("test cell ids are valid")
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// The canonical encoding of a layer, rebuilt by hand from the module docs.
    #[test]
    fn the_encoding_lists_tag_mode_rules_selector_and_cells_in_order() {
        let a = ring(1, &["n1"]);
        let b = ring(2, &["n2"]);
        let selector = ring(3, &["b"]);
        let layer = Cells::builder()
            .directory(
                Directory::new()
                    .exact("k", cell("a"))
                    .prefix("p:", cell("a")),
            )
            .ring_selector(selector.clone())
            .cell(cell("b"), b.clone())
            .cell(cell("a"), a.clone())
            .build()
            .expect("valid layer");
        let want = [
            hex(b"CELL"),
            "00".to_owned(),                // mode: within
            "01000000".to_owned(),          // 1 exact rule:
            "01000000".to_owned() + "6b",   // "k"
            "01000000".to_owned() + "61",   // -> "a"
            "01000000".to_owned(),          // 1 prefix rule:
            "02000000".to_owned() + "703a", // "p:"
            "01000000".to_owned() + "61",   // -> "a"
            "01".to_owned(),                // selector ring present:
            hex(&selector.fingerprint().get().to_le_bytes()),
            "02000000".to_owned(), // 2 cells, by ID:
            "01000000".to_owned() + "61",
            hex(&a.fingerprint().get().to_le_bytes()),
            "01000000".to_owned() + "62",
            hex(&b.fingerprint().get().to_le_bytes()),
        ]
        .concat();
        let bytes: Vec<u8> = (0..want.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&want[i..i + 2], 16).expect("hex"))
            .collect();
        assert_eq!(layer.fingerprint(), Fingerprint::new(Xxh3.hash(&bytes, 0)));
    }

    #[test]
    fn a_layer_without_a_selector_ring_encodes_a_zero_flag() {
        let a = ring(1, &["n1"]);
        let layer = Cells::builder()
            .directory(Directory::new().catch_all(cell("a")))
            .cell(cell("a"), a.clone())
            .build()
            .expect("valid layer");
        let mut bytes = b"CELL".to_vec();
        bytes.push(0); // mode
        bytes.extend_from_slice(&0u32.to_le_bytes()); // no exact rules
        bytes.extend_from_slice(&1u32.to_le_bytes()); // 1 prefix rule:
        bytes.extend_from_slice(&0u32.to_le_bytes()); // the empty prefix
        bytes.extend_from_slice(&1u32.to_le_bytes()); // -> "a"
        bytes.push(b'a');
        bytes.push(0); // no selector ring
        bytes.extend_from_slice(&1u32.to_le_bytes()); // 1 cell:
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(b'a');
        bytes.extend_from_slice(&a.fingerprint().get().to_le_bytes());
        assert_eq!(layer.fingerprint(), Fingerprint::new(Xxh3.hash(&bytes, 0)));
    }

    #[test]
    fn rings_report_their_own_seed_and_layers_every_seed_inside() {
        let inner = Cells::builder()
            .ring_selector(ring(5, &["x"]))
            .cell(cell("x"), ring(6, &["n1"]))
            .build()
            .expect("valid layer");
        assert!(Placement::hashes_with_seed(&ring(4, &["n"]), 4));
        assert!(!Placement::hashes_with_seed(&ring(4, &["n"]), 5));
        assert!(inner.hashes_with_seed(5) && inner.hashes_with_seed(6));
        assert!(!inner.hashes_with_seed(7));
    }
}
