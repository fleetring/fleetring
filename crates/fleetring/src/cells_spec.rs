//! Cells specs: the portable description of a cells layer, at every depth (ADR-0008, ADR-0012).
//!
//! A [`CellsSpec`] is to a [`Cells`] layer what a [`RingSpec`] is to a [`Ring`], and it nests:
//! each cell holds a ring spec or another cells spec. Loading a spec checks every fingerprint
//! inside it. Each layer's check reuses the fingerprints its rings and nested layers store once
//! those are checked, so writing or loading a spec costs one fingerprint per ring at any depth.

use std::fmt;

use crate::cells::layer_fingerprint;
use crate::{
    BuildCellsError, CellId, Cells, Directory, Fingerprint, FromSpecError, Placement, Ring,
    RingSpec,
};

/// A portable, versioned description of a [`Cells`] layer: everything needed to rebuild it
/// exactly, at every depth.
///
/// [`Cells::to_spec`] writes a spec, and [`Cells::from_spec`] rebuilds the layer from one. A
/// cells spec travels the way a [`RingSpec`] does: the control plane stores it and ships it to
/// routers, and every router rebuilds exactly the same layer. It holds the layer's
/// [`Directory`], the spec of its selector ring, and a [`CellSpec`] for each cell: the cell's ID
/// and the spec of its placement, which is a ring spec or, for a nested layer, another cells
/// spec.
///
/// The [`fingerprint`](Self::fingerprint) protects the spec, as a ring spec's does. It is the
/// layer's [`Cells::fingerprint`], which covers the directory rules and the fingerprint of every
/// ring and nested layer inside, and `from_spec` checks it along with every fingerprint inside.
/// A spec that changed after it was written is therefore rejected rather than silently placing
/// keys elsewhere, which is also why deserializing can ignore unknown fields. A layer has no
/// epoch of its own; its rings carry theirs.
///
/// The fields are public, so a spec can be inspected and stored like any record. The struct is
/// `#[non_exhaustive]` so later formats can add fields: get a spec from `to_spec` or by
/// deserializing one, not from a struct literal.
///
/// With the `serde` feature, a spec serializes to JSON or another self-describing format, with
/// its fields in the order below. The directory takes the form [`Directory`] describes, a ring
/// spec takes the form of a [`RingSpec`], and each cell is an object with its `id` and its
/// `placement`: `{"ring": ...}` for a ring, or `{"cells": ...}` for a nested layer. The layer in
/// the first example below serializes, with `serde_json::to_string_pretty`, as:
///
/// ```json
/// {
///   "format": 1,
///   "directory": {
///     "exact": [
///       {
///         "key": {
///           "hex": "ff00"
///         },
///         "cell": "vip"
///       }
///     ],
///     "prefix": [
///       {
///         "prefix": "",
///         "cell": "main"
///       },
///       {
///         "prefix": "vip:",
///         "cell": "vip"
///       }
///     ]
///   },
///   "ring_selector": null,
///   "cells": [
///     {
///       "id": "main",
///       "placement": {
///         "ring": {
///           "format": 1,
///           "epoch": 0,
///           "hasher": "xxh3-64",
///           "seed": 1,
///           "strategy": {
///             "kind": "rendezvous"
///           },
///           "replication_factor": 3,
///           "nodes": [
///             {
///               "id": "m1",
///               "weight": 1,
///               "zone": null,
///               "host": null
///             }
///           ],
///           "fingerprint": "2e785be0a27abddf"
///         }
///       }
///     },
///     {
///       "id": "vip",
///       "placement": {
///         "ring": {
///           "format": 1,
///           "epoch": 0,
///           "hasher": "xxh3-64",
///           "seed": 2,
///           "strategy": {
///             "kind": "rendezvous"
///           },
///           "replication_factor": 3,
///           "nodes": [
///             {
///               "id": "v1",
///               "weight": 1,
///               "zone": null,
///               "host": null
///             }
///           ],
///           "fingerprint": "fa50a1e6a2ff3ce7"
///         }
///       }
///     }
///   ],
///   "fingerprint": "2ec6a5648d7ab6a1"
/// }
/// ```
///
/// # Examples
///
/// ```
/// use fleetring::{Cells, CellsSpec, Directory, Node, PlacementSpec, Ring, Xxh3};
///
/// let ring = |seed: u64, id: &str| {
///     Ring::builder()
///         .seed(seed)
///         .node(Node::new(id.parse().expect("valid id")))
///         .build()
/// };
/// let layer = Cells::builder()
///     .directory(
///         Directory::new()
///             .exact([0xff, 0x00], "vip".parse()?)
///             .prefix("vip:", "vip".parse()?)
///             .catch_all("main".parse()?),
///     )
///     .cell("vip".parse()?, ring(2, "v1")?)
///     .cell("main".parse()?, ring(1, "m1")?)
///     .build()?;
/// let spec = layer.to_spec();
/// assert_eq!(spec.format, CellsSpec::FORMAT);
/// assert_eq!(spec.ring_selector, None);
/// let ids: Vec<&str> = spec.cells.iter().map(|cell| cell.id.as_str()).collect();
/// assert_eq!(ids, ["main", "vip"]);
/// assert!(matches!(spec.cells[0].placement, PlacementSpec::Ring(_)));
/// assert_eq!(spec.fingerprint, layer.fingerprint());
///
/// let copy: Cells = Cells::from_spec(&spec, Xxh3)?;
/// assert_eq!(copy.replicas([0xff, 0x00]), layer.replicas([0xff, 0x00]));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// With the `serde` feature, a spec round-trips through JSON:
///
/// ```
/// # use fleetring::{Cells, CellsSpec, Directory, Node, Ring, Xxh3};
/// # let ring = Ring::builder().seed(1).node(Node::new("m1".parse()?)).build()?;
/// # let layer = Cells::builder()
/// #     .directory(Directory::new().catch_all("main".parse()?))
/// #     .cell("main".parse()?, ring)
/// #     .build()?;
/// # #[cfg(feature = "serde")]
/// # {
/// let json = serde_json::to_string(&layer.to_spec())?;
/// let spec: CellsSpec = serde_json::from_str(&json)?;
/// let copy: Cells = Cells::from_spec(&spec, Xxh3)?;
/// assert_eq!(copy.fingerprint(), layer.fingerprint());
/// # }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
// Serialized field names and order are part of the format; tests/it/cells_spec.rs locks them. A
// field added later needs `#[serde(default)]` so older specs still deserialize.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct CellsSpec {
    /// The format version: [`CellsSpec::FORMAT`] in every spec this release writes.
    ///
    /// [`Cells::from_spec`] rejects every other version, in a nested layer too. Deserializing
    /// accepts any number, so `from_spec` can report an unsupported version as such.
    pub format: u32,

    /// The layer's directory rules, which are empty when the layer selects by ring alone.
    pub directory: Directory,

    /// The spec of the layer's selector ring, whose nodes are the cells, or `None` when the layer
    /// selects by directory alone.
    pub ring_selector: Option<RingSpec>,

    /// The layer's cells.
    ///
    /// [`Cells::to_spec`] lists them in canonical order, sorted by ID. [`Cells::from_spec`]
    /// accepts them in any order.
    pub cells: Vec<CellSpec>,

    /// The fingerprint of the layer the spec describes, which [`Cells::from_spec`] recomputes
    /// and checks.
    pub fingerprint: Fingerprint,
}

impl CellsSpec {
    /// The format version this release writes, and the only one it reads.
    pub const FORMAT: u32 = 1;
}

/// One cell of a [`CellsSpec`]: the cell's ID and the spec of its placement.
///
/// The placement is a [`RingSpec`] for a cell that is a ring, or a [`CellsSpec`] for a cell that
/// is a nested layer. With the `serde` feature, a cell serializes as an object with the fields
/// `id` and `placement`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct CellSpec {
    /// The cell's ID, which directory rules and selector ring nodes name.
    pub id: CellId,

    /// The spec of the cell's placement: its ring, or its nested layer.
    pub placement: PlacementSpec,
}

/// The spec of a [`Placement`]: a ring's [`RingSpec`], or a cells layer's [`CellsSpec`].
///
/// A [`CellSpec`] holds one, since a cell is either kind of placement. Later releases may add
/// kinds of placement, so matches on this enum need a wildcard arm.
///
/// With the `serde` feature, a placement spec serializes as an object with one field, named for
/// its variant in snake case, that holds the spec: `{"ring": {...}}` or `{"cells": {...}}`.
///
/// # Examples
///
/// ```
/// use fleetring::{Cells, Directory, Node, PlacementSpec, Ring};
///
/// let ring = Ring::builder().seed(1).node(Node::new("n1".parse()?)).build()?;
/// let region = Cells::builder()
///     .directory(Directory::new().catch_all("eu-1".parse()?))
///     .cell("eu-1".parse()?, ring)
///     .build()?;
/// let fleet = Cells::builder()
///     .directory(Directory::new().catch_all("eu".parse()?))
///     .cell("eu".parse()?, region)
///     .build()?;
///
/// let spec = fleet.to_spec();
/// let PlacementSpec::Cells(eu) = &spec.cells[0].placement else {
///     return Err("cell eu is a nested layer".into());
/// };
/// assert!(matches!(eu.cells[0].placement, PlacementSpec::Ring(_)));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[non_exhaustive]
pub enum PlacementSpec {
    /// A ring's spec: [`Ring::to_spec`] writes it, and [`Ring::from_spec`] reads it.
    Ring(RingSpec),

    /// A cells layer's spec: [`Cells::to_spec`] writes it, and [`Cells::from_spec`] reads it.
    Cells(CellsSpec),
}

impl PlacementSpec {
    /// Returns the fingerprint the spec stores.
    fn fingerprint(&self) -> Fingerprint {
        match self {
            Self::Ring(ring) => ring.fingerprint,
            Self::Cells(layer) => layer.fingerprint,
        }
    }

    /// Returns the number of layers the spec nests: 0 for a ring, and for a cells layer, one
    /// more than its deepest cell.
    pub(crate) fn depth(&self) -> usize {
        match self {
            Self::Ring(_) => 0,
            Self::Cells(layer) => {
                let deepest = layer.cells.iter().map(|cell| cell.placement.depth()).max();
                deepest.unwrap_or(0) + 1
            }
        }
    }
}

/// The error returned when [`Cells::from_spec`] rejects a spec.
///
/// `from_spec` checks a layer's format, then its selector ring, then its cells in ID order,
/// checking a nested layer in full before moving on, then the layer as a whole, then its
/// fingerprint, and reports the first problem it finds. Every variant holds the
/// [`path`](Self::path) of the cell where it found the problem.
///
/// # Examples
///
/// ```
/// use fleetring::{Cells, Directory, FromCellsSpecError, Node, Ring, Xxh3};
///
/// let ring = Ring::builder().seed(1).node(Node::new("n1".parse()?)).build()?;
/// let region = Cells::builder()
///     .directory(Directory::new().catch_all("eu-1".parse()?))
///     .cell("eu-1".parse()?, ring)
///     .build()?;
/// let fleet = Cells::builder()
///     .directory(Directory::new().catch_all("eu".parse()?))
///     .cell("eu".parse()?, region)
///     .build()?;
///
/// // A spec of two layers doesn't load as one layer of rings.
/// let error = Cells::<Ring>::from_spec(&fleet.to_spec(), Xxh3)
///     .err()
///     .ok_or("the spec should be rejected")?;
/// assert_eq!(
///     error,
///     FromCellsSpecError::DepthMismatch { path: vec!["eu".parse()?], expected: 0, found: 1 }
/// );
/// assert_eq!(
///     error.to_string(),
///     "cell 'eu' is a cells layer of depth 1 in the spec, but the type expects a ring"
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FromCellsSpecError {
    /// A layer's format version isn't [`CellsSpec::FORMAT`].
    #[error(
        "cells spec{} has unsupported format {found}; the supported format is {}",
        OfCell(.path),
        CellsSpec::FORMAT
    )]
    UnsupportedFormat {
        /// The path of the cell that holds the layer, which is empty for the top layer.
        path: Vec<CellId>,
        /// The layer's format version.
        found: u32,
    },

    /// [`Ring::from_spec`] rejected a layer's selector ring spec.
    ///
    /// The [`FromSpecError`] it returned is the error's [`source`](std::error::Error::source).
    #[error("selector ring spec{} is invalid", OfCell(.path))]
    InvalidSelector {
        /// The path of the cell that holds the layer, which is empty for the top layer.
        path: Vec<CellId>,
        /// Why `Ring::from_spec` rejected the selector ring spec.
        #[source]
        source: FromSpecError,
    },

    /// A cell's spec is a ring where the cell type is a cells layer, or a cells layer where the
    /// cell type is a ring.
    ///
    /// A placement's depth is the number of layers on a key's path through it: 0 for a [`Ring`],
    /// 1 for a `Cells<Ring>`, 2 for a `Cells<Cells<Ring>>`, and so on. The spec of a `Cells<P>`
    /// must give each of its cells the depth of `P`.
    #[error(
        "cell '{}' is {} in the spec, but the type expects {}",
        CellPath(.path),
        Depth(*.found),
        Depth(*.expected)
    )]
    DepthMismatch {
        /// The path of the cell.
        path: Vec<CellId>,
        /// The depth of the cell type.
        expected: usize,
        /// The depth of the cell's spec. For a cells spec whose cells differ in depth, this
        /// counts the deepest.
        found: usize,
    },

    /// [`Ring::from_spec`] rejected a cell's ring spec.
    ///
    /// The [`FromSpecError`] it returned is the error's [`source`](std::error::Error::source).
    #[error("ring spec of cell '{}' is invalid", CellPath(.path))]
    InvalidRing {
        /// The path of the cell.
        path: Vec<CellId>,
        /// Why `Ring::from_spec` rejected the ring spec.
        #[source]
        source: FromSpecError,
    },

    /// A layer's directory, selector ring, and cells don't form a valid layer.
    ///
    /// The [`BuildCellsError`] that [`CellsBuilder::build`](crate::CellsBuilder::build) returned
    /// is the error's [`source`](std::error::Error::source). It names the cell inside the layer
    /// that is at fault, if a single cell is.
    #[error("cells spec{} describes an invalid layer", OfCell(.path))]
    InvalidLayer {
        /// The path of the cell that holds the layer, which is empty for the top layer.
        path: Vec<CellId>,
        /// Why the layer doesn't build.
        #[source]
        source: BuildCellsError,
    },

    /// The fingerprint stored in a layer's spec differs from the fingerprint of the layer it
    /// describes.
    ///
    /// Every ring and nested layer inside passed its own fingerprint check first, so the layer's
    /// own inputs changed after the spec was written, such as a directory rule or a cell ID, or
    /// the spec's writer computes layer fingerprints differently.
    #[error(
        "cells spec{} stores fingerprint {stored}, but the layer it describes has fingerprint {computed}",
        OfCell(.path)
    )]
    FingerprintMismatch {
        /// The path of the cell that holds the layer, which is empty for the top layer.
        path: Vec<CellId>,
        /// The fingerprint stored in the spec.
        stored: Fingerprint,
        /// The fingerprint of the layer built from the spec.
        computed: Fingerprint,
    },
}

impl FromCellsSpecError {
    /// Returns the path of the cell where [`Cells::from_spec`] found the problem, top layer
    /// first.
    ///
    /// For a problem with one cell's spec, its depth or its ring, this is the cell's path. For a
    /// problem with a layer, its format, selector ring, cells as a whole, or fingerprint, it is
    /// the path of the cell that holds the layer, which is empty for the top layer.
    #[must_use]
    pub fn path(&self) -> &[CellId] {
        match self {
            Self::UnsupportedFormat { path, .. }
            | Self::InvalidSelector { path, .. }
            | Self::DepthMismatch { path, .. }
            | Self::InvalidRing { path, .. }
            | Self::InvalidLayer { path, .. }
            | Self::FingerprintMismatch { path, .. } => path,
        }
    }
}

/// Displays a cell's path as its IDs joined with `/`, such as `eu/eu-3`.
struct CellPath<'a>(&'a [CellId]);

impl fmt::Display for CellPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (position, cell) in self.0.iter().enumerate() {
            if position > 0 {
                f.write_str("/")?;
            }
            f.write_str(cell.as_str())?;
        }
        Ok(())
    }
}

/// Displays where a layer sits: nothing for the top layer, or ` of cell 'eu/eu-3'` for the
/// layer that cell `eu-3` of cell `eu` holds.
struct OfCell<'a>(&'a [CellId]);

impl fmt::Display for OfCell<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }
        write!(f, " of cell '{}'", CellPath(self.0))
    }
}

/// Displays a placement of the given depth: a ring, or a cells layer of that depth.
struct Depth(usize);

impl fmt::Display for Depth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            0 => f.write_str("a ring"),
            depth => write!(f, "a cells layer of depth {depth}"),
        }
    }
}

impl<P: Placement> Cells<P> {
    /// Returns the layer's spec: a portable description that [`from_spec`](Self::from_spec)
    /// rebuilds the layer from.
    ///
    /// The spec lists the cells in canonical order, sorted by ID, each with its ring's spec or
    /// its nested layer's spec, and carries the layer's [fingerprint](Self::fingerprint). This
    /// method computes the fingerprint of every ring inside once, at any depth, and each layer's
    /// fingerprint from those.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Cells, Directory, Node, PlacementSpec, Ring};
    ///
    /// let ring = |seed: u64, id: &str| {
    ///     Ring::builder()
    ///         .seed(seed)
    ///         .node(Node::new(id.parse().expect("valid id")))
    ///         .build()
    /// };
    /// let fleet = Cells::builder()
    ///     .directory(Directory::new().prefix("eu:", "eu".parse()?).catch_all("us".parse()?))
    ///     .cell("us".parse()?, ring(2, "us-1")?)
    ///     .cell("eu".parse()?, ring(1, "eu-1")?)
    ///     .build()?;
    /// let spec = fleet.to_spec();
    /// assert_eq!(&spec.directory, fleet.directory());
    /// assert_eq!(spec.ring_selector, None);
    /// let ids: Vec<&str> = spec.cells.iter().map(|cell| cell.id.as_str()).collect();
    /// assert_eq!(ids, ["eu", "us"]);
    /// let us = fleet.cell("us").ok_or("no us cell")?;
    /// assert_eq!(spec.cells[1].placement, PlacementSpec::Ring(us.to_spec()));
    /// assert_eq!(spec.fingerprint, fleet.fingerprint());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Panics
    ///
    /// Panics in the cases where [`fingerprint`](Self::fingerprint) does.
    #[must_use]
    pub fn to_spec(&self) -> CellsSpec {
        let ring_selector = self.ring_selector().map(Ring::to_spec);
        let cells: Vec<CellSpec> = self
            .cells()
            .map(|(id, cell)| CellSpec {
                id: id.clone(),
                placement: cell.to_placement_spec(),
            })
            .collect();
        let fingerprint =
            stored_fingerprint(self.directory(), ring_selector.as_ref(), cells.iter());
        CellsSpec {
            format: CellsSpec::FORMAT,
            directory: self.directory().clone(),
            ring_selector,
            cells,
            fingerprint,
        }
    }

    /// Rebuilds a layer from its spec, giving every ring in it a clone of `hasher`.
    ///
    /// A spec records only its rings' hasher names, so the caller supplies the hasher, as for
    /// [`Ring::from_spec`]. Each layer of the spec, from the top, is checked in order. Its format
    /// must be [`CellsSpec::FORMAT`]. Its selector ring spec, then each cell's ring spec in ID
    /// order, is loaded as `Ring::from_spec` loads one, which checks the hasher's name and the
    /// ring's fingerprint; a nested layer's spec is checked in full, the same way. The layer is
    /// then built through [`Cells::builder`], which validates its directory, selector ring, and
    /// cells and accepts the cells in any order. Last, its [fingerprint](Self::fingerprint) is
    /// recomputed and compared with the stored one.
    ///
    /// The spec must nest exactly as `P` does: each cell of a `Cells<Ring>` spec must be a ring,
    /// and each cell of a `Cells<Cells<Ring>>` spec a layer whose cells are rings. A spec that
    /// nests otherwise is rejected, never misread.
    ///
    /// Rebuilding costs one build per ring and layer, and one fingerprint per ring: a layer's
    /// fingerprint check reuses the fingerprints stored for its rings and nested layers, which
    /// are checked first.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Cells, Directory, FromCellsSpecError, Node, Ring, Xxh3};
    ///
    /// let ring = Ring::builder().seed(1).node(Node::new("n1".parse()?)).build()?;
    /// let fleet = Cells::builder()
    ///     .directory(Directory::new().catch_all("main".parse()?))
    ///     .cell("main".parse()?, ring)
    ///     .build()?;
    /// let spec = fleet.to_spec();
    /// let copy: Cells = Cells::from_spec(&spec, Xxh3)?;
    /// assert_eq!(copy.replicas("tenant:acme"), fleet.replicas("tenant:acme"));
    ///
    /// // A spec whose directory changed no longer matches its fingerprint.
    /// let mut tampered = spec.clone();
    /// tampered.directory = tampered.directory.exact("tenant:acme", "main".parse()?);
    /// let error = Cells::<Ring>::from_spec(&tampered, Xxh3)
    ///     .err()
    ///     .ok_or("tampered spec accepted")?;
    /// assert!(matches!(error, FromCellsSpecError::FingerprintMismatch { .. }));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Builds nothing and returns an error if, checking each layer in this order:
    ///
    /// - the layer's format isn't [`CellsSpec::FORMAT`]
    ///   ([`FromCellsSpecError::UnsupportedFormat`]);
    /// - `Ring::from_spec` rejects the selector ring spec
    ///   ([`FromCellsSpecError::InvalidSelector`]);
    /// - in ID order, a cell's spec is a ring where `P` is a cells layer, or the reverse
    ///   ([`FromCellsSpecError::DepthMismatch`]); `Ring::from_spec` rejects a cell's ring spec
    ///   ([`FromCellsSpecError::InvalidRing`]); or a nested layer's spec fails these checks;
    /// - the directory, selector ring, and cells don't form a valid layer
    ///   ([`FromCellsSpecError::InvalidLayer`], whose source is the [`BuildCellsError`]);
    /// - the rebuilt layer's fingerprint isn't the stored one
    ///   ([`FromCellsSpecError::FingerprintMismatch`]).
    ///
    /// Each error holds the [path](FromCellsSpecError::path) of the cell where the problem is.
    ///
    /// # Panics
    ///
    /// Panics in the cases where [`fingerprint`](Self::fingerprint) does.
    pub fn from_spec(spec: &CellsSpec, hasher: P::Hasher) -> Result<Self, FromCellsSpecError>
    where
        P::Hasher: Clone,
    {
        Self::from_spec_at(spec, &hasher, &mut Vec::new())
    }

    /// Rebuilds the layer that the cell at `path` holds from `spec`, as `from_spec` describes.
    pub(crate) fn from_spec_at(
        spec: &CellsSpec,
        hasher: &P::Hasher,
        path: &mut Vec<CellId>,
    ) -> Result<Self, FromCellsSpecError>
    where
        P::Hasher: Clone,
    {
        if spec.format != CellsSpec::FORMAT {
            return Err(FromCellsSpecError::UnsupportedFormat {
                path: path.clone(),
                found: spec.format,
            });
        }
        let ring = spec
            .ring_selector
            .as_ref()
            .map(|ring| Ring::from_spec(ring, hasher.clone()))
            .transpose()
            .map_err(|source| FromCellsSpecError::InvalidSelector {
                path: path.clone(),
                source,
            })?;
        let mut specs: Vec<&CellSpec> = spec.cells.iter().collect();
        specs.sort_by(|a, b| a.id.cmp(&b.id));
        let mut cells = Vec::with_capacity(specs.len());
        for cell in &specs {
            path.push(cell.id.clone());
            let placement = P::from_placement_spec(&cell.placement, hasher, path)?;
            path.pop();
            cells.push((cell.id.clone(), placement));
        }
        let mut builder = Cells::builder()
            .directory(spec.directory.clone())
            .cells(cells);
        if let Some(ring) = ring {
            builder = builder.ring_selector(ring);
        }
        let layer = builder
            .build()
            .map_err(|source| FromCellsSpecError::InvalidLayer {
                path: path.clone(),
                source,
            })?;
        // Every ring and nested layer inside passed its fingerprint check, so the fingerprints
        // their specs store are theirs, and `build` found the cells' IDs distinct.
        let computed = stored_fingerprint(
            &spec.directory,
            spec.ring_selector.as_ref(),
            specs.iter().copied(),
        );
        if computed != spec.fingerprint {
            return Err(FromCellsSpecError::FingerprintMismatch {
                path: path.clone(),
                stored: spec.fingerprint,
                computed,
            });
        }
        Ok(layer)
    }
}

/// Computes the fingerprint of the layer with `directory`, from the fingerprints stored in the
/// specs of its selector ring and its cells, which must come sorted by ID.
fn stored_fingerprint<'c>(
    directory: &Directory,
    ring_selector: Option<&RingSpec>,
    cells: impl ExactSizeIterator<Item = &'c CellSpec>,
) -> Fingerprint {
    layer_fingerprint(
        directory,
        ring_selector.map(|ring| ring.fingerprint),
        cells.map(|cell| (&cell.id, cell.placement.fingerprint())),
    )
}
