//! Ring specs: the portable description of a ring, its canonical encoding, and its fingerprint
//! (ADR-0008).
//!
//! # Canonical encoding
//!
//! The canonical encoding is the byte string of a ring's placement inputs, and the first part of
//! what the fingerprint hashes. It is crate-private; placement spec v1 (FR-014) defines it for
//! ports.
//!
//! Every integer is little-endian. A *string* is its byte length as a `u32`, then its UTF-8
//! bytes. An *optional label* is a `u8` flag, 0 when absent and 1 when present, followed by the
//! label as a string when present. The fields, in order:
//!
//! 1. format: `u32`, always 1.
//! 2. strategy: a `u8` tag, 0 for rendezvous or 1 for vnodes. The vnode tag is followed by its
//!    `tokens_per_weight` as a `u32`.
//! 3. seed: `u64`.
//! 4. replication factor: `u8`.
//! 5. hasher: the hasher's algorithm name, as a string.
//! 6. node count: `u32`.
//! 7. the nodes, in canonical order (sorted by ID, comparing bytes), each as:
//!    1. ID: string;
//!    2. weight: `u32`;
//!    3. zone: optional label, absent for the default zone;
//!    4. host: optional label, absent when the node is its own host.
//!
//! The epoch is left out, because epochs never affect placement.
//!
//! # Fingerprint
//!
//! The fingerprint is XXH3-64 with seed 0 over the canonical encoding followed by the probe
//! section. It always uses XXH3 and seed 0, whatever the ring's own hasher and seed.
//!
//! The probe section covers 64 probe keys, `fleetring-probe-00` through `fleetring-probe-63`,
//! in that order: the UTF-8 bytes of `format!("fleetring-probe-{i:02}")` for `i` in `0..64`. For
//! each key it holds the key's replica set at the ring's replication factor: the number of
//! replicas as a `u32`, then each replica's canonical index (its position in `Ring::nodes`) as a
//! `u32`, primary first. Hashing actual placements means a hasher or port that places any probe
//! key differently changes the fingerprint, even when every input matches.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;

use crate::{
    BuildRingError, Epoch, Host, Node, NodeId, ReplicationFactor, Ring, StableHasher, Strategy,
    VnodeParams, Weight, Xxh3, Zone,
};

/// The number of probe keys whose placements the fingerprint covers.
const PROBE_COUNT: u32 = 64;

/// The XXH3-64 seed of the fingerprint, which never depends on the ring's own seed.
const FINGERPRINT_SEED: u64 = 0;

/// The canonical encoding's tag for [`Strategy::Rendezvous`].
const TAG_RENDEZVOUS: u8 = 0;

/// The canonical encoding's tag for [`Strategy::Vnodes`].
const TAG_VNODES: u8 = 1;

/// The number of hex digits in a fingerprint's text form.
const FINGERPRINT_DIGITS: usize = 16;

/// A portable, versioned description of a [`Ring`]: everything needed to rebuild it exactly.
///
/// [`Ring::to_spec`] writes a spec, and [`Ring::from_spec`] rebuilds the ring from one. Specs are
/// how rings travel: the control plane stores them and ships them to routers and clients. With
/// the `serde` feature, a spec serializes to JSON or another self-describing format, and
/// deserializing ignores unknown fields.
///
/// The [`fingerprint`](Self::fingerprint) protects the spec. It covers every field that affects
/// placement and the placements themselves, and `from_spec` recomputes it, so a spec that changed
/// after it was written, or that is loaded with a different hasher, is rejected rather than
/// silently placing keys elsewhere. That is also why ignoring unknown fields is safe. The
/// [`epoch`](Self::epoch) is the one field the fingerprint leaves out, because it never affects
/// placement. [`Fingerprint`] describes how peers compare rings.
///
/// The fields are public, so a spec can be inspected and stored like any record. The struct is
/// `#[non_exhaustive]` so later formats can add fields: get a spec from `to_spec` or by
/// deserializing one, not from a struct literal.
///
/// # Examples
///
/// ```
/// use fleetring::{Node, Ring, RingSpec, Xxh3};
///
/// let ring = Ring::builder()
///     .seed(7)
///     .node(Node::new("laptop-1".parse()?).with_zone("home".parse()?))
///     .node(Node::new("nas".parse()?).with_zone("garage".parse()?))
///     .build()?;
/// let spec = ring.to_spec();
/// assert_eq!(spec.format, RingSpec::FORMAT);
/// assert_eq!(spec.hasher, "xxh3-64");
/// assert_eq!(spec.seed, 7);
///
/// let copy = Ring::from_spec(&spec, Xxh3)?;
/// assert_eq!(copy.replicas("agent:42"), ring.replicas("agent:42"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// With the `serde` feature, a spec round-trips through JSON:
///
/// ```
/// # use fleetring::{Node, Ring, RingSpec, Xxh3};
/// # let ring = Ring::builder().node(Node::new("nas".parse()?)).build()?;
/// # #[cfg(feature = "serde")]
/// # {
/// let json = serde_json::to_string(&ring.to_spec())?;
/// let spec: RingSpec = serde_json::from_str(&json)?;
/// let copy = Ring::from_spec(&spec, Xxh3)?;
/// assert_eq!(copy.fingerprint(), ring.fingerprint());
/// # }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
// Serialized field names and order are part of the format; tests/fixtures/ring-spec-v1.json
// locks them. A field added later needs `#[serde(default)]` so older specs still deserialize.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct RingSpec {
    /// The format version: [`RingSpec::FORMAT`] in every spec this release writes.
    ///
    /// [`Ring::from_spec`] rejects every other version. Deserializing accepts any number, so
    /// `from_spec` can report an unsupported version as such.
    pub format: u32,

    /// The ring's version, assigned by the control plane.
    ///
    /// The fingerprint leaves the epoch out, because epochs never affect placement.
    pub epoch: Epoch,

    /// The [algorithm name](StableHasher::algorithm) of the ring's hasher, such as `"xxh3-64"`.
    pub hasher: String,

    /// The seed for every hash the ring computes.
    pub seed: u64,

    /// The placement strategy and its parameters.
    pub strategy: StrategySpec,

    /// How many replicas each key gets, counting the primary.
    pub replication_factor: ReplicationFactor,

    /// The ring's nodes.
    ///
    /// [`Ring::to_spec`] lists them in canonical order, sorted by ID. [`Ring::from_spec`] accepts
    /// them in any order.
    pub nodes: Vec<NodeSpec>,

    /// The fingerprint of the ring the spec describes, which [`Ring::from_spec`] recomputes and
    /// checks.
    pub fingerprint: Fingerprint,
}

impl RingSpec {
    /// The format version this release writes, and the only one it reads.
    pub const FORMAT: u32 = 1;
}

/// One node of a [`RingSpec`]: its identity, weight, and topology labels.
///
/// A node spec holds the same data as a [`Node`], but its fields, and their serialized names,
/// belong to the spec format. Convert with `NodeSpec::from(&node)` and `Node::from(spec)`.
///
/// # Examples
///
/// ```
/// use fleetring::{Node, NodeSpec};
///
/// let node = Node::new("nas".parse()?).with_zone("garage".parse()?);
/// let spec = NodeSpec::from(&node);
/// assert_eq!(spec.id.as_str(), "nas");
/// assert_eq!(spec.host, None);
/// assert_eq!(Node::from(spec), node);
/// # Ok::<(), fleetring::ParseLabelError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct NodeSpec {
    /// The node's stable identity.
    pub id: NodeId,

    /// The node's relative capacity.
    pub weight: Weight,

    /// The node's zone, or `None` for the default zone.
    pub zone: Option<Zone>,

    /// The node's host label, or `None` if the node is its own host.
    pub host: Option<Host>,
}

impl From<&Node> for NodeSpec {
    fn from(node: &Node) -> Self {
        Self {
            id: node.id().clone(),
            weight: node.weight(),
            zone: node.zone().cloned(),
            host: node.host().cloned(),
        }
    }
}

impl From<NodeSpec> for Node {
    fn from(spec: NodeSpec) -> Self {
        let NodeSpec {
            id,
            weight,
            zone,
            host,
        } = spec;
        let mut node = Node::new(id).with_weight(weight);
        if let Some(zone) = zone {
            node = node.with_zone(zone);
        }
        if let Some(host) = host {
            node = node.with_host(host);
        }
        node
    }
}

/// The placement strategy of a [`RingSpec`], with its parameters.
///
/// Each variant mirrors a [`Strategy`] variant; convert between the two with `From`. Later
/// releases may add strategies, so matches on this enum need a wildcard arm.
///
/// With the `serde` feature, a strategy serializes as an object whose `kind` field names the
/// variant in snake case, alongside the variant's parameters: `{"kind": "rendezvous"}`.
///
/// # Examples
///
/// ```
/// use fleetring::{Strategy, StrategySpec};
///
/// assert_eq!(StrategySpec::from(Strategy::Rendezvous), StrategySpec::Rendezvous);
/// assert_eq!(Strategy::from(StrategySpec::Rendezvous), Strategy::Rendezvous);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "kind", rename_all = "snake_case"))]
#[non_exhaustive]
pub enum StrategySpec {
    /// Weighted rendezvous hashing: [`Strategy::Rendezvous`].
    Rendezvous,

    /// A weighted virtual-node ring: [`Strategy::Vnodes`].
    Vnodes {
        /// Tokens per unit of node weight.
        tokens_per_weight: NonZeroU32,
    },
}

impl From<Strategy> for StrategySpec {
    fn from(strategy: Strategy) -> Self {
        match strategy {
            Strategy::Rendezvous => Self::Rendezvous,
            Strategy::Vnodes(params) => Self::Vnodes {
                tokens_per_weight: params.tokens_per_weight(),
            },
        }
    }
}

impl From<StrategySpec> for Strategy {
    fn from(spec: StrategySpec) -> Self {
        match spec {
            StrategySpec::Rendezvous => Self::Rendezvous,
            StrategySpec::Vnodes { tokens_per_weight } => {
                Self::Vnodes(VnodeParams::new(tokens_per_weight))
            }
        }
    }
}

/// A 64-bit digest that identifies a ring's placement function.
///
/// [`Ring::fingerprint`] computes it, and [`RingSpec`] stores it. It covers every input that
/// decides placement: the hasher's algorithm name, the seed, the strategy and its parameters, the
/// replication factor, and every node's ID, weight, zone, and host. It also covers the actual
/// replica sets of 64 fixed probe keys, so a hasher or port that places any of them differently
/// changes the fingerprint, even when every input matches. It leaves out the [`Epoch`], which
/// never affects placement.
///
/// Peers compare rings by exchanging `(epoch, fingerprint)` pairs. A lower epoch means a stale
/// ring. The same epoch with a different fingerprint is a bug or a misconfiguration, such as a
/// wrong seed or hasher, and must fail loudly rather than route keys.
///
/// A fingerprint displays as exactly 16 lowercase hex digits, and parses from that form only.
///
/// # Examples
///
/// ```
/// use fleetring::{Epoch, Fingerprint, Node, Ring};
///
/// /// Fails if a peer at this ring's epoch runs a different placement function.
/// fn check_peer(ring: &Ring, epoch: Epoch, fingerprint: Fingerprint) -> Result<(), String> {
///     let ours = ring.fingerprint();
///     if epoch == ring.epoch() && fingerprint != ours {
///         return Err(format!("epoch {epoch}: peer runs {fingerprint}, this node runs {ours}"));
///     }
///     Ok(())
/// }
///
/// let ring = Ring::builder()
///     .epoch(Epoch::new(7))
///     .node(Node::new("nas".parse()?))
///     .build()?;
/// // Peers announce fingerprints as text.
/// let announced: Fingerprint = ring.fingerprint().to_string().parse()?;
/// check_peer(&ring, Epoch::new(7), announced)?;
///
/// let misconfigured = ring.to_builder().seed(1).build()?;
/// assert!(check_peer(&ring, Epoch::new(7), misconfigured.fingerprint()).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(u64);

impl Fingerprint {
    /// Returns the fingerprint whose value is `value`, such as one read back from storage.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fingerprint as an integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Fingerprint {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Fingerprint> for u64 {
    fn from(fingerprint: Fingerprint) -> Self {
        fingerprint.0
    }
}

impl fmt::Display for Fingerprint {
    /// Formats the fingerprint as exactly 16 lowercase hex digits.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for Fingerprint {
    type Err = ParseFingerprintError;

    /// Parses exactly 16 lowercase hex digits, the form `Display` writes.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != FINGERPRINT_DIGITS {
            return Err(ParseFingerprintError {
                reason: Reason::Length { len: text.len() },
            });
        }
        text.bytes()
            .enumerate()
            .try_fold(0u64, |value, (index, byte)| {
                let digit = match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => {
                        return Err(ParseFingerprintError {
                            reason: Reason::InvalidByte { byte, index },
                        });
                    }
                };
                Ok((value << 4) | u64::from(digit))
            })
            .map(Self)
    }
}

/// The error returned when a string is not a valid [`Fingerprint`].
///
/// A fingerprint's text form is exactly 16 lowercase hex digits: `0`–`9` and `a`–`f`.
///
/// # Examples
///
/// ```
/// use fleetring::Fingerprint;
///
/// let error = "0123456789ABCDEF".parse::<Fingerprint>().err().ok_or("uppercase accepted")?;
/// assert_eq!(
///     error.to_string(),
///     "fingerprint has invalid byte 'A' at index 10; allowed are 0-9 and a-f"
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFingerprintError {
    reason: Reason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reason {
    Length { len: usize },
    InvalidByte { byte: u8, index: usize },
}

impl fmt::Display for ParseFingerprintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.reason {
            Reason::Length { len } => write!(
                f,
                "fingerprint is {len} bytes long; it must be {FINGERPRINT_DIGITS} hex digits"
            ),
            Reason::InvalidByte { byte, index } => write!(
                f,
                "fingerprint has invalid byte '{}' at index {index}; allowed are 0-9 and a-f",
                byte.escape_ascii()
            ),
        }
    }
}

impl std::error::Error for ParseFingerprintError {}

/// The error returned when [`Ring::from_spec`] rejects a spec.
///
/// `from_spec` checks the format, then the hasher, then the ring itself, then the fingerprint,
/// and reports the first problem it finds.
///
/// # Examples
///
/// ```
/// use fleetring::{FromSpecError, Node, Ring, Xxh3};
///
/// let ring = Ring::builder().node(Node::new("nas".parse()?)).build()?;
/// let mut spec = ring.to_spec();
/// spec.format = 2;
/// let error = Ring::from_spec(&spec, Xxh3).err().ok_or("the spec should be rejected")?;
/// assert_eq!(error, FromSpecError::UnsupportedFormat { found: 2 });
/// assert_eq!(
///     error.to_string(),
///     "ring spec format 2 is not supported; the supported format is 1"
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FromSpecError {
    /// The spec's format version isn't [`RingSpec::FORMAT`].
    #[error(
        "ring spec format {found} is not supported; the supported format is {}",
        RingSpec::FORMAT
    )]
    UnsupportedFormat {
        /// The spec's format version.
        found: u32,
    },

    /// The spec names a different hasher algorithm from the hasher passed to `from_spec`.
    #[error("ring spec uses hasher '{spec}', but the given hasher is '{hasher}'")]
    HasherMismatch {
        /// The algorithm name the spec records.
        spec: String,
        /// The algorithm name of the hasher passed to `from_spec`.
        hasher: String,
    },

    /// The fingerprint stored in the spec differs from the fingerprint of the ring it describes.
    ///
    /// Either the spec changed after it was written, or this process places keys differently
    /// from the one that wrote it, for example with a hasher that shares the algorithm name but
    /// not the output.
    #[error(
        "ring spec fingerprint is {stored}, but the ring it describes has fingerprint {computed}"
    )]
    FingerprintMismatch {
        /// The fingerprint stored in the spec.
        stored: Fingerprint,
        /// The fingerprint of the ring built from the spec.
        computed: Fingerprint,
    },

    /// The spec's settings and nodes don't form a valid ring.
    ///
    /// The [`BuildRingError`] that [`RingBuilder::build`](crate::RingBuilder::build) returned is
    /// the error's [`source`](std::error::Error::source).
    #[error("ring spec describes an invalid ring")]
    InvalidRing(#[source] BuildRingError),
}

impl<H: StableHasher> Ring<H> {
    /// Returns the ring's spec: a portable description that [`from_spec`](Self::from_spec)
    /// rebuilds the ring from.
    ///
    /// The spec lists the nodes in canonical order and carries the ring's
    /// [`fingerprint`](Self::fingerprint), which this method computes.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Node, Ring, StrategySpec};
    ///
    /// let ring = Ring::builder()
    ///     .seed(7)
    ///     .node(Node::new("nas".parse()?).with_zone("garage".parse()?))
    ///     .node(Node::new("laptop-1".parse()?))
    ///     .build()?;
    /// let spec = ring.to_spec();
    /// assert_eq!(spec.seed, 7);
    /// assert_eq!(spec.strategy, StrategySpec::Rendezvous);
    /// let ids: Vec<&str> = spec.nodes.iter().map(|node| node.id.as_str()).collect();
    /// assert_eq!(ids, ["laptop-1", "nas"]);
    /// assert_eq!(spec.fingerprint, ring.fingerprint());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Panics
    ///
    /// Panics in the same case as [`fingerprint`](Self::fingerprint).
    #[must_use]
    pub fn to_spec(&self) -> RingSpec {
        RingSpec {
            format: RingSpec::FORMAT,
            epoch: self.epoch(),
            hasher: self.hasher().algorithm().to_owned(),
            seed: self.seed(),
            strategy: StrategySpec::from(self.strategy()),
            replication_factor: self.replication_factor(),
            nodes: self.nodes().iter().map(NodeSpec::from).collect(),
            fingerprint: self.fingerprint(),
        }
    }

    /// Computes the ring's fingerprint, which identifies its placement function.
    ///
    /// [`Fingerprint`] describes what it covers and how peers use it. It is always XXH3-64 with
    /// seed 0, whatever the ring's own hasher and seed, so rings with different hashers can be
    /// compared.
    ///
    /// The fingerprint is computed on every call, not cached. A call costs one canonical
    /// encoding of the ring plus 64 probe lookups, about as much as 64 calls to
    /// [`replicas`](Self::replicas), so compute it once per ring and keep it.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{Epoch, Node, Ring};
    ///
    /// let ring = Ring::builder().node(Node::new("nas".parse()?)).build()?;
    /// // Epochs never affect placement, so they never affect the fingerprint.
    /// let next = ring.to_builder().epoch(Epoch::new(1)).build()?;
    /// assert_eq!(next.fingerprint(), ring.fingerprint());
    /// // A new seed places keys differently.
    /// let reseeded = ring.to_builder().seed(1).build()?;
    /// assert_ne!(reseeded.fingerprint(), ring.fingerprint());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the hasher's [algorithm name](StableHasher::algorithm) is longer than
    /// `u32::MAX` bytes, which the fingerprint's encoding can't represent.
    #[must_use]
    pub fn fingerprint(&self) -> Fingerprint {
        let mut encoder = Encoder::default();
        self.encode_canonical(&mut encoder);
        self.encode_probes(&mut encoder);
        Fingerprint(Xxh3.hash(&encoder.bytes, FINGERPRINT_SEED))
    }

    /// Rebuilds a ring from its spec, placing keys with `hasher`.
    ///
    /// A spec records only its hasher's algorithm name, so the caller supplies the hasher, and
    /// `from_spec` checks that the names match. It then builds the ring through
    /// [`Ring::builder`], which validates the settings and nodes and accepts the nodes in any
    /// order. Last, it recomputes the [fingerprint](Self::fingerprint) and compares it with the
    /// stored one. That check confirms the new ring has the placement inputs of the ring that
    /// wrote the spec and places every probe key the same way: it fails if an input changed after
    /// the spec was written, or if `hasher` shares the spec's algorithm name but places a probe
    /// key differently.
    ///
    /// The ring takes the spec's epoch unchecked, because the fingerprint leaves epochs out.
    /// Rebuilding costs one [`build`](crate::RingBuilder::build) and one fingerprint.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::{FromSpecError, Node, Ring, Xxh3};
    ///
    /// let ring = Ring::builder()
    ///     .node(Node::new("laptop-1".parse()?))
    ///     .node(Node::new("nas".parse()?))
    ///     .build()?;
    /// let spec = ring.to_spec();
    /// let copy = Ring::from_spec(&spec, Xxh3)?;
    /// assert_eq!(copy.replicas("agent:42"), ring.replicas("agent:42"));
    ///
    /// // A spec whose placement inputs changed no longer matches its fingerprint.
    /// let mut tampered = spec.clone();
    /// tampered.seed = 1;
    /// let error = Ring::from_spec(&tampered, Xxh3).err().ok_or("tampered spec accepted")?;
    /// assert!(matches!(error, FromSpecError::FingerprintMismatch { .. }));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Builds nothing and returns an error if, checking in this order:
    ///
    /// - the spec's format isn't [`RingSpec::FORMAT`] ([`FromSpecError::UnsupportedFormat`]);
    /// - `hasher`'s algorithm name isn't the one the spec records
    ///   ([`FromSpecError::HasherMismatch`]);
    /// - the settings and nodes don't form a valid ring ([`FromSpecError::InvalidRing`], whose
    ///   source is the [`BuildRingError`]);
    /// - the rebuilt ring's fingerprint isn't the stored one
    ///   ([`FromSpecError::FingerprintMismatch`]).
    ///
    /// # Panics
    ///
    /// Panics in the same case as [`fingerprint`](Self::fingerprint).
    pub fn from_spec(spec: &RingSpec, hasher: H) -> Result<Self, FromSpecError> {
        if spec.format != RingSpec::FORMAT {
            return Err(FromSpecError::UnsupportedFormat { found: spec.format });
        }
        let algorithm = hasher.algorithm();
        if algorithm != spec.hasher {
            return Err(FromSpecError::HasherMismatch {
                spec: spec.hasher.clone(),
                hasher: algorithm.to_owned(),
            });
        }
        let ring = Ring::builder()
            .hasher(hasher)
            .seed(spec.seed)
            .strategy(Strategy::from(spec.strategy))
            .replication_factor(spec.replication_factor)
            .epoch(spec.epoch)
            .nodes(spec.nodes.iter().cloned().map(Node::from))
            .build()
            .map_err(FromSpecError::InvalidRing)?;
        let computed = ring.fingerprint();
        if computed != spec.fingerprint {
            return Err(FromSpecError::FingerprintMismatch {
                stored: spec.fingerprint,
                computed,
            });
        }
        Ok(ring)
    }

    /// Appends the ring's canonical encoding, as the module docs define it.
    fn encode_canonical(&self, out: &mut Encoder) {
        out.put_u32(RingSpec::FORMAT);
        match self.strategy() {
            Strategy::Rendezvous => out.put_u8(TAG_RENDEZVOUS),
            Strategy::Vnodes(params) => {
                out.put_u8(TAG_VNODES);
                out.put_u32(params.tokens_per_weight().get());
            }
        }
        out.put_u64(self.seed());
        out.put_u8(self.replication_factor().get());
        out.put_str(self.hasher().algorithm());
        out.put_usize(self.node_count());
        for node in self.nodes() {
            out.put_str(node.id().as_str());
            out.put_u32(node.weight().get());
            out.put_label(node.zone().map(Zone::as_str));
            out.put_label(node.host().map(Host::as_str));
        }
    }

    /// Appends the probe section, as the module docs define it.
    fn encode_probes(&self, out: &mut Encoder) {
        let nodes = self.nodes();
        for probe in 0..PROBE_COUNT {
            let replicas = self.replicas(format!("fleetring-probe-{probe:02}"));
            out.put_usize(replicas.len());
            for replica in &replicas {
                let index = nodes
                    .binary_search_by(|node| node.id().cmp(replica.id()))
                    .expect("every replica is a node of the ring");
                out.put_usize(index);
            }
        }
    }
}

/// Writes the integers, strings, and optional labels of the canonical encoding.
#[derive(Debug, Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn put_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    /// Writes a length, count, or index as a `u32`.
    fn put_usize(&mut self, value: usize) {
        let value = u32::try_from(value).unwrap_or_else(|_| {
            panic!("{value} exceeds u32::MAX, the largest length or index the encoding holds")
        });
        self.put_u32(value);
    }

    /// Writes `text` as its byte length, then its UTF-8 bytes.
    fn put_str(&mut self, text: &str) {
        self.put_usize(text.len());
        self.bytes.extend_from_slice(text.as_bytes());
    }

    /// Writes flag 0 for an absent label, or flag 1 and then the label.
    fn put_label(&mut self, label: Option<&str>) {
        match label {
            None => self.put_u8(0),
            Some(label) => {
                self.put_u8(1);
                self.put_str(label);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TableHasher;

    fn node(id: &str) -> Node {
        Node::new(id.parse().expect("test ids are valid"))
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn canonical<H: StableHasher>(ring: &Ring<H>) -> String {
        let mut encoder = Encoder::default();
        ring.encode_canonical(&mut encoder);
        hex(&encoder.bytes)
    }

    fn probes<H: StableHasher>(ring: &Ring<H>) -> String {
        let mut encoder = Encoder::default();
        ring.encode_probes(&mut encoder);
        hex(&encoder.bytes)
    }

    /// One node, `a`, with every setting at its default.
    fn one_node() -> Ring {
        Ring::builder().node(node("a")).build().expect("valid ring")
    }

    /// The canonical encoding of [`one_node`].
    const ONE_NODE: &str = concat!(
        "01000000",         // format 1
        "00",               // strategy: rendezvous
        "0000000000000000", // seed 0
        "03",               // replication factor 3
        "07000000",         // hasher name: 7 bytes,
        "787868332d3634",   // "xxh3-64"
        "01000000",         // 1 node
        "01000000",         // id: 1 byte,
        "61",               // "a"
        "01000000",         // weight 1
        "00",               // zone: the default zone
        "00",               // host: its own
    );

    /// The fingerprint of [`one_node`]: XXH3-64, seed 0, of [`ONE_NODE`] followed by 64 probes
    /// that each place the key on node 0 alone. Cross-checked with `xxhsum -H3` from xxHash
    /// 0.8.4, the C reference implementation.
    const ONE_NODE_FINGERPRINT: Fingerprint = Fingerprint(0x872e_343c_5916_f7e2);

    /// Two nodes with labels, a weight above 255, and a seed whose bytes all differ, so the
    /// encoding shows byte order.
    fn two_nodes() -> Ring {
        Ring::builder()
            .seed(0x0102_0304_0506_0708)
            .replication_factor(ReplicationFactor::new(2).expect("2 is nonzero"))
            .epoch(Epoch::new(99))
            .node(
                node("b")
                    .with_weight(Weight::new(258).expect("258 is nonzero"))
                    .with_zone("z1".parse().expect("valid zone"))
                    .with_host("h1".parse().expect("valid host")),
            )
            .node(node("a").with_host("h2".parse().expect("valid host")))
            .build()
            .expect("valid ring")
    }

    /// The canonical encoding of [`two_nodes`].
    const TWO_NODES: &str = concat!(
        "01000000",         // format 1
        "00",               // strategy: rendezvous
        "0807060504030201", // seed 0x0102030405060708
        "02",               // replication factor 2
        "07000000",         // hasher name: 7 bytes,
        "787868332d3634",   // "xxh3-64"
        "02000000",         // 2 nodes, in canonical order
        "01000000",         // id: 1 byte,
        "61",               // "a"
        "01000000",         // weight 1
        "00",               // zone: the default zone
        "01",               // host: present,
        "02000000",         // 2 bytes,
        "6832",             // "h2"
        "01000000",         // id: 1 byte,
        "62",               // "b"
        "02010000",         // weight 258
        "01",               // zone: present,
        "02000000",         // 2 bytes,
        "7a31",             // "z1"
        "01",               // host: present,
        "02000000",         // 2 bytes,
        "6831",             // "h1"
    );

    #[test]
    fn a_one_node_ring_encodes_every_field_in_order() {
        assert_eq!(canonical(&one_node()), ONE_NODE);
    }

    #[test]
    fn a_two_node_ring_encodes_labels_weights_and_settings_little_endian() {
        assert_eq!(canonical(&two_nodes()), TWO_NODES);
    }

    #[test]
    fn the_epoch_is_not_encoded() {
        let ring = two_nodes();
        let later = ring
            .to_builder()
            .epoch(Epoch::new(u64::MAX))
            .build()
            .expect("valid ring");
        assert_eq!(canonical(&later), canonical(&ring));
        assert_eq!(probes(&later), probes(&ring));
    }

    #[test]
    fn a_one_node_ring_hashes_its_encoding_and_64_single_replica_probes() {
        let ring = one_node();
        // Each probe: one replica, then canonical index 0.
        assert_eq!(probes(&ring), "0100000000000000".repeat(64));
        assert_eq!(ring.fingerprint(), ONE_NODE_FINGERPRINT);
    }

    #[test]
    fn probes_list_the_replica_count_then_canonical_indices_primary_first() {
        // Two equal nodes at replication factor 3: every probe holds both nodes, in either order.
        let ring = Ring::builder()
            .nodes(["a", "b"].map(node))
            .build()
            .expect("valid ring");
        let a_first = "020000000000000001000000";
        let b_first = "020000000100000000000000";
        let section = probes(&ring);
        assert_eq!(section.len(), 64 * a_first.len());
        let entries: Vec<&str> = (0..64)
            .map(|probe| &section[probe * a_first.len()..(probe + 1) * a_first.len()])
            .collect();
        for (probe, entry) in entries.iter().enumerate() {
            let key = format!("fleetring-probe-{probe:02}");
            let want = match ring.primary(&key).id().as_str() {
                "a" => a_first,
                _ => b_first,
            };
            assert_eq!(*entry, want, "{key}");
        }
        assert!(
            entries.contains(&a_first) && entries.contains(&b_first),
            "64 probes put each of two equal nodes first at least once"
        );
    }

    #[test]
    fn the_probes_are_keys_00_to_63() {
        let nodes = ["a", "b", "c", "d"].map(node);
        let build = |hasher: TableHasher| {
            Ring::builder()
                .hasher(hasher)
                .nodes(nodes.clone())
                .build()
                .expect("valid ring")
        };
        let base = build(TableHasher::new());
        for key in [
            "fleetring-probe-64",
            "fleetring-probe-0",
            "fleetring-probe-100",
            "agent:1",
        ] {
            let other = build(TableHasher::new().with(key, 0));
            assert_eq!(other.fingerprint(), base.fingerprint(), "{key} is no probe");
        }
        for probe in [0, 63] {
            let key = format!("fleetring-probe-{probe:02}");
            let moved = (0..64)
                .map(|hash| build(TableHasher::new().with(&key, hash)))
                .find(|ring| ring.replicas(&key) != base.replicas(&key))
                .expect("some hash moves the probe key");
            assert_ne!(moved.fingerprint(), base.fingerprint(), "{key} is a probe");
        }
    }
}
