//! Stable hashing for placement (ADR-0002).

/// A 64-bit hash function whose output never changes.
///
/// Every placement decision starts with a hash, and every process that computes placement must
/// agree on it across restarts, machines, architectures, Rust releases, `fleetring` releases,
/// and languages. An implementation must therefore be a fully specified function of its
/// configuration, the input bytes, and the seed. It must not depend on per-process state such
/// as random seeds, on byte order, or on pointer width.
///
/// This trait is deliberately not [`std::hash::Hasher`]. That interface is fed through
/// [`std::hash::Hash`] implementations, which the standard library documents as not portable
/// across platforms or compiler versions.
///
/// [`Xxh3`] is the default. Implement this trait to place keys with another algorithm, for
/// example to match a legacy system's placement, and pin the implementation's output with test
/// vectors: changing it moves data.
///
/// # Examples
///
/// ```
/// use fleetring::StableHasher;
///
/// /// FNV-1a: stable, but its weak mixing makes it a poor choice for placement.
/// #[derive(Debug, Clone, Copy, Default)]
/// struct Fnv1a;
///
/// impl StableHasher for Fnv1a {
///     fn algorithm(&self) -> &'static str {
///         "fnv1a-64"
///     }
///
///     fn hash(&self, bytes: &[u8], seed: u64) -> u64 {
///         let mut hash = 0xcbf2_9ce4_8422_2325 ^ seed;
///         for &byte in bytes {
///             hash ^= u64::from(byte);
///             hash = hash.wrapping_mul(0x0100_0000_01b3);
///         }
///         hash
///     }
/// }
///
/// assert_eq!(Fnv1a.hash(b"", 0), 0xcbf2_9ce4_8422_2325);
/// ```
pub trait StableHasher {
    /// Returns the algorithm's stable name, such as `"xxh3-64"`.
    ///
    /// Ring specs record this name so that a ring written with one algorithm is never loaded
    /// with another. Two hashers that report the same name must produce identical output.
    fn algorithm(&self) -> &'static str;

    /// Hashes `bytes` under `seed`.
    ///
    /// The result must depend only on the hasher's configuration, `bytes`, and `seed`.
    fn hash(&self, bytes: &[u8], seed: u64) -> u64;
}

impl<H: StableHasher + ?Sized> StableHasher for &H {
    #[inline]
    fn algorithm(&self) -> &'static str {
        (**self).algorithm()
    }

    #[inline]
    fn hash(&self, bytes: &[u8], seed: u64) -> u64 {
        (**self).hash(bytes, seed)
    }
}

impl<H: StableHasher + ?Sized> StableHasher for Box<H> {
    #[inline]
    fn algorithm(&self) -> &'static str {
        (**self).algorithm()
    }

    #[inline]
    fn hash(&self, bytes: &[u8], seed: u64) -> u64 {
        (**self).hash(bytes, seed)
    }
}

/// XXH3-64, the default placement hasher.
///
/// Computes `XXH3_64bits_withSeed` from xxHash. The XXH3 output format has been frozen since
/// xxHash 0.8.0, implementations exist for every major language, and it is fast on the short
/// inputs placement hashes: keys, node IDs, and 16-byte draws.
///
/// # Examples
///
/// ```
/// use fleetring::{StableHasher, Xxh3};
///
/// assert_eq!(Xxh3.algorithm(), "xxh3-64");
/// assert_eq!(Xxh3.hash(b"", 0), 0x2d06_8005_38d3_94c2);
/// assert_ne!(Xxh3.hash(b"agent:sam", 0), Xxh3.hash(b"agent:sam", 1));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Xxh3;

impl StableHasher for Xxh3 {
    #[inline]
    fn algorithm(&self) -> &'static str {
        "xxh3-64"
    }

    #[inline]
    fn hash(&self, bytes: &[u8], seed: u64) -> u64 {
        twox_hash::XxHash3_64::oneshot_with_seed(seed, bytes)
    }
}
