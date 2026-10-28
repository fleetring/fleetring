//! Replica sets and replication factors (ADR-0006).

use std::fmt;
use std::num::NonZeroU8;

/// How many replicas each key gets, counting the primary.
///
/// The default is 3. A ring with fewer eligible nodes than this still places every key,
/// on as many nodes as it can.
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
///
/// assert_eq!(ReplicationFactor::default().get(), 3);
/// assert_eq!(ReplicationFactor::new(5).map(ReplicationFactor::get), Some(5));
/// assert_eq!(ReplicationFactor::new(0), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicationFactor(NonZeroU8);

impl ReplicationFactor {
    /// A single copy: the primary only.
    pub const ONE: Self = Self(NonZeroU8::MIN);

    const DEFAULT: Self = Self(NonZeroU8::new(3).expect("3 is nonzero"));

    /// Returns a replication factor of `value`, or `None` if `value` is zero.
    #[must_use]
    pub const fn new(value: u8) -> Option<Self> {
        match NonZeroU8::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the replication factor as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0.get()
    }
}

impl Default for ReplicationFactor {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<NonZeroU8> for ReplicationFactor {
    fn from(value: NonZeroU8) -> Self {
        Self(value)
    }
}

impl From<ReplicationFactor> for u8 {
    fn from(factor: ReplicationFactor) -> Self {
        factor.get()
    }
}

impl fmt::Display for ReplicationFactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
