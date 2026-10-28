//! Ring versions assigned by the control plane (ADR-0001, ADR-0008).

use std::fmt;

/// The version of a ring, assigned by the control plane.
///
/// Epochs only grow within a cluster, so a peer whose ring has a lower epoch knows its ring is
/// stale. Epochs never affect placement: two rings that differ only in epoch place every key
/// identically.
///
/// # Examples
///
/// ```
/// use fleetring::Epoch;
///
/// assert!(Epoch::new(7) > Epoch::default());
/// assert_eq!(u64::from(Epoch::new(7)), 7);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(u64);

impl Epoch {
    /// Returns the epoch numbered `value`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the epoch as an integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Epoch {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Epoch> for u64 {
    fn from(epoch: Epoch) -> Self {
        epoch.0
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
