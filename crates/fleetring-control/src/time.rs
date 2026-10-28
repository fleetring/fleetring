//! Control-node time: the stamp on each command and the spans that leases and liveness use.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A point in time stamped by the control node, in milliseconds since an epoch it chooses.
///
/// `apply` never reads a clock; it only compares these stamps, so the epoch they count from
/// (usually the Unix epoch) doesn't matter as long as one control node keeps using it.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Millis, Timestamp};
///
/// let start = Timestamp::from_millis(1_000);
/// assert_eq!(start.saturating_add(Millis::new(500)).as_millis(), 1_500);
/// assert!(Timestamp::from_millis(u64::MAX).saturating_add(Millis::new(1)) == Timestamp::MAX);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The earliest timestamp.
    pub const ZERO: Self = Self(0);

    /// The latest timestamp.
    pub const MAX: Self = Self(u64::MAX);

    /// Returns the timestamp `millis` milliseconds after the control node's epoch.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Returns the timestamp as milliseconds since the control node's epoch.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Returns the timestamp `span` later, stopping at [`Timestamp::MAX`].
    #[must_use]
    pub const fn saturating_add(self, span: Millis) -> Self {
        Self(self.0.saturating_add(span.0))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}

/// A span of time in whole milliseconds.
///
/// # Examples
///
/// ```
/// use fleetring_control::Millis;
///
/// assert_eq!(Millis::new(250).get(), 250);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Millis(u64);

impl Millis {
    /// Returns a span of `millis` milliseconds.
    #[must_use]
    pub const fn new(millis: u64) -> Self {
        Self(millis)
    }

    /// Returns the span in milliseconds.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the sum of two spans, stopping at `u64::MAX` milliseconds.
    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

impl fmt::Display for Millis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}
