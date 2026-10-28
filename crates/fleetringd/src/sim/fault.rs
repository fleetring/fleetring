//! The faults a test can inject into a [`Cluster`](crate::sim::Cluster).

use std::fmt;
use std::time::Duration;

/// A fault, or the repair of one, between hosts or on one host.
///
/// Link faults name two hosts and act in both directions.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fault {
    /// Drops every message between `a` and `b`. Open connections stall rather than fail.
    Partition {
        /// One side.
        a: String,
        /// The other side.
        b: String,
    },
    /// Ends a partition between `a` and `b`.
    Repair {
        /// One side.
        a: String,
        /// The other side.
        b: String,
    },
    /// Delays every message between `a` and `b` by `latency`.
    Latency {
        /// One side.
        a: String,
        /// The other side.
        b: String,
        /// The delay.
        latency: Duration,
    },
    /// Breaks the link between `a` and `b` at random, dropping what is in flight.
    Loss {
        /// One side.
        a: String,
        /// The other side.
        b: String,
        /// How often the link breaks.
        rate: LossRate,
    },
    /// Kills a host's daemon at once, as a power loss would. Its memory is lost; its data
    /// directory survives. Its peers get no reset: their connections to it stall until they time
    /// out.
    Crash {
        /// The host.
        host: String,
    },
    /// Shuts a host's daemon down gracefully, as a signal would.
    Stop {
        /// The host.
        host: String,
    },
    /// Starts a crashed or stopped host's daemon again, on the same data directory.
    Restart {
        /// The host.
        host: String,
    },
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Partition { a, b } => write!(f, "partition {a} {b}"),
            Self::Repair { a, b } => write!(f, "repair {a} {b}"),
            Self::Latency { a, b, latency } => {
                write!(f, "latency {a} {b} {}ms", latency.as_millis())
            }
            Self::Loss { a, b, rate } => write!(f, "loss {a} {b} {rate}"),
            Self::Crash { host } => write!(f, "crash {host}"),
            Self::Stop { host } => write!(f, "stop {host}"),
            Self::Restart { host } => write!(f, "restart {host}"),
        }
    }
}

/// How often a lossy link breaks, in thousandths per simulation tick.
///
/// turmoil breaks a link with this probability on each tick, dropping every message in flight on
/// it, and repairs it on the next. TCP has no retransmission in turmoil, so a break stalls the
/// connections that lose a segment, as a real partition can.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LossRate(u16);

impl LossRate {
    /// No loss.
    pub const NONE: Self = Self(0);

    /// A rate of `per_mille` thousandths, capped at 1000.
    #[must_use]
    pub const fn per_mille(per_mille: u16) -> Self {
        if per_mille > 1000 {
            Self(1000)
        } else {
            Self(per_mille)
        }
    }

    /// Returns the rate in thousandths.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    /// The probability turmoil takes.
    pub(crate) fn probability(self) -> f64 {
        f64::from(self.0) / 1000.0
    }
}

impl fmt::Display for LossRate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}‰", self.0)
    }
}
