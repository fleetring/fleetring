//! The control node's clock: where command stamps come from (ADR-0017, ADR-0020).
//!
//! Daemon code never reads the wall clock directly. The control node takes time from a [`Clock`]
//! it is given: [`SystemClock`] in production, and [`SimClock`], which follows `tokio::time`, under
//! simulation and in tests.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fleetring_control::{Millis, Timestamp};

/// A source of command stamps, in milliseconds.
///
/// The control node stamps each command with `now()` before it logs it. A clock may go backwards;
/// the state machine treats an earlier stamp as the last accepted time.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Returns the current time.
    fn now(&self) -> Timestamp;
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now(&self) -> Timestamp {
        (**self).now()
    }
}

impl<C: Clock + ?Sized> Clock for Box<C> {
    fn now(&self) -> Timestamp {
        (**self).now()
    }
}

/// Wall-clock time in milliseconds since the Unix epoch.
///
/// This is the only place in the daemon that reads the system clock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SystemClock;

impl SystemClock {
    /// Returns the system clock.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        #[expect(
            clippy::disallowed_methods,
            reason = "the production clock is the one place that reads wall-clock time"
        )]
        let now = SystemTime::now();
        // A clock set before 1970 reads as the epoch; the state machine never lets time go back.
        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        Timestamp::from_millis(whole_millis(since_epoch))
    }
}

/// Time that follows `tokio::time`, counted from a base timestamp.
///
/// Under turmoil, `tokio::time` is the simulated clock, so stamps are deterministic. In a Tokio
/// test with paused time, the clock moves only when the test advances time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimClock {
    base: Timestamp,
    start: tokio::time::Instant,
}

impl SimClock {
    /// Returns a clock that reads `base` now and advances with `tokio::time`.
    #[must_use]
    pub fn new(base: Timestamp) -> Self {
        Self {
            base,
            start: tokio::time::Instant::now(),
        }
    }
}

impl Clock for SimClock {
    fn now(&self) -> Timestamp {
        self.base
            .saturating_add(Millis::new(whole_millis(self.start.elapsed())))
    }
}

fn whole_millis(span: Duration) -> u64 {
    u64::try_from(span.as_millis()).unwrap_or(u64::MAX)
}
