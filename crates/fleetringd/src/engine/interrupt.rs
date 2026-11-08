//! Stopping a running statement from outside the database's thread (FR-078).
//!
//! An [`Interrupt`] is a flag shared between the caller of a statement and the engine running it.
//! The caller raises it, at its deadline or when it goes away; the engine checks it while the
//! statement runs and stops the statement once it is raised. The flag knows nothing of time:
//! whoever raises it keeps the clock, so an engine never reads one.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::engine::EngineError;

/// Not raised.
const CLEAR: u8 = 0;
/// Raised by [`Interruption::Deadline`].
const DEADLINE: u8 = 1;
/// Raised by [`Interruption::Cancelled`].
const CANCELLED: u8 = 2;

/// Why a statement was interrupted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Interruption {
    /// It ran past its deadline.
    Deadline,
    /// Its caller went away before it finished: the call was cancelled.
    Cancelled,
}

impl Interruption {
    fn code(self) -> u8 {
        match self {
            Self::Deadline => DEADLINE,
            Self::Cancelled => CANCELLED,
        }
    }
}

impl fmt::Display for Interruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Deadline => "it ran past its deadline",
            Self::Cancelled => "its call was cancelled",
        })
    }
}

/// A flag that stops a statement while it runs, once raised.
///
/// Clones share the flag. It is raised at most once: the first [`Interruption`] stays, so a
/// deadline that passes as the caller goes away still reads as the deadline. A fresh interrupt is
/// never raised until someone raises it.
///
/// # Examples
///
/// ```
/// use fleetringd::engine::{Interrupt, Interruption};
///
/// let interrupt = Interrupt::new();
/// let engine_side = interrupt.clone();
/// assert_eq!(engine_side.raised(), None);
/// interrupt.raise(Interruption::Deadline);
/// interrupt.raise(Interruption::Cancelled);
/// assert_eq!(engine_side.raised(), Some(Interruption::Deadline));
/// ```
#[derive(Debug, Clone, Default)]
pub struct Interrupt {
    state: Arc<AtomicU8>,
}

impl Interrupt {
    /// An interrupt that isn't raised.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Raises the interrupt for `why`, unless it is already raised.
    pub fn raise(&self, why: Interruption) {
        // A failed exchange means it was raised already, and the first reason stays.
        let _ =
            self.state
                .compare_exchange(CLEAR, why.code(), Ordering::Release, Ordering::Relaxed);
    }

    /// Why the interrupt was raised, or `None` if it hasn't been.
    #[must_use]
    pub fn raised(&self) -> Option<Interruption> {
        match self.state.load(Ordering::Acquire) {
            DEADLINE => Some(Interruption::Deadline),
            CANCELLED => Some(Interruption::Cancelled),
            _ => None,
        }
    }

    /// Fails if the interrupt is raised: engines call it before they start a call, and wherever
    /// else they can stop.
    ///
    /// # Errors
    ///
    /// [`EngineError::Interrupted`], with the reason, if the interrupt is raised.
    pub fn check(&self) -> Result<(), EngineError> {
        match self.raised() {
            Some(why) => Err(EngineError::Interrupted(why)),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_reason_stays() {
        let interrupt = Interrupt::new();
        assert_eq!(interrupt.raised(), None);
        interrupt.raise(Interruption::Cancelled);
        interrupt.raise(Interruption::Deadline);
        assert_eq!(interrupt.raised(), Some(Interruption::Cancelled));
    }

    #[test]
    fn clones_share_the_flag() {
        let interrupt = Interrupt::new();
        let clone = interrupt.clone();
        clone.raise(Interruption::Deadline);
        assert_eq!(interrupt.raised(), Some(Interruption::Deadline));
    }
}
