//! The fork-reset write's marker, as engines record it inside a fork (ADR-0046, point 6).

use crate::engine::Position;

/// The marker a fork's reset leaves inside the database, in the same transaction as the reset
/// (ADR-0046, point 6.1).
///
/// It names the fork it was written for: the fork's incarnation and the fork operation, which
/// together are its *identity* here, the database's name being implied by the copy. A reset finds
/// its own marker by that identity alone. The source's incarnation and the position the fork was
/// seeded at come along for provenance. Every field is engine-neutral; how an engine stores the
/// marker is its own business.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkMarker {
    /// The fork's own incarnation.
    pub fork_incarnation: u64,
    /// The fork operation's ID, as its 16 bytes.
    pub operation: [u8; 16],
    /// The incarnation of the database the fork was seeded from.
    pub source_incarnation: u64,
    /// The source's position the fork was seeded at, as the engine encoded it.
    pub at: Position,
}

impl ForkMarker {
    /// Whether `other` was written for the same fork: the same fork incarnation and operation.
    #[must_use]
    pub fn same_fork(&self, other: &Self) -> bool {
        self.fork_incarnation == other.fork_incarnation && self.operation == other.operation
    }
}

/// What [`Database::fork_reset`](crate::engine::Database::fork_reset) did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ForkReset {
    /// The database already held this fork's marker, so the reset had run and committed before:
    /// nothing was written.
    AlreadyDone,
    /// The reset ran and committed.
    Written {
        /// The position just past the reset's transaction, which holds the marker.
        position: Position,
    },
}
