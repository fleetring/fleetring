//! The bounds on what delegation adds to control state, and how `apply` counts against them
//! (ADR-0047, points 5.10, 6.2, and 6.3).
//!
//! Every count here is a function of the state and the command's times that ignores entries
//! already past their `kept_until` or out of their window, so a limit never depends on whether a
//! `Tick` has pruned them yet.

use crate::{Millis, PrincipalName, State, Timestamp};

use super::record::RateKey;

/// The most live delegations one root lineage holds at once (ADR-0047, point 5.10). A
/// delegation revoked by ID frees its slot at once, since the revocation prunes its record.
pub const MAX_LIVE_PER_LINEAGE: usize = 256;

/// The most delegations one caller principal records in a minute (ADR-0047, point 5.10).
pub const CALLER_RATE: usize = 30;

/// The most delegations the delegated agents of one root lineage record in a minute, never
/// counting the root agent's own (ADR-0047, point 5.10).
pub const LINEAGE_RATE: usize = 60;

/// The span a rate window counts commands over: one minute.
pub const RATE_WINDOW: Millis = Millis::new(60_000);

/// The backstop: the most unpruned delegation records one root lineage holds, above the about
/// 6,000 the rates allow (ADR-0047, point 6.3). It never refuses while pruning and the rates
/// work as specified.
pub const MAX_RECORDS_PER_LINEAGE: usize = 8_192;

/// What a record is kept for beyond its token's life: 30 seconds of node leeway and a 1-hour
/// margin against a leader's clock stepping forward (ADR-0047, point 6.1).
pub const RECORD_MARGIN: Millis = Millis::new(30_000 + 3_600_000);

/// How long an operation outlives its delegation's record (ADR-0047, point 6.2).
pub const OPERATION_MARGIN: Millis = Millis::new(3_600_000);

/// How long a tombstone lasts from the revocation that wrote it (ADR-0047, point 6.2).
pub const TOMBSTONE_LIFE: Millis = Millis::new(3_600_000);

/// How far past the longest `kept_until` of the records a revocation prunes its entry is kept,
/// so that no token it refused can revive once the entry is forgotten (ADR-0047, point 11.5).
pub const REVOCATION_MARGIN: Millis = Millis::new(30_000);

/// The most denylist entries the ancestor rule keeps per root lineage in each of its two
/// budgets: the entries the root agent adds, and the entries its delegated agents add between
/// them. Neither counts the root's own principal entry or its revocations of root tokens it
/// presented (ADR-0047, point 11.6, as amended 2026-11-13).
pub const MAX_ANCESTOR_ENTRIES_PER_LINEAGE: usize = 1_024;

/// The most revocations one caller principal makes under the ancestor rule in a minute
/// (ADR-0047, point 11.6).
pub const ANCESTOR_CALLER_RATE: usize = 30;

/// The most revocations the delegated agents of one root lineage make under the ancestor rule
/// in a minute, never counting the root agent's own (ADR-0047, point 11.6).
pub const ANCESTOR_LINEAGE_RATE: usize = 60;

/// How long a root agent's principal entry is kept from its revocation: 30 days, the longest a
/// root token lives, plus its 30 seconds of leeway and the 1-hour margin (ADR-0047, point 11.5).
/// A principal revocation with `fleet` keeps its entry as long, since the authority's tokens of
/// any principal live as long.
pub const ROOT_PRINCIPAL_LIFE: Millis = Millis::new(30 * 24 * 3_600_000 + 30_000 + 3_600_000);

/// The records of one root lineage at a command's times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LineageCount {
    /// The live ones: kept, and expiring after the command's stamp.
    pub(crate) live: usize,
    /// The kept ones, live or not: what the backstop counts.
    pub(crate) kept: usize,
}

/// Counts the records of the lineage rooted at `root` for a command stamped `at` that runs at
/// `now`.
pub(crate) fn lineage_count(
    state: &State,
    root: &PrincipalName,
    at: Timestamp,
    now: Timestamp,
) -> LineageCount {
    state
        .delegations
        .values()
        .filter(|record| record.is_kept(now) && record.principal().is_at_or_below(root))
        .fold(LineageCount { live: 0, kept: 0 }, |count, record| {
            LineageCount {
                live: count.live + usize::from(record.claims.expires_at > at),
                kept: count.kept + 1,
            }
        })
}

/// Whether a time `counted` in a rate window still counts at `now`.
pub(crate) fn in_window(counted: Timestamp, now: Timestamp) -> bool {
    counted.saturating_add(RATE_WINDOW) > now
}

/// How many commands the window `key` counts at `now`.
pub(crate) fn window_count(state: &State, key: &RateKey, now: Timestamp) -> usize {
    state.rate_windows.get(key).map_or(0, |times| {
        times
            .iter()
            .filter(|counted| in_window(**counted, now))
            .count()
    })
}
