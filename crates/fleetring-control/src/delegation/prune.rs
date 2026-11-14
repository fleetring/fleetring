//! Forgetting delegation state: expired records, operations, tombstones, and rate-window times,
//! and the records a revocation ends (ADR-0047, points 6.2 and 11.4).
//!
//! `RecordDelegation`, `RevokeToken`, and `Tick` prune in their own `apply`, after every check
//! has passed, so a refused command changes nothing.

use crate::{Added, Revoked, State, Timestamp};

use super::limits::{TOMBSTONE_LIFE, in_window};
use super::record::OperationOutcome;

/// Forgets every record and operation kept until `now` or earlier, every rate-window time out of
/// its window at `now`, and every window left empty.
pub(crate) fn forget_through(state: &mut State, now: Timestamp) {
    state.delegations.retain(|_, record| record.is_kept(now));
    state
        .delegation_operations
        .retain(|_, operation| operation.is_kept(now));
    state.rate_windows.retain(|_, times| {
        times.retain(|counted| in_window(*counted, now));
        !times.is_empty()
    });
}

/// Prunes every record still kept at `now` that `revoked` ends, and tombstones each one's
/// operation for [`TOMBSTONE_LIFE`] from `now`; returns the longest `kept_until` among them, if
/// any (ADR-0047, point 11.4).
///
/// `Revoked::Token(t)` ends every record whose ID or chain names `t`, and the local fleet's
/// `Revoked::Principal` for `Q` every record whose principal is at or below `Q`: a record's
/// `iat` is an earlier command's effective time, so always within the revocation's `before`.
/// `Revoked::Key` ends none: it ends copies, not delegations.
pub(crate) fn prune_revoked(
    state: &mut State,
    revoked: &Revoked,
    now: Timestamp,
) -> Option<Timestamp> {
    let ended: Vec<_> = state
        .delegations
        .iter()
        .filter(|(_, record)| {
            record.is_kept(now)
                && match revoked {
                    Revoked::Token(token) => record.descends_from(token),
                    Revoked::Principal {
                        fleet: None,
                        principal,
                    } => record.principal().is_at_or_below(principal),
                    Revoked::Principal { fleet: Some(_), .. } | Revoked::Key(_) => false,
                }
        })
        .map(|(id, _)| id.clone())
        .collect();
    let mut longest = None;
    for id in ended {
        let Some(record) = state.delegations.remove(&id) else {
            continue;
        };
        longest = longest.max(Some(record.kept_until));
        if let Some(operation) = state
            .delegation_operations
            .get_mut(&record.operation)
            .filter(|operation| operation.delegation == id)
        {
            operation.outcome = OperationOutcome::Revoked { at: now };
            operation.kept_until = now.saturating_add(TOMBSTONE_LIFE);
        }
    }
    longest
}

/// Whether a [`Tick`](crate::Action::Tick) at `now`, or at [`State::now`] if `now` is earlier,
/// would forget a delegation record, an operation, a tombstone, or a denylist entry the ancestor
/// rule added (ADR-0047, point 6.2).
///
/// `RecordDelegation` and `RevokeToken` prune in their own `apply`, but an idle lineage sends
/// neither, so the control node's timer asks this and proposes a `Tick` when it says so. Rate
/// windows don't need one: each holds at most its limit's number of times, and every command
/// that adds one also records a delegation whose own expiry brings a `Tick`.
///
/// # Examples
///
/// ```
/// use fleetring_control::{delegation, ControlConfig, State, Timestamp};
///
/// let state = State::new(ControlConfig::new());
/// assert!(!delegation::tick_due(&state, Timestamp::MAX));
/// ```
#[must_use]
pub fn tick_due(state: &State, now: Timestamp) -> bool {
    let now = now.max(state.now());
    state
        .delegations
        .values()
        .any(|record| !record.is_kept(now))
        || state
            .delegation_operations
            .values()
            .any(|operation| !operation.is_kept(now))
        || state
            .revocations
            .iter()
            .any(|(_, entry)| matches!(entry.added, Added::Ancestor { .. }) && entry.until <= now)
}
