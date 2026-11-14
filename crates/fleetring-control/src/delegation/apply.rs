//! How `apply` records a delegation (ADR-0047, points 5.7 to 5.10 and 6.1 to 6.3).
//!
//! Every check is a function of the state and the command's two times, the stamp `at` and the
//! effective time `now`, that ignores entries past their `kept_until` or out of their window.
//! Only once the last check has passed does `apply` prune and insert, so a refused command
//! leaves the state exactly as it was.

use fleetring::NodeId;

use crate::fork::OperationId;
use crate::scope::{MAX_SCOPES, Scopes};
use crate::{Applied, KeyId, Millis, Outcome, PrincipalName, Rejection, State, Timestamp};

use super::limits::{
    CALLER_RATE, LINEAGE_RATE, MAX_LIVE_PER_LINEAGE, MAX_RECORDS_PER_LINEAGE, OPERATION_MARGIN,
    RECORD_MARGIN, lineage_count, window_count,
};
use super::prune::forget_through;
use super::record::{
    CallerToken, CanonicalRequest, ClaimField, Delegation, DelegationOperation, Forgery, Limit,
    OperationOutcome, RateKey, RateKind, RateScope, RequestField, Resent, StructureViolation,
};
use super::{
    AgentName, ChainLink, DelegationId, ParseAgentNameError, SignerKind, TokenClaims, TokenKind,
};

/// One second, the grain of every recorded time (the plan's principle P4).
const SECOND: u64 = 1_000;

/// What a `RecordDelegation` carries, as [`Action::RecordDelegation`](crate::Action) defines it.
#[derive(Debug)]
pub(crate) struct Proposed {
    pub(crate) operation: OperationId,
    pub(crate) requested_by: PrincipalName,
    pub(crate) request: CanonicalRequest,
    pub(crate) caller: CallerToken,
    pub(crate) id: DelegationId,
    pub(crate) scopes: Scopes,
    pub(crate) not_before: Timestamp,
    pub(crate) expires_at: Timestamp,
}

/// The command's two times: its stamp, and the time it runs at, never earlier than the state's.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Times {
    pub(crate) at: Timestamp,
    pub(crate) now: Timestamp,
}

/// Applies `RecordDelegation`, logged by `recorded_by` (ADR-0047, point 5.9).
pub(crate) fn record_delegation(
    state: &mut State,
    times: Times,
    recorded_by: Option<NodeId>,
    proposed: Proposed,
) -> Outcome {
    let Some(recorded_by) = recorded_by else {
        return Err(Rejection::Unstamped);
    };
    if proposed.scopes.len() > MAX_SCOPES {
        return Err(Rejection::DelegationStructure {
            reason: StructureViolation::TooManyScopes,
        });
    }
    let asked = Resend {
        operation: proposed.operation,
        requested_by: &proposed.requested_by,
        request: &proposed.request,
        caller_key: &proposed.caller.kid,
    };
    if let Some(resent) = resend(state, times, &asked) {
        let (id, record) = resent?;
        return Ok(Applied::DelegationRecorded {
            id: id.clone(),
            claims: record.claims.clone(),
            resent: Resent::Yes,
        });
    }

    let checked = check(state, times, &proposed)?;

    forget_through(state, times.now);
    insert(state, times, recorded_by, proposed, checked)
}

/// What a resend is judged by: the operation, who asks, what they ask, and the key that signed
/// the copy of their token they present.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Resend<'a> {
    pub(crate) operation: OperationId,
    pub(crate) requested_by: &'a PrincipalName,
    pub(crate) request: &'a CanonicalRequest,
    pub(crate) caller_key: &'a KeyId,
}

/// Answers `asked` if it resends an operation still kept: its live record with its ID, or why
/// the resend is refused; `None` if the operation is new (ADR-0047, points 5.7 and 5.8).
///
/// A request of more than [`MAX_SCOPES`] scopes is refused before anything normalizes them.
pub(crate) fn resend<'s>(
    state: &'s State,
    times: Times,
    asked: &Resend<'_>,
) -> Option<Result<(&'s DelegationId, &'s Delegation), Rejection>> {
    if asked
        .request
        .scopes
        .as_ref()
        .is_some_and(|scopes| scopes.len() > MAX_SCOPES)
    {
        return Some(Err(Rejection::DelegationStructure {
            reason: StructureViolation::TooManyScopes,
        }));
    }
    let operation = state
        .delegation_operations
        .get(&asked.operation)
        .filter(|operation| operation.is_kept(times.now))?;
    Some(answer_resend(state, times, operation, asked))
}

/// The record a resend of the kept `operation` returns, or why it is refused.
fn answer_resend<'s>(
    state: &'s State,
    times: Times,
    operation: &DelegationOperation,
    asked: &Resend<'_>,
) -> Result<(&'s DelegationId, &'s Delegation), Rejection> {
    let id = asked.operation;
    if operation.requested_by != *asked.requested_by {
        return Err(Rejection::OperationNotYours { operation: id });
    }
    if let Some(field) = operation.request.first_difference(asked.request) {
        return Err(Rejection::DelegationConflict {
            operation: id,
            field,
        });
    }
    match operation.outcome {
        OperationOutcome::Recorded => {}
        OperationOutcome::Revoked { .. } => {
            return Err(Rejection::DelegationRevoked { operation: id });
        }
    }
    let (delegation, record) = state
        .delegations
        .get_key_value(&operation.delegation)
        .filter(|(_, record)| record.operation == id && record.is_live(times.at, times.now))
        .ok_or(Rejection::DelegationExpired { operation: id })?;
    // The record's last link names the copy of the parent's token it was minted from. Only that
    // copy receives the child again, so every descendant of a copy another key signed names that
    // key (ADR-0047, point 8, amendment of 2026-11-13).
    if record
        .claims
        .chain
        .last()
        .is_none_or(|link| link.kid != *asked.caller_key)
    {
        return Err(Rejection::DelegationConflict {
            operation: id,
            field: RequestField::CallerKey,
        });
    }
    // Every copy, whatever issuer key signs it: the denylist's one walk, less a copy's key.
    if state
        .revocations
        .refuses_every_copy(&record.claims)
        .is_some()
    {
        return Err(Rejection::DelegationRevoked { operation: id });
    }
    Ok((delegation, record))
}

/// What the checks resolved, for the insertion.
#[derive(Debug)]
struct Checked {
    claims: TokenClaims,
    caller: AgentName,
}

/// Every check of a new delegation, in the order of the plan: its structure, its parent's
/// record, the caller's revocations, forged lineage, the limits, and the ID.
fn check(state: &State, times: Times, proposed: &Proposed) -> Result<Checked, Rejection> {
    let structure = |reason| Rejection::DelegationStructure { reason };
    let caller = &proposed.caller;
    let presented = &caller.claims;

    // Structure, whoever proposed it.
    if proposed.requested_by != presented.principal {
        return Err(structure(StructureViolation::RequesterNotCaller));
    }
    let caller_name = AgentName::try_from(presented.principal.clone())
        .map_err(|_| structure(StructureViolation::CallerNotAgent))?;
    if presented.kind != Some(TokenKind::Agent) {
        return Err(structure(StructureViolation::CallerKindNotAgent));
    }
    let Some(owner) = presented.owner.clone() else {
        return Err(structure(StructureViolation::NoOwner));
    };
    if proposed.request.caller_token != presented.id {
        return Err(structure(StructureViolation::CallerTokenDiffers));
    }
    let child = caller_name
        .child(&proposed.request.segment)
        .map_err(|error| match error {
            ParseAgentNameError::TooDeep { .. } => Rejection::DelegationLimit {
                limit: Limit::Depth,
            },
            ParseAgentNameError::Principal(_)
            | ParseAgentNameError::NotAgent { .. }
            | ParseAgentNameError::Segment { .. }
            | ParseAgentNameError::InvalidSegment => structure(StructureViolation::Segment),
        })?;
    if !is_whole_second(proposed.not_before) || !is_whole_second(proposed.expires_at) {
        return Err(structure(StructureViolation::NotWholeSeconds));
    }
    if proposed.scopes != proposed.scopes.normalized() {
        return Err(Rejection::NonCanonicalScopes);
    }
    let issued_at = floor_to_second(times.now);
    let mut chain = presented.chain.clone();
    chain.push(ChainLink {
        id: presented.id.clone(),
        kid: caller.kid.clone(),
    });
    let claims = TokenClaims {
        fleet: presented.fleet.clone(),
        principal: child.as_principal().clone(),
        id: proposed.id.as_token_id().clone(),
        issued_at: Some(issued_at),
        not_before: proposed.not_before.max(issued_at),
        expires_at: proposed.expires_at,
        scopes: proposed.scopes.clone(),
        kind: Some(TokenKind::Agent),
        owner: Some(owner),
        parent: Some(presented.principal.clone()),
        chain,
    };
    claims
        .check_recordable()
        .map_err(|reason| Rejection::DelegationClaims { reason })?;
    if claims.expires_at > presented.expires_at {
        return Err(structure(StructureViolation::ExpiresAfterCaller));
    }

    // Against a delegated parent's record.
    let signer = SignerKind::for_principal(&presented.principal);
    let parent_record = match signer {
        SignerKind::Authority => None,
        SignerKind::Issuer => {
            let forged = |reason| Rejection::ForgedLineage { reason };
            let record = DelegationId::try_from(presented.id.clone())
                .ok()
                .and_then(|id| state.delegations.get(&id))
                .filter(|record| record.is_kept(times.now))
                .ok_or(forged(Forgery::Unrecorded))?;
            if !record.is_live(times.at, times.now) {
                return Err(forged(Forgery::NotLive));
            }
            if claims.owner != record.claims.owner {
                return Err(structure(StructureViolation::OwnerDiffers));
            }
            let parent_link = ChainLink {
                id: record.claims.id.clone(),
                kid: caller.kid.clone(),
            };
            let extends = claims.chain.split_last().is_some_and(|(last, rest)| {
                *last == parent_link && rest == record.claims.chain.as_slice()
            });
            if !extends {
                return Err(structure(StructureViolation::ChainDiffers));
            }
            record
                .claims
                .scopes
                .grant_step_strict(&claims.scopes)
                .map_err(|reason| Rejection::NotNarrowed { reason })?;
            if claims.expires_at > record.claims.expires_at {
                return Err(structure(StructureViolation::ExpiresAfterParent));
            }
            Some(record)
        }
    };

    // Revocation of the caller: its own ID and key, every ID and key in its chain, and its
    // principal or any ancestor's, with the leeway for a root caller's.
    if let Some(by) = state
        .revocations
        .refuses_token(&presented.token_ref(&caller.kid, signer))
    {
        return Err(Rejection::CallerRevoked { by });
    }

    // Forged lineage: the presented claims are the recorded ones, field by field.
    if let Some(field) =
        parent_record.and_then(|record| ClaimField::first_difference(&record.claims, presented))
    {
        return Err(Rejection::ForgedLineage {
            reason: Forgery::Differs { field },
        });
    }

    // The child's own name revoked in the second its `iat` falls in: every node would refuse
    // the token, so the proposer retries a second later (ADR-0047, points 5.9 and 11.5).
    if state
        .revocations
        .principal_refusing(&claims.principal, claims.issued_at, SignerKind::Issuer)
        .is_some()
    {
        return Err(Rejection::RevokedThisSecond { principal: child });
    }

    // Limits.
    let root = caller_name.root();
    let limit = |limit| Rejection::DelegationLimit { limit };
    let count = lineage_count(state, root.as_principal(), times.at, times.now);
    if count.live >= MAX_LIVE_PER_LINEAGE {
        return Err(limit(Limit::Live));
    }
    if window_count(state, &caller_window(&caller_name), times.now) >= CALLER_RATE {
        return Err(limit(Limit::CallerRate));
    }
    if caller_name.is_delegated()
        && window_count(state, &lineage_window(&root), times.now) >= LINEAGE_RATE
    {
        return Err(limit(Limit::LineageRate));
    }
    if count.kept >= MAX_RECORDS_PER_LINEAGE {
        return Err(limit(Limit::Backstop));
    }

    if state
        .delegations
        .get(&proposed.id)
        .is_some_and(|record| record.is_kept(times.now))
    {
        return Err(Rejection::DelegationIdTaken {
            id: proposed.id.clone(),
        });
    }
    Ok(Checked {
        claims,
        caller: caller_name,
    })
}

/// Records the checked delegation, its operation, and its effective time in the caller's rate
/// window and, for a delegated caller, its lineage's.
fn insert(
    state: &mut State,
    times: Times,
    recorded_by: NodeId,
    proposed: Proposed,
    checked: Checked,
) -> Outcome {
    let Checked { claims, caller } = checked;
    let Proposed {
        operation,
        requested_by,
        mut request,
        caller: presented,
        id,
        ..
    } = proposed;
    let life = Millis::new(
        claims
            .expires_at
            .as_millis()
            .saturating_sub(times.at.as_millis()),
    );
    let kept_until = times.now.saturating_add(life).saturating_add(RECORD_MARGIN);
    request.scopes = request.scopes.as_ref().map(Scopes::normalized);
    state.delegation_operations.insert(
        operation,
        DelegationOperation {
            requested_by: requested_by.clone(),
            request,
            delegation: id.clone(),
            outcome: OperationOutcome::Recorded,
            kept_until: kept_until.saturating_add(OPERATION_MARGIN),
        },
    );
    state.delegations.insert(
        id.clone(),
        Delegation {
            claims: claims.clone(),
            parent_token: presented.claims.id,
            operation,
            requested_by,
            recorded_by,
            recorded_at: times.now,
            kept_until,
        },
    );
    count(state, caller_window(&caller), times.now);
    if caller.is_delegated() {
        count(state, lineage_window(&caller.root()), times.now);
    }
    Ok(Applied::DelegationRecorded {
        id,
        claims,
        resent: Resent::No,
    })
}

/// Adds `now` to the window `key`.
fn count(state: &mut State, key: RateKey, now: Timestamp) {
    state.rate_windows.entry(key).or_default().push_back(now);
}

/// The window that counts `caller`'s delegations.
fn caller_window(caller: &AgentName) -> RateKey {
    RateKey {
        kind: RateKind::Delegation,
        scope: RateScope::Caller(caller.as_principal().clone()),
    }
}

/// The window that counts the delegations of the delegated agents below `root`.
fn lineage_window(root: &AgentName) -> RateKey {
    RateKey {
        kind: RateKind::Delegation,
        scope: RateScope::Lineage(root.clone()),
    }
}

fn is_whole_second(time: Timestamp) -> bool {
    time.as_millis().is_multiple_of(SECOND)
}

fn floor_to_second(time: Timestamp) -> Timestamp {
    Timestamp::from_millis(time.as_millis() - time.as_millis() % SECOND)
}
