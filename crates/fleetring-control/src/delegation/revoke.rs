//! How `apply` revokes a principal with `fleet`, and a token or a principal under the ancestor
//! rule (ADR-0047, point 11).
//!
//! As for a delegation, every check is a function of the state and the command's two times that
//! ignores entries past their `kept_until`, their `until`, or their window. Only once the last
//! check has passed does `apply` prune the records the revocation ends, replace the entries it
//! covers, and add its own, so a refused command leaves the state exactly as it was.

use std::collections::BTreeMap;

use crate::{
    Added, Applied, Entry, Millis, Outcome, PrincipalName, Rejection, Revoked, State, Timestamp,
    TokenId,
};

use super::apply::Times;
use super::limits::{
    ANCESTOR_CALLER_RATE, ANCESTOR_LINEAGE_RATE, MAX_ANCESTOR_ENTRIES_PER_LINEAGE, RECORD_MARGIN,
    REVOCATION_MARGIN, ROOT_PRINCIPAL_LIFE, window_count,
};
use super::prune::{forget_through, prune_revoked};
use super::record::{
    AncestorTarget, AncestorViolation, CallerToken, ClaimField, Forgery, RateKey, RateKind,
    RateScope, RevocationLimit,
};
use super::{AgentName, DelegationId, TokenClaims};

/// Applies `RevokePrincipal`, a principal revocation with `fleet` (ADR-0047, points 11.3 to
/// 11.5).
///
/// Its `before` is the effective time. Its entry is kept [`ROOT_PRINCIPAL_LIFE`] from then, the
/// longest any authority-signed token it may refuse lives, and at least 30 seconds past the
/// longest `kept_until` among the records it prunes and the longest `until` among the entries it
/// replaces. An entry added with `fleet` is never replaced, and no cap or rate counts it.
pub(crate) fn revoke_principal(
    state: &mut State,
    now: Timestamp,
    principal: PrincipalName,
) -> Outcome {
    let revoked = Revoked::principal(principal);
    let replaced = replaced_by(state, &revoked, now);
    let until = now.saturating_add(ROOT_PRINCIPAL_LIFE);
    let entry = Entry {
        until,
        added: Added::Fleet,
        before: Some(now),
        of: None,
    };
    let entry = record(state, now, revoked.clone(), entry, &replaced);
    Ok(Applied::PrincipalRevoked {
        revoked,
        before: now,
        until: entry.until,
    })
}

/// Applies `RevokeAsAncestor`: `caller`'s revocation of `target` under the ancestor rule
/// (ADR-0047, points 11.3 to 11.7).
pub(crate) fn revoke_as_ancestor(
    state: &mut State,
    times: Times,
    target: AncestorTarget,
    caller: CallerToken,
) -> Outcome {
    let Checked {
        caller: by,
        plan,
        rated,
    } = check(state, times, target, &caller)?;
    let now = times.now;
    let applied = match plan {
        Plan::One(planned) => match land(state, times, &caller, &by, planned) {
            (revoked @ Revoked::Principal { .. }, until) => Applied::PrincipalRevoked {
                revoked,
                before: now,
                until,
            },
            (revoked @ (Revoked::Token(_) | Revoked::Key(_)), until) => {
                Applied::TokenRevoked { revoked, until }
            }
        },
        Plan::Pair { principal, token } => {
            let (principal, until) = land(state, times, &caller, &by, principal);
            let (token, token_until) = match token {
                Paired::Own(planned) => {
                    let (token, until) = land(state, times, &caller, &by, planned);
                    (token, Some(until))
                }
                Paired::Covered(token) => (token, None),
            };
            Applied::PrincipalAndTokenRevoked {
                principal,
                before: now,
                until,
                token,
                token_until,
            }
        }
    };
    if rated == Rated::Yes {
        count(state, caller_window(&by), now);
        if by.is_delegated() {
            count(state, lineage_window(&by.root()), now);
        }
    }
    Ok(applied)
}

/// Adds `planned`'s entry, pruning and replacing what it ends; returns its key and its `until`
/// as it now stands.
fn land(
    state: &mut State,
    times: Times,
    caller: &CallerToken,
    by: &AgentName,
    planned: Planned,
) -> (Revoked, Timestamp) {
    let Planned {
        revoked,
        kind,
        replaced,
    } = planned;
    let now = times.now;
    let (until, before, of) = match kind {
        Kind::RootToken => (
            // The presented token's life, counted from the command's stamp, as a record's is.
            now.saturating_add(Millis::new(
                caller
                    .claims
                    .expires_at
                    .as_millis()
                    .saturating_sub(times.at.as_millis()),
            ))
            .saturating_add(RECORD_MARGIN),
            None,
            None,
        ),
        // At least 30 seconds past the longest `kept_until` among the records it prunes, which
        // `record` adds: every token it can refuse belongs to one of them.
        Kind::Delegation { of } => (now.saturating_add(REVOCATION_MARGIN), None, Some(of)),
        Kind::RootPrincipal => (now.saturating_add(ROOT_PRINCIPAL_LIFE), Some(now), None),
        Kind::DelegatedPrincipal => (now.saturating_add(REVOCATION_MARGIN), Some(now), None),
    };
    let entry = Entry {
        until,
        added: Added::Ancestor {
            root: by.root(),
            by: by.clone(),
        },
        before,
        of,
    };
    let until = record(state, now, revoked.clone(), entry, &replaced).until;
    (revoked, until)
}

/// What an ancestor-rule revocation adds.
#[derive(Debug)]
enum Kind {
    /// The root token the caller presented: an entry no principal entry replaces and the cap
    /// doesn't count.
    RootToken,
    /// A delegation's token, the caller's own or one recorded below it, whose principal is `of`.
    Delegation { of: AgentName },
    /// A root's own principal, which the cap doesn't count.
    RootPrincipal,
    /// A delegated agent's principal.
    DelegatedPrincipal,
}

impl Kind {
    /// Whether the lineage's cap counts the entry (ADR-0047, I11).
    fn is_counted(&self) -> bool {
        match self {
            Self::RootToken | Self::RootPrincipal => false,
            Self::Delegation { .. } | Self::DelegatedPrincipal => true,
        }
    }

    /// Whether this is a root agent's revocation of itself: of its own principal, or of the
    /// root token it presented. Each ends the token that asked, so it limits itself, and no rate
    /// counts it (ADR-0047, point 11.6).
    fn is_root_self(&self) -> bool {
        matches!(self, Self::RootToken | Self::RootPrincipal)
    }
}

/// One entry an ancestor-rule revocation adds, as the checks resolved it.
#[derive(Debug)]
struct Planned {
    revoked: Revoked,
    kind: Kind,
    /// The live `Ancestor` entries it replaces.
    replaced: Vec<Revoked>,
}

/// What a command's target resolved to.
#[derive(Debug)]
enum Plan {
    /// A token or a principal.
    One(Planned),
    /// A principal and a token, the principal's entry added first.
    Pair { principal: Planned, token: Paired },
}

impl Plan {
    /// The entries the command adds.
    fn entries(&self) -> impl Iterator<Item = &Planned> {
        let (first, second) = match self {
            Self::One(planned) => (planned, None),
            Self::Pair {
                principal,
                token: Paired::Own(token),
            } => (principal, Some(token)),
            Self::Pair {
                principal,
                token: Paired::Covered(_),
            } => (principal, None),
        };
        std::iter::once(first).chain(second)
    }
}

/// The token of a [`Plan::Pair`].
#[derive(Debug)]
enum Paired {
    /// It gets an entry of its own.
    Own(Planned),
    /// The principal's entry covers it: a delegation recorded at or below the principal, whose
    /// record the principal's revocation prunes.
    Covered(Revoked),
}

/// Whether the rates count a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rated {
    Yes,
    /// Every entry it adds is a root agent's revocation of itself.
    No,
}

/// What the checks resolved.
#[derive(Debug)]
struct Checked {
    caller: AgentName,
    plan: Plan,
    rated: Rated,
}

/// Every check of an ancestor-rule revocation, in order: the caller, its record if it is
/// delegated, the targets, the rates, and the cap.
fn check(
    state: &State,
    times: Times,
    target: AncestorTarget,
    caller: &CallerToken,
) -> Result<Checked, Rejection> {
    let presented = &caller.claims;
    let by =
        AgentName::try_from(presented.principal.clone()).map_err(|_| Rejection::NotAncestor {
            reason: AncestorViolation::CallerNotAgent,
        })?;

    // A delegated caller is vouched for by its record, exactly as presented (I6).
    if by.is_delegated() {
        let forged = |reason| Rejection::ForgedLineage { reason };
        let record = DelegationId::try_from(presented.id.clone())
            .ok()
            .and_then(|id| state.delegations.get(&id))
            .filter(|record| record.is_kept(times.now))
            .ok_or(forged(Forgery::Unrecorded))?;
        if !record.is_live(times.at, times.now) {
            return Err(forged(Forgery::NotLive));
        }
        if let Some(field) = ClaimField::first_difference(&record.claims, presented) {
            return Err(forged(Forgery::Differs { field }));
        }
    }

    // The targets: what the caller presented, itself, or what is recorded below it.
    let plan = match target {
        AncestorTarget::Token(id) => Plan::One(token_target(state, times, &by, presented, id)?),
        AncestorTarget::Principal(q) => Plan::One(principal_target(state, times, &by, q)?),
        AncestorTarget::PrincipalAndToken { principal, token } => {
            let principal = principal_target(state, times, &by, principal)?;
            let token = token_target(state, times, &by, presented, token)?;
            let covered = match (&principal.revoked, &token.kind) {
                (Revoked::Principal { principal: q, .. }, Kind::Delegation { of }) => {
                    of.as_principal().is_at_or_below(q)
                }
                _ => false,
            };
            let token = if covered {
                Paired::Covered(token.revoked)
            } else {
                Paired::Own(token)
            };
            Plan::Pair { principal, token }
        }
    };

    // The rates: the caller's own, and, for a delegated caller, its lineage's. A root's
    // revocation of itself counts toward neither.
    let limit = |limit| Rejection::RevocationLimit { limit };
    let root = by.root();
    let rated = if plan.entries().all(|planned| planned.kind.is_root_self()) {
        Rated::No
    } else {
        Rated::Yes
    };
    if rated == Rated::Yes {
        if window_count(state, &caller_window(&by), times.now) >= ANCESTOR_CALLER_RATE {
            return Err(limit(RevocationLimit::CallerRate));
        }
        if by.is_delegated()
            && window_count(state, &lineage_window(&root), times.now) >= ANCESTOR_LINEAGE_RATE
        {
            return Err(limit(RevocationLimit::LineageRate));
        }
    }

    // The cap counts each budget of the lineage apart: the root's own entries, and its delegated
    // agents' between them (ADR-0047, point 11.6, as amended 2026-11-13). It refuses only a
    // revocation that would take the caller's budget past the cap: one that adds more new keys
    // it counts than it replaces from that budget. A new entry falls in its adder's budget, and
    // an entry raised in place keeps its first adder's.
    let budget = Budget::of(&root, &by);
    let added = plan
        .entries()
        .filter(|planned| {
            planned.kind.is_counted()
                && state
                    .revocations
                    .get(&planned.revoked)
                    .is_none_or(|entry| entry.until <= times.now)
        })
        .count();
    let replaced = plan
        .entries()
        .flat_map(|planned| &planned.replaced)
        .filter(|key| {
            state
                .revocations
                .get(key)
                .and_then(|entry| counted_in(key, entry))
                == Some((&root, budget))
        })
        .count();
    if let Some(raised) = added.checked_sub(replaced).filter(|raised| *raised > 0)
        && lineage_entries(state, &root, budget, times.now)
            .count()
            .saturating_add(raised)
            > MAX_ANCESTOR_ENTRIES_PER_LINEAGE
    {
        return Err(limit(match budget {
            Budget::Root => RevocationLimit::RootEntries,
            Budget::Descendants => RevocationLimit::Entries {
                busiest: busiest_child(state, &by, times.now),
            },
        }));
    }

    Ok(Checked {
        caller: by,
        plan,
        rated,
    })
}

/// A token target: the token `presented`, or a delegation recorded below `by`.
fn token_target(
    state: &State,
    times: Times,
    by: &AgentName,
    presented: &TokenClaims,
    id: TokenId,
) -> Result<Planned, Rejection> {
    let kind = if id == presented.id {
        if by.is_delegated() {
            Kind::Delegation { of: by.clone() }
        } else {
            Kind::RootToken
        }
    } else {
        let of = recorded_below(state, times, by, |record| record.claims.id == id)
            .ok_or_else(|| Rejection::NoLiveDelegation { id: id.clone() })?;
        Kind::Delegation { of }
    };
    Ok(Planned {
        revoked: Revoked::Token(id),
        kind,
        replaced: Vec::new(),
    })
}

/// A principal target: `by` itself, or the principal of a delegation recorded below it.
fn principal_target(
    state: &State,
    times: Times,
    by: &AgentName,
    q: AgentName,
) -> Result<Planned, Rejection> {
    if q != *by
        && recorded_below(state, times, by, |record| {
            record.principal() == q.as_principal()
        })
        .is_none()
    {
        return Err(Rejection::NotAncestor {
            reason: AncestorViolation::NotBelowCaller,
        });
    }
    let kind = if q.is_delegated() {
        Kind::DelegatedPrincipal
    } else {
        Kind::RootPrincipal
    };
    let revoked = Revoked::principal(q.into());
    let replaced = replaced_by(state, &revoked, times.now);
    Ok(Planned {
        revoked,
        kind,
        replaced,
    })
}

/// The principal of the delegation recorded strictly below `caller` that `matches`, if one
/// does. A record is kept past its token's `exp` for a margin, while nodes whose clocks lag the
/// leader's may still accept the token, so it stays revocable until it is no longer kept.
fn recorded_below(
    state: &State,
    times: Times,
    caller: &AgentName,
    matches: impl Fn(&super::Delegation) -> bool,
) -> Option<AgentName> {
    state
        .delegations
        .values()
        .filter(|record| record.is_kept(times.now))
        .filter(|record| {
            let principal = record.principal();
            principal != caller.as_principal() && principal.is_at_or_below(caller.as_principal())
        })
        .find(|record| matches(record))
        .and_then(|record| AgentName::try_from(record.principal().clone()).ok())
}

/// The live `Ancestor` entries a revocation of `revoked` replaces (ADR-0047, point 11.4): for a
/// principal `Q`, the principal entries for names below `Q` and the delegation token entries
/// whose principal is at or below `Q`. A token entry replaces nothing; root-token and `Fleet`
/// entries are never replaced.
fn replaced_by(state: &State, revoked: &Revoked, now: Timestamp) -> Vec<Revoked> {
    let Revoked::Principal {
        fleet: None,
        principal: q,
    } = revoked
    else {
        return Vec::new();
    };
    state
        .revocations
        .iter()
        .filter(|(_, entry)| entry.until > now && matches!(entry.added, Added::Ancestor { .. }))
        .filter(|(key, entry)| match key {
            Revoked::Principal {
                fleet: None,
                principal,
            } => principal != q && principal.is_at_or_below(q),
            Revoked::Token(_) => entry
                .of
                .as_ref()
                .is_some_and(|of| of.as_principal().is_at_or_below(q)),
            Revoked::Principal { fleet: Some(_), .. } | Revoked::Key(_) => false,
        })
        .map(|(key, _)| key.clone())
        .collect()
}

/// Prunes what `revoked` ends, forgets what the time has passed, replaces `replaced`, and adds
/// `entry`, kept at least 30 seconds past every record it pruned and as long as every entry it
/// replaced; returns the entry as it now stands.
fn record<'s>(
    state: &'s mut State,
    now: Timestamp,
    revoked: Revoked,
    mut entry: Entry,
    replaced: &[Revoked],
) -> &'s Entry {
    if let Some(kept_until) = prune_revoked(state, &revoked, now) {
        entry.until = entry
            .until
            .max(kept_until.saturating_add(REVOCATION_MARGIN));
    }
    forget_through(state, now);
    state.revocations.forget_through(now);
    for key in replaced {
        if let Some(gone) = state.revocations.get(key) {
            entry.until = entry.until.max(gone.until);
        }
        state.revocations.remove(key);
    }
    state.revocations.insert(revoked, entry)
}

/// Which of a lineage's two budgets of [`MAX_ANCESTOR_ENTRIES_PER_LINEAGE`] an entry counts in
/// (ADR-0047, point 11.6, as amended 2026-11-13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Budget {
    /// The entries the root agent added itself.
    Root,
    /// The entries the lineage's delegated agents added, between them.
    Descendants,
}

impl Budget {
    /// The budget of an entry `by` added in `root`'s lineage.
    fn of(root: &AgentName, by: &AgentName) -> Self {
        if by == root {
            Self::Root
        } else {
            Self::Descendants
        }
    }
}

/// Whether the cap counts `revoked`'s entry, and in which lineage and budget: every `Ancestor`
/// entry but the root's own principal entry and a root token's entry (ADR-0047, I11).
fn counted_in<'e>(revoked: &Revoked, entry: &'e Entry) -> Option<(&'e AgentName, Budget)> {
    let Added::Ancestor { root, by } = &entry.added else {
        return None;
    };
    let exempt = match revoked {
        Revoked::Principal { principal, .. } => principal == root.as_principal(),
        Revoked::Token(_) => entry.of.is_none(),
        Revoked::Key(_) => false,
    };
    (!exempt).then(|| (root, Budget::of(root, by)))
}

/// The live entries the cap counts in `budget` of `root`'s lineage, with what each refuses: the
/// principal it names, or the principal a token was the token of. Which entries count is
/// [`counted_in`]'s alone to say.
fn lineage_entries<'s>(
    state: &'s State,
    root: &'s AgentName,
    budget: Budget,
    now: Timestamp,
) -> impl Iterator<Item = &'s PrincipalName> + 's {
    state
        .revocations
        .iter()
        .filter_map(move |(revoked, entry)| {
            if entry.until <= now || counted_in(revoked, entry) != Some((root, budget)) {
                return None;
            }
            match revoked {
                Revoked::Principal { principal, .. } => Some(principal),
                Revoked::Token(_) => Some(
                    entry
                        .of
                        .as_ref()
                        .map_or(root.as_principal(), AgentName::as_principal),
                ),
                Revoked::Key(_) => None,
            }
        })
}

/// The child of `caller` whose subtree holds the most entries the descendants' budget counts,
/// the first by name on a tie; `None` if no child's subtree holds one.
fn busiest_child(state: &State, caller: &AgentName, now: Timestamp) -> Option<AgentName> {
    let root = caller.root();
    let mut held: BTreeMap<&str, usize> = BTreeMap::new();
    for subject in lineage_entries(state, &root, Budget::Descendants, now) {
        let Some(rest) = subject
            .as_str()
            .strip_prefix(caller.as_str())
            .and_then(|rest| rest.strip_prefix('.'))
        else {
            continue;
        };
        let segment = rest.split('.').next().unwrap_or(rest);
        *held.entry(segment).or_default() += 1;
    }
    let mut busiest: Option<(&str, usize)> = None;
    for (segment, count) in held {
        if busiest.is_none_or(|(_, most)| count > most) {
            busiest = Some((segment, count));
        }
    }
    busiest.and_then(|(segment, _)| caller.child(segment).ok())
}

/// Adds `now` to the window `key`.
fn count(state: &mut State, key: RateKey, now: Timestamp) {
    state.rate_windows.entry(key).or_default().push_back(now);
}

/// The window that counts `caller`'s ancestor-rule revocations.
fn caller_window(caller: &AgentName) -> RateKey {
    RateKey {
        kind: RateKind::AncestorRevocation,
        scope: RateScope::Caller(caller.as_principal().clone()),
    }
}

/// The window that counts the ancestor-rule revocations of the delegated agents below `root`.
fn lineage_window(root: &AgentName) -> RateKey {
    RateKey {
        kind: RateKind::AncestorRevocation,
        scope: RateScope::Lineage(root.clone()),
    }
}
