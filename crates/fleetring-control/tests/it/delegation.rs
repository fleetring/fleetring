//! Delegations in control state (ADR-0047, points 5.7 to 5.10 and 6.1 to 6.3): what
//! `RecordDelegation`'s `apply` refuses whoever proposes it, retries, limits and rate windows,
//! retention and pruning, `recorded_by`, the feature gate, and byte stability.

use fleetring_control::delegation::{
    self, AncestorTarget, CALLER_RATE, ChainLink, ClaimField, ClaimsViolation, DelegationOperation,
    Forgery, LINEAGE_RATE, Limit, MAX_LIVE_PER_LINEAGE, MAX_RECORDS_PER_LINEAGE, OperationOutcome,
    RECORD_MARGIN, RateKind, RateScope, RequestField, Resent, SignerKind, StructureViolation,
    TokenClaims, TokenKind,
};
use fleetring_control::scope::{GrantStepError, MAX_SCOPES, Scopes};
use fleetring_control::{
    Action, Applied, Command, Feature, Member, Rejection, Release, Revoked, Software, State,
    Timestamp, apply,
};
use proptest::prelude::*;

use crate::common::delegation::{
    AUTHORITY, Ask, HOUR, ISSUER, RECORDER, as_ancestor, copy, delegating, drawn, fleet, key, leaf,
    op, principal, principal_target, recorded, root_token, scopes, stamped, token,
};
use crate::common::{at, config, id};

/// 2 hours in, a whole second: the default child's expiry from a command near 0.
const EXP: u64 = 2 * HOUR;

/// Root `agent-42` delegates `agent-42.c7` with scopes that let it delegate, at `millis`.
fn c7(state: &mut State, millis: u64) -> TokenClaims {
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(
        1,
        "c7",
        delegating("agent-42.c7"),
        millis + EXP - millis % 1_000,
    );
    recorded(stamped(state, millis, ask.action(&root)))
}

/// Applies `action` stamped `millis` and checks that a refusal left the state byte for byte as
/// it was.
fn refused(state: &mut State, millis: u64, action: Action) -> Rejection {
    let before = serde_json::to_string(state).expect("serializes");
    let rejection = stamped(state, millis, action).expect_err("refused");
    assert_eq!(
        serde_json::to_string(state).expect("serializes"),
        before,
        "{rejection:?} changed the state"
    );
    rejection
}

fn structure(reason: StructureViolation) -> Rejection {
    Rejection::DelegationStructure { reason }
}

// ---- I1 and I2: whatever the proposer sends ----

#[test]
fn apply_refuses_out_of_tier_scopes_whoever_proposes() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    for (n, outside) in [
        "admin:agent-43-*",
        "read:pub-*",
        "admin:agent-420-*",
        "grant:agent-42.d1.*",
        "admin:agent-42.c7x*",
        "fleet",
    ]
    .into_iter()
    .enumerate()
    {
        let ask = Ask::new(
            u16::try_from(n).expect("small") + 10,
            "c7",
            scopes(&["admin:agent-42.c7-*", outside]),
            EXP,
        );
        let rejection = refused(&mut state, 1_000, ask.action(&root));
        assert!(
            matches!(
                rejection,
                Rejection::DelegationClaims {
                    reason: ClaimsViolation::Tier(_)
                }
            ),
            "{outside}: {rejection:?}"
        );
    }
    assert_eq!(state.delegations().len(), 0);
}

#[test]
fn apply_refuses_non_canonical_or_too_many_scopes() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let mut ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    // Out of order, then a scope another covers.
    let mut reordered: Vec<_> = ask.scopes.as_slice().to_vec();
    reordered.reverse();
    ask.scopes = Scopes::new(reordered);
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&root)),
        Rejection::NonCanonicalScopes
    );
    let covered = Scopes::new(
        ["admin:agent-42.c7-*", "read:agent-42.c7-x"]
            .iter()
            .map(|text| text.parse().expect("a scope")),
    );
    ask.scopes = covered;
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&root)),
        Rejection::NonCanonicalScopes
    );
    let many: Vec<String> = (0..=MAX_SCOPES)
        .map(|n| format!("read:agent-42.c7-{n:04}"))
        .collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    ask.scopes = scopes(&many);
    assert_eq!(ask.scopes.len(), MAX_SCOPES + 1);
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&root)),
        structure(StructureViolation::TooManyScopes)
    );
}

#[test]
fn apply_refuses_exp_after_the_callers() {
    let mut state = fleet();
    let mut root = root_token("agent-42", "root-jti");
    root.claims.expires_at = at(HOUR);
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), HOUR + 1_000);
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&root)),
        structure(StructureViolation::ExpiresAfterCaller)
    );
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), HOUR);
    recorded(stamped(&mut state, 1_000, ask.action(&root)));
}

/// `iat` comes from the effective time and `nbf` is raised to it, so a child is never issued
/// after it starts; a lifetime over 24 hours, a start more than 24 hours after `iat`, and a
/// ninth level are refused.
#[test]
fn apply_refuses_iat_after_nbf_lifetime_over_24h_nbf_over_24h_ahead_and_depth_nine() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");

    // A start before the effective time is raised to it, rounded down to the second.
    let ask = Ask::new(1, "c7", delegating("agent-42.c7"), EXP);
    let claims = recorded(stamped(&mut state, 5_400, ask.action(&root)));
    assert_eq!(claims.issued_at, Some(at(5_000)));
    assert_eq!(claims.not_before, at(5_000));

    let mut long = Ask::new(2, "d1", leaf("agent-42.d1"), 0);
    long.not_before = at(10_000);
    long.expires_at = at(10_000 + 24 * HOUR + 1_000);
    assert_eq!(
        refused(&mut state, 10_000, long.action(&root)),
        Rejection::DelegationClaims {
            reason: ClaimsViolation::TooLong
        }
    );
    let mut late = Ask::new(3, "d2", leaf("agent-42.d2"), 0);
    late.not_before = at(10_000 + 24 * HOUR + 1_000);
    late.expires_at = at(10_000 + 25 * HOUR);
    assert_eq!(
        refused(&mut state, 10_000, late.action(&root)),
        Rejection::DelegationClaims {
            reason: ClaimsViolation::StartsTooLate
        }
    );
    late.not_before = at(10_000 + 24 * HOUR);
    recorded(stamped(&mut state, 10_000, late.action(&root)));

    // Eight levels below the root, then a ninth.
    let mut caller = copy(&claims, ISSUER);
    let mut name = "agent-42.c7".to_owned();
    for level in 2..=8_u16 {
        name.push_str(".s");
        let ask = Ask::new(10 + level, "s", delegating(&name), EXP);
        caller = copy(
            &recorded(stamped(&mut state, 20_000, ask.action(&caller))),
            ISSUER,
        );
    }
    assert_eq!(name.matches('.').count(), 8);
    let ask = Ask::new(30, "s", leaf(&format!("{name}.s")), EXP);
    assert_eq!(
        refused(&mut state, 20_000, ask.action(&caller)),
        Rejection::DelegationLimit {
            limit: Limit::Depth
        }
    );
}

#[test]
fn apply_refuses_times_not_whole_seconds() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP + 1);
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&root)),
        structure(StructureViolation::NotWholeSeconds)
    );
    let mut ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    ask.not_before = at(2_500);
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&root)),
        structure(StructureViolation::NotWholeSeconds)
    );
}

#[test]
fn apply_refuses_what_only_an_agent_with_an_owner_may_ask() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);

    let mut action = ask.action(&root);
    if let Action::RecordDelegation { requested_by, .. } = &mut action {
        *requested_by = principal("agent-43");
    }
    assert_eq!(
        refused(&mut state, 1_000, action),
        structure(StructureViolation::RequesterNotCaller)
    );
    let service = root_token("workload", "svc-jti");
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&service)),
        structure(StructureViolation::CallerNotAgent)
    );
    let mut untyped = root.clone();
    untyped.claims.kind = None;
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&untyped)),
        structure(StructureViolation::CallerKindNotAgent)
    );
    let mut ownerless = root.clone();
    ownerless.claims.owner = None;
    assert_eq!(
        refused(&mut state, 1_000, ask.action(&ownerless)),
        structure(StructureViolation::NoOwner)
    );
    let mut action = ask.action(&root);
    if let Action::RecordDelegation { request, .. } = &mut action {
        request.caller_token = token("another-jti");
    }
    assert_eq!(
        refused(&mut state, 1_000, action),
        structure(StructureViolation::CallerTokenDiffers)
    );
    let mut bad = ask.clone();
    bad.segment = "C-7".to_owned();
    assert_eq!(
        refused(&mut state, 1_000, bad.action(&root)),
        structure(StructureViolation::Segment)
    );
}

// ---- Against the parent's record ----

#[test]
fn apply_refuses_a_child_wider_than_its_recorded_parent() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    // c7 may only read its children's databases.
    let narrow = scopes(&[
        "admin:agent-42.c7-*",
        "read:agent-42.c7.*",
        "grant:agent-42.c7.*",
    ]);
    let ask = Ask::new(1, "c7", narrow, EXP);
    let c7 = recorded(stamped(&mut state, 1_000, ask.action(&root)));
    let caller = copy(&c7, ISSUER);
    let wider = Ask::new(2, "r1", leaf("agent-42.c7.r1"), EXP);
    assert!(matches!(
        refused(&mut state, 2_000, wider.action(&caller)),
        Rejection::NotNarrowed {
            reason: GrantStepError::NotCovered { .. }
        }
    ));
    let within = Ask::new(3, "r1", scopes(&["read:agent-42.c7.r1-*"]), EXP);
    recorded(stamped(&mut state, 2_000, within.action(&caller)));
}

#[test]
fn apply_refuses_a_child_whose_chain_omits_its_parent() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_000);
    // A chain of the right length that names another root token in place of the real one.
    let mut forged = copy(&c7, ISSUER);
    forged.claims.chain = vec![ChainLink {
        id: token("other-root"),
        kid: key(AUTHORITY),
    }];
    let ask = Ask::new(2, "r1", leaf("agent-42.c7.r1"), EXP);
    assert_eq!(
        refused(&mut state, 2_000, ask.action(&forged)),
        structure(StructureViolation::ChainDiffers)
    );
    // And one too short fails the structure every node checks.
    forged.claims.chain.clear();
    assert!(matches!(
        refused(&mut state, 2_000, ask.action(&forged)),
        Rejection::DelegationClaims {
            reason: ClaimsViolation::ChainLength { links: 1, depth: 2 }
        }
    ));
}

#[test]
fn apply_refuses_another_owner_or_a_later_exp_than_the_parent_record() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_000);
    let mut owned = copy(&c7, ISSUER);
    owned.claims.owner = Some(principal("mallory"));
    let ask = Ask::new(2, "r1", leaf("agent-42.c7.r1"), EXP);
    assert_eq!(
        refused(&mut state, 2_000, ask.action(&owned)),
        structure(StructureViolation::OwnerDiffers)
    );
    let mut later = copy(&c7, ISSUER);
    later.claims.expires_at = at(EXP + HOUR);
    let ask = Ask::new(3, "r1", leaf("agent-42.c7.r1"), EXP + 2_000);
    assert_eq!(
        refused(&mut state, 2_000, ask.action(&later)),
        structure(StructureViolation::ExpiresAfterParent)
    );
}

// ---- I3 ----

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Whatever the caller, a recorded child's chain is the caller's chain followed by the
    /// caller's own ID and the key of the copy it presented, its parent is the caller, and its
    /// owner the caller's.
    #[test]
    fn recorded_child_extends_the_callers_chain_by_its_copy(
        depth in 0_usize..8,
        kids in proptest::collection::vec(0_u8..4, 8),
        stamp in 0_u64..10_000,
    ) {
        let mut state = fleet();
        let mut caller = root_token("agent-7", "root-7");
        let mut name = "agent-7".to_owned();
        for (level, kid) in kids.iter().enumerate().take(depth + 1) {
            name.push_str(&format!(".l{level}"));
            let n = u16::try_from(level).expect("small") + 1;
            let ask = Ask::new(n, &format!("l{level}"), delegating(&name), EXP);
            let child = recorded(stamped(&mut state, stamp, ask.action(&caller)));
            let mut expected = caller.claims.chain.clone();
            expected.push(ChainLink { id: caller.claims.id.clone(), kid: caller.kid.clone() });
            prop_assert_eq!(&child.chain, &expected);
            prop_assert_eq!(child.parent.as_ref(), Some(&caller.claims.principal));
            prop_assert_eq!(&child.owner, &caller.claims.owner);
            prop_assert_eq!(child.chain.len(), level + 1);
            caller = copy(&child, &format!("issuer-{}", kid));
        }
    }
}

// ---- I6 ----

#[test]
fn apply_refuses_an_unrecorded_delegated_caller() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_000);
    let mut stranger = copy(&c7, ISSUER);
    stranger.claims.id = drawn(999).into();
    let ask = Ask::new(2, "r1", leaf("agent-42.c7.r1"), EXP);
    assert_eq!(
        refused(&mut state, 2_000, ask.action(&stranger)),
        Rejection::ForgedLineage {
            reason: Forgery::Unrecorded
        }
    );
    // A real record, presented once its token expired: a child must end after its `iat` and
    // no later than its parent's record, so the structure refuses it before liveness is asked.
    let caller = copy(&c7, ISSUER);
    let after = c7.expires_at.as_millis();
    assert_eq!(
        refused(&mut state, after, ask.action(&caller)),
        Rejection::DelegationClaims {
            reason: ClaimsViolation::EndsBeforeStart
        }
    );
    let mut outliving = ask.clone();
    outliving.expires_at = at(after + 1_000);
    assert_eq!(
        refused(&mut state, after, outliving.action(&caller)),
        structure(StructureViolation::ExpiresAfterCaller)
    );
}

#[test]
fn apply_refuses_presented_claims_differing_from_the_record() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_000);
    let ask = Ask::new(2, "r1", leaf("agent-42.c7.r1"), EXP - HOUR);
    let differs = |field| Rejection::ForgedLineage {
        reason: Forgery::Differs { field },
    };
    type Edit = fn(&mut TokenClaims);
    let cases: [(Edit, Rejection); 11] = [
        (|c| c.fleet = "other".into(), differs(ClaimField::Fleet)),
        (
            |c| c.principal = principal("agent-42.c8"),
            differs(ClaimField::Principal),
        ),
        // Another ID names no record.
        (
            |c| c.id = drawn(77).into(),
            Rejection::ForgedLineage {
                reason: Forgery::Unrecorded,
            },
        ),
        (
            |c| c.issued_at = Some(Timestamp::ZERO),
            differs(ClaimField::IssuedAt),
        ),
        (
            |c| c.not_before = at(c.not_before.as_millis() + 1_000),
            differs(ClaimField::NotBefore),
        ),
        (
            |c| c.expires_at = at(c.expires_at.as_millis() - 1_000),
            differs(ClaimField::ExpiresAt),
        ),
        (
            |c| c.scopes = delegating("agent-42"),
            differs(ClaimField::Scopes),
        ),
        (
            |c| c.kind = None,
            structure(StructureViolation::CallerKindNotAgent),
        ),
        (
            |c| c.owner = Some(principal("mallory")),
            structure(StructureViolation::OwnerDiffers),
        ),
        (
            |c| c.parent = Some(principal("agent-43")),
            differs(ClaimField::Parent),
        ),
        (
            |c| c.chain[0].kid = key("other-authority"),
            structure(StructureViolation::ChainDiffers),
        ),
    ];
    for (edit, expected) in cases {
        let mut presented = copy(&c7, ISSUER);
        edit(&mut presented.claims);
        assert_eq!(refused(&mut state, 2_000, ask.action(&presented)), expected);
    }
    // The fields the earlier checks catch are still caught by the comparison itself.
    let mut presented = c7.clone();
    presented.chain[0].kid = key("other-authority");
    assert_eq!(
        ClaimField::first_difference(&c7, &presented),
        Some(ClaimField::Chain)
    );
    presented = c7.clone();
    presented.owner = None;
    assert_eq!(
        ClaimField::first_difference(&c7, &presented),
        Some(ClaimField::Owner)
    );
    recorded(stamped(&mut state, 2_000, ask.action(&copy(&c7, ISSUER))));
}

// ---- I7: retries ----

fn resent(outcome: Result<Applied, Rejection>) -> TokenClaims {
    match outcome {
        Ok(Applied::DelegationRecorded {
            claims,
            resent: Resent::Yes,
            ..
        }) => claims,
        other => panic!("expected a resend: {other:?}"),
    }
}

#[test]
fn resend_same_requester_same_request_records_once() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    let first = recorded(stamped(&mut state, 1_000, ask.action(&root)));
    // A resend draws another ID and resolves later times; both are ignored.
    let mut again = ask.clone();
    again.id = drawn(2);
    again.expires_at = at(EXP + 60_000);
    let action = again.action(&root);
    let answered = state
        .delegation_resend(&action, at(5_000))
        .expect("a resend")
        .expect("answered");
    assert_eq!(answered.1.claims, first);
    assert_eq!(resent(stamped(&mut state, 5_000, action)), first);
    assert_eq!(state.delegations().len(), 1);
    let window = state
        .rate_windows()
        .find(|(key, _)| key.scope == RateScope::Caller(principal("agent-42")))
        .map(|(_, times)| times.count());
    assert_eq!(window, Some(1), "a resend counts against no rate");
}

#[test]
fn resend_with_scopes_reordered_is_the_same_request() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    let first = recorded(stamped(&mut state, 1_000, ask.action(&root)));
    let mut action = ask.action(&root);
    if let Action::RecordDelegation { request, .. } = &mut action {
        let mut reversed = ask.scopes.as_slice().to_vec();
        reversed.reverse();
        request.scopes = Some(Scopes::new(reversed));
    }
    assert_eq!(resent(stamped(&mut state, 2_000, action)), first);
}

#[test]
fn resend_from_another_requester_is_not_yours() {
    let mut state = fleet();
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    recorded(stamped(
        &mut state,
        1_000,
        ask.action(&root_token("agent-42", "root-jti")),
    ));
    let other = Ask::new(1, "c7", leaf("agent-43.c7"), EXP);
    assert_eq!(
        refused(
            &mut state,
            2_000,
            other.action(&root_token("agent-43", "jti-43"))
        ),
        Rejection::OperationNotYours { operation: op(1) }
    );
}

#[test]
fn resend_with_a_changed_field_conflicts_naming_it() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    recorded(stamped(&mut state, 1_000, ask.action(&root)));
    let conflict = |field| Rejection::DelegationConflict {
        operation: op(1),
        field,
    };
    let mut segment = ask.clone();
    segment.segment = "c8".to_owned();
    assert_eq!(
        refused(&mut state, 2_000, segment.action(&root)),
        conflict(RequestField::Segment)
    );
    let mut defaults = ask.clone();
    defaults.explicit = false;
    assert_eq!(
        refused(&mut state, 2_000, defaults.action(&root)),
        conflict(RequestField::Scopes)
    );
    for (edit, field) in [
        (
            (|request: &mut delegation::CanonicalRequest| {
                request.lifetime = Some(fleetring_control::Millis::new(60_000));
            }) as fn(&mut delegation::CanonicalRequest),
            RequestField::Lifetime,
        ),
        (
            |request| request.not_before = Some(at(1_000)),
            RequestField::NotBefore,
        ),
    ] {
        let mut action = ask.action(&root);
        if let Action::RecordDelegation { request, .. } = &mut action {
            edit(request);
        }
        assert_eq!(refused(&mut state, 2_000, action), conflict(field));
    }
    let renewed = root_token("agent-42", "renewed-jti");
    assert_eq!(
        refused(&mut state, 2_000, ask.action(&renewed)),
        conflict(RequestField::CallerToken)
    );
}

#[test]
fn racing_resends_record_once() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    let mut racer = ask.clone();
    racer.id = drawn(2);
    let first = recorded(stamped(&mut state, 1_000, ask.action(&root)));
    assert_eq!(
        resent(stamped(&mut state, 1_000, racer.action(&root))),
        first
    );
    assert_eq!(state.delegations().len(), 1);
}

#[test]
fn resend_of_a_revoked_delegation_is_refused_by_its_tombstone() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    let claims = recorded(stamped(&mut state, 1_000, ask.action(&root)));
    stamped(
        &mut state,
        2_000,
        Action::RevokeToken {
            revoked: Revoked::Token(claims.id.clone()),
            until: at(EXP),
        },
    )
    .expect("revoked");
    assert_eq!(state.delegations().len(), 0);
    assert_eq!(
        refused(&mut state, 3_000, ask.action(&root)),
        Rejection::DelegationRevoked { operation: op(1) }
    );
    // A key in its chain revoked prunes nothing, and still refuses a resend.
    let mut state = fleet();
    recorded(stamped(&mut state, 1_000, ask.action(&root)));
    stamped(
        &mut state,
        2_000,
        Action::RevokeToken {
            revoked: Revoked::Key(key(AUTHORITY)),
            until: Timestamp::MAX,
        },
    )
    .expect("revoked");
    assert_eq!(state.delegations().len(), 1);
    assert_eq!(
        refused(&mut state, 3_000, ask.action(&root)),
        Rejection::DelegationRevoked { operation: op(1) }
    );
}

/// A resend presents the copy of its token the delegation was minted from: a copy of the same
/// claims that another key signed, a stolen issuer key's re-signing say, gets a conflict naming
/// the key, never the child, whose chain names the first copy's key (ADR-0047, point 8).
#[test]
fn resend_from_a_copy_another_key_signed_conflicts_naming_the_key() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_000);
    let ask = Ask::new(2, "r1", leaf("agent-42.c7.r1"), EXP);
    let r1 = recorded(stamped(&mut state, 2_000, ask.action(&copy(&c7, ISSUER))));
    assert_eq!(
        r1.chain.last().map(|link| link.kid.clone()),
        Some(key(ISSUER))
    );
    let other_key = ask.action(&copy(&c7, "issuer-stolen"));
    assert_eq!(
        state
            .delegation_resend(&other_key, at(3_000))
            .map(|answer| answer.map(|_| ())),
        Some(Err(Rejection::DelegationConflict {
            operation: op(2),
            field: RequestField::CallerKey,
        }))
    );
    assert_eq!(
        refused(&mut state, 3_000, other_key),
        Rejection::DelegationConflict {
            operation: op(2),
            field: RequestField::CallerKey,
        }
    );
    // The copy it was minted from still receives it.
    assert_eq!(
        resent(stamped(&mut state, 3_000, ask.action(&copy(&c7, ISSUER)))),
        r1
    );
}

/// A resend returns only the record its own operation recorded: once that record is forgotten,
/// a later delegation that drew the same ID, for another lineage, is never handed to the first
/// operation's requester.
#[test]
fn resend_never_returns_a_record_another_operation_recorded() {
    let mut state = fleet();
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), 61_000);
    let first = recorded(stamped(
        &mut state,
        1_000,
        ask.action(&root_token("agent-42", "root-jti")),
    ));
    let kept_until = state
        .delegations()
        .next()
        .map(|(_, record)| record.kept_until)
        .expect("a record");
    // The record is forgotten; its operation is kept another hour.
    let later = kept_until.as_millis();
    stamped(&mut state, later, Action::Tick).expect("tick");
    assert_eq!(state.delegations().len(), 0);
    assert!(state.delegation_operation(&op(1), at(later)).is_some());
    // Another lineage's delegation draws the same ID, which no kept record holds.
    let mut other = Ask::new(2, "x", leaf("agent-43.x"), later + 2 * HOUR);
    other.id = ask.id.clone();
    let theirs = recorded(stamped(
        &mut state,
        later,
        other.action(&root_token("agent-43", "jti-43")),
    ));
    assert_eq!(theirs.id, first.id);
    assert_eq!(
        refused(
            &mut state,
            later,
            ask.action(&root_token("agent-42", "root-jti"))
        ),
        Rejection::DelegationExpired { operation: op(1) }
    );
}

/// A new operation that draws the ID of a kept record is refused, so the control node draws
/// again; once the record is forgotten, the ID is free.
#[test]
fn a_drawn_id_a_kept_record_holds_is_taken() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), 61_000);
    let first = recorded(stamped(&mut state, 1_000, ask.action(&root)));
    let mut again = Ask::new(2, "c8", leaf("agent-42.c8"), 121_000);
    again.id = ask.id.clone();
    assert_eq!(
        refused(&mut state, 2_000, again.action(&root)),
        Rejection::DelegationIdTaken { id: ask.id.clone() }
    );
    // Expired but kept, it still holds its ID.
    assert_eq!(
        refused(&mut state, 62_000, again.action(&root)),
        Rejection::DelegationIdTaken { id: ask.id.clone() }
    );
    let kept_until = state
        .delegations()
        .next()
        .map(|(_, record)| record.kept_until.as_millis())
        .expect("a record");
    again.expires_at = at(kept_until + HOUR);
    let second = recorded(stamped(&mut state, kept_until, again.action(&root)));
    assert_eq!(second.id, first.id);
}

#[test]
fn delegation_operations_are_separate_from_fork_operations() {
    let mut state = crate::common::fork::fleet(&["n0", "n1", "n2"]);
    let shared = crate::common::fork::op(9);
    crate::common::run(&mut state, 10, crate::common::fork::fork_request(9, "t"))
        .expect("the fork");
    let mut ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    ask.operation = shared;
    recorded(stamped(
        &mut state,
        1_000,
        ask.action(&root_token("agent-42", "root-jti")),
    ));
    assert!(state.fork_operation(&shared, at(1_000)).is_some());
    assert!(state.delegation_operation(&shared, at(1_000)).is_some());
}

// ---- I4, minting half ----

#[test]
fn caller_revoked_first_is_refused() {
    let root = root_token("agent-42", "root-jti");
    let base = fleet();
    let mut state = base.clone();
    let c7 = c7(&mut state, 1_000);
    let caller = copy(&c7, ISSUER);
    let ask = Ask::new(2, "r1", leaf("agent-42.c7.r1"), EXP);
    for revoked in [Revoked::Key(key(ISSUER)), Revoked::Key(key(AUTHORITY))] {
        let mut state = state.clone();
        stamped(
            &mut state,
            2_000,
            Action::RevokeToken {
                revoked: revoked.clone(),
                until: Timestamp::MAX,
            },
        )
        .expect("revoked");
        assert_eq!(
            refused(&mut state, 3_000, ask.action(&caller)),
            Rejection::CallerRevoked { by: revoked }
        );
    }
    // The caller's own ID and its chain's prune its record, so the denylist refuses it first
    // when the record is kept by a root caller's token, and forged lineage when not.
    let mut revoked_root = base.clone();
    stamped(
        &mut revoked_root,
        2_000,
        Action::RevokeToken {
            revoked: Revoked::Token(root.claims.id.clone()),
            until: at(EXP),
        },
    )
    .expect("revoked");
    let ask = Ask::new(3, "c7", leaf("agent-42.c7"), EXP);
    assert_eq!(
        refused(&mut revoked_root, 3_000, ask.action(&root)),
        Rejection::CallerRevoked {
            by: Revoked::Token(root.claims.id.clone())
        }
    );
    for revoked in [c7.id.clone(), root.claims.id.clone()] {
        let mut state = state.clone();
        stamped(
            &mut state,
            2_000,
            Action::RevokeToken {
                revoked: Revoked::Token(revoked),
                until: at(EXP),
            },
        )
        .expect("revoked");
        let ask = Ask::new(4, "r1", leaf("agent-42.c7.r1"), EXP);
        assert_eq!(
            refused(&mut state, 3_000, ask.action(&caller)),
            Rejection::ForgedLineage {
                reason: Forgery::Unrecorded
            }
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// However delegations and revocations, by token or principal, with `fleet` or under the
    /// ancestor rule, interleave, at equal or increasing stamps, no live delegation is left that
    /// the denylist refuses (I4).
    #[test]
    fn prop_no_interleaving_leaves_a_live_child_of_a_revoked_parent(
        steps in proptest::collection::vec((0_u8..8, 0_u8..8, 0_u64..2), 1..40),
    ) {
        let mut state = fleet();
        let root = root_token("agent-42", "root-jti");
        let mut callers = vec![root.clone()];
        let mut clock = 1_000;
        for (n, (kind, pick, dt)) in steps.into_iter().enumerate() {
            clock += dt * 1_000;
            let n = u16::try_from(n).expect("small") + 1;
            let caller = callers[usize::from(pick) % callers.len()].clone();
            let own_principal = || {
                AncestorTarget::Principal(
                    caller.claims.principal.as_str().parse().expect("an agent name"),
                )
            };
            let action = match kind {
                0..=2 => {
                    let name = format!("{}.s{n}", caller.claims.principal);
                    let ask = Ask::new(n, &format!("s{n}"), delegating(&name), EXP);
                    if let Ok(Applied::DelegationRecorded { claims, .. }) =
                        stamped(&mut state, clock, ask.action(&caller))
                    {
                        callers.push(copy(&claims, ISSUER));
                    }
                    None
                }
                3 => Some(Action::RevokeToken {
                    revoked: Revoked::Token(caller.claims.id.clone()),
                    until: at(30 * 24 * HOUR),
                }),
                4 => Some(as_ancestor(&caller, AncestorTarget::Token(caller.claims.id.clone()))),
                5 => Some(as_ancestor(&caller, own_principal())),
                6 => Some(as_ancestor(&root, own_principal())),
                _ => Some(Action::RevokePrincipal {
                    principal: caller.claims.principal.clone(),
                }),
            };
            if let Some(action) = action {
                let _ = stamped(&mut state, clock, action);
            }
            for (_, record) in state.delegations() {
                if !record.is_live(at(clock), state.now()) {
                    continue;
                }
                let issuer = key(ISSUER);
                let copy = record.claims.token_ref(&issuer, SignerKind::Issuer);
                let refused = state.revocations().refuses_token(&copy);
                prop_assert!(refused.is_none(), "{record:?} outlived {refused:?}");
            }
        }
    }
}

// ---- Limits and rates ----

/// Root `agent-42` delegates `count` leaves `l<start>…`, `per_minute` a minute, from `millis`;
/// returns the time after the last.
fn mint_leaves(state: &mut State, start: u16, count: u16, mut millis: u64) -> u64 {
    let root = root_token("agent-42", "root-jti");
    for n in start..start + count {
        if (n - start).is_multiple_of(30) && n != start {
            millis += 61_000;
        }
        let name = format!("agent-42.l{n}");
        let ask = Ask::new(n, &format!("l{n}"), leaf(&name), 20 * HOUR);
        recorded(stamped(state, millis, ask.action(&root)));
    }
    millis
}

#[test]
fn live_delegations_per_root_are_capped_at_256() {
    let mut state = fleet();
    let count = u16::try_from(MAX_LIVE_PER_LINEAGE).expect("small");
    let millis = mint_leaves(&mut state, 1, count, 1_000) + 61_000;
    let ask = Ask::new(999, "full", leaf("agent-42.full"), 20 * HOUR);
    assert_eq!(
        refused(
            &mut state,
            millis,
            ask.action(&root_token("agent-42", "root-jti"))
        ),
        Rejection::DelegationLimit { limit: Limit::Live }
    );
    // Another lineage is unaffected.
    let other = Ask::new(998, "c1", leaf("agent-43.c1"), 20 * HOUR);
    recorded(stamped(
        &mut state,
        millis,
        other.action(&root_token("agent-43", "jti-43")),
    ));
}

/// A command stamped by a lagging clock runs at the state's time, which `iat`, `recorded_at`,
/// and the start of `kept_until` come from, while the token's life in `kept_until` and whether
/// a record is live are judged by the stamp (ADR-0047, points 5.6, 5.9 and 6.1).
#[test]
fn a_lagging_stamp_records_at_the_effective_time_and_counts_live_by_the_stamp() {
    let mut state = fleet();
    stamped(&mut state, 100_400, Action::Tick).expect("tick");
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    let claims = recorded(stamped(&mut state, 1_000, ask.action(&root)));
    assert_eq!(claims.issued_at, Some(at(100_000)));
    assert_eq!(claims.not_before, at(100_000));
    let record = state
        .delegations()
        .next()
        .map(|(_, record)| record.clone())
        .expect("a record");
    assert_eq!(record.recorded_at, at(100_400));
    assert_eq!(
        record.kept_until,
        at(100_400)
            .saturating_add(fleetring_control::Millis::new(EXP - 1_000))
            .saturating_add(RECORD_MARGIN)
    );

    // A full lineage whose tokens expired at the state's time but not at a lagging stamp is
    // still full for that stamp.
    let mut state = fleet();
    let count = u16::try_from(MAX_LIVE_PER_LINEAGE).expect("small");
    mint_leaves(&mut state, 1, count, 1_000);
    let expired = 20 * HOUR;
    stamped(&mut state, expired + 600_000, Action::Tick).expect("tick");
    assert_eq!(state.delegations().len(), MAX_LIVE_PER_LINEAGE, "kept");
    let ask = Ask::new(999, "full", leaf("agent-42.full"), expired + 2 * HOUR);
    assert_eq!(
        refused(&mut state, expired - 600_000, ask.action(&root)),
        Rejection::DelegationLimit { limit: Limit::Live }
    );
    recorded(stamped(&mut state, expired, ask.action(&root)));
}

/// The state `fleet()` reaches after `agent-42.c7` is recorded at 1,000, with `records` more
/// records of `agent-42.c7` kept until 100 hours, each with its own ID: past its token's
/// `exp` (2 hours), never live, but counted by the backstop.
fn with_kept_records(records: u32) -> State {
    let mut state = fleet();
    c7(&mut state, 1_000);
    let mut value = serde_json::to_value(&state).expect("serializes");
    let delegations = value["delegations"]
        .as_object_mut()
        .expect("a map of records");
    let template = delegations.values().next().cloned().expect("a record");
    for n in 0..records {
        let id = drawn(1_000_000 + n).to_string();
        let mut record = template.clone();
        record["claims"]["id"] = serde_json::Value::from(id.as_str());
        record["kept_until"] = serde_json::Value::from(100 * HOUR);
        delegations.insert(id, record);
    }
    serde_json::from_value(value).expect("deserializes")
}

/// The backstop refuses a delegation once its lineage holds 8,192 unpruned records, live or not
/// (ADR-0047, point 6.3), and not one record before.
#[test]
fn the_backstop_refuses_at_8192_unpruned_records_in_a_lineage() {
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(2, "d1", leaf("agent-42.d1"), 4 * HOUR);
    let below = u32::try_from(MAX_RECORDS_PER_LINEAGE).expect("small") - 2;
    let mut state = with_kept_records(below);
    assert_eq!(state.delegations().len(), MAX_RECORDS_PER_LINEAGE - 1);
    recorded(stamped(&mut state, 3 * HOUR, ask.action(&root)));
    let mut state = with_kept_records(below + 1);
    assert_eq!(state.delegations().len(), MAX_RECORDS_PER_LINEAGE);
    assert_eq!(
        refused(&mut state, 3 * HOUR, ask.action(&root)),
        Rejection::DelegationLimit {
            limit: Limit::Backstop
        }
    );
    // Another lineage's records don't count.
    let other = Ask::new(3, "x", leaf("agent-43.x"), 4 * HOUR);
    recorded(stamped(
        &mut state,
        3 * HOUR,
        other.action(&root_token("agent-43", "jti-43")),
    ));
}

#[test]
fn revoked_delegations_free_their_slots() {
    let mut state = fleet();
    let count = u16::try_from(MAX_LIVE_PER_LINEAGE).expect("small");
    let millis = mint_leaves(&mut state, 1, count, 1_000) + 61_000;
    let first = drawn(1);
    stamped(
        &mut state,
        millis,
        Action::RevokeToken {
            revoked: Revoked::Token(first.into()),
            until: at(20 * HOUR),
        },
    )
    .expect("revoked");
    let ask = Ask::new(999, "next", leaf("agent-42.next"), 20 * HOUR);
    recorded(stamped(
        &mut state,
        millis,
        ask.action(&root_token("agent-42", "root-jti")),
    ));
}

#[test]
fn rate_per_caller_counts_effective_times_and_a_lagging_clock_cant_reopen_it() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let caller_rate = u16::try_from(CALLER_RATE).expect("small");
    for n in 1..=caller_rate {
        let ask = Ask::new(n, &format!("l{n}"), leaf(&format!("agent-42.l{n}")), EXP);
        recorded(stamped(&mut state, 100_000, ask.action(&root)));
    }
    let ask = Ask::new(100, "x", leaf("agent-42.x"), EXP);
    let caller_limit = Rejection::DelegationLimit {
        limit: Limit::CallerRate,
    };
    assert_eq!(
        refused(&mut state, 159_999, ask.action(&root)),
        caller_limit
    );
    // A stamp from a clock that lags by more than the window still runs at the state's time.
    assert_eq!(refused(&mut state, 1_000, ask.action(&root)), caller_limit);
    // Another caller has its own window.
    let other = Ask::new(101, "x", leaf("agent-43.x"), EXP);
    recorded(stamped(
        &mut state,
        100_000,
        other.action(&root_token("agent-43", "j43")),
    ));
    recorded(stamped(&mut state, 160_000, ask.action(&root)));
}

#[test]
fn lineage_rate_counts_delegated_agents_and_never_refuses_the_root() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    // Three delegated agents, recorded a minute before.
    let mut children = Vec::new();
    for (n, segment) in [(1, "a"), (2, "b"), (3, "c")] {
        let name = format!("agent-42.{segment}");
        let ask = Ask::new(n, segment, delegating(&name), 20 * HOUR);
        children.push(copy(
            &recorded(stamped(&mut state, 1_000, ask.action(&root))),
            ISSUER,
        ));
    }
    let mut n = 10;
    let millis = 100_000;
    // Two of them use the lineage's 60.
    for child in &children[..2] {
        for _ in 0..30 {
            n += 1;
            let name = format!("{}.g{n}", child.claims.principal);
            let ask = Ask::new(n, &format!("g{n}"), leaf(&name), 20 * HOUR);
            recorded(stamped(&mut state, millis, ask.action(child)));
        }
    }
    let third = &children[2];
    let ask = Ask::new(
        500,
        "g",
        leaf(&format!("{}.g", third.claims.principal)),
        20 * HOUR,
    );
    assert_eq!(
        refused(&mut state, millis, ask.action(third)),
        Rejection::DelegationLimit {
            limit: Limit::LineageRate
        }
    );
    // The root's own 30 are untouched by its descendants'.
    for k in 0..30 {
        let ask = Ask::new(
            600 + k,
            &format!("r{k}"),
            leaf(&format!("agent-42.r{k}")),
            20 * HOUR,
        );
        recorded(stamped(&mut state, millis, ask.action(&root)));
    }
    let lineage = state
        .rate_windows()
        .find(|(key, _)| {
            key.kind == RateKind::Delegation
                && key.scope == RateScope::Lineage("agent-42".parse().expect("a name"))
        })
        .map(|(_, times)| times.count());
    assert_eq!(lineage, Some(LINEAGE_RATE));
}

#[test]
fn revocation_never_reopens_the_rate_window() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let caller_rate = u16::try_from(CALLER_RATE).expect("small");
    for n in 1..=caller_rate {
        let ask = Ask::new(n, &format!("l{n}"), leaf(&format!("agent-42.l{n}")), EXP);
        recorded(stamped(&mut state, 10_000, ask.action(&root)));
    }
    for n in 1..=caller_rate {
        stamped(
            &mut state,
            20_000,
            Action::RevokeToken {
                revoked: Revoked::Token(drawn(u32::from(n)).into()),
                until: at(EXP),
            },
        )
        .expect("revoked");
    }
    assert_eq!(state.delegations().len(), 0);
    let ask = Ask::new(100, "again", leaf("agent-42.again"), EXP);
    assert_eq!(
        refused(&mut state, 30_000, ask.action(&root)),
        Rejection::DelegationLimit {
            limit: Limit::CallerRate
        }
    );
    recorded(stamped(&mut state, 70_000, ask.action(&root)));
}

/// Mints at the most the rates allow, from the root and two delegated agents, revoking every
/// other child, for `minutes`, and checks the records and operations stay within ADR-0047 point
/// 6.3's bounds at every minute.
fn churn(minutes: u64) {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let mut callers = vec![root.clone()];
    for (n, segment) in [(1, "a"), (2, "b")] {
        let name = format!("agent-42.{segment}");
        let mut ask = Ask::new(n, segment, delegating(&name), 0);
        ask.expires_at = at(24 * HOUR);
        callers.push(copy(
            &recorded(stamped(&mut state, 0, ask.action(&root))),
            ISSUER,
        ));
    }
    let mut n: u32 = 10;
    for minute in 0..minutes {
        let millis = 1_000 + minute * 60_000;
        for caller in &callers {
            for _ in 0..CALLER_RATE {
                n += 1;
                let name = format!("{}.m{n}", caller.claims.principal);
                let mut ask = Ask::new(0, &format!("m{n}"), leaf(&name), 0);
                ask.operation =
                    fleetring_control::fork::OperationId::from_bytes(u128::from(n).to_le_bytes());
                ask.id = drawn(n);
                // Short lives, so records pile up only within their margin.
                ask.expires_at = at(millis - millis % 1_000 + 60_000);
                let outcome = stamped(&mut state, millis, ask.action(caller));
                if let Ok(Applied::DelegationRecorded { claims, .. }) = outcome {
                    if n.is_multiple_of(2) {
                        stamped(
                            &mut state,
                            millis,
                            Action::RevokeToken {
                                revoked: Revoked::Token(claims.id),
                                until: at(millis + HOUR),
                            },
                        )
                        .expect("revoked");
                    }
                } else {
                    assert!(
                        matches!(
                            outcome,
                            Err(Rejection::DelegationLimit {
                                limit: Limit::Live | Limit::CallerRate | Limit::LineageRate
                            })
                        ),
                        "{outcome:?}"
                    );
                }
            }
        }
        let records = state.delegations().len();
        let elapsed = usize::try_from(minute + 1).expect("small");
        assert!(records <= 6_000, "{records} records after {minute} minutes");
        assert!(
            records <= 2 + 90 * elapsed,
            "{records} after {elapsed} minutes"
        );
        let operations = state.delegation_operations().len();
        assert!(
            operations <= 17_000,
            "{operations} operations after {minute} minutes"
        );
        assert!(
            operations <= 2 + 90 * elapsed,
            "{operations} after {elapsed} minutes"
        );
    }
}

#[test]
fn records_per_lineage_stay_bounded_under_mint_and_revoke_churn() {
    churn(8);
}

/// The same churn for 100 minutes, past every record's margin, where the bound is reached.
#[test]
#[ignore = "heavy: about 9,000 delegations; run in the release statistics job"]
fn records_per_lineage_stay_bounded_under_mint_and_revoke_churn_for_100_minutes() {
    churn(100);
}

// ---- Check first, prune last ----

#[test]
fn a_refused_record_delegation_or_revoke_token_leaves_the_state_byte_identical() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_000);
    // Long after c7's record is past its `kept_until`, nothing has pruned it.
    let late = c7.expires_at.as_millis() + 2 * HOUR;
    let ask = Ask::new(5, "x", leaf("agent-42.x"), late + EXP);
    let mut wrong = ask.clone();
    wrong.expires_at = at(late + EXP + 1);
    refused(
        &mut state,
        late,
        wrong.action(&root_token("agent-42", "root-jti")),
    );
    refused(
        &mut state,
        late,
        Action::RevokeToken {
            revoked: Revoked::Token(token("t")),
            until: at(late),
        },
    );
    assert_eq!(state.delegations().len(), 1, "still unpruned");
    // An accepted one prunes it.
    let mut root = root_token("agent-42", "root-jti");
    root.claims.expires_at = Timestamp::MAX;
    recorded(stamped(&mut state, late, ask.action(&root)));
    assert_eq!(state.delegations().len(), 1);
    assert!(
        state
            .delegations()
            .all(|(_, record)| record.claims.id != c7.id)
    );
}

#[test]
fn expired_entries_never_count_toward_a_limit() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let caller_rate = u16::try_from(CALLER_RATE).expect("small");
    for n in 1..=caller_rate {
        let ask = Ask::new(n, &format!("l{n}"), leaf(&format!("agent-42.l{n}")), 61_000);
        recorded(stamped(&mut state, 1_000, ask.action(&root)));
    }
    // A minute on, the window's times and every expired record still sit in the state, and
    // count for nothing.
    let ask = Ask::new(100, "x", leaf("agent-42.x"), EXP);
    let before = state.clone();
    let mut probe = before.clone();
    recorded(stamped(&mut probe, 61_000, ask.action(&root)));
    assert_eq!(before.delegations().len(), usize::from(caller_rate));
    let window = probe
        .rate_windows()
        .find(|(key, _)| key.scope == RateScope::Caller(principal("agent-42")))
        .map(|(_, times)| times.count());
    assert_eq!(window, Some(1), "the old times are pruned once it passes");
}

// ---- Pruning ----

#[test]
fn records_outlive_their_tokens_then_are_pruned() {
    let mut state = fleet();
    let claims = c7(&mut state, 1_000);
    let record = state
        .delegations()
        .next()
        .map(|(_, record)| record.clone())
        .expect("a record");
    // kept_until = effective + (exp − stamp) + 30 s + 1 h.
    assert_eq!(
        record.kept_until,
        at(1_000)
            .saturating_add(fleetring_control::Millis::new(EXP))
            .saturating_add(RECORD_MARGIN)
    );
    assert_eq!(record.recorded_by.as_str(), RECORDER);
    let after_exp = claims.expires_at.as_millis() + 1;
    stamped(&mut state, after_exp, Action::Tick).expect("tick");
    assert_eq!(state.delegations().len(), 1, "kept past its token");
    stamped(&mut state, record.kept_until.as_millis(), Action::Tick).expect("tick");
    assert_eq!(state.delegations().len(), 0);
    let operation = state
        .delegation_operation(&op(1), record.kept_until)
        .expect("the operation outlives its record")
        .kept_until;
    assert_eq!(
        operation,
        record
            .kept_until
            .saturating_add(fleetring_control::Millis::new(HOUR))
    );
    stamped(&mut state, operation.as_millis(), Action::Tick).expect("tick");
    assert!(serde_json::to_value(&state).expect("serializes")["delegation_operations"].is_null());
}

#[test]
fn record_delegation_and_revoke_token_prune_without_a_tick() {
    let mut state = fleet();
    let first = c7(&mut state, 1_000);
    let kept = state
        .delegations()
        .next()
        .map(|(_, r)| r.kept_until)
        .expect("a record");
    let mut root = root_token("agent-42", "root-jti");
    root.claims.expires_at = Timestamp::MAX;
    let ask = Ask::new(2, "d", leaf("agent-42.d"), kept.as_millis() + EXP);
    recorded(stamped(&mut state, kept.as_millis(), ask.action(&root)));
    assert!(state.delegations().all(|(_, r)| r.claims.id != first.id));

    let mut state = fleet();
    c7(&mut state, 1_000);
    stamped(
        &mut state,
        kept.as_millis(),
        Action::RevokeToken {
            revoked: Revoked::Key(key("unrelated")),
            until: Timestamp::MAX,
        },
    )
    .expect("revoked");
    assert_eq!(state.delegations().len(), 0);
}

#[test]
fn tombstone_lasts_one_hour_from_the_revocation() {
    let mut state = fleet();
    let claims = c7(&mut state, 1_000);
    let revoked_at = 10_000;
    stamped(
        &mut state,
        revoked_at,
        Action::RevokeToken {
            revoked: Revoked::Token(claims.id),
            until: at(EXP),
        },
    )
    .expect("revoked");
    let operation: DelegationOperation = state
        .delegation_operation(&op(1), at(revoked_at))
        .cloned()
        .expect("tombstoned");
    assert_eq!(
        operation.outcome,
        OperationOutcome::Revoked { at: at(revoked_at) }
    );
    assert_eq!(operation.kept_until, at(revoked_at + HOUR));
    assert!(delegation::tick_due(&state, at(revoked_at + HOUR)));
    assert!(!delegation::tick_due(&state, at(revoked_at + HOUR - 1)));
    stamped(&mut state, revoked_at + HOUR, Action::Tick).expect("tick");
    assert!(
        state
            .delegation_operation(&op(1), at(revoked_at + HOUR))
            .is_none()
    );
}

#[test]
fn operation_forgotten_after_its_retention_records_anew() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    let first = recorded(stamped(&mut state, 1_000, ask.action(&root)));
    let gone = state
        .delegation_operation(&op(1), at(0))
        .map(|operation| operation.kept_until.as_millis())
        .expect("kept");
    assert_eq!(
        refused(&mut state, gone - 1, ask.action(&root)),
        Rejection::DelegationExpired { operation: op(1) }
    );
    let mut again = ask.clone();
    again.id = drawn(2);
    again.expires_at = at(gone - gone % 1_000 + EXP);
    let mut long_root = root.clone();
    long_root.claims.expires_at = Timestamp::MAX;
    let second = recorded(stamped(&mut state, gone, again.action(&long_root)));
    assert_ne!(second.id, first.id);
}

#[test]
fn fleet_token_revocation_until_covers_the_records_it_prunes() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    c7(&mut state, 1_000);
    let longest = state
        .delegations()
        .map(|(_, record)| record.kept_until)
        .max()
        .expect("a record");
    let revoked = stamped(
        &mut state,
        2_000,
        Action::RevokeToken {
            revoked: Revoked::Token(root.claims.id.clone()),
            until: at(3_000),
        },
    )
    .expect("revoked");
    assert_eq!(
        revoked,
        Applied::TokenRevoked {
            revoked: Revoked::Token(root.claims.id),
            until: longest.saturating_add(delegation::REVOCATION_MARGIN),
        }
    );
    assert_eq!(state.delegations().len(), 0);
}

#[test]
fn tick_due_when_the_earliest_kept_until_passes() {
    let mut state = fleet();
    assert!(!delegation::tick_due(&state, Timestamp::MAX));
    c7(&mut state, 1_000);
    let kept = state
        .delegations()
        .next()
        .map(|(_, r)| r.kept_until)
        .expect("a record");
    assert!(!delegation::tick_due(&state, at(kept.as_millis() - 1)));
    assert!(delegation::tick_due(&state, kept));
    stamped(&mut state, kept.as_millis(), Action::Tick).expect("tick");
    // The operation is next, an hour later.
    assert!(!delegation::tick_due(&state, kept));
    assert!(delegation::tick_due(
        &state,
        kept.saturating_add(fleetring_control::Millis::new(HOUR))
    ));
}

// ---- recorded_by ----

#[test]
fn record_delegation_without_recorded_by_is_refused() {
    let mut state = fleet();
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    let before = state.clone();
    let action = ask.action(&root_token("agent-42", "root-jti"));
    assert_eq!(
        apply(&mut state, Command::new(at(1_000), action)),
        Err(Rejection::Unstamped)
    );
    assert_eq!(state, before);
}

#[test]
fn recorded_by_comes_from_the_stamp_and_replays_identically() {
    let mut state = fleet();
    let root = root_token("agent-42", "root-jti");
    let mut log = Vec::new();
    for (n, by) in [(1_u16, "c1"), (2, "c2")] {
        let ask = Ask::new(n, &format!("c{n}"), leaf(&format!("agent-42.c{n}")), EXP);
        let command = Command::stamped(at(1_000), id(by), ask.action(&root));
        log.push(command.clone());
        apply(&mut state, command).expect("recorded");
    }
    let by: Vec<_> = state
        .delegations()
        .map(|(_, record)| record.recorded_by.as_str().to_owned())
        .collect();
    let mut sorted = by.clone();
    sorted.sort();
    assert_eq!(sorted, ["c1", "c2"]);
    let encoded = serde_json::to_string(&log).expect("serializes");
    let decoded: Vec<Command> = serde_json::from_str(&encoded).expect("decodes");
    let mut replayed = fleet();
    for command in decoded {
        apply(&mut replayed, command).expect("replays");
    }
    assert_eq!(replayed, state);
}

#[test]
fn commands_without_recorded_by_serialize_as_before() {
    let tick = Command::new(at(5), Action::Tick);
    assert_eq!(
        serde_json::to_string(&tick).expect("serializes"),
        r#"{"at":5,"action":"tick"}"#
    );
    let stamped = Command::stamped(at(5), id("c1"), Action::Tick);
    let text = serde_json::to_string(&stamped).expect("serializes");
    assert_eq!(text, r#"{"at":5,"recorded_by":"c1","action":"tick"}"#);
    assert_eq!(
        serde_json::from_str::<Command>(&text).expect("decodes"),
        stamped
    );
}

// ---- The feature ----

/// Software of 0.7, with every feature but `delegation`.
fn without_delegation() -> Software {
    [
        Feature::Undrain,
        Feature::Metadata,
        Feature::Expiry,
        Feature::Fork,
    ]
    .into_iter()
    .fold(
        Software::new(Release::new(0, 7, 0), 1),
        Software::with_feature,
    )
}

#[test]
fn every_delegation_action_needs_the_feature() {
    let root = root_token("agent-42", "root-jti");
    let ask = Ask::new(1, "c7", leaf("agent-42.c7"), EXP);
    let action = ask.action(&root);
    let principal_actions = [
        Action::RevokePrincipal {
            principal: principal("agent-42"),
        },
        Action::RevokeToken {
            revoked: Revoked::principal(principal("agent-42")),
            until: Timestamp::MAX,
        },
        as_ancestor(&root, principal_target("agent-42")),
        as_ancestor(&root, AncestorTarget::Token(root.claims.id.clone())),
    ];
    for needs in std::iter::once(&action).chain(&principal_actions) {
        assert_eq!(needs.features(), [Feature::Delegation], "{needs:?}");
    }
    // Every action that needs it is refused on a fleet with a member that lacks it, and the
    // first accepted one, whichever it is, enables it.
    for needs in &principal_actions {
        let mut state = fleet();
        let member = Member::new(id("old")).with_software(without_delegation());
        apply(&mut state, Command::new(at(0), Action::Join { member })).expect("join");
        assert!(matches!(
            refused(&mut state, 1_000, needs.clone()),
            Rejection::FeatureUnsupported {
                feature: Feature::Delegation,
                ..
            }
        ));
    }
    for enables in [&principal_actions[0], &principal_actions[2]] {
        let mut state = fleet();
        stamped(&mut state, 1_000, enables.clone()).expect("accepted");
        assert!(state.is_enabled(Feature::Delegation), "{enables:?}");
    }
    // Today's revocation targets need nothing, so a 0.7 member keeps revoking.
    for revoked in [Revoked::Token(token("t")), Revoked::Key(key("k"))] {
        assert!(
            Action::RevokeToken {
                revoked,
                until: Timestamp::MAX
            }
            .features()
            .is_empty()
        );
    }

    let mut state = fleet();
    let member = Member::new(id("old")).with_software(without_delegation());
    apply(&mut state, Command::new(at(0), Action::Join { member })).expect("join");
    assert_eq!(
        state.check_admission(&action),
        Err(Rejection::FeatureUnsupported {
            feature: Feature::Delegation,
            id: id("old"),
            release: Release::new(0, 7, 0),
        })
    );
    assert!(matches!(
        refused(&mut state, 1_000, action.clone()),
        Rejection::FeatureUnsupported { .. }
    ));
    let mut state = fleet();
    assert!(!state.is_enabled(Feature::Delegation));
    recorded(stamped(&mut state, 1_000, action));
    assert!(state.is_enabled(Feature::Delegation));
    // From then on a member without it can't join.
    let member = Member::new(id("old")).with_software(without_delegation());
    assert!(matches!(
        apply(&mut state, Command::new(at(2_000), Action::Join { member })),
        Err(Rejection::MissingFeature {
            feature: Feature::Delegation,
            ..
        })
    ));
}

/// A control node replays up to a snapshot's worth of commands its previous binary logged with
/// this binary's `apply`. A 0.7 control node keeps feature names it doesn't know in members'
/// software, so its log may show every member reporting `delegation`; no command it could have
/// logged may then apply differently here. Only `RecordDelegation`, which no 0.7 writer can log,
/// needs the feature, so such a log replays to the state it would without the name.
#[test]
fn a_log_whose_members_report_delegation_replays_as_without_it() {
    use crate::common::{SoftwareMix, fixed_ops, record_with};

    let (log, outcomes, state) = record_with(&fixed_ops(0xDE1E, 2_000), SoftwareMix::Current);
    let strip = |value: &mut serde_json::Value| strip_delegation(value);
    let mut previous = State::new(config());
    let mut previous_outcomes = Vec::new();
    let mut reported = false;
    for command in &log {
        let mut value = serde_json::to_value(command).expect("serializes");
        reported |= value.to_string().contains("\"delegation\"");
        strip(&mut value);
        let stripped: Command = serde_json::from_value(value).expect("decodes");
        previous_outcomes.push(apply(&mut previous, stripped));
    }
    assert!(reported, "the log's members report delegation");
    assert_eq!(previous_outcomes, outcomes);
    assert!(!state.is_enabled(Feature::Delegation));
    let mut current = serde_json::to_value(&state).expect("serializes");
    strip(&mut current);
    assert_eq!(
        current,
        serde_json::to_value(&previous).expect("serializes")
    );
}

/// Removes the name `delegation` from every list of feature names in `value`.
fn strip_delegation(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(items) => {
            items.retain(|item| item.as_str() != Some("delegation"));
            items.iter_mut().for_each(strip_delegation);
        }
        serde_json::Value::Object(map) => map.values_mut().for_each(strip_delegation),
        _ => {}
    }
}

// ---- I8 and byte stability ----

#[test]
fn control_state_never_holds_token_bytes() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_000);
    let ask = Ask::new(2, "r1", leaf("agent-42.c7.r1"), EXP);
    recorded(stamped(&mut state, 2_000, ask.action(&copy(&c7, ISSUER))));
    let snapshot = serde_json::to_string(&state).expect("serializes");
    // A JWT's header and claims are base64url JSON, starting `eyJ`, joined by dots.
    assert!(!snapshot.contains("eyJ"), "{snapshot}");
    let value = serde_json::to_value(&state).expect("serializes");
    fn no_token_field(value: &serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, inner) in map {
                    assert!(
                        !matches!(key.as_str(), "token" | "signature" | "x5c" | "bearer"),
                        "{key}"
                    );
                    no_token_field(inner);
                }
            }
            serde_json::Value::Array(items) => items.iter().for_each(no_token_field),
            _ => {}
        }
    }
    no_token_field(&value);
}

#[test]
fn state_without_delegations_serializes_as_before() {
    let snapshot = include_str!("fixtures/pre_0_8_state.json");
    let state: State = serde_json::from_str(snapshot).expect("loads");
    assert_eq!(state.delegations().len(), 0);
    assert_eq!(state.rate_windows().len(), 0);
    assert_eq!(serde_json::to_string(&state).expect("serializes"), snapshot);
    // A tick and a revocation, which prune delegation state, change nothing else either.
    let mut ticked = state.clone();
    let now = state.now().as_millis() + 1;
    apply(&mut ticked, Command::new(at(now), Action::Tick)).expect("tick");
    apply(
        &mut ticked,
        Command::new(
            at(now),
            Action::RevokeToken {
                revoked: Revoked::Token(token("t")),
                until: at(now + 1_000),
            },
        ),
    )
    .expect("revoked");
    let value = serde_json::to_value(&ticked).expect("serializes");
    for field in ["delegations", "delegation_operations", "rate_windows"] {
        assert!(value.get(field).is_none(), "{field}");
    }
    assert_eq!(
        value["revocations"],
        serde_json::json!([[{"token": "t"}, now + 1_000]])
    );
}

#[test]
fn log_without_delegations_serializes_as_before() {
    for log in [
        include_str!("fixtures/pre_0_8_log.json"),
        include_str!("fixtures/pre_0_8_mixed_log.json"),
    ] {
        let commands: Vec<Command> = serde_json::from_str(log).expect("loads");
        assert!(commands.iter().all(|command| command.recorded_by.is_none()));
        assert_eq!(serde_json::to_string(&commands).expect("serializes"), log);
    }
}

#[test]
fn delegation_state_round_trips_through_a_snapshot() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_000);
    stamped(
        &mut state,
        2_000,
        Action::RevokeToken {
            revoked: Revoked::Token(c7.id),
            until: at(EXP),
        },
    )
    .expect("revoked");
    c7_again(&mut state);
    let snapshot = serde_json::to_string(&state).expect("serializes");
    let restored: State = serde_json::from_str(&snapshot).expect("restores");
    assert_eq!(restored, state);
    assert_eq!(
        serde_json::to_string(&restored).expect("serializes"),
        snapshot
    );
}

fn c7_again(state: &mut State) {
    let ask = Ask::new(2, "c7", delegating("agent-42.c7"), EXP);
    recorded(stamped(
        state,
        3_000,
        ask.action(&root_token("agent-42", "root-jti")),
    ));
}

#[test]
fn a_recorded_childs_claims_pass_what_every_node_checks() {
    let mut state = fleet();
    let c7 = c7(&mut state, 1_500);
    assert_eq!(c7.check_issuer_signed().map(|name| name.depth()), Ok(1));
    assert_eq!(c7.kind, Some(TokenKind::Agent));
    assert_eq!(c7.fleet.as_ref(), "prod");
}
