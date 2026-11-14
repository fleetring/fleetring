//! Principal revocations and the ancestor rule in control state (ADR-0047, point 11): what a
//! principal entry refuses, how `apply` sets `before` and `until`, which entries replace which,
//! what revocations prune, the same-second refusal, the ancestor rule's reach (I10), its cap and
//! rates (I11), the feature boundary, and byte stability.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use fleetring_control::delegation::{
    ANCESTOR_CALLER_RATE, ANCESTOR_LINEAGE_RATE, AgentName, AncestorTarget, AncestorViolation,
    CallerToken, ClaimField, Forgery, MAX_ANCESTOR_ENTRIES_PER_LINEAGE, OperationOutcome,
    RECORD_MARGIN, REVOCATION_MARGIN, ROOT_PRINCIPAL_LIFE, RevocationLimit, SignerKind,
    TOMBSTONE_LIFE, TokenClaims,
};
use fleetring_control::{
    Action, Added, Applied, Command, Entry, Feature, FleetRef, Member, Rejection, Release,
    Revocations, Revoked, Software, State, Timestamp, TokenId, TokenRef, apply,
};

use crate::common::delegation::{
    AUTHORITY, Ask, HOUR, ISSUER, as_ancestor, copy, delegate, delegating, fleet, key, leaf,
    principal, principal_target, recorded, refused, root_token, stamped, token,
};
use crate::common::{at, id};

fn agent(name: &str) -> AgentName {
    name.parse().expect("an agent name")
}

fn root() -> CallerToken {
    root_token("agent-42", "root-jti")
}

fn token_target(id: &TokenId) -> AncestorTarget {
    AncestorTarget::Token(id.clone())
}

/// The entry that refuses the copy of `claims` an issuer signed, if one does.
fn refusal(state: &State, claims: &TokenClaims) -> Option<Revoked> {
    let signer = SignerKind::for_principal(&claims.principal);
    let kid = key(match signer {
        SignerKind::Authority => AUTHORITY,
        SignerKind::Issuer => ISSUER,
    });
    state
        .revocations()
        .refuses_token(&claims.token_ref(&kid, signer))
}

/// The `before` and `until` a principal revocation chose.
fn principal_revoked(outcome: Result<Applied, Rejection>) -> (Timestamp, Timestamp) {
    match outcome {
        Ok(Applied::PrincipalRevoked { before, until, .. }) => (before, until),
        other => panic!("expected a principal revocation: {other:?}"),
    }
}

/// The `until` a token revocation chose.
fn token_revoked(outcome: Result<Applied, Rejection>) -> Timestamp {
    match outcome {
        Ok(Applied::TokenRevoked { until, .. }) => until,
        other => panic!("expected a token revocation: {other:?}"),
    }
}

/// The live entries of `root`'s lineage that the cap counts: every `Ancestor` entry but the
/// root's own principal and the root tokens it revoked.
fn counted(state: &State, root: &str) -> usize {
    state
        .revocations()
        .iter()
        .filter(|(revoked, entry)| {
            entry.until > state.now()
                && match &entry.added {
                    Added::Ancestor { root: lineage, .. } => {
                        lineage.as_str() == root
                            && match revoked {
                                Revoked::Principal { principal, .. } => principal.as_str() != root,
                                Revoked::Token(_) => entry.of.is_some(),
                                _ => true,
                            }
                    }
                    _ => false,
                }
        })
        .count()
}

/// A lineage: `agent-42` delegates `c7` at 1 s, which delegates `r1` and `r2` at 2 s, and `r1`
/// delegates `x` at 3 s.
struct Lineage {
    state: State,
    c7: TokenClaims,
    r1: TokenClaims,
    r2: TokenClaims,
    x: TokenClaims,
}

fn lineage() -> Lineage {
    let mut state = fleet();
    let c7 = delegate(
        &mut state,
        1_000,
        1,
        &root(),
        "c7",
        delegating("agent-42.c7"),
    );
    let r1 = delegate(
        &mut state,
        2_000,
        2,
        &copy(&c7, ISSUER),
        "r1",
        delegating("agent-42.c7.r1"),
    );
    let r2 = delegate(
        &mut state,
        2_000,
        3,
        &copy(&c7, ISSUER),
        "r2",
        leaf("agent-42.c7.r2"),
    );
    let x = delegate(
        &mut state,
        3_000,
        4,
        &copy(&r1, ISSUER),
        "x",
        leaf("agent-42.c7.r1.x"),
    );
    Lineage {
        state,
        c7,
        r1,
        r2,
        x,
    }
}

/// The longest `kept_until` among the records named `names` or below them.
fn longest_kept(state: &State, name: &str) -> Timestamp {
    let name = principal(name);
    state
        .delegations()
        .filter(|(_, record)| record.principal().is_at_or_below(&name))
        .map(|(_, record)| record.kept_until)
        .max()
        .expect("a record")
}

// ---- What a principal entry refuses ----

#[test]
fn principal_revocation_matches_agent_42_and_below_never_agent_420() {
    let Lineage {
        mut state, c7, x, ..
    } = lineage();
    let other = root_token("agent-420", "root-420");
    let other_child = delegate(&mut state, 3_000, 9, &other, "c1", leaf("agent-420.c1"));
    let four = root_token("agent-4", "root-4");
    // `agent-42` revokes itself, ending its subtree.
    principal_revoked(stamped(
        &mut state,
        10_000,
        as_ancestor(&root(), principal_target("agent-42")),
    ));
    let by = Revoked::principal(principal("agent-42"));
    assert_eq!(refusal(&state, &root().claims), Some(by.clone()));
    assert_eq!(refusal(&state, &c7), Some(by.clone()));
    assert_eq!(refusal(&state, &x), Some(by.clone()));
    assert_eq!(refusal(&state, &other.claims), None);
    assert_eq!(refusal(&state, &other_child), None);
    assert_eq!(refusal(&state, &four.claims), None);
    // `apply` agrees: agent-42 can't delegate, agent-420 still can.
    let ask = Ask::new(20, "c9", leaf("agent-42.c9"), 11_000 + 2 * HOUR);
    assert_eq!(
        refused(&mut state, 11_000, ask.action(&root())),
        Rejection::CallerRevoked { by }
    );
    delegate(&mut state, 11_000, 21, &other, "c2", leaf("agent-420.c2"));
}

#[test]
fn principal_entries_are_one_per_fleet_and_principal_raised_in_place() {
    let Lineage { mut state, .. } = lineage();
    let c7 = Revoked::principal(principal("agent-42.c7"));
    let (first, first_until) = principal_revoked(stamped(
        &mut state,
        10_000,
        as_ancestor(&root(), principal_target("agent-42.c7")),
    ));
    let entry = state.revocations().get(&c7).expect("an entry").clone();
    assert_eq!(entry.before, Some(first));
    assert!(matches!(entry.added, Added::Ancestor { .. }));

    // The operator revokes it again: still one entry, raised, and now added with `fleet`.
    let (second, second_until) = principal_revoked(stamped(
        &mut state,
        20_000,
        Action::RevokePrincipal {
            principal: principal("agent-42.c7"),
        },
    ));
    assert!(second > first && second_until >= first_until);
    let entry = state.revocations().get(&c7).expect("an entry").clone();
    assert_eq!(
        (entry.added.clone(), entry.before),
        (Added::Fleet, Some(second))
    );

    // `agent-42` delegates `c7` again, then revokes it under the ancestor rule: the entry is
    // raised once more and stays `Fleet`.
    delegate(&mut state, 21_000, 30, &root(), "c7", leaf("agent-42.c7"));
    let (third, _) = principal_revoked(stamped(
        &mut state,
        22_000,
        as_ancestor(&root(), principal_target("agent-42.c7")),
    ));
    let entry = state.revocations().get(&c7).expect("an entry");
    assert_eq!(
        (entry.added.clone(), entry.before),
        (Added::Fleet, Some(third))
    );
    assert!(entry.until >= second_until);
    let principals = state
        .revocations()
        .iter()
        .filter(|(revoked, _)| matches!(revoked, Revoked::Principal { .. }))
        .count();
    assert_eq!(principals, 1);
}

#[test]
fn before_is_the_effective_time_and_the_leeway_is_by_signer() {
    let mut state = fleet();
    stamped(&mut state, 10_000, Action::Tick).expect("tick");
    // Stamped by a clock behind control time: `before` is the effective time.
    let (before, _) = principal_revoked(stamped(
        &mut state,
        8_000,
        Action::RevokePrincipal {
            principal: principal("agent-42"),
        },
    ));
    assert_eq!(before, at(10_000));
    let list = state.revocations();
    let (id, kid) = (token("t"), key("k"));
    let issued = |name: &str, signer: SignerKind, millis: u64| {
        let name = principal(name);
        list.refuses_token(&TokenRef {
            id: &id,
            kid: &kid,
            chain: &[],
            principal: &name,
            issued_at: Some(at(millis)),
            signer,
        })
        .is_some()
    };
    // A root token's `iat` comes from its signer's clock: 30 s of leeway.
    assert!(issued("agent-42", SignerKind::Authority, 40_000));
    assert!(!issued("agent-42", SignerKind::Authority, 40_001));
    // A delegated token's `iat` is control time: none.
    assert!(issued("agent-42.c7", SignerKind::Issuer, 10_000));
    assert!(!issued("agent-42.c7", SignerKind::Issuer, 10_001));
}

#[test]
fn missing_iat_counts_as_issued_before_every_before() {
    let list: Revocations = [(
        Revoked::principal(principal("agent-42")),
        Entry::fleet_principal(Timestamp::MAX, at(1)),
    )]
    .into_iter()
    .collect();
    let (id, kid) = (token("t"), key("k"));
    for (name, signer) in [
        ("agent-42", SignerKind::Authority),
        ("agent-42.c7", SignerKind::Issuer),
    ] {
        let name = principal(name);
        let unsaid = TokenRef {
            id: &id,
            kid: &kid,
            chain: &[],
            principal: &name,
            issued_at: None,
            signer,
        };
        assert!(list.refuses_token(&unsaid).is_some(), "{name}");
    }
}

// ---- `before` and `until` ----

#[test]
fn until_follows_the_name_and_what_the_entry_covers() {
    let lineage = lineage();
    let now = 10_000;

    // A root agent's own principal: 30 days, 30 seconds, and an hour.
    let mut state = lineage.state.clone();
    let (_, until) = principal_revoked(stamped(
        &mut state,
        now,
        as_ancestor(&root(), principal_target("agent-42")),
    ));
    assert_eq!(until, at(now).saturating_add(ROOT_PRINCIPAL_LIFE));

    // A delegated principal: the longest `kept_until` it covers, plus 30 seconds.
    let mut state = lineage.state.clone();
    let covered = longest_kept(&state, "agent-42.c7.r1");
    let (_, until) = principal_revoked(stamped(
        &mut state,
        now,
        as_ancestor(&root(), principal_target("agent-42.c7.r1")),
    ));
    assert_eq!(until, covered.saturating_add(REVOCATION_MARGIN));

    // A delegation's token: the longest `kept_until` of it and the records below it, plus 30
    // seconds.
    let mut state = lineage.state.clone();
    let covered = longest_kept(&state, "agent-42.c7");
    let until = token_revoked(stamped(
        &mut state,
        now,
        as_ancestor(&root(), token_target(&lineage.c7.id)),
    ));
    assert_eq!(until, covered.saturating_add(REVOCATION_MARGIN));

    // The caller's own root token: its `exp` counted from the command's stamp, plus 30 seconds
    // and an hour, from the effective time.
    let mut state = lineage.state.clone();
    stamped(&mut state, now, Action::Tick).expect("tick");
    let caller = root();
    let until = token_revoked(stamped(
        &mut state,
        now - 4_000,
        as_ancestor(&caller, token_target(&caller.claims.id)),
    ));
    let life = caller.claims.expires_at.as_millis() - (now - 4_000);
    assert_eq!(
        until,
        at(now + life).saturating_add(RECORD_MARGIN),
        "the stamp, not the effective time, counts the life"
    );
    let entry = state
        .revocations()
        .get(&Revoked::Token(caller.claims.id.clone()))
        .expect("an entry");
    assert_eq!(entry.of, None, "a root token's entry names no delegation");
}

#[test]
fn fleet_principal_revocation_until_covers_the_records_it_prunes() {
    let Lineage { mut state, .. } = lineage();
    let covered = longest_kept(&state, "agent-42.c7");
    let (before, until) = principal_revoked(stamped(
        &mut state,
        10_000,
        Action::RevokePrincipal {
            principal: principal("agent-42.c7"),
        },
    ));
    assert_eq!(before, at(10_000));
    assert_eq!(until, at(10_000).saturating_add(ROOT_PRINCIPAL_LIFE));
    assert!(until >= covered.saturating_add(REVOCATION_MARGIN));
    assert!(
        state
            .delegations()
            .all(|(_, record)| !record.principal().is_at_or_below(&principal("agent-42.c7")))
    );
}

// ---- Replacement ----

#[test]
fn only_principal_entries_replace_and_never_root_token_or_fleet_entries() {
    let Lineage {
        mut state,
        c7,
        r2,
        x,
        ..
    } = lineage();
    let r3 = delegate(
        &mut state,
        3_000,
        5,
        &copy(&c7, ISSUER),
        "r3",
        leaf("agent-42.c7.r3"),
    );
    let c7_copy = copy(&c7, ISSUER);
    // Ancestor entries: `c7` ends `x` by token and `r2` by principal.
    let x_until = token_revoked(stamped(
        &mut state,
        4_000,
        as_ancestor(&c7_copy, token_target(&x.id)),
    ));
    principal_revoked(stamped(
        &mut state,
        4_000,
        as_ancestor(&c7_copy, principal_target("agent-42.c7.r2")),
    ));
    // A `Fleet` entry in the subtree, and the root's own token revoked by the root.
    stamped(
        &mut state,
        5_000,
        Action::RevokeToken {
            revoked: Revoked::Token(r3.id.clone()),
            until: at(5_000 + HOUR),
        },
    )
    .expect("revoked");
    token_revoked(stamped(
        &mut state,
        6_000,
        as_ancestor(&root(), token_target(&root().claims.id)),
    ));
    assert_eq!(state.revocations().len(), 4);

    // The root's principal replaces exactly the two ancestor entries below it.
    let (_, until) = principal_revoked(stamped(
        &mut state,
        7_000,
        as_ancestor(&root(), principal_target("agent-42")),
    ));
    let keys: BTreeSet<Revoked> = state
        .revocations()
        .iter()
        .map(|(revoked, _)| revoked.clone())
        .collect();
    assert_eq!(
        keys,
        BTreeSet::from([
            Revoked::Token(root().claims.id),
            Revoked::Token(r3.id.clone()),
            Revoked::principal(principal("agent-42")),
        ])
    );
    assert!(until >= x_until);
    // Whatever the replaced entries refused, the new one refuses too.
    for claims in [&x, &r2] {
        assert!(refusal(&state, claims).is_some(), "{}", claims.principal);
    }
}

/// A principal entry added with `fleet` below an ancestor-rule principal revocation stays: only
/// `Ancestor` entries are replaced, so the operator's entry keeps its own `until` (ADR-0047,
/// point 11.4).
#[test]
fn a_fleet_principal_entry_below_is_never_replaced() {
    let Lineage { mut state, .. } = lineage();
    let r2 = Revoked::principal(principal("agent-42.c7.r2"));
    let (_, fleet_until) = principal_revoked(stamped(
        &mut state,
        4_000,
        Action::RevokePrincipal {
            principal: principal("agent-42.c7.r2"),
        },
    ));
    principal_revoked(stamped(
        &mut state,
        5_000,
        as_ancestor(&root(), principal_target("agent-42.c7")),
    ));
    let kept = state.revocations().get(&r2).expect("the fleet entry stays");
    assert_eq!(kept.added, Added::Fleet);
    assert_eq!(kept.until, fleet_until);
}

/// A twin, a second delegation recorded at the caller's own name, isn't below the caller: a
/// delegated caller can revoke its own token, never its twin's (ADR-0047, point 11.3).
#[test]
fn a_twin_at_the_callers_own_name_is_not_below_it() {
    let mut state = fleet();
    let d1 = delegate(&mut state, 1_000, 1, &root(), "c7", leaf("agent-42.c7"));
    let d2 = delegate(&mut state, 1_000, 2, &root(), "c7", leaf("agent-42.c7"));
    assert_eq!(d1.principal, d2.principal);
    assert_eq!(
        refused(
            &mut state,
            2_000,
            as_ancestor(&copy(&d1, ISSUER), token_target(&d2.id))
        ),
        Rejection::NoLiveDelegation { id: d2.id.clone() }
    );
    assert_eq!(refusal(&state, &d2), None);
}

/// `tick_due` covers an ancestor-rule entry's `until`, even with no record or operation left
/// to prune (ADR-0047, point 11.5).
#[test]
fn tick_due_when_an_ancestor_entry_is_due() {
    use fleetring_control::delegation::tick_due;

    let mut state = fleet();
    let c7 = delegate(&mut state, 1_000, 1, &root(), "c7", leaf("agent-42.c7"));
    let until = token_revoked(stamped(
        &mut state,
        2_000,
        as_ancestor(&root(), token_target(&c7.id)),
    ));
    // The operation's tombstone goes an hour after the revocation, long before the entry.
    stamped(&mut state, 2_000 + HOUR, Action::Tick).expect("tick");
    assert_eq!(state.delegations().len(), 0);
    assert_eq!(state.delegation_operations().len(), 0);
    assert!(!tick_due(&state, at(until.as_millis() - 1)));
    assert!(tick_due(&state, until));
    stamped(&mut state, until.as_millis(), Action::Tick).expect("tick");
    assert!(state.revocations().is_empty());
    assert!(!tick_due(&state, Timestamp::MAX));
}

/// `42.c7` revoked by ID, delegated again, a grandchild under each, then `agent-42.c7`'s
/// principal revoked: neither grandchild revives (ADR-0047, I5).
#[test]
fn token_entry_replaces_nothing_with_a_reused_segment() {
    let mut state = fleet();
    let d1 = delegate(
        &mut state,
        1_000,
        1,
        &root(),
        "c7",
        delegating("agent-42.c7"),
    );
    let g1 = delegate(
        &mut state,
        2_000,
        2,
        &copy(&d1, ISSUER),
        "r1",
        leaf("agent-42.c7.r1"),
    );
    token_revoked(stamped(
        &mut state,
        3_000,
        as_ancestor(&root(), token_target(&d1.id)),
    ));
    let d1_entry = Revoked::Token(d1.id.clone());
    assert_eq!(
        state
            .revocations()
            .get(&d1_entry)
            .and_then(|entry| entry.of.clone()),
        Some(agent("agent-42.c7"))
    );
    let d2 = delegate(
        &mut state,
        4_000,
        3,
        &root(),
        "c7",
        delegating("agent-42.c7"),
    );
    let g2 = delegate(
        &mut state,
        5_000,
        4,
        &copy(&d2, ISSUER),
        "r1",
        leaf("agent-42.c7.r1"),
    );
    assert_eq!(g1.principal, g2.principal, "the segment is reused");
    assert_eq!(refusal(&state, &g1), Some(d1_entry.clone()));
    assert_eq!(refusal(&state, &g2), None);

    // Another token entry, for `d2`, replaces nothing: `d1`'s stays and still ends `g1`.
    let mut by_token = state.clone();
    token_revoked(stamped(
        &mut by_token,
        6_000,
        as_ancestor(&root(), token_target(&d2.id)),
    ));
    assert!(by_token.revocations().contains(&d1_entry));
    assert_eq!(refusal(&by_token, &g1), Some(d1_entry.clone()));
    assert!(refusal(&by_token, &g2).is_some());

    // The principal replaces `d1`'s entry, and refuses every one of them by name and time.
    principal_revoked(stamped(
        &mut state,
        6_000,
        as_ancestor(&root(), principal_target("agent-42.c7")),
    ));
    assert!(!state.revocations().contains(&d1_entry));
    for claims in [&d1, &g1, &d2, &g2] {
        assert_eq!(
            refusal(&state, claims),
            Some(Revoked::principal(principal("agent-42.c7"))),
            "{} revived",
            claims.id
        );
    }
}

/// A principal entry that replaces a token entry is kept at least as long as it, even when
/// what the principal prunes itself expires much sooner (ADR-0047, point 11.4).
#[test]
fn a_replacing_entry_is_kept_as_long_as_what_it_replaces() {
    let mut state = fleet();
    let d1 = recorded(stamped(
        &mut state,
        1_000,
        Ask::new(1, "c7", leaf("agent-42.c7"), 20 * HOUR).action(&root()),
    ));
    let first = token_revoked(stamped(
        &mut state,
        2_000,
        as_ancestor(&root(), token_target(&d1.id)),
    ));
    let later = 70_000;
    recorded(stamped(
        &mut state,
        later,
        Ask::new(2, "c7", leaf("agent-42.c7"), later + 120_000).action(&root()),
    ));
    let (_, until) = principal_revoked(stamped(
        &mut state,
        later + 1_000,
        as_ancestor(&root(), principal_target("agent-42.c7")),
    ));
    assert!(
        state
            .revocations()
            .get(&Revoked::Token(d1.id.clone()))
            .is_none()
    );
    assert!(
        until >= first,
        "the replacing entry ends at {until:?}, before the {first:?} it replaced"
    );
}

// ---- Pruning ----

#[test]
fn revocation_prunes_what_it_ends_and_tombstones_their_operations() {
    let lineage = lineage();
    let cases: [(&str, Action); 3] = [
        (
            "ancestor principal",
            as_ancestor(&root(), principal_target("agent-42.c7.r1")),
        ),
        (
            "ancestor token",
            as_ancestor(&copy(&lineage.c7, ISSUER), token_target(&lineage.r1.id)),
        ),
        (
            "fleet principal",
            Action::RevokePrincipal {
                principal: principal("agent-42.c7.r1"),
            },
        ),
    ];
    for (case, action) in cases {
        let mut state = lineage.state.clone();
        stamped(&mut state, 10_000, action).expect(case);
        let left: BTreeSet<String> = state
            .delegations()
            .map(|(_, record)| record.principal().to_string())
            .collect();
        let kept = BTreeSet::from(["agent-42.c7".to_owned(), "agent-42.c7.r2".to_owned()]);
        assert_eq!(left, kept, "{case}");
        for ended in [&lineage.r1, &lineage.x] {
            let (_, operation) = state
                .delegation_operations()
                .find(|(_, operation)| operation.delegation.as_token_id() == &ended.id)
                .expect("the operation is kept");
            assert_eq!(
                operation.outcome,
                OperationOutcome::Revoked { at: at(10_000) },
                "{case}"
            );
            assert_eq!(
                operation.kept_until,
                at(10_000).saturating_add(TOMBSTONE_LIFE)
            );
        }
        // A resend of `r1`'s operation is refused by its tombstone.
        let resend = Ask::new(
            2,
            "r1",
            delegating("agent-42.c7.r1"),
            lineage.r1.expires_at.as_millis(),
        )
        .action(&copy(&lineage.c7, ISSUER));
        assert!(matches!(
            refused(&mut state, 11_000, resend),
            Rejection::DelegationRevoked { .. }
        ));
    }
}

// ---- I4: the same second ----

#[test]
fn same_second_redelegation_of_a_revoked_name_is_refused_as_retryable() {
    let mut state = fleet();
    delegate(&mut state, 1_000, 1, &root(), "c7", leaf("agent-42.c7"));
    let (before, _) = principal_revoked(stamped(
        &mut state,
        5_300,
        as_ancestor(&root(), principal_target("agent-42.c7")),
    ));
    assert_eq!(before, at(5_300));
    // Within the same second, the child's `iat` would be 5 s, at or before `before`.
    let again = Ask::new(2, "c7", leaf("agent-42.c7"), 6_000 + 2 * HOUR).action(&root());
    for millis in [5_800, 5_000] {
        assert_eq!(
            refused(&mut state, millis, again.clone()),
            Rejection::RevokedThisSecond {
                principal: agent("agent-42.c7")
            }
        );
    }
    // A second later the retry records a token no node refuses.
    let claims = recorded(stamped(&mut state, 6_000, again));
    assert_eq!(claims.issued_at, Some(at(6_000)));
    assert_eq!(refusal(&state, &claims), None);
}

// ---- I10: the ancestor rule's reach ----

#[test]
fn ancestor_rule_allows_self_own_token_and_recorded_delegations_below() {
    let lineage = lineage();
    let c7 = copy(&lineage.c7, ISSUER);
    let allowed: [(&str, Action, Revoked, Option<&str>); 7] = [
        (
            "root's own token",
            as_ancestor(&root(), token_target(&root().claims.id)),
            Revoked::Token(root().claims.id),
            None,
        ),
        (
            "root's own principal",
            as_ancestor(&root(), principal_target("agent-42")),
            Revoked::principal(principal("agent-42")),
            None,
        ),
        (
            "root, a grandchild's token",
            as_ancestor(&root(), token_target(&lineage.r1.id)),
            Revoked::Token(lineage.r1.id.clone()),
            Some("agent-42.c7.r1"),
        ),
        (
            "root, a great-grandchild's principal",
            as_ancestor(&root(), principal_target("agent-42.c7.r1.x")),
            Revoked::principal(principal("agent-42.c7.r1.x")),
            None,
        ),
        (
            "c7's own token",
            as_ancestor(&c7, token_target(&lineage.c7.id)),
            Revoked::Token(lineage.c7.id.clone()),
            Some("agent-42.c7"),
        ),
        (
            "c7's own principal",
            as_ancestor(&c7, principal_target("agent-42.c7")),
            Revoked::principal(principal("agent-42.c7")),
            None,
        ),
        (
            "c7, a child's token",
            as_ancestor(&c7, token_target(&lineage.r2.id)),
            Revoked::Token(lineage.r2.id.clone()),
            Some("agent-42.c7.r2"),
        ),
    ];
    for (case, action, revoked, of) in allowed {
        let mut state = lineage.state.clone();
        let Action::RevokeAsAncestor { caller, .. } = &action else {
            unreachable!("built as an ancestor revocation");
        };
        let by = agent(caller.claims.principal.as_str());
        stamped(&mut state, 10_000, action).expect(case);
        let entry = state.revocations().get(&revoked).expect(case);
        assert_eq!(
            entry.added,
            Added::Ancestor {
                root: agent("agent-42"),
                by
            },
            "{case}"
        );
        assert_eq!(entry.of, of.map(agent), "{case}");
    }
}

#[test]
fn ancestor_rule_refuses_siblings_parents_other_roots_unrecorded_names_and_keys() {
    let Lineage {
        mut state,
        c7,
        r1,
        r2,
        ..
    } = lineage();
    let c8 = delegate(&mut state, 3_000, 6, &root(), "c8", leaf("agent-42.c8"));
    let other = root_token("agent-420", "root-420");
    let other_child = delegate(&mut state, 3_000, 7, &other, "c7", leaf("agent-420.c7"));
    let c7_copy = copy(&c7, ISSUER);
    let r2_copy = copy(&r2, ISSUER);
    let not_below = Rejection::NotAncestor {
        reason: AncestorViolation::NotBelowCaller,
    };
    let not_found = |id: &TokenId| Rejection::NoLiveDelegation { id: id.clone() };
    let cases: Vec<(&str, Action, Rejection)> = vec![
        (
            "a child, its parent",
            as_ancestor(&c7_copy, principal_target("agent-42")),
            not_below.clone(),
        ),
        (
            "a child, its parent's token",
            as_ancestor(&c7_copy, token_target(&root().claims.id)),
            not_found(&root().claims.id),
        ),
        (
            "a sibling",
            as_ancestor(&c7_copy, principal_target("agent-42.c8")),
            not_below.clone(),
        ),
        (
            "a sibling's token",
            as_ancestor(&c7_copy, token_target(&c8.id)),
            not_found(&c8.id),
        ),
        (
            "a sibling's child",
            as_ancestor(&r2_copy, token_target(&r1.id)),
            not_found(&r1.id),
        ),
        (
            "another root",
            as_ancestor(&root(), principal_target("agent-420")),
            not_below.clone(),
        ),
        (
            "another root's child",
            as_ancestor(&root(), token_target(&other_child.id)),
            not_found(&other_child.id),
        ),
        (
            "a name it is a byte prefix of",
            as_ancestor(&root(), principal_target("agent-420.c7")),
            not_below.clone(),
        ),
        (
            "an unrecorded name",
            as_ancestor(&root(), principal_target("agent-42.c9")),
            not_below.clone(),
        ),
        (
            "an unrecorded token",
            as_ancestor(&root(), token_target(&token("never-issued"))),
            not_found(&token("never-issued")),
        ),
        (
            "a key's ID as a token",
            as_ancestor(&root(), token_target(&token(AUTHORITY))),
            not_found(&token(AUTHORITY)),
        ),
    ];
    for (case, action, expected) in cases {
        assert_eq!(refused(&mut state, 10_000, action), expected, "{case}");
    }
    // `agent-420` itself reaches the child `agent-42` can't.
    let action = as_ancestor(&other, principal_target("agent-420.c7"));
    stamped(&mut state, 10_000, action).expect("its own child");
    // A key is never a target: the action has no way to name one.
    assert!(serde_json::from_str::<AncestorTarget>(r#"{"key":"authority"}"#).is_err());
    // The refusal of an unknown token says nothing about it.
    assert_eq!(
        not_found(&token("x")).to_string(),
        "no live delegation with that token id"
    );
}

/// A target needs only to be recorded: a delegation whose `exp` the leader's clock has passed
/// is kept for its margin, and nodes whose clocks lag may still accept it, so an ancestor can
/// still revoke it, by token or by principal (ADR-0047, point 11.3).
#[test]
fn an_expired_but_kept_delegation_can_still_be_revoked() {
    let mut state = fleet();
    let c7 = recorded(stamped(
        &mut state,
        1_000,
        Ask::new(1, "c7", leaf("agent-42.c7"), 120_000).action(&root()),
    ));
    let d1 = recorded(stamped(
        &mut state,
        1_000,
        Ask::new(2, "d1", leaf("agent-42.d1"), 120_000).action(&root()),
    ));
    let e1 = recorded(stamped(
        &mut state,
        1_000,
        Ask::new(3, "e1", leaf("agent-42.e1"), 120_000).action(&root()),
    ));
    // Once a record's margin has passed, it is no longer a target, though nothing pruned it.
    let mut forgotten = state.clone();
    let gone = 120_000 + RECORD_MARGIN.get() + 1_000;
    assert_eq!(
        refused(
            &mut forgotten,
            gone,
            as_ancestor(&root(), token_target(&e1.id))
        ),
        Rejection::NoLiveDelegation { id: e1.id.clone() }
    );
    let past = 130_000;
    token_revoked(stamped(
        &mut state,
        past,
        as_ancestor(&root(), token_target(&c7.id)),
    ));
    assert!(refusal(&state, &c7).is_some());
    principal_revoked(stamped(
        &mut state,
        past,
        as_ancestor(&root(), principal_target("agent-42.d1")),
    ));
    assert!(refusal(&state, &d1).is_some());
}

#[test]
fn services_cannot_use_the_ancestor_rule() {
    let mut state = fleet();
    let mut workload = root_token("workload", "svc-jti");
    workload.claims.kind = None;
    for target in [
        token_target(&workload.claims.id),
        principal_target("agent-42"),
    ] {
        assert_eq!(
            refused(&mut state, 1_000, as_ancestor(&workload, target)),
            Rejection::NotAncestor {
                reason: AncestorViolation::CallerNotAgent
            }
        );
    }
    // No target can name a service's child, so `workload` can never end `workload.prod`.
    assert!(serde_json::from_str::<AncestorTarget>(r#"{"principal":"workload.prod"}"#).is_err());
}

#[test]
fn forged_caller_claims_cannot_revoke() {
    let Lineage {
        mut state, c7, r1, ..
    } = lineage();
    let target = principal_target("agent-42.c7.r1");
    // A delegation nobody recorded.
    let mut fabricated = copy(&c7, ISSUER);
    fabricated.claims.id = token("AAAAAAAAAAAAAAAAAAAAAA");
    assert_eq!(
        refused(&mut state, 10_000, as_ancestor(&fabricated, target.clone())),
        Rejection::ForgedLineage {
            reason: Forgery::Unrecorded
        }
    );
    // A real delegation's ID with claims the control plane didn't record.
    let mut wider = copy(&c7, ISSUER);
    wider.claims.scopes = delegating("agent-42");
    assert_eq!(
        refused(&mut state, 10_000, as_ancestor(&wider, target.clone())),
        Rejection::ForgedLineage {
            reason: Forgery::Differs {
                field: ClaimField::Scopes
            }
        }
    );
    // A delegation past its expiry.
    let late = c7.expires_at.as_millis();
    assert_eq!(
        refused(&mut state, late, as_ancestor(&copy(&c7, ISSUER), target)),
        Rejection::ForgedLineage {
            reason: Forgery::NotLive
        }
    );
    assert!(state.revocations().is_empty());
    assert!(refusal(&state, &r1).is_none());
}

// ---- I11: the cap and the rates ----

/// Who added an entry the cap counts, which picks the budget it counts in: the root itself, or
/// one of its delegated agents (ADR-0047, point 11.6, as amended 2026-11-13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Adder {
    Root,
    Descendant,
}

/// The live entries `agent-42`'s lineage holds in `adder`'s budget.
fn budget(state: &State, adder: Adder) -> usize {
    state
        .revocations()
        .iter()
        .filter(|(revoked, entry)| {
            entry.until > state.now()
                && match &entry.added {
                    Added::Ancestor { root, by } => {
                        root.as_str() == "agent-42"
                            && (by == root) == (adder == Adder::Root)
                            && match revoked {
                                Revoked::Principal { principal, .. } => {
                                    principal.as_str() != "agent-42"
                                }
                                Revoked::Token(_) => entry.of.is_some(),
                                _ => true,
                            }
                    }
                    _ => false,
                }
        })
        .count()
}

/// `caller` delegates `count` leaves `l0`, `l1`, and so on, and ends each by token, at its rate
/// of 30 a minute from `millis`, drawing operations from `first`; returns the time a minute
/// after the last.
fn fill(state: &mut State, caller: &CallerToken, count: u16, first: u16, mut millis: u64) -> u64 {
    for n in 0..count {
        if n > 0 && usize::from(n).is_multiple_of(ANCESTOR_CALLER_RATE) {
            millis += 61_000;
        }
        let segment = format!("l{n}");
        let name = format!("{}.{segment}", caller.claims.principal);
        let child = delegate(state, millis, first + n, caller, &segment, leaf(&name));
        token_revoked(stamped(
            state,
            millis,
            as_ancestor(caller, token_target(&child.id)),
        ));
    }
    millis + 61_000
}

/// A lineage with one budget full, `millis` a minute after the last entry, and two of the
/// root's children: `b`, which can delegate, and `c`.
struct Capped {
    state: State,
    millis: u64,
    b: CallerToken,
    c: CallerToken,
}

/// The root's own budget full: `agent-42` has ended `agent-42.d.k` and 1,023 leaves by token,
/// and `agent-42.b` three children of its own, which count in the descendants' budget.
fn root_full() -> &'static Capped {
    static CAPPED: OnceLock<Capped> = OnceLock::new();
    CAPPED.get_or_init(|| {
        let mut state = fleet();
        let b = copy(
            &delegate(&mut state, 1_000, 1, &root(), "b", delegating("agent-42.b")),
            ISSUER,
        );
        let c = copy(
            &delegate(&mut state, 1_000, 2, &root(), "c", leaf("agent-42.c")),
            ISSUER,
        );
        let d = delegate(&mut state, 1_000, 3, &root(), "d", delegating("agent-42.d"));
        for n in 4..7 {
            let segment = format!("k{n}");
            let child = delegate(
                &mut state,
                2_000,
                n,
                &b,
                &segment,
                leaf(&format!("agent-42.b.{segment}")),
            );
            token_revoked(stamped(
                &mut state,
                3_000,
                as_ancestor(&b, token_target(&child.id)),
            ));
        }
        let k = delegate(
            &mut state,
            2_000,
            7,
            &copy(&d, ISSUER),
            "k",
            leaf("agent-42.d.k"),
        );
        token_revoked(stamped(
            &mut state,
            3_000,
            as_ancestor(&root(), token_target(&k.id)),
        ));
        let leaves = u16::try_from(MAX_ANCESTOR_ENTRIES_PER_LINEAGE - 1).expect("small");
        let millis = fill(&mut state, &root(), leaves, 10, 70_000);
        assert_eq!(
            budget(&state, Adder::Root),
            MAX_ANCESTOR_ENTRIES_PER_LINEAGE
        );
        assert_eq!(budget(&state, Adder::Descendant), 3);
        Capped {
            state,
            millis,
            b,
            c,
        }
    })
}

/// The descendants' budget full: `agent-42.b` has ended 1,024 children by token, and the root
/// has added nothing.
fn descendants_full() -> &'static Capped {
    static CAPPED: OnceLock<Capped> = OnceLock::new();
    CAPPED.get_or_init(|| {
        let mut state = fleet();
        let b = copy(
            &delegate(&mut state, 1_000, 1, &root(), "b", delegating("agent-42.b")),
            ISSUER,
        );
        let c = copy(
            &delegate(&mut state, 1_000, 2, &root(), "c", delegating("agent-42.c")),
            ISSUER,
        );
        let children = u16::try_from(MAX_ANCESTOR_ENTRIES_PER_LINEAGE).expect("small");
        let millis = fill(&mut state, &b, children, 10, 2_000);
        assert_eq!(
            budget(&state, Adder::Descendant),
            MAX_ANCESTOR_ENTRIES_PER_LINEAGE
        );
        assert_eq!(budget(&state, Adder::Root), 0);
        Capped {
            state,
            millis,
            b,
            c,
        }
    })
}

#[test]
fn ancestor_entries_capped_per_lineage_root_self_revocation_exempt() {
    let Capped { state, millis, .. } = root_full();
    let (mut state, millis) = (state.clone(), *millis);
    // A new entry of the root's own is refused.
    let x = delegate(&mut state, millis, 2_000, &root(), "x", leaf("agent-42.x"));
    assert_eq!(
        refused(
            &mut state,
            millis,
            as_ancestor(&root(), token_target(&x.id))
        ),
        Rejection::RevocationLimit {
            limit: RevocationLimit::RootEntries
        }
    );
    // The root's revocation of the root token it presented is exempt, and replaces nothing.
    let mut own_token = state.clone();
    token_revoked(stamped(
        &mut own_token,
        millis,
        as_ancestor(&root(), token_target(&root().claims.id)),
    ));
    assert_eq!(
        budget(&own_token, Adder::Root),
        MAX_ANCESTOR_ENTRIES_PER_LINEAGE
    );
    // The root's revocation of its own principal is exempt, and replaces every counted entry,
    // in both budgets.
    principal_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), principal_target("agent-42")),
    ));
    assert_eq!(counted(&state, "agent-42"), 0);
    assert_eq!(state.revocations().len(), 1);
}

/// The root's own churn fills only its own budget: its refusal says so, and its delegated
/// agents still add entries to theirs (ADR-0047, point 11.6, as amended 2026-11-13).
#[test]
fn the_roots_own_churn_fills_only_its_own_budget() {
    let Capped {
        state,
        millis,
        b,
        c,
        ..
    } = root_full();
    let (mut state, millis) = (state.clone(), *millis);
    let x = delegate(&mut state, millis, 2_000, &root(), "x", leaf("agent-42.x"));
    let refusal = refused(
        &mut state,
        millis,
        as_ancestor(&root(), token_target(&x.id)),
    );
    assert!(
        refusal
            .to_string()
            .contains("the root's own budget is full"),
        "{refusal}"
    );
    // `b` ends a child of its own, and `c` itself, both in the descendants' budget.
    let child = delegate(&mut state, millis, 2_001, b, "y", leaf("agent-42.b.y"));
    token_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(b, token_target(&child.id)),
    ));
    principal_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(c, principal_target("agent-42.c")),
    ));
    assert_eq!(budget(&state, Adder::Descendant), 5);
    assert_eq!(
        budget(&state, Adder::Root),
        MAX_ANCESTOR_ENTRIES_PER_LINEAGE
    );
}

/// A descendant filling the descendants' budget never stops the root: it revokes a live child
/// by token, and another by principal, from its own budget (ADR-0047, point 11.6, as amended
/// 2026-11-13).
#[test]
fn a_descendant_filling_the_shared_budget_never_stops_the_root() {
    let Capped {
        state, millis, b, ..
    } = descendants_full();
    let (mut state, millis) = (state.clone(), *millis);
    // `b` itself is refused, naming its busiest child.
    let x = delegate(&mut state, millis, 2_000, b, "x", leaf("agent-42.b.x"));
    assert_eq!(
        refused(&mut state, millis, as_ancestor(b, token_target(&x.id))),
        Rejection::RevocationLimit {
            limit: RevocationLimit::Entries {
                busiest: Some(agent("agent-42.b.l0"))
            }
        }
    );
    let y = delegate(&mut state, millis, 2_001, &root(), "y", leaf("agent-42.y"));
    token_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), token_target(&y.id)),
    ));
    principal_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), principal_target("agent-42.c")),
    ));
    assert_eq!(budget(&state, Adder::Root), 2);
    assert_eq!(
        budget(&state, Adder::Descendant),
        MAX_ANCESTOR_ENTRIES_PER_LINEAGE
    );
}

/// An agent of a sibling branch is still bounded by the descendants' budget its sibling filled,
/// until the root revokes the sibling, which frees it.
#[test]
fn a_sibling_branch_is_still_bounded_by_the_shared_budget() {
    let Capped {
        state, millis, c, ..
    } = descendants_full();
    let (mut state, millis) = (state.clone(), *millis);
    let z = delegate(&mut state, millis, 2_000, c, "z", leaf("agent-42.c.z"));
    let full = Rejection::RevocationLimit {
        limit: RevocationLimit::Entries { busiest: None },
    };
    assert_eq!(
        refused(&mut state, millis, as_ancestor(c, token_target(&z.id))),
        full
    );
    assert_eq!(
        refused(
            &mut state,
            millis,
            as_ancestor(c, principal_target("agent-42.c"))
        ),
        full
    );
    principal_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), principal_target("agent-42.b")),
    ));
    assert_eq!(budget(&state, Adder::Descendant), 0);
    token_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(c, token_target(&z.id)),
    ));
}

/// At its own cap, the root's revocation that replaces an entry of its own budget, or raises
/// one in place, is never refused. One that replaces only its descendants' entries would grow
/// its budget, so it is refused: that keeps both budgets, and so I11's bound, whole.
#[test]
fn cap_never_refuses_a_revocation_that_replaces() {
    let Capped { state, millis, .. } = root_full();
    let (mut state, mut millis) = (state.clone(), *millis);
    // Revoking `d`'s principal replaces the root's own entry for `d.k`.
    let (first, _) = principal_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), principal_target("agent-42.d")),
    ));
    assert_eq!(
        budget(&state, Adder::Root),
        MAX_ANCESTOR_ENTRIES_PER_LINEAGE
    );
    // Revoking `d` again, once it is delegated again, raises its entry in place.
    millis += 1_000;
    delegate(&mut state, millis, 3_010, &root(), "d", leaf("agent-42.d"));
    let (second, _) = principal_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), principal_target("agent-42.d")),
    ));
    assert!(second > first);
    assert_eq!(
        budget(&state, Adder::Root),
        MAX_ANCESTOR_ENTRIES_PER_LINEAGE
    );
    // `b`'s principal would replace only `b`'s three entries, in the descendants' budget.
    assert_eq!(
        refused(
            &mut state,
            millis,
            as_ancestor(&root(), principal_target("agent-42.b"))
        ),
        Rejection::RevocationLimit {
            limit: RevocationLimit::RootEntries
        }
    );
}

#[test]
fn expired_ancestor_entries_never_count_toward_the_cap() {
    let Capped { state, millis, .. } = root_full();
    let mut state = state.clone();
    // Long after every entry's `until`, nothing has forgotten them, and they count for nothing.
    let late = millis + 6 * HOUR;
    let leaf_claims = delegate(&mut state, late, 2_000, &root(), "x", leaf("agent-42.x"));
    let before = state.revocations().len();
    assert!(
        before >= MAX_ANCESTOR_ENTRIES_PER_LINEAGE,
        "still unforgotten"
    );
    token_revoked(stamped(
        &mut state,
        late,
        as_ancestor(&root(), token_target(&leaf_claims.id)),
    ));
    assert_eq!(state.revocations().len(), 1);
}

/// The root's own principal entry takes no room in the root's budget: after revoking itself,
/// the root, with a root token minted later, still adds 1,024 entries of its own (ADR-0047,
/// I11).
#[test]
fn the_roots_principal_entry_takes_no_room_in_its_budget() {
    let mut state = fleet();
    let (before, _) = principal_revoked(stamped(
        &mut state,
        1_000,
        as_ancestor(&root(), principal_target("agent-42")),
    ));
    let mut renewed = root_token("agent-42", "root-jti-3");
    renewed.claims.issued_at = Some(Timestamp::from_millis(
        before.as_millis() - before.as_millis() % 1_000 + 31_000,
    ));
    let entries = u16::try_from(MAX_ANCESTOR_ENTRIES_PER_LINEAGE).expect("small");
    fill(&mut state, &renewed, entries, 10, 40_000);
    assert_eq!(
        budget(&state, Adder::Root),
        MAX_ANCESTOR_ENTRIES_PER_LINEAGE
    );
}

/// A root token the root revoked itself takes no room in the root's budget: once its earliest
/// entry expires, one slot is free, and after the root ends another of its root tokens the slot
/// is still free for a child's revocation (ADR-0047, I11).
#[test]
fn root_token_entries_take_no_room_in_the_roots_budget() {
    let Capped { state, .. } = root_full();
    let mut state = state.clone();
    let mut untils: Vec<Timestamp> = state
        .revocations()
        .iter()
        .filter(|(_, entry)| {
            matches!(&entry.added, Added::Ancestor { root, by } if root == by) && entry.of.is_some()
        })
        .map(|(_, entry)| entry.until)
        .collect();
    untils.sort();
    let (earliest, next) = (untils[0], untils[1]);
    assert!(next > earliest, "one entry expires first");
    let millis = earliest.as_millis();
    let second = root_token("agent-42", "root-jti-2");
    token_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&second, token_target(&second.claims.id)),
    ));
    assert_eq!(
        budget(&state, Adder::Root),
        MAX_ANCESTOR_ENTRIES_PER_LINEAGE - 1
    );
    let x = delegate(&mut state, millis, 2_000, &root(), "x", leaf("agent-42.x"));
    token_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), token_target(&x.id)),
    ));
    assert_eq!(
        budget(&state, Adder::Root),
        MAX_ANCESTOR_ENTRIES_PER_LINEAGE
    );
}

/// `agent-42` delegates `a`, `b`, and `c`, each of which delegates `count` leaves of its own, a
/// minute apart; returns the leaves of each, and the time a minute after the last.
fn three_parents(state: &mut State, count: u16) -> (Vec<(CallerToken, Vec<TokenClaims>)>, u64) {
    let mut parents = Vec::new();
    let mut millis = 1_000;
    for (n, segment) in [(1_u16, "a"), (2, "b"), (3, "c")] {
        let name = format!("agent-42.{segment}");
        let parent = copy(
            &delegate(state, millis, n, &root(), segment, delegating(&name)),
            ISSUER,
        );
        parents.push((parent, Vec::new()));
    }
    for (index, (parent, leaves)) in parents.iter_mut().enumerate() {
        millis += 61_000;
        for k in 0..count {
            // Within the parent's own delegation rate.
            if k > 0 && usize::from(k).is_multiple_of(ANCESTOR_CALLER_RATE) {
                millis += 61_000;
            }
            let n = 100 * u16::try_from(index + 1).expect("small") + k;
            let segment = format!("l{k}");
            let name = format!("{}.{segment}", parent.claims.principal);
            leaves.push(delegate(state, millis, n, parent, &segment, leaf(&name)));
        }
    }
    (parents, millis + 61_000)
}

#[test]
fn ancestor_rate_per_caller_and_lineage_never_counts_the_root() {
    let mut state = fleet();
    let rate = u16::try_from(ANCESTOR_CALLER_RATE).expect("small");
    let (parents, millis) = three_parents(&mut state, rate + 1);
    // `a` and `b` use their own rates, which make the lineage's.
    for (parent, leaves) in &parents[..2] {
        for child in &leaves[..usize::from(rate)] {
            token_revoked(stamped(
                &mut state,
                millis,
                as_ancestor(parent, token_target(&child.id)),
            ));
        }
    }
    let (a, a_leaves) = &parents[0];
    assert_eq!(
        refused(
            &mut state,
            millis,
            as_ancestor(a, token_target(&a_leaves[usize::from(rate)].id))
        ),
        Rejection::RevocationLimit {
            limit: RevocationLimit::CallerRate
        }
    );
    assert_eq!(2 * ANCESTOR_CALLER_RATE, ANCESTOR_LINEAGE_RATE);
    let (c, c_leaves) = &parents[2];
    assert_eq!(
        refused(
            &mut state,
            millis,
            as_ancestor(c, token_target(&c_leaves[0].id))
        ),
        Rejection::RevocationLimit {
            limit: RevocationLimit::LineageRate
        }
    );
    // The root is never counted in, nor refused by, the lineage's rate: its own 30 apply.
    for child in &c_leaves[..usize::from(rate)] {
        token_revoked(stamped(
            &mut state,
            millis,
            as_ancestor(&root(), token_target(&child.id)),
        ));
    }
    assert_eq!(
        refused(
            &mut state,
            millis,
            as_ancestor(&root(), token_target(&c_leaves[usize::from(rate)].id))
        ),
        Rejection::RevocationLimit {
            limit: RevocationLimit::CallerRate
        }
    );
    // A minute on, both windows have reopened.
    let later = millis + 61_000;
    token_revoked(stamped(
        &mut state,
        later,
        as_ancestor(c, token_target(&c_leaves[usize::from(rate)].id)),
    ));
}

/// A descendant holding `grant` fills its lineage's 256 live slots, its own rates, and the
/// lineage's delegation window, and adds entries of its own; none of it stops its root's
/// revocation of it, which frees the slots, so the root delegates again (ADR-0047, points 5.10
/// and 11.6).
#[test]
fn a_descendant_filling_the_live_cap_never_stops_its_root() {
    use fleetring_control::delegation::{CALLER_RATE, Limit, MAX_LIVE_PER_LINEAGE};

    let mut state = fleet();
    let d = delegate(&mut state, 1_000, 1, &root(), "d", delegating("agent-42.d"));
    let d_copy = copy(&d, ISSUER);
    let mut children = Vec::new();
    let mut minted: u16 = 10;
    let mut mint = |state: &mut State, millis: u64, children: &mut Vec<TokenClaims>| {
        minted += 1;
        let n = minted;
        let segment = format!("k{n}");
        let name = format!("agent-42.d.{segment}");
        children.push(delegate(state, millis, n, &d_copy, &segment, leaf(&name)));
    };
    // Up to the 256 live, the root's `d` included, at d's 30 a minute.
    let mut millis = 1_000;
    while children.len() + 1 < MAX_LIVE_PER_LINEAGE {
        if children.len().is_multiple_of(CALLER_RATE) {
            millis += 61_000;
        }
        mint(&mut state, millis, &mut children);
    }
    // In the last minute, `d` ends 30 of its children by token, a full window of its own, and
    // mints 30 more, a full delegation window: 256 live again.
    millis += 61_000;
    for child in children.drain(..ANCESTOR_CALLER_RATE).collect::<Vec<_>>() {
        token_revoked(stamped(
            &mut state,
            millis,
            as_ancestor(&d_copy, token_target(&child.id)),
        ));
    }
    for _ in 0..CALLER_RATE {
        mint(&mut state, millis, &mut children);
    }
    let ask = Ask::new(5_000, "e", leaf("agent-42.e"), millis + HOUR);
    assert_eq!(
        refused(&mut state, millis, ask.action(&root())),
        Rejection::DelegationLimit { limit: Limit::Live }
    );
    // The root revokes `d` by principal: accepted, replacing d's entries and pruning every one
    // of its records.
    principal_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), principal_target("agent-42.d")),
    ));
    assert_eq!(state.delegations().len(), 0);
    assert_eq!(
        counted(&state, "agent-42"),
        1,
        "d's principal replaced its 30 entries"
    );
    recorded(stamped(&mut state, millis, ask.action(&root())));
}

/// Revocations that prune every record they end still count: the 31st within the minute is
/// refused though nothing it revoked is left.
#[test]
fn revocation_never_reopens_the_rate_window() {
    let mut state = fleet();
    let rate = u16::try_from(ANCESTOR_CALLER_RATE).expect("small");
    let mut leaves = Vec::new();
    for n in 0..=rate {
        let millis = if n < rate { 1_000 } else { 62_000 };
        let segment = format!("l{n}");
        leaves.push(delegate(
            &mut state,
            millis,
            n + 1,
            &root(),
            &segment,
            leaf(&format!("agent-42.{segment}")),
        ));
    }
    let millis = 63_000;
    for child in &leaves[..usize::from(rate)] {
        principal_revoked(stamped(
            &mut state,
            millis,
            as_ancestor(
                &root(),
                AncestorTarget::Principal(agent(child.principal.as_str())),
            ),
        ));
    }
    assert_eq!(
        state.delegations().len(),
        1,
        "every revoked record is pruned"
    );
    assert_eq!(
        refused(
            &mut state,
            millis + 30_000,
            as_ancestor(&root(), token_target(&leaves[usize::from(rate)].id))
        ),
        Rejection::RevocationLimit {
            limit: RevocationLimit::CallerRate
        }
    );
}

/// `agent-42` with `count` leaves, a delegation window's worth each minute from 1 s, and the
/// stamp a minute after the last, when every window has closed.
fn root_with_leaves(count: u16) -> (State, Vec<TokenClaims>, u64) {
    use fleetring_control::delegation::CALLER_RATE;

    let mut state = fleet();
    let mut millis = 1_000;
    let leaves = (0..count)
        .map(|n| {
            if n > 0 && usize::from(n).is_multiple_of(CALLER_RATE) {
                millis += 61_000;
            }
            let segment = format!("l{n}");
            delegate(
                &mut state,
                millis,
                n + 1,
                &root(),
                &segment,
                leaf(&format!("agent-42.{segment}")),
            )
        })
        .collect();
    (state, leaves, millis + 61_000)
}

/// A root's revocation of its own principal or of the root token it presented is never counted
/// toward, nor refused by, its own rate, so `agent remove <root>` after 29 revocations in the
/// minute can't land the principal and then be refused the token, whose clock-free handle it
/// would lose for good (ADR-0047, points 11.6 and 11.7).
#[test]
fn a_root_revoking_itself_is_never_split_or_refused_by_its_own_rate() {
    let rate = u16::try_from(ANCESTOR_CALLER_RATE).expect("small");
    let (mut state, leaves, millis) = root_with_leaves(rate + 1);
    let own_token = || token_target(&root().claims.id);
    for child in &leaves[..usize::from(rate) - 1] {
        token_revoked(stamped(
            &mut state,
            millis,
            as_ancestor(&root(), token_target(&child.id)),
        ));
    }

    // 29 in the window: the principal and then the token, as two commands, both land.
    let mut split = state.clone();
    principal_revoked(stamped(
        &mut split,
        millis,
        as_ancestor(&root(), principal_target("agent-42")),
    ));
    token_revoked(stamped(
        &mut split,
        millis,
        as_ancestor(&root(), own_token()),
    ));
    assert!(
        split
            .revocations()
            .contains(&Revoked::Token(root().claims.id))
    );

    // Neither is counted: after the root revokes another root token it holds, which ends none
    // of the leaves, its 30th revocation of a leaf still lands, and the 31st is refused.
    let mut uncounted = state.clone();
    let second = root_token("agent-42", "root-jti-2");
    token_revoked(stamped(
        &mut uncounted,
        millis,
        as_ancestor(&second, token_target(&second.claims.id)),
    ));
    token_revoked(stamped(
        &mut uncounted,
        millis,
        as_ancestor(&root(), token_target(&leaves[usize::from(rate) - 1].id)),
    ));
    assert_eq!(
        refused(
            &mut uncounted,
            millis,
            as_ancestor(&root(), token_target(&leaves[usize::from(rate)].id))
        ),
        Rejection::RevocationLimit {
            limit: RevocationLimit::CallerRate
        }
    );

    // 30 in the window: the root's revocations of itself still land, alone or as a pair.
    token_revoked(stamped(
        &mut state,
        millis,
        as_ancestor(&root(), token_target(&leaves[usize::from(rate) - 1].id)),
    ));
    let mut alone = state.clone();
    principal_revoked(stamped(
        &mut alone,
        millis,
        as_ancestor(&root(), principal_target("agent-42")),
    ));
    token_revoked(stamped(
        &mut alone,
        millis,
        as_ancestor(&root(), own_token()),
    ));
    match stamped(
        &mut state,
        millis,
        as_ancestor(
            &root(),
            AncestorTarget::PrincipalAndToken {
                principal: agent("agent-42"),
                token: root().claims.id,
            },
        ),
    ) {
        Ok(Applied::PrincipalAndTokenRevoked {
            token_until: Some(_),
            ..
        }) => {}
        other => panic!("expected both revoked: {other:?}"),
    }
    assert_eq!(
        serde_json::to_string(state.revocations()).expect("serializes"),
        serde_json::to_string(alone.revocations()).expect("serializes"),
        "the pair adds what its two commands add"
    );
}

/// The principal and the token of one command land together or not at all, and a token the
/// principal covers gets no entry of its own (ADR-0047, point 11.7).
#[test]
fn principal_and_token_land_together_or_not_at_all() {
    let pair = |principal: &str, token: &TokenId| AncestorTarget::PrincipalAndToken {
        principal: agent(principal),
        token: token.clone(),
    };
    let Lineage {
        mut state, c7, r1, ..
    } = lineage();

    // Refused whole: an unknown token after an allowed principal, or a principal not below.
    assert_eq!(
        refused(
            &mut state,
            4_000,
            as_ancestor(&root(), pair("agent-42", &token("unknown")))
        ),
        Rejection::NoLiveDelegation {
            id: token("unknown")
        }
    );
    assert_eq!(
        refused(
            &mut state,
            4_000,
            as_ancestor(&copy(&c7, ISSUER), pair("agent-42", &c7.id))
        ),
        Rejection::NotAncestor {
            reason: AncestorViolation::NotBelowCaller
        }
    );

    // A delegated agent removing itself: its own token is covered by its principal.
    let mut own = state.clone();
    match stamped(
        &mut own,
        4_000,
        as_ancestor(&copy(&c7, ISSUER), pair("agent-42.c7", &c7.id)),
    ) {
        Ok(Applied::PrincipalAndTokenRevoked {
            token,
            token_until: None,
            ..
        }) => assert_eq!(token, Revoked::Token(c7.id.clone())),
        other => panic!("expected the token covered: {other:?}"),
    }
    assert!(!own.revocations().contains(&Revoked::Token(c7.id.clone())));
    assert_eq!(
        refusal(&own, &c7),
        Some(Revoked::principal(principal("agent-42.c7")))
    );

    // The root names a lower principal and a delegation below it: covered too, though the
    // caller isn't the principal.
    let mut lower = state.clone();
    match stamped(
        &mut lower,
        4_000,
        as_ancestor(&root(), pair("agent-42.c7", &r1.id)),
    ) {
        Ok(Applied::PrincipalAndTokenRevoked {
            token_until: None, ..
        }) => {}
        other => panic!("expected the token covered: {other:?}"),
    }
    assert_eq!(lower.revocations().len(), 1);

    // The root names a lower principal and its own root token: both get entries.
    match stamped(
        &mut state,
        4_000,
        as_ancestor(&root(), pair("agent-42.c7", &root().claims.id)),
    ) {
        Ok(Applied::PrincipalAndTokenRevoked {
            token_until: Some(_),
            ..
        }) => {}
        other => panic!("expected both entries: {other:?}"),
    }
    assert!(
        state
            .revocations()
            .contains(&Revoked::Token(root().claims.id))
    );
    assert!(
        state
            .revocations()
            .contains(&Revoked::principal(principal("agent-42.c7")))
    );
}

/// Mints at the most the rates allow, from the root and two delegated agents, revoking every
/// other child by principal under the ancestor rule, for `minutes`, and checks the records,
/// operations, and counted entries stay within ADR-0047's bounds at every minute.
fn principal_churn(minutes: u64) {
    use fleetring_control::delegation::{CALLER_RATE, Limit};

    let mut state = fleet();
    let mut callers = vec![root()];
    for (n, segment) in [(1, "a"), (2, "b")] {
        let name = format!("agent-42.{segment}");
        let mut ask = Ask::new(n, segment, delegating(&name), 0);
        ask.expires_at = at(24 * HOUR);
        callers.push(copy(
            &recorded(stamped(&mut state, 0, ask.action(&root()))),
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
                ask.id = crate::common::delegation::drawn(n);
                ask.expires_at = at(millis - millis % 1_000 + 60_000);
                match stamped(&mut state, millis, ask.action(caller)) {
                    Ok(Applied::DelegationRecorded { .. }) if n.is_multiple_of(2) => {
                        let revoked = stamped(
                            &mut state,
                            millis,
                            as_ancestor(caller, AncestorTarget::Principal(agent(&name))),
                        );
                        assert!(
                            matches!(
                                revoked,
                                Ok(Applied::PrincipalRevoked { .. })
                                    | Err(Rejection::RevocationLimit {
                                        limit: RevocationLimit::Entries { .. }
                                    })
                            ),
                            "{revoked:?}"
                        );
                    }
                    Ok(_) => {}
                    outcome => assert!(
                        matches!(
                            outcome,
                            Err(Rejection::DelegationLimit {
                                limit: Limit::Live | Limit::CallerRate | Limit::LineageRate
                            })
                        ),
                        "{outcome:?}"
                    ),
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
        let entries = counted(&state, "agent-42");
        for adder in [Adder::Root, Adder::Descendant] {
            let held = budget(&state, adder);
            assert!(
                held <= MAX_ANCESTOR_ENTRIES_PER_LINEAGE,
                "{held} entries in {adder:?}'s budget"
            );
        }
        assert!(entries <= 45 * elapsed, "{entries} after {elapsed} minutes");
    }
}

#[test]
fn records_per_lineage_stay_bounded_under_mint_and_principal_revoke_churn() {
    principal_churn(8);
}

/// The same churn for 100 minutes, past every record's margin, where the cap is reached.
#[test]
#[ignore = "heavy: about 9,000 delegations; run in the release statistics job"]
fn records_per_lineage_stay_bounded_under_mint_and_principal_revoke_churn_for_100_minutes() {
    principal_churn(100);
}

// ---- The feature boundary ----

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
fn fleet_principal_revocation_on_a_mixed_fleet_is_refused() {
    let mut state = fleet();
    let member = Member::new(id("old")).with_software(without_delegation());
    apply(&mut state, Command::new(at(0), Action::Join { member })).expect("join");
    let unsupported = Rejection::FeatureUnsupported {
        feature: Feature::Delegation,
        id: id("old"),
        release: Release::new(0, 7, 0),
    };
    for action in [
        Action::RevokePrincipal {
            principal: principal("agent-42"),
        },
        Action::RevokeToken {
            revoked: Revoked::principal(principal("agent-42")),
            until: Timestamp::MAX,
        },
        as_ancestor(&root(), principal_target("agent-42")),
        as_ancestor(&root(), token_target(&root().claims.id)),
    ] {
        assert_eq!(state.check_admission(&action), Err(unsupported.clone()));
        assert_eq!(refused(&mut state, 1_000, action), unsupported);
    }
    assert!(!state.is_enabled(Feature::Delegation));
}

#[test]
fn revoke_token_with_a_principal_target_is_refused() {
    let mut state = fleet();
    let revoked = Revoked::principal(principal("agent-42"));
    assert_eq!(
        refused(
            &mut state,
            1_000,
            Action::RevokeToken {
                revoked: revoked.clone(),
                until: Timestamp::MAX,
            }
        ),
        Rejection::PrincipalNeedsItsAction { revoked }
    );
    assert!(
        !state.is_enabled(Feature::Delegation),
        "a refusal enables nothing"
    );
}

#[test]
fn fleet_some_is_refused_in_0_8() {
    let mut state = fleet();
    let revoked = Revoked::Principal {
        fleet: Some("other".parse::<FleetRef>().expect("a fleet")),
        principal: principal("agent-42"),
    };
    assert_eq!(
        refused(
            &mut state,
            1_000,
            Action::RevokeToken {
                revoked: revoked.clone(),
                until: Timestamp::MAX,
            }
        ),
        Rejection::PrincipalNeedsItsAction { revoked }
    );
    // No action can name another fleet; and a stored ancestor-rule entry that does is refused.
    let stored = r#"[[{"principal":{"fleet":"other","principal":"agent-42"}},5,{"added":{"ancestor":{"root":"agent-42","by":"agent-42"}},"before":1}]]"#;
    assert!(serde_json::from_str::<Revocations>(stored).is_err());
}

/// Once `delegation` is enabled, what a revocation does depends on the log alone: a member that
/// reports software without the feature changes no outcome (ADR-0038, point 4; the replay rule).
#[test]
fn revocation_outcomes_never_depend_on_members_software() {
    let Lineage { mut state, c7, .. } = lineage();
    assert!(state.is_enabled(Feature::Delegation));
    let mut reported = state.clone();
    let heartbeat = Action::Heartbeat {
        id: id("n1"),
        replicas: Vec::new(),
        unknown_reports: fleetring_control::UnknownReports::Skip,
        software: Some(without_delegation()),
        disk_free_bytes: None,
    };
    for state in [&mut state, &mut reported] {
        stamped(state, 9_000, heartbeat.clone()).expect("heartbeat");
    }
    let mut reported_without = reported.clone();
    let heartbeat = Action::Heartbeat {
        id: id("n2"),
        replicas: Vec::new(),
        unknown_reports: fleetring_control::UnknownReports::Skip,
        software: None,
        disk_free_bytes: None,
    };
    stamped(&mut reported_without, 9_000, heartbeat).expect("heartbeat");
    let actions = [
        as_ancestor(&copy(&c7, ISSUER), principal_target("agent-42.c7.r2")),
        as_ancestor(&root(), token_target(&c7.id)),
        Action::RevokePrincipal {
            principal: principal("agent-42"),
        },
    ];
    let outcomes = |state: &mut State| -> Vec<_> {
        actions
            .iter()
            .map(|action| stamped(state, 10_000, action.clone()))
            .collect()
    };
    let expected = outcomes(&mut state);
    assert_eq!(outcomes(&mut reported_without), expected);
    assert_eq!(reported_without.revocations(), state.revocations());
    assert_eq!(
        reported_without.delegations().count(),
        state.delegations().count()
    );
}

// ---- Byte stability ----

#[test]
fn revocations_without_new_entries_serialize_as_before() {
    let list: Revocations = [
        (Revoked::Token(token("t1")), at(5)),
        (Revoked::Key(key("k1")), Timestamp::MAX),
    ]
    .into_iter()
    .collect();
    let text = format!(r#"[[{{"token":"t1"}},5],[{{"key":"k1"}},{}]]"#, u64::MAX);
    assert_eq!(serde_json::to_string(&list).expect("serializes"), text);
    assert_eq!(
        serde_json::from_str::<Revocations>(&text).expect("loads"),
        list
    );
    // Once `delegation` is enabled, an operator's token revocation still writes 0.7's form.
    let Lineage { mut state, .. } = lineage();
    stamped(
        &mut state,
        10_000,
        Action::RevokeToken {
            revoked: Revoked::Token(token("t1")),
            until: at(20_000),
        },
    )
    .expect("revoked");
    assert_eq!(
        serde_json::to_value(state.revocations()).expect("serializes"),
        serde_json::json!([[{"token": "t1"}, 20_000]])
    );
}

/// One entry of each shape: a fleet token, a fleet key, a fleet principal, an ancestor
/// principal, an ancestor token with `of`, and an ancestor root token.
fn every_entry_shape() -> State {
    let Lineage {
        mut state, c7, r1, ..
    } = lineage();
    let c7_copy = copy(&c7, ISSUER);
    stamped(
        &mut state,
        10_000,
        Action::RevokeToken {
            revoked: Revoked::Token(token("fleet-token")),
            until: at(10_000 + HOUR),
        },
    )
    .expect("fleet token");
    stamped(
        &mut state,
        10_000,
        Action::RevokeToken {
            revoked: Revoked::Key(key("fleet-key")),
            until: Timestamp::MAX,
        },
    )
    .expect("fleet key");
    stamped(
        &mut state,
        11_000,
        Action::RevokePrincipal {
            principal: principal("workload"),
        },
    )
    .expect("fleet principal");
    stamped(
        &mut state,
        12_000,
        as_ancestor(&c7_copy, principal_target("agent-42.c7.r2")),
    )
    .expect("ancestor principal");
    stamped(
        &mut state,
        13_000,
        as_ancestor(&c7_copy, token_target(&r1.id)),
    )
    .expect("ancestor token");
    stamped(
        &mut state,
        14_000,
        as_ancestor(&root(), token_target(&root().claims.id)),
    )
    .expect("ancestor root token");
    state
}

#[test]
fn revocation_entries_serialize_stably_once_delegation_is_enabled() {
    let state = every_entry_shape();
    let pinned = include_str!("fixtures/revocation_entries.json").trim_end();
    let text = serde_json::to_string_pretty(state.revocations()).expect("serializes");
    assert_eq!(text, pinned, "the entries' bytes changed");
    let back: Revocations = serde_json::from_str(pinned).expect("loads");
    assert_eq!(&back, state.revocations());
    // Through a whole snapshot, too.
    let snapshot = serde_json::to_string(&state).expect("serializes");
    let reloaded: State = serde_json::from_str(&snapshot).expect("loads");
    assert_eq!(reloaded, state);
}

#[test]
fn a_principal_revocation_replays_identically() {
    use crate::replay::{
        check_replay_from_scratch, check_replay_from_snapshots, check_replay_through_serialization,
    };

    // The log `every_entry_shape` writes, replayed three ways.
    let mut state = State::new(crate::common::config());
    let mut log = Vec::new();
    let mut outcomes = Vec::new();
    let mut run = |command: Command, state: &mut State| {
        outcomes.push(apply(state, command.clone()));
        log.push(command);
    };
    for node in ["n0", "n1", "n2"] {
        let member = Member::new(id(node)).with_software(crate::common::current_software());
        run(Command::new(at(0), Action::Join { member }), &mut state);
    }
    let recorder = id(crate::common::delegation::RECORDER);
    let c7_ask = Ask::new(1, "c7", delegating("agent-42.c7"), 1_000 + 2 * HOUR);
    run(
        Command::stamped(at(1_000), recorder.clone(), c7_ask.action(&root())),
        &mut state,
    );
    let c7 = state
        .delegations()
        .map(|(_, record)| record.claims.clone())
        .next()
        .expect("c7");
    let r1_ask = Ask::new(2, "r1", leaf("agent-42.c7.r1"), 1_000 + 2 * HOUR);
    run(
        Command::stamped(
            at(2_000),
            recorder.clone(),
            r1_ask.action(&copy(&c7, ISSUER)),
        ),
        &mut state,
    );
    for (millis, action) in [
        (
            3_000,
            as_ancestor(&copy(&c7, ISSUER), principal_target("agent-42.c7.r1")),
        ),
        (
            3_000,
            Action::RevokePrincipal {
                principal: principal("workload"),
            },
        ),
        (
            3_500,
            Action::RevokeToken {
                revoked: Revoked::principal(principal("x")),
                until: Timestamp::MAX,
            },
        ),
        (4_000, as_ancestor(&root(), token_target(&c7.id))),
        (5_000, as_ancestor(&root(), principal_target("agent-42"))),
    ] {
        run(
            Command::stamped(at(millis), recorder.clone(), action),
            &mut state,
        );
    }
    let refusals: Vec<_> = outcomes.iter().filter(|outcome| outcome.is_err()).collect();
    assert_eq!(
        refusals.len(),
        1,
        "only RevokeToken's principal: {refusals:?}"
    );
    check_replay_from_scratch(&log, &outcomes, &state);
    check_replay_from_snapshots(&log, &outcomes, &state);
    check_replay_through_serialization(&log, &outcomes, &state);
}
