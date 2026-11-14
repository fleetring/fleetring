//! The lineage vocabulary (ADR-0047): agent names, the agent tier, and the denylist's verdict on
//! a token and its chain.

use std::collections::BTreeSet;

use fleetring_control::delegation::tier::{self, TierViolation};
use fleetring_control::delegation::{
    AgentName, ChainLink, ClaimsViolation, DelegationId, MAX_DELEGATED_LIFETIME, MAX_DEPTH,
    MAX_NOT_BEFORE_LEAD, ParseAgentNameError, SignerKind, TokenClaims, TokenKind,
};
use fleetring_control::matcher::DatabaseMatcher;
use fleetring_control::scope::{Action, Scope, Scopes};
use fleetring_control::{
    KeyId, Millis, ParsePrincipalNameError, PrincipalName, Revocations, Revoked, Timestamp,
    TokenId, TokenRef,
};
use proptest::prelude::*;

fn agent(text: &str) -> AgentName {
    text.parse()
        .unwrap_or_else(|e| panic!("'{text}' is an agent name: {e}"))
}

fn principal(text: &str) -> PrincipalName {
    text.parse()
        .unwrap_or_else(|e| panic!("'{text}' is a principal name: {e}"))
}

fn scope(text: &str) -> Scope {
    text.parse()
        .unwrap_or_else(|e| panic!("'{text}' is a scope: {e}"))
}

fn scopes(texts: &[&str]) -> Scopes {
    texts.iter().map(|text| scope(text)).collect()
}

fn token(text: &str) -> TokenId {
    text.parse().expect("a token id")
}

fn key(text: &str) -> KeyId {
    text.parse().expect("a key id")
}

fn link(id: &str, kid: &str) -> ChainLink {
    ChainLink {
        id: token(id),
        kid: key(kid),
    }
}

fn denylist(entries: &[Revoked]) -> Revocations {
    entries
        .iter()
        .map(|revoked| (revoked.clone(), Timestamp::MAX))
        .collect()
}

/// ADR-0047, "Words": an agent name is `agent-<id>` with one to eight `.<segment>`, each part
/// `[a-z0-9_]{1,32}`, at most 64 bytes; a dotted service name is not one.
#[test]
fn agent_names_parse_root_and_delegated_and_refuse_the_rest() {
    let root = agent("agent-42");
    assert!(!root.is_delegated());
    assert_eq!((root.depth(), root.parent()), (0, None));

    let child = agent("agent-42.c7");
    assert!(child.is_delegated());
    assert_eq!(child.depth(), 1);
    assert_eq!(child.root(), root);
    assert_eq!(child.parent(), Some(root.clone()));
    assert_eq!(child.as_principal(), &principal("agent-42.c7"));

    let deepest = agent("agent-42.a.b.c.d.e.f.g.h");
    assert_eq!(deepest.depth(), MAX_DEPTH);
    assert_eq!(deepest.root(), root);
    agent(&format!("agent-{}", "x".repeat(32)));
    agent("agent-a_1.b_2");

    let nine = "agent-42.a.b.c.d.e.f.g.h.i";
    assert!(nine.len() <= 64, "depth is bounded apart from length");
    assert!(matches!(
        nine.parse::<AgentName>(),
        Err(ParseAgentNameError::TooDeep { depth: 9, .. })
    ));
    let long = format!("agent-42.{}", "x".repeat(56));
    assert_eq!(long.len(), 65);
    assert!(matches!(
        long.parse::<AgentName>(),
        Err(ParseAgentNameError::Principal(
            ParsePrincipalNameError::TooLong { len: 65, .. }
        ))
    ));
    for refused in [
        "agent-42.",
        "agent-42..x",
        "agent-.c7",
        "agent-",
        "agent-42-x",
        "agent-42.c-7",
        &format!("agent-42.{}", "x".repeat(33)),
    ] {
        assert!(
            matches!(
                refused.parse::<AgentName>(),
                Err(ParseAgentNameError::Segment { .. })
            ),
            "{refused}"
        );
    }
    for refused in ["agent-42.C7", "Agent-42", "agent-42 c7", ""] {
        assert!(
            matches!(
                refused.parse::<AgentName>(),
                Err(ParseAgentNameError::Principal(_))
            ),
            "{refused}"
        );
    }
    for refused in ["workload.prod", "workload", "agent", "agents-42"] {
        assert!(
            matches!(
                refused.parse::<AgentName>(),
                Err(ParseAgentNameError::NotAgent { .. })
            ),
            "{refused}"
        );
    }
    let error = "agent-42.c-7".parse::<AgentName>().expect_err("refused");
    assert_eq!(
        error.to_string(),
        "'agent-42.c-7' isn't an agent name: after 'agent-', each part between dots is 1 to 32 \
         of a-z, 0-9, and '_'"
    );
}

/// ADR-0047, "Words" and point 11.1: at or below means equal, or followed by `.`, byte for byte.
#[test]
fn at_or_below_matches_agent_42_and_its_children_never_agent_420() {
    let root = principal("agent-42");
    for below in ["agent-42", "agent-42.c7", "agent-42.c7.r1", "agent-42.c70"] {
        assert!(principal(below).is_at_or_below(&root), "{below}");
    }
    for outside in [
        "agent-420",
        "agent-420.c7",
        "agent-4",
        "agent-42-x",
        "agent-43.c7",
    ] {
        assert!(!principal(outside).is_at_or_below(&root), "{outside}");
    }
    let child = principal("agent-42.c7");
    assert!(principal("agent-42.c7.r1").is_at_or_below(&child));
    assert!(!principal("agent-42.c70").is_at_or_below(&child));
    assert!(!root.is_at_or_below(&child), "an ancestor is never below");
    // Service names compare the same way; only `fleet` revokes through them (point 11.3).
    assert!(principal("workload.prod").is_at_or_below(&principal("workload")));
    assert!(!principal("workloads").is_at_or_below(&principal("workload")));
}

/// ADR-0047, point 3.4 to 3.7: each refusal names the first scope that breaks the tier.
#[test]
fn tier_refusals_name_the_scope() {
    let child = agent("agent-42.c7");
    let refused = |text: &str| tier::check(&child, &scopes(&[text]));
    for outside in [
        "admin:agent-43-*",
        "read:pub-*",
        "admin:agent-420-*",
        "read:*",
    ] {
        assert_eq!(
            refused(outside),
            Err(TierViolation::OutsideRoot {
                scope: scope(outside)
            }),
            "{outside}"
        );
    }
    for grant in [
        "grant:agent-42.d1.*",
        "grant:agent-42.c7*",
        "grant:agent-42.*",
        "grant:agent-42-*",
    ] {
        assert_eq!(
            refused(grant),
            Err(TierViolation::GrantOutsideChild {
                scope: scope(grant)
            }),
            "{grant}"
        );
    }
    for loose in [
        "admin:agent-42.c7x*",
        "read:agent-42-x*",
        "grant:agent-42.c7.r*",
    ] {
        assert_eq!(
            refused(loose),
            Err(TierViolation::NoSeparator {
                scope: scope(loose)
            }),
            "{loose}"
        );
    }
    assert_eq!(refused("fleet"), Err(TierViolation::Fleet));

    for accepted in [
        "fork:agent-42-memory",
        "admin:agent-42-*",
        "read:agent-42.*",
        "admin:agent-42.c7-*",
        "stream:agent-42.d1-x",
        "grant:agent-42.c7.*",
        "grant:agent-42.c7.r1-*",
        "grant:agent-42.c7.r1",
    ] {
        assert_eq!(refused(accepted), Ok(()), "{accepted}");
    }
    assert_eq!(tier::check(&child, &Scopes::none()), Ok(()));

    // A root agent has no agent tier, whatever the scopes.
    for set in [Scopes::none(), scopes(&["admin:agent-42-*"])] {
        assert_eq!(
            tier::check(&agent("agent-42"), &set),
            Err(TierViolation::NotDelegated)
        );
    }

    // The first offending scope, in the order given, is the one named.
    assert_eq!(
        tier::check(
            &child,
            &scopes(&["admin:agent-42.c7-*", "read:agent-43-*", "fleet"])
        ),
        Err(TierViolation::OutsideRoot {
            scope: scope("read:agent-43-*")
        })
    );
    assert_eq!(
        TierViolation::OutsideRoot {
            scope: scope("read:pub-*")
        }
        .to_string(),
        "the scope read:pub-* isn't within its root agent's databases, agent-<root>- or \
         agent-<root>."
    );
}

/// ADR-0047, point 5.3: the grant step alone doesn't keep a child inside the tier, which is why
/// the handler and `apply` also run `tier::check`.
#[test]
fn the_grant_step_can_pass_what_the_tier_refuses() {
    let parent = scopes(&["admin:agent-42.*", "grant:agent-42.*"]);
    let asked = scopes(&["grant:agent-42.*"]);
    assert_eq!(parent.grant_step_strict(&asked), Ok(asked.clone()));
    assert_eq!(
        tier::check(&agent("agent-42.c7"), &asked),
        Err(TierViolation::GrantOutsideChild {
            scope: scope("grant:agent-42.*")
        })
    );
}

/// The pieces generated patterns are made of: the bytes a careless comparison gets wrong.
const PIECES: &[&str] = &[
    "agent-",
    "agent-42",
    "agent-420",
    "agent-43",
    "agent-4",
    "-",
    ".",
    "c7",
    "c70",
    "x",
    "0",
    "pub",
    "agent-42.c7",
    "agent-42.d1",
    "r1",
    "_",
];

/// How generated patterns start, so that many land inside, at the edge of, and just outside the
/// namespaces the tier checks.
const STARTS: &[&str] = &[
    "agent-42-",
    "agent-42.",
    "agent-42.c7.",
    "agent-42.c7.r1.",
    "agent-42.d1.",
    "agent-42",
    "agent-420",
    "",
];

/// The delegated agents generated checks run for, all under root 42.
const CHILDREN: &[&str] = &["agent-42.c7", "agent-42.c7.r1", "agent-42.d1"];

fn arb_pattern() -> impl Strategy<Value = String> {
    (
        prop::sample::select(STARTS),
        prop::collection::vec(prop::sample::select(PIECES), 0..3),
        any::<bool>(),
    )
        .prop_filter_map("a pattern isn't empty", |(start, pieces, star)| {
            let mut text = format!("{start}{}", pieces.concat());
            if star {
                text.push('*');
            }
            (!text.is_empty()).then_some(text)
        })
}

fn arb_scope() -> impl Strategy<Value = Scope> {
    prop_oneof![
        1 => Just(Scope::fleet()),
        12 => (prop::sample::select(&Action::ALL[..]), arb_pattern()).prop_map(|(action, pattern)| {
            Scope::new(action, pattern.parse::<DatabaseMatcher>().expect("not empty"))
        }),
    ]
}

/// The bytes before a pattern's final `*`, or the whole name, and whether it ends in `*`.
fn body(pattern: &str) -> (&str, bool) {
    match pattern.strip_suffix('*') {
        Some(prefix) => (prefix, true),
        None => (pattern, false),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// ADR-0047, point 3.5 and 3.6, as the plan pins them: the tier accepts a scope exactly when
    /// the lattice's own `covers` puts it under `agent-<root>-` or `agent-<root>.` (and a grant
    /// under `<child>.`), and the separator rule holds. A name-level oracle checks `covers`.
    #[test]
    fn prop_tier_check_agrees_with_coverage(
        child in prop::sample::select(CHILDREN),
        scope in arb_scope(),
    ) {
        let child = agent(child);
        let verdict = tier::check(&child, &Scopes::new([scope.clone()]));
        let Some(action) = scope.action() else {
            prop_assert_eq!(verdict, Err(TierViolation::Fleet));
            return Ok(());
        };
        let text = scope.to_string();
        let pattern = text.split_once(':').expect("a database scope").1;
        let (body, star) = body(pattern);

        let pattern_of = |prefix: String| -> DatabaseMatcher {
            format!("{prefix}*").parse().expect("a prefix")
        };
        let covered_by = |prefix: &str| {
            Scope::new(action, pattern_of(prefix.to_owned())).covers(&scope)
        };
        let root = child.root();
        let in_root = covered_by(&format!("{root}-")) || covered_by(&format!("{root}."));
        // Every name the pattern matches starts with one of the two prefixes exactly when its
        // body does, a prefix pattern's included.
        prop_assert_eq!(
            in_root,
            body.starts_with(&format!("{root}-")) || body.starts_with(&format!("{root}.")),
            "coverage of {}", scope
        );
        let in_child = covered_by(&format!("{child}."));
        prop_assert_eq!(in_child, body.starts_with(&format!("{child}.")), "coverage of {}", scope);
        let separated = !star || body.bytes().last().is_none_or(|byte| !byte.is_ascii_alphanumeric());

        let expected = if !in_root {
            Err(TierViolation::OutsideRoot { scope: scope.clone() })
        } else if action == Action::Grant && !in_child {
            Err(TierViolation::GrantOutsideChild { scope: scope.clone() })
        } else if !separated {
            Err(TierViolation::NoSeparator { scope: scope.clone() })
        } else {
            Ok(())
        };
        prop_assert_eq!(verdict, expected, "{} for {}", scope, child);
    }

    /// A set is in the tier exactly when each of its scopes is, and a refusal names the first
    /// scope, in the order given, that a check of it alone refuses.
    #[test]
    fn prop_tier_check_of_a_set_is_its_first_refused_scope(
        child in prop::sample::select(CHILDREN),
        set in prop::collection::vec(arb_scope(), 0..6),
    ) {
        let child = agent(child);
        let first = set
            .iter()
            .map(|scope| tier::check(&child, &Scopes::new([scope.clone()])))
            .find(Result::is_err)
            .unwrap_or(Ok(()));
        prop_assert_eq!(tier::check(&child, &Scopes::new(set)), first);
    }
}

fn arb_segment() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => prop::sample::select(&["c7", "c70", "r1", "d1", "x", "0", "_"][..]).prop_map(str::to_owned),
        1 => "[a-z0-9_]{1,32}",
    ]
}

fn arb_agent() -> impl Strategy<Value = AgentName> {
    (
        prop::sample::select(&["42", "420", "4", "a_b"][..]),
        prop::collection::vec(arb_segment(), 0..=MAX_DEPTH),
    )
        .prop_filter_map("within 64 bytes", |(id, segments)| {
            let mut text = format!("agent-{id}");
            for segment in segments {
                text.push('.');
                text.push_str(&segment);
            }
            text.parse().ok()
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// `root`, `parent`, `child`, and `depth` walk one lineage, and at-or-below is that lineage.
    #[test]
    fn prop_agent_names_walk_their_lineage(name in arb_agent(), other in arb_agent()) {
        let mut ancestors = vec![name.clone()];
        let mut below = name.clone();
        while let Some(parent) = below.parent() {
            prop_assert_eq!(parent.depth() + 1, below.depth());
            let (_, segment) = below
                .as_str()
                .rsplit_once('.')
                .expect("a delegated name has a last segment");
            prop_assert_eq!(parent.child(segment), Ok(below.clone()));
            ancestors.push(parent.clone());
            below = parent;
        }
        prop_assert_eq!(ancestors.len(), name.depth() + 1);
        let root = name.root();
        prop_assert_eq!(ancestors.last(), Some(&root));
        prop_assert_eq!(name.is_delegated(), name.depth() > 0);
        prop_assert_eq!(SignerKind::for_principal(name.as_principal()) == SignerKind::Issuer, name.is_delegated());

        let at_or_below = name.as_principal().is_at_or_below(other.as_principal());
        prop_assert_eq!(at_or_below, ancestors.contains(&other), "{} under {}", name, other);
    }
}

/// ADR-0047, point 3: a token is refused by its own `jti` or `kid`, or by any `id` or `kid` in
/// its chain, whichever keys signed which copies; and the first match is returned.
#[test]
fn refuses_token_matches_own_id_key_every_chain_id_and_every_chain_kid() {
    let chain = [
        link("root-jti", "authority"),
        link("c7-delegation", "issuer-a"),
        link("r1-delegation", "issuer-b"),
    ];
    let principal = principal("agent-42.c7.r1.z");
    let id = token("own-delegation");
    let kid = key("issuer-c");
    let copy = TokenRef {
        id: &id,
        kid: &kid,
        chain: &chain,
        principal: &principal,
        issued_at: Some(Timestamp::from_millis(5_000)),
        signer: SignerKind::Issuer,
    };
    assert_eq!(Revocations::new().refuses_token(&copy), None);

    let each = [
        Revoked::Token(id.clone()),
        Revoked::Key(kid.clone()),
        Revoked::Token(chain[0].id.clone()),
        Revoked::Key(chain[0].kid.clone()),
        Revoked::Token(chain[1].id.clone()),
        Revoked::Key(chain[1].kid.clone()),
        Revoked::Token(chain[2].id.clone()),
        Revoked::Key(chain[2].kid.clone()),
    ];
    for revoked in &each {
        assert_eq!(
            denylist(std::slice::from_ref(revoked)).refuses_token(&copy),
            Some(revoked.clone()),
            "{revoked}"
        );
    }
    // Entries naming nothing in the token, or naming a key as a token, refuse nothing.
    let unrelated = denylist(&[
        Revoked::Token(token("other")),
        Revoked::Key(key("other")),
        Revoked::Token(token("authority")),
        Revoked::Key(key("root-jti")),
    ]);
    assert_eq!(unrelated.refuses_token(&copy), None);

    // Several matches: the token's own entries first, then the chain root first.
    assert_eq!(
        denylist(&each).refuses_token(&copy),
        Some(Revoked::Token(id.clone()))
    );
    assert_eq!(
        denylist(&each[2..]).refuses_token(&copy),
        Some(Revoked::Token(chain[0].id.clone()))
    );
    assert_eq!(
        denylist(&[each[7].clone(), each[4].clone()]).refuses_token(&copy),
        Some(each[4].clone())
    );

    // A root token has no chain: only its own ID and key refuse it.
    let root = TokenRef {
        chain: &[],
        signer: SignerKind::Authority,
        ..copy
    };
    for (list, refused) in [
        (denylist(&each[..1]), true),
        (denylist(&each[1..2]), true),
        (denylist(&each[2..]), false),
    ] {
        assert_eq!(list.refuses_token(&root).is_some(), refused, "{list:?}");
    }
}

/// A pool of IDs small enough that generated tokens and lists overlap often.
const IDS: &[&str] = &["t0", "t1", "t2", "t3", "t4", "t5"];
const KIDS: &[&str] = &["k0", "k1", "k2", "k3"];

fn arb_revoked() -> impl Strategy<Value = Revoked> {
    prop_oneof![
        prop::sample::select(IDS).prop_map(|id| Revoked::Token(token(id))),
        prop::sample::select(KIDS).prop_map(|kid| Revoked::Key(key(kid))),
    ]
}

fn arb_link() -> impl Strategy<Value = ChainLink> {
    (prop::sample::select(IDS), prop::sample::select(KIDS)).prop_map(|(id, kid)| link(id, kid))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Adding entries never un-refuses a token, and the verdict is exactly whether the list
    /// shares an entry with the token's own ID and key and its chain's.
    #[test]
    fn prop_refuses_token_is_monotonic_in_the_list(
        id in prop::sample::select(IDS),
        kid in prop::sample::select(KIDS),
        chain in prop::collection::vec(arb_link(), 0..=MAX_DEPTH),
        listed in prop::collection::vec(arb_revoked(), 0..6),
        more in prop::collection::vec(arb_revoked(), 0..6),
    ) {
        let (id, kid, principal) = (token(id), key(kid), principal("agent-42.c7"));
        let token = TokenRef {
            id: &id,
            kid: &kid,
            chain: &chain,
            principal: &principal,
            issued_at: None,
            signer: SignerKind::Issuer,
        };
        let names: BTreeSet<Revoked> = [Revoked::Token(id.clone()), Revoked::Key(kid.clone())]
            .into_iter()
            .chain(chain.iter().flat_map(|link| {
                [Revoked::Token(link.id.clone()), Revoked::Key(link.kid.clone())]
            }))
            .collect();

        let smaller = denylist(&listed);
        let larger = denylist(&[listed.clone(), more].concat());
        for list in [&smaller, &larger] {
            let verdict = list.refuses_token(&token);
            let shared = list.iter().any(|(revoked, _)| names.contains(revoked));
            prop_assert_eq!(verdict.is_some(), shared);
            if let Some(revoked) = verdict {
                prop_assert!(list.contains(&revoked) && names.contains(&revoked));
            }
        }
        if smaller.refuses_token(&token).is_some() {
            prop_assert!(larger.refuses_token(&token).is_some());
        }
    }
}

/// ADR-0047, point 3 and point 11.1: a principal revocation refuses a token whose subject is at
/// or below its principal and whose `iat` is at or before `before`, exactly for an issuer's
/// token and with 30 seconds of leeway for the authority's.
#[test]
fn principal_revocations_judge_root_tokens_with_leeway_and_delegated_ones_exactly() {
    let before = Timestamp::from_millis(1_000_000);
    let (id, kid) = (token("t"), key("k"));
    let at = |name: &PrincipalName, millis: u64, signer: SignerKind| {
        let token = TokenRef {
            id: &id,
            kid: &kid,
            chain: &[],
            principal: name,
            issued_at: Some(Timestamp::from_millis(millis)),
            signer,
        };
        token.refused_by_principal(&principal("agent-42"), before)
    };

    let root = principal("agent-42");
    assert!(at(&root, 1_000_000, SignerKind::Authority));
    assert!(
        at(&root, 1_030_000, SignerKind::Authority),
        "within the leeway"
    );
    assert!(!at(&root, 1_030_001, SignerKind::Authority));

    let child = principal("agent-42.c7.r1");
    assert!(at(&child, 1_000_000, SignerKind::Issuer), "iat at before");
    assert!(
        !at(&child, 1_000_001, SignerKind::Issuer),
        "no leeway for an issuer"
    );
    assert!(at(&child, 0, SignerKind::Issuer));

    // Never outside the principal's subtree, however early the token.
    for outside in ["agent-420", "agent-420.c7", "agent-43.c7", "agent-4"] {
        assert!(
            !at(&principal(outside), 0, SignerKind::Authority),
            "{outside}"
        );
        assert!(!at(&principal(outside), 0, SignerKind::Issuer), "{outside}");
    }

    // The leeway saturates instead of wrapping at the end of time.
    let late = TokenRef {
        id: &id,
        kid: &kid,
        chain: &[],
        principal: &root,
        issued_at: Some(Timestamp::MAX),
        signer: SignerKind::Authority,
    };
    assert!(late.refused_by_principal(&root, Timestamp::MAX));
    assert!(!late.refused_by_principal(&root, before));
}

/// ADR-0047, point 3: a token without `iat` counts as issued before every revocation.
#[test]
fn a_token_without_iat_is_refused_by_every_covering_principal_revocation() {
    let (id, kid, name) = (token("t"), key("k"), principal("agent-42.c7"));
    for signer in [SignerKind::Authority, SignerKind::Issuer] {
        let token = TokenRef {
            id: &id,
            kid: &kid,
            chain: &[],
            principal: &name,
            issued_at: None,
            signer,
        };
        assert!(token.refused_by_principal(&principal("agent-42"), Timestamp::ZERO));
        assert!(token.refused_by_principal(&name, Timestamp::ZERO));
        assert!(!token.refused_by_principal(&principal("agent-42.c7.r1"), Timestamp::MAX));
    }
}

/// ADR-0047, points 2.6 and 3: an issuer signs only delegated agent names, the authority every
/// other name, so `apply` can derive a caller's signer from its name.
#[test]
fn the_signer_follows_from_the_name() {
    for issuer in ["agent-42.c7", "agent-42.c7.r1"] {
        assert_eq!(
            SignerKind::for_principal(&principal(issuer)),
            SignerKind::Issuer
        );
    }
    // A dotted name that isn't an agent name, `agent-42.c-7` included, names no delegated agent.
    for authority in [
        "agent-42",
        "workload",
        "workload.prod",
        "agent-42-x",
        "agent-42.c-7",
    ] {
        assert_eq!(
            SignerKind::for_principal(&principal(authority)),
            SignerKind::Authority,
            "{authority}"
        );
    }
}

fn claims() -> TokenClaims {
    TokenClaims {
        fleet: "prod".into(),
        principal: principal("agent-42.c7"),
        id: DelegationId::from_bytes([7; 16]).into(),
        issued_at: Some(Timestamp::from_millis(1_000)),
        not_before: Timestamp::from_millis(1_000),
        expires_at: Timestamp::from_millis(7_201_000),
        scopes: scopes(&["admin:agent-42.c7-*", "stream:agent-42.c7-*"]),
        kind: Some(TokenKind::Agent),
        owner: Some(principal("sam")),
        parent: Some(principal("agent-42")),
        chain: vec![link("root-jti", "authority")],
    }
}

/// ADR-0047, point 2.2: the claims are every claim but `kid` and `x5c`, and a copy's
/// `TokenRef` takes its ID, chain, subject, and `iat` from them.
#[test]
fn token_claims_round_trip_and_refuse_unknown_fields() {
    let delegated = claims();
    let json = serde_json::to_string(&delegated).expect("serializes");
    assert_eq!(
        serde_json::from_str::<TokenClaims>(&json).expect("deserializes"),
        delegated
    );
    let mut value: serde_json::Value = serde_json::from_str(&json).expect("json");
    value["x5c"] = serde_json::json!(["MII…"]);
    assert!(serde_json::from_value::<TokenClaims>(value).is_err());

    // A root token's claims omit what it doesn't have.
    let root = TokenClaims {
        principal: principal("agent-42"),
        id: token("root-jti"),
        issued_at: None,
        kind: None,
        owner: None,
        parent: None,
        chain: Vec::new(),
        ..claims()
    };
    let json = serde_json::to_string(&root).expect("serializes");
    for absent in ["issued_at", "kind", "owner", "parent", "chain"] {
        assert!(!json.contains(absent), "{absent} in {json}");
    }
    assert_eq!(
        serde_json::from_str::<TokenClaims>(&json).expect("deserializes"),
        root
    );

    let kid = key("issuer-a");
    let copy = delegated.token_ref(&kid, SignerKind::Issuer);
    assert_eq!(
        copy,
        TokenRef {
            id: &delegated.id,
            kid: &kid,
            chain: &delegated.chain,
            principal: &delegated.principal,
            issued_at: delegated.issued_at,
            signer: SignerKind::Issuer,
        }
    );
    assert_eq!(
        denylist(&[Revoked::Token(token("root-jti"))]).refuses_token(&copy),
        Some(Revoked::Token(token("root-jti")))
    );
}

/// Recorded delegations hold `TokenClaims` in control state, which the log and every snapshot
/// carry, so their JSON is pinned byte for byte: a change here changes what replicas store.
#[test]
fn token_claims_serialize_exactly_as_pinned() {
    let delegated = claims();
    let golden = concat!(
        r#"{"fleet":"prod","principal":"agent-42.c7","id":"BwcHBwcHBwcHBwcHBwcHBw","#,
        r#""issued_at":1000,"not_before":1000,"expires_at":7201000,"#,
        r#""scopes":["admin:agent-42.c7-*","stream:agent-42.c7-*"],"kind":"agent","#,
        r#""owner":"sam","parent":"agent-42","chain":[{"id":"root-jti","kid":"authority"}]}"#,
    );
    assert_eq!(
        serde_json::to_string(&delegated).expect("serializes"),
        golden
    );
    assert_eq!(
        serde_json::from_str::<TokenClaims>(golden).expect("deserializes"),
        delegated
    );

    // A root token's claims, which leave out every optional claim.
    let root = TokenClaims {
        principal: principal("agent-42"),
        id: token("root-jti"),
        issued_at: None,
        scopes: scopes(&["admin:agent-42-*", "grant:agent-42.*"]),
        kind: None,
        owner: None,
        parent: None,
        chain: Vec::new(),
        ..claims()
    };
    let golden = concat!(
        r#"{"fleet":"prod","principal":"agent-42","id":"root-jti","not_before":1000,"#,
        r#""expires_at":7201000,"scopes":["admin:agent-42-*","grant:agent-42.*"]}"#,
    );
    assert_eq!(serde_json::to_string(&root).expect("serializes"), golden);
    assert_eq!(
        serde_json::from_str::<TokenClaims>(golden).expect("deserializes"),
        root
    );
}

/// ADR-0047, point 2.4: a delegation ID is a token ID, so the denylist names it as one.
#[test]
fn a_delegation_id_is_a_token_id() {
    let id = DelegationId::from_bytes([0x42; 16]);
    let as_token: TokenId = id.clone().into();
    assert_eq!(as_token.as_str(), id.as_str());
    assert_eq!(id.as_token_id(), &as_token);
    assert_eq!(DelegationId::try_from(as_token), Ok(id.clone()));
    assert!(DelegationId::try_from(token("root-jti")).is_err());
    let json = serde_json::to_string(&id).expect("serializes");
    assert_eq!(
        serde_json::from_str::<DelegationId>(&json).expect("deserializes"),
        id
    );
    assert!(serde_json::from_str::<DelegationId>(r#""root-jti""#).is_err());
}

fn at(millis: u64) -> Timestamp {
    Timestamp::from_millis(millis)
}

fn later(time: Timestamp, span: Millis) -> Timestamp {
    time.saturating_add(span)
}

/// ADR-0047, point 3.1 to 3.3: the subject, the kind, the parent, and the chain.
#[test]
fn issuer_signed_claims_need_a_delegated_subject_its_parent_and_a_link_per_level() {
    let valid = claims();
    assert_eq!(valid.check_issuer_signed(), Ok(agent("agent-42.c7")));
    assert_eq!(valid.check_recordable(), Ok(agent("agent-42.c7")));

    let refused = |claims: TokenClaims, want: ClaimsViolation, case: &str| {
        assert_eq!(claims.check_issuer_signed(), Err(want.clone()), "{case}");
        assert_eq!(claims.check_recordable(), Err(want), "{case}");
    };

    // Point 3.1.
    for subject in ["agent-42", "workload.prod", "agent-42.c-7", "agent-42-x"] {
        refused(
            TokenClaims {
                principal: principal(subject),
                ..claims()
            },
            ClaimsViolation::NotDelegated,
            subject,
        );
    }

    // Point 3.2.
    refused(
        TokenClaims {
            kind: None,
            ..claims()
        },
        ClaimsViolation::NotAgent,
        "no kind",
    );
    for parent in [None, Some("agent-43"), Some("agent-42.c7"), Some("agent-4")] {
        refused(
            TokenClaims {
                parent: parent.map(principal),
                ..claims()
            },
            ClaimsViolation::Parent,
            &format!("parent {parent:?}"),
        );
    }

    // Point 3.3: one link per level, at most eight.
    for links in [0, 2] {
        refused(
            TokenClaims {
                chain: vec![link("root-jti", "authority"); links],
                ..claims()
            },
            ClaimsViolation::ChainLength { links, depth: 1 },
            &format!("{links} links"),
        );
    }
    let deepest = agent("agent-42.a.b.c.d.e.f.g.h");
    let at_depth = |links: usize| TokenClaims {
        principal: deepest.as_principal().clone(),
        parent: deepest.parent().map(PrincipalName::from),
        chain: vec![link("an-ancestor", "a-key"); links],
        scopes: scopes(&["admin:agent-42.a.b.c.d.e.f.g.h-*"]),
        ..claims()
    };
    assert_eq!(
        at_depth(MAX_DEPTH).check_issuer_signed(),
        Ok(deepest.clone())
    );
    refused(
        at_depth(MAX_DEPTH + 1),
        ClaimsViolation::ChainLength {
            links: MAX_DEPTH + 1,
            depth: MAX_DEPTH,
        },
        "nine links at depth eight",
    );

    // Points 3.4 to 3.7, through `tier::check`.
    for (text, violation) in [
        ("fleet", TierViolation::Fleet),
        (
            "admin:agent-43-*",
            TierViolation::OutsideRoot {
                scope: scope("admin:agent-43-*"),
            },
        ),
        (
            "grant:agent-42.d1.*",
            TierViolation::GrantOutsideChild {
                scope: scope("grant:agent-42.d1.*"),
            },
        ),
        (
            "admin:agent-42.c7x*",
            TierViolation::NoSeparator {
                scope: scope("admin:agent-42.c7x*"),
            },
        ),
    ] {
        refused(
            TokenClaims {
                scopes: scopes(&[text]),
                ..claims()
            },
            ClaimsViolation::Tier(violation),
            text,
        );
    }

    // The first rule broken, in point order, is the one named.
    refused(
        TokenClaims {
            kind: None,
            chain: Vec::new(),
            scopes: scopes(&["fleet"]),
            issued_at: None,
            ..claims()
        },
        ClaimsViolation::NotAgent,
        "every rule from 3.2 on broken",
    );
    refused(
        TokenClaims {
            scopes: scopes(&["fleet"]),
            issued_at: None,
            ..claims()
        },
        ClaimsViolation::Tier(TierViolation::Fleet),
        "the tier and the times broken",
    );
}

/// ADR-0047, points 3.8 and 5.9: `iat ≤ nbf < exp`, at most 24 hours from `nbf` to `exp`, and,
/// for a recorded delegation only, `nbf` at most 24 hours after `iat`; each at its boundary.
#[test]
fn issuer_signed_claims_times_hold_at_each_boundary() {
    let day = Millis::new(24 * 60 * 60 * 1_000);
    assert_eq!(MAX_DELEGATED_LIFETIME, day);
    assert_eq!(MAX_NOT_BEFORE_LEAD, day);
    let millisecond = Millis::new(1);
    let times =
        |issued_at: Option<Timestamp>, not_before: Timestamp, expires_at: Timestamp| TokenClaims {
            issued_at,
            not_before,
            expires_at,
            ..claims()
        };
    let iat = at(1_000_000);

    let accepted = [
        ("iat = nbf", times(Some(iat), iat, later(iat, millisecond))),
        (
            "iat before nbf",
            times(
                Some(iat),
                later(iat, millisecond),
                later(iat, Millis::new(2)),
            ),
        ),
        (
            "exactly 24 hours",
            times(Some(iat), iat, later(iat, MAX_DELEGATED_LIFETIME)),
        ),
        (
            "nbf exactly 24 hours after iat",
            times(
                Some(iat),
                later(iat, MAX_NOT_BEFORE_LEAD),
                later(iat, Millis::new(day.get() * 2)),
            ),
        ),
    ];
    for (case, claims) in &accepted {
        assert!(claims.check_issuer_signed().is_ok(), "{case}");
        assert!(claims.check_recordable().is_ok(), "{case}");
    }

    let refused = [
        (
            "no iat",
            times(None, iat, later(iat, day)),
            ClaimsViolation::NoIssuedAt,
        ),
        (
            "iat 1 ms after nbf",
            times(Some(later(iat, millisecond)), iat, later(iat, day)),
            ClaimsViolation::IssuedAfterStart,
        ),
        (
            "nbf = exp",
            times(Some(iat), iat, iat),
            ClaimsViolation::EndsBeforeStart,
        ),
        (
            "nbf after exp",
            times(Some(iat), later(iat, millisecond), iat),
            ClaimsViolation::EndsBeforeStart,
        ),
        (
            "24 hours and 1 ms",
            times(
                Some(iat),
                iat,
                later(iat, MAX_DELEGATED_LIFETIME.saturating_add(millisecond)),
            ),
            ClaimsViolation::TooLong,
        ),
        (
            "nbf near the end of time",
            times(Some(iat), at(u64::MAX - 10), Timestamp::MAX),
            ClaimsViolation::StartsTooLate,
        ),
    ];
    for (case, claims, want) in refused {
        if want == ClaimsViolation::StartsTooLate {
            assert!(
                claims.check_issuer_signed().is_ok(),
                "{case}: nodes don't check the lead"
            );
        } else {
            assert_eq!(claims.check_issuer_signed(), Err(want.clone()), "{case}");
        }
        assert_eq!(claims.check_recordable(), Err(want), "{case}");
    }

    // The lead: 24 hours is recordable, 24 hours and 1 ms isn't, though a node accepts both.
    let late = later(iat, MAX_NOT_BEFORE_LEAD.saturating_add(millisecond));
    let too_late = times(Some(iat), late, later(late, day));
    assert!(
        too_late.check_issuer_signed().is_ok(),
        "nodes don't check the lead"
    );
    assert_eq!(
        too_late.check_recordable(),
        Err(ClaimsViolation::StartsTooLate)
    );
}
