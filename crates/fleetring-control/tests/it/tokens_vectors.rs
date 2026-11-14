//! The golden vectors of token spec v1 (`docs/spec/tokens-v1.md`, ADR-0047 point 6.5).
//!
//! `tests/vectors/tokens-v1.json` describes its own layout in its `layout` field. Every section
//! but `signed_tokens` and `refused_tokens`, which `fleetringd`'s `tests/tokens_vectors.rs`
//! checks, runs here through the scope grammar, the lattice, the agent names, the tier, the
//! claims checks, and the denylist. The file is append-only: `tests/vectors/tokens-v1.lock` freezes every case, and the
//! spec's tables must each appear in the file, so the spec and the vectors never drift.
//!
//! `include_str!` embeds the files, so these tests also run on targets without a filesystem.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::OnceLock;

use fleetring_control::delegation::tier::{self, TierViolation};
use fleetring_control::delegation::{
    AgentName, ChainLink, ClaimsViolation, DelegationId, ParseAgentNameError, SignerKind,
    TokenClaims,
};
use fleetring_control::matcher::DatabaseMatcher;
use fleetring_control::scope::{Action, GrantStepError, ParseScopeError, Scope, Scopes};
use fleetring_control::{
    DatabaseName, KeyId, PrincipalName, Revocations, Revoked, Timestamp, TokenId, TokenRef,
};
use serde_json::{Map, Value, json};

/// The vector file.
const VECTORS: &str = include_str!("../vectors/tokens-v1.json");

/// The freeze guard: one checkpoint per section, `<section> <count> <digest>`.
const LOCK: &str = include_str!("../vectors/tokens-v1.lock");

/// The spec, whose tables must each appear in the vectors.
const SPEC: &str = include_str!("../../../../docs/spec/tokens-v1.md");

/// The sections this test checks, in file order; `signed_tokens` and `refused_tokens` are
/// `fleetringd`'s.
const SECTIONS: &[&str] = &[
    "scope_texts",
    "pattern_matches",
    "scope_sets",
    "action_meets",
    "covers",
    "meets",
    "narrowing",
    "agent_names",
    "at_or_below",
    "tier",
    "claims_bases",
    "issuer_signed_claims",
    "delegation_ids",
    "token_claims_json",
    "revocations",
    "principal_revocations",
    "signed_tokens",
    "refused_tokens",
];

fn vectors() -> &'static Value {
    static PARSED: OnceLock<Value> = OnceLock::new();
    PARSED.get_or_init(|| serde_json::from_str(VECTORS).expect("tokens-v1.json is valid JSON"))
}

fn section(name: &str) -> &'static [Value] {
    vectors()[name]
        .as_array()
        .unwrap_or_else(|| panic!("tokens-v1.json has the array {name}"))
}

fn text(value: &Value) -> &str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{value} is a string"))
}

fn texts(value: &Value) -> Vec<&str> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{value} is an array"))
        .iter()
        .map(text)
        .collect()
}

fn scope(text: &str) -> Scope {
    text.parse()
        .unwrap_or_else(|e| panic!("'{text}' is a scope: {e}"))
}

fn scopes(value: &Value) -> Scopes {
    texts(value).into_iter().map(scope).collect()
}

fn scope_texts(scopes: &Scopes) -> Value {
    scopes
        .as_slice()
        .iter()
        .map(|scope| Value::String(scope.to_string()))
        .collect()
}

fn principal(text: &str) -> PrincipalName {
    text.parse()
        .unwrap_or_else(|e| panic!("'{text}' is a principal name: {e}"))
}

fn agent(text: &str) -> AgentName {
    text.parse()
        .unwrap_or_else(|e| panic!("'{text}' is an agent name: {e}"))
}

fn millis(value: &Value) -> Timestamp {
    Timestamp::from_millis(
        value
            .as_u64()
            .unwrap_or_else(|| panic!("{value} is a time in milliseconds")),
    )
}

/// An error object, `{"error": kind, …}`.
fn error(kind: &str) -> Value {
    json!({ "error": kind })
}

/// A violation object, `{"violation": kind, …fields}`.
fn violation(kind: &str, fields: &[(&str, Value)]) -> Value {
    let mut object = Map::new();
    object.insert("violation".to_owned(), Value::String(kind.to_owned()));
    for (name, value) in fields {
        object.insert((*name).to_owned(), value.clone());
    }
    Value::Object(object)
}

fn tier_result(result: Result<(), TierViolation>) -> Value {
    let named = |kind: &str, scope: &Scope| violation(kind, &[("scope", json!(scope.to_string()))]);
    match result {
        Ok(()) => json!("ok"),
        Err(TierViolation::NotDelegated) => violation("not_delegated", &[]),
        Err(TierViolation::Fleet) => violation("fleet", &[]),
        Err(TierViolation::OutsideRoot { scope }) => named("outside_root", &scope),
        Err(TierViolation::GrantOutsideChild { scope }) => named("grant_outside_child", &scope),
        Err(TierViolation::NoSeparator { scope }) => named("no_separator", &scope),
        Err(other) => panic!("a tier violation the vectors don't name: {other:?}"),
    }
}

fn claims_result(result: Result<AgentName, ClaimsViolation>) -> Value {
    let simple = |kind: &str| violation(kind, &[]);
    match result {
        Ok(_) => json!("ok"),
        Err(ClaimsViolation::NotDelegated) => simple("not_delegated"),
        Err(ClaimsViolation::NotAgent) => simple("not_agent"),
        Err(ClaimsViolation::Parent) => simple("parent"),
        Err(ClaimsViolation::ChainLength { links, depth }) => violation(
            "chain_length",
            &[("links", json!(links)), ("depth", json!(depth))],
        ),
        Err(ClaimsViolation::Tier(tier)) => violation("tier", &[("tier", tier_result(Err(tier)))]),
        Err(ClaimsViolation::NoIssuedAt) => simple("no_issued_at"),
        Err(ClaimsViolation::IssuedAfterStart) => simple("issued_after_start"),
        Err(ClaimsViolation::EndsBeforeStart) => simple("ends_before_start"),
        Err(ClaimsViolation::TooLong) => simple("too_long"),
        Err(ClaimsViolation::StartsTooLate) => simple("starts_too_late"),
        Err(other) => panic!("a claims violation the vectors don't name: {other:?}"),
    }
}

fn grant_step_result(result: Result<Scopes, GrantStepError>) -> Value {
    match result {
        Ok(scopes) => scope_texts(&scopes),
        Err(GrantStepError::NoGrant) => error("no_grant"),
        Err(GrantStepError::Empty) => error("empty"),
        Err(GrantStepError::NotCovered { scope }) => {
            json!({ "error": "not_covered", "scope": scope.to_string() })
        }
        Err(other) => panic!("a grant step error the vectors don't name: {other:?}"),
    }
}

/// Applies `patch` to `target` as RFC 7386 says: objects merge, `null` removes, anything else
/// replaces.
fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(fields) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Value::Object(object) = target else {
        unreachable!("made an object above");
    };
    for (name, value) in fields {
        if value.is_null() {
            object.remove(name);
        } else {
            merge_patch(object.entry(name.clone()).or_insert(Value::Null), value);
        }
    }
}

#[test]
fn the_file_has_every_section_and_names_its_spec() {
    let object = vectors().as_object().expect("an object");
    let present: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|key| !matches!(*key, "spec" | "layout"))
        .collect();
    let expected: BTreeSet<&str> = SECTIONS.iter().copied().collect();
    assert_eq!(
        present.iter().copied().collect::<BTreeSet<_>>(),
        expected,
        "tokens-v1.json's sections"
    );
    assert_eq!(vectors()["spec"], "docs/spec/tokens-v1.md");
    for name in SECTIONS {
        assert!(!section(name).is_empty(), "{name} has cases");
    }
}

/// Spec section 2: every accepted text is the canonical text of what it parses to.
#[test]
fn scope_texts_parse_as_the_vectors_say() {
    for case in section("scope_texts") {
        let input = text(&case["text"]);
        let parsed = input.parse::<Scope>();
        if let Some(kind) = case.get("error") {
            let got = match parsed {
                Err(ParseScopeError::Shape { .. }) => "shape",
                Err(ParseScopeError::UnknownAction { .. }) => "unknown_action",
                Err(ParseScopeError::EmptyPattern { .. }) => "empty_pattern",
                Err(other) => panic!("{input:?}: an error the vectors don't name: {other:?}"),
                Ok(scope) => panic!("{input:?} parsed as {scope}, but the vectors refuse it"),
            };
            assert_eq!(got, text(kind), "{input:?}");
            continue;
        }
        let parsed = parsed.unwrap_or_else(|e| panic!("{input:?} is a scope: {e}"));
        assert_eq!(parsed.to_string(), text(&case["canonical"]), "{input:?}");
        assert_eq!(
            text(&case["canonical"]),
            input,
            "{input:?}: accepted text is canonical"
        );
        let expected = &case["scope"];
        if expected == "fleet" {
            assert_eq!(parsed, Scope::fleet(), "{input:?}");
            continue;
        }
        let action: Action = text(&expected["action"]).parse().expect("an action");
        let (pattern_text, prefix) = match (expected.get("exact"), expected.get("prefix")) {
            (Some(exact), None) => (text(exact).to_owned(), None),
            (None, Some(prefix)) => (format!("{}*", text(prefix)), Some(text(prefix))),
            _ => panic!("{input:?}: a scope is exact or a prefix"),
        };
        let pattern: DatabaseMatcher = pattern_text.parse().expect("a pattern");
        assert_eq!(pattern.prefix(), prefix, "{input:?}");
        assert_eq!(parsed, Scope::new(action, pattern), "{input:?}");
    }
}

/// Spec section 2.2: a pattern matches names byte for byte.
#[test]
fn patterns_match_as_the_vectors_say() {
    for case in section("pattern_matches") {
        let pattern: DatabaseMatcher = text(&case["pattern"]).parse().expect("a pattern");
        let database: DatabaseName = text(&case["database"]).parse().expect("a database name");
        assert_eq!(
            Value::Bool(pattern.matches(&database)),
            case["matches"],
            "{case}"
        );
    }
}

/// Spec section 3.3: the canonical form of a set, which is its own canonical form.
#[test]
fn scope_sets_normalize_as_the_vectors_say() {
    for case in section("scope_sets") {
        let canonical = scopes(&case["scopes"]).normalized();
        assert_eq!(scope_texts(&canonical), case["canonical"], "{case}");
        assert_eq!(canonical.normalized(), canonical, "{case}: idempotent");
    }
}

/// Spec section 3.1: the meet of two actions, both ways round.
#[test]
fn actions_meet_as_the_vectors_say() {
    let mut pairs = BTreeSet::new();
    for case in section("action_meets") {
        let a: Action = text(&case["a"]).parse().expect("an action");
        let b: Action = text(&case["b"]).parse().expect("an action");
        for (x, y) in [(a, b), (b, a)] {
            let meet = x.meet(y).map_or(Value::Null, |m| json!(m.as_str()));
            assert_eq!(meet, case["meet"], "{x} ∧ {y}");
        }
        pairs.insert((a.min(b), a.max(b)));
    }
    assert_eq!(pairs.len(), 21, "every unordered pair of the six actions");
}

/// Spec section 3.2: covering, and the meet of two scopes, both ways round.
#[test]
fn scopes_cover_and_meet_as_the_vectors_say() {
    for case in section("covers") {
        let (held, wanted) = (scope(text(&case["held"])), scope(text(&case["wanted"])));
        assert_eq!(Value::Bool(held.covers(&wanted)), case["covers"], "{case}");
    }
    for case in section("meets") {
        let (a, b) = (scope(text(&case["a"])), scope(text(&case["b"])));
        for (x, y) in [(&a, &b), (&b, &a)] {
            let meet = x.meet(y).map_or(Value::Null, |m| json!(m.to_string()));
            assert_eq!(meet, case["meet"], "{x} ∧ {y}");
        }
    }
}

/// Spec sections 3.4 and 3.5: the intersection, the grant step, and the strict grant step.
#[test]
fn narrowing_gives_what_the_vectors_say() {
    for case in section("narrowing") {
        let (effective, requested) = (scopes(&case["effective"]), scopes(&case["requested"]));
        match text(&case["operation"]) {
            "intersect" => {
                let got = effective.intersect(&requested).expect("within the bound");
                assert_eq!(scope_texts(&got), case["result"], "{case}");
                let other = requested.intersect(&effective).expect("within the bound");
                assert_eq!(got, other, "{case}: commutative");
                assert!(case.get("strict").is_none(), "{case}");
            }
            "grant_step" => {
                let got = grant_step_result(effective.grant_step(&requested));
                assert_eq!(got, case["result"], "{case}");
                let strict = grant_step_result(effective.grant_step_strict(&requested));
                assert_eq!(strict, case["strict"], "{case}: strict");
            }
            other => panic!("unknown operation {other}"),
        }
    }
}

/// Spec section 4.1: agent names, their depth, root, and parent.
#[test]
fn agent_names_parse_as_the_vectors_say() {
    for case in section("agent_names") {
        let input = text(&case["text"]);
        let parsed = input.parse::<AgentName>();
        if let Some(kind) = case.get("error") {
            let got = match parsed {
                Err(ParseAgentNameError::Principal(_)) => "principal",
                Err(ParseAgentNameError::NotAgent { .. }) => "not_agent",
                Err(ParseAgentNameError::Segment { .. }) => "segment",
                Err(ParseAgentNameError::TooDeep { .. }) => "too_deep",
                Err(other) => panic!("{input:?}: an error the vectors don't name: {other:?}"),
                Ok(name) => panic!("{input:?} parsed as {name}, but the vectors refuse it"),
            };
            assert_eq!(got, text(kind), "{input:?}");
            continue;
        }
        let name = parsed.unwrap_or_else(|e| panic!("{input:?} is an agent name: {e}"));
        assert_eq!(json!(name.depth()), case["depth"], "{input}");
        assert_eq!(json!(name.root().as_str()), case["root"], "{input}");
        let parent = name.parent().map_or(Value::Null, |p| json!(p.as_str()));
        assert_eq!(parent, case["parent"], "{input}");
        assert_eq!(name.is_delegated(), name.depth() > 0, "{input}");
    }
}

/// Spec section 4.2: at or below.
#[test]
fn at_or_below_compares_as_the_vectors_say() {
    for case in section("at_or_below") {
        let name = principal(text(&case["name"]));
        let ancestor = principal(text(&case["ancestor"]));
        assert_eq!(
            Value::Bool(name.is_at_or_below(&ancestor)),
            case["result"],
            "{case}"
        );
    }
}

/// Spec section 7.2: the agent tier.
#[test]
fn the_tier_check_gives_what_the_vectors_say() {
    for case in section("tier") {
        let got = tier_result(tier::check(
            &agent(text(&case["agent"])),
            &scopes(&case["scopes"]),
        ));
        assert_eq!(got, case["result"], "{case}");
    }
}

/// The base claims a case names, patched.
fn patched_claims(case: &Value) -> TokenClaims {
    let base = section("claims_bases")
        .iter()
        .find(|base| base["name"] == case["base"])
        .unwrap_or_else(|| panic!("{case}: no base named {}", case["base"]));
    let mut claims = base["claims"].clone();
    merge_patch(&mut claims, &case["patch"]);
    serde_json::from_value(claims).unwrap_or_else(|e| panic!("{case}: claims: {e}"))
}

/// Spec sections 7.1 and 7.3: the structural checks of nodes, and of `apply` and the signer.
#[test]
fn issuer_signed_claims_check_as_the_vectors_say() {
    for base in section("claims_bases") {
        serde_json::from_value::<TokenClaims>(base["claims"].clone())
            .unwrap_or_else(|e| panic!("{}: claims: {e}", base["name"]));
    }
    for case in section("issuer_signed_claims") {
        let claims = patched_claims(case);
        let name = &case["name"];
        assert_eq!(
            claims_result(claims.check_issuer_signed()),
            case["issuer_signed"],
            "{name}"
        );
        assert_eq!(
            claims_result(claims.check_recordable()),
            case["recordable"],
            "{name}: recordable"
        );
    }
}

/// Spec section 5.2: a delegation ID has one spelling.
#[test]
fn delegation_ids_encode_and_parse_as_the_vectors_say() {
    for case in section("delegation_ids") {
        let input = text(&case["text"]);
        if case.get("error").is_some() {
            assert!(input.parse::<DelegationId>().is_err(), "{input:?}");
            assert_eq!(
                Value::Bool(input.parse::<TokenId>().is_ok()),
                case["token_id"],
                "{input:?}: a token id"
            );
            continue;
        }
        let hex = text(&case["bytes"]);
        assert_eq!(hex.len(), 32, "{input}: 16 bytes");
        let bytes: [u8; 16] =
            std::array::from_fn(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex"));
        let id = DelegationId::from_bytes(bytes);
        assert_eq!(id.as_str(), input);
        assert_eq!(input.parse::<DelegationId>(), Ok(id.clone()), "{input}");
        assert!(
            matches!(input.chars().last(), Some('A' | 'Q' | 'g' | 'w')),
            "{input}"
        );
        assert_eq!(id.as_token_id().as_str(), input, "{input}: a token id");
    }
}

/// Spec section 6.4: `TokenClaims`' JSON, byte for byte.
#[test]
fn token_claims_serialize_as_the_vectors_say() {
    for case in section("token_claims_json") {
        let golden = text(&case["json"]);
        let claims: TokenClaims =
            serde_json::from_str(golden).unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
        assert_eq!(
            serde_json::to_string(&claims).expect("serializes"),
            golden,
            "{}",
            case["name"]
        );
    }
}

fn signer(value: &Value) -> SignerKind {
    match text(value) {
        "authority" => SignerKind::Authority,
        "issuer" => SignerKind::Issuer,
        other => panic!("unknown signer {other}"),
    }
}

/// Spec section 8.1: the denylist along the lineage, and which entry it names.
#[test]
fn revocations_refuse_as_the_vectors_say() {
    for case in section("revocations") {
        let token = &case["token"];
        let id: TokenId = text(&token["id"]).parse().expect("a token id");
        let kid: KeyId = text(&token["kid"]).parse().expect("a key id");
        let chain: Vec<ChainLink> =
            serde_json::from_value(token["chain"].clone()).expect("a chain");
        let name = principal(text(&token["principal"]));
        let issued_at = (!token["issued_at"].is_null()).then(|| millis(&token["issued_at"]));
        let entries: Vec<Revoked> =
            serde_json::from_value(case["denylist"].clone()).expect("denylist entries");
        let list: Revocations = entries
            .into_iter()
            .map(|revoked| (revoked, Timestamp::MAX))
            .collect();
        let token = TokenRef {
            id: &id,
            kid: &kid,
            chain: &chain,
            principal: &name,
            issued_at,
            signer: signer(&token["signer"]),
        };
        let got = list
            .refuses_token(&token)
            .map_or(Value::Null, |revoked| json!(revoked));
        assert_eq!(got, case["refused_by"], "{}", case["name"]);
    }
}

/// Spec section 8.2: a principal revocation, with the authority's leeway.
#[test]
fn principal_revocations_refuse_as_the_vectors_say() {
    let (id, kid): (TokenId, KeyId) = ("t".parse().expect("an id"), "k".parse().expect("a key"));
    for case in section("principal_revocations") {
        let name = principal(text(&case["principal"]));
        let token = TokenRef {
            id: &id,
            kid: &kid,
            chain: &[],
            principal: &name,
            issued_at: (!case["issued_at"].is_null()).then(|| millis(&case["issued_at"])),
            signer: signer(&case["signer"]),
        };
        let refused =
            token.refused_by_principal(&principal(text(&case["revoked"])), millis(&case["before"]));
        assert_eq!(Value::Bool(refused), case["refused"], "{case}");
    }
}

/// The claims of every signed token parse; `fleetringd` checks the signatures and encoding.
#[test]
fn signed_tokens_carry_valid_claims() {
    for case in section("signed_tokens") {
        let claims: TokenClaims = serde_json::from_value(case["claims"].clone())
            .unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
        if case["signer"] == "issuer" {
            claims
                .check_recordable()
                .unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
        }
    }
}

// The freeze guard.

/// FNV-1a, 64 bits: a small, fixed hash for the freeze guard, so this crate needs no hashing
/// dependency.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// `value` as compact JSON with object keys sorted bytewise, so the digest doesn't depend on how
/// a parser orders fields.
fn canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(object) => {
            let mut fields: Vec<(&String, &Value)> = object.iter().collect();
            fields.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push('{');
            for (index, (name, field)) in fields.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(name.clone()).to_string());
                out.push(':');
                canonical(field, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                canonical(item, out);
            }
            out.push(']');
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            out.push_str(&value.to_string());
        }
    }
}

/// The digest of the first `count` cases of `name`.
fn frozen_digest(name: &str, count: usize) -> u64 {
    let mut text = String::new();
    for case in &section(name)[..count] {
        canonical(case, &mut text);
        text.push('\n');
    }
    fnv1a(text.as_bytes())
}

/// The lock's checkpoints, in order: section, case count, digest.
fn checkpoints() -> Vec<(&'static str, usize, u64)> {
    LOCK.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [name, count, digest] = fields[..] else {
                panic!("tokens-v1.lock: expected `<section> <count> <digest>`, found {line:?}");
            };
            let count = count.parse().expect("a case count");
            let digest = u64::from_str_radix(digest, 16).expect("16 hex digits");
            (name, count, digest)
        })
        .collect()
}

/// Every case of every section is frozen, and no frozen case changed: the vectors are
/// append-only (spec section 9).
#[test]
fn every_case_is_frozen_and_none_changed() {
    let mut frozen: Vec<(&str, usize)> = Vec::new();
    for (name, count, digest) in checkpoints() {
        assert!(
            SECTIONS.contains(&name),
            "tokens-v1.lock names no section {name}"
        );
        let cases = section(name).len();
        assert!(
            count <= cases,
            "tokens-v1.lock freezes {count} cases of {name}, but tokens-v1.json holds only \
             {cases}: frozen cases are never removed"
        );
        if let Some((_, before)) = frozen.iter().rev().find(|(section, _)| *section == name) {
            assert!(count > *before, "{name}'s checkpoints only ever grow");
        }
        let actual = frozen_digest(name, count);
        assert_eq!(
            actual, digest,
            "the first {count} cases of {name} changed: they hash to {actual:016x}, but \
             tokens-v1.lock froze them as {digest:016x}. Golden vectors are append-only: \
             restore the frozen cases and add new ones at the end of the section. A change to a \
             frozen result is a new spec version."
        );
        frozen.push((name, count));
    }
    let mut missing = String::new();
    for name in SECTIONS {
        let cases = section(name).len();
        let last = frozen
            .iter()
            .rev()
            .find(|(section, _)| section == name)
            .map_or(0, |(_, count)| *count);
        if last < cases {
            let digest = frozen_digest(name, cases);
            writeln!(missing, "{name} {cases} {digest:016x}").expect("writes to a string");
        }
    }
    assert!(
        missing.is_empty(),
        "cases not yet frozen; once they are right, append to tokens-v1.lock:\n{missing}"
    );
}

// The spec's tables.

/// The rows of every table in the spec whose header row is `header`, as cells.
fn spec_rows(header: &str) -> Vec<Vec<&'static str>> {
    let mut rows = Vec::new();
    let mut lines = SPEC.lines().peekable();
    let mut tables = 0;
    while let Some(line) = lines.next() {
        if line.trim() != header {
            continue;
        }
        tables += 1;
        lines.next(); // The delimiter row.
        while let Some(row) = lines.next_if(|line| line.starts_with('|')) {
            let cells: Vec<&str> = row
                .trim()
                .trim_matches('|')
                .split('|')
                .map(str::trim)
                .collect();
            rows.push(cells);
        }
    }
    assert!(tables > 0, "the spec has a table headed {header}");
    rows
}

/// The code spans of a cell, in order.
fn spans(cell: &str) -> Vec<&str> {
    cell.split('`').skip(1).step_by(2).collect()
}

/// The code spans of a cell that are scopes.
fn scope_spans(cell: &str) -> Vec<String> {
    spans(cell)
        .into_iter()
        .filter(|span| span.parse::<Scope>().is_ok())
        .map(str::to_owned)
        .collect()
}

#[test]
fn every_row_of_the_spec_narrowing_table_is_a_vector() {
    for row in spec_rows("| Effective | Block | Operation | Result |") {
        let [effective, block, operation, result] = row[..] else {
            panic!("a narrowing row has four cells: {row:?}");
        };
        let operation = match operation {
            "intersect" => "intersect",
            "grant step" => "grant_step",
            other => panic!("unknown operation {other}"),
        };
        let (effective, block) = (scope_spans(effective), scope_spans(block));
        let case = section("narrowing")
            .iter()
            .find(|case| {
                texts(&case["effective"]) == effective
                    && texts(&case["requested"]) == block
                    && case["operation"] == operation
            })
            .unwrap_or_else(|| panic!("no narrowing vector for the spec's row {row:?}"));
        if result.starts_with("refused") {
            assert!(case["result"].get("error").is_some(), "{row:?}");
        } else if result.starts_with("nothing") {
            assert_eq!(case["result"], json!([]), "{row:?}");
        } else {
            assert_eq!(case["result"], json!(scope_spans(result)), "{row:?}");
        }
    }
}

#[test]
fn every_row_of_the_spec_tier_table_is_a_vector() {
    for row in spec_rows("| Agent | Scopes | Result |") {
        let [name, set, result] = row[..] else {
            panic!("a tier row has three cells: {row:?}");
        };
        let name = spans(name)[0];
        let set = scope_spans(set);
        let case = section("tier")
            .iter()
            .find(|case| case["agent"] == name && texts(&case["scopes"]) == set)
            .unwrap_or_else(|| panic!("no tier vector for the spec's row {row:?}"));
        if result.starts_with("accepted") {
            assert_eq!(case["result"], "ok", "{row:?}");
        } else {
            assert_eq!(case["result"]["violation"], spans(result)[0], "{row:?}");
        }
    }
}

#[test]
fn every_row_of_the_spec_scope_text_table_is_a_vector() {
    for row in spec_rows("| Scope text | Result |") {
        let input = spans(row[0])[0];
        let case = section("scope_texts")
            .iter()
            .find(|case| case["text"] == input)
            .unwrap_or_else(|| panic!("no scope text vector for the spec's row {row:?}"));
        if row[1].starts_with("refused") {
            assert_eq!(case["error"], spans(row[1])[0], "{row:?}");
        } else {
            assert!(case.get("error").is_none(), "{row:?}");
        }
    }
}

#[test]
fn every_row_of_the_spec_agent_name_table_is_a_vector() {
    for row in spec_rows("| Name | Result |") {
        let input = spans(row[0])[0];
        let case = section("agent_names")
            .iter()
            .find(|case| case["text"] == input)
            .unwrap_or_else(|| panic!("no agent name vector for the spec's row {row:?}"));
        if row[1].starts_with("refused") {
            assert_eq!(case["error"], spans(row[1])[0], "{row:?}");
        } else {
            let depth: u64 = row[1]
                .trim_start_matches("depth ")
                .split(|c: char| !c.is_ascii_digit())
                .next()
                .and_then(|digits| digits.parse().ok())
                .unwrap_or_else(|| panic!("{row:?}: `depth <n>`"));
            assert_eq!(case["depth"], depth, "{row:?}");
        }
    }
}

#[test]
fn every_row_of_the_spec_delegation_id_table_is_a_vector() {
    for row in spec_rows("| Delegation ID | Result |") {
        let input = spans(row[0])[0];
        let case = section("delegation_ids")
            .iter()
            .find(|case| case["text"] == input)
            .unwrap_or_else(|| panic!("no delegation ID vector for the spec's row {row:?}"));
        assert_eq!(
            row[1].starts_with("refused"),
            case.get("error").is_some(),
            "{row:?}"
        );
    }
}
