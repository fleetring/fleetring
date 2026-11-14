//! Shorthands for delegation tests: a fleet whose members support `delegation`, a root agent's
//! presented token, `RecordDelegation` commands built from a caller's claims, and revocations
//! under the ancestor rule.

use fleetring_control::delegation::{
    AncestorTarget, CallerToken, CanonicalRequest, DelegationId, Resent, TokenClaims, TokenKind,
};
use fleetring_control::fork::OperationId;
use fleetring_control::scope::{Scope, Scopes};
use fleetring_control::{
    Action, Applied, Command, KeyId, Member, Outcome, PrincipalName, Rejection, State, Timestamp,
    TokenId, apply,
};

use crate::common::{at, config, current_software, id};

/// The control node that stamps every delegation command.
pub(crate) const RECORDER: &str = "c1";

/// The key that signs root tokens.
pub(crate) const AUTHORITY: &str = "authority";

/// The issuer key that signs delegated copies.
pub(crate) const ISSUER: &str = "issuer-a";

/// One hour in milliseconds.
pub(crate) const HOUR: u64 = 3_600_000;

pub(crate) fn principal(name: &str) -> PrincipalName {
    name.parse().expect("a principal name")
}

pub(crate) fn key(text: &str) -> KeyId {
    text.parse().expect("a key id")
}

pub(crate) fn token(text: &str) -> TokenId {
    text.parse().expect("a token id")
}

/// The scopes `texts`, in canonical form.
pub(crate) fn scopes(texts: &[&str]) -> Scopes {
    texts
        .iter()
        .map(|text| text.parse::<Scope>().expect("a scope"))
        .collect::<Scopes>()
        .normalized()
}

/// What `name` may hold to delegate further: its own databases and its children's, and a grant
/// over its children.
pub(crate) fn delegating(name: &str) -> Scopes {
    scopes(&[
        &format!("admin:{name}-*"),
        &format!("stream:{name}-*"),
        &format!("admin:{name}.*"),
        &format!("stream:{name}.*"),
        &format!("grant:{name}.*"),
    ])
}

/// A leaf's scopes: its own databases only.
pub(crate) fn leaf(name: &str) -> Scopes {
    scopes(&[&format!("admin:{name}-*"), &format!("stream:{name}-*")])
}

/// The operation with ID bytes all `n`, and a second byte `m`.
pub(crate) fn op(n: u16) -> OperationId {
    let [a, b] = n.to_le_bytes();
    let mut bytes = [0xD0; 16];
    bytes[0] = a;
    bytes[1] = b;
    OperationId::from_bytes(bytes)
}

/// The delegation ID drawn for draw `n`.
pub(crate) fn drawn(n: u32) -> DelegationId {
    let mut bytes = [0x5A; 16];
    bytes[..4].copy_from_slice(&n.to_le_bytes());
    DelegationId::from_bytes(bytes)
}

/// Root agent `name`'s token, signed by the authority, valid from 0 for 30 days, with `jti`.
pub(crate) fn root_token(name: &str, jti: &str) -> CallerToken {
    CallerToken {
        claims: TokenClaims {
            fleet: "prod".into(),
            principal: principal(name),
            id: token(jti),
            issued_at: Some(Timestamp::ZERO),
            not_before: Timestamp::ZERO,
            expires_at: Timestamp::from_millis(30 * 24 * HOUR),
            scopes: delegating(name),
            kind: Some(TokenKind::Agent),
            owner: Some(principal("sam")),
            parent: None,
            chain: Vec::new(),
        },
        kid: key(AUTHORITY),
    }
}

/// The copy of a recorded delegation's token that `kid` signed, as its holder presents it.
pub(crate) fn copy(claims: &TokenClaims, kid: &str) -> CallerToken {
    CallerToken {
        claims: claims.clone(),
        kid: key(kid),
    }
}

/// How a test delegation asks: its operation, drawn ID, segment, scopes, and times.
#[derive(Debug, Clone)]
pub(crate) struct Ask {
    pub(crate) operation: OperationId,
    pub(crate) id: DelegationId,
    pub(crate) segment: String,
    /// The scopes resolved and asked for, or the defaults if `explicit` is false.
    pub(crate) scopes: Scopes,
    pub(crate) explicit: bool,
    pub(crate) not_before: Timestamp,
    pub(crate) expires_at: Timestamp,
}

impl Ask {
    /// Operation `n` asks for `segment`'s child, drawing ID `n`, with explicit `scopes`, valid
    /// from 0 (raised to `iat`) until `expires_at`.
    pub(crate) fn new(n: u16, segment: &str, scopes: Scopes, expires_at: u64) -> Self {
        Self {
            operation: op(n),
            id: drawn(u32::from(n)),
            segment: segment.to_owned(),
            scopes,
            explicit: true,
            not_before: Timestamp::ZERO,
            expires_at: Timestamp::from_millis(expires_at),
        }
    }

    /// The `RecordDelegation` that `caller` sends.
    pub(crate) fn action(&self, caller: &CallerToken) -> Action {
        Action::RecordDelegation {
            operation: self.operation,
            requested_by: caller.claims.principal.clone(),
            request: CanonicalRequest {
                segment: self.segment.as_str().into(),
                scopes: self.explicit.then(|| self.scopes.clone()),
                lifetime: None,
                not_before: None,
                caller_token: caller.claims.id.clone(),
            },
            caller: caller.clone(),
            id: self.id.clone(),
            scopes: self.scopes.clone(),
            not_before: self.not_before,
            expires_at: self.expires_at,
        }
    }
}

/// A fleet of three members that support every feature, `delegation` included, joined at 0.
pub(crate) fn fleet() -> State {
    let mut state = State::new(config());
    for node in ["n0", "n1", "n2"] {
        let member = Member::new(id(node)).with_software(current_software());
        apply(&mut state, Command::new(at(0), Action::Join { member })).expect("join");
    }
    state
}

/// Applies `action` stamped `millis` by [`RECORDER`].
pub(crate) fn stamped(state: &mut State, millis: u64, action: Action) -> Outcome {
    apply(state, Command::stamped(at(millis), id(RECORDER), action))
}

/// Applies `action` stamped `millis` by [`RECORDER`], expecting a refusal, and checks that it left
/// the state byte for byte as it was (check first, prune last).
pub(crate) fn refused(state: &mut State, millis: u64, action: Action) -> Rejection {
    let before = serde_json::to_string(state).expect("serializes");
    let rejection = stamped(state, millis, action).expect_err("refused");
    assert_eq!(
        serde_json::to_string(state).expect("serializes"),
        before,
        "{rejection:?} changed the state"
    );
    rejection
}

/// `caller`, an agent, delegates its child `segment` at `millis` with `scopes`, as operation and
/// draw `n`, for 2 hours or until the caller's token expires; returns the recorded claims.
pub(crate) fn delegate(
    state: &mut State,
    millis: u64,
    n: u16,
    caller: &CallerToken,
    segment: &str,
    scopes: Scopes,
) -> TokenClaims {
    let expires_at = (millis - millis % 1_000 + 2 * HOUR).min(caller.claims.expires_at.as_millis());
    let ask = Ask::new(n, segment, scopes, expires_at);
    recorded(stamped(state, millis, ask.action(caller)))
}

/// `caller`'s revocation of `target` under the ancestor rule.
pub(crate) fn as_ancestor(caller: &CallerToken, target: AncestorTarget) -> Action {
    Action::RevokeAsAncestor {
        target,
        caller: caller.clone(),
    }
}

/// The ancestor-rule target that is the principal `name`.
pub(crate) fn principal_target(name: &str) -> AncestorTarget {
    AncestorTarget::Principal(name.parse().expect("an agent name"))
}

/// The claims a `RecordDelegation` recorded, or a panic naming what it returned.
pub(crate) fn recorded(outcome: Outcome) -> TokenClaims {
    match outcome {
        Ok(Applied::DelegationRecorded {
            claims,
            resent: Resent::No,
            ..
        }) => claims,
        other => panic!("expected a new delegation: {other:?}"),
    }
}
