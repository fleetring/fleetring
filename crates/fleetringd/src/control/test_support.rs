//! Shorthands for the control plane's unit tests.

use fleetring_control::delegation::{
    CallerToken, CanonicalRequest, DelegationId, TokenClaims, TokenKind,
};
use fleetring_control::fork::OperationId;
use fleetring_control::scope::Scopes;
use fleetring_control::{Action, Feature, Member, Release, Software, Timestamp};

/// A member that reports every fleet feature, `delegation` included.
pub(crate) fn member_with_every_feature(id: &str) -> Member {
    let software = Feature::ALL.into_iter().fold(
        Software::new(Release::new(0, 8, 0), 1),
        Software::with_feature,
    );
    Member::new(id.parse().expect("an id")).with_software(software)
}

/// The scopes `texts`, in canonical form.
pub(crate) fn scopes(texts: &[&str]) -> Scopes {
    texts
        .iter()
        .map(|text| text.parse().expect("a scope"))
        .collect::<Scopes>()
        .normalized()
}

/// A `RecordDelegation` from root `agent-42`'s token for the child `agent-42.c7`, lasting
/// `life_ms` from `now` rounded down to the second, under operation and delegation IDs whose
/// bytes are all `n`.
pub(crate) fn root_delegation(now: Timestamp, n: u8, life_ms: u64) -> Action {
    delegation_from_root(now, n, &["admin:agent-42.c7-*"], life_ms)
}

/// A `RecordDelegation` from root `agent-42`'s token for the child `agent-42.c7` with
/// `scope_texts`, lasting `life_ms` from `now` rounded down to the second, under operation and
/// delegation IDs whose bytes are all `n`.
pub(crate) fn delegation_from_root(
    now: Timestamp,
    n: u8,
    scope_texts: &[&str],
    life_ms: u64,
) -> Action {
    let scopes = scopes(scope_texts);
    let start = now.as_millis() - now.as_millis() % 1_000;
    let caller = CallerToken {
        claims: TokenClaims {
            fleet: "prod".into(),
            principal: "agent-42".parse().expect("a name"),
            id: "root-jti".parse().expect("an id"),
            issued_at: Some(Timestamp::ZERO),
            not_before: Timestamp::ZERO,
            expires_at: Timestamp::MAX,
            scopes: self::scopes(&["admin:agent-42.*", "grant:agent-42.*"]),
            kind: Some(TokenKind::Agent),
            owner: Some("sam".parse().expect("a name")),
            parent: None,
            chain: Vec::new(),
        },
        kid: "authority".parse().expect("a key id"),
    };
    Action::RecordDelegation {
        operation: OperationId::from_bytes([n; 16]),
        requested_by: caller.claims.principal.clone(),
        request: CanonicalRequest {
            segment: "c7".into(),
            scopes: Some(scopes.clone()),
            lifetime: None,
            not_before: None,
            caller_token: caller.claims.id.clone(),
        },
        caller,
        id: DelegationId::from_bytes([n; 16]),
        scopes,
        not_before: Timestamp::from_millis(start),
        expires_at: Timestamp::from_millis(start + life_ms),
    }
}
