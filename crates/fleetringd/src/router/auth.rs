//! Who a request comes from, before the router contacts any node (ADR-0037, point 18).
//!
//! A request carries a FleetRing token in `Authorization: Bearer`. The router verifies it with
//! the fleet's authority keys and the fleet name its own certificate carries, at its own clock,
//! and checks `read` on the database the request names. The router has no denylist: data nodes,
//! which do, check the token again on every call the router makes with it, so a revoked token
//! that passes here is refused there (ADR-0026, point 6).

use fleetring_control::{DatabaseName, Revocations, Timestamp, TokenId};
use hyper::HeaderMap;
use hyper::header::AUTHORIZATION;

use crate::auth::{Action, BearerToken, Scopes, Target, TokenVerifier};
use crate::tls::Principal;
use crate::transport::Credentials;

/// A request's verified caller: the token's principal and scopes, and the token itself, which
/// the router forwards unchanged on every call it makes for the request. `Debug` never shows
/// the token.
#[derive(Clone)]
pub(crate) struct Caller {
    pub(crate) principal: Principal,
    pub(crate) token_id: TokenId,
    scopes: Scopes,
    /// The token as the router sends it on to nodes.
    pub(crate) credentials: Credentials,
}

impl std::fmt::Debug for Caller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Caller")
            .field("principal", &self.principal)
            .field("token_id", &self.token_id)
            .finish_non_exhaustive()
    }
}

/// Why a request was refused before anything ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// No token, or one that doesn't verify: `401`.
    Unauthenticated(String),
    /// A token that doesn't allow the request: `403`.
    PermissionDenied(String),
}

/// The caller `headers` carry a token for, verified with `verifier` at `now`.
///
/// # Errors
///
/// [`Refusal::Unauthenticated`] without an `Authorization: Bearer` header, or with a token that
/// doesn't verify.
pub(crate) fn authenticate(
    headers: &HeaderMap,
    verifier: &TokenVerifier,
    now: Timestamp,
) -> Result<Caller, Refusal> {
    let header = headers.get(AUTHORIZATION).ok_or_else(|| {
        Refusal::Unauthenticated(
            "the request has no token: send a FleetRing token as Authorization: Bearer <token>"
                .to_owned(),
        )
    })?;
    let text = header
        .to_str()
        .map_err(|_| Refusal::Unauthenticated("the authorization header isn't text".to_owned()))?;
    let token = text
        .strip_prefix("Bearer ")
        .or_else(|| text.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            Refusal::Unauthenticated("the authorization header isn't a bearer token".to_owned())
        })?;
    let token = BearerToken::new(token);
    // The router has no denylist; the nodes it calls check revocations.
    let verified = verifier
        .verify(&token, now, &Revocations::default())
        .map_err(|error| Refusal::Unauthenticated(format!("the token is refused: {error}")))?;
    let credentials = Credentials::bearer(&token).map_err(|error| {
        Refusal::Unauthenticated(format!("the token can't be forwarded: {error}"))
    })?;
    Ok(Caller {
        principal: verified.spec.principal,
        token_id: verified.spec.id,
        scopes: verified.spec.scopes,
        credentials,
    })
}

/// Checks that `caller` may read `database`, the least any request through the router needs.
///
/// # Errors
///
/// [`Refusal::PermissionDenied`] naming the database, never the caller's scopes.
pub(crate) fn require_read(caller: &Caller, database: &DatabaseName) -> Result<(), Refusal> {
    if caller
        .scopes
        .allows(Target::Database(Action::Read, database))
    {
        return Ok(());
    }
    tracing::warn!(
        target: "fleetringd::audit",
        principal = %caller.principal,
        credential = %format!("token:{}", caller.token_id),
        action = "read",
        database = database.as_str(),
        rpc = "router",
        "denied",
    );
    Err(Refusal::PermissionDenied(format!(
        "this token can't read database '{database}'"
    )))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fleetring_control::Millis;

    use super::*;
    use crate::auth::{Scope, TokenSigner, TokenSpec};
    use crate::tls::{Authority, Identity, Validity};

    fn signer_and_verifier() -> (TokenSigner, TokenVerifier) {
        let validity = Validity::new(Timestamp::ZERO, Duration::from_secs(100 * 365 * 24 * 3600));
        let authority =
            Authority::create("test".parse().expect("a fleet"), validity).expect("an authority");
        let issued = authority
            .issue_client(&"router".parse().expect("a principal"), validity)
            .expect("a client certificate");
        let identity =
            Identity::from_issued(authority.certificate_pem(), &issued).expect("an identity");
        let signer = authority.token_signer().expect("a signer");
        (signer, identity.token_verifier())
    }

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, value.parse().expect("header"));
        headers
    }

    #[test]
    fn a_token_is_required_and_verified() {
        let (signer, verifier) = signer_and_verifier();
        let now = Timestamp::from_millis(1_000_000);
        let token = signer
            .issue(&TokenSpec {
                principal: "app".parse().expect("principal"),
                scopes: ["read:notes".parse::<Scope>().expect("a scope")]
                    .into_iter()
                    .collect(),
                id: "t1".parse().expect("token id"),
                not_before: now,
                expires_at: now.saturating_add(Millis::new(60_000)),
            })
            .expect("issued");
        let missing = authenticate(&HeaderMap::new(), &verifier, now).expect_err("no token");
        assert!(matches!(missing, Refusal::Unauthenticated(_)));
        let garbage = authenticate(&headers("Bearer nope"), &verifier, now).expect_err("garbage");
        assert!(matches!(garbage, Refusal::Unauthenticated(_)));
        let basic = authenticate(&headers("Basic abc"), &verifier, now).expect_err("not bearer");
        assert!(matches!(basic, Refusal::Unauthenticated(_)));
        let caller = authenticate(
            &headers(&format!("Bearer {}", token.expose())),
            &verifier,
            now,
        )
        .expect("verified");
        assert_eq!(caller.principal.as_str(), "app");
        assert!(caller.credentials.has_token());
        assert!(
            !format!("{caller:?}").contains(token.expose()),
            "Debug hides the token"
        );
        let later = now.saturating_add(Millis::new(120_000));
        let expired = authenticate(
            &headers(&format!("Bearer {}", token.expose())),
            &verifier,
            later,
        )
        .expect_err("expired");
        assert!(matches!(expired, Refusal::Unauthenticated(_)));
        require_read(&caller, &"notes".parse().expect("name")).expect("may read");
        let denied = require_read(&caller, &"other".parse().expect("name")).expect_err("can't");
        assert_eq!(
            denied,
            Refusal::PermissionDenied("this token can't read database 'other'".to_owned())
        );
    }
}
