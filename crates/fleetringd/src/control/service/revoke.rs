//! `Control.RevokeToken`: who may revoke what (ADR-0047, point 11.3).
//!
//! | Caller | Target | Action |
//! |---|---|---|
//! | `fleet` scope, or anonymous on a plaintext fleet | a token or a key | `RevokeToken`, its `until` as asked, a token's capped at the longest a token lives |
//! | `fleet` scope | a principal | `RevokePrincipal` |
//! | an agent's token, carried directly | a token or a principal | `RevokeAsAncestor`, whose `apply` checks the rule; `until_ms` is ignored |
//! | an agent's token, relay-carried | its own token ID | `RevokeAsAncestor` (point 4.5's one exception) |
//! | an agent's token, relay-carried | anything else | `PERMISSION_DENIED` |
//! | an agent's token | a key | `PERMISSION_DENIED`: keys are `fleet`'s |
//! | a non-agent's token without `fleet` | anything | `PERMISSION_DENIED` |
//! | a certificate without `fleet`, or a node | anything | `PERMISSION_DENIED` at the boundary |
//!
//! A principal, and every revocation under the ancestor rule, needs the `delegation` fleet
//! feature. A request may name a token and a principal at once: an agent removing itself needs
//! both (point 11.7), and its token authenticates no second request once either lands. Under the
//! ancestor rule both go in one `RevokeAsAncestor`, so they land together or not at all, and a
//! token the principal covers (a delegation recorded at or below it) gets no entry of its own.
//! With `fleet`, the principal is revoked first, then the token. `apply`'s refusals of the
//! commands that need `delegation` are answered without logging them (`SubmitError::Refused`).
//!
//! Each request writes one audit line naming the caller, its credential by ID, the targets, the
//! authority (`fleet` or `ancestor`), and the outcome; never a token.

use std::sync::Arc;

use fleetring_control::delegation::{AgentName, AncestorTarget, CallerToken, TokenClaims};
use fleetring_control::{
    Action, Applied, Feature, KeyId, Millis, PrincipalName, Rejection, Revoked, Timestamp,
};
use fleetring_proto::v1::{RevokeTokenRequest, RevokeTokenResponse};
use tonic::{Request, Status};

use super::{AUDIT, ControlService, parse, rejection_status, unexpected};
use crate::auth::{
    Caller, Carriage, Credential, MAX_TOKEN_LIFETIME, NOT_BEFORE_LEEWAY_SECS, Signer, Target,
    authorized,
};
use crate::data::convert::revocation_request_from_proto;

/// Who revokes, and on what authority.
#[derive(Debug)]
pub(super) enum Revoker {
    /// A principal with `fleet`, or anyone on a plaintext fleet: anything, as before 0.8.
    Fleet,
    /// An agent under the ancestor rule, by the token it presented.
    Ancestor(AncestorCaller),
}

/// An agent revoking under the ancestor rule, as the request boundary authenticated it.
#[derive(Debug)]
pub(super) struct AncestorCaller {
    pub(super) claims: Arc<TokenClaims>,
    pub(super) key: KeyId,
    pub(super) signer: Signer,
    pub(super) carriage: Carriage,
}

impl AncestorCaller {
    /// The token it presented, as `apply` checks it.
    fn presented(&self) -> CallerToken {
        CallerToken {
            claims: (*self.claims).clone(),
            kid: self.key.clone(),
        }
    }
}

/// What a request revokes: a token or a key, a principal, or a token and a principal.
#[derive(Debug)]
pub(super) struct Targets {
    revocation: Option<(Revoked, Timestamp)>,
    principal: Option<PrincipalName>,
}

impl Targets {
    /// Reads `request`.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` if it names nothing, names something invalid, or holds a principal's
    /// revocation in `revocation`, whose `before` only the control plane sets.
    pub(super) fn from_request(request: RevokeTokenRequest) -> Result<Self, Status> {
        let revocation = request
            .revocation
            .map(revocation_request_from_proto)
            .transpose()?;
        let principal = if request.principal.is_empty() {
            None
        } else {
            Some(parse("principal", &request.principal)?)
        };
        if revocation.is_none() && principal.is_none() {
            return Err(Status::invalid_argument(
                "missing revocation: name a token, a key, or a principal",
            ));
        }
        Ok(Self {
            revocation,
            principal,
        })
    }

    /// The targets, as an audit line names them.
    fn describe(&self) -> String {
        let principal = self
            .principal
            .as_ref()
            .map(|principal| Revoked::principal(principal.clone()).to_string());
        let revocation = self
            .revocation
            .as_ref()
            .map(|(revoked, _)| revoked.to_string());
        principal
            .into_iter()
            .chain(revocation)
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl Revoker {
    /// The revoker of `request`.
    ///
    /// # Errors
    ///
    /// `PERMISSION_DENIED` for a caller with neither `fleet` nor a token naming an agent.
    pub(super) fn of<T>(request: &Request<T>) -> Result<Self, Status> {
        let (name, credential) = match authorized(request)?.caller() {
            Caller::Anonymous => return Ok(Self::Fleet),
            Caller::Principal { scopes, .. } if scopes.allows(Target::Fleet) => {
                return Ok(Self::Fleet);
            }
            Caller::Principal {
                name, credential, ..
            } => (name, credential),
            Caller::Node(_) => {
                return Err(Status::permission_denied(
                    "RevokeToken needs the fleet scope, or an agent's bearer token for the \
                     ancestor rule",
                ));
            }
        };
        let (claims, key, signer, carriage) = match credential {
            Credential::Token {
                claims,
                key,
                signer,
                carriage,
                ..
            } => (claims, key, signer, *carriage),
            Credential::Certificate => {
                return Err(Status::permission_denied(
                    "RevokeToken needs the fleet scope, or an agent's bearer token for the \
                     ancestor rule",
                ));
            }
        };
        if AgentName::try_from(claims.principal.clone()).is_err() {
            return Err(Status::permission_denied(format!(
                "only an agent revokes without the fleet scope, and '{name}' isn't one"
            )));
        }
        Ok(Self::Ancestor(AncestorCaller {
            claims: Arc::clone(claims),
            key: key.clone(),
            signer: signer.clone(),
            carriage,
        }))
    }

    fn authority(&self) -> &'static str {
        match self {
            Self::Fleet => "fleet",
            Self::Ancestor(_) => "ancestor",
        }
    }
}

/// What an audit line names of the caller: never its token.
pub(super) fn who<T>(request: &Request<T>) -> (String, String) {
    match authorized(request).map(crate::auth::Authorized::caller) {
        Ok(Caller::Principal {
            name, credential, ..
        }) => {
            let credential = match credential {
                Credential::Certificate => "certificate".to_owned(),
                Credential::Token { id, .. } => format!("token:{id}"),
            };
            (name.to_string(), credential)
        }
        Ok(Caller::Node(id)) => (format!("node:{id}"), "certificate".to_owned()),
        Ok(Caller::Anonymous) | Err(_) => ("anonymous".to_owned(), "plaintext".to_owned()),
    }
}

/// Writes a revocation's audit line: the caller, the targets, the authority, and the outcome.
pub(super) fn audit(
    (principal, credential): &(String, String),
    targets: &str,
    authority: Option<&str>,
    result: &Result<RevokeTokenResponse, Status>,
) {
    match result {
        Ok(response) => tracing::info!(
            target: AUDIT,
            principal,
            credential,
            targets,
            authority,
            before = response.before_ms,
            until = response.until_ms,
            result = "revoked",
            rpc = "RevokeToken",
            "revoked",
        ),
        Err(status) => tracing::warn!(
            target: AUDIT,
            principal,
            credential,
            targets,
            authority,
            result = "refused",
            code = ?status.code(),
            reason = status.message(),
            rpc = "RevokeToken",
            "refused a revocation",
        ),
    }
}

impl ControlService {
    /// Revokes `targets` on `revoker`'s authority, in the order and by the rules the module
    /// documents, and answers what `apply` chose.
    pub(super) async fn revoke(
        &self,
        revoker: &Revoker,
        targets: Targets,
    ) -> Result<RevokeTokenResponse, Status> {
        if let Revoker::Ancestor(caller) = revoker {
            check_ancestor_targets(caller, &targets)?;
        }
        if targets.principal.is_some() || matches!(revoker, Revoker::Ancestor(_)) {
            self.check_delegation()?;
        }
        if let Revoker::Ancestor(caller) = revoker
            && let Some(by) = caller.signer.refusal_in(
                self.node.state().revocations(),
                &caller.claims,
                &caller.key,
            )
        {
            return Err(rejection_status(&Rejection::CallerRevoked { by }));
        }

        match revoker {
            Revoker::Ancestor(caller) => self.revoke_as_ancestor(caller, targets).await,
            Revoker::Fleet => self.revoke_with_fleet(targets).await,
        }
    }

    /// Revokes `targets` under the ancestor rule, in one command: a principal and a token land
    /// together or not at all, and `apply` gives a token the principal covers no entry.
    async fn revoke_as_ancestor(
        &self,
        caller: &AncestorCaller,
        targets: Targets,
    ) -> Result<RevokeTokenResponse, Status> {
        let principal = targets.principal.map(ancestor_principal).transpose()?;
        let token = match targets.revocation {
            None => None,
            Some((Revoked::Token(id), _)) => Some(id),
            // `check_ancestor_targets` refused a key, and the request's conversion a principal.
            Some((revoked @ (Revoked::Key(_) | Revoked::Principal { .. }), _)) => {
                return Err(Status::permission_denied(format!(
                    "the ancestor rule can't revoke {revoked}"
                )));
            }
        };
        let target = match (principal, token) {
            (Some(principal), Some(token)) => {
                AncestorTarget::PrincipalAndToken { principal, token }
            }
            (Some(principal), None) => AncestorTarget::Principal(principal),
            (None, Some(token)) => AncestorTarget::Token(token),
            // `Targets::from_request` refused a request that names nothing.
            (None, None) => {
                return Err(Status::invalid_argument(
                    "missing revocation: name a token, a key, or a principal",
                ));
            }
        };
        let action = Action::RevokeAsAncestor {
            target,
            caller: caller.presented(),
        };
        let mut response = RevokeTokenResponse::default();
        match self.submit(action).await? {
            (Applied::TokenRevoked { until, .. }, _) => response.until_ms = until.as_millis(),
            (Applied::PrincipalRevoked { before, until, .. }, _) => {
                response.until_ms = until.as_millis();
                response.before_ms = Some(before.as_millis());
                response.principal_until_ms = Some(until.as_millis());
            }
            (
                Applied::PrincipalAndTokenRevoked {
                    before,
                    until,
                    token_until,
                    ..
                },
                _,
            ) => {
                response.until_ms = token_until.unwrap_or(until).as_millis();
                response.before_ms = Some(before.as_millis());
                response.principal_until_ms = Some(until.as_millis());
            }
            (other, _) => return Err(unexpected(&other)),
        }
        Ok(response)
    }

    /// Revokes `targets` with `fleet`: the principal first, then the token or key.
    async fn revoke_with_fleet(&self, targets: Targets) -> Result<RevokeTokenResponse, Status> {
        let mut response = RevokeTokenResponse::default();
        let mut landed = None;
        if let Some(principal) = targets.principal {
            match self.submit(Action::RevokePrincipal { principal }).await? {
                (
                    Applied::PrincipalRevoked {
                        revoked,
                        before,
                        until,
                    },
                    _,
                ) => {
                    response.until_ms = until.as_millis();
                    response.before_ms = Some(before.as_millis());
                    response.principal_until_ms = Some(until.as_millis());
                    landed = Some(revoked);
                }
                (other, _) => return Err(unexpected(&other)),
            }
        }
        let Some((revoked, until)) = targets.revocation else {
            return Ok(response);
        };
        let until = self.token_entry_until(&revoked, until);
        match self.submit(Action::RevokeToken { revoked, until }).await {
            Ok((Applied::TokenRevoked { until, .. }, _)) => {
                response.until_ms = until.as_millis();
                Ok(response)
            }
            Ok((other, _)) => Err(unexpected(&other)),
            Err(status) => Err(match landed {
                // The principal's revocation stands; say so beside why the token's didn't.
                Some(first) => Status::new(
                    status.code(),
                    format!(
                        "{first} is revoked, but the token isn't: {}",
                        status.message()
                    ),
                ),
                None => status,
            }),
        }
    }

    /// The `until` of a token's entry revoked with `fleet`: as asked, but no later than the
    /// longest any token lasts, since keeping it longer only grows the list. A key's entry may
    /// last forever.
    fn token_entry_until(&self, revoked: &Revoked, until: Timestamp) -> Timestamp {
        match revoked {
            Revoked::Token(_) => until.min(
                self.clock.now().saturating_add(Millis::new(
                    u64::try_from(MAX_TOKEN_LIFETIME.as_millis())
                        .unwrap_or(u64::MAX)
                        .saturating_add(NOT_BEFORE_LEEWAY_SECS * 1_000),
                )),
            ),
            // The request's conversion refuses a principal, whose revocation the control plane
            // times itself; `apply` refuses one in `RevokeToken` too.
            Revoked::Key(_) | Revoked::Principal { .. } => until,
        }
    }

    /// Whether the fleet may revoke a principal or under the ancestor rule: both need the
    /// `delegation` fleet feature, which a member lacking it refuses, naming itself.
    fn check_delegation(&self) -> Result<(), Status> {
        self.node
            .state()
            .check_feature(Feature::Delegation)
            .map_err(|rejection| rejection_status(&rejection))
    }
}

/// Checks what the ancestor rule lets no caller ask for, before anything is submitted: a key,
/// which only `fleet` revokes, and, for a relay-carried token, anything but its own token ID
/// (ADR-0047, points 4.5 and 11.3). The rest is `apply`'s.
fn check_ancestor_targets(caller: &AncestorCaller, targets: &Targets) -> Result<(), Status> {
    if let Some((Revoked::Key(_), _)) = &targets.revocation {
        return Err(Status::permission_denied(
            "the ancestor rule revokes no key; revoking a key needs the fleet scope",
        ));
    }
    match caller.carriage {
        Carriage::Direct => Ok(()),
        Carriage::Relayed => {
            let own = matches!(
                (&targets.revocation, &targets.principal),
                (Some((Revoked::Token(id), _)), None) if *id == caller.claims.id
            );
            if own {
                Ok(())
            } else {
                Err(Status::permission_denied(
                    "the ancestor rule needs a direct connection with the certificate of the \
                     caller or an ancestor; a relay-carried token may revoke only its own token id",
                ))
            }
        }
    }
}

/// A principal target of the ancestor rule, which only an agent name can be.
fn ancestor_principal(principal: PrincipalName) -> Result<AgentName, Status> {
    AgentName::try_from(principal.clone()).map_err(|_| {
        Status::permission_denied(format!(
            "the ancestor rule revokes only agent names at or below the caller, and \
             '{principal}' isn't an agent name"
        ))
    })
}

/// The audit line's targets and authority for a request, read before it is consumed.
pub(super) fn described(
    revoker: Option<&Revoker>,
    targets: Option<&Targets>,
) -> (String, Option<&'static str>) {
    (
        targets.map_or_else(String::new, Targets::describe),
        revoker.map(Revoker::authority),
    )
}
