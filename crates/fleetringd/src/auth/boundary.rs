//! The request boundary: who is calling, and whether they may (ADR-0025, ADR-0026).
//!
//! [`AuthLayer`] wraps every service a daemon serves. For each request it finds the RPC in
//! [`RPCS`](crate::auth::RPCS) (a path the table doesn't list is refused), resolves the
//! [`Caller`] from the connection's certificate and any `authorization: Bearer` token, and
//! enforces the requirements that need no request body: node-only calls, `fleet`, and the
//! floor for listings. It passes the decision on in an [`Authorized`] extension, from which each
//! handler checks the database its request names before it touches any state, and with which a
//! streaming handler ends its stream once the token behind it expires or is revoked.
//!
//! Refusals are `PERMISSION_DENIED`; a token that doesn't verify is `UNAUTHENTICATED`; and a
//! token a data node can't check, because its denylist hasn't arrived or has gone stale, is
//! `UNAVAILABLE`, which a client retries. Each refusal is logged once on the `fleetringd::audit`
//! target with the principal, the credential, the action, the database, and the RPC; allowed requests
//! are logged at debug. Credentials are never logged.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use fleetring_control::{DatabaseName, KeyId, Revocations, Timestamp, TokenId};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt as _};
use tonic::codegen::http;
use tonic::{Request, Status};
use tower::{Layer, Service};

use crate::auth::{Action, BearerToken, Caller, Credential, Denied, Denylist, Requirement, Rpc};
use crate::clock::{Clock, SystemClock};
use crate::config::{PrincipalConfig, TokenForwarding};
use crate::control::ControlHandle;
use crate::tls::{Identity, Peer, Principal, Security};

/// The metadata key a bearer token travels in.
pub const AUTHORIZATION: &str = "authorization";

/// The scheme before the token in [`AUTHORIZATION`].
const BEARER: &str = "Bearer ";

/// The audit log's target.
const AUDIT: &str = "fleetringd::audit";

/// How often a stream opened with a token checks, between items, that the token still holds.
pub const STREAM_RECHECK: Duration = Duration::from_secs(1);

/// How many items a guarded stream holds for a subscriber that hasn't read them yet.
const STREAM_BUFFER: usize = 16;

/// A response stream, as the services return them.
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Where the denylist comes from.
#[derive(Clone)]
enum Source {
    /// A data node's copy, refreshed by its joins and heartbeats, trusted only while it is at
    /// most `max_age` old.
    Denylist { list: Denylist, max_age: Duration },
    /// The control node's own state, always current.
    Control(ControlHandle),
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Denylist { list, max_age } => f
                .debug_struct("Denylist")
                .field("list", list)
                .field("max_age", max_age)
                .finish(),
            Self::Control(_) => f.write_str("Control"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    scopes: crate::auth::Scopes,
    forward_tokens: TokenForwarding,
}

/// Decides who a request comes from: the principals and their scopes, the signing keys
/// tokens are verified with, the denylist, and the clock token lifetimes are judged by.
///
/// Cloning is cheap; clones share everything.
#[derive(Debug, Clone)]
pub struct Authorizer {
    inner: Arc<Inner>,
}

#[derive(Debug, Clone)]
struct Inner {
    principals: BTreeMap<Principal, Entry>,
    /// The identity whose trusted authorities verify tokens; `None` in plaintext, where a
    /// token can't be verified and is refused.
    identity: Option<Identity>,
    revocations: Source,
    clock: Arc<dyn Clock>,
}

impl Authorizer {
    /// An authorizer for `principals` on connections secured as `security`, judging token
    /// lifetimes by `clock`. Until a denylist is attached, every token is refused as
    /// unavailable: a node that can't check revocations doesn't trust tokens.
    #[must_use]
    pub fn new(principals: &[PrincipalConfig], security: &Security, clock: Arc<dyn Clock>) -> Self {
        let principals = principals
            .iter()
            .map(|principal| {
                (
                    principal.name.clone(),
                    Entry {
                        scopes: principal.scopes.clone(),
                        forward_tokens: principal.forward_tokens,
                    },
                )
            })
            .collect();
        let identity = match security {
            Security::Tls(identity) => Some(identity.clone()),
            Security::Plaintext => None,
        };
        Self {
            inner: Arc::new(Inner {
                principals,
                identity,
                revocations: Source::Denylist {
                    list: Denylist::new(),
                    max_age: Duration::ZERO,
                },
                clock,
            }),
        }
    }

    /// An authorizer for plaintext connections with no principals: every request without a
    /// token passes, as before authorization existed. For tests that serve a service on its own.
    #[must_use]
    pub fn plaintext() -> Self {
        Self::new(&[], &Security::Plaintext, Arc::new(SystemClock))
    }

    /// Checks tokens against `denylist`, as a data node's joins and heartbeats keep it, and
    /// refuses every token while the list is missing or more than `max_age` old.
    #[must_use]
    pub fn with_denylist(self, denylist: Denylist, max_age: Duration) -> Self {
        self.with_source(Source::Denylist {
            list: denylist,
            max_age,
        })
    }

    /// Checks tokens against the control node's own state.
    #[must_use]
    pub fn with_control(self, control: ControlHandle) -> Self {
        self.with_source(Source::Control(control))
    }

    fn with_source(self, revocations: Source) -> Self {
        let inner = Arc::unwrap_or_clone(self.inner);
        Self {
            inner: Arc::new(Inner {
                revocations,
                ..inner
            }),
        }
    }

    /// Runs `check` on the current denylist, or answers `UNAVAILABLE` if a data node's copy is
    /// missing or stale.
    fn with_revocations<T>(&self, check: impl FnOnce(&Revocations) -> T) -> Result<T, Status> {
        match &self.inner.revocations {
            Source::Control(control) => Ok(check(control.state().revocations())),
            Source::Denylist { list, max_age } => match list.age() {
                None => Err(Status::unavailable(
                    "this node hasn't received the token denylist yet; retry",
                )),
                Some(age) if age > *max_age => Err(Status::unavailable(
                    "this node's token denylist is stale; retry",
                )),
                Some(_) => Ok(check(&list.load())),
            },
        }
    }

    /// Resolves who sent a request: the node or principal its connection authenticated as, or
    /// the principal its `token` names.
    ///
    /// # Errors
    ///
    /// `UNAUTHENTICATED` for a token that doesn't verify, or one on a plaintext connection;
    /// `UNAVAILABLE` for a token while this node's denylist is missing or stale;
    /// `PERMISSION_DENIED` for a token on a node's connection, or one naming another principal
    /// than the client certificate unless that principal forwards tokens.
    pub fn resolve(&self, peer: &Peer, token: Option<&BearerToken>) -> Result<Caller, Status> {
        self.resolve_identified(peer, token)
            .map_err(|refused| refused.status)
    }

    /// Resolves as [`resolve`](Self::resolve) does, and names the token in a refusal when it
    /// verified far enough to have an ID, for the audit line.
    fn resolve_identified(
        &self,
        peer: &Peer,
        token: Option<&BearerToken>,
    ) -> Result<Caller, Refused> {
        let Some(token) = token else {
            return Ok(match peer {
                Peer::Node(id) => Caller::Node(id.clone()),
                Peer::Principal(name) => Caller::Principal {
                    name: name.clone(),
                    scopes: self
                        .inner
                        .principals
                        .get(name)
                        .map(|entry| entry.scopes.clone())
                        .unwrap_or_default(),
                    credential: Credential::Certificate,
                },
                _ => Caller::Anonymous,
            });
        };
        let certificate = match peer {
            Peer::Node(_) => {
                return Err(Status::permission_denied(
                    "a node's connection carries no bearer token",
                )
                .into());
            }
            Peer::Principal(name) => Some(name),
            _ => None,
        };
        let Some(identity) = &self.inner.identity else {
            return Err(Status::unauthenticated(
                "bearer tokens need a TLS connection to verify against",
            )
            .into());
        };
        let verifier = identity.token_verifier();
        let now = self.inner.clock.now();
        let verified = self
            .with_revocations(|revoked| verifier.verify_identified(token, now, revoked))?
            .map_err(|refusal| Refused {
                status: Status::unauthenticated(refusal.clone().into_error().to_string()),
                token: refusal.token().cloned(),
            })?;
        if let Some(name) = certificate
            && *name != verified.grant.principal
        {
            let forwards = self
                .inner
                .principals
                .get(name)
                .is_some_and(|entry| entry.forward_tokens == TokenForwarding::Allowed);
            if !forwards {
                return Err(Refused {
                    status: Status::permission_denied(
                        "the token names another principal than the client certificate",
                    ),
                    token: Some(verified.grant.id),
                });
            }
        }
        Ok(Caller::Principal {
            name: verified.grant.principal,
            scopes: verified.grant.scopes,
            credential: Credential::Token {
                id: verified.grant.id,
                key: verified.key,
                expires_at: verified.grant.expires_at,
            },
        })
    }

    /// Whether the token `id`, signed by `key` and expiring at `expires_at`, still holds now.
    fn token_holds(&self, id: &TokenId, key: &KeyId, expires_at: Timestamp) -> Result<(), Status> {
        if self.inner.clock.now() >= expires_at {
            return Err(Status::unauthenticated("the token has expired"));
        }
        if self.with_revocations(|revoked| revoked.refuses(id, key))? {
            return Err(Status::unauthenticated("the token is revoked"));
        }
        Ok(())
    }

    /// Resolves and checks a request at the boundary, and returns what handlers need.
    fn admit<B>(&self, request: &http::Request<B>) -> Result<Authorized, Status> {
        let path = request.uri().path();
        let Some(rpc) = crate::auth::rpc_at(path) else {
            tracing::warn!(target: AUDIT, path, "refused a call to an rpc without a scope requirement");
            return Err(Status::permission_denied(format!(
                "{path} has no scope requirement"
            )));
        };
        let peer = match request.extensions().get::<Peer>() {
            Some(peer) => peer.clone(),
            None if self.inner.identity.is_some() => {
                // Every connection to a TLS daemon is authenticated; a request without the
                // connection's identity means the transport is wired wrong. Fail closed.
                tracing::error!(target: AUDIT, rpc = rpc.method(), "refused a request whose connection carries no identity");
                return Err(Status::unauthenticated(
                    "the connection carries no identity",
                ));
            }
            None => Peer::Plaintext,
        };
        let resolved = match bearer_token(request.headers()) {
            Ok(token) => self.resolve_identified(&peer, token.as_ref()),
            Err(status) => Err(status.into()),
        };
        let caller = match resolved {
            Ok(caller) => caller,
            Err(Refused { status, token }) => {
                let principal = match &peer {
                    Peer::Node(id) => format!("node:{id}"),
                    Peer::Principal(name) => name.to_string(),
                    _ => "anonymous".to_owned(),
                };
                // The ID is there whenever the token verified far enough to have one.
                let credential =
                    token.map_or_else(|| "token".to_owned(), |id| format!("token:{id}"));
                tracing::warn!(
                    target: AUDIT,
                    principal,
                    credential,
                    action = action_of(rpc),
                    database = None::<&str>,
                    rpc = rpc.method(),
                    code = ?status.code(),
                    reason = status.message(),
                    "refused a token",
                );
                return Err(status);
            }
        };
        let span = tracing::Span::current();
        match &caller {
            Caller::Node(id) => {
                span.record("node", id.as_str());
            }
            Caller::Principal { name, .. } => {
                span.record("principal", name.as_str());
            }
            _ => {}
        }
        let authorized = Authorized {
            caller,
            rpc,
            authorizer: self.clone(),
        };
        match rpc.requirement {
            // The database is in the body; the handler checks it.
            Requirement::Database(_) | Requirement::DatabaseOrFleet(_) => Ok(authorized),
            _ => {
                authorized.check(None)?;
                Ok(authorized)
            }
        }
    }
}

/// Why a request's credentials were refused, and which token it carried if the token verified far
/// enough to have an ID. The ID is for the audit line; the client only sees the status.
struct Refused {
    status: Status,
    token: Option<TokenId>,
}

impl From<Status> for Refused {
    fn from(status: Status) -> Self {
        Self {
            status,
            token: None,
        }
    }
}

/// The token in the `authorization` header, if there is one.
fn bearer_token(headers: &http::HeaderMap) -> Result<Option<BearerToken>, Status> {
    let Some(value) = headers.get(AUTHORIZATION) else {
        return Ok(None);
    };
    let text = value
        .to_str()
        .map_err(|_| Status::unauthenticated("the authorization header isn't text"))?;
    let token = text
        .strip_prefix(BEARER)
        .or_else(|| text.strip_prefix("bearer "))
        .ok_or_else(|| Status::unauthenticated("the authorization header isn't a bearer token"))?;
    Ok(Some(BearerToken::new(token.trim())))
}

/// The boundary's decision about a request: who sent it, and which RPC it is. Handlers read it
/// from the request's extensions to check the database the request names, and to guard the streams
/// they return.
#[derive(Debug, Clone)]
pub struct Authorized {
    caller: Caller,
    rpc: &'static Rpc,
    authorizer: Authorizer,
}

impl Authorized {
    /// Returns who sent the request.
    #[must_use]
    pub fn caller(&self) -> &Caller {
        &self.caller
    }

    /// Returns the RPC.
    #[must_use]
    pub fn rpc(&self) -> &'static Rpc {
        self.rpc
    }

    /// Checks the request against its RPC's requirement, with `database` the one it names, and
    /// logs the decision.
    fn check(&self, database: Option<&DatabaseName>) -> Result<(), Status> {
        if let Some(database) = database {
            tracing::Span::current().record("database", database.as_str());
        }
        match self.caller.authorize(self.rpc, database) {
            Ok(()) => {
                audit_allowed(&self.caller, self.rpc, database);
                Ok(())
            }
            Err(denied) => {
                audit_denied(&self.caller, self.rpc, &denied);
                Err(denied.to_status())
            }
        }
    }

    /// Whether the caller may see `database` in a listing filtered by `action`.
    #[must_use]
    pub fn sees(&self, action: Action, database: &DatabaseName) -> bool {
        self.caller.sees(action, database)
    }

    /// Ends `stream` with `UNAUTHENTICATED` once the token the request carried expires or is
    /// revoked, and with `UNAVAILABLE` if this node's denylist goes stale meanwhile. The token
    /// is checked before each item and every [`STREAM_RECHECK`] between items. A stream opened
    /// without a token is returned as it is: certificates aren't revoked one by one (there is no
    /// revocation list), so it lasts as long as its connection, and a certificate stops being
    /// accepted only when it expires or the fleet stops trusting the authority that issued it.
    pub fn guard<T: Send + 'static>(&self, stream: BoxStream<T>) -> BoxStream<T> {
        let Caller::Principal {
            credential:
                Credential::Token {
                    id,
                    key,
                    expires_at,
                },
            ..
        } = &self.caller
        else {
            return stream;
        };
        let (id, key, expires_at) = (id.clone(), key.clone(), *expires_at);
        let authorizer = self.authorizer.clone();
        let rpc = self.rpc.method();
        let (sender, receiver) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(async move {
            let mut stream = stream;
            let mut recheck = tokio::time::interval(STREAM_RECHECK);
            recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let item = tokio::select! {
                    biased;
                    () = sender.closed() => return,
                    _ = recheck.tick() => None,
                    item = stream.next() => match item {
                        Some(item) => Some(item),
                        None => return,
                    },
                };
                if let Err(status) = authorizer.token_holds(&id, &key, expires_at) {
                    tracing::info!(target: AUDIT, token_id = %id, rpc, reason = status.message(), "ended a stream");
                    // Either the subscriber reads the refusal or it has gone; the stream ends.
                    let _ = sender.send(Err(status)).await;
                    return;
                }
                if let Some(item) = item
                    && sender.send(item).await.is_err()
                {
                    return;
                }
            }
        });
        Box::pin(ReceiverStream::new(receiver))
    }
}

/// The principal and credential an audit line names: never the credential itself.
fn who(caller: &Caller) -> (String, String) {
    match caller {
        Caller::Node(id) => (format!("node:{id}"), "certificate".to_owned()),
        Caller::Principal {
            name, credential, ..
        } => {
            let credential = match credential {
                Credential::Certificate => "certificate".to_owned(),
                Credential::Token { id, .. } => format!("token:{id}"),
            };
            (name.to_string(), credential)
        }
        Caller::Anonymous => ("anonymous".to_owned(), "plaintext".to_owned()),
    }
}

/// The action an RPC's requirement names, for audit lines.
fn action_of(rpc: &Rpc) -> String {
    match rpc.requirement {
        Requirement::Node => "node".to_owned(),
        Requirement::Fleet => "fleet".to_owned(),
        Requirement::Database(action) | Requirement::DatabaseOrFleet(action) => {
            action.as_str().to_owned()
        }
        Requirement::Listing(actions) => actions
            .iter()
            .map(|action| action.as_str())
            .collect::<Vec<_>>()
            .join("|"),
    }
}

fn audit_denied(caller: &Caller, rpc: &Rpc, denied: &Denied) {
    let (principal, credential) = who(caller);
    tracing::warn!(
        target: AUDIT,
        principal,
        credential,
        action = action_of(rpc),
        database = denied.database().map(DatabaseName::as_str),
        rpc = rpc.method(),
        "denied",
    );
}

fn audit_allowed(caller: &Caller, rpc: &Rpc, database: Option<&DatabaseName>) {
    if !tracing::enabled!(target: AUDIT, tracing::Level::DEBUG) {
        return;
    }
    let (principal, credential) = who(caller);
    tracing::debug!(
        target: AUDIT,
        principal,
        credential,
        action = action_of(rpc),
        database = database.map(DatabaseName::as_str),
        rpc = rpc.method(),
        "allowed",
    );
}

/// Why the boundary refused a request, when its status code alone doesn't say: the trace layer
/// reads it from the response's extensions to count the refusal by reason (ADR-0034).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The path names no RPC in [`RPCS`](crate::auth::RPCS).
    NoRequirement,
    /// The request carried a token this node couldn't check: its denylist is missing or stale.
    DenylistStale,
}

impl Refusal {
    /// The `reason` label of `fleetring_auth_refusals_total`.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::NoRequirement => "no_requirement",
            Self::DenylistStale => "denylist_stale",
        }
    }
}

/// The boundary's decision for `request`.
///
/// # Errors
///
/// `PERMISSION_DENIED` if the request didn't pass through an [`AuthLayer`], so nothing
/// authorized it.
pub fn authorized<T>(request: &Request<T>) -> Result<&Authorized, Status> {
    request.extensions().get::<Authorized>().ok_or_else(|| {
        tracing::error!(target: AUDIT, "a request reached a handler without passing the boundary");
        Status::permission_denied("the request wasn't authorized")
    })
}

/// Checks that `request`'s caller may take its RPC's action on `database`, the one the request
/// names, and logs the decision.
///
/// # Errors
///
/// `PERMISSION_DENIED` naming the action and the database, never the caller's scopes.
pub fn require_database<T>(request: &Request<T>, database: &DatabaseName) -> Result<(), Status> {
    authorized(request)?.check(Some(database))
}

/// A tower layer that puts [`Authorizer`]'s checks in front of every service it wraps.
#[derive(Debug, Clone)]
pub struct AuthLayer {
    authorizer: Authorizer,
}

impl AuthLayer {
    /// Checks requests with `authorizer`.
    #[must_use]
    pub fn new(authorizer: Authorizer) -> Self {
        Self { authorizer }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            authorizer: self.authorizer.clone(),
        }
    }
}

/// A service behind the request boundary; see [`AuthLayer`].
#[derive(Debug, Clone)]
pub struct AuthService<S> {
    inner: S,
    authorizer: Authorizer,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

impl<S, B> Service<http::Request<B>> for AuthService<S>
where
    S: Service<http::Request<B>, Response = http::Response<tonic::body::Body>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        match self.authorizer.admit(&request) {
            Ok(authorized) => {
                request.extensions_mut().insert(authorized);
                Box::pin(self.inner.call(request))
            }
            Err(status) => {
                // `admit` answers `UNAVAILABLE` only for a token it can't check for want of a
                // fresh denylist.
                let refusal = if crate::auth::rpc_at(request.uri().path()).is_none() {
                    Some(Refusal::NoRequirement)
                } else if status.code() == tonic::Code::Unavailable {
                    Some(Refusal::DenylistStale)
                } else {
                    None
                };
                let mut response = status.into_http();
                if let Some(refusal) = refusal {
                    response.extensions_mut().insert(refusal);
                }
                Box::pin(std::future::ready(Ok(response)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use fleetring_control::Revoked;

    use super::*;
    use crate::auth::{Grant, Scopes, TokenSigner};
    use crate::clock::SimClock;
    use crate::tls::{Authority, Validity};

    /// The denylist bound the tests run with.
    const MAX_AGE: Duration = Duration::from_secs(3);

    struct Fixture {
        identity: Identity,
        signer: TokenSigner,
        signer_authority: Authority,
    }

    fn fixture() -> Fixture {
        let validity = Validity::new(
            Timestamp::ZERO,
            std::time::Duration::from_secs(100 * 365 * 24 * 3600),
        );
        let authority =
            Authority::create("test".parse().expect("a fleet"), validity).expect("an authority");
        let issued = authority
            .issue_node(
                &"n1".parse().expect("a node id"),
                &["127.0.0.1".to_owned()],
                validity,
            )
            .expect("a node certificate");
        let identity =
            Identity::from_issued(authority.certificate_pem(), &issued).expect("an identity");
        let signer = authority.token_signer().expect("a signer");
        Fixture {
            identity,
            signer,
            signer_authority: authority,
        }
    }

    fn principals(entries: &[(&str, &[&str], TokenForwarding)]) -> Vec<PrincipalConfig> {
        entries
            .iter()
            .map(|(name, scopes, forward_tokens)| PrincipalConfig {
                name: name.parse().expect("a principal"),
                scopes: scopes
                    .iter()
                    .map(|scope| scope.parse().expect("a scope"))
                    .collect(),
                forward_tokens: *forward_tokens,
            })
            .collect()
    }

    /// A token valid from 0 for an hour.
    fn token(signer: &TokenSigner, principal: &str, scopes: &[&str], id: &str) -> BearerToken {
        token_until(signer, principal, scopes, id, 3_600_000)
    }

    fn token_until(
        signer: &TokenSigner,
        principal: &str,
        scopes: &[&str],
        id: &str,
        expires_ms: u64,
    ) -> BearerToken {
        signer
            .issue(&Grant {
                principal: principal.parse().expect("a principal"),
                scopes: scopes
                    .iter()
                    .map(|scope| scope.parse().expect("a scope"))
                    .collect::<Scopes>(),
                id: id.parse().expect("a token id"),
                not_before: Timestamp::ZERO,
                expires_at: Timestamp::from_millis(expires_ms),
            })
            .expect("a token")
    }

    /// A fresh, empty denylist.
    fn fresh() -> Denylist {
        let list = Denylist::new();
        list.replace(Revocations::new());
        list
    }

    fn authorizer_at(
        fixture: &Fixture,
        entries: &[(&str, &[&str], TokenForwarding)],
        clock: SimClock,
        denylist: Denylist,
    ) -> Authorizer {
        Authorizer::new(
            &principals(entries),
            &Security::Tls(fixture.identity.clone()),
            Arc::new(clock),
        )
        .with_denylist(denylist, MAX_AGE)
    }

    fn authorizer(fixture: &Fixture, entries: &[(&str, &[&str], TokenForwarding)]) -> Authorizer {
        authorizer_at(
            fixture,
            entries,
            SimClock::new(Timestamp::from_millis(1_000)),
            fresh(),
        )
    }

    fn principal(name: &str) -> Peer {
        Peer::Principal(name.parse().expect("a principal"))
    }

    fn scopes_of(caller: &Caller) -> Vec<String> {
        match caller {
            Caller::Principal { scopes, .. } => {
                scopes.as_slice().iter().map(ToString::to_string).collect()
            }
            other => panic!("expected a principal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_certificate_takes_its_scopes_from_the_configuration() {
        let fixture = fixture();
        let auth = authorizer(
            &fixture,
            &[("app", &["read:tenant:*"], TokenForwarding::Refused)],
        );
        let app = auth.resolve(&principal("app"), None).expect("resolves");
        assert_eq!(scopes_of(&app), ["read:tenant:*"]);
        let stranger = auth.resolve(&principal("other"), None).expect("resolves");
        assert!(scopes_of(&stranger).is_empty());
        let node = auth
            .resolve(&Peer::Node("n2".parse().expect("id")), None)
            .expect("resolves");
        assert_eq!(node, Caller::Node("n2".parse().expect("id")));
    }

    #[tokio::test]
    async fn a_token_acts_with_its_own_scopes() {
        let fixture = fixture();
        let auth = authorizer(&fixture, &[("app", &["admin:*"], TokenForwarding::Refused)]);
        let bearer = token(&fixture.signer, "app", &["read:tenant:acme/*"], "t1");
        let caller = auth
            .resolve(&principal("app"), Some(&bearer))
            .expect("resolves");
        assert_eq!(scopes_of(&caller), ["read:tenant:acme/*"]);
        assert!(matches!(
            caller,
            Caller::Principal {
                credential: Credential::Token { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_token_for_another_principal_needs_a_forwarding_certificate() {
        let fixture = fixture();
        let auth = authorizer(
            &fixture,
            &[
                ("app", &[], TokenForwarding::Refused),
                ("router", &[], TokenForwarding::Allowed),
            ],
        );
        let alice = token(&fixture.signer, "alice", &["read:tenant:alice/*"], "t1");
        let refused = auth
            .resolve(&principal("app"), Some(&alice))
            .expect_err("app can't carry alice's token");
        assert_eq!(refused.code(), tonic::Code::PermissionDenied);
        let forwarded = auth
            .resolve(&principal("router"), Some(&alice))
            .expect("the router forwards it");
        assert!(matches!(&forwarded, Caller::Principal { name, .. } if name.as_str() == "alice"));
        assert_eq!(scopes_of(&forwarded), ["read:tenant:alice/*"]);
        let node = auth
            .resolve(&Peer::Node("n1".parse().expect("id")), Some(&alice))
            .expect_err("a node carries no token");
        assert_eq!(node.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn expired_tampered_and_revoked_tokens_are_unauthenticated() {
        let fixture = fixture();
        let denylist = fresh();
        let auth = authorizer_at(
            &fixture,
            &[],
            SimClock::new(Timestamp::from_millis(1_000)),
            denylist.clone(),
        );
        let bearer = token(&fixture.signer, "app", &["read:x"], "t1");
        assert!(auth.resolve(&principal("app"), Some(&bearer)).is_ok());

        let tampered = BearerToken::new(format!("{}x", bearer.expose()));
        let error = auth
            .resolve(&principal("app"), Some(&tampered))
            .expect_err("tampered");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);

        denylist.replace(
            [(Revoked::Token("t1".parse().expect("id")), Timestamp::MAX)]
                .into_iter()
                .collect(),
        );
        let error = auth
            .resolve(&principal("app"), Some(&bearer))
            .expect_err("revoked");
        assert_eq!(error.message(), "the token is revoked");

        let late = authorizer_at(
            &fixture,
            &[],
            SimClock::new(Timestamp::from_millis(3_600_000)),
            fresh(),
        );
        let error = late
            .resolve(&principal("app"), Some(&bearer))
            .expect_err("expired");
        assert_eq!(error.message(), "the token has expired");

        let other = Authority::create(
            "test".parse().expect("a fleet"),
            Validity::new(Timestamp::ZERO, std::time::Duration::from_secs(3600)),
        )
        .expect("another authority");
        let foreign = token(
            &other.token_signer().expect("a signer"),
            "app",
            &["admin:*"],
            "t2",
        );
        let error = auth
            .resolve(&principal("app"), Some(&foreign))
            .expect_err("untrusted key");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert!(fixture.signer_authority.token_signer().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn tokens_wait_for_a_fresh_denylist_and_certificates_never_do() {
        let fixture = fixture();
        let denylist = Denylist::new();
        let auth = authorizer_at(
            &fixture,
            &[("app", &["read:x"], TokenForwarding::Refused)],
            SimClock::new(Timestamp::from_millis(1_000)),
            denylist.clone(),
        );
        let bearer = token(&fixture.signer, "app", &["read:x"], "t1");

        // A node that has started but not joined has no list: tokens wait, certificates don't.
        let waiting = auth
            .resolve(&principal("app"), Some(&bearer))
            .expect_err("no list yet");
        assert_eq!(waiting.code(), tonic::Code::Unavailable, "{waiting}");
        assert!(auth.resolve(&principal("app"), None).is_ok());

        denylist.replace(Revocations::new());
        assert!(auth.resolve(&principal("app"), Some(&bearer)).is_ok());

        // Cut off from the control plane, the list ages out, and tokens wait again.
        tokio::time::advance(MAX_AGE + Duration::from_millis(1)).await;
        let stale = auth
            .resolve(&principal("app"), Some(&bearer))
            .expect_err("stale");
        assert_eq!(stale.code(), tonic::Code::Unavailable, "{stale}");
        assert_eq!(
            stale.message(),
            "this node's token denylist is stale; retry"
        );
        assert!(auth.resolve(&principal("app"), None).is_ok());

        denylist.replace(Revocations::new());
        assert!(auth.resolve(&principal("app"), Some(&bearer)).is_ok());
    }

    #[tokio::test]
    async fn tokens_on_plaintext_are_refused_and_plaintext_without_one_is_anonymous() {
        let auth = Authorizer::plaintext();
        assert_eq!(
            auth.resolve(&Peer::Plaintext, None).ok(),
            Some(Caller::Anonymous)
        );
        let error = auth
            .resolve(&Peer::Plaintext, Some(&BearerToken::new("x.y.z")))
            .expect_err("no verifier");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn the_bearer_header_is_parsed_and_anything_else_refused() {
        let mut headers = http::HeaderMap::new();
        assert_eq!(
            bearer_token(&headers).ok().map(|t| t.is_some()),
            Some(false)
        );
        headers.insert(AUTHORIZATION, "Bearer a.b.c".parse().expect("a header"));
        assert_eq!(
            bearer_token(&headers)
                .ok()
                .map(|t| t.map(|t| t.expose().to_owned())),
            Some(Some("a.b.c".to_owned()))
        );
        headers.insert(AUTHORIZATION, "Basic xyz".parse().expect("a header"));
        assert!(bearer_token(&headers).is_err());
    }

    #[test]
    fn an_unlisted_path_is_refused_at_the_boundary() {
        let auth = Authorizer::plaintext();
        let request = http::Request::builder()
            .uri("/fleetring.v1.Database/Nope")
            .body(())
            .expect("a request");
        let error = auth.admit(&request).expect_err("refused");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        let request = http::Request::builder()
            .uri("/fleetring.v1.Database/Query")
            .body(())
            .expect("a request");
        assert!(auth.admit(&request).is_ok());
    }

    #[tokio::test]
    async fn a_tls_daemon_refuses_a_request_without_the_connections_identity() {
        let fixture = fixture();
        let auth = authorizer(&fixture, &[]);
        let request = http::Request::builder()
            .uri("/fleetring.v1.Database/Query")
            .body(())
            .expect("a request");
        let error = auth.admit(&request).expect_err("no peer");
        assert_eq!(error.code(), tonic::Code::Unauthenticated, "{error}");
        let mut request = http::Request::builder()
            .uri("/fleetring.v1.Database/Query")
            .body(())
            .expect("a request");
        request.extensions_mut().insert(principal("app"));
        assert!(auth.admit(&request).is_ok());
    }

    /// The decision for a `WatchLeases` request carrying `bearer`.
    fn watch(auth: &Authorizer, bearer: &BearerToken) -> Authorized {
        let mut request = http::Request::builder()
            .uri("/fleetring.v1.Database/WatchLeases")
            .header(AUTHORIZATION, format!("Bearer {}", bearer.expose()))
            .body(())
            .expect("a request");
        request.extensions_mut().insert(principal("app"));
        auth.admit(&request).expect("admitted")
    }

    /// A stream that never yields.
    fn silent() -> BoxStream<u32> {
        Box::pin(tokio_stream::pending())
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_ends_when_its_token_expires() {
        let fixture = fixture();
        let clock = SimClock::new(Timestamp::from_millis(1_000));
        let auth = authorizer_at(&fixture, &[], clock, fresh());
        let bearer = token_until(&fixture.signer, "app", &["read:x"], "t1", 4_000);
        let mut stream = watch(&auth, &bearer).guard(silent());
        // Keep the list fresh while time passes; only the token's expiry ends the stream.
        let refresher = {
            let denylist = auth.clone();
            tokio::spawn(async move {
                loop {
                    if let Source::Denylist { list, .. } = &denylist.inner.revocations {
                        list.replace(Revocations::new());
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            })
        };
        let ended = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("the stream ends")
            .expect("with an item");
        let status = ended.expect_err("a refusal");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "the token has expired");
        assert!(stream.next().await.is_none());
        refresher.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_ends_when_its_token_is_revoked_and_passes_items_until_then() {
        let fixture = fixture();
        let denylist = fresh();
        let auth = authorizer_at(
            &fixture,
            &[],
            SimClock::new(Timestamp::from_millis(1_000)),
            denylist.clone(),
        );
        let bearer = token(&fixture.signer, "app", &["read:x"], "t1");
        let (items, receiver) = mpsc::channel(4);
        let mut stream = watch(&auth, &bearer).guard(Box::pin(ReceiverStream::new(receiver)));
        items.send(Ok(7_u32)).await.expect("sent");
        assert_eq!(
            stream.next().await.map(|item| item.ok()),
            Some(Some(7)),
            "items pass while the token holds"
        );
        denylist.replace(
            [(
                Revoked::Key(fixture.signer.key_id().clone()),
                Timestamp::MAX,
            )]
            .into_iter()
            .collect(),
        );
        let ended = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("the stream ends")
            .expect("with an item");
        assert_eq!(
            ended.expect_err("a refusal").message(),
            "the token is revoked"
        );

        // A stream opened with a certificate is left alone.
        let mut request = http::Request::builder()
            .uri("/fleetring.v1.Database/WatchLeases")
            .body(())
            .expect("a request");
        request.extensions_mut().insert(principal("app"));
        let cert = Authorizer::new(
            &principals(&[("app", &["read:x"], TokenForwarding::Refused)]),
            &Security::Tls(fixture.identity.clone()),
            Arc::new(SystemClock),
        )
        .admit(&request)
        .expect("admitted");
        let (items, receiver) = mpsc::channel(4);
        let mut stream = cert.guard(Box::pin(ReceiverStream::new(receiver)));
        items.send(Ok(9_u32)).await.expect("sent");
        assert_eq!(stream.next().await.map(|item| item.ok()), Some(Some(9)));
    }
    /// Collects the audit lines a test logs.
    #[derive(Clone, Default)]
    struct AuditLog(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for AuditLog {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("the log isn't poisoned")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for AuditLog {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl AuditLog {
        fn lines(&self) -> Vec<String> {
            let bytes = self.0.lock().expect("the log isn't poisoned").clone();
            String::from_utf8_lossy(&bytes)
                .lines()
                .map(str::to_owned)
                .collect()
        }
    }

    /// Admits a call to `Join` as `app`, carrying `bearer`, and returns the audit lines logged
    /// and whether it was admitted.
    fn admit_with(auth: &Authorizer, bearer: &BearerToken) -> (Vec<String>, bool) {
        let log = AuditLog::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .finish();
        let mut request = http::Request::builder()
            .uri("/fleetring.v1.Control/Join")
            .header(AUTHORIZATION, format!("Bearer {}", bearer.expose()))
            .body(())
            .expect("a request");
        request.extensions_mut().insert(principal("app"));
        let admitted =
            tracing::subscriber::with_default(subscriber, || auth.admit(&request).is_ok());
        (log.lines(), admitted)
    }

    #[test]
    fn a_refused_tokens_audit_line_names_it_once_it_verified() {
        let fixture = fixture();
        let denylist = fresh();
        let auth = authorizer_at(
            &fixture,
            &[],
            SimClock::new(Timestamp::from_millis(1_000)),
            denylist.clone(),
        );
        let credential = |lines: &[String]| -> String {
            let refusal = lines
                .iter()
                .find(|line| line.contains("refused a token"))
                .unwrap_or_else(|| panic!("no refusal in {lines:?}"));
            refusal
                .split_whitespace()
                .find(|word| word.starts_with("credential="))
                .unwrap_or_else(|| panic!("no credential in {refusal}"))
                .to_owned()
        };

        // A revoked token, as `token revoke --token-id` leaves it.
        let bearer = token(&fixture.signer, "app", &["read:x"], "t1");
        denylist.replace(
            [(Revoked::Token("t1".parse().expect("id")), Timestamp::MAX)]
                .into_iter()
                .collect(),
        );
        let (lines, admitted) = admit_with(&auth, &bearer);
        assert!(!admitted);
        assert_eq!(credential(&lines), "credential=\"token:t1\"", "{lines:?}");
        assert!(
            lines
                .iter()
                .any(|line| line.contains("the token is revoked")),
            "{lines:?}"
        );

        // So is a token whose signing key is revoked.
        denylist.replace(
            [(
                Revoked::Key(fixture.signer.key_id().clone()),
                Timestamp::MAX,
            )]
            .into_iter()
            .collect(),
        );
        let other = token(&fixture.signer, "app", &["read:x"], "t2");
        let (lines, _) = admit_with(&auth, &other);
        assert_eq!(credential(&lines), "credential=\"token:t2\"", "{lines:?}");

        // And an expired one.
        denylist.replace(Revocations::new());
        let late = authorizer_at(
            &fixture,
            &[],
            SimClock::new(Timestamp::from_millis(3_600_000)),
            fresh(),
        );
        let (lines, _) = admit_with(&late, &bearer);
        assert_eq!(credential(&lines), "credential=\"token:t1\"", "{lines:?}");

        // A token that never verified has no ID to name, and a forged ID isn't believed.
        let tampered = BearerToken::new(format!("{}x", bearer.expose()));
        let (lines, _) = admit_with(&auth, &tampered);
        assert_eq!(credential(&lines), "credential=\"token\"", "{lines:?}");
        let foreign = Authority::create(
            "test".parse().expect("a fleet"),
            Validity::new(Timestamp::ZERO, std::time::Duration::from_secs(3600)),
        )
        .expect("another authority");
        let forged = token(
            &foreign.token_signer().expect("a signer"),
            "app",
            &["admin:*"],
            "t9",
        );
        let (lines, _) = admit_with(&auth, &forged);
        assert_eq!(credential(&lines), "credential=\"token\"", "{lines:?}");
    }
}
