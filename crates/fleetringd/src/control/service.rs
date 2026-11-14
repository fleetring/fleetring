//! The gRPC `Control` service over the control node (ADR-0017, ADR-0019).

use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use fleetring::{NodeId, ReplicationFactor, Weight};
use fleetring_control::delegation::{
    AgentName, CallerToken, CanonicalRequest, ClaimField, Delegation, DelegationId, Forgery,
    Resent, TokenClaims, TokenKind,
};
use fleetring_control::fork::{ForkExpiry, ForkIntent, ForkLimits, OperationId, OperationRecord};
use fleetring_control::scope::Scopes;
use fleetring_control::{
    Acks, Action, Applied, DatabaseName, Durability, Engine, ExpiryChange, Feature, Incarnation,
    KeyId, Lease, LeaseEpoch, Liveness, Member, MetadataSpec, Millis, Position, PrincipalName,
    Rejection, Revoked, Software, State, Timestamp,
};
use fleetring_proto::v1::control_server::Control;
use fleetring_proto::v1::{
    self as proto, AcquireLeaseRequest, ChangeDurabilityRequest, ChangeDurabilityResponse,
    CompletePromotionRequest, CompletePromotionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateDatabaseWithMetadataRequest, DelegateTokenRequest,
    DelegateTokenResponse, DrainRequest, DrainResponse, DropDatabaseRequest, DropDatabaseResponse,
    ForkDatabaseRequest, ForkOperationStatus, GetForkOperationRequest, HeartbeatRequest,
    HeartbeatResponse, JoinRequest, JoinResponse, LeaseResponse, LeaveRequest, LeaveResponse,
    ListDatabasesRequest, ListDatabasesResponse, ListDelegationsRequest, ListDelegationsResponse,
    LocateRequest, LocateResponse, MembershipUpdate, Placement, PromotionResponse,
    RecordFencedRequest, RecordFencedResponse, RenewLeaseRequest, RevokeTokenRequest,
    RevokeTokenResponse, StartPromotionRequest, TransferLeaseRequest, UndrainRequest,
    UndrainResponse, UpdateDatabaseRequest, UpdateDatabaseResponse, WatchMembershipRequest,
};
use fleetring_proto::{PROTOCOL_VERSION, check_protocol};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::auth::{
    Action as AuthAction, Authorized, BearerToken, BindingDenied, Caller, Carriage, Credential,
    DelegationSigner, IssueError, Signer, Unbound, authorized, require_database,
};
use crate::clock::{Clock, SystemClock};
use crate::control::delegation::{self, Times};
use crate::control::fork::{
    SeedProgressBoard, fork_status, operation_from_proto, operation_status, replica_from_proto,
    role_to_proto,
};
use crate::control::node::{ControlHandle, SubmitError};
use crate::control::notices::DropNotices;
use crate::data::convert::{
    acks_from_proto, acks_to_proto, change_from_proto, description_from_proto, labels_from_proto,
    metadata_to_proto, revocations_to_proto, spec_expiry_from_proto,
};
use crate::data::{Descriptions, Expiry, ExpiryRequest, LabelRequirement, is_invisible};
use crate::dev::DevMode;
use crate::environment::Network;
use crate::replication::ForkFaults;
use crate::router::{Entropy, SystemEntropy};
use crate::software::{software_from_proto, software_to_proto};
use crate::tls::{IssuerCertError, IssuerPair, Peer};

mod revoke;

/// The stream `WatchMembership` returns.
pub type MembershipStream = Pin<Box<dyn Stream<Item = Result<MembershipUpdate, Status>> + Send>>;

/// The durability a database is created with when its creation names none: the control node's
/// `[replication] acks` and `min_replicas` (ADR-0023).
///
/// A default floor that doesn't fit a database's replication factor is clamped into W ≤ floor
/// ≤ R, since it was set for the fleet rather than for that database; an explicit floor in a
/// creation is rejected instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct DurabilityDefaults {
    /// The acknowledgement mode.
    pub acks: Acks,
    /// The floor on reachable replicas, or `None` for the mode's W.
    pub min_replicas: Option<u8>,
}

impl DurabilityDefaults {
    /// [`Acks::Quorum`] with the floor at W.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            acks: Acks::Quorum,
            min_replicas: None,
        }
    }

    /// The defaults for a database with `replication_factor` replicas.
    #[must_use]
    pub fn for_factor(self, replication_factor: ReplicationFactor) -> Durability {
        let write = Durability::new(self.acks, replication_factor).min_replicas();
        let floor = self
            .min_replicas
            .unwrap_or(write)
            .clamp(write, replication_factor.get());
        Durability::with_min_replicas(self.acks, floor, replication_factor)
            .unwrap_or_else(|_| Durability::new(self.acks, replication_factor))
    }
}

/// The least time ahead of the control plane's time that a new expiry may be (ADR-0041,
/// point 4): an expiry that passes before the creating client's next call is a mistake.
pub const MIN_EXPIRY: Millis = Millis::new(60_000);

/// How many incarnations a creation or a fork draws before it gives up: `apply` refuses one that
/// is 0 once forks are enabled, or that a database or a retained operation already uses, and
/// the service draws again (ADR-0046, point 1.1). Drawing more than twice means the generator is
/// broken.
const INCARNATION_DRAWS: usize = 4;

/// How many delegation IDs a delegation draws before it gives up: `apply` refuses one a kept
/// record already has, and the service draws again (ADR-0047, point 2.4). Drawing more than
/// twice means the generator is broken.
const DELEGATION_ID_DRAWS: usize = 4;

/// The log target of audit lines, as the request boundary writes them.
const AUDIT: &str = "fleetringd::audit";

/// The `Control` service: every mutation goes through the control node's single writer, and
/// reads come from its published state.
#[derive(Debug, Clone)]
pub struct ControlService {
    node: ControlHandle,
    replication_factor: ReplicationFactor,
    defaults: DurabilityDefaults,
    clock: Arc<dyn Clock>,
    dev: DevMode,
    notices: Option<DropNotices>,
    /// Where incarnations come from (ADR-0020, ADR-0046 point 1.1).
    entropy: Arc<dyn Entropy>,
    /// The limits each fork this service proposes records (ADR-0046, point 7).
    fork_limits: ForkLimits,
    /// Test-only faults: a lost reply to a fork request (FR-086).
    fork_faults: ForkFaults,
    /// The progress of the copies replicas stream, for forks' status.
    progress: SeedProgressBoard,
    /// The control node's delegation issuer pair, which signs delegated agents' tokens
    /// (ADR-0047, point 1); `None` refuses every delegation.
    issuer: Option<IssuerPair>,
}

impl ControlService {
    /// Serves the control node behind `node`, creating databases with three replicas and the
    /// default durability unless a creation says otherwise.
    #[must_use]
    pub fn new(node: ControlHandle) -> Self {
        Self {
            node,
            replication_factor: ReplicationFactor::default(),
            defaults: DurabilityDefaults::new(),
            clock: Arc::new(SystemClock),
            dev: DevMode::Off,
            notices: None,
            entropy: Arc::new(SystemEntropy::new()),
            fork_limits: ForkLimits::new(),
            fork_faults: ForkFaults::default(),
            progress: SeedProgressBoard::default(),
            issuer: None,
        }
    }

    /// Signs delegated agents' tokens with `pair`, the control node's `[control] issuer_cert`
    /// and `issuer_key` (ADR-0047, point 1.3). Without one, `DelegateToken` answers
    /// `FAILED_PRECONDITION`.
    #[must_use]
    pub fn with_issuer(mut self, pair: IssuerPair) -> Self {
        self.issuer = Some(pair);
        self
    }

    /// Draws incarnations from `entropy`, the operating system's randomness by default, and a
    /// simulation's seeded generator under simulation (ADR-0020).
    #[must_use]
    pub fn with_entropy(mut self, entropy: Arc<dyn Entropy>) -> Self {
        self.entropy = entropy;
        self
    }

    /// Passes the fork requests this service applies through `faults`, which a test uses to
    /// lose a reply (FR-086). Without the `fault-hooks` feature the faults do nothing.
    #[must_use]
    pub fn with_fork_faults(mut self, faults: ForkFaults) -> Self {
        self.fork_faults = faults;
        self
    }

    /// Records `limits` with each fork this service proposes: the control node's `[fork]`
    /// configuration (ADR-0046, point 7). A fork keeps the limits it was made under.
    #[must_use]
    pub fn with_fork_limits(mut self, limits: ForkLimits) -> Self {
        self.fork_limits = limits;
        self
    }

    /// Draws a database's incarnation: 64 bits from the injected generator. `apply` refuses 0
    /// where it would store it, and the caller draws again.
    fn draw_incarnation(&self) -> Incarnation {
        let mut bytes = [0_u8; 8];
        self.entropy.fill(&mut bytes);
        Incarnation::new(u64::from_le_bytes(bytes))
    }

    /// Tells the replicas of each database it drops over `network`, so that they delete their
    /// copies (ADR-0042). Without it, their copies are kept as orphans.
    #[must_use]
    pub fn with_network(mut self, network: Network) -> Self {
        self.notices = Some(DropNotices::new(network));
        self
    }

    /// Tells every membership watcher whether this control node runs in dev mode (ADR-0039).
    #[must_use]
    pub fn with_dev_mode(mut self, dev: DevMode) -> Self {
        self.dev = dev;
        self
    }

    /// Reads the time from `clock`, which bounds how long a token's revocation is kept; the
    /// system clock by default.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Creates databases whose creation names no replication factor with `replication_factor`
    /// replicas: the control node's `[replication] replication_factor` (ADR-0039). The factor is
    /// filled in before the creation is logged, so the log never depends on this setting.
    #[must_use]
    pub fn with_default_replication_factor(
        mut self,
        replication_factor: ReplicationFactor,
    ) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// The replication factor a creation asks for: `requested`, or the fleet's default when it
    /// is 0.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` if `requested` is above 255.
    fn replication_factor(&self, requested: u32) -> Result<ReplicationFactor, Status> {
        if requested == 0 {
            return Ok(self.replication_factor);
        }
        u8::try_from(requested)
            .ok()
            .and_then(ReplicationFactor::new)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "invalid replication factor {requested}: expected 1 to 255, or 0 for the \
                     fleet's default"
                ))
            })
    }

    /// Creates databases whose creation names no durability with `defaults`.
    #[must_use]
    pub fn with_durability_defaults(mut self, defaults: DurabilityDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// The durability a creation asks for: `acks` and `min_replicas` as the request carries
    /// them, with the fleet's defaults for what it leaves unset.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` if an explicit floor doesn't fit `replication_factor`.
    fn durability(
        &self,
        acks: i32,
        min_replicas: u32,
        replication_factor: ReplicationFactor,
    ) -> Result<Durability, Status> {
        let acks = acks_from_proto(acks).unwrap_or(self.defaults.acks);
        let min_replicas = u8::try_from(min_replicas).map_err(|_| {
            Status::invalid_argument(format!("invalid min_replicas {min_replicas}"))
        })?;
        if min_replicas == 0 {
            let defaults = DurabilityDefaults {
                acks,
                ..self.defaults
            };
            return Ok(defaults.for_factor(replication_factor));
        }
        Durability::with_min_replicas(acks, min_replicas, replication_factor)
            .map_err(|error| Status::invalid_argument(error.to_string()))
    }

    /// What a creation asks for, resolved before the command is stamped and logged, so the log
    /// holds the factor and durability themselves and replays the same whatever the
    /// configuration says later (ADR-0039).
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for an invalid name, one with an invisible character included, factor,
    /// engine, or durability.
    fn creation(
        &self,
        request: &CreateDatabaseRequest,
    ) -> Result<(DatabaseName, ReplicationFactor, Engine, Durability), Status> {
        let name: DatabaseName = parse("database name", &request.database)?;
        check_new_name(&name)?;
        let replication_factor = self.replication_factor(request.replication_factor)?;
        let engine = engine_from(request.engine)?;
        let durability = self.durability(request.acks, request.min_replicas, replication_factor)?;
        Ok((name, replication_factor, engine, durability))
    }

    /// The time `expiry` names, resolved before the command is stamped and logged, so the log
    /// holds only absolute times (ADR-0041, point 4). A duration counts from the later of this
    /// node's clock and the control plane's last applied stamp, which may be ahead of this
    /// node's clock after a failover from a node whose clock ran ahead; resolving against the
    /// later of the two keeps a fresh duration from landing behind the time `apply` judges by.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for a time less than [`MIN_EXPIRY`] after that time.
    fn resolve_expiry(&self, expiry: Expiry) -> Result<Timestamp, Status> {
        let now = self.clock.now().max(self.node.state().now());
        let at = match expiry {
            Expiry::At(at) => at,
            Expiry::In(duration) => now.saturating_add(Millis::new(
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            )),
        };
        if at < now.saturating_add(MIN_EXPIRY) {
            return Err(Status::invalid_argument(
                "expiry must be at least 1m from now",
            ));
        }
        Ok(at)
    }

    /// The expiry change an update asks for, with a new time resolved as
    /// [`resolve_expiry`](Self::resolve_expiry) does.
    fn expiry_change(&self, expiry: ExpiryRequest) -> Result<ExpiryChange, Status> {
        Ok(match expiry {
            ExpiryRequest::Keep => ExpiryChange::Keep,
            ExpiryRequest::At(at) => ExpiryChange::Set(self.resolve_expiry(Expiry::At(at))?),
            ExpiryRequest::In(duration) => {
                ExpiryChange::Set(self.resolve_expiry(Expiry::In(duration))?)
            }
            ExpiryRequest::Clear => ExpiryChange::Clear,
        })
    }

    /// Submits a creation and answers with the new database's placement.
    async fn create(
        &self,
        name: DatabaseName,
        replication_factor: ReplicationFactor,
        action: Action,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        match self.submit_drawing(action).await? {
            (Applied::DatabaseCreated { .. }, state) => {
                let placement = placement(&state, &name, Descriptions::Include, &self.progress)?;
                let replicas = placement.replicas.len();
                if replicas < usize::from(replication_factor.get()) {
                    // Accepted all the same: the fleet may still be growing, and placement
                    // fills the replica set as members join (ADR-0039).
                    tracing::warn!(
                        database = %name,
                        replication_factor = replication_factor.get(),
                        replicas,
                        "the database has fewer replicas than its replication factor until more \
                         data nodes join"
                    );
                }
                Ok(Response::new(CreateDatabaseResponse {
                    placement: Some(placement),
                }))
            }
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn submit(&self, action: Action) -> Result<(Applied, Arc<State>), Status> {
        let committed = self.node.submit(action).await.map_err(submit_status)?;
        let (outcome, state) = committed.into_parts();
        match outcome {
            Ok(applied) => Ok((applied, state)),
            Err(rejection) => Err(rejection_status(&rejection)),
        }
    }

    /// Operation `operation`'s status from its `record` in `state`, with the fork's placement
    /// while its name holds the operation's incarnation.
    fn status_of_operation(
        &self,
        state: &State,
        operation: OperationId,
        record: &OperationRecord,
    ) -> Result<ForkOperationStatus, Status> {
        let placement = state
            .database(record.target())
            .filter(|database| database.incarnation() == record.target_incarnation())
            .map(|database| {
                placement(
                    state,
                    database.name(),
                    Descriptions::Include,
                    &self.progress,
                )
            })
            .transpose()?;
        Ok(operation_status(state, operation, record, placement))
    }

    /// Submits `action`, a command that brings a database name into existence, with an
    /// incarnation drawn from the injected generator, and draws again and resubmits while
    /// `apply` refuses the incarnation as unavailable (ADR-0046, point 1.1). Every creation draws
    /// one, before forks are enabled too: `apply` stores 0 until then, so a creation screened
    /// before the feature was enabled and applied after it still stores a random value.
    ///
    /// # Errors
    ///
    /// As [`ControlService::submit`], and `UNAVAILABLE` if every draw was refused.
    async fn submit_drawing(&self, mut action: Action) -> Result<(Applied, Arc<State>), Status> {
        for _ in 0..INCARNATION_DRAWS {
            let drawn = self.draw_incarnation();
            match &mut action {
                Action::CreateDatabase { incarnation, .. }
                | Action::ForkDatabase { incarnation, .. } => *incarnation = drawn,
                _ => {}
            }
            let committed = self
                .node
                .submit(action.clone())
                .await
                .map_err(submit_status)?;
            match committed.into_parts() {
                (Ok(applied), state) => return Ok((applied, state)),
                (Err(Rejection::IncarnationUnavailable { incarnation }), _) => {
                    tracing::warn!(%incarnation, "the incarnation drawn is unavailable; drawing again");
                }
                (Err(rejection), _) => return Err(rejection_status(&rejection)),
            }
        }
        Err(Status::unavailable(
            "every incarnation drawn for the database was unavailable; retry",
        ))
    }
}

impl ControlService {
    /// Mints a delegated agent's token for `caller` (ADR-0047, point 5), in the order the plan
    /// gives. Every refusal comes before anything is logged, `apply`'s too, which the writer
    /// answers unlogged (`control/node.rs`), and the token reaches no log,
    /// error, or metric: it travels only in the reply.
    async fn delegate(
        &self,
        caller: &Delegator,
        request: DelegateTokenRequest,
    ) -> Result<Delegated, Status> {
        let operation = operation_from_proto(&request.operation_id)?;
        let child = delegation::child_name(&caller.name, &request.segment)?;
        let asked = delegation::parse_scopes(&request.scopes)?;
        let lifetime = request.lifetime_ms.map(Millis::new);
        let not_before = request.not_before_ms.map(Timestamp::from_millis);

        // The feature: a fleet with a member that lacks it never delegates.
        let state = self.node.state();
        state
            .check_feature(Feature::Delegation)
            .map_err(|rejection| rejection_status(&rejection))?;

        // The time every check below judges by.
        let now = delegation::resolve_now(self.clock.now(), state.now())?;

        // The issuer: one signer for the whole request, so its certificate, key ID, and expiry
        // come from one pair even if a rotation lands meanwhile.
        let signer = self.issuer_signer(&state, now)?;

        // The caller, against this state: its lineage recorded as presented, and nothing in it
        // revoked, the authority key that certified its issuer included.
        check_lineage(&state, caller, now)?;
        if let Some(by) = caller
            .signer
            .refusal_in(state.revocations(), &caller.claims, &caller.key)
        {
            return Err(rejection_status(&Rejection::CallerRevoked { by }));
        }

        let presented = CallerToken {
            claims: (*caller.claims).clone(),
            kid: caller.key.clone(),
        };
        let canonical = CanonicalRequest {
            segment: request.segment.into(),
            scopes: asked.as_ref().map(Scopes::normalized),
            lifetime,
            not_before,
            caller_token: caller.claims.id.clone(),
        };
        // Resolved below for a new delegation; a resend's are ignored.
        let mut action = Action::RecordDelegation {
            operation,
            requested_by: caller.name.as_principal().clone(),
            request: canonical,
            caller: presented.clone(),
            id: DelegationId::from_bytes([0; 16]),
            scopes: Scopes::none(),
            not_before: Timestamp::from_millis(0),
            expires_at: Timestamp::from_millis(0),
        };

        // A resend from state, answered without logging.
        if let Some(resent) = state.delegation_resend(&action, now) {
            let (id, record) = resent.map_err(|rejection| rejection_status(&rejection))?;
            let recorded = Recorded {
                id: id.clone(),
                claims: record.claims.clone(),
                origin: Origin::Resent,
            };
            return self.sign_recorded(&signer, caller, lifetime, recorded);
        }

        let scopes = delegation::resolve_scopes(&caller.scopes, &child, asked.as_ref())?;
        let times = delegation::resolve_times(now, caller.claims.expires_at, not_before, lifetime)?;
        if times.expires_at > signer.expires_at() {
            return Err(issuer_refusal(
                self.issuer.as_ref(),
                "expires before the token would",
            ));
        }

        // A token every node would refuse is refused before anything is recorded (P13).
        let mut id = self.draw_delegation_id();
        let trial = delegation::child_claims(&presented, &child, &id, scopes.clone(), times, now);
        signer.sign(&trial).map_err(|error| sign_status(&error))?;

        fill(&mut action, &id, scopes, times);
        for draw in 1..=DELEGATION_ID_DRAWS {
            // The writer answers `apply`'s refusal of this command without logging it.
            let outcome = match self.node.submit(action.clone()).await {
                Ok(committed) => committed.into_outcome(),
                Err(SubmitError::Refused(rejection)) => Err(rejection),
                Err(error) => return Err(submit_status(error)),
            };
            match outcome {
                Ok(Applied::DelegationRecorded { id, claims, resent }) => {
                    let origin = match resent {
                        Resent::No => Origin::Recorded,
                        Resent::Yes => Origin::Resent,
                    };
                    let recorded = Recorded { id, claims, origin };
                    return self.sign_recorded(&signer, caller, lifetime, recorded);
                }
                Ok(other) => return Err(unexpected(&other)),
                Err(Rejection::DelegationIdTaken { .. }) if draw < DELEGATION_ID_DRAWS => {
                    tracing::warn!("the delegation ID drawn is taken; drawing again");
                    id = self.draw_delegation_id();
                    if let Action::RecordDelegation { id: drawn, .. } = &mut action {
                        *drawn = id.clone();
                    }
                }
                Err(rejection) => return Err(rejection_status(&rejection)),
            }
        }
        Err(Status::unavailable(
            "every delegation ID drawn was taken; retry with the same operation ID",
        ))
    }

    /// Signs `recorded`, a recorded delegation's claims, with `signer`, the one pair this
    /// request took, and answers with the token.
    fn sign_recorded(
        &self,
        signer: &DelegationSigner,
        caller: &Delegator,
        lifetime: Option<Millis>,
        recorded: Recorded,
    ) -> Result<Delegated, Status> {
        let Recorded { id, claims, origin } = recorded;
        if claims.expires_at > signer.expires_at() {
            return Err(issuer_refusal(
                self.issuer.as_ref(),
                "expires before the token would",
            ));
        }
        let token = signer.sign(&claims).map_err(|error| {
            tracing::error!(%error, delegation = %id, "a recorded delegation couldn't be signed");
            Status::unavailable(
                "the delegation is recorded but couldn't be signed; retry with the same operation \
                 ID",
            )
        })?;
        Ok(Delegated {
            capped: delegation::capped(lifetime, claims.expires_at, caller.claims.expires_at),
            token,
            id,
            issuer_key: signer.key_id().clone(),
            claims,
            origin,
        })
    }

    /// The signer of the issuer pair in use, if it may sign now (ADR-0047, point 1.6): the pair
    /// is configured, its key and the authority key that certified it aren't revoked, and its
    /// certificate is valid at `now`. The signer reads no clock, so this is where the time is
    /// checked; a pair that has expired, or isn't valid yet, loads by design.
    fn issuer_signer(&self, state: &State, now: Timestamp) -> Result<DelegationSigner, Status> {
        let Some(pair) = &self.issuer else {
            return Err(Status::failed_precondition(
                "this fleet has no delegation issuer; run `fleetring cert issuer`",
            ));
        };
        let refuse = |why: &str| issuer_refusal(Some(pair), why);
        let signer = pair.signer().map_err(|error| {
            tracing::error!(%error, "the delegation issuer pair can't sign");
            refuse("can't be used")
        })?;
        let certificate = signer.certificate();
        let revocations = state.revocations();
        if revocations.contains(&Revoked::Key(signer.key_id().clone())) {
            return Err(refuse("has a revoked key"));
        }
        if revocations.contains(&Revoked::Key(certificate.authority_key_id().clone())) {
            return Err(refuse("was certified by a revoked authority key"));
        }
        match certificate.valid_at(now) {
            Ok(()) => Ok(signer),
            Err(IssuerCertError::NotYetValid) => Err(refuse("isn't valid yet")),
            Err(_) => Err(refuse("has expired")),
        }
    }

    /// A delegation ID: 16 bytes from the injected generator (ADR-0047, point 2.4).
    fn draw_delegation_id(&self) -> DelegationId {
        let mut bytes = [0_u8; 16];
        self.entropy.fill(&mut bytes);
        DelegationId::from_bytes(bytes)
    }
}

/// Fills a `RecordDelegation`'s resolved fields: the drawn ID, the scopes, and the times.
fn fill(action: &mut Action, drawn: &DelegationId, resolved: Scopes, times: Times) {
    if let Action::RecordDelegation {
        id,
        scopes,
        not_before,
        expires_at,
        ..
    } = action
    {
        *id = drawn.clone();
        *scopes = resolved;
        *not_before = times.not_before;
        *expires_at = times.expires_at;
    }
}

/// A caller that may ask for a delegation, as the request boundary authenticated it: an agent,
/// by its own token, carried directly.
#[derive(Debug)]
struct Delegator {
    /// The caller's name, `P`.
    name: AgentName,
    /// Every claim of the token it presented.
    claims: Arc<TokenClaims>,
    /// The key that signed the copy it presented.
    key: KeyId,
    /// Who signed that copy.
    signer: Signer,
    /// Its effective scopes: its token's.
    scopes: Scopes,
}

impl Delegator {
    /// The caller of `request`, if it may ask for a delegation at all (ADR-0047, points 4.5 and
    /// 5.2): a principal authenticated by its token, never a certificate alone; its token not
    /// relay-carried; an agent's token, naming an agent and an owner.
    ///
    /// # Errors
    ///
    /// `PERMISSION_DENIED` naming which of these the caller isn't.
    fn of<T>(request: &Request<T>) -> Result<Self, Status> {
        let (name, scopes, credential) = match authorized(request)?.caller() {
            Caller::Principal {
                name,
                scopes,
                credential,
            } => (name, scopes, credential),
            Caller::Node(_) | Caller::Anonymous => {
                return Err(Status::permission_denied(
                    "delegation needs an agent's bearer token",
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
                    "delegation needs an agent's bearer token; a certificate alone can't delegate",
                ));
            }
        };
        match carriage {
            Carriage::Direct => {}
            Carriage::Relayed => {
                return Err(Status::permission_denied(
                    "delegation needs a direct connection with the certificate of the caller or \
                     an ancestor",
                ));
            }
        }
        if claims.kind != Some(TokenKind::Agent) {
            return Err(Status::permission_denied(
                "only an agent's token can delegate, and this token doesn't say it is one",
            ));
        }
        let agent = AgentName::try_from(claims.principal.clone()).map_err(|_| {
            Status::permission_denied(format!(
                "only an agent can delegate, and '{name}' isn't one"
            ))
        })?;
        if claims.owner.is_none() {
            return Err(Status::permission_denied(
                "an agent's token without an owner can't delegate",
            ));
        }
        Ok(Self {
            name: agent,
            claims: Arc::clone(claims),
            key: key.clone(),
            signer: signer.clone(),
            scopes: scopes.clone(),
        })
    }
}

/// Whether a delegation was recorded by this request or answered a resend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Recorded,
    Resent,
}

impl Origin {
    fn as_str(self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::Resent => "resent",
        }
    }
}

/// A recorded delegation, to sign: its ID, its claims, and whether this request recorded it.
struct Recorded {
    id: DelegationId,
    claims: TokenClaims,
    origin: Origin,
}

/// A signed delegation, for the reply and the audit line.
struct Delegated {
    token: BearerToken,
    id: DelegationId,
    claims: TokenClaims,
    issuer_key: KeyId,
    capped: bool,
    origin: Origin,
}

impl Delegated {
    fn into_response(self) -> DelegateTokenResponse {
        DelegateTokenResponse {
            token: self.token.expose().to_owned(),
            delegation_id: self.id.to_string(),
            principal: self.claims.principal.to_string(),
            scopes: scope_texts(&self.claims.scopes),
            not_before_ms: self.claims.not_before.as_millis(),
            expires_at_ms: self.claims.expires_at.as_millis(),
            capped: self.capped,
            issuer_key_id: self.issuer_key.to_string(),
        }
    }
}

/// Each scope's canonical text, in order.
fn scope_texts(scopes: &Scopes) -> Vec<String> {
    scopes.as_slice().iter().map(ToString::to_string).collect()
}

/// Checks a delegated caller's lineage against `state` (ADR-0047, point 5.9, I6): its
/// delegation is recorded and live, and the claims it presented are the recorded ones, field by
/// field. A root caller has no record. `apply` checks the same before it records; checking here
/// too refuses a forged caller's resend, which `apply` never sees.
fn check_lineage(state: &State, caller: &Delegator, now: Timestamp) -> Result<(), Status> {
    if !caller.name.is_delegated() {
        return Ok(());
    }
    let forged = |reason| rejection_status(&Rejection::ForgedLineage { reason });
    let record = DelegationId::try_from(caller.claims.id.clone())
        .ok()
        .and_then(|id| state.delegation(&id, now))
        .ok_or_else(|| forged(Forgery::Unrecorded))?;
    if !record.is_live(now, now) {
        return Err(forged(Forgery::NotLive));
    }
    if let Some(field) = ClaimField::first_difference(&record.claims, &caller.claims) {
        return Err(forged(Forgery::Differs { field }));
    }
    Ok(())
}

/// The `FAILED_PRECONDITION` a delegation answers when the issuer pair can't sign it, naming
/// the certificate file and the command that makes a new pair (ADR-0047, point 1.6).
fn issuer_refusal(pair: Option<&IssuerPair>, why: &str) -> Status {
    let cert = pair.map_or_else(
        || "certificate".to_owned(),
        |pair| format!("certificate {}", pair.files().cert.display()),
    );
    Status::failed_precondition(format!(
        "the delegation issuer {cert} {why}; issue a new pair with `fleetring cert issuer` and \
         replace [control] issuer_cert and issuer_key"
    ))
}

/// The status a trial signature's refusal answers, before anything is recorded.
fn sign_status(error: &IssueError) -> Status {
    match error {
        IssueError::TooLarge { .. } => {
            Status::invalid_argument("the child's token would exceed 8 KiB; ask for fewer scopes")
        }
        IssueError::OutlivesCertificate => Status::failed_precondition(
            "the delegation issuer certificate expires before the token would; issue a new pair \
             with `fleetring cert issuer`",
        ),
        other => Status::invalid_argument(format!("can't delegate: {other}")),
    }
}

/// Writes the audit line of a delegation (ADR-0047, point 5.12): the caller, the child, the
/// scopes, the expiry, the operation, the delegation ID, the issuer key's ID, and the result;
/// never the token.
fn audit_delegation(
    caller: &Delegator,
    request: &AuditedRequest,
    result: &Result<Delegated, Status>,
) {
    let credential = format!("token:{}", caller.claims.id);
    match result {
        Ok(delegated) => tracing::info!(
            target: AUDIT,
            principal = %caller.name,
            credential,
            child = %delegated.claims.principal,
            scopes = %scope_texts(&delegated.claims.scopes).join(","),
            expires_at = delegated.claims.expires_at.as_millis(),
            operation = %request.operation,
            delegation = %delegated.id,
            issuer_key = %delegated.issuer_key,
            result = delegated.origin.as_str(),
            rpc = "DelegateToken",
            "delegated a token",
        ),
        Err(status) => tracing::warn!(
            target: AUDIT,
            principal = %caller.name,
            credential,
            segment = %request.segment,
            operation = %request.operation,
            result = "refused",
            code = ?status.code(),
            reason = status.message(),
            rpc = "DelegateToken",
            "refused a delegation",
        ),
    }
}

/// What an audit line names of the request itself.
struct AuditedRequest {
    operation: String,
    segment: String,
}

impl AuditedRequest {
    fn of(request: &DelegateTokenRequest) -> Self {
        Self {
            operation: operation_from_proto(&request.operation_id)
                .map_or_else(|_| "invalid".to_owned(), |operation| operation.to_string()),
            segment: request.segment.escape_debug().to_string(),
        }
    }
}

/// A live delegation record as `ListDelegations` returns it: never its token.
fn delegation_record(id: &DelegationId, record: &Delegation) -> proto::DelegationRecord {
    let claims = &record.claims;
    proto::DelegationRecord {
        principal: claims.principal.to_string(),
        delegation_id: id.to_string(),
        parent: claims
            .parent
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        chain: claims
            .chain
            .iter()
            .map(|link| proto::ChainLink {
                id: link.id.to_string(),
                kid: link.kid.to_string(),
            })
            .collect(),
        issued_at_ms: claims.issued_at.map_or(0, Timestamp::as_millis),
        expires_at_ms: claims.expires_at.as_millis(),
        recorded_by: record.recorded_by.to_string(),
        recorded_at_ms: record.recorded_at.as_millis(),
    }
}

/// Maps a rejection to the gRPC status a caller receives, with the rejection's message.
///
/// - `NOT_FOUND`: an unknown member or database, or, under the ancestor rule, a token ID that
///   is neither the caller's nor a live delegation's below it. An unknown database also names
///   itself in the
///   [`MISSING_DATABASE_KEY`] metadata, so that a caller can tell the two apart
///   ([`missing_database`]).
/// - `ALREADY_EXISTS`: the database exists, the member is already fenced, or a fork operation
///   ID was reused with a different request.
/// - `PERMISSION_DENIED`: a fork operation ID that another principal requested; a delegation
///   wider than its parent's record, by a revoked or forged caller; a revocation outside the
///   ancestor rule's reach.
/// - `RESOURCE_EXHAUSTED`: too few replicas have room for a fork, too many fork operations are
///   kept, or a delegation's or an ancestor-rule revocation's limit is reached.
/// - `UNAVAILABLE`: an incarnation or delegation ID that turned out to be in use, which a retry
///   draws again, or a delegation of a name revoked less than a second ago.
/// - `INVALID_ARGUMENT`: the request itself is malformed: a duplicate report, a join that
///   would make an invalid ring, durability settings that don't fit the replication factor, or
///   an update that would leave too many labels.
/// - `ABORTED`: a conditional metadata update whose version no longer holds.
/// - `UNAVAILABLE`: an expiry the control plane's time passed meanwhile, which a retry resolves.
/// - `FAILED_PRECONDITION`: anything that depends on the current state: liveness, leases, epochs,
///   promotions, fleet features, and exhausted counters.
#[must_use]
pub fn rejection_status(rejection: &Rejection) -> Status {
    let message = rejection.to_string();
    match rejection {
        Rejection::UnknownMember { .. } => Status::not_found(message),
        // Says nothing more, so nobody can probe for another principal's tokens (ADR-0047,
        // point 11.3).
        Rejection::NoLiveDelegation { .. } => Status::not_found(message),
        Rejection::UnknownDatabase { database } => {
            let mut status = Status::not_found(message);
            if let Ok(value) = MetadataValue::try_from(database.as_str()) {
                status.metadata_mut().insert(MISSING_DATABASE_KEY, value);
            }
            status
        }
        Rejection::DatabaseExists { .. }
        | Rejection::AlreadyFenced { .. }
        | Rejection::OperationConflict { .. }
        | Rejection::DelegationConflict { .. } => Status::already_exists(message),
        // Says nothing about the operation beyond that it isn't the caller's (ADR-0046,
        // point 5.2).
        Rejection::OperationNotYours { .. } => Status::permission_denied(message),
        // Room on the replicas, or for another operation record, may come later.
        Rejection::InsufficientDisk { .. } | Rejection::TooManyOperations { .. } => {
            Status::resource_exhausted(message)
        }
        // A delegation's limits (ADR-0047, point 5.10): a window may reopen, a slot free.
        Rejection::DelegationLimit { .. } => Status::resource_exhausted(message),
        // A delegation the request itself makes invalid, whoever proposed it.
        Rejection::DelegationStructure { .. }
        | Rejection::DelegationClaims { .. }
        | Rejection::NonCanonicalScopes => Status::invalid_argument(message),
        // The caller may not have it: wider than its parent's record, revoked, or forged.
        Rejection::NotNarrowed { .. }
        | Rejection::CallerRevoked { .. }
        | Rejection::ForgedLineage { .. } => Status::permission_denied(message),
        // Outside the caller's reach under the ancestor rule (ADR-0047, point 11.3).
        Rejection::NotAncestor { .. } => Status::permission_denied(message),
        // The ancestor rule's cap and rates (ADR-0047, point 11.6): a window may reopen, and
        // revoking the busiest child frees entries.
        Rejection::RevocationLimit { .. } => Status::resource_exhausted(message),
        Rejection::InvalidTopology { .. }
        | Rejection::DuplicateReport { .. }
        | Rejection::InvalidDurability { .. }
        | Rejection::TooManyLabels { .. } => Status::invalid_argument(message),
        // Another update landed first; the caller reads the metadata again (ADR-0041, point 2).
        Rejection::MetadataConflict { .. } => Status::aborted(message),
        // The service resolved the expiry against an older time than the command was stamped
        // with; a retry resolves it against the newer one (ADR-0041, point 4).
        Rejection::ExpiryPassed { .. } => {
            Status::unavailable("the control plane's time moved past the requested expiry; retry")
        }
        // The service draws again before it answers; a caller who still sees it retries.
        Rejection::IncarnationUnavailable { .. } | Rejection::DelegationIdTaken { .. } => {
            Status::unavailable(message)
        }
        // A delegation of a name revoked in the same second, which a retry a second later
        // records (ADR-0047, points 5.9 and 11.5).
        Rejection::RevokedThisSecond { .. } => Status::unavailable(message),
        _ => Status::failed_precondition(message),
    }
}

/// The metadata key of a `NOT_FOUND` status about an unknown database; its value names the
/// database.
pub const MISSING_DATABASE_KEY: &str = "fleetring-missing-database";

/// The database a `NOT_FOUND` status says doesn't exist, or `None` if the status is about
/// something else, such as an unknown member.
#[must_use]
pub fn missing_database(status: &Status) -> Option<&str> {
    if status.code() != tonic::Code::NotFound {
        return None;
    }
    status.metadata().get(MISSING_DATABASE_KEY)?.to_str().ok()
}

fn submit_status(error: SubmitError) -> Status {
    match error {
        SubmitError::Stopped => Status::unavailable(error.to_string()),
        SubmitError::Storage(_) => Status::internal(error.to_string()),
        SubmitError::Refused(rejection) => rejection_status(&rejection),
    }
}

fn unexpected(applied: &Applied) -> Status {
    tracing::error!(?applied, "control command had an unexpected outcome");
    Status::internal("unexpected outcome")
}

fn parse<T>(field: &str, text: &str) -> Result<T, Status>
where
    T: FromStr,
    T::Err: Display,
{
    text.parse()
        .map_err(|error| Status::invalid_argument(format!("invalid {field}: {error}")))
}

pub(crate) fn member_from(node: Option<proto::Node>) -> Result<Member, Status> {
    let node = node.ok_or_else(|| Status::invalid_argument("missing node"))?;
    let mut member = Member::new(parse("node id", &node.id)?);
    if let Some(zone) = &node.zone {
        member = member.with_zone(parse("zone", zone)?);
    }
    if let Some(host) = &node.host {
        member = member.with_host(parse("host", host)?);
    }
    // An empty address is no address, as a node too old to send one.
    if let Some(address) = node.address.filter(|address| !address.is_empty()) {
        member = member.with_address(address);
    }
    // Proto3 can't tell an unset weight from 0, so 0 means the default weight.
    Ok(member.with_weight(Weight::new(node.weight).unwrap_or(Weight::ONE)))
}

pub(crate) fn node_to_proto(member: &Member) -> proto::Node {
    proto::Node {
        id: member.id().to_string(),
        zone: member.zone().map(ToString::to_string),
        host: member.host().map(ToString::to_string),
        weight: member.weight().get(),
        address: member.address().map(str::to_owned),
    }
}

fn engine_from(engine: i32) -> Result<Engine, Status> {
    match proto::Engine::try_from(engine) {
        // Proto3 sends an unset engine as unspecified; SQLite is the only engine so far.
        Ok(proto::Engine::Unspecified | proto::Engine::Sqlite) => Ok(Engine::Sqlite),
        Err(_) => Err(Status::invalid_argument(format!("unknown engine {engine}"))),
    }
}

fn engine_to_proto(engine: Engine) -> proto::Engine {
    match engine {
        Engine::Sqlite => proto::Engine::Sqlite,
        _ => proto::Engine::Unspecified,
    }
}

fn lease_response(database: &DatabaseName, lease: &Lease) -> LeaseResponse {
    LeaseResponse {
        database: database.to_string(),
        epoch: lease.epoch().get(),
        expires_at_ms: lease.expires_at().as_millis(),
    }
}

/// Where a database lives: its lease holder as primary, the rest of its replica set, and what the
/// control plane knows about each replica.
///
/// While a promotion is in progress the database has no primary: the old holder's lease is at an
/// older epoch than the database's, and nobody holds the new one yet. The placement names the
/// promotion's candidate instead, so that a replica can check a fence or a stream from it
/// (ADR-0031).
///
/// The placement carries the database's metadata, if it has any, with its description text as
/// `descriptions` says, its incarnation, and, for a fork, the fork's state, with the seeder's
/// progress from `progress`. It also names where the ring places the database and the
/// membership epoch it was read at, so that a primary counts the write quorums of both the
/// replica set promotions fence and placement while they differ (ADR-0031, amendment 2).
fn placement(
    state: &State,
    name: &DatabaseName,
    descriptions: Descriptions,
    progress: &SeedProgressBoard,
) -> Result<Placement, Status> {
    let database = state.database(name).ok_or_else(|| {
        rejection_status(&Rejection::UnknownDatabase {
            database: name.clone(),
        })
    })?;
    let replicas = state
        .replica_set(name)
        .map_err(|rejection| rejection_status(&rejection))?;
    let primary = database
        .lease()
        .filter(|lease| lease.epoch() == database.epoch())
        .map(Lease::holder);
    let followers = replicas
        .iter()
        .filter(|id| Some(*id) != primary)
        .map(ToString::to_string)
        .collect();
    let now = state.now();
    let replicas = replicas
        .iter()
        .map(|id| {
            let report = state.report(id, name);
            proto::Replica {
                node_id: id.to_string(),
                address: state
                    .member(id)
                    .and_then(Member::address)
                    .map(str::to_owned),
                live: state.liveness(id, now) == Some(Liveness::Live),
                reported_role: report
                    .map_or(proto::ReplicaRole::Unspecified, |report| {
                        role_to_proto(report.role())
                    })
                    .into(),
                position: report.map(|report| report.position().as_bytes().to_vec()),
            }
        })
        .collect();
    let candidate = database
        .promotion()
        .filter(|promotion| promotion.epoch() == database.epoch())
        .map(|promotion| promotion.candidate().to_string());
    let durability = database.durability();
    Ok(Placement {
        database: name.to_string(),
        engine: engine_to_proto(database.engine()).into(),
        epoch: database.epoch().get(),
        primary: primary.map(ToString::to_string),
        followers,
        replicas,
        replication_factor: u32::from(database.replication_factor().get()),
        acks: acks_to_proto(durability.acks()).into(),
        min_replicas: u32::from(durability.min_replicas()),
        candidate,
        metadata: database
            .metadata()
            .map(|metadata| metadata_to_proto(metadata, descriptions)),
        incarnation: database.incarnation().get(),
        fork: database
            .fork()
            .map(|fork| fork_status(state, database, fork, progress)),
        placed_replicas: state
            .placed_replica_set(name)
            .map_err(|rejection| rejection_status(&rejection))?
            .iter()
            .map(ToString::to_string)
            .collect(),
        membership_epoch: Some(state.membership_epoch().get()),
    })
}

/// The forks that aren't ready yet whose replica set names `node`: those it seeds, or opens
/// once seeded (ADR-0046, points 4 and 6).
fn pending_forks(state: &State, node: &NodeId) -> Vec<String> {
    state
        .databases()
        .filter(|database| {
            database.fork().is_some_and(|fork| {
                matches!(
                    fork.state(),
                    fleetring_control::fork::ForkState::Seeding { .. }
                        | fleetring_control::fork::ForkState::Seeded { .. }
                )
            })
        })
        .filter(|database| {
            state
                .replica_set(database.name())
                .is_ok_and(|replicas| replicas.contains(node))
        })
        .map(|database| database.name().to_string())
        .collect()
}

/// What a membership update says about a member besides its attributes, which changes without
/// the membership epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberFlags {
    live: bool,
    draining: bool,
    /// The software it runs, which never changes the epoch (ADR-0038).
    software: Option<Software>,
}

/// Whether each member is live as of the state's time, whether it is draining, and what it runs,
/// in ID order.
fn member_flags(state: &State) -> Vec<MemberFlags> {
    let now = state.now();
    state
        .members()
        .map(|member| MemberFlags {
            live: state.liveness(member.id(), now) == Some(Liveness::Live),
            draining: state.is_draining(member.id()),
            software: member.software().cloned(),
        })
        .collect()
}

/// The membership as of the state's time, with the fleet features enabled, and whether this is
/// a dev mode control node.
fn membership_update(state: &State, flags: &[MemberFlags], dev: DevMode) -> MembershipUpdate {
    MembershipUpdate {
        membership_epoch: state.membership_epoch().get(),
        members: state
            .members()
            .zip(flags)
            .map(|(member, flags)| proto::Member {
                node: Some(node_to_proto(member)),
                live: flags.live,
                draining: flags.draining,
                software: flags.software.as_ref().map(software_to_proto),
            })
            .collect(),
        features: state
            .features()
            .map(|feature| feature.as_str().to_owned())
            .collect(),
        dev_mode: dev == DevMode::On,
    }
}

/// The databases `id` holds the lease on, in name order.
fn leases_held_by(state: &State, id: &fleetring::NodeId) -> Vec<String> {
    state
        .databases()
        .filter(|database| {
            database
                .lease()
                .is_some_and(|lease| lease.holder() == id && lease.epoch() == database.epoch())
        })
        .map(|database| database.name().to_string())
        .collect()
}

/// The principal that made `request`, as control state records it: a database's creator or a
/// description's author. It comes from the request boundary's caller, never from the request's
/// fields (ADR-0041, point 1); `None` for a node and for an anonymous caller.
///
/// # Errors
///
/// `INTERNAL` if the principal's name doesn't fit the grammar control state records, which a
/// test holds equal to a certificate's: it would take a change to one of them.
fn creator<T>(request: &Request<T>) -> Result<Option<PrincipalName>, Status> {
    let Ok(authorized) = authorized(request) else {
        return Ok(None);
    };
    match authorized.caller() {
        Caller::Principal { name, .. } => {
            PrincipalName::try_from(name).map(Some).map_err(|error| {
                tracing::error!(principal = %name, %error, "a principal's name can't be recorded");
                Status::internal("the caller's principal name can't be recorded")
            })
        }
        Caller::Node(_) | Caller::Anonymous => Ok(None),
    }
}

/// The prefix of the database names FleetRing keeps for its own databases (ADR-0044, the
/// `_fleetring/identity` database). It is a namespace of databases, apart from the SQLite
/// adapter's `_fleetring_*` tables inside each one.
pub(crate) const RESERVED_DATABASE_PREFIX: &str = "_fleetring";

/// Checks a new database's name at the request boundary: it may not start with
/// [`RESERVED_DATABASE_PREFIX`], and may hold no character a person reading it can't see, the
/// format characters (Unicode `Cf`, by the fixed table descriptions use, which takes in the
/// bidirectional controls and the zero-width characters) and the line and paragraph separators
/// (ADR-0041, amendment 5). `DatabaseName` itself accepts them, and so does `apply`, so a log
/// that holds such a name still replays.
///
/// # Errors
///
/// `INVALID_ARGUMENT` naming the reserved prefix, or the character and its byte offset.
fn check_new_name(name: &DatabaseName) -> Result<(), Status> {
    // Checked here, at the request, and never when the log replays, so that a database created
    // under such a name before the rule existed stays as it is (#114).
    if name.as_str().starts_with(RESERVED_DATABASE_PREFIX) {
        return Err(Status::invalid_argument(format!(
            "invalid database name: names starting with {RESERVED_DATABASE_PREFIX} are reserved \
             for FleetRing's own databases"
        )));
    }
    match name
        .as_str()
        .char_indices()
        .find(|(_, character)| is_invisible(*character))
    {
        Some((at, character)) => Err(Status::invalid_argument(format!(
            "invalid database name: it contains the character U+{:04X} at byte {at}, which a new \
             name may not hold",
            u32::from(character)
        ))),
        None => Ok(()),
    }
}

/// The node `request` comes from, if a node certificate made it: the binding checks are for
/// nodes, and a principal's request is judged by its scopes alone (ADR-0031, point 2).
///
/// # Errors
///
/// `PERMISSION_DENIED` if the request didn't pass through the request boundary.
fn calling_node<T>(request: &Request<T>) -> Result<Option<NodeId>, Status> {
    Ok(match authorized(request)?.caller() {
        Caller::Node(id) => Some(id.clone()),
        Caller::Principal { .. } | Caller::Anonymous => None,
    })
}

/// Checks that `node` is in `database`'s replica set, for a call bound to `replica`.
///
/// # Errors
///
/// `NOT_FOUND` for a database control state doesn't know, and a binding refusal otherwise.
fn require_replica(
    state: &State,
    method: &'static str,
    node: &NodeId,
    database: &DatabaseName,
) -> Result<(), Status> {
    let replicas = state
        .replica_set(database)
        .map_err(|rejection| rejection_status(&rejection))?;
    if replicas.contains(node) {
        return Ok(());
    }
    Err(BindingDenied::new(
        method,
        node.clone(),
        Unbound::NotReplica {
            database: database.clone(),
        },
    )
    .into_status())
}

/// Checks that `node` is the candidate of `database`'s promotion to `epoch`, for a call bound to
/// `candidate`.
///
/// An epoch has one candidate, so a check against one state still holds when the command
/// applies (ADR-0031, point 2). An unknown database, and an epoch whose promotion is over or
/// never started, are left to `apply`, which rejects them; no promotion to such an epoch can
/// start later, since a new one takes the epoch after the database's. An epoch past the
/// database's is refused here, because a promotion to it could start before the command applies.
///
/// # Errors
///
/// A binding refusal.
fn require_candidate(
    state: &State,
    method: &'static str,
    node: &NodeId,
    database: &DatabaseName,
    epoch: LeaseEpoch,
) -> Result<(), Status> {
    let Some(db) = state.database(database) else {
        return Ok(());
    };
    let bound = match db.promotion() {
        Some(promotion) if promotion.epoch() == epoch => promotion.candidate() == node,
        _ => epoch <= db.epoch(),
    };
    if bound {
        return Ok(());
    }
    Err(BindingDenied::new(
        method,
        node.clone(),
        Unbound::NotCandidate {
            database: database.clone(),
            epoch,
        },
    )
    .into_status())
}

/// Checks that `node` holds `database`'s lease, for a call bound to `holder`.
///
/// The lease changes holder only when a promotion completes, and its candidate is never
/// draining, so a lease that changed hands before the command applies is one `apply` refuses to
/// transfer.
///
/// # Errors
///
/// A binding refusal, also for a database control state doesn't know.
fn require_holder(
    state: &State,
    method: &'static str,
    node: &NodeId,
    database: &DatabaseName,
) -> Result<(), Status> {
    let db = state.database(database);
    if db
        .and_then(|db| db.lease())
        .is_some_and(|lease| lease.holder() == node)
    {
        return Ok(());
    }
    Err(BindingDenied::new(
        method,
        node.clone(),
        Unbound::NotHolder {
            database: database.clone(),
            epoch: db.map_or(LeaseEpoch::ZERO, |db| db.epoch()),
        },
    )
    .into_status())
}

#[tonic::async_trait]
impl Control for ControlService {
    type WatchMembershipStream = MembershipStream;

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let peer = Peer::of(&request);
        let request = request.into_inner();
        check_protocol(request.protocol.as_ref())?;
        let mut member = member_from(request.node)?;
        peer.check_node(member.id(), "Join")?;
        // A node that reports nothing runs 0.3.0 (ADR-0038, point 2).
        if let Some(software) = request.software {
            member = member.with_software(software_from_proto(software)?);
        }
        match self.submit(Action::Join { member }).await? {
            (
                Applied::MemberJoined {
                    membership_epoch, ..
                },
                state,
            ) => Ok(Response::new(JoinResponse {
                protocol: Some(PROTOCOL_VERSION),
                membership_epoch: membership_epoch.get(),
                revocations: revocations_to_proto(state.revocations()),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let peer = Peer::of(&request);
        let request = request.into_inner();
        let id: NodeId = parse("node id", &request.node_id)?;
        peer.check_node(&id, "Heartbeat")?;
        let mut replicas = Vec::with_capacity(request.databases.len());
        let mut progress = Vec::new();
        for status in request.databases {
            let (replica, streaming) = replica_from_proto(status)?;
            replicas.push(replica);
            progress.extend(streaming);
        }
        // The software goes into the log only when it corrects the record, as after the control
        // node was upgraded beside members that joined it earlier: not once per heartbeat.
        let software = request
            .software
            .map(software_from_proto)
            .transpose()?
            .filter(|software| {
                self.node.state().member(&id).and_then(Member::software) != Some(software)
            });
        let (applied, state) = self
            .submit(Action::Heartbeat {
                id: id.clone(),
                replicas,
                unknown_reports: fleetring_control::UnknownReports::Skip,
                software,
                disk_free_bytes: request.disk_free_bytes,
            })
            .await?;
        self.progress.record(&state, &id, progress);
        let unknown_databases = match applied {
            Applied::HeartbeatRecorded { unknown, .. } => unknown
                .into_iter()
                .map(|database| database.as_str().to_owned())
                .collect(),
            _ => Vec::new(),
        };
        Ok(Response::new(HeartbeatResponse {
            membership_epoch: state.membership_epoch().get(),
            unknown_databases,
            revocations: revocations_to_proto(state.revocations()),
            forks: pending_forks(&state, &id),
        }))
    }

    /// Sends the membership once its epoch passes `after_epoch`, then again whenever the epoch
    /// grows, a member's liveness, draining, or software changes, or a fleet feature is enabled.
    /// Liveness is judged at the time of the last applied command, so it changes only as
    /// commands, such as other members' heartbeats, arrive.
    async fn watch_membership(
        &self,
        request: Request<WatchMembershipRequest>,
    ) -> Result<Response<Self::WatchMembershipStream>, Status> {
        let caller = authorized(&request)?.clone();
        let after_epoch = request.into_inner().after_epoch;
        // What the watcher holds: none until the first state is seen.
        let mut held: Option<(u64, Vec<MemberFlags>, Vec<Feature>)> = None;
        let dev = self.dev;
        let updates = WatchStream::new(self.node.watch()).filter_map(move |state| {
            let epoch = state.membership_epoch().get();
            let flags = member_flags(&state);
            let features: Vec<Feature> = state.features().collect();
            // Epochs only grow, and members' attributes change only with the epoch, so a higher
            // epoch, new flags, or new features are a change the watcher hasn't seen.
            let send = match &held {
                None => epoch > after_epoch,
                Some((held_epoch, held_flags, held_features)) => {
                    epoch > (*held_epoch).max(after_epoch)
                        || flags != *held_flags
                        || features != *held_features
                }
            };
            let update = send.then(|| Ok(membership_update(&state, &flags, dev)));
            held = Some((epoch, flags, features));
            update
        });
        Ok(Response::new(caller.guard(Box::pin(updates))))
    }

    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let (name, replication_factor, engine, durability) = self.creation(request.get_ref())?;
        // A plain creation records its creator only once the fleet has enabled `metadata`, so
        // the first ordinary creation after an upgrade doesn't enable it and close the rollback
        // window (ADR-0041, point 3). The feature is enabled for good, so once it is, no member
        // can lack it.
        let metadata = if self.node.state().is_enabled(Feature::Metadata) {
            Some(MetadataSpec {
                created_by: creator(&request)?,
                ..MetadataSpec::default()
            })
        } else {
            None
        };
        let action = Action::CreateDatabase {
            name: name.clone(),
            replication_factor,
            engine,
            durability: Some(durability),
            metadata,
            // Drawn when it is submitted.
            incarnation: Incarnation::LEGACY,
        };
        self.create(name, replication_factor, action).await
    }

    /// Creates a database with a description, labels, or an expiry, recording the caller as its
    /// creator. A fleet with a member that lacks `metadata`, or `expiry` for an expiry, refuses
    /// it before it is logged (ADR-0041, point 3).
    async fn create_database_with_metadata(
        &self,
        request: Request<CreateDatabaseWithMetadataRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let database = request
            .get_ref()
            .database
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing database"))?;
        let name: DatabaseName = parse("database name", &database.database)?;
        require_database(&request, &name)?;
        let (name, replication_factor, engine, durability) = self.creation(database)?;
        let metadata = request.get_ref().metadata.clone().unwrap_or_default();
        let spec = MetadataSpec {
            description: description_from_proto(&metadata.description)?,
            labels: labels_from_proto(&metadata.labels)?,
            expires_at: spec_expiry_from_proto(&metadata)
                .map(|expiry| self.resolve_expiry(expiry))
                .transpose()?,
            created_by: creator(&request)?,
        };
        if !spec.is_explicit() {
            return Err(Status::invalid_argument(
                "the creation carries no description, labels, or expiry; use CreateDatabase for \
                 a database without metadata",
            ));
        }
        let action = Action::CreateDatabase {
            name: name.clone(),
            replication_factor,
            engine,
            durability: Some(durability),
            metadata: Some(spec),
            // Drawn when it is submitted.
            incarnation: Incarnation::LEGACY,
        };
        self.create(name, replication_factor, action).await
    }

    /// Patches a database's description, labels, or expiry, recording the caller as the
    /// description's author when the description changes, and answers with the metadata after
    /// it. A new expiry is resolved to a time before the update is logged.
    async fn update_database(
        &self,
        request: Request<UpdateDatabaseRequest>,
    ) -> Result<Response<UpdateDatabaseResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let (mut change, expiry) = change_from_proto(request.get_ref())?;
        change.expiry = self.expiry_change(expiry)?;
        let action = Action::UpdateDatabase {
            name: name.clone(),
            change,
            by: creator(&request)?,
            if_version: request.get_ref().if_version,
        };
        match self.submit(action).await? {
            (Applied::DatabaseUpdated { .. }, state) => {
                let metadata = state
                    .database(&name)
                    .and_then(|database| database.metadata())
                    .ok_or_else(|| Status::internal("the update left no metadata"))?;
                Ok(Response::new(UpdateDatabaseResponse {
                    metadata: Some(metadata_to_proto(metadata, Descriptions::Include)),
                }))
            }
            (other, _) => Err(unexpected(&other)),
        }
    }

    /// Creates a fork (ADR-0046): it needs `admin` on the target, `fork` on the source, and a new
    /// name for the target. The request is resolved before it is logged: the fleet's defaults
    /// for what it leaves unset, the caller as its requester, a relative expiry's time, this
    /// control node's `[fork]` limits, and an incarnation from the injected generator. A resend
    /// of a known operation is answered from control state and never logged (ADR-0046, point
    /// 5.2).
    async fn fork_database(
        &self,
        request: Request<ForkDatabaseRequest>,
    ) -> Result<Response<ForkOperationStatus>, Status> {
        let target: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &target)?;
        let source: DatabaseName = parse("source", &request.get_ref().source)?;
        authorized(&request)?.require_also(AuthAction::Fork, &source)?;
        check_new_name(&target)?;
        let requested_by = creator(&request)?;
        let request = request.into_inner();
        let operation = operation_from_proto(&request.operation_id)?;
        let replication_factor = self.replication_factor(request.replication_factor)?;
        let durability = self.durability(request.acks, request.min_replicas, replication_factor)?;
        let metadata = request.metadata.unwrap_or_default();
        let expiry = spec_expiry_from_proto(&metadata);
        let mut intent = ForkIntent::new(target, source, replication_factor);
        intent.expect_source_incarnation = request.expect_source_incarnation.map(Incarnation::new);
        intent.durability = Some(durability);
        intent.description = description_from_proto(&metadata.description)?;
        intent.labels = labels_from_proto(&metadata.labels)?;
        intent.expiry = match expiry {
            None => ForkExpiry::Never,
            Some(Expiry::At(at)) => ForkExpiry::At(at),
            Some(Expiry::In(span)) => ForkExpiry::After(Millis::new(
                u64::try_from(span.as_millis()).unwrap_or(u64::MAX),
            )),
        };
        intent.requested_by = requested_by;
        let state = self.node.state();
        let now = self.clock.now().max(state.now());
        // A resend resolves nothing again, so its expiry isn't held to the minimum again.
        let resolved_expiry = match expiry {
            Some(expiry) if state.fork_operation(&operation, now).is_none() => {
                let at = self.resolve_expiry(expiry)?;
                matches!(expiry, Expiry::In(_)).then_some(at)
            }
            _ => None,
        };
        let action = Action::ForkDatabase {
            operation,
            intent,
            // Drawn when it is submitted.
            incarnation: Incarnation::LEGACY,
            resolved_expiry,
            limits: self.fork_limits,
        };
        if let Some(resent) = state.fork_resend(&action, now) {
            let record = resent.map_err(|rejection| rejection_status(&rejection))?;
            return Ok(Response::new(
                self.status_of_operation(&state, operation, record)?,
            ));
        }
        let (_, state) = self.submit_drawing(action).await?;
        if self.fork_faults.take_reply_drop() {
            return Err(Status::unavailable("the reply was lost"));
        }
        let record = state
            .fork_operation(&operation, state.now())
            .ok_or_else(|| Status::internal("the fork left no operation record"))?;
        Ok(Response::new(
            self.status_of_operation(&state, operation, record)?,
        ))
    }

    /// A fork operation's status, to the principal that requested it, or to one with `admin` on
    /// its target while the target's name holds the operation's incarnation (ADR-0046, point
    /// 5.4). To anyone else, the operation is unknown: the answer is the `NOT_FOUND` an unknown
    /// operation gets, and the refusal is audited.
    async fn get_fork_operation(
        &self,
        request: Request<GetForkOperationRequest>,
    ) -> Result<Response<ForkOperationStatus>, Status> {
        let authorized = authorized(&request)?;
        let requester = creator(&request)?;
        let operation = operation_from_proto(&request.get_ref().operation_id)?;
        let state = self.node.state();
        let now = self.clock.now().max(state.now());
        let known = state.fork_operation(&operation, now);
        let visible = match authorized.caller() {
            Caller::Anonymous => known,
            Caller::Principal { .. } => known
                .filter(|record| requester.is_some() && record.intent().requested_by == requester)
                .or_else(|| {
                    state
                        .fork_operation_of_target(&operation, now)
                        .filter(|record| authorized.may(AuthAction::Admin, record.target()))
                }),
            Caller::Node(_) => None,
        };
        let Some(record) = visible else {
            if let Some(record) = known {
                authorized.audit_refused(AuthAction::Admin, record.target());
            }
            return Err(Status::not_found(format!(
                "fork operation {operation} is unknown"
            )));
        };
        Ok(Response::new(
            self.status_of_operation(&state, operation, record)?,
        ))
    }

    async fn drop_database(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        // The replicas as they were before the drop: the drop takes the database's reports.
        let before = self.node.state();
        match self.submit(Action::DropDatabase { name }).await? {
            (Applied::DatabaseDropped { database, epoch }, _) => {
                if let Some(notices) = &self.notices {
                    notices.send(
                        DropNotices::recipients(&before, &database),
                        &database,
                        epoch,
                    );
                }
                Ok(Response::new(DropDatabaseResponse { epoch: epoch.get() }))
            }
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn change_durability(
        &self,
        request: Request<ChangeDurabilityRequest>,
    ) -> Result<Response<ChangeDurabilityResponse>, Status> {
        let database: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &database)?;
        let request = request.into_inner();
        let replication_factor = self
            .node
            .state()
            .database(&database)
            .ok_or_else(|| {
                rejection_status(&Rejection::UnknownDatabase {
                    database: database.clone(),
                })
            })?
            .replication_factor();
        let acks = acks_from_proto(request.acks)
            .ok_or_else(|| Status::invalid_argument("an acks mode is required"))?;
        let min_replicas = u8::try_from(request.min_replicas).map_err(|_| {
            Status::invalid_argument(format!("invalid min_replicas {}", request.min_replicas))
        })?;
        let durability = if min_replicas == 0 {
            Durability::new(acks, replication_factor)
        } else {
            Durability::with_min_replicas(acks, min_replicas, replication_factor)
                .map_err(|error| Status::invalid_argument(error.to_string()))?
        };
        let action = Action::ChangeDurability {
            database,
            durability,
        };
        match self.submit(action).await? {
            (
                Applied::DurabilityChanged {
                    durability,
                    current,
                    ..
                },
                _,
            ) => Ok(Response::new(ChangeDurabilityResponse {
                acks: acks_to_proto(durability.acks()).into(),
                min_replicas: u32::from(durability.min_replicas()),
                current_acks: acks_to_proto(current.acks()).into(),
                current_min_replicas: u32::from(current.min_replicas()),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn locate(
        &self,
        request: Request<LocateRequest>,
    ) -> Result<Response<LocateResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let state = self.node.state();
        let mut placement = placement(&state, &name, Descriptions::Include, &self.progress)?;
        if self.fork_faults.hides_replica_sets() {
            // As a control node from before replica sets were shown (0.7) answers.
            placement.placed_replicas.clear();
            placement.membership_epoch = None;
        }
        Ok(Response::new(LocateResponse {
            placement: Some(placement),
        }))
    }

    /// Lists the databases the caller may read, in name order, and then only those the
    /// request's prefix, labels, and `after` admit, up to its limit. The filters apply after the
    /// scope's, so they never reveal a database, or a label, the caller may not read.
    async fn list_databases(
        &self,
        request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let caller = authorized(&request)?;
        let filter = request.get_ref();
        let requirements = filter
            .labels
            .iter()
            .map(|text| {
                text.parse::<LabelRequirement>().map_err(|error| {
                    Status::invalid_argument(format!("invalid label requirement '{text}': {error}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let descriptions = if filter.descriptions {
            Descriptions::Include
        } else {
            Descriptions::Omit
        };
        let limit = match filter.limit {
            0 => usize::MAX,
            limit => usize::try_from(limit).unwrap_or(usize::MAX),
        };
        let state = self.node.state();
        let mut admitted = state
            .databases()
            .filter(|database| caller.sees(AuthAction::Read, database.name()))
            .filter(|database| database.name().as_str().starts_with(&filter.prefix))
            .filter(|database| {
                let labels = database.metadata().map(|metadata| metadata.labels());
                requirements
                    .iter()
                    .all(|requirement| labels.is_some_and(|labels| requirement.matches(labels)))
            })
            .filter(|database| database.name().as_str() > filter.after.as_str());
        let databases = admitted
            .by_ref()
            .take(limit)
            .map(|database| placement(&state, database.name(), descriptions, &self.progress))
            .collect::<Result<Vec<_>, _>>()?;
        let next_after = match (admitted.next(), databases.last()) {
            (Some(_), Some(last)) => last.database.clone(),
            _ => String::new(),
        };
        Ok(Response::new(ListDatabasesResponse {
            databases,
            next_after,
        }))
    }

    /// Bound to `self`: FR-046's certificate check, then `apply`, which grants a lease only to a
    /// live, undraining member of the replica set (ADR-0031, point 2).
    async fn acquire_lease(
        &self,
        request: Request<AcquireLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        let peer = Peer::of(&request);
        let request = request.into_inner();
        let database = parse("database name", &request.database)?;
        let node = parse("node id", &request.node_id)?;
        peer.check_node(&node, "AcquireLease")?;
        match self.submit(Action::AcquireLease { database, node }).await? {
            (Applied::LeaseGranted { database, lease }, _) => {
                Ok(Response::new(lease_response(&database, &lease)))
            }
            (other, _) => Err(unexpected(&other)),
        }
    }

    /// Bound to `self`: FR-046's certificate check, then `apply`, which renews a lease only for
    /// its holder at its epoch (ADR-0031, point 2).
    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        let peer = Peer::of(&request);
        let request = request.into_inner();
        let database = parse("database name", &request.database)?;
        let node = parse("node id", &request.node_id)?;
        peer.check_node(&node, "RenewLease")?;
        let epoch = LeaseEpoch::new(request.epoch);
        let action = Action::RenewLease {
            database,
            node,
            epoch,
        };
        match self.submit(action).await? {
            (Applied::LeaseRenewed { database, lease }, _) => {
                Ok(Response::new(lease_response(&database, &lease)))
            }
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> Result<Response<LeaveResponse>, Status> {
        let id = parse("node id", &request.get_ref().node_id)?;
        Peer::of(&request).check_node(&id, "Leave")?;
        match self.submit(Action::Leave { id }).await? {
            (
                Applied::MemberLeft {
                    membership_epoch, ..
                },
                _,
            ) => Ok(Response::new(LeaveResponse {
                membership_epoch: membership_epoch.get(),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    /// Bound to `replica`: a node outside the database's replica set is refused.
    async fn start_promotion(
        &self,
        request: Request<StartPromotionRequest>,
    ) -> Result<Response<PromotionResponse>, Status> {
        let database = parse("database name", &request.get_ref().database)?;
        if let Some(node) = calling_node(&request)? {
            require_replica(&self.node.state(), "StartPromotion", &node, &database)?;
        }
        match self.submit(Action::StartPromotion { database }).await? {
            (
                Applied::PromotionStarted {
                    database,
                    epoch,
                    candidate,
                    ..
                },
                _,
            ) => Ok(Response::new(PromotionResponse {
                database: database.to_string(),
                epoch: epoch.get(),
                candidate: candidate.to_string(),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn record_fenced(
        &self,
        request: Request<RecordFencedRequest>,
    ) -> Result<Response<RecordFencedResponse>, Status> {
        // The node named here is the fenced replica, reported by the candidate that fenced it,
        // so it is no claim about the caller's own identity; the caller must be the candidate.
        let caller = calling_node(&request)?;
        let request = request.into_inner();
        let database = parse("database name", &request.database)?;
        let epoch = LeaseEpoch::new(request.epoch);
        if let Some(caller) = caller {
            require_candidate(
                &self.node.state(),
                "RecordFenced",
                &caller,
                &database,
                epoch,
            )?;
        }
        let action = Action::RecordFenced {
            database,
            node: parse("node id", &request.node_id)?,
            epoch,
            position: Position::new(request.position),
        };
        match self.submit(action).await? {
            (Applied::FenceRecorded { fenced, needed, .. }, _) => {
                Ok(Response::new(RecordFencedResponse {
                    fenced: u32::from(fenced),
                    needed: u32::from(needed),
                }))
            }
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn complete_promotion(
        &self,
        request: Request<CompletePromotionRequest>,
    ) -> Result<Response<CompletePromotionResponse>, Status> {
        let caller = calling_node(&request)?;
        let request = request.into_inner();
        let database = parse("database name", &request.database)?;
        let epoch = LeaseEpoch::new(request.epoch);
        if let Some(caller) = caller {
            require_candidate(
                &self.node.state(),
                "CompletePromotion",
                &caller,
                &database,
                epoch,
            )?;
        }
        let action = Action::CompletePromotion { database, epoch };
        match self.submit(action).await? {
            (
                Applied::PromotionCompleted {
                    database,
                    lease,
                    adopted,
                    ..
                },
                _,
            ) => Ok(Response::new(CompletePromotionResponse {
                lease: Some(lease_response(&database, &lease)),
                adopted: adopted.as_bytes().to_vec(),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn drain(
        &self,
        request: Request<DrainRequest>,
    ) -> Result<Response<DrainResponse>, Status> {
        let id = parse("node id", &request.get_ref().node_id)?;
        Peer::of(&request).check_node(&id, "Drain")?;
        match self.submit(Action::Drain { id }).await? {
            (Applied::MemberDraining { id }, state) => Ok(Response::new(DrainResponse {
                primaries: leases_held_by(&state, &id),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn undrain(
        &self,
        request: Request<UndrainRequest>,
    ) -> Result<Response<UndrainResponse>, Status> {
        let id = parse("node id", &request.get_ref().node_id)?;
        match self.submit(Action::Undrain { id }).await? {
            (Applied::MemberUndrained { .. }, _) => Ok(Response::new(UndrainResponse {})),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn transfer_lease(
        &self,
        request: Request<TransferLeaseRequest>,
    ) -> Result<Response<PromotionResponse>, Status> {
        let database: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &database)?;
        // Bound to `holder`: a node hands over only a lease it holds, as a draining node does.
        if let Some(node) = calling_node(&request)? {
            require_holder(&self.node.state(), "TransferLease", &node, &database)?;
        }
        let request = request.into_inner();
        let target = if request.target.is_empty() {
            None
        } else {
            Some(parse("target node id", &request.target)?)
        };
        match self
            .submit(Action::TransferLease { database, target })
            .await?
        {
            (
                Applied::PromotionStarted {
                    database,
                    epoch,
                    candidate,
                    ..
                },
                _,
            ) => Ok(Response::new(PromotionResponse {
                database: database.to_string(),
                epoch: epoch.get(),
                candidate: candidate.to_string(),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn revoke_token(
        &self,
        request: Request<RevokeTokenRequest>,
    ) -> Result<Response<RevokeTokenResponse>, Status> {
        let caller = revoke::who(&request);
        let revoker = revoke::Revoker::of(&request);
        let targets = revoke::Targets::from_request(request.into_inner());
        let (described, authority) =
            revoke::described(revoker.as_ref().ok(), targets.as_ref().ok());
        let result = match (revoker, targets) {
            (Ok(revoker), Ok(targets)) => self.revoke(&revoker, targets).await,
            (Err(status), _) | (_, Err(status)) => Err(status),
        };
        revoke::audit(&caller, &described, authority, &result);
        result.map(Response::new)
    }

    async fn delegate_token(
        &self,
        request: Request<DelegateTokenRequest>,
    ) -> Result<Response<DelegateTokenResponse>, Status> {
        let caller = Delegator::of(&request).inspect_err(|status| {
            let principal = match authorized(&request).map(Authorized::caller) {
                Ok(Caller::Principal { name, .. }) => name.to_string(),
                Ok(Caller::Node(id)) => format!("node:{id}"),
                Ok(Caller::Anonymous) | Err(_) => "anonymous".to_owned(),
            };
            tracing::warn!(
                target: AUDIT,
                principal,
                result = "refused",
                code = ?status.code(),
                reason = status.message(),
                rpc = "DelegateToken",
                "refused a delegation",
            );
        })?;
        let request = request.into_inner();
        let audited = AuditedRequest::of(&request);
        let result = self.delegate(&caller, request).await;
        audit_delegation(&caller, &audited, &result);
        result.map(|delegated| Response::new(delegated.into_response()))
    }

    async fn list_delegations(
        &self,
        request: Request<ListDelegationsRequest>,
    ) -> Result<Response<ListDelegationsResponse>, Status> {
        let request = request.into_inner();
        let recorded_by: Option<NodeId> = request
            .recorded_by
            .as_deref()
            .map(|text| parse("recorded_by", text))
            .transpose()?;
        let from = request.recorded_from_ms.map(Timestamp::from_millis);
        let to = request.recorded_to_ms.map(Timestamp::from_millis);
        let state = self.node.state();
        let now = self.clock.now().max(state.now());
        let delegations = state
            .delegations()
            .filter(|(_, record)| record.is_live(now, now))
            .filter(|(_, record)| {
                recorded_by
                    .as_ref()
                    .is_none_or(|by| record.recorded_by == *by)
            })
            .filter(|(_, record)| from.is_none_or(|from| record.recorded_at >= from))
            .filter(|(_, record)| to.is_none_or(|to| record.recorded_at <= to))
            .map(|(id, record)| delegation_record(id, record))
            .collect();
        Ok(Response::new(ListDelegationsResponse { delegations }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_name_under_the_reserved_prefix_is_refused() {
        for name in [
            "_fleetring",
            "_fleetring/identity",
            "_fleetring-x",
            "_fleetringdb",
        ] {
            let refused = check_new_name(&name.parse().expect("a name")).expect_err(name);
            assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{name}");
            assert!(refused.message().contains("reserved"), "{refused:?}");
        }
        for name in ["fleetring", "_fleet", "agent-42-_fleetring", "_fleetrin"] {
            assert!(
                check_new_name(&name.parse().expect("a name")).is_ok(),
                "{name}"
            );
        }
    }

    use std::time::Duration;

    use crate::clock::SimClock;
    use crate::control::node::{ControlNode, ControlOptions, StorageIo};
    use crate::control::test_support::{member_with_every_feature, root_delegation};
    use crate::tls::{Authority, Identity, IssuerFiles, IssuerPair, Validity};

    /// A control node with one member that reports every feature, and its service, which signs
    /// with an issuer pair the test's authority certified, on a clock the test advances.
    struct Fixture {
        _dir: tempfile::TempDir,
        node: ControlNode,
        clock: SimClock,
        service: ControlService,
        /// The ID of the authority key that certified the service's issuer pair.
        certifier: KeyId,
    }

    impl Fixture {
        async fn new() -> Self {
            let dir = tempfile::tempdir().expect("a directory");
            let clock = SimClock::new(Timestamp::from_millis(1_000_000));
            let options = ControlOptions::default()
                .with_storage_io(StorageIo::Inline)
                .with_recorder("control".parse().expect("an id"));
            let node = ControlNode::open(dir.path().join("control.db"), options, clock)
                .await
                .expect("opens");
            let handle = node.handle();
            handle
                .submit(Action::Join {
                    member: member_with_every_feature("data-1"),
                })
                .await
                .expect("the join commits");
            let year = Validity::new(Timestamp::ZERO, Duration::from_secs(365 * 24 * 3_600));
            let authority =
                Authority::create("prod".parse().expect("a fleet"), year).expect("an authority");
            let control: NodeId = "control".parse().expect("an id");
            let certificate = authority
                .issue_node(&control, &["127.0.0.1".to_owned()], year)
                .expect("a node certificate");
            let identity = Identity::from_issued(authority.certificate_pem(), &certificate)
                .expect("an identity");
            let issued = authority
                .issue_issuer(
                    &control,
                    Validity::new(Timestamp::ZERO, Duration::from_secs(30 * 24 * 3_600)),
                )
                .expect("an issuer pair");
            let files = IssuerFiles {
                cert: dir.path().join("issuer.crt"),
                key: dir.path().join("issuer.key"),
            };
            std::fs::write(&files.cert, issued.certificate_pem()).expect("written");
            std::fs::write(&files.key, issued.key_pem()).expect("written");
            let pair = IssuerPair::load(&files, &identity, Arc::new(clock)).expect("loads");
            let certifier = pair
                .signer()
                .expect("a signer")
                .certificate()
                .authority_key_id()
                .clone();
            let service = ControlService::new(handle)
                .with_clock(Arc::new(clock))
                .with_issuer(pair);
            Self {
                _dir: dir,
                node,
                clock,
                service,
                certifier,
            }
        }

        async fn submit(&self, action: Action) -> Applied {
            self.node
                .handle()
                .submit(action)
                .await
                .expect("commits")
                .into_outcome()
                .expect("applied")
        }

        async fn revoke(&self, key: &str) {
            self.submit(Action::RevokeToken {
                revoked: Revoked::Key(key.parse().expect("a key id")),
                until: Timestamp::MAX,
            })
            .await;
        }
    }

    /// `ListDelegations` returns live records only: one kept past its token's `exp`, as every
    /// record is for an hour and a half, is left out (ADR-0047, point 6.4).
    #[tokio::test(start_paused = true)]
    async fn list_delegations_leaves_out_records_kept_past_their_tokens() {
        let fixture = Fixture::new().await;
        fixture
            .submit(root_delegation(fixture.clock.now(), 1, 60_000))
            .await;
        let listed = || async {
            Control::list_delegations(
                &fixture.service,
                Request::new(ListDelegationsRequest::default()),
            )
            .await
            .expect("lists")
            .into_inner()
            .delegations
            .len()
        };
        assert_eq!(listed().await, 1);
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert_eq!(
            fixture.node.handle().state().delegations().len(),
            1,
            "kept past its token"
        );
        assert_eq!(listed().await, 0);
        fixture.node.shutdown().await;
    }

    /// The control node never signs with an issuer pair whose certifying authority key is
    /// revoked, since every node refuses the tokens it would sign (ADR-0047, point 1.6).
    #[tokio::test(start_paused = true)]
    async fn issuer_certified_by_a_revoked_authority_key_is_failed_precondition() {
        let fixture = Fixture::new().await;
        let now = fixture.clock.now();
        assert!(
            fixture
                .service
                .issuer_signer(&fixture.node.handle().state(), now)
                .is_ok()
        );
        fixture.revoke(&fixture.certifier.to_string()).await;
        match fixture
            .service
            .issuer_signer(&fixture.node.handle().state(), now)
        {
            Ok(_) => panic!("signed with a pair whose authority key is revoked"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
                assert!(
                    status
                        .message()
                        .contains("was certified by a revoked authority key"),
                    "{status:?}"
                );
            }
        }
        fixture.node.shutdown().await;
    }

    /// The handler checks the caller against the denylist in control state, not only the one
    /// the request boundary read: a caller the boundary admitted before a revocation reached it
    /// is refused, here by the authority key that certified its copy's issuer, which only the
    /// handler knows and `apply` never sees (ADR-0047, point 5.9).
    #[tokio::test(start_paused = true)]
    async fn caller_revoked_after_the_boundary_admitted_it_is_refused() {
        use crate::control::test_support::delegation_from_root;

        let fixture = Fixture::new().await;
        let now = fixture.clock.now();
        let Applied::DelegationRecorded { claims, .. } = fixture
            .submit(delegation_from_root(
                now,
                1,
                &["admin:agent-42.c7.*", "grant:agent-42.c7.*"],
                3_600_000,
            ))
            .await
        else {
            panic!("expected a delegation");
        };
        let caller = Delegator {
            name: AgentName::try_from(claims.principal.clone()).expect("an agent"),
            scopes: claims.scopes.clone(),
            claims: Arc::new(claims),
            key: "issuer-c7".parse().expect("a key id"),
            signer: Signer::Issuer {
                name: "control-old".parse().expect("an id"),
                authority: "other-authority".parse().expect("a key id"),
            },
        };
        let request = |operation: u8| DelegateTokenRequest {
            operation_id: vec![operation; 16],
            segment: "r1".to_owned(),
            ..DelegateTokenRequest::default()
        };
        assert!(fixture.service.delegate(&caller, request(8)).await.is_ok());
        fixture.revoke("other-authority").await;
        match fixture.service.delegate(&caller, request(9)).await {
            Ok(_) => panic!("delegated from a revoked caller"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
                assert!(status.message().contains("other-authority"), "{status:?}");
            }
        }
        assert_eq!(fixture.node.handle().state().delegations().len(), 2);
        fixture.node.shutdown().await;
    }

    /// `agent-42.c7`, recorded, presenting a copy whose issuer `other-authority` certified, as
    /// the request boundary would have admitted it under the ancestor rule.
    async fn recorded_ancestor(fixture: &Fixture) -> revoke::AncestorCaller {
        use crate::control::test_support::delegation_from_root;

        let Applied::DelegationRecorded { claims, .. } = fixture
            .submit(delegation_from_root(
                fixture.clock.now(),
                1,
                &["admin:agent-42.c7.*", "grant:agent-42.c7.*"],
                3_600_000,
            ))
            .await
        else {
            panic!("expected a delegation");
        };
        revoke::AncestorCaller {
            claims: Arc::new(claims),
            key: "issuer-c7".parse().expect("a key id"),
            signer: Signer::Issuer {
                name: "control-old".parse().expect("an id"),
                authority: "other-authority".parse().expect("a key id"),
            },
            carriage: crate::auth::Carriage::Direct,
        }
    }

    /// A request revoking the token with ID `id`.
    fn own_token(id: &fleetring_control::TokenId) -> revoke::Targets {
        use fleetring_proto::v1::{Revocation, RevokeTokenRequest, revocation};

        revoke::Targets::from_request(RevokeTokenRequest {
            revocation: Some(Revocation {
                revoked: Some(revocation::Revoked::TokenId(id.to_string())),
                until_ms: 0,
            }),
            principal: String::new(),
        })
        .expect("a valid request")
    }

    /// The revocation handler, like `delegate`, checks the caller against the denylist in
    /// control state, which here revokes the authority key that certified its copy's issuer:
    /// only the handler knows that key, and `apply` would accept the revocation.
    #[tokio::test(start_paused = true)]
    async fn ancestor_caller_revoked_after_the_boundary_admitted_it_is_refused() {
        let fixture = Fixture::new().await;
        let caller = recorded_ancestor(&fixture).await;
        let id = caller.claims.id.clone();
        fixture.revoke("other-authority").await;
        match fixture
            .service
            .revoke(&revoke::Revoker::Ancestor(caller), own_token(&id))
            .await
        {
            Ok(response) => panic!("a revoked caller revoked: {response:?}"),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
                assert!(status.message().contains("other-authority"), "{status:?}");
            }
        }
        assert!(
            !fixture
                .node
                .handle()
                .state()
                .revocations()
                .contains(&Revoked::Token(id))
        );
        fixture.node.shutdown().await;
    }
}
