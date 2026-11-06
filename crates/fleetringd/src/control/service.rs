//! The gRPC `Control` service over the control node (ADR-0017, ADR-0019).

use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use fleetring::{NodeId, ReplicationFactor, Weight};
use fleetring_control::{
    Acks, Action, Applied, DatabaseName, Durability, Engine, ExpiryChange, Feature, HostedReplica,
    Lease, LeaseEpoch, Liveness, Member, MetadataSpec, Millis, Position, PrincipalName, Rejection,
    ReplicaRole, Revoked, Software, State, Timestamp,
};
use fleetring_proto::v1::control_server::Control;
use fleetring_proto::v1::{
    self as proto, AcquireLeaseRequest, ChangeDurabilityRequest, ChangeDurabilityResponse,
    CompletePromotionRequest, CompletePromotionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateDatabaseWithMetadataRequest, DrainRequest, DrainResponse,
    DropDatabaseRequest, DropDatabaseResponse, HeartbeatRequest, HeartbeatResponse, JoinRequest,
    JoinResponse, LeaseResponse, LeaveRequest, LeaveResponse, ListDatabasesRequest,
    ListDatabasesResponse, LocateRequest, LocateResponse, MembershipUpdate, Placement,
    PromotionResponse, RecordFencedRequest, RecordFencedResponse, RenewLeaseRequest,
    RevokeTokenRequest, RevokeTokenResponse, StartPromotionRequest, TransferLeaseRequest,
    UndrainRequest, UndrainResponse, UpdateDatabaseRequest, UpdateDatabaseResponse,
    WatchMembershipRequest,
};
use fleetring_proto::{PROTOCOL_VERSION, check_protocol};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::auth::{
    Action as AuthAction, BindingDenied, Caller, Unbound, authorized, require_database,
};
use crate::auth::{MAX_TOKEN_LIFETIME, NOT_BEFORE_LEEWAY_SECS};
use crate::clock::{Clock, SystemClock};
use crate::control::node::{ControlHandle, SubmitError};
use crate::control::notices::DropNotices;
use crate::data::convert::{
    acks_from_proto, acks_to_proto, change_from_proto, description_from_proto, labels_from_proto,
    metadata_to_proto, revocation_from_proto, revocations_to_proto, spec_expiry_from_proto,
};
use crate::data::{Descriptions, Expiry, ExpiryRequest, LabelRequirement, is_invisible};
use crate::dev::DevMode;
use crate::environment::Network;
use crate::software::{software_from_proto, software_to_proto};
use crate::tls::Peer;

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
        }
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
        match self.submit(action).await? {
            (Applied::DatabaseCreated { .. }, state) => {
                let placement = placement(&state, &name, Descriptions::Include)?;
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
}

/// Maps a rejection to the gRPC status a caller receives, with the rejection's message.
///
/// - `NOT_FOUND`: an unknown member or database. An unknown database also names itself in the
///   [`MISSING_DATABASE_KEY`] metadata, so that a caller can tell the two apart
///   ([`missing_database`]).
/// - `ALREADY_EXISTS`: the database exists, or the member is already fenced.
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
        Rejection::UnknownDatabase { database } => {
            let mut status = Status::not_found(message);
            if let Ok(value) = MetadataValue::try_from(database.as_str()) {
                status.metadata_mut().insert(MISSING_DATABASE_KEY, value);
            }
            status
        }
        Rejection::DatabaseExists { .. } | Rejection::AlreadyFenced { .. } => {
            Status::already_exists(message)
        }
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

fn replica_from(status: proto::DatabaseStatus) -> Result<HostedReplica, Status> {
    let role = match proto::ReplicaRole::try_from(status.role) {
        Ok(proto::ReplicaRole::Primary) => ReplicaRole::Primary,
        Ok(proto::ReplicaRole::Follower) => ReplicaRole::Follower,
        _ => {
            return Err(Status::invalid_argument(format!(
                "database '{}' has no valid role",
                status.database
            )));
        }
    };
    Ok(HostedReplica::new(
        parse("database name", &status.database)?,
        role,
        Position::new(status.position),
    )
    .with_log_epoch(LeaseEpoch::new(status.epoch)))
}

fn engine_from(engine: i32) -> Result<Engine, Status> {
    match proto::Engine::try_from(engine) {
        // Proto3 sends an unset engine as unspecified; SQLite is the only engine so far.
        Ok(proto::Engine::Unspecified | proto::Engine::Sqlite) => Ok(Engine::Sqlite),
        Err(_) => Err(Status::invalid_argument(format!("unknown engine {engine}"))),
    }
}

fn role_to_proto(role: ReplicaRole) -> proto::ReplicaRole {
    match role {
        ReplicaRole::Primary => proto::ReplicaRole::Primary,
        ReplicaRole::Follower => proto::ReplicaRole::Follower,
        _ => proto::ReplicaRole::Unspecified,
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
/// `descriptions` says.
fn placement(
    state: &State,
    name: &DatabaseName,
    descriptions: Descriptions,
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
    })
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
        let id = parse("node id", &request.node_id)?;
        peer.check_node(&id, "Heartbeat")?;
        let replicas = request
            .databases
            .into_iter()
            .map(replica_from)
            .collect::<Result<Vec<_>, _>>()?;
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
                id,
                replicas,
                unknown_reports: fleetring_control::UnknownReports::Skip,
                software,
            })
            .await?;
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
        Ok(Response::new(LocateResponse {
            placement: Some(placement(&state, &name, Descriptions::Include)?),
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
            .map(|database| placement(&state, database.name(), descriptions))
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
        let revocation = request
            .into_inner()
            .revocation
            .ok_or_else(|| Status::invalid_argument("missing revocation"))?;
        let (revoked, until) = revocation_from_proto(revocation)?;
        // No token outlives this, so keeping a token's entry longer only grows the list. A key's
        // entry may last forever.
        let until = match revoked {
            Revoked::Token(_) => until.min(
                self.clock.now().saturating_add(Millis::new(
                    u64::try_from(MAX_TOKEN_LIFETIME.as_millis())
                        .unwrap_or(u64::MAX)
                        .saturating_add(NOT_BEFORE_LEEWAY_SECS * 1_000),
                )),
            ),
            Revoked::Key(_) => until,
        };
        match self.submit(Action::RevokeToken { revoked, until }).await? {
            (Applied::TokenRevoked { revoked, until }, _) => {
                tracing::info!(%revoked, until = until.as_millis(), "revoked");
                Ok(Response::new(RevokeTokenResponse {
                    until_ms: until.as_millis(),
                }))
            }
            (other, _) => Err(unexpected(&other)),
        }
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
}
