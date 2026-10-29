//! The gRPC `Control` service over the control node (ADR-0017, ADR-0019).

use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use fleetring::{ReplicationFactor, Weight};
use fleetring_control::{
    Acks, Action, Applied, DatabaseName, Durability, Engine, HostedReplica, Lease, LeaseEpoch,
    Liveness, Member, Position, Rejection, Role, State,
};
use fleetring_proto::v1::control_server::Control;
use fleetring_proto::v1::{
    self as proto, AcquireLeaseRequest, ChangeDurabilityRequest, ChangeDurabilityResponse,
    CompletePromotionRequest, CompletePromotionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, DrainRequest, DrainResponse, DropDatabaseRequest, DropDatabaseResponse,
    HeartbeatRequest, HeartbeatResponse, JoinRequest, JoinResponse, LeaseResponse, LeaveRequest,
    LeaveResponse, ListDatabasesRequest, ListDatabasesResponse, LocateRequest, LocateResponse,
    MembershipUpdate, Placement, PromotionResponse, RecordFencedRequest, RecordFencedResponse,
    RenewLeaseRequest, StartPromotionRequest, TransferLeaseRequest, WatchMembershipRequest,
};
use fleetring_proto::{PROTOCOL_VERSION, check_protocol};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::control::node::{ControlHandle, SubmitError};
use crate::data::convert::{acks_from_proto, acks_to_proto};

/// The stream `WatchMembership` returns.
pub type MembershipStream = Pin<Box<dyn Stream<Item = Result<MembershipUpdate, Status>> + Send>>;

/// The durability a database is created with when its creation names none: the control node's
/// `[replication] acks` and `min_replicas` (ADR-0023).
///
/// A default floor that doesn't fit a database's replication factor is clamped into W ≤ floor
/// ≤ R, since it was set for the cluster rather than for that database; an explicit floor in a
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

/// The `Control` service: every mutation goes through the control node's single writer, and
/// reads come from its published state.
#[derive(Debug, Clone)]
pub struct ControlService {
    node: ControlHandle,
    defaults: DurabilityDefaults,
}

impl ControlService {
    /// Serves the control node behind `node`, creating databases with the default durability
    /// unless a creation says otherwise.
    #[must_use]
    pub fn new(node: ControlHandle) -> Self {
        Self {
            node,
            defaults: DurabilityDefaults::new(),
        }
    }

    /// Creates databases whose creation names no durability with `defaults`.
    #[must_use]
    pub fn with_durability_defaults(mut self, defaults: DurabilityDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// The durability a creation asks for: `acks` and `min_replicas` as the request carries
    /// them, with the cluster's defaults for what it leaves unset.
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
///   would make an invalid ring, or durability settings that don't fit the replication factor.
/// - `FAILED_PRECONDITION`: anything that depends on the current state: liveness, leases, epochs,
///   promotions, and exhausted counters.
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
        | Rejection::InvalidDurability { .. } => Status::invalid_argument(message),
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
    let role = match proto::Role::try_from(status.role) {
        Ok(proto::Role::Primary) => Role::Primary,
        Ok(proto::Role::Follower) => Role::Follower,
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

fn role_to_proto(role: Role) -> proto::Role {
    match role {
        Role::Primary => proto::Role::Primary,
        Role::Follower => proto::Role::Follower,
        _ => proto::Role::Unspecified,
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
/// older epoch than the database's, and nobody holds the new one yet.
fn placement(state: &State, name: &DatabaseName) -> Result<Placement, Status> {
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
                    .map_or(proto::Role::Unspecified, |report| {
                        role_to_proto(report.role())
                    })
                    .into(),
                position: report.map(|report| report.position().as_bytes().to_vec()),
            }
        })
        .collect();
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
    })
}

/// What a membership update says about a member besides its attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemberFlags {
    live: bool,
    draining: bool,
}

/// Whether each member is live as of the state's time, and whether it is draining, in ID order.
fn member_flags(state: &State) -> Vec<MemberFlags> {
    let now = state.now();
    state
        .members()
        .map(|member| MemberFlags {
            live: state.liveness(member.id(), now) == Some(Liveness::Live),
            draining: state.is_draining(member.id()),
        })
        .collect()
}

/// The membership as of the state's time.
fn membership_update(state: &State, flags: &[MemberFlags]) -> MembershipUpdate {
    MembershipUpdate {
        membership_epoch: state.membership_epoch().get(),
        members: state
            .members()
            .zip(flags)
            .map(|(member, flags)| proto::Member {
                node: Some(node_to_proto(member)),
                live: flags.live,
                draining: flags.draining,
            })
            .collect(),
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

#[tonic::async_trait]
impl Control for ControlService {
    type WatchMembershipStream = MembershipStream;

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol.as_ref())?;
        let member = member_from(request.node)?;
        match self.submit(Action::Join { member }).await? {
            (
                Applied::MemberJoined {
                    membership_epoch, ..
                },
                _,
            ) => Ok(Response::new(JoinResponse {
                protocol: Some(PROTOCOL_VERSION),
                membership_epoch: membership_epoch.get(),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let id = parse("node id", &request.node_id)?;
        let replicas = request
            .databases
            .into_iter()
            .map(replica_from)
            .collect::<Result<Vec<_>, _>>()?;
        let (applied, state) = self
            .submit(Action::Heartbeat {
                id,
                replicas,
                unknown_reports: fleetring_control::UnknownReports::Skip,
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
        }))
    }

    /// Sends the membership once its epoch passes `after_epoch`, then again whenever the epoch
    /// grows or a member's liveness or draining changes. Liveness is judged at the time of the
    /// last applied command, so it changes only as commands, such as other members' heartbeats,
    /// arrive.
    async fn watch_membership(
        &self,
        request: Request<WatchMembershipRequest>,
    ) -> Result<Response<Self::WatchMembershipStream>, Status> {
        let after_epoch = request.into_inner().after_epoch;
        // The epoch and flags the watcher holds: none until the first state is seen.
        let mut held: Option<(u64, Vec<MemberFlags>)> = None;
        let updates = WatchStream::new(self.node.watch()).filter_map(move |state| {
            let epoch = state.membership_epoch().get();
            let flags = member_flags(&state);
            // Epochs only grow, and members change only with the epoch, so a higher epoch or new
            // flags are a change the watcher hasn't seen.
            let send = match &held {
                None => epoch > after_epoch,
                Some((held_epoch, held_flags)) => {
                    epoch > (*held_epoch).max(after_epoch) || flags != *held_flags
                }
            };
            let update = send.then(|| Ok(membership_update(&state, &flags)));
            held = Some((epoch, flags));
            update
        });
        Ok(Response::new(Box::pin(updates)))
    }

    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let request = request.into_inner();
        let name: DatabaseName = parse("database name", &request.database)?;
        let replication_factor = u8::try_from(request.replication_factor)
            .ok()
            .and_then(ReplicationFactor::new)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "invalid replication factor {}: expected 1 to 255",
                    request.replication_factor
                ))
            })?;
        let engine = engine_from(request.engine)?;
        let durability = self.durability(request.acks, request.min_replicas, replication_factor)?;
        let action = Action::CreateDatabase {
            name: name.clone(),
            replication_factor,
            engine,
            durability: Some(durability),
        };
        match self.submit(action).await? {
            (Applied::DatabaseCreated { .. }, state) => Ok(Response::new(CreateDatabaseResponse {
                placement: Some(placement(&state, &name)?),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn drop_database(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        match self.submit(Action::DropDatabase { name }).await? {
            (Applied::DatabaseDropped { epoch, .. }, _) => {
                Ok(Response::new(DropDatabaseResponse { epoch: epoch.get() }))
            }
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn change_durability(
        &self,
        request: Request<ChangeDurabilityRequest>,
    ) -> Result<Response<ChangeDurabilityResponse>, Status> {
        let request = request.into_inner();
        let database: DatabaseName = parse("database name", &request.database)?;
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
        let state = self.node.state();
        Ok(Response::new(LocateResponse {
            placement: Some(placement(&state, &name)?),
        }))
    }

    async fn list_databases(
        &self,
        _request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let state = self.node.state();
        let databases = state
            .databases()
            .map(|database| placement(&state, database.name()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(ListDatabasesResponse { databases }))
    }

    async fn acquire_lease(
        &self,
        request: Request<AcquireLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        let request = request.into_inner();
        let database = parse("database name", &request.database)?;
        let node = parse("node id", &request.node_id)?;
        match self.submit(Action::AcquireLease { database, node }).await? {
            (Applied::LeaseGranted { database, lease }, _) => {
                Ok(Response::new(lease_response(&database, &lease)))
            }
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        let request = request.into_inner();
        let database = parse("database name", &request.database)?;
        let node = parse("node id", &request.node_id)?;
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

    async fn start_promotion(
        &self,
        request: Request<StartPromotionRequest>,
    ) -> Result<Response<PromotionResponse>, Status> {
        let database = parse("database name", &request.get_ref().database)?;
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
        let request = request.into_inner();
        let action = Action::RecordFenced {
            database: parse("database name", &request.database)?,
            node: parse("node id", &request.node_id)?,
            epoch: LeaseEpoch::new(request.epoch),
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
        let request = request.into_inner();
        let action = Action::CompletePromotion {
            database: parse("database name", &request.database)?,
            epoch: LeaseEpoch::new(request.epoch),
        };
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
        match self.submit(Action::Drain { id }).await? {
            (Applied::MemberDraining { id }, state) => Ok(Response::new(DrainResponse {
                primaries: leases_held_by(&state, &id),
            })),
            (other, _) => Err(unexpected(&other)),
        }
    }

    async fn transfer_lease(
        &self,
        request: Request<TransferLeaseRequest>,
    ) -> Result<Response<PromotionResponse>, Status> {
        let request = request.into_inner();
        let database = parse("database name", &request.database)?;
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
}
