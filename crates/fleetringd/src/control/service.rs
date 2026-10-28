//! The gRPC `Control` service over the control node (ADR-0017, ADR-0019).

use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use fleetring::{ReplicationFactor, Weight};
use fleetring_control::{
    Action, Applied, DatabaseName, Engine, HostedReplica, Lease, LeaseEpoch, Liveness, Member,
    Position, Rejection, Role, State,
};
use fleetring_proto::v1::control_server::Control;
use fleetring_proto::v1::{
    self as proto, AcquireLeaseRequest, CreateDatabaseRequest, CreateDatabaseResponse,
    HeartbeatRequest, HeartbeatResponse, JoinRequest, JoinResponse, LeaseResponse, LocateRequest,
    LocateResponse, MembershipUpdate, Placement, RenewLeaseRequest, WatchMembershipRequest,
};
use fleetring_proto::{PROTOCOL_VERSION, check_protocol};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::control::node::{ControlHandle, SubmitError};

/// The stream `WatchMembership` returns.
pub type MembershipStream = Pin<Box<dyn Stream<Item = Result<MembershipUpdate, Status>> + Send>>;

/// The `Control` service: every mutation goes through the control node's single writer, and
/// reads come from its published state.
#[derive(Debug, Clone)]
pub struct ControlService {
    node: ControlHandle,
}

impl ControlService {
    /// Serves the control node behind `node`.
    #[must_use]
    pub fn new(node: ControlHandle) -> Self {
        Self { node }
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
/// - `NOT_FOUND`: an unknown member or database.
/// - `ALREADY_EXISTS`: the database exists, or the member is already fenced.
/// - `INVALID_ARGUMENT`: the request itself is malformed: a duplicate report, or a join that
///   would make an invalid ring.
/// - `FAILED_PRECONDITION`: anything that depends on the current state: liveness, leases, epochs,
///   promotions, and exhausted counters.
#[must_use]
pub fn rejection_status(rejection: &Rejection) -> Status {
    let message = rejection.to_string();
    match rejection {
        Rejection::UnknownMember { .. } | Rejection::UnknownDatabase { .. } => {
            Status::not_found(message)
        }
        Rejection::DatabaseExists { .. } | Rejection::AlreadyFenced { .. } => {
            Status::already_exists(message)
        }
        Rejection::InvalidTopology { .. } | Rejection::DuplicateReport { .. } => {
            Status::invalid_argument(message)
        }
        _ => Status::failed_precondition(message),
    }
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

fn member_from(node: Option<proto::Node>) -> Result<Member, Status> {
    let node = node.ok_or_else(|| Status::invalid_argument("missing node"))?;
    let mut member = Member::new(parse("node id", &node.id)?);
    if let Some(zone) = &node.zone {
        member = member.with_zone(parse("zone", zone)?);
    }
    if let Some(host) = &node.host {
        member = member.with_host(parse("host", host)?);
    }
    // Proto3 can't tell an unset weight from 0, so 0 means the default weight.
    Ok(member.with_weight(Weight::new(node.weight).unwrap_or(Weight::ONE)))
}

fn node_to_proto(member: &Member) -> proto::Node {
    proto::Node {
        id: member.id().to_string(),
        zone: member.zone().map(ToString::to_string),
        host: member.host().map(ToString::to_string),
        weight: member.weight().get(),
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
    ))
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

/// Where a database lives: its lease holder as primary, and the rest of its replica set.
fn placement(state: &State, name: &DatabaseName) -> Result<Placement, Status> {
    let database = state.database(name).ok_or_else(|| {
        rejection_status(&Rejection::UnknownDatabase {
            database: name.clone(),
        })
    })?;
    let replicas = state
        .replica_set(name)
        .map_err(|rejection| rejection_status(&rejection))?;
    let primary = database.lease().map(Lease::holder);
    let followers = replicas
        .iter()
        .filter(|id| Some(*id) != primary)
        .map(ToString::to_string)
        .collect();
    Ok(Placement {
        database: name.to_string(),
        engine: engine_to_proto(database.engine()).into(),
        epoch: database.epoch().get(),
        primary: primary.map(ToString::to_string),
        followers,
    })
}

/// The membership as of the state's time.
fn membership_update(state: &State) -> MembershipUpdate {
    let now = state.now();
    MembershipUpdate {
        membership_epoch: state.membership_epoch().get(),
        members: state
            .members()
            .map(|member| proto::Member {
                node: Some(node_to_proto(member)),
                live: state.liveness(member.id(), now) == Some(Liveness::Live),
            })
            .collect(),
    }
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
        let (_, state) = self.submit(Action::Heartbeat { id, replicas }).await?;
        Ok(Response::new(HeartbeatResponse {
            membership_epoch: state.membership_epoch().get(),
        }))
    }

    async fn watch_membership(
        &self,
        request: Request<WatchMembershipRequest>,
    ) -> Result<Response<Self::WatchMembershipStream>, Status> {
        let mut last = request.into_inner().after_epoch;
        let updates = WatchStream::new(self.node.watch()).filter_map(move |state| {
            let epoch = state.membership_epoch().get();
            // Epochs only grow, so a higher one is a change the watcher hasn't seen.
            (epoch > last).then(|| {
                last = epoch;
                Ok(membership_update(&state))
            })
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
        let action = Action::CreateDatabase {
            name: name.clone(),
            replication_factor,
            engine,
        };
        match self.submit(action).await? {
            (Applied::DatabaseCreated { .. }, state) => Ok(Response::new(CreateDatabaseResponse {
                placement: Some(placement(&state, &name)?),
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
}
