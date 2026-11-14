//! A simulated client: runs a workload and records every operation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{Action, DatabaseName, Incarnation, LeaseEpoch};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::{
    AcquireLeaseRequest, CreateDatabaseRequest, DelegateTokenRequest, DelegateTokenResponse,
    Engine, FenceRequest, HeartbeatRequest, JoinRequest, LeaseResponse, LocateRequest,
    LocateResponse, Node, RenewLeaseRequest, ReplicateMessage, ReplicateOpen, Revocation,
    RevokeTokenRequest, SnapshotRequest, replicate_message, revocation,
};

use crate::auth::refused_binding;
use crate::control::ControlClient;
use crate::control::fork::replica_to_proto;
use crate::control::rejection_status;
use crate::data::{Client, DatabaseSpec, Expiry, ForkSpec, LeaseUpdate, NewMetadata};
use crate::engine::{Statement, Value};
use crate::environment::Network;
use crate::replication::LeaseChange;
use crate::sim::SimRng;
use crate::sim::fleet::{PORT, certificate_validity};
use crate::sim::history::DurabilityMode;
use crate::sim::host::{Shared, WorkloadState};
use crate::sim::trace::EventKind;
use crate::sim::workload::{
    Ack, LeaseObservation, LeaseSeen, Operation, Outcome, Step, WRITES_TABLE, Workload,
};
use crate::software::{running, software_to_proto};
use crate::tls::{Authority, Identity, Security};
use crate::transport::{Backoff, replication_client};

/// How long a lease watcher waits before it tries a node again after a stream failed to open or
/// ended.
const WATCH_RETRY: Duration = Duration::from_millis(50);

/// How long a lease watcher lets a stream stay silent before it opens a replacement: a crashed
/// host's stream stalls rather than ends, and the leases the replacement starts with catch the watcher up.
const WATCH_IDLE: Duration = Duration::from_secs(1);

/// Retries for simulated clients: quick, because simulated calls fail fast.
const CLIENT_BACKOFF: Backoff = Backoff {
    initial: Duration::from_millis(10),
    max: Duration::from_millis(100),
    attempts: 3,
};

/// A router a client sends its writes and reads through, with its token (ADR-0037).
#[derive(Debug, Clone)]
pub(super) struct Via {
    /// The router's `host:port`.
    pub(super) address: String,
    /// The token the client's requests carry.
    pub(super) token: String,
}

/// What a client needs besides its workload.
pub(super) struct Session {
    pub(super) name: String,
    pub(super) control: String,
    /// The data node database calls go to first, if not the primary the control plane names.
    pub(super) entry: Option<String>,
    /// The router writes and reads go through, if they go through one.
    pub(super) via: Option<Via>,
    pub(super) timeout: Duration,
    pub(super) shared: Shared,
    /// The principal the client acts as, issued by the fleet's test authority.
    pub(super) identity: Identity,
    /// The fleet's test authority, which issues a node certificate for each node the
    /// workload acts as: node calls need one (ADR-0025).
    pub(super) authority: Arc<Authority>,
}

/// A client: runs `workload`, records each operation, and finishes when the workload does.
pub(super) async fn run_client(
    session: Session,
    mut workload: Box<dyn Workload>,
    mut rng: SimRng,
) -> turmoil::Result {
    let Session {
        name,
        control,
        entry,
        via,
        timeout,
        shared,
        identity,
        authority,
    } = session;
    let mut nodes = NodeClients {
        authority,
        control: control.clone(),
        clients: BTreeMap::new(),
    };
    let network = Network::simulated().with_security(Security::Tls(identity));
    let client = Client::new(control, network.clone()).with_backoff(CLIENT_BACKOFF);
    let client = match entry {
        Some(entry) => client.with_entry(entry),
        None => client,
    };
    loop {
        match workload.next(&mut rng) {
            Step::Run(operation) => {
                let invoked_at = turmoil::sim_elapsed().unwrap_or_default();
                let id = shared
                    .lock()
                    .history
                    .begin(name.as_str(), invoked_at, operation.clone());
                let node = nodes.for_operation(&operation);
                let outcome = match tokio::time::timeout(
                    timeout,
                    execute(
                        &client,
                        node.as_ref(),
                        &nodes,
                        &network,
                        &shared,
                        via.as_ref(),
                        &operation,
                    ),
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        // The connection may have stalled; dial a new one next time.
                        client.reconnect();
                        nodes.clients.clear();
                        Outcome::Indeterminate { code: None }
                    }
                };
                let completed_at = turmoil::sim_elapsed().unwrap_or_default();
                workload.observe(&operation, &outcome);
                shared.record(
                    &name,
                    EventKind::Operation {
                        operation: operation.clone(),
                        outcome: outcome.clone(),
                    },
                );
                shared.lock().history.complete(id, completed_at, outcome);
            }
            Step::Sleep(duration) => tokio::time::sleep(duration).await,
            Step::Done => break,
        }
    }
    shared.record(&name, EventKind::WorkloadDone);
    shared.lock().workloads.insert(name, WorkloadState::Done);
    Ok(())
}

/// One attempt per call: a workload's resends are operations of their own, in the history.
const ONE_ATTEMPT: Backoff = Backoff {
    initial: Duration::from_millis(10),
    max: Duration::from_millis(10),
    attempts: 1,
};

/// What a `DelegateToken` reply acknowledges.
fn delegated(response: DelegateTokenResponse) -> Result<Ack, tonic::Status> {
    let invalid = |what: &str| tonic::Status::internal(format!("the reply's {what} is invalid"));
    Ok(Ack::Delegated {
        delegation: response
            .delegation_id
            .parse()
            .map_err(|_| invalid("delegation ID"))?,
        principal: response.principal,
        scopes: response.scopes,
        expires_at: fleetring_control::Timestamp::from_millis(response.expires_at_ms),
        issuer_key: response
            .issuer_key_id
            .parse()
            .map_err(|_| invalid("issuer key ID"))?,
        token: crate::auth::BearerToken::new(response.token),
    })
}

/// The control clients a workload acts as nodes through, one per node, each with a certificate
/// for its node.
struct NodeClients {
    authority: Arc<Authority>,
    control: String,
    clients: BTreeMap<NodeId, ControlClient>,
}

impl NodeClients {
    /// A network that presents `node`'s certificate from the fleet's authority.
    fn network_of(&self, node: &NodeId) -> Network {
        let issued = self
            .authority
            .issue_node(node, &[node.to_string()], certificate_validity())
            .unwrap_or_else(|error| panic!("can't issue a certificate for node {node}: {error}"));
        let identity = Identity::from_issued(self.authority.certificate_pem(), &issued)
            .unwrap_or_else(|error| panic!("can't load the certificate of node {node}: {error}"));
        Network::simulated().with_security(Security::Tls(identity))
    }

    /// A network whose connections present a client certificate for `principal` from the
    /// fleet's authority, and whose calls carry `token`: an agent's.
    fn agent_network(
        &self,
        principal: &crate::tls::Principal,
        token: &crate::auth::BearerToken,
    ) -> Result<Network, tonic::Status> {
        let issued = self
            .authority
            .issue_client(principal, certificate_validity())
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let identity = Identity::from_issued(self.authority.certificate_pem(), &issued)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let credentials = crate::transport::Credentials::bearer(token)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(Network::simulated()
            .with_security(Security::Tls(identity))
            .with_credentials(credentials))
    }

    /// The client for the node `operation` acts as, if it is a node's call.
    fn for_operation(&mut self, operation: &Operation) -> Option<ControlClient> {
        let node = match operation {
            Operation::Join { node }
            | Operation::JoinWith { node, .. }
            | Operation::JoinWithout { node, .. }
            | Operation::Report { node, .. }
            | Operation::Heartbeat { node }
            | Operation::AcquireLease { node, .. }
            | Operation::RenewLease { node, .. } => node,
            _ => return None,
        };
        let client = self.clients.entry(node.clone()).or_insert_with(|| {
            let issued = self
                .authority
                .issue_node(node, &[node.to_string()], certificate_validity())
                .unwrap_or_else(|error| {
                    panic!("can't issue a certificate for node {node}: {error}")
                });
            let identity = Identity::from_issued(self.authority.certificate_pem(), &issued)
                .unwrap_or_else(|error| {
                    panic!("can't load the certificate of node {node}: {error}")
                });
            Network::simulated()
                .with_security(Security::Tls(identity))
                .control_client(self.control.clone())
        });
        Some(client.clone())
    }
}

/// Sends `operation` and returns its outcome. A node's call goes through `node`, the client
/// with that node's certificate; an impostor's node call goes over `network`, with the client's
/// own certificate.
async fn execute(
    data: &Client,
    node: Option<&ControlClient>,
    nodes: &NodeClients,
    network: &Network,
    shared: &Shared,
    via: Option<&Via>,
    operation: &Operation,
) -> Outcome {
    if let Some(via) = via {
        match operation {
            Operation::Write { database, value } => {
                return crate::sim::hrana::write(via, database, *value).await;
            }
            Operation::Read { database, value } => {
                return crate::sim::hrana::read(via, database, *value).await;
            }
            _ => {}
        }
    }
    let client = node.unwrap_or_else(|| data.control());
    let result = match operation {
        Operation::Join { node } => client
            .join(JoinRequest {
                protocol: Some(PROTOCOL_VERSION),
                node: Some(Node {
                    id: node.to_string(),
                    zone: None,
                    host: None,
                    weight: 1,
                    address: None,
                }),
                software: Some(software_to_proto(&running())),
            })
            .await
            .map(|response| Ack::Joined {
                membership_epoch: response.membership_epoch,
            }),
        Operation::JoinWith { node, features } => client
            .join(JoinRequest {
                protocol: Some(PROTOCOL_VERSION),
                node: Some(Node {
                    id: node.to_string(),
                    zone: None,
                    host: None,
                    weight: 1,
                    address: None,
                }),
                software: Some(software_to_proto(
                    &features
                        .iter()
                        .copied()
                        .fold(running(), fleetring_control::Software::with_feature),
                )),
            })
            .await
            .map(|response| Ack::Joined {
                membership_epoch: response.membership_epoch,
            }),
        Operation::JoinWithout { node, features } => client
            .join(JoinRequest {
                protocol: Some(PROTOCOL_VERSION),
                node: Some(Node {
                    id: node.to_string(),
                    zone: None,
                    host: None,
                    weight: 1,
                    address: None,
                }),
                software: Some(software_to_proto(&without_features(features))),
            })
            .await
            .map(|response| Ack::Joined {
                membership_epoch: response.membership_epoch,
            }),
        Operation::Report {
            node,
            replicas,
            disk_free_bytes,
        } => client
            .heartbeat(HeartbeatRequest {
                node_id: node.to_string(),
                databases: replicas
                    .iter()
                    .map(|replica| replica_to_proto(replica, None))
                    .collect(),
                software: Some(software_to_proto(&running())),
                disk_free_bytes: *disk_free_bytes,
            })
            .await
            .map(|_| Ack::HeartbeatRecorded),
        Operation::Fork {
            source,
            target,
            operation,
            replication_factor,
            expect_source_incarnation,
        } => {
            let mut spec = ForkSpec::new(*operation);
            spec.database.replication_factor = u8::try_from(*replication_factor)
                .ok()
                .and_then(ReplicationFactor::new);
            spec.expect_source_incarnation = *expect_source_incarnation;
            data.fork_database(source, target, &spec)
                .await
                .map(|status| Ack::Forked {
                    incarnation: Incarnation::new(status.incarnation),
                })
        }
        Operation::Heartbeat { node } => client
            .heartbeat(HeartbeatRequest {
                node_id: node.to_string(),
                databases: Vec::new(),
                software: Some(software_to_proto(&running())),
                disk_free_bytes: None,
            })
            .await
            .map(|_| Ack::HeartbeatRecorded),
        Operation::Leave { node } => {
            return submit(shared, Action::Leave { id: node.clone() }, Ack::Left).await;
        }
        Operation::Drop { database } => data.drop_database(database).await.map(|_| Ack::Dropped),
        Operation::Propose { action } => return propose(shared, (**action).clone()).await,
        Operation::SeedingProbe {
            to,
            as_node,
            request,
        } => return Outcome::Acknowledged(seeding_probe(nodes, to, as_node, request).await),
        Operation::Pad { database, bytes } => pad(data, database, *bytes).await,
        Operation::Watch { database } => watch(data, database).await,
        Operation::Drain { node } => {
            return submit(shared, Action::Drain { id: node.clone() }, Ack::Draining).await;
        }
        Operation::Revoke { revoked, until } => {
            let action = Action::RevokeToken {
                revoked: revoked.clone(),
                until: *until,
            };
            return submit(shared, action, Ack::Revoked).await;
        }
        Operation::RevokePrincipal { principal } => {
            let action = Action::RevokePrincipal {
                principal: principal.clone(),
            };
            return submit(shared, action, Ack::Revoked).await;
        }
        Operation::RevokeAs {
            certificate,
            token,
            principal,
            token_id,
        } => {
            let request = RevokeTokenRequest {
                revocation: token_id.as_ref().map(|id| Revocation {
                    revoked: Some(revocation::Revoked::TokenId(id.to_string())),
                    until_ms: 0,
                }),
                principal: principal
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            };
            match nodes.agent_network(certificate, token) {
                Ok(network) => network
                    .control_client(nodes.control.clone())
                    .with_backoff(ONE_ATTEMPT)
                    .revoke_token(request)
                    .await
                    .map(|_| Ack::Revoked),
                Err(status) => Err(status),
            }
        }
        Operation::Delegate {
            certificate,
            token,
            operation,
            segment,
            scopes,
            lifetime,
        } => {
            let request = DelegateTokenRequest {
                operation_id: operation.to_bytes().to_vec(),
                segment: segment.clone(),
                scopes: scopes.clone(),
                lifetime_ms: lifetime.map(fleetring_control::Millis::get),
                not_before_ms: None,
            };
            match nodes.agent_network(certificate, token) {
                Ok(network) => network
                    .control_client(nodes.control.clone())
                    .with_backoff(ONE_ATTEMPT)
                    .delegate_token(request)
                    .await
                    .and_then(delegated),
                Err(status) => Err(status),
            }
        }
        Operation::QueryAs {
            certificate,
            token,
            database,
        } => match nodes.agent_network(certificate, token) {
            Ok(network) => Client::new(nodes.control.clone(), network)
                .with_backoff(ONE_ATTEMPT)
                .query(database, Statement::new("SELECT 1"))
                .await
                .map(|_| Ack::Queried),
            Err(status) => Err(status),
        },
        Operation::CreateAs {
            certificate,
            token,
            database,
        } => match nodes.agent_network(certificate, token) {
            Ok(network) => Client::new(nodes.control.clone(), network)
                .with_backoff(ONE_ATTEMPT)
                .create_database_with(database, DatabaseSpec::new(ReplicationFactor::ONE))
                .await
                .map(|_| Ack::Created),
            Err(status) => Err(status),
        },
        Operation::CreateDatabase {
            database,
            replication_factor,
        } => client
            .create_database(CreateDatabaseRequest {
                database: database.to_string(),
                replication_factor: *replication_factor,
                engine: Engine::Sqlite.into(),
                acks: fleetring_proto::v1::Acks::Unspecified.into(),
                min_replicas: 0,
            })
            .await
            .map(|_| Ack::Created),
        Operation::Locate { database } => client
            .locate(LocateRequest {
                database: database.to_string(),
            })
            .await
            .and_then(located),
        Operation::AcquireLease { database, node } => client
            .acquire_lease(AcquireLeaseRequest {
                database: database.to_string(),
                node_id: node.to_string(),
            })
            .await
            .map(lease_ack),
        Operation::RenewLease {
            database,
            node,
            epoch,
        } => client
            .renew_lease(RenewLeaseRequest {
                database: database.to_string(),
                node_id: node.to_string(),
                epoch: epoch.get(),
            })
            .await
            .map(lease_ack),
        Operation::Create {
            database,
            replication_factor,
            durability,
        } => create(data, database, *replication_factor, *durability).await,
        Operation::CreateExpiring {
            database,
            replication_factor,
            expires_in,
        } => create_expiring(data, database, *replication_factor, *expires_in).await,
        Operation::Write { database, value } => data
            .execute(
                database,
                Statement::with_params(
                    format!("INSERT INTO {WRITES_TABLE} (value) VALUES (?)"),
                    [Value::Integer(*value)],
                ),
            )
            .await
            .map(|written| Ack::Written {
                acknowledged_by: written.acknowledged_by,
                replicas: written.replicas,
                under_replicated: written.under_replicated,
                primary: written.primary,
                epoch: written.epoch,
            }),
        Operation::Read { database, value } => read(data, database, *value).await,
        Operation::ImpostorReplicate { .. }
        | Operation::ImpostorFence { .. }
        | Operation::ImpostorSnapshot { .. } => {
            return Outcome::Acknowledged(impostor(network, operation).await);
        }
    };
    result.map_or_else(
        |status| Outcome::from_status(&status),
        Outcome::Acknowledged,
    )
}

/// Creates `database` in `durability` if it doesn't exist yet, then its writes table.
async fn create(
    client: &Client,
    database: &DatabaseName,
    replication_factor: u32,
    durability: DurabilityMode,
) -> Result<Ack, tonic::Status> {
    let replication_factor = u8::try_from(replication_factor)
        .ok()
        .and_then(ReplicationFactor::new)
        .ok_or_else(|| tonic::Status::invalid_argument("invalid replication factor"))?;
    let spec = DatabaseSpec::new(replication_factor).with_acks(durability.acks());
    let location = match client.create_database_with(database, spec).await {
        Ok(location) => location,
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            client.locate(database).await?
        }
        Err(status) => return Err(status),
    };
    let primary = location
        .primary
        .ok_or_else(|| tonic::Status::internal("created without a primary"))?;
    client
        .execute(
            database,
            Statement::new(format!(
                "CREATE TABLE IF NOT EXISTS {WRITES_TABLE} (value INTEGER NOT NULL)"
            )),
        )
        .await?;
    Ok(Ack::Opened {
        primary,
        epoch: location.epoch,
        durability: DurabilityMode::from(location.durability.acks()),
    })
}

/// Creates `database` expiring `expires_in` after the control node takes the creation, opens it
/// on its primary, and creates its [`WRITES_TABLE`].
async fn create_expiring(
    client: &Client,
    database: &DatabaseName,
    replication_factor: u32,
    expires_in: Duration,
) -> Result<Ack, tonic::Status> {
    let replication_factor = u8::try_from(replication_factor)
        .ok()
        .and_then(ReplicationFactor::new)
        .ok_or_else(|| tonic::Status::invalid_argument("invalid replication factor"))?;
    let metadata = NewMetadata {
        expiry: Some(Expiry::In(expires_in)),
        ..NewMetadata::default()
    };
    let spec = DatabaseSpec::new(replication_factor);
    let location = match client
        .create_database_with_metadata(database, spec, &metadata)
        .await
    {
        Ok(location) => location,
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            client.locate(database).await?
        }
        Err(status) => return Err(status),
    };
    let primary = location
        .primary
        .ok_or_else(|| tonic::Status::internal("created without a primary"))?;
    client
        .execute(
            database,
            Statement::new(format!(
                "CREATE TABLE IF NOT EXISTS {WRITES_TABLE} (value INTEGER NOT NULL)"
            )),
        )
        .await?;
    Ok(Ack::Opened {
        primary,
        epoch: location.epoch,
        durability: DurabilityMode::from(location.durability.acks()),
    })
}

/// Reads `database`'s writes table: every value, or whether `value` is there.
async fn read(
    client: &Client,
    database: &DatabaseName,
    value: Option<i64>,
) -> Result<Ack, tonic::Status> {
    let statement = match value {
        Some(value) => Statement::with_params(
            format!("SELECT value FROM {WRITES_TABLE} WHERE value = ?"),
            [Value::Integer(value)],
        ),
        None => Statement::new(format!("SELECT value FROM {WRITES_TABLE}")),
    };
    let rows = client.query(database, statement).await?;
    let values = rows
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Integer(value)) => Ok(*value),
            other => Err(tonic::Status::internal(format!(
                "the writes table holds {other:?}, not an integer"
            ))),
        })
        .collect::<Result<_, _>>()?;
    Ok(Ack::Read { values })
}

fn located(response: LocateResponse) -> Result<Ack, tonic::Status> {
    let placement = response
        .placement
        .ok_or_else(|| tonic::Status::internal("no placement"))?;
    let primary = placement
        .primary
        .map(|id| id.parse())
        .transpose()
        .map_err(|_| tonic::Status::internal("invalid primary"))?;
    Ok(Ack::Located {
        epoch: LeaseEpoch::new(placement.epoch),
        primary,
    })
}

fn lease_ack(response: LeaseResponse) -> Ack {
    Ack::Lease {
        epoch: LeaseEpoch::new(response.epoch),
        expires_at_ms: response.expires_at_ms,
    }
}

/// Submits `action` to the control node in process, as an operator on its host would, and
/// acknowledges it with `ack`.
async fn submit(shared: &Shared, action: Action, ack: Ack) -> Outcome {
    let Some(control) = shared.control_handle() else {
        return Outcome::Indeterminate {
            code: Some(tonic::Code::Unavailable),
        };
    };
    match control.submit(action).await {
        Ok(committed) => match committed.into_outcome() {
            Ok(_) => Outcome::Acknowledged(ack),
            Err(rejection) => Outcome::from_status(&rejection_status(&rejection)),
        },
        Err(_) => Outcome::Indeterminate {
            code: Some(tonic::Code::Unavailable),
        },
    }
}

/// What a lease watcher needs: the host it follows and where to record what it learns.
pub(super) struct WatcherSession {
    pub(super) name: String,
    pub(super) node: String,
    pub(super) address: String,
    pub(super) timeout: Duration,
    pub(super) shared: Shared,
    /// The principal the watcher acts as, issued by the fleet's test authority.
    pub(super) identity: Identity,
}

/// A lease watcher: follows one data host's `WatchLeases` stream for the whole run, as a worker
/// beside the node would, and records every snapshot entry and event in the history (FR-042).
///
/// It opens the stream again whenever it ends or fails, and replaces a stream that has been
/// silent for [`WATCH_IDLE`] with a fresh one, swapping only once the replacement's held leases have
/// arrived, so that no change falls between two streams.
pub(super) async fn run_lease_watcher(session: WatcherSession) -> turmoil::Result {
    let WatcherSession {
        name,
        node,
        address,
        timeout,
        shared,
        identity,
    } = session;
    let network = Network::simulated().with_security(Security::Tls(identity));
    let open = || async {
        // A fresh client each time: a connection to a crashed host stalls rather than fails.
        let client = Client::new(address.clone(), network.clone());
        let mut updates = tokio::time::timeout(timeout, client.watch_leases(&address))
            .await
            .ok()?
            .ok()?;
        let first = tokio::time::timeout(timeout, updates.next()).await.ok()?;
        match first {
            Ok(Some(update)) => {
                record_lease_update(&shared, &name, &node, &update);
                Some(updates)
            }
            _ => None,
        }
    };
    loop {
        let Some(mut updates) = open().await else {
            tokio::time::sleep(WATCH_RETRY).await;
            continue;
        };
        loop {
            match tokio::time::timeout(WATCH_IDLE, updates.next()).await {
                Ok(Ok(Some(update))) => record_lease_update(&shared, &name, &node, &update),
                Ok(Ok(None) | Err(_)) => break,
                Err(_) => {
                    if let Some(replacement) = open().await {
                        updates = replacement;
                    }
                }
            }
        }
        tokio::time::sleep(WATCH_RETRY).await;
    }
}

fn record_lease_update(shared: &Shared, watcher: &str, node: &str, update: &LeaseUpdate) {
    let at = turmoil::sim_elapsed().unwrap_or_default();
    let observations: Vec<LeaseObservation> = match update {
        LeaseUpdate::Held { leases, .. } => leases
            .iter()
            .map(|(database, epoch)| {
                LeaseObservation::new(node, at, database.clone(), *epoch, LeaseSeen::Held)
            })
            .collect(),
        LeaseUpdate::Event(event) => {
            let seen = match event.change {
                LeaseChange::Gained => LeaseSeen::Gained,
                LeaseChange::Lost => LeaseSeen::Lost,
            };
            vec![LeaseObservation::new(
                node,
                at,
                event.database.clone(),
                event.epoch,
                seen,
            )]
        }
    };
    for observation in observations {
        shared.record(watcher, EventKind::Lease(observation.clone()));
        shared.lock().history.observe_lease(observation);
    }
}

/// Makes an impostor's node call with the client's certificate, and returns how the node
/// answered: [`Ack::Refused`] with the status code and the binding the refusal names, or
/// [`Ack::Served`].
async fn impostor(network: &Network, operation: &Operation) -> Ack {
    let answer = match operation {
        Operation::ImpostorReplicate {
            database,
            target,
            epoch,
        } => {
            let open = ReplicateMessage {
                message: Some(replicate_message::Message::Open(ReplicateOpen {
                    database: database.to_string(),
                    epoch: epoch.get(),
                    epoch_start: None,
                    epoch_start_chain: None,
                })),
            };
            // No metadata names the sender: the certificate does.
            match channel(network, target) {
                Ok(channel) => replication_client(channel)
                    .replicate(tokio_stream::iter([open]))
                    .await
                    .map(drop),
                Err(status) => Err(status),
            }
        }
        Operation::ImpostorFence {
            database,
            target,
            epoch,
            candidate,
        } => {
            let request = FenceRequest {
                database: database.to_string(),
                epoch: epoch.get(),
                candidate: candidate.to_string(),
            };
            match channel(network, target) {
                Ok(channel) => replication_client(channel).fence(request).await.map(drop),
                Err(status) => Err(status),
            }
        }
        Operation::ImpostorSnapshot {
            database,
            target,
            epoch,
        } => {
            let request = SnapshotRequest {
                database: database.to_string(),
                epoch: epoch.get(),
                ..SnapshotRequest::default()
            };
            match channel(network, target) {
                Ok(channel) => replication_client(channel)
                    .snapshot(request)
                    .await
                    .map(drop),
                Err(status) => Err(status),
            }
        }
        _ => Err(tonic::Status::invalid_argument("not an impostor's call")),
    };
    match answer {
        Ok(()) => {
            tracing::error!(%operation, "a node served an impostor");
            Ack::Served
        }
        Err(status) => Ack::Refused {
            code: status.code(),
            binding: refused_binding(&status).map(str::to_owned),
        },
    }
}

/// A channel to `target`'s services over `network`.
fn channel(network: &Network, target: &NodeId) -> Result<tonic::transport::Channel, tonic::Status> {
    network
        .channel(&format!("{target}:{PORT}"))
        .map_err(|error| tonic::Status::unavailable(error.to_string()))
}

/// Submits `action` to the control node in process, as its fork driver would propose it, and
/// says whether `apply` accepted it.
async fn propose(shared: &Shared, action: Action) -> Outcome {
    let Some(control) = shared.control_handle() else {
        return Outcome::Indeterminate {
            code: Some(tonic::Code::Unavailable),
        };
    };
    match control.submit(action).await {
        Ok(committed) => Outcome::Acknowledged(Ack::Proposed {
            accepted: committed.into_outcome().is_ok(),
        }),
        Err(_) => Outcome::Indeterminate {
            code: Some(tonic::Code::Unavailable),
        },
    }
}

/// Sends the seeding `Snapshot` `request` to `to` with `as_node`'s certificate, and returns how
/// `to` answered: [`Ack::Served`] if it began a stream, else [`Ack::Refused`].
async fn seeding_probe(
    nodes: &NodeClients,
    to: &NodeId,
    as_node: &NodeId,
    request: &crate::sim::workload::SeedingProbe,
) -> Ack {
    let message = SnapshotRequest {
        database: request.database.to_string(),
        epoch: 0,
        fork: Some(request.fork.name.to_string()),
        fork_incarnation: request.fork.incarnation.get(),
        operation: crate::control::fork::operation_to_proto(request.fork.operation),
        fork_attempt: request.attempt,
        source_incarnation: request.source_incarnation.get(),
        staged: request.staged,
    };
    let network = nodes.network_of(as_node);
    let answer = match channel(&network, to) {
        Ok(channel) => replication_client(channel)
            .snapshot(message)
            .await
            .map(drop),
        Err(status) => Err(status),
    };
    match answer {
        Ok(()) => Ack::Served,
        Err(status) => Ack::Refused {
            code: status.code(),
            binding: refused_binding(&status).map(str::to_owned),
        },
    }
}

/// Writes `bytes` bytes of padding to `database`, in a table of its own.
async fn pad(client: &Client, database: &DatabaseName, bytes: u32) -> Result<Ack, tonic::Status> {
    client
        .execute(
            database,
            Statement::new("CREATE TABLE IF NOT EXISTS padding (bytes BLOB)"),
        )
        .await?;
    client
        .execute(
            database,
            Statement::with_params(
                "INSERT INTO padding (bytes) VALUES (zeroblob(?))",
                [Value::Integer(i64::from(bytes))],
            ),
        )
        .await?;
    Ok(Ack::Padded)
}

/// How long a [`Operation::Watch`] follows a database's changes.
const WATCH_FOR: Duration = Duration::from_millis(300);

/// Watches `database`'s recorded changes from the log's beginning for [`WATCH_FOR`], and returns
/// the values of the rows it saw inserted into the writes table.
async fn watch(client: &Client, database: &DatabaseName) -> Result<Ack, tonic::Status> {
    let mut updates = client.watch_changes(database, None).await?;
    let mut values = Vec::new();
    let deadline = tokio::time::Instant::now() + WATCH_FOR;
    loop {
        let next = tokio::time::timeout_at(deadline, updates.next()).await;
        match next {
            Err(_) | Ok(Ok(None)) => break,
            Ok(Err(status)) if values.is_empty() => return Err(status),
            Ok(Err(_)) => break,
            Ok(Ok(Some(crate::data::ChangeUpdate::Transaction(transaction)))) => {
                for change in &transaction.changes {
                    if change.table != WRITES_TABLE {
                        continue;
                    }
                    let value = change.values().iter().find_map(|column| {
                        match (column.name.as_str(), &column.value) {
                            ("value", Value::Integer(value)) => Some(*value),
                            _ => None,
                        }
                    });
                    values.extend(value);
                }
            }
            Ok(Ok(Some(_))) => {}
        }
    }
    Ok(Ack::Watched { values })
}

/// This build's software without `features`, as a node of the previous release reports it.
fn without_features(features: &[fleetring_control::Feature]) -> fleetring_control::Software {
    let running = running();
    fleetring_control::Feature::ALL
        .into_iter()
        .filter(|feature| running.supports(*feature) && !features.contains(feature))
        .fold(
            fleetring_control::Software::new(running.release().clone(), running.control_format()),
            fleetring_control::Software::with_feature,
        )
}
