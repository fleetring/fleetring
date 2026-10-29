//! A simulated client: runs a workload and records every operation.

use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{Action, DatabaseName, LeaseEpoch};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::{
    AcquireLeaseRequest, CreateDatabaseRequest, Engine, HeartbeatRequest, JoinRequest,
    LeaseResponse, LocateRequest, LocateResponse, Node, RenewLeaseRequest,
};

use crate::control::rejection_status;
use crate::data::{Client, DatabaseSpec};
use crate::engine::{Statement, Value};
use crate::environment::Network;
use crate::sim::SimRng;
use crate::sim::history::DurabilityMode;
use crate::sim::host::{Shared, WorkloadState};
use crate::sim::trace::EventKind;
use crate::sim::workload::{Ack, Operation, Outcome, Step, WRITES_TABLE, Workload};
use crate::transport::Backoff;

/// Retries for simulated clients: quick, because simulated calls fail fast.
const CLIENT_BACKOFF: Backoff = Backoff {
    initial: Duration::from_millis(10),
    max: Duration::from_millis(100),
    attempts: 3,
};

/// What a client needs besides its workload.
pub(super) struct Session {
    pub(super) name: String,
    pub(super) control: String,
    /// The data node database calls go to first, if not the primary the control plane names.
    pub(super) entry: Option<String>,
    pub(super) timeout: Duration,
    pub(super) shared: Shared,
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
        timeout,
        shared,
    } = session;
    let client = Client::new(control, Network::Simulated).with_backoff(CLIENT_BACKOFF);
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
                let outcome = match tokio::time::timeout(
                    timeout,
                    execute(&client, &shared, &operation),
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        // The connection may have stalled; dial a new one next time.
                        client.reconnect();
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

/// Sends `operation` and returns its outcome.
async fn execute(data: &Client, shared: &Shared, operation: &Operation) -> Outcome {
    let client = data.control();
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
            })
            .await
            .map(|response| Ack::Joined {
                membership_epoch: response.membership_epoch,
            }),
        Operation::Heartbeat { node } => client
            .heartbeat(HeartbeatRequest {
                node_id: node.to_string(),
                databases: Vec::new(),
            })
            .await
            .map(|_| Ack::HeartbeatRecorded),
        Operation::Leave { node } => return leave(shared, node).await,
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

/// Submits a leave to the control node in process, as an operator on its host would.
async fn leave(shared: &Shared, node: &NodeId) -> Outcome {
    let Some(control) = shared.control_handle() else {
        return Outcome::Indeterminate {
            code: Some(tonic::Code::Unavailable),
        };
    };
    match control.submit(Action::Leave { id: node.clone() }).await {
        Ok(committed) => match committed.into_outcome() {
            Ok(_) => Outcome::Acknowledged(Ack::Left),
            Err(rejection) => Outcome::from_status(&rejection_status(&rejection)),
        },
        Err(_) => Outcome::Indeterminate {
            code: Some(tonic::Code::Unavailable),
        },
    }
}
