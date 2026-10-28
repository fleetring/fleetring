//! A simulated client: runs a workload and records every operation.

use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{Action, LeaseEpoch};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::{
    AcquireLeaseRequest, CreateDatabaseRequest, Engine, HeartbeatRequest, JoinRequest,
    LeaseResponse, LocateRequest, LocateResponse, Node, RenewLeaseRequest,
};

use crate::control::{ControlClient, rejection_status};
use crate::sim::SimRng;
use crate::sim::host::{Shared, WorkloadState};
use crate::sim::trace::EventKind;
use crate::sim::workload::{Ack, Operation, Outcome, Record, Step, Workload};
use crate::transport::{Backoff, sim};

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
        timeout,
        shared,
    } = session;
    let client =
        ControlClient::with_connector(control, sim::channel_to).with_backoff(CLIENT_BACKOFF);
    loop {
        match workload.next(&mut rng) {
            Step::Run(operation) => {
                let invoked_at = turmoil::sim_elapsed().unwrap_or_default();
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
                shared.lock().history.push(Record::new(
                    name.as_str(),
                    invoked_at,
                    completed_at,
                    operation,
                    outcome,
                ));
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
async fn execute(client: &ControlClient, shared: &Shared, operation: &Operation) -> Outcome {
    let result = match operation {
        Operation::Join { node } => client
            .join(JoinRequest {
                protocol: Some(PROTOCOL_VERSION),
                node: Some(Node {
                    id: node.to_string(),
                    zone: None,
                    host: None,
                    weight: 1,
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
    };
    result.map_or_else(
        |status| Outcome::from_status(&status),
        Outcome::Acknowledged,
    )
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
