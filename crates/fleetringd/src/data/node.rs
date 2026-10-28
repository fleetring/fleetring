//! A data node's side of the `Database` service: which databases it is primary for, and where
//! to send requests for the others.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetring_proto::v1::{AcquireLeaseRequest, LocateRequest, Placement};
use tonic::Status;

use crate::control::ControlClient;
use crate::data::redirect::not_primary;
use crate::data::service::database_status;
use crate::engine::EngineError;
use crate::replication::{PrimaryRole, Replication};
use crate::{DatabaseError, Registry};

/// A lease this node holds, as far as it knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeldLease {
    /// The lease's epoch.
    pub(crate) epoch: LeaseEpoch,
    /// The database's replication factor, R.
    pub(crate) replication_factor: ReplicationFactor,
}

impl HeldLease {
    /// The lease `placement` names, with the database's replication factor: the placement's,
    /// or the size of its replica set from a control node that doesn't send one.
    fn of(placement: &Placement) -> Self {
        let size = if placement.replicas.is_empty() {
            placement.followers.len() + 1
        } else {
            placement.replicas.len()
        };
        let factor = if placement.replication_factor == 0 {
            u32::try_from(size).unwrap_or(u32::MAX)
        } else {
            placement.replication_factor
        };
        Self {
            epoch: LeaseEpoch::new(placement.epoch),
            replication_factor: u8::try_from(factor)
                .ok()
                .and_then(ReplicationFactor::new)
                .unwrap_or(ReplicationFactor::ONE),
        }
    }

    /// R, the size the database's replica set should have.
    pub(crate) fn replicas(self) -> u32 {
        u32::from(self.replication_factor.get())
    }
}

/// The data role's database side: the open databases, the leases this node holds, and the
/// replication that ships their writes to their followers.
///
/// A node serves a database only while it holds the database's lease. It learns that it does
/// when it takes the first lease in [`DataNode::create`], or, after a restart, from the control
/// plane the first time a request for the database arrives. Either way it then serves the
/// database as its primary through [`Replication::serve_primary`], at the lease's epoch. Any
/// other request is redirected to the primary the control plane names. Cloning is cheap and
/// shares the node.
#[derive(Debug, Clone)]
pub(crate) struct DataNode {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    id: NodeId,
    replication: Replication,
    control: ControlClient,
    /// How long a node that starts serving a database waits for its followers' streams before
    /// it takes writes.
    writable_wait: Duration,
    leases: Mutex<BTreeMap<DatabaseName, HeldLease>>,
}

impl DataNode {
    /// A node with ID `id` that keeps its databases in `replication`'s registry, and asks the
    /// control plane through `control`. A database it starts serving waits up to `writable_wait`
    /// for enough followers to take writes.
    pub(crate) fn new(
        id: NodeId,
        replication: Replication,
        control: ControlClient,
        writable_wait: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                id,
                replication,
                control,
                writable_wait,
                leases: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub(crate) fn registry(&self) -> &Registry {
        self.inner.replication.registry()
    }

    pub(crate) fn replication(&self) -> &Replication {
        &self.inner.replication
    }

    /// Returns the lease this node holds on `name`, so the caller may serve it locally.
    ///
    /// # Errors
    ///
    /// A redirect ([`not_primary`]) if another node holds the lease; `FAILED_PRECONDITION` if no
    /// node does yet; `UNAVAILABLE` if the control plane can't be reached or the primary has no
    /// known address; and the control plane's status otherwise, such as `NOT_FOUND`.
    pub(crate) async fn route(&self, name: &DatabaseName) -> Result<HeldLease, Status> {
        if let Some(lease) = self.held(name) {
            return Ok(lease);
        }
        let placement = self.locate(name).await?;
        match placement.primary.as_deref() {
            Some(primary) if primary == self.inner.id.as_str() => {
                // A primary that restarted learns its lease again here. Its database must exist.
                self.registry()
                    .open(name)
                    .await
                    .map_err(|error| missing_file(name, &error))?;
                Ok(self.serve(name, &placement).await)
            }
            Some(primary) => Err(redirect(name, &placement, primary)),
            None => Err(Status::failed_precondition(format!(
                "database {name} has no primary yet; finish creating it"
            ))),
        }
    }

    /// Makes this node `name`'s primary, if the control plane has created the database and not
    /// leased it yet: takes the first lease, then creates the database. On the primary it only
    /// creates the database if it is missing, so repeating it after a failure is harmless.
    ///
    /// # Errors
    ///
    /// A redirect if another node holds the lease; `FAILED_PRECONDITION` if this node isn't in
    /// the database's replica set, or the control plane refuses the lease; and the status of a
    /// failed control call or database creation.
    pub(crate) async fn create(&self, name: &DatabaseName) -> Result<Placement, Status> {
        let placement = self.locate(name).await?;
        let id = self.inner.id.as_str();
        match placement.primary.as_deref() {
            Some(primary) if primary == id => {
                self.create_file(name).await?;
                self.serve(name, &placement).await;
                Ok(placement)
            }
            Some(primary) => Err(redirect(name, &placement, primary)),
            None => {
                if !placement
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == id)
                {
                    return Err(Status::failed_precondition(format!(
                        "{id} isn't in the replica set of {name}"
                    )));
                }
                let lease = self
                    .inner
                    .control
                    .acquire_lease(AcquireLeaseRequest {
                        database: name.to_string(),
                        node_id: id.to_owned(),
                    })
                    .await?;
                tracing::info!(database = %name, epoch = lease.epoch, "took the first lease");
                self.create_file(name).await?;
                let placement = self.locate(name).await?;
                self.serve(name, &placement).await;
                Ok(placement)
            }
        }
    }

    async fn create_file(&self, name: &DatabaseName) -> Result<(), Status> {
        match self.registry().create(name).await {
            Ok(_) => {
                tracing::info!(database = %name, "created the database");
                Ok(())
            }
            Err(error) if matches!(error.engine_error(), Some(EngineError::AlreadyExists)) => {
                Ok(())
            }
            Err(error) => Err(database_status(&error)),
        }
    }

    async fn locate(&self, name: &DatabaseName) -> Result<Placement, Status> {
        self.inner
            .control
            .locate(LocateRequest {
                database: name.to_string(),
            })
            .await?
            .placement
            .ok_or_else(|| Status::internal("the control plane sent no placement"))
    }

    fn held(&self, name: &DatabaseName) -> Option<HeldLease> {
        self.leases().get(name).copied()
    }

    /// Records the lease `placement` names, serves the database as its primary at the lease's
    /// epoch if this node didn't already, and waits until enough followers are connected for a
    /// write to reach its quorum, or `writable_wait` passes.
    async fn serve(&self, name: &DatabaseName, placement: &Placement) -> HeldLease {
        let lease = HeldLease::of(placement);
        let previous = self.leases().insert(name.clone(), lease);
        if previous != Some(lease) {
            tracing::info!(
                database = %name,
                epoch = lease.epoch.get(),
                replication_factor = lease.replication_factor.get(),
                "serving as primary",
            );
            let role = PrimaryRole {
                replication_factor: lease.replication_factor,
                epoch: lease.epoch,
            };
            if let Err(error) = self.inner.replication.serve_primary(name.clone(), role) {
                tracing::warn!(database = %name, %error, "not replicating");
            }
        }
        if !self
            .inner
            .replication
            .await_writable(name, self.inner.writable_wait)
            .await
        {
            tracing::warn!(
                database = %name,
                wait = ?self.inner.writable_wait,
                "too few followers are connected for writes to reach their quorum",
            );
        }
        lease
    }

    fn leases(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, HeldLease>> {
        self.inner.leases.lock().expect("lease table lock poisoned")
    }
}

/// The redirect to `primary`, or `UNAVAILABLE` if the control plane knows no address for it.
fn redirect(name: &DatabaseName, placement: &Placement, primary: &str) -> Status {
    let address = placement
        .replicas
        .iter()
        .find(|replica| replica.node_id == primary)
        .and_then(|replica| replica.address.as_deref());
    match address {
        Some(address) => not_primary(name.as_str(), address),
        None => Status::unavailable(format!(
            "the primary of {name}, {primary}, has no known address"
        )),
    }
}

fn missing_file(name: &DatabaseName, error: &DatabaseError) -> Status {
    if matches!(error.engine_error(), Some(EngineError::NotFound)) {
        Status::failed_precondition(format!(
            "this node holds the lease on {name} but has no copy of it; finish creating it"
        ))
    } else {
        database_status(error)
    }
}
