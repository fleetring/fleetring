//! A data node's side of the `Database` service: which databases it is primary for, and where
//! to send requests for the others.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, Durability, LeaseEpoch};
use fleetring_proto::v1::{AcquireLeaseRequest, LocateRequest, Placement};
use tokio::sync::watch;
use tonic::Status;

use crate::control::ControlClient;
use crate::data::convert::durability_of;
use crate::data::redirect::not_primary;
use crate::data::service::database_status;
use crate::engine::EngineError;
use crate::replication::{PrimaryRole, Replication, ServePrimaryError};
use crate::{DatabaseError, Registry};

/// A lease this node holds, as far as it knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeldLease {
    /// The lease's epoch.
    pub(crate) epoch: LeaseEpoch,
    /// The database's replication factor, R.
    pub(crate) replication_factor: ReplicationFactor,
    /// The durability settings the lease was granted under, once known. A lease built without
    /// them, as a promotion's is, learns them from the control plane before it is served
    /// ([`DataNode::start_serving`]).
    pub(crate) durability: Option<Durability>,
}

impl HeldLease {
    /// The lease `placement` names, with the database's replication factor: the placement's,
    /// or the size of its replica set from a control node that doesn't send one; and its
    /// durability settings.
    pub(super) fn of(placement: &Placement) -> Self {
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
        let replication_factor = u8::try_from(factor)
            .ok()
            .and_then(ReplicationFactor::new)
            .unwrap_or(ReplicationFactor::ONE);
        Self {
            epoch: LeaseEpoch::new(placement.epoch),
            replication_factor,
            durability: Some(durability_of(placement, replication_factor)),
        }
    }

    /// The lease at `epoch` on a database with `replication_factor` replicas, whose durability
    /// settings are learned from the control plane when it is served.
    pub(super) fn new(epoch: LeaseEpoch, replication_factor: ReplicationFactor) -> Self {
        Self {
            epoch,
            replication_factor,
            durability: None,
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
/// when it takes the first lease in [`DataNode::create`], when it completes a promotion as its
/// candidate, or, after a restart, from the control plane: eagerly for every database it has a
/// record of, and otherwise when a request for the database
/// arrives. It then leads the database through [`Replication::lead`], at the lease's epoch. It
/// stops as soon as it learns of a newer epoch: from a renewal the control plane refuses, or
/// from a fence or a stream at that epoch, which [`DataNode::route`] sees in the replication's
/// highest epoch. A renewal refused as an unknown database means the database was dropped, and
/// the node deletes its copy (see [`LeaseKeeper`](crate::data::LeaseKeeper)). Any other request
/// is redirected to the primary the control plane names. Cloning is cheap and shares the node.
#[derive(Debug, Clone)]
pub(crate) struct DataNode {
    pub(super) inner: Arc<Inner>,
}

#[derive(Debug)]
pub(super) struct Inner {
    pub(super) id: NodeId,
    pub(super) replication: Replication,
    pub(super) control: ControlClient,
    /// How long a node that starts serving a database waits for its followers' streams before
    /// it takes writes.
    pub(super) writable_wait: Duration,
    leases: Mutex<BTreeMap<DatabaseName, HeldLease>>,
    /// The databases this node has started serving and still waits for the followers of, each
    /// with the number of starts waiting.
    settling: watch::Sender<BTreeMap<DatabaseName, usize>>,
}

/// Counts one start of `name` as settling until it is dropped, even if the start is cancelled.
struct Settling<'node> {
    node: &'node DataNode,
    name: DatabaseName,
}

impl<'node> Settling<'node> {
    fn begin(node: &'node DataNode, name: &DatabaseName) -> Self {
        node.inner.settling.send_modify(|settling| {
            *settling.entry(name.clone()).or_default() += 1;
        });
        Self {
            node,
            name: name.clone(),
        }
    }
}

impl Drop for Settling<'_> {
    fn drop(&mut self) {
        self.node.inner.settling.send_modify(|settling| {
            if let Some(count) = settling.get_mut(&self.name) {
                *count -= 1;
                if *count == 0 {
                    settling.remove(&self.name);
                }
            }
        });
    }
}

impl DataNode {
    /// A node with ID `id` that keeps its databases in `replication`'s registry, and asks the
    /// control plane through `control`. A database it starts serving waits up to `writable_wait`
    /// for every live follower, and at least a write quorum, to connect.
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
                settling: watch::Sender::new(BTreeMap::new()),
            }),
        }
    }

    pub(crate) fn registry(&self) -> &Registry {
        self.inner.replication.registry()
    }

    pub(crate) fn replication(&self) -> &Replication {
        &self.inner.replication
    }

    /// This node's ID.
    pub(crate) fn id(&self) -> &NodeId {
        &self.inner.id
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
            self.await_settled(name).await;
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
                self.serve(name, &placement).await
            }
            Some(primary) => Err(redirect(name, &placement, primary)),
            None => Err(no_primary(name, &placement)),
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
                self.serve(name, &placement).await?;
                Ok(placement)
            }
            Some(primary) => Err(redirect(name, &placement, primary)),
            None if placement.epoch > 0 => Err(no_primary(name, &placement)),
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
                self.serve(name, &placement).await?;
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

    pub(super) async fn locate(&self, name: &DatabaseName) -> Result<Placement, Status> {
        self.inner
            .control
            .locate(LocateRequest {
                database: name.to_string(),
            })
            .await?
            .placement
            .ok_or_else(|| Status::internal("the control plane sent no placement"))
    }

    /// The lease this node holds on `name`, unless a newer epoch has reached it since, in which
    /// case it forgets the lease.
    pub(super) fn held(&self, name: &DatabaseName) -> Option<HeldLease> {
        let lease = self.leases().get(name).copied()?;
        let newer = self
            .inner
            .replication
            .highest_epoch(name)
            .is_some_and(|highest| highest > lease.epoch);
        if newer {
            self.forget(name, lease.epoch);
            return None;
        }
        Some(lease)
    }

    /// Every lease this node holds, by database.
    pub(super) fn held_leases(&self) -> BTreeMap<DatabaseName, HeldLease> {
        self.leases().clone()
    }

    /// Forgets the lease on `name` at `epoch`, if that is the one it holds.
    pub(super) fn forget(&self, name: &DatabaseName, epoch: LeaseEpoch) -> bool {
        let mut leases = self.leases();
        if leases.get(name).is_some_and(|lease| lease.epoch == epoch) {
            leases.remove(name);
            true
        } else {
            false
        }
    }

    /// Forgets whatever lease this node holds on `name`, as when the database is dropped.
    pub(super) fn forget_any(&self, name: &DatabaseName) {
        self.leases().remove(name);
    }

    /// Leads `name` under `lease` if this node doesn't already, records the lease, and waits
    /// until every live follower, and at least enough for a write to be admitted and reach its
    /// quorum, is connected or `writable_wait` passes.
    ///
    /// Waiting for every live follower, not just a write quorum, means the first write isn't
    /// reported under-replicated while all of its replicas are up. The lease is recorded first,
    /// so that renewals go on during the wait, and [`DataNode::route`] holds a request back
    /// until the wait ends. If the wait runs out before the database's `min_replicas` are
    /// connected, the database is served but every write is refused until they are
    /// (ADR-0023).
    ///
    /// A lease that doesn't carry the database's durability settings learns them from the
    /// control plane first, so that a promoted primary serves under the settings the new lease
    /// was granted with.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` if this node has seen a newer epoch, can't record the lease, or can't learn
    /// the durability settings.
    pub(super) async fn start_serving(
        &self,
        name: &DatabaseName,
        lease: HeldLease,
        start: Option<crate::engine::Position>,
    ) -> Result<(), Status> {
        let durability = match lease.durability {
            Some(durability) => durability,
            None => {
                let placement = self.locate(name).await.map_err(|status| {
                    Status::unavailable(format!(
                        "can't learn the durability settings of {name}: {status}"
                    ))
                })?;
                durability_of(&placement, lease.replication_factor)
            }
        };
        let lease = HeldLease {
            durability: Some(durability),
            ..lease
        };
        let role =
            PrimaryRole::new(lease.replication_factor, lease.epoch).with_durability(durability);
        let serving = self.inner.replication.serving(name) == Some(role);
        if !serving || start.is_some() {
            tracing::info!(
                database = %name,
                epoch = lease.epoch.get(),
                replication_factor = lease.replication_factor.get(),
                acks = %durability.acks(),
                min_replicas = durability.min_replicas(),
                "serving as primary",
            );
            match self.inner.replication.lead(name, role, start).await {
                Ok(()) => {}
                Err(error @ ServePrimaryError::SelfReplicating { .. }) => {
                    tracing::warn!(database = %name, %error, "not replicating");
                }
                Err(error) => {
                    return Err(Status::unavailable(format!("can't serve {name}: {error}")));
                }
            }
        }
        let _settling = Settling::begin(self, name);
        self.leases().insert(name.clone(), lease);
        self.await_replicas(name).await;
        Ok(())
    }

    /// Records the lease `placement` names and serves the database as its primary at the lease's
    /// epoch, as [`DataNode::start_serving`] does.
    async fn serve(&self, name: &DatabaseName, placement: &Placement) -> Result<HeldLease, Status> {
        let lease = HeldLease::of(placement);
        self.start_serving(name, lease, None).await?;
        Ok(lease)
    }

    /// Waits until every follower the membership view counts live, and at least enough for a
    /// write to be admitted and reach its quorum, is connected, or `writable_wait` passes.
    async fn await_replicas(&self, name: &DatabaseName) {
        let replication = &self.inner.replication;
        let wait = self.inner.writable_wait;
        if replication.await_replicas(name, wait).await {
            return;
        }
        if replication.is_writable(name) {
            tracing::info!(
                database = %name,
                ?wait,
                "some live followers aren't connected yet, so writes report under-replicated",
            );
        } else {
            tracing::warn!(
                database = %name,
                ?wait,
                "too few followers are connected; writes are refused until enough are",
            );
        }
    }

    /// Waits until no start of `name` is waiting for its followers any more, or `writable_wait`
    /// passes.
    async fn await_settled(&self, name: &DatabaseName) {
        let mut settling = self.inner.settling.subscribe();
        let settled = settling.wait_for(|settling| !settling.contains_key(name));
        // The sender lives as long as the node, and the wait is bounded like the start's own.
        let _ = tokio::time::timeout(self.inner.writable_wait, settled).await;
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

/// The status for a database the control plane names no primary for: one that is failing over is
/// worth retrying, and one that never had a lease needs its creation finished.
fn no_primary(name: &DatabaseName, placement: &Placement) -> Status {
    if placement.epoch > 0 {
        Status::unavailable(format!(
            "database {name} is failing over to epoch {}; retry",
            placement.epoch
        ))
    } else {
        Status::failed_precondition(format!(
            "database {name} has no primary yet; finish creating it"
        ))
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
