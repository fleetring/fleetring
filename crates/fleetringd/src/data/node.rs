//! A data node's side of the `Database` service: which databases it is primary for, and where
//! to send requests for the others.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, Durability, LeaseEpoch};
use fleetring_proto::v1::{AcquireLeaseRequest, LocateRequest, Placement};
use tokio::sync::watch;
use tonic::Status;

use crate::changes::ChangeStreams;
use crate::control::ControlClient;
use crate::data::convert::durability_of;
use crate::data::redirect::not_primary;
use crate::data::service::database_status;
use crate::durability::{Archive, RestoreError, RestoreTarget, Restored, StoreError, restore_into};
use crate::engine::EngineError;
use crate::observe::DataMetrics;
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
    pub(super) changes: ChangeStreams,
    pub(super) control: ControlClient,
    /// The archives this node writes, which restores read from (FR-049).
    pub(super) archives: Vec<Archive>,
    /// How long a node that starts serving a database waits for its followers' streams before
    /// it takes writes.
    pub(super) writable_wait: Duration,
    leases: Mutex<BTreeMap<DatabaseName, HeldLease>>,
    /// The databases this node has started serving and still waits for the followers of, each
    /// with the number of starts waiting.
    settling: Counts,
    /// The databases whose requests are refused while their leases are handed over. A request is
    /// admitted, and counted in `admitted`, under this lock, so that none slips in between.
    refusing: Mutex<BTreeSet<DatabaseName>>,
    /// The requests admitted for each database that haven't put their writes in replication's
    /// hands yet ([`Admitted`]); a drain waits for them before it steps down.
    admitted: Counts,
    /// Held while a drain runs, so that two never hand the same lease over.
    pub(super) draining: tokio::sync::Mutex<()>,
    /// One permit: held while this node builds a restored or verified copy on its disk.
    staging: tokio::sync::Semaphore,
    /// Held per database while it is created or restored here, so that two never race.
    creating: NameLocks,
    /// Where writes, queries, renewals, transfers, and promotions are counted.
    metrics: DataMetrics,
}

/// Refuses requests for one database until it is dropped.
pub(super) struct Refusing<'node> {
    node: &'node DataNode,
    name: DatabaseName,
}

impl Drop for Refusing<'_> {
    fn drop(&mut self) {
        self.node.refusing().remove(&self.name);
    }
}

/// How many of something are under way per database; a database with none has no entry.
type Counts = watch::Sender<BTreeMap<DatabaseName, usize>>;

/// Counts one of something under way for `name` in `counts` until it is dropped, even if it is
/// cancelled.
#[derive(Debug)]
struct Counted<'node> {
    counts: &'node Counts,
    name: DatabaseName,
}

impl<'node> Counted<'node> {
    fn begin(counts: &'node Counts, name: &DatabaseName) -> Self {
        counts.send_modify(|counts| {
            *counts.entry(name.clone()).or_default() += 1;
        });
        Self {
            counts,
            name: name.clone(),
        }
    }
}

impl Drop for Counted<'_> {
    fn drop(&mut self) {
        self.counts.send_modify(|counts| {
            if let Some(count) = counts.get_mut(&self.name) {
                *count -= 1;
                if *count == 0 {
                    counts.remove(&self.name);
                }
            }
        });
    }
}

/// A request [`DataNode::route`] admitted on a database this node is primary for. A drain that
/// starts while it lives waits for it to be dropped before it steps down, so that its write
/// isn't cut off between its commit and replication: drop it once the write is in
/// replication's hands ([`Registry::commit`]), or the request has ended.
#[derive(Debug)]
pub(crate) struct Admitted<'node> {
    lease: HeldLease,
    _counted: Counted<'node>,
}

impl Admitted<'_> {
    /// The lease the request is served under.
    pub(crate) fn lease(&self) -> HeldLease {
        self.lease
    }
}

impl DataNode {
    /// A node with ID `id` that keeps its databases in `replication`'s registry, and asks the
    /// control plane through `control`. A database it starts serving waits up to `writable_wait`
    /// for every live follower, and at least a write quorum, to connect. It counts what it does
    /// in `metrics`.
    pub(crate) fn new(
        id: NodeId,
        replication: Replication,
        changes: ChangeStreams,
        control: ControlClient,
        archives: Vec<Archive>,
        writable_wait: Duration,
        metrics: DataMetrics,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                id,
                replication,
                changes,
                control,
                archives,
                writable_wait,
                leases: Mutex::new(BTreeMap::new()),
                settling: watch::Sender::new(BTreeMap::new()),
                refusing: Mutex::new(BTreeSet::new()),
                admitted: watch::Sender::new(BTreeMap::new()),
                draining: tokio::sync::Mutex::new(()),
                staging: tokio::sync::Semaphore::new(1),
                creating: NameLocks::default(),
                metrics,
            }),
        }
    }

    /// Where the node counts writes, queries, renewals, transfers, and promotions.
    pub(crate) fn metrics(&self) -> &DataMetrics {
        &self.inner.metrics
    }

    pub(crate) fn registry(&self) -> &Registry {
        self.inner.replication.registry()
    }

    pub(crate) fn replication(&self) -> &Replication {
        &self.inner.replication
    }

    pub(crate) fn change_streams(&self) -> &ChangeStreams {
        &self.inner.changes
    }

    /// The archive this node writes under `name`.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` naming the archive if no `[[archive]]` entry here has that name.
    pub(crate) fn archive(&self, name: &str) -> Result<&Archive, Status> {
        self.inner
            .archives
            .iter()
            .find(|archive| archive.name() == name)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "this node has no archive named `{name}`; add an [[archive]] entry for it"
                ))
            })
    }

    /// Holds this node's one restore or archive verification while the permit lives: each builds
    /// a whole copy on the node's disk, so a node runs one at a time.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` while another is running, which a retry settles.
    pub(crate) fn staging_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>, Status> {
        self.inner.staging.try_acquire().map_err(|_| {
            Status::unavailable(
                "this node is already restoring or verifying an archive; retry when it is done",
            )
        })
    }

    /// Builds `name`, a database the control plane has created and never leased, from the
    /// archive's copy of `source` at `target`, and serves it: restores into a staging directory,
    /// verifying every batch against the hash chain, moves the copy into place, takes the first
    /// lease, and serves it as `create` does. A copy a failed restore left here is replaced; one
    /// a lease was taken for never is.
    ///
    /// # Errors
    ///
    /// `FAILED_PRECONDITION` if the database has a lease or this node isn't in its replica set,
    /// or the archive can't reach the target; `UNAVAILABLE` if this node is already restoring or
    /// verifying an archive; `DATA_LOSS` naming the object if one is missing
    /// or altered; `UNAVAILABLE` if the store can't be reached; and the status of a failed
    /// control call.
    pub(crate) async fn restore(
        &self,
        name: &DatabaseName,
        archive: &Archive,
        source: &DatabaseName,
        target: &RestoreTarget,
    ) -> Result<(Placement, Restored), Status> {
        let _staging = self.staging_permit()?;
        // A create or another restore of the name waits, so neither removes or replaces the
        // copy this one installs.
        let _creating = self.inner.creating.lock(name).await;
        let placement = self.locate(name).await?;
        let id = self.inner.id.as_str();
        if placement.epoch > 0 || placement.primary.is_some() {
            return Err(Status::failed_precondition(format!(
                "{name} already has a lease at epoch {}; a restore builds a new database, so \
                 drop it first or restore under another name",
                placement.epoch
            )));
        }
        if !placement
            .replicas
            .iter()
            .any(|replica| replica.node_id == id)
        {
            return Err(Status::failed_precondition(format!(
                "{id} isn't in the replica set of {name}"
            )));
        }
        // Never leased, so nothing in a copy here was ever acknowledged: it is what a failed
        // restore or create left.
        self.registry().close(name).await;
        self.registry()
            .remove(name)
            .await
            .map_err(|error| Status::internal(format!("can't remove the old copy: {error}")))?;
        let replication = self.replication();
        let staging = replication.restore_staging();
        let restored = restore_into(
            archive.store().as_ref(),
            &archive.keys(source),
            source,
            replication.engine(),
            target,
            &staging,
            replication.execution(),
        )
        .await
        .map_err(|error| restore_status(source, archive.name(), &error))?;
        replication
            .install_restored(name, &staging, restored.position.as_ref(), restored.chain)
            .await?;
        let lease = self
            .inner
            .control
            .acquire_lease(AcquireLeaseRequest {
                database: name.to_string(),
                node_id: id.to_owned(),
            })
            .await?;
        tracing::info!(database = %name, %source, archive = archive.name(), epoch = lease.epoch, position = ?restored.position, segments = restored.segments, "restored from the archive and took the first lease");
        let placement = self.locate(name).await?;
        self.serve(name, &placement).await?;
        Ok((placement, restored))
    }

    /// This node's ID.
    pub(crate) fn id(&self) -> &NodeId {
        &self.inner.id
    }

    /// Admits a request for `name` if this node holds its lease, so the caller may serve it
    /// locally while the returned guard lives.
    ///
    /// # Errors
    ///
    /// A redirect ([`not_primary`]) if another node holds the lease; `FAILED_PRECONDITION` if no
    /// node does yet; `UNAVAILABLE` if the control plane can't be reached, the primary has no
    /// known address, or this node is handing the lease over; and the control plane's status
    /// otherwise, such as `NOT_FOUND`.
    pub(crate) async fn route(&self, name: &DatabaseName) -> Result<Admitted<'_>, Status> {
        if let Some(lease) = self.held(name) {
            let admitted = self.admit(name, lease)?;
            self.await_settled(name).await;
            return Ok(admitted);
        }
        let placement = self.locate(name).await?;
        match placement.primary.as_deref() {
            Some(primary) if primary == self.inner.id.as_str() => {
                // A primary that restarted learns its lease again here. Its database must exist.
                self.registry()
                    .open(name)
                    .await
                    .map_err(|error| missing_file(name, &error))?;
                let lease = self.serve(name, &placement).await?;
                self.admit(name, lease)
            }
            Some(primary) => Err(redirect(name, &placement, primary)),
            None => Err(no_primary(name, &placement)),
        }
    }

    /// Admits a request for `name` under `lease`, unless the lease is being handed over. The
    /// refusal is checked and the request counted under one lock, so a drain that refuses
    /// requests afterwards sees every request admitted before it.
    fn admit(&self, name: &DatabaseName, lease: HeldLease) -> Result<Admitted<'_>, Status> {
        let refusing = self.refusing();
        if refusing.contains(name) {
            return Err(Status::unavailable(format!(
                "database {name} is moving to a new primary; retry"
            )));
        }
        let counted = Counted::begin(&self.inner.admitted, name);
        drop(refusing);
        Ok(Admitted {
            lease,
            _counted: counted,
        })
    }

    /// Waits until every request admitted for `name` has put its write in replication's hands or
    /// ended, or `limit` passes. Returns whether they all did. Called once requests are refused
    /// ([`DataNode::refuse_requests`]), so that no new one is admitted meanwhile.
    pub(super) async fn await_admitted(&self, name: &DatabaseName, limit: Duration) -> bool {
        let mut admitted = self.inner.admitted.subscribe();
        let done = admitted.wait_for(|admitted| !admitted.contains_key(name));
        // The sender lives as long as the node.
        matches!(tokio::time::timeout(limit, done).await, Ok(Ok(_)))
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
        let _creating = self.inner.creating.lock(name).await;
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
    ///
    /// The lease watch reports the loss first, so that a watcher hears of it before requests for
    /// the database are redirected (FR-042).
    pub(super) fn forget(&self, name: &DatabaseName, epoch: LeaseEpoch) -> bool {
        let mut leases = self.leases();
        if leases.get(name).is_some_and(|lease| lease.epoch == epoch) {
            self.inner.replication.withdraw(name);
            leases.remove(name);
            true
        } else {
            false
        }
    }

    /// Forgets whatever lease this node holds on `name`, as when the database is dropped.
    pub(super) fn forget_any(&self, name: &DatabaseName) {
        let mut leases = self.leases();
        if leases.contains_key(name) {
            self.inner.replication.withdraw(name);
        }
        leases.remove(name);
    }

    /// Leads `name` under `lease` if this node doesn't already, records the lease, and waits
    /// until every live follower, and at least enough for a write to be admitted and reach its
    /// quorum, is connected or `writable_wait` passes.
    ///
    /// Waiting for every live follower, not just a write quorum, means the first write isn't
    /// reported under-replicated while all of its replicas are live. The lease is recorded first,
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
        let _settling = Counted::begin(&self.inner.settling, name);
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

    /// Refuses requests for `name` with `UNAVAILABLE` until the guard is dropped, while its lease
    /// is handed over. Once the lease is gone, requests are redirected as usual.
    pub(super) fn refuse_requests(&self, name: &DatabaseName) -> Refusing<'_> {
        self.refusing().insert(name.clone());
        Refusing {
            node: self,
            name: name.clone(),
        }
    }

    fn leases(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, HeldLease>> {
        self.inner.leases.lock().expect("lease table lock poisoned")
    }

    fn refusing(&self) -> MutexGuard<'_, BTreeSet<DatabaseName>> {
        self.inner
            .refusing
            .lock()
            .expect("refusal set lock poisoned")
    }
}

/// An async lock per database name, each forgotten once no one holds or waits for it.
#[derive(Debug, Default)]
struct NameLocks(Mutex<BTreeMap<DatabaseName, Arc<tokio::sync::Mutex<()>>>>);

/// Holds one name's lock while it lives.
struct NameGuard<'locks> {
    locks: &'locks NameLocks,
    name: DatabaseName,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl NameLocks {
    fn map(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, Arc<tokio::sync::Mutex<()>>>> {
        self.0.lock().expect("name locks poisoned")
    }

    /// Waits until no one else holds `name`'s lock, then holds it.
    async fn lock(&self, name: &DatabaseName) -> NameGuard<'_> {
        let lock = Arc::clone(self.map().entry(name.clone()).or_default());
        let guard = lock.lock_owned().await;
        NameGuard {
            locks: self,
            name: name.clone(),
            guard: Some(guard),
        }
    }
}

impl Drop for NameGuard<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        // A poisoned lock means a panic elsewhere; a destructor mustn't add another.
        let Ok(mut map) = self.locks.0.lock() else {
            return;
        };
        if map
            .get(&self.name)
            .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            map.remove(&self.name);
        }
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

/// The status a failed restore of `source` from `archive` answers with.
pub(crate) fn restore_status(source: &DatabaseName, archive: &str, error: &RestoreError) -> Status {
    let message = format!("restoring {source} from archive `{archive}`: {error}");
    match error {
        RestoreError::DataLoss { .. } => Status::data_loss(message),
        RestoreError::Plan(_)
        | RestoreError::NotBoundary { .. }
        | RestoreError::SelfReplicating(_) => Status::failed_precondition(message),
        RestoreError::Store(StoreError::Unavailable { .. }) => Status::unavailable(message),
        _ => Status::internal(message),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn one_name_is_created_or_restored_by_one_caller_at_a_time() {
        let locks = NameLocks::default();
        let a: DatabaseName = "a".parse().expect("a name");
        let b: DatabaseName = "b".parse().expect("a name");
        let first = locks.lock(&a).await;
        // Another name isn't held up.
        drop(locks.lock(&b).await);
        let second = tokio::time::timeout(Duration::from_millis(50), locks.lock(&a)).await;
        assert!(second.is_err(), "the second caller waits for the first");
        drop(first);
        let third = locks.lock(&a).await;
        drop(third);
        assert!(locks.map().is_empty(), "locks no one holds are forgotten");
    }
}
