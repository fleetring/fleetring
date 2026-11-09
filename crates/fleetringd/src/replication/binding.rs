//! The data node's side of node calls bound to placement (ADR-0031).
//!
//! A replica learns who may lead an epoch from control state once, through an
//! [`EpochAuthority`], and then trusts its durable epoch record: a stream or a fence from the
//! node the record names at that epoch needs no control round trip, so a follower that restarts
//! while the control plane is down still accepts its primary. A node the record doesn't name is
//! checked against control state before the record names it; while control state can't be
//! reached, it is answered `UNAVAILABLE` and retries.
//!
//! Membership changes publish the new ring on each node at a different moment. A stream's
//! receiver counts as a replica under its current ring, the ring before it ([`replicas_of`]), or
//! the replica set control state names now. A snapshot goes only to a follower on the primary's
//! current ring or in control state's replica set: the ring before lingers until the next
//! change, and must not keep a node a follower that long (ADR-0031, amendment).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use fleetring::{NodeId, ReplicationFactor, Ring};
use fleetring_control::fork::ForkIdentity;
use fleetring_control::{DatabaseName, Incarnation, LeaseEpoch};
use fleetring_proto::v1::{LocateRequest, SnapshotRequest};
use tonic::Status;
use tonic::metadata::MetadataValue;

use super::Inner;
use super::fork::view::{holders_from_placement, view_from_placement};
use super::fork::{DatabaseView, ForkPhase, ForkView};
use crate::MembershipView;
use crate::auth::{BINDING_KEY, Binding, BindingDenied, SeedingMismatch, Unbound};
use crate::control::ControlClient;
use crate::control::fork::operation_from_proto;

/// Who may lead a database now, as control state says (ADR-0031, point 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochHolders {
    /// The database's current epoch.
    pub epoch: LeaseEpoch,
    /// The node holding the lease at `epoch`, if one does.
    pub holder: Option<NodeId>,
    /// The candidate of the promotion to `epoch`, while one is in progress.
    pub candidate: Option<NodeId>,
    /// The replica set control state places the database on.
    pub replicas: Vec<NodeId>,
    /// How many replicas the database should have.
    pub replication_factor: ReplicationFactor,
}

impl EpochHolders {
    /// Whether `node` holds `epoch`, or is promoting to it.
    #[must_use]
    pub fn leads(&self, node: &NodeId, epoch: LeaseEpoch) -> bool {
        self.epoch == epoch
            && (self.holder.as_ref() == Some(node) || self.candidate.as_ref() == Some(node))
    }
}

/// The future [`EpochAuthority::holders`] returns.
pub type HoldersFuture<'a> =
    Pin<Box<dyn Future<Output = Result<EpochHolders, Status>> + Send + 'a>>;

/// The future [`EpochAuthority::view`] returns.
pub type ViewFuture<'a> = Pin<Box<dyn Future<Output = Result<DatabaseView, Status>> + Send + 'a>>;

/// Where a data node asks who leads an epoch.
pub trait EpochAuthority: fmt::Debug + Send + Sync + 'static {
    /// Who leads `database` now.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` when control state can't be reached, `NOT_FOUND` for an unknown database.
    fn holders(&self, database: &DatabaseName) -> HoldersFuture<'_>;

    /// Who leads `database` now, with its incarnation and its fork record (ADR-0046). An
    /// authority that knows no more than [`holders`](Self::holders) answers the legacy
    /// incarnation and no fork, which every seeding check refuses.
    ///
    /// # Errors
    ///
    /// As [`holders`](Self::holders).
    fn view(&self, database: &DatabaseName) -> ViewFuture<'_> {
        let holders = self.holders(database);
        Box::pin(async move {
            Ok(DatabaseView {
                holders: holders.await?,
                incarnation: Incarnation::LEGACY,
                fork: None,
            })
        })
    }
}

/// The control plane's answer to `Locate`: the epoch, the primary, and the candidate.
impl EpochAuthority for ControlClient {
    fn holders(&self, database: &DatabaseName) -> HoldersFuture<'_> {
        let request = LocateRequest {
            database: database.to_string(),
        };
        Box::pin(async move {
            let placement = self
                .locate(request)
                .await?
                .placement
                .ok_or_else(|| Status::internal("the reply has no placement"))?;
            holders_from_placement(&placement)
        })
    }

    fn view(&self, database: &DatabaseName) -> ViewFuture<'_> {
        let request = LocateRequest {
            database: database.to_string(),
        };
        Box::pin(async move {
            let placement = self
                .locate(request)
                .await?
                .placement
                .ok_or_else(|| Status::internal("the reply has no placement"))?;
            view_from_placement(&placement)
        })
    }
}

/// A fixed answer per database, for tests: `NOT_FOUND` for a database it doesn't list.
impl EpochAuthority for BTreeMap<DatabaseName, EpochHolders> {
    fn holders(&self, database: &DatabaseName) -> HoldersFuture<'_> {
        let answer = self
            .get(database)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("database '{database}' does not exist")));
        Box::pin(std::future::ready(answer))
    }
}

impl<T: EpochAuthority> EpochAuthority for Arc<T> {
    fn holders(&self, database: &DatabaseName) -> HoldersFuture<'_> {
        (**self).holders(database)
    }

    fn view(&self, database: &DatabaseName) -> ViewFuture<'_> {
        (**self).view(database)
    }
}

/// The replica set of `database` at `factor` under `view`'s ring, together with the one under
/// `previous`, the ring before it (ADR-0031, point 4): every node a membership change may still
/// legitimately treat as a replica. The one function live migration extends with its target set.
pub(super) fn replicas_of(
    view: &MembershipView,
    previous: Option<&Ring>,
    database: &DatabaseName,
    factor: ReplicationFactor,
) -> BTreeSet<NodeId> {
    let current = view.ring().map(|ring| &**ring);
    current
        .into_iter()
        .chain(previous)
        .flat_map(|ring| ring.replicas_n(database.as_str(), factor).to_ids())
        .collect()
}

/// Counts and audits `denied`, and returns the status the caller sees.
pub(super) fn refuse(
    inner: &Inner,
    database: Option<&DatabaseName>,
    denied: BindingDenied,
) -> Status {
    inner.binding_refused(database);
    denied.into_status()
}

/// The stream's epoch is behind control state's: the caller led it once, perhaps, but no longer.
fn superseded(database: &DatabaseName, epoch: u64, current: LeaseEpoch) -> Status {
    Status::failed_precondition(format!(
        "epoch {epoch} of {database} is superseded: control state is at epoch {current}"
    ))
}

/// Checks a `Replicate` stream that `caller` opens for `database` at `epoch` against its binding,
/// before anything is recorded: `caller` holds the epoch or is promoting to it, and this node is
/// a replica of the database.
///
/// The epoch record decides when it names a holder at `epoch`; control state decides otherwise,
/// once, and the stream then records the caller as the epoch's holder. An epoch below the
/// record's is left to the record, which refuses it.
///
/// # Errors
///
/// A binding refusal; `FAILED_PRECONDITION` if control state has moved past `epoch`; and the
/// authority's error as it is, `UNAVAILABLE` while control state can't be reached.
pub(super) async fn check_stream(
    inner: &Inner,
    database: &DatabaseName,
    epoch: u64,
    caller: &NodeId,
) -> Result<(), Status> {
    let record = inner.epochs.record(database);
    let lease_epoch = LeaseEpoch::new(epoch);
    let not_holder = || {
        BindingDenied::new(
            "Replicate",
            caller.clone(),
            Unbound::NotHolder {
                database: database.clone(),
                epoch: lease_epoch,
            },
        )
        .naming(Binding::HolderOrCandidate)
    };
    if epoch < record.highest() {
        return Ok(());
    }
    if epoch == record.highest() {
        match record.holder() {
            Some(holder) if holder == caller.as_str() => return Ok(()),
            Some(_) => return Err(refuse(inner, Some(database), not_holder())),
            None => {}
        }
    }
    let holders = inner.authority.holders(database).await?;
    if holders.epoch > lease_epoch {
        return Err(superseded(database, epoch, holders.epoch));
    }
    if !holders.leads(caller, lease_epoch) {
        return Err(refuse(inner, Some(database), not_holder()));
    }
    let view = inner.membership.view();
    let mut replicas = replicas_of(
        &view,
        view.previous_ring().map(|ring| &**ring),
        database,
        holders.replication_factor,
    );
    replicas.extend(holders.replicas);
    if !replicas.contains(&inner.node) {
        let denied = BindingDenied::new(
            "Replicate",
            caller.clone(),
            Unbound::NotReplica {
                database: database.clone(),
            },
        );
        return Err(refuse(inner, Some(database), denied));
    }
    Ok(())
}

/// Checks a `Fence` that `caller` sends for `database` at `epoch`, naming `candidate`: the
/// request names the caller, and control state names the caller the candidate of the promotion
/// to `epoch` (or, once that promotion completed, the holder at it). A fence the record already
/// took from the caller at `epoch` needs no second look, and an epoch below the record's is left
/// to the record, which refuses it.
///
/// # Errors
///
/// As [`check_stream`].
pub(super) async fn check_fence(
    inner: &Inner,
    database: &DatabaseName,
    epoch: u64,
    candidate: Option<&NodeId>,
    caller: &NodeId,
) -> Result<(), Status> {
    let lease_epoch = LeaseEpoch::new(epoch);
    let not_candidate = || {
        BindingDenied::new(
            "Fence",
            caller.clone(),
            Unbound::NotCandidate {
                database: database.clone(),
                epoch: lease_epoch,
            },
        )
    };
    match candidate {
        Some(candidate) if candidate == caller => {}
        Some(candidate) => {
            let denied = BindingDenied::new(
                "Fence",
                caller.clone(),
                Unbound::OtherNode {
                    named: candidate.clone(),
                },
            );
            return Err(refuse(inner, Some(database), denied));
        }
        // A fence must name its candidate, so that the record can name the epoch's holder.
        None => return Err(refuse(inner, Some(database), not_candidate())),
    }
    let record = inner.epochs.record(database);
    if epoch < record.highest()
        || (epoch == record.highest() && record.holder() == Some(caller.as_str()))
    {
        return Ok(());
    }
    let holders = inner.authority.holders(database).await?;
    if holders.epoch > lease_epoch {
        return Err(superseded(database, epoch, holders.epoch));
    }
    if !holders.leads(caller, lease_epoch) {
        return Err(refuse(inner, Some(database), not_candidate()));
    }
    Ok(())
}

/// Checks a `Fetch` that `caller` sends for `database` at `epoch`: this replica was fenced at
/// `epoch` for the caller, as its epoch record says. No control round trip: the fence checked
/// the caller against control state before it recorded it. A replica that has moved past `epoch`
/// is left to the fetch, which refuses it.
///
/// # Errors
///
/// A binding refusal.
pub(super) fn check_fetch(
    inner: &Inner,
    database: &DatabaseName,
    epoch: u64,
    caller: &NodeId,
) -> Result<(), Status> {
    let record = inner.epochs.record(database);
    if epoch < record.highest()
        || (epoch == record.highest() && record.holder() == Some(caller.as_str()))
    {
        return Ok(());
    }
    let denied = BindingDenied::new(
        "Fetch",
        caller.clone(),
        Unbound::NotCandidate {
            database: database.clone(),
            epoch: LeaseEpoch::new(epoch),
        },
    );
    Err(refuse(inner, Some(database), denied))
}

/// Checks a `Promote` for `database` at `epoch`: control state names this node the candidate of
/// the promotion to `epoch` (or, once it completed, the holder at it). The caller isn't checked
/// further: a repeated notice of a real promotion is harmless.
///
/// # Errors
///
/// A binding refusal naming this node; `FAILED_PRECONDITION` if control state has moved past
/// `epoch`; and the authority's error as it is.
pub(super) async fn check_promote(
    inner: &Inner,
    database: &DatabaseName,
    epoch: u64,
) -> Result<(), Status> {
    let lease_epoch = LeaseEpoch::new(epoch);
    let holders = inner.authority.holders(database).await?;
    if holders.epoch > lease_epoch {
        return Err(superseded(database, epoch, holders.epoch));
    }
    if holders.leads(&inner.node, lease_epoch) {
        return Ok(());
    }
    let denied = BindingDenied::new(
        "Promote",
        inner.node.clone(),
        Unbound::NotCandidate {
            database: database.clone(),
            epoch: lease_epoch,
        },
    );
    Err(refuse(inner, Some(database), denied))
}

/// Checks a `DropCopy` naming `database` at `epoch`, whoever made it (ADR-0042). The drop
/// stands only if control state no longer holds the database, or holds a newer one of the name at
/// a lower epoch or, when the notice names one, at another incarnation (ADR-0046, point 4.6),
/// and `epoch` is exactly the highest epoch this node's copy recorded, so the notice names this
/// copy's incarnation. A live database's control epoch equals its copies' highest, and a
/// database created again under the name restarts its epochs and draws a new incarnation, so
/// neither a forged notice nor a late one for an earlier incarnation deletes anything.
///
/// # Errors
///
/// - A notice for a database control state still holds at `epoch` or later, at the notice's
///   incarnation, fails the binding: `PERMISSION_DENIED` for a node certificate,
///   `FAILED_PRECONDITION` for a plaintext caller.
/// - One whose epoch isn't this node's copy's gets `FAILED_PRECONDITION`, and changes nothing: a
///   replica that missed a promotion is told of the drop honestly, and keeps its copy as an
///   orphan. A node with no copy of the name accepts it, and has nothing to delete.
pub(super) async fn check_drop(
    inner: &Inner,
    database: &DatabaseName,
    epoch: u64,
    incarnation: Option<Incarnation>,
    caller: Option<&NodeId>,
) -> Result<(), Status> {
    let lease_epoch = LeaseEpoch::new(epoch);
    let dropped = match inner.authority.view(database).await {
        Ok(view) => {
            view.holders.epoch < lease_epoch
                || incarnation.is_some_and(|dropped| dropped != view.incarnation)
        }
        Err(status) if status.code() == tonic::Code::NotFound => true,
        Err(status) => return Err(status),
    };
    if !dropped {
        let reason = Unbound::NotDropped {
            database: database.clone(),
            epoch: lease_epoch,
        };
        return match caller {
            Some(caller) => {
                let denied = BindingDenied::new("DropCopy", caller.clone(), reason);
                Err(refuse(inner, Some(database), denied))
            }
            None => Err(Status::failed_precondition(reason.to_string())),
        };
    }
    match inner.epochs.highest(database) {
        None => Ok(()),
        Some(highest) if highest == epoch => Ok(()),
        // A notice that names a seeding fork's identity is about its staged copies, which the
        // node judges again under the name's lock; its copy of the name, if any, stays.
        Some(_) if epoch == 0 && incarnation.is_some() => Ok(()),
        Some(highest) => Err(Status::failed_precondition(format!(
            "this node's copy of {database} is at epoch {highest}, not the dropped epoch {epoch}; \
             it keeps the copy"
        ))),
    }
}

/// Checks a `Snapshot` that `caller` asks this node for, of `database` at `epoch`: this node
/// serves the database at `epoch`, and the caller is a follower it streams the database to
/// under its current ring, or a replica control state places the database on now, which a
/// primary that hasn't published the newest ring yet will stream to next.
///
/// The ring before the current one doesn't count here, unlike for a stream's receiver: it stays
/// in the view until the next membership change, so a node it named would stay a follower long
/// after the change it was kept for, and could read the database whole.
///
/// A node that doesn't serve the database at `epoch` refuses too, since nobody is its follower
/// there; a replica, by this node's ring or control state, is told so as a stale request, as
/// before, since a legitimate follower may ask a primary that has just stepped down.
///
/// # Errors
///
/// A binding refusal; `FAILED_PRECONDITION` for a replica that asked the wrong node; and the
/// authority's error as it is.
pub(super) async fn check_snapshot(
    inner: &Inner,
    database: &DatabaseName,
    epoch: u64,
    caller: &NodeId,
) -> Result<(), Status> {
    if *caller == inner.node {
        return Err(refuse(
            inner,
            Some(database),
            not_follower(database, caller),
        ));
    }
    let served = inner
        .primary(database)
        .filter(|primary| primary.role().epoch.get() == epoch);
    if let Some(primary) = &served
        && inner.followers_of(primary).followers.contains(caller)
    {
        return Ok(());
    }
    let holders = inner.authority.holders(database).await?;
    let placed = holders.replicas.contains(caller);
    match served {
        Some(_) if placed => Ok(()),
        Some(_) => Err(refuse(
            inner,
            Some(database),
            not_follower(database, caller),
        )),
        None => {
            let view = inner.membership.view();
            let ringed = replicas_of(&view, None, database, holders.replication_factor);
            if placed || ringed.contains(caller) {
                Err(Status::failed_precondition(format!(
                    "{database} has no primary at epoch {epoch} on this node"
                )))
            } else {
                Err(refuse(
                    inner,
                    Some(database),
                    not_follower(database, caller),
                ))
            }
        }
    }
}

fn not_follower(database: &DatabaseName, caller: &NodeId) -> BindingDenied {
    BindingDenied::new(
        "Snapshot",
        caller.clone(),
        Unbound::NotFollower {
            database: database.clone(),
        },
    )
}

/// A `Snapshot` request that seeds a fork (ADR-0046, points 4.1 and 4.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SeedingRequest {
    /// The database the request names: the source, from the seeder; the fork, from a peer.
    pub(super) database: DatabaseName,
    /// The fork identity the request names.
    pub(super) fork: ForkIdentity,
    /// The attempt it names.
    pub(super) attempt: u32,
    /// The source incarnation it names.
    pub(super) source_incarnation: Incarnation,
    /// Whether it asks the seeder for its staged copy, rather than the source's primary for a
    /// pinned copy.
    pub(super) staged: bool,
}

impl SeedingRequest {
    /// The fork `request` seeds, or `None` if it names no fork and asks for a follower's
    /// snapshot.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for an invalid name or operation ID, or a staged request whose
    /// database isn't the fork.
    pub(super) fn parse(request: &SnapshotRequest) -> Result<Option<Self>, Status> {
        let Some(fork) = request.fork.as_deref() else {
            return Ok(None);
        };
        let parse = |text: &str| -> Result<DatabaseName, Status> {
            text.parse().map_err(|error| {
                Status::invalid_argument(format!("invalid database name: {error}"))
            })
        };
        let fork = parse(fork)?;
        let database = parse(&request.database)?;
        if request.staged && database != fork {
            return Err(Status::invalid_argument(format!(
                "a request for a staged copy names the fork, '{fork}', not '{database}'"
            )));
        }
        Ok(Some(Self {
            database,
            fork: ForkIdentity::new(
                fork,
                Incarnation::new(request.fork_incarnation),
                operation_from_proto(&request.operation)?,
            ),
            attempt: request.fork_attempt,
            source_incarnation: Incarnation::new(request.source_incarnation),
            staged: request.staged,
        }))
    }
}

/// What a seeding request was granted: for a request to the source's primary, the epoch it
/// leads the source at.
#[derive(Debug, Clone, Copy)]
pub(super) struct SeedingGrant {
    pub(super) source_epoch: LeaseEpoch,
}

/// Refuses a seeding request that `caller` made: `PERMISSION_DENIED` with [`BINDING_KEY`]
/// metadata naming `seeding`, counted and audited as every binding refusal is. A plaintext
/// caller, which carries no identity to audit, gets the same status.
fn refuse_seeding(
    inner: &Inner,
    request: &SeedingRequest,
    caller: Option<&NodeId>,
    mismatch: SeedingMismatch,
) -> Status {
    let reason = Unbound::NotSeeding {
        fork: request.fork.name.clone(),
        mismatch,
    };
    match caller {
        Some(caller) => refuse(
            inner,
            Some(&request.fork.name),
            BindingDenied::new("Snapshot", caller.clone(), reason),
        ),
        None => {
            inner.binding_refused(Some(&request.fork.name));
            tracing::warn!(target: "fleetringd::audit", principal = "plaintext", rpc = "Snapshot", database = %request.fork.name, binding = Binding::Seeding.as_str(), "denied");
            let mut status = Status::permission_denied(format!(
                "Snapshot refused for a plaintext peer: {reason}"
            ));
            status.metadata_mut().insert(
                BINDING_KEY,
                MetadataValue::from_static(Binding::Seeding.as_str()),
            );
            status
        }
    }
}

/// The fork `request` names, as control state shows it, if its whole identity matches; a
/// refusal otherwise.
async fn current_fork(
    inner: &Inner,
    request: &SeedingRequest,
    caller: Option<&NodeId>,
) -> Result<(DatabaseView, ForkView), Status> {
    let refused = |mismatch| Err(refuse_seeding(inner, request, caller, mismatch));
    let view = match inner.authority.view(&request.fork.name).await {
        Ok(view) => view,
        Err(status) if status.code() == tonic::Code::NotFound => {
            return refused(SeedingMismatch::NoFork);
        }
        Err(status) => return Err(status),
    };
    let Some(fork) = view.fork.clone() else {
        return refused(SeedingMismatch::NoFork);
    };
    if fork.identity.incarnation != request.fork.incarnation {
        return refused(SeedingMismatch::ForkIncarnation);
    }
    if fork.identity.operation != request.fork.operation {
        return refused(SeedingMismatch::Operation);
    }
    Ok((view, fork))
}

/// The attempt's seeder, if `fork` is seeding exactly `attempt`; a refusal otherwise.
fn seeding_attempt(
    inner: &Inner,
    request: &SeedingRequest,
    caller: Option<&NodeId>,
    fork: &ForkView,
) -> Result<Option<NodeId>, Status> {
    match &fork.phase {
        ForkPhase::Seeding {
            attempt, seeder, ..
        } if *attempt == request.attempt => Ok(seeder.clone()),
        ForkPhase::Seeding { attempt, .. } => Err(refuse_seeding(
            inner,
            request,
            caller,
            SeedingMismatch::Attempt {
                requested: request.attempt,
                current: *attempt,
            },
        )),
        _ => Err(refuse_seeding(
            inner,
            request,
            caller,
            SeedingMismatch::NotSeeding,
        )),
    }
}

/// Checks a seeding `Snapshot` that `caller` asks this node for (ADR-0046, points 4.1 and 4.4),
/// comparing the whole fork identity, the attempt, the source, and the source incarnation with
/// this node's control-state view, before anything is pinned or read.
///
/// - **From the seeder, to the source's primary:** this node holds the source's lease at the
///   source incarnation the request names; the fork is seeding the request's attempt from that
///   source and incarnation; and the caller is the attempt's seeder.
/// - **From a replica, to the seeder** (`staged`): the fork is seeding the request's attempt;
///   this node is its seeder; and the caller is in the fork's replica set.
///
/// The identity checks run for every caller; the caller's own role is checked only for a node
/// certificate, since a plaintext peer carries no identity (ADR-0031).
///
/// # Errors
///
/// A `seeding` binding refusal, and the authority's error as it is, `UNAVAILABLE` while control
/// state can't be reached.
pub(super) async fn check_seeding(
    inner: &Inner,
    request: &SeedingRequest,
    caller: Option<&NodeId>,
) -> Result<SeedingGrant, Status> {
    let refused = |mismatch| Err(refuse_seeding(inner, request, caller, mismatch));
    // A seeder may be its source's primary, as on a node of its own, and asks itself; a node is
    // never its own peer.
    if request.staged && caller == Some(&inner.node) {
        return refused(SeedingMismatch::NotReplica);
    }
    let (view, fork) = current_fork(inner, request, caller).await?;
    if request.staged {
        // A peer may leave the source incarnation unset; one it sets must be the fork's.
        if !request.source_incarnation.is_legacy()
            && request.source_incarnation != fork.source_incarnation
        {
            return refused(SeedingMismatch::SourceIncarnation);
        }
        let seeder = seeding_attempt(inner, request, caller, &fork)?;
        if seeder.as_ref() != Some(&inner.node) {
            return refused(SeedingMismatch::NotTheSeeder);
        }
        if let Some(caller) = caller
            && !view.holders.replicas.contains(caller)
        {
            return refused(SeedingMismatch::NotReplica);
        }
        return Ok(SeedingGrant {
            source_epoch: LeaseEpoch::ZERO,
        });
    }
    if fork.source != request.database {
        return refused(SeedingMismatch::Source);
    }
    if fork.source_incarnation != request.source_incarnation {
        return refused(SeedingMismatch::SourceIncarnation);
    }
    let seeder = seeding_attempt(inner, request, caller, &fork)?;
    if let Some(caller) = caller
        && seeder.as_ref() != Some(caller)
    {
        return refused(SeedingMismatch::NotSeeder);
    }
    let source = match inner.authority.view(&request.database).await {
        Ok(source) => source,
        Err(status) if status.code() == tonic::Code::NotFound => {
            return refused(SeedingMismatch::SourceIncarnation);
        }
        Err(status) => return Err(status),
    };
    if source.incarnation != request.source_incarnation {
        return refused(SeedingMismatch::SourceIncarnation);
    }
    let epoch = source.holders.epoch;
    let leads = source.holders.holder.as_ref() == Some(&inner.node)
        && inner
            .primary(&request.database)
            .is_some_and(|primary| primary.role().epoch == epoch);
    if !leads {
        return refused(SeedingMismatch::NotSourcePrimary);
    }
    Ok(SeedingGrant {
        source_epoch: epoch,
    })
}
