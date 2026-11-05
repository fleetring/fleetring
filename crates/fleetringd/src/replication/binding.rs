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
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetring_proto::v1::LocateRequest;
use tonic::Status;

use super::Inner;
use crate::MembershipView;
use crate::auth::{Binding, BindingDenied, Unbound};
use crate::control::ControlClient;

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

/// Where a data node asks who leads an epoch.
pub trait EpochAuthority: fmt::Debug + Send + Sync + 'static {
    /// Who leads `database` now.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` when control state can't be reached, `NOT_FOUND` for an unknown database.
    fn holders(&self, database: &DatabaseName) -> HoldersFuture<'_>;
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
            let node = |text: &str| {
                text.parse::<NodeId>().map_err(|error| {
                    Status::internal(format!("the reply has an invalid node id: {error}"))
                })
            };
            let replicas = placement
                .replicas
                .iter()
                .map(|replica| node(&replica.node_id))
                .collect::<Result<Vec<_>, _>>()?;
            let replication_factor = u8::try_from(placement.replication_factor)
                .ok()
                .and_then(ReplicationFactor::new)
                .or_else(|| {
                    u8::try_from(replicas.len())
                        .ok()
                        .and_then(ReplicationFactor::new)
                })
                .unwrap_or(ReplicationFactor::ONE);
            Ok(EpochHolders {
                epoch: LeaseEpoch::new(placement.epoch),
                holder: placement.primary.as_deref().map(node).transpose()?,
                candidate: placement.candidate.as_deref().map(node).transpose()?,
                replicas,
                replication_factor,
            })
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

/// Checks a `DropCopy` naming `database` at `epoch`, whoever made it (ADR-0042): the drop stands
/// only if `epoch` is exactly the highest epoch this node's copy recorded, so the notice names
/// this copy's incarnation, and control state no longer holds the database, or holds a newer one
/// of the name at a lower epoch. A live database's control epoch equals its copies' highest, and
/// a database created again under the name restarts its epochs, so neither a forged notice nor a
/// late one for an earlier incarnation passes. A node certificate is refused with the binding;
/// a plaintext caller with `FAILED_PRECONDITION`.
pub(super) async fn check_drop(
    inner: &Inner,
    database: &DatabaseName,
    epoch: u64,
    caller: Option<&NodeId>,
) -> Result<(), Status> {
    let lease_epoch = LeaseEpoch::new(epoch);
    if inner.epochs.highest(database) == Some(epoch) {
        match inner.authority.holders(database).await {
            Ok(holders) if holders.epoch < lease_epoch => return Ok(()),
            Ok(_) => {}
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(()),
            Err(status) => return Err(status),
        }
    }
    let reason = Unbound::NotDropped {
        database: database.clone(),
        epoch: lease_epoch,
    };
    match caller {
        Some(caller) => {
            let denied = BindingDenied::new("DropCopy", caller.clone(), reason);
            Err(refuse(inner, Some(database), denied))
        }
        None => Err(Status::failed_precondition(reason.to_string())),
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
