//! A data node's view of a database and of the fork that made it, as control state's placement
//! shows it (ADR-0046): what the `seeding` binding, the seeder, and the opening of a fork judge
//! by. Positions and chain hashes stay opaque bytes here, as everywhere above the adapter.

use std::collections::BTreeSet;

use fleetring::NodeId;
use fleetring_control::fork::{ForkIdentity, OperationId};
use fleetring_control::{DatabaseName, Incarnation, LeaseEpoch};
use fleetring_proto::v1 as proto;
use tonic::Status;

use crate::control::fork::operation_from_proto;
use crate::engine::Position;
use crate::replication::{ChainHash, EpochHolders};

/// A database as control state shows it: who may lead it, its incarnation, and its fork record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseView {
    /// Who leads it, and its replica set.
    pub holders: EpochHolders,
    /// Its incarnation; [`Incarnation::LEGACY`] from a control node that predates forks.
    pub incarnation: Incarnation,
    /// The fork that made it, if one did.
    pub fork: Option<ForkView>,
}

/// A fork's record, as a placement carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkView {
    /// The fork identity: its name, its incarnation, and its operation.
    pub identity: ForkIdentity,
    /// The database it copies.
    pub source: DatabaseName,
    /// The source incarnation it copies.
    pub source_incarnation: Incarnation,
    /// Where it stands.
    pub phase: ForkPhase,
}

/// Where a fork stands, with what each state carries that a data node acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ForkPhase {
    /// An attempt copies the source.
    Seeding {
        /// The attempt, from 1.
        attempt: u32,
        /// The attempt's seeder.
        seeder: Option<NodeId>,
        /// The replicas that refused the copy for want of disk.
        disk_refused: BTreeSet<NodeId>,
    },
    /// A completion quorum holds the copy; the fork-reset write hasn't been recorded yet.
    Seeded {
        /// The attempt that fetched the copy.
        attempt: u32,
        /// The source position the copy holds.
        at: Position,
        /// The source's chain hash there.
        chain_hash: ChainHash,
        /// The recorded holders: the fork's replica set until it is ready.
        holders: BTreeSet<NodeId>,
        /// The fork-reset write its primary reports committed at the bootstrap quorum, if it
        /// does yet: the epoch and the position.
        reset: Option<(LeaseEpoch, Position)>,
    },
    /// The reset is committed: the fork is an ordinary database.
    Ready {
        /// The recorded holders it keeps as its replica set until control state releases them,
        /// once its primary reports that placement's write quorum holds its log; `None` once
        /// released, and from a control node that predates keeping them (ADR-0046, point 3.10,
        /// as built).
        holders: Option<BTreeSet<NodeId>>,
    },
    /// The fork failed for good.
    Failed,
    /// A state this build doesn't know.
    Unknown,
}

impl ForkView {
    /// The attempt the fork is seeding, or fetched its copy in.
    #[must_use]
    pub fn attempt(&self) -> Option<u32> {
        match &self.phase {
            ForkPhase::Seeding { attempt, .. } | ForkPhase::Seeded { attempt, .. } => {
                Some(*attempt)
            }
            _ => None,
        }
    }

    /// Whether the fork isn't ready yet: seeding or seeded.
    #[must_use]
    pub fn is_pending(&self) -> bool {
        matches!(
            self.phase,
            ForkPhase::Seeding { .. } | ForkPhase::Seeded { .. }
        )
    }

    /// Whether the fork is ready but keeps its recorded holders as its replica set.
    #[must_use]
    pub fn keeps_holders(&self) -> bool {
        matches!(self.phase, ForkPhase::Ready { holders: Some(_) })
    }
}

fn node(text: &str) -> Result<NodeId, Status> {
    text.parse()
        .map_err(|error| Status::internal(format!("the placement has an invalid node id: {error}")))
}

fn nodes(texts: &[String]) -> Result<BTreeSet<NodeId>, Status> {
    texts.iter().map(|text| node(text)).collect()
}

/// The fork record `status`, of the database `name` at `incarnation`, as a view.
///
/// # Errors
///
/// `INTERNAL` if the record is malformed: an invalid operation ID, source name, node ID, or
/// chain hash.
pub(crate) fn fork_from_proto(
    name: &DatabaseName,
    incarnation: Incarnation,
    status: &proto::ForkStatus,
) -> Result<ForkView, Status> {
    let malformed = |what: &str| Status::internal(format!("the fork record has {what}"));
    let operation: OperationId = operation_from_proto(&status.operation_id)
        .map_err(|_| malformed("an invalid operation ID"))?;
    let source: DatabaseName = status
        .source
        .parse()
        .map_err(|_| malformed("an invalid source name"))?;
    let phase = match proto::ForkState::try_from(status.state) {
        Ok(proto::ForkState::Seeding) => ForkPhase::Seeding {
            attempt: status.attempt,
            seeder: status.seeder.as_deref().map(node).transpose()?,
            disk_refused: nodes(&status.disk_refused)?,
        },
        Ok(proto::ForkState::Seeded) => ForkPhase::Seeded {
            attempt: status.attempt,
            at: Position::from(status.position.clone().unwrap_or_default()),
            chain_hash: status
                .chain_hash
                .as_deref()
                .and_then(ChainHash::from_bytes)
                .ok_or_else(|| malformed("no valid chain hash"))?,
            holders: nodes(&status.holders)?,
            reset: status
                .reset_epoch
                .zip(status.reset_position.clone())
                .map(|(epoch, position)| (LeaseEpoch::new(epoch), Position::from(position))),
        },
        Ok(proto::ForkState::Ready) => ForkPhase::Ready {
            holders: (!status.holders.is_empty())
                .then(|| nodes(&status.holders))
                .transpose()?,
        },
        Ok(proto::ForkState::Failed) => ForkPhase::Failed,
        _ => ForkPhase::Unknown,
    };
    Ok(ForkView {
        identity: ForkIdentity::new(name.clone(), incarnation, operation),
        source,
        source_incarnation: Incarnation::new(status.source_incarnation),
        phase,
    })
}

/// `placement` as a view.
///
/// # Errors
///
/// `INTERNAL` if the placement is malformed.
pub(crate) fn view_from_placement(placement: &proto::Placement) -> Result<DatabaseView, Status> {
    let name: DatabaseName = placement.database.parse().map_err(|error| {
        Status::internal(format!("the placement names no valid database: {error}"))
    })?;
    let incarnation = Incarnation::new(placement.incarnation);
    Ok(DatabaseView {
        holders: holders_from_placement(placement)?,
        incarnation,
        fork: placement
            .fork
            .as_ref()
            .map(|fork| fork_from_proto(&name, incarnation, fork))
            .transpose()?,
    })
}

/// Who leads the database `placement` names, and its replica set.
///
/// # Errors
///
/// `INTERNAL` for an invalid node ID.
pub(crate) fn holders_from_placement(placement: &proto::Placement) -> Result<EpochHolders, Status> {
    let replicas = placement
        .replicas
        .iter()
        .map(|replica| node(&replica.node_id))
        .collect::<Result<Vec<_>, _>>()?;
    let replication_factor = u8::try_from(placement.replication_factor)
        .ok()
        .and_then(fleetring::ReplicationFactor::new)
        .or_else(|| {
            u8::try_from(replicas.len())
                .ok()
                .and_then(fleetring::ReplicationFactor::new)
        })
        .unwrap_or(fleetring::ReplicationFactor::ONE);
    Ok(EpochHolders {
        epoch: LeaseEpoch::new(placement.epoch),
        holder: placement.primary.as_deref().map(node).transpose()?,
        candidate: placement.candidate.as_deref().map(node).transpose()?,
        replicas,
        replication_factor,
    })
}
