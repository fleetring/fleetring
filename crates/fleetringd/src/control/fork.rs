//! Forks over the protocol (ADR-0046): what replicas report about forks in heartbeats, a fork's
//! status in a placement, and a fork operation's status.
//!
//! Incarnations travel as `fixed64` and operation IDs as 16 bytes; text prints them as 16
//! lowercase hex digits and 26 lowercase base32 characters (ADR-0046, points 1.4 and 5.1).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use fleetring::NodeId;
use fleetring_control::fork::{
    CancelCause, ChainHash, CopyLossReason, CopyLost, Fork, ForkExpiry, ForkFailure, ForkIdentity,
    ForkIntent, ForkState, OperationId, OperationOutcome, OperationRecord, ResetReport, RetryCause,
    SeedFailure, SeedFailureReason, StagedCopy, completion_quorum,
};
use fleetring_control::{
    Database, DatabaseName, HostedReplica, Incarnation, LeaseEpoch, Position, ReplicaRole, Report,
    State,
};
use fleetring_proto::v1 as proto;
use fleetring_proto::v1::metadata_spec::Expiry as SpecExpiry;
use tonic::Status;

use crate::data::convert::{acks_to_proto, labels_to_proto, parse};

/// How many bytes an operation ID takes on the wire.
const OPERATION_ID_BYTES: usize = 16;

/// The operation ID `bytes` carry.
///
/// # Errors
///
/// `INVALID_ARGUMENT` unless `bytes` holds exactly 16 bytes.
pub(crate) fn operation_from_proto(bytes: &[u8]) -> Result<OperationId, Status> {
    let bytes: [u8; OPERATION_ID_BYTES] = bytes.try_into().map_err(|_| {
        Status::invalid_argument(format!(
            "invalid operation ID: expected {OPERATION_ID_BYTES} bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(OperationId::from_bytes(bytes))
}

/// The bytes an operation ID travels as.
pub(crate) fn operation_to_proto(operation: OperationId) -> Vec<u8> {
    operation.to_bytes().to_vec()
}

/// How far a replica has streamed a copy of a fork, as it reports it in a heartbeat (ADR-0046,
/// point 4.5). It is for status only: nothing is decided by it, so control state doesn't keep
/// it, and the control node shows the seeder's in the fork's placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedProgress {
    /// The fork the copy is of.
    pub fork: ForkIdentity,
    /// The attempt that streams it.
    pub attempt: u32,
    /// How many bytes have arrived.
    pub bytes: u64,
    /// The copy's size, or 0 until the stream's first chunk says.
    pub total_bytes: u64,
}

/// The role a heartbeat reports.
pub(super) fn role_from_proto(role: i32, database: &str) -> Result<ReplicaRole, Status> {
    match proto::ReplicaRole::try_from(role) {
        Ok(proto::ReplicaRole::Primary) => Ok(ReplicaRole::Primary),
        Ok(proto::ReplicaRole::Follower) => Ok(ReplicaRole::Follower),
        _ => Err(Status::invalid_argument(format!(
            "database '{database}' has no valid role"
        ))),
    }
}

/// The protocol form of a replica's role.
pub(super) fn role_to_proto(role: ReplicaRole) -> proto::ReplicaRole {
    match role {
        ReplicaRole::Primary => proto::ReplicaRole::Primary,
        ReplicaRole::Follower => proto::ReplicaRole::Follower,
        _ => proto::ReplicaRole::Unspecified,
    }
}

/// What one database's status in a heartbeat says: the replica as control state records it,
/// with what it reports about a fork of that name, and the progress of a copy it streams, which
/// control state doesn't keep.
///
/// A copy-loss reason this build doesn't know reads as damaged, since the copy is lost either
/// way; a seed failure whose reason it doesn't know reads as a broken transfer if the node calls
/// it retryable, and is left out otherwise, so that a newer node's report never fails a fork
/// for a reason this control node can't name.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for an invalid database name or role, or an operation ID that isn't 16
/// bytes.
pub(crate) fn replica_from_proto(
    status: proto::DatabaseStatus,
) -> Result<(HostedReplica, Option<SeedProgress>), Status> {
    let role = role_from_proto(status.role, &status.database)?;
    let database: DatabaseName = parse("database name", &status.database)?;
    let identity = |incarnation: u64, operation: &[u8]| -> Result<ForkIdentity, Status> {
        Ok(ForkIdentity::new(
            database.clone(),
            Incarnation::new(incarnation),
            operation_from_proto(operation)?,
        ))
    };
    let mut replica = HostedReplica::new(database.clone(), role, Position::new(status.position))
        .with_log_epoch(LeaseEpoch::new(status.epoch));
    if let Some(size_bytes) = status.size_bytes {
        replica = replica.with_size_bytes(size_bytes);
    }
    if let Some(staged) = status.staged {
        replica = replica.with_staged(StagedCopy::new(
            identity(staged.fork_incarnation, &staged.operation)?,
            staged.attempt,
            Position::new(staged.position),
            ChainHash::new(staged.chain_hash),
            Incarnation::new(staged.source_incarnation),
        ));
    }
    if let Some(lost) = status.copy_lost {
        let reason = match proto::CopyLossReason::try_from(lost.reason) {
            Ok(proto::CopyLossReason::Missing) => CopyLossReason::Missing,
            _ => CopyLossReason::Damaged,
        };
        replica = replica.with_copy_lost(CopyLost::new(
            identity(lost.fork_incarnation, &lost.operation)?,
            lost.attempt,
            reason,
        ));
    }
    if let Some(failure) = status.seed_failure {
        let fork = identity(failure.fork_incarnation, &failure.operation)?;
        match seed_failure_reason(failure.reason, failure.retryable) {
            Some(reason) => {
                replica =
                    replica.with_seed_failure(SeedFailure::new(fork, failure.attempt, reason));
            }
            None => tracing::warn!(
                %fork,
                attempt = failure.attempt,
                reason = failure.reason,
                "a replica reported a seed failure for a reason this release doesn't know; \
                 ignored"
            ),
        }
    }
    if let Some(reset) = status.reset {
        let fork = identity(reset.fork_incarnation, &reset.operation)?;
        let epoch = LeaseEpoch::new(reset.epoch);
        let position = Position::new(reset.position);
        replica = replica.with_reset(if reset.committed {
            ResetReport::committed(fork, epoch, position)
        } else {
            ResetReport::running(fork, epoch, position)
        });
    }
    let progress = status
        .seed_progress
        .map(|progress| {
            Ok::<_, Status>(SeedProgress {
                fork: identity(progress.fork_incarnation, &progress.operation)?,
                attempt: progress.attempt,
                bytes: progress.bytes,
                total_bytes: progress.total_bytes,
            })
        })
        .transpose()?;
    Ok((replica, progress))
}

/// The seed failure reason a report names, if this build knows it or the node calls the failure
/// retryable.
fn seed_failure_reason(reason: i32, retryable: bool) -> Option<SeedFailureReason> {
    match proto::SeedFailureReason::try_from(reason) {
        Ok(proto::SeedFailureReason::InsufficientDisk) => Some(SeedFailureReason::InsufficientDisk),
        Ok(proto::SeedFailureReason::SourceTooLarge) => Some(SeedFailureReason::SourceTooLarge),
        Ok(proto::SeedFailureReason::EngineUnsupported) => {
            Some(SeedFailureReason::EngineUnsupported)
        }
        Ok(proto::SeedFailureReason::TransferFailed) => Some(SeedFailureReason::TransferFailed),
        Ok(proto::SeedFailureReason::PinExpired) => Some(SeedFailureReason::PinExpired),
        Ok(proto::SeedFailureReason::Unspecified) | Err(_) => {
            retryable.then_some(SeedFailureReason::TransferFailed)
        }
    }
}

#[cfg_attr(
    not(any(test, feature = "simulation")),
    expect(
        dead_code,
        reason = "the simulation's reports use it until data nodes report forks (FR-085, stage C)"
    )
)]
fn seed_failure_reason_to_proto(reason: SeedFailureReason) -> proto::SeedFailureReason {
    match reason {
        SeedFailureReason::InsufficientDisk => proto::SeedFailureReason::InsufficientDisk,
        SeedFailureReason::SourceTooLarge => proto::SeedFailureReason::SourceTooLarge,
        SeedFailureReason::EngineUnsupported => proto::SeedFailureReason::EngineUnsupported,
        SeedFailureReason::TransferFailed => proto::SeedFailureReason::TransferFailed,
        SeedFailureReason::PinExpired => proto::SeedFailureReason::PinExpired,
        _ => proto::SeedFailureReason::Unspecified,
    }
}

/// The status a heartbeat carries for `replica`, with `progress` if it streams a copy: what a
/// data node sends, and what [`replica_from_proto`] reads back.
#[cfg_attr(
    not(any(test, feature = "simulation")),
    expect(
        dead_code,
        reason = "the simulation's reports use it until data nodes report forks (FR-085, stage C)"
    )
)]
pub(crate) fn replica_to_proto(
    replica: &HostedReplica,
    progress: Option<&SeedProgress>,
) -> proto::DatabaseStatus {
    let incarnation = |fork: &ForkIdentity| fork.incarnation.get();
    let operation = |fork: &ForkIdentity| operation_to_proto(fork.operation);
    proto::DatabaseStatus {
        database: replica.database().to_string(),
        role: role_to_proto(replica.role()).into(),
        epoch: replica.log_epoch().get(),
        position: replica.position().as_bytes().to_vec(),
        staged: replica.staged().map(|staged| proto::StagedCopy {
            fork_incarnation: incarnation(staged.fork()),
            operation: operation(staged.fork()),
            attempt: staged.attempt(),
            position: staged.position().as_bytes().to_vec(),
            chain_hash: staged.chain_hash().as_bytes().to_vec(),
            source_incarnation: staged.source_incarnation().get(),
        }),
        copy_lost: replica.copy_lost().map(|lost| proto::CopyLost {
            fork_incarnation: incarnation(lost.fork()),
            operation: operation(lost.fork()),
            attempt: lost.attempt(),
            reason: match lost.reason() {
                CopyLossReason::Missing => proto::CopyLossReason::Missing,
                _ => proto::CopyLossReason::Damaged,
            }
            .into(),
        }),
        seed_failure: replica.seed_failure().map(|failure| proto::SeedFailure {
            fork_incarnation: incarnation(failure.fork()),
            operation: operation(failure.fork()),
            attempt: failure.attempt(),
            reason: seed_failure_reason_to_proto(failure.reason()).into(),
            retryable: failure.reason().is_retryable(),
        }),
        reset: replica.reset().map(|reset| proto::ForkReset {
            fork_incarnation: incarnation(reset.fork()),
            operation: operation(reset.fork()),
            epoch: reset.epoch().get(),
            position: reset.position().as_bytes().to_vec(),
            committed: reset.is_committed(),
        }),
        seed_progress: progress.map(|progress| proto::SeedProgress {
            fork_incarnation: incarnation(&progress.fork),
            operation: operation(&progress.fork),
            attempt: progress.attempt,
            bytes: progress.bytes,
            total_bytes: progress.total_bytes,
        }),
        size_bytes: replica.size_bytes(),
    }
}

/// What each node last reported about the copies it streams, for status: the control node
/// keeps it in memory, beside control state, and loses it on a restart until the next
/// heartbeats.
#[derive(Debug, Clone, Default)]
pub(crate) struct SeedProgressBoard {
    inner: Arc<Mutex<BTreeMap<(NodeId, DatabaseName), SeedProgress>>>,
}

impl SeedProgressBoard {
    /// Replaces what `node` reported with `reports`, and forgets what nodes that are no longer
    /// members of `state` reported.
    pub(crate) fn record(
        &self,
        state: &State,
        node: &NodeId,
        reports: impl IntoIterator<Item = SeedProgress>,
    ) {
        let mut board = self.inner.lock().expect("seed progress lock poisoned");
        board.retain(|(reporter, _), _| reporter != node && state.member(reporter).is_some());
        for progress in reports {
            board.insert((node.clone(), progress.fork.name.clone()), progress);
        }
    }

    /// What `node` last reported about its copy of `fork` in `attempt`, if anything.
    fn of(&self, node: &NodeId, fork: &ForkIdentity, attempt: u32) -> Option<SeedProgress> {
        let board = self.inner.lock().expect("seed progress lock poisoned");
        board
            .get(&(node.clone(), fork.name.clone()))
            .filter(|progress| progress.fork == *fork && progress.attempt == attempt)
            .cloned()
    }
}

fn names(nodes: &BTreeSet<NodeId>) -> Vec<String> {
    nodes.iter().map(ToString::to_string).collect()
}

fn fork_state_to_proto(state: &ForkState) -> proto::ForkState {
    match state {
        ForkState::Seeding { .. } => proto::ForkState::Seeding,
        ForkState::Seeded { .. } => proto::ForkState::Seeded,
        ForkState::Ready { .. } => proto::ForkState::Ready,
        ForkState::Failed { .. } => proto::ForkState::Failed,
        _ => proto::ForkState::Unspecified,
    }
}

fn retry_cause_to_proto(cause: RetryCause) -> proto::RetryCause {
    match cause {
        RetryCause::SeederDown => proto::RetryCause::SeederDown,
        RetryCause::AttemptTimedOut => proto::RetryCause::AttemptTimedOut,
        RetryCause::TransferFailed => proto::RetryCause::TransferFailed,
        RetryCause::PinExpired => proto::RetryCause::PinExpired,
        RetryCause::HolderLost => proto::RetryCause::HolderLost,
        RetryCause::HoldersLeft => proto::RetryCause::HoldersLeft,
        RetryCause::SeederDiskRefused => proto::RetryCause::SeederDiskRefused,
        _ => proto::RetryCause::Unspecified,
    }
}

/// The protocol form of a fork's failure, after `attempts` attempts, or 0 if unknown.
fn failure_to_proto(failure: ForkFailure, attempts: u32) -> proto::ForkFailure {
    let (reason, last) = match failure {
        ForkFailure::SourceGone => (proto::ForkFailureReason::SourceGone, None),
        ForkFailure::SourceRecreated => (proto::ForkFailureReason::SourceRecreated, None),
        ForkFailure::RetriesExhausted { last } => {
            (proto::ForkFailureReason::RetriesExhausted, Some(last))
        }
        ForkFailure::DeadlineExceeded { last } => {
            (proto::ForkFailureReason::DeadlineExceeded, Some(last))
        }
        ForkFailure::SourceTooLarge => (proto::ForkFailureReason::SourceTooLarge, None),
        ForkFailure::InsufficientDisk => (proto::ForkFailureReason::InsufficientDisk, None),
        ForkFailure::EngineUnsupported => (proto::ForkFailureReason::EngineUnsupported, None),
        _ => (proto::ForkFailureReason::Unspecified, None),
    };
    proto::ForkFailure {
        reason: reason.into(),
        last_cause: last
            .map_or(proto::RetryCause::Unspecified, retry_cause_to_proto)
            .into(),
        attempts,
    }
}

/// The replicas of `db`, a seeding fork, whose latest report is the staged copy of `fork` that
/// `attempt` fetched, leaving out those that refused it for want of disk: the holders a
/// completion would name now.
fn staged_holders(
    state: &State,
    db: &Database,
    fork: &ForkIdentity,
    attempt: u32,
    disk_refused: &BTreeSet<NodeId>,
) -> Vec<String> {
    let replicas = state.replica_set(db.name()).unwrap_or_default();
    replicas
        .iter()
        .filter(|id| !disk_refused.contains(*id))
        .filter(|id| {
            state
                .report(id, db.name())
                .and_then(Report::staged)
                .is_some_and(|staged| staged.fork() == fork && staged.attempt() == attempt)
        })
        .map(ToString::to_string)
        .collect()
}

/// `db`'s fork record as a placement carries it, with the seeder's progress from `progress`.
pub(crate) fn fork_status(
    state: &State,
    db: &Database,
    fork: &Fork,
    progress: &SeedProgressBoard,
) -> proto::ForkStatus {
    let identity = ForkIdentity::new(db.name().clone(), db.incarnation(), fork.operation());
    let mut status = proto::ForkStatus {
        state: fork_state_to_proto(fork.state()).into(),
        operation_id: operation_to_proto(fork.operation()),
        source: fork.source().to_string(),
        source_incarnation: fork.source_incarnation().get(),
        max_attempts: fork.limits().max_attempts,
        holders_needed: u32::from(completion_quorum(db.replication_factor(), db.durability())),
        requested_by: fork.requested_by().map(ToString::to_string),
        requested_at_ms: fork.requested_at().as_millis(),
        ..proto::ForkStatus::default()
    };
    match fork.state() {
        ForkState::Seeding {
            attempt,
            seeder,
            seeder_excluded,
            disk_refused,
            deadline,
            last_cause,
            ..
        } => {
            status.attempt = *attempt;
            status.seeder = Some(seeder.to_string());
            status.seeder_excluded = names(seeder_excluded);
            status.disk_refused = names(disk_refused);
            status.holders = staged_holders(state, db, &identity, *attempt, disk_refused);
            status.deadline_at_ms = Some(deadline.as_millis());
            status.last_cause = last_cause
                .map_or(proto::RetryCause::Unspecified, retry_cause_to_proto)
                .into();
            if let Some(progress) = progress.of(seeder, &identity, *attempt) {
                status.bytes = Some(progress.bytes);
                status.total_bytes = Some(progress.total_bytes);
            }
        }
        ForkState::Seeded {
            attempt,
            at,
            chain_hash,
            holders,
            completed_at,
            ..
        } => {
            status.attempt = *attempt;
            status.holders = names(holders);
            status.position = Some(at.as_bytes().to_vec());
            status.chain_hash = Some(chain_hash.as_bytes().to_vec());
            status.completed_at_ms = Some(completed_at.as_millis());
        }
        ForkState::Ready {
            at,
            chain_hash,
            completed_at,
            reset_epoch,
            reset_position,
            ready_at,
        } => {
            status.position = Some(at.as_bytes().to_vec());
            status.chain_hash = Some(chain_hash.as_bytes().to_vec());
            status.completed_at_ms = Some(completed_at.as_millis());
            status.ready_at_ms = Some(ready_at.as_millis());
            status.reset_epoch = Some(reset_epoch.get());
            status.reset_position = Some(reset_position.as_bytes().to_vec());
        }
        ForkState::Failed {
            reason,
            attempts,
            failed_at,
        } => {
            status.attempt = *attempts;
            status.failure = Some(failure_to_proto(*reason, *attempts));
            status.failed_at_ms = Some(failed_at.as_millis());
        }
        _ => {}
    }
    status
}

/// The request `intent` records, under `operation`.
fn intent_to_proto(operation: OperationId, intent: &ForkIntent) -> proto::ForkDatabaseRequest {
    proto::ForkDatabaseRequest {
        operation_id: operation_to_proto(operation),
        database: intent.name.to_string(),
        source: intent.source.to_string(),
        expect_source_incarnation: intent.expect_source_incarnation.map(Incarnation::get),
        replication_factor: u32::from(intent.replication_factor.get()),
        acks: intent
            .durability
            .map_or(proto::Acks::Unspecified, |durability| {
                acks_to_proto(durability.acks())
            })
            .into(),
        min_replicas: intent
            .durability
            .map_or(0, |durability| u32::from(durability.min_replicas())),
        metadata: Some(proto::MetadataSpec {
            description: intent
                .description
                .as_ref()
                .map(|text| text.as_str().to_owned())
                .unwrap_or_default(),
            labels: labels_to_proto(&intent.labels),
            expiry: match intent.expiry {
                ForkExpiry::After(span) => Some(SpecExpiry::ExpiresInMs(span.get())),
                ForkExpiry::At(at) => Some(SpecExpiry::ExpiresAtMs(at.as_millis())),
                _ => None,
            },
        }),
    }
}

/// Operation `operation`'s status, from its `record`, with the fork's `placement` while its
/// name still holds the operation's incarnation.
pub(crate) fn operation_status(
    state: &State,
    operation: OperationId,
    record: &OperationRecord,
    placement: Option<proto::Placement>,
) -> proto::ForkOperationStatus {
    let intent = record.intent();
    // The current fork, if the name still holds the database the operation made.
    let fork = state
        .database(record.target())
        .filter(|db| db.incarnation() == record.target_incarnation())
        .and_then(Database::fork)
        .filter(|fork| fork.operation() == operation);
    let mut status = proto::ForkOperationStatus {
        operation_id: operation_to_proto(operation),
        database: record.target().to_string(),
        incarnation: record.target_incarnation().get(),
        source: intent.source.to_string(),
        source_incarnation: record.source_incarnation().get(),
        updated_at_ms: record.updated_at().as_millis(),
        finished_at_ms: record.finished_at().map(|at| at.as_millis()),
        forgotten_at_ms: record.forgotten_at().map(|at| at.as_millis()),
        request: Some(intent_to_proto(operation, intent)),
        requested_by: intent.requested_by.as_ref().map(ToString::to_string),
        expires_at_ms: record.expires_at().map(|at| at.as_millis()),
        placement,
        ..proto::ForkOperationStatus::default()
    };
    let state = match record.outcome() {
        OperationOutcome::Pending { attempt } => {
            status.attempt = *attempt;
            proto::ForkState::Seeding
        }
        OperationOutcome::Seeded => proto::ForkState::Seeded,
        OperationOutcome::Ready { at, chain_hash } => {
            status.position = Some(at.as_bytes().to_vec());
            status.chain_hash = Some(chain_hash.as_bytes().to_vec());
            proto::ForkState::Ready
        }
        OperationOutcome::Failed { reason } => {
            let attempts = match fork.map(Fork::state) {
                Some(ForkState::Failed { attempts, .. }) => *attempts,
                _ => 0,
            };
            status.failure = Some(failure_to_proto(*reason, attempts));
            proto::ForkState::Failed
        }
        OperationOutcome::Cancelled { cause } => {
            status.cancel_cause = match cause {
                CancelCause::Dropped => proto::ForkCancelCause::Dropped,
                CancelCause::Expired => proto::ForkCancelCause::Expired,
                _ => proto::ForkCancelCause::Unspecified,
            }
            .into();
            proto::ForkState::Cancelled
        }
        _ => proto::ForkState::Unspecified,
    };
    status.state = state.into();
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(name: &str, incarnation: u64, operation: u8) -> ForkIdentity {
        ForkIdentity::new(
            name.parse().expect("a name"),
            Incarnation::new(incarnation),
            OperationId::from_bytes([operation; 16]),
        )
    }

    #[test]
    fn every_report_field_round_trips_through_a_heartbeat() {
        let fork = identity("t", 0x3F9A_0C6E_21D4_B870, 7);
        let replica = HostedReplica::new(
            fork.name.clone(),
            ReplicaRole::Follower,
            Position::new([1, 2, 3]),
        )
        .with_log_epoch(LeaseEpoch::new(4))
        .with_size_bytes(123_456)
        .with_staged(StagedCopy::new(
            fork.clone(),
            2,
            Position::new([9, 9]),
            ChainHash::new([5, 6, 7]),
            Incarnation::new(u64::MAX),
        ))
        .with_copy_lost(CopyLost::new(fork.clone(), 2, CopyLossReason::Missing))
        .with_seed_failure(SeedFailure::new(
            fork.clone(),
            2,
            SeedFailureReason::PinExpired,
        ))
        .with_reset(ResetReport::committed(
            fork.clone(),
            LeaseEpoch::new(3),
            Position::new([8]),
        ));
        let progress = SeedProgress {
            fork: fork.clone(),
            attempt: 2,
            bytes: 41 << 20,
            total_bytes: 120 << 20,
        };
        let sent = replica_to_proto(&replica, Some(&progress));
        assert_eq!(
            sent.staged.as_ref().map(|staged| staged.source_incarnation),
            Some(u64::MAX)
        );
        let (received, reported) = replica_from_proto(sent).expect("a valid status");
        assert_eq!(received, replica);
        assert_eq!(reported, Some(progress));

        // Every reason, and a reset still running, survive too.
        for reason in [
            SeedFailureReason::InsufficientDisk,
            SeedFailureReason::SourceTooLarge,
            SeedFailureReason::EngineUnsupported,
            SeedFailureReason::TransferFailed,
            SeedFailureReason::PinExpired,
        ] {
            let replica =
                HostedReplica::new(fork.name.clone(), ReplicaRole::Primary, Position::new([1]))
                    .with_seed_failure(SeedFailure::new(fork.clone(), 1, reason))
                    .with_copy_lost(CopyLost::new(fork.clone(), 1, CopyLossReason::Damaged))
                    .with_reset(ResetReport::running(
                        fork.clone(),
                        LeaseEpoch::new(1),
                        Position::new([2]),
                    ));
            let (received, progress) =
                replica_from_proto(replica_to_proto(&replica, None)).expect("valid");
            assert_eq!(received, replica, "{reason:?}");
            assert_eq!(progress, None);
        }

        // A status without fork fields, as an older node sends, reads as none.
        let plain = HostedReplica::new(fork.name, ReplicaRole::Primary, Position::new([1]));
        let (received, _) = replica_from_proto(replica_to_proto(&plain, None)).expect("valid");
        assert_eq!(received, plain);
    }

    #[test]
    fn reasons_this_build_doesnt_know_are_read_conservatively() {
        let status = |reason: i32, retryable: bool| proto::DatabaseStatus {
            database: "t".to_owned(),
            role: proto::ReplicaRole::Follower.into(),
            seed_failure: Some(proto::SeedFailure {
                fork_incarnation: 1,
                operation: vec![1; 16],
                attempt: 1,
                reason,
                retryable,
            }),
            copy_lost: Some(proto::CopyLost {
                fork_incarnation: 1,
                operation: vec![1; 16],
                attempt: 1,
                reason: 99,
            }),
            ..proto::DatabaseStatus::default()
        };
        let (retryable, _) = replica_from_proto(status(99, true)).expect("valid");
        assert_eq!(
            retryable.seed_failure().map(SeedFailure::reason),
            Some(SeedFailureReason::TransferFailed)
        );
        assert_eq!(
            retryable.copy_lost().map(CopyLost::reason),
            Some(CopyLossReason::Damaged)
        );
        let (permanent, _) = replica_from_proto(status(99, false)).expect("valid");
        assert_eq!(permanent.seed_failure(), None);
    }

    #[test]
    fn an_operation_id_is_sixteen_bytes() {
        assert_eq!(
            operation_from_proto(&[3; 16]).ok(),
            Some(OperationId::from_bytes([3; 16]))
        );
        for bytes in [&[][..], &[1; 15], &[1; 17]] {
            let refused = operation_from_proto(bytes).expect_err("not 16 bytes");
            assert_eq!(refused.code(), tonic::Code::InvalidArgument);
        }
        let refused = replica_from_proto(proto::DatabaseStatus {
            database: "t".to_owned(),
            role: proto::ReplicaRole::Follower.into(),
            staged: Some(proto::StagedCopy {
                operation: vec![1; 3],
                ..proto::StagedCopy::default()
            }),
            ..proto::DatabaseStatus::default()
        })
        .expect_err("a short operation ID");
        assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    }
}
