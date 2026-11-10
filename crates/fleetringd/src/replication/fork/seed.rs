//! Fetching a fork's copy (ADR-0046, points 4.1, 4.3, 4.4, and 7): the attempt's seeder from
//! the source's primary, every other replica from the seeder, each staging it durably under the
//! fork identity before it reports it.
//!
//! - **Admission** happens once the stream's first chunk says how big the copy is: the node
//!   reserves the copy and a tenth more, or refuses for want of disk and reports it.
//! - **Retries.** The seeder fetches again on a broken transfer, from the source's current
//!   primary, up to `[fork] transfer_retries` times with a doubling backoff, then reports the
//!   attempt failed. A refusal that carries a retry-after, a source whose acknowledged position
//!   isn't in its log yet, or one holding as many pins as it may, costs no retry. A pin that
//!   expires ends the attempt at once, reported as such. A peer waits for the seeder for as long
//!   as the attempt lasts.
//! - Every fetch stops as soon as control state no longer shows its attempt for this node.

use std::sync::Arc;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::fork::{SeedFailure, SeedFailureReason};
use fleetring_control::{DatabaseName, Incarnation};
use fleetring_proto::v1::{SnapshotChunk, SnapshotRequest};
use tokio::time;
use tonic::{Code, Status};

use super::super::Inner;
use super::super::archive::Unpacker;
use super::super::chain::ChainHash;
use super::super::follower::blocking;
use super::super::snapshot::check_copy;
use super::faults::FaultPoint;
use super::staged::{CommitStep, Sidecar, StagedFile, StagedKey};
use super::{ForkPhase, Refused, Reservation, retry_after};
use crate::control::SeedProgress;
use crate::control::fork::operation_to_proto;
use crate::engine::Position;
use crate::transport::replication_client;

/// The metadata key an `ABORTED` seeding stream carries when its pin expired.
pub(crate) const PIN_EXPIRED_KEY: &str = "fleetring-pin-expired";

/// The longest a peer waits between two requests to a seeder that isn't ready.
const PEER_BACKOFF_CAP: Duration = Duration::from_secs(5);

/// Where a node fetches a fork's copy from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SeedFrom {
    /// The source's primary: this node is the attempt's seeder.
    Source,
    /// The attempt's seeder, which has staged its copy.
    Seeder(NodeId),
}

/// A copy a node fetches: the fork identity, the attempt, the source, and where from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeedPlan {
    pub(crate) key: StagedKey,
    pub(crate) source: DatabaseName,
    pub(crate) source_incarnation: Incarnation,
    pub(crate) from: SeedFrom,
}

/// How one fetch ended, when it didn't stage the copy.
#[derive(Debug)]
enum Fetch {
    /// The node hasn't room for the copy.
    Refused(Refused),
    /// Ask again after this long; it costs no retry.
    Again(Duration),
    /// The source's pin expired.
    PinExpired(Status),
    /// The transfer broke.
    Failed(Status),
}

impl From<Status> for Fetch {
    fn from(status: Status) -> Self {
        if status.code() == Code::Aborted && status.metadata().get(PIN_EXPIRED_KEY).is_some() {
            return Self::PinExpired(status);
        }
        match retry_after(&status) {
            Some(after) => Self::Again(after),
            None => Self::Failed(status),
        }
    }
}

/// Fetches and stages the copy `plan` names, retrying as the [module documentation](self) says,
/// and reports how it ended.
pub(in crate::replication) async fn run(inner: Arc<Inner>, plan: SeedPlan) {
    let forks = &inner.forks;
    let config = forks.config();
    let name = plan.key.fork.name.clone();
    let mut spent = 0_u32;
    let mut backoff = config.transfer_backoff();
    let failure = loop {
        match still_current(&inner, &plan).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!(fork = %plan.key.fork, attempt = plan.key.attempt, "the attempt is over; stopped fetching its copy");
                forks.report_progress(&name, None);
                return;
            }
            Err(status) => {
                tracing::debug!(fork = %plan.key.fork, %status, "can't ask control state about the fork; asking again");
                time::sleep(backoff).await;
                continue;
            }
        }
        forks
            .faults()
            .pass(FaultPoint::Fetch, plan.key.fork.operation)
            .await;
        match fetch(&inner, &plan).await {
            Ok((sidecar, reservation)) => {
                forks
                    .faults()
                    .pass(FaultPoint::Staged, plan.key.fork.operation)
                    .await;
                let bytes = sidecar.size;
                let published = forks.store().publish(sidecar);
                drop(reservation);
                forks.report_progress(&name, None);
                if published {
                    inner.metrics.fork_seed_bytes(bytes);
                    tracing::info!(target: "fleetringd::audit", fork = %plan.key.fork, attempt = plan.key.attempt, from = ?plan.from, source = %plan.source, source_incarnation = %plan.source_incarnation, bytes, "staged a copy of the fork");
                } else {
                    tracing::info!(fork = %plan.key.fork, attempt = plan.key.attempt, "the staged copy was deleted before it was reported");
                }
                return;
            }
            Err(Fetch::Refused(Refused::Disk)) => break SeedFailureReason::InsufficientDisk,
            Err(Fetch::PinExpired(status)) => {
                tracing::warn!(fork = %plan.key.fork, attempt = plan.key.attempt, %status, "the source's pin expired");
                break SeedFailureReason::PinExpired;
            }
            Err(Fetch::Again(after)) => time::sleep(after).await,
            Err(Fetch::Failed(status)) => {
                if let SeedFrom::Seeder(_) = plan.from {
                    // A peer waits for the seeder as long as the attempt lasts.
                    tracing::debug!(fork = %plan.key.fork, %status, "can't copy the seeder's staged copy yet");
                    time::sleep(backoff).await;
                    backoff = (backoff * 2).min(PEER_BACKOFF_CAP);
                    continue;
                }
                spent += 1;
                if spent > config.transfer_retries {
                    tracing::warn!(fork = %plan.key.fork, attempt = plan.key.attempt, %status, retries = config.transfer_retries, "can't fetch the fork's copy; reporting the attempt failed");
                    break SeedFailureReason::TransferFailed;
                }
                tracing::info!(fork = %plan.key.fork, attempt = plan.key.attempt, %status, retry = spent, "the transfer broke; fetching again");
                time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
        }
    };
    forks.report_progress(&name, None);
    inner.metrics.fork_attempt(match failure {
        SeedFailureReason::InsufficientDisk => "insufficient_disk",
        SeedFailureReason::PinExpired => "pin_expired",
        _ => "transfer_failed",
    });
    tracing::info!(target: "fleetringd::audit", fork = %plan.key.fork, attempt = plan.key.attempt, source = %plan.source, reason = ?failure, "couldn't seed the fork");
    forks.report_failure(SeedFailure::new(
        plan.key.fork.clone(),
        plan.key.attempt,
        failure,
    ));
}

/// Whether control state still shows `plan`'s attempt seeding, with this node in its replica
/// set and not refused for want of disk, and the seeder `plan` fetches from as its seeder.
async fn still_current(inner: &Inner, plan: &SeedPlan) -> Result<bool, Status> {
    let view = match inner.authority.view(&plan.key.fork.name).await {
        Ok(view) => view,
        Err(status) if status.code() == Code::NotFound => return Ok(false),
        Err(status) => return Err(status),
    };
    let Some(fork) = view.fork else {
        return Ok(false);
    };
    let ForkPhase::Seeding {
        attempt,
        seeder,
        disk_refused,
    } = &fork.phase
    else {
        return Ok(false);
    };
    let seeder_now = match &plan.from {
        SeedFrom::Source => seeder.as_ref() == Some(&inner.node),
        SeedFrom::Seeder(from) => seeder.as_ref() == Some(from),
    };
    Ok(fork.identity == plan.key.fork
        && *attempt == plan.key.attempt
        && seeder_now
        && view.holders.replicas.contains(&inner.node)
        && !disk_refused.contains(&inner.node))
}

/// The node to ask, and the request.
async fn request_for(inner: &Inner, plan: &SeedPlan) -> Result<(NodeId, SnapshotRequest), Fetch> {
    let fork = &plan.key.fork;
    let mut request = SnapshotRequest {
        fork: Some(fork.name.to_string()),
        fork_incarnation: fork.incarnation.get(),
        operation: operation_to_proto(fork.operation),
        fork_attempt: plan.key.attempt,
        source_incarnation: plan.source_incarnation.get(),
        ..SnapshotRequest::default()
    };
    match &plan.from {
        SeedFrom::Source => {
            let source = inner.authority.view(&plan.source).await?;
            let primary = source.holders.holder.clone().ok_or_else(|| {
                Fetch::Failed(Status::unavailable(format!(
                    "{} has no primary now",
                    plan.source
                )))
            })?;
            request.database = plan.source.to_string();
            request.epoch = source.holders.epoch.get();
            Ok((primary, request))
        }
        SeedFrom::Seeder(seeder) => {
            request.database = fork.name.to_string();
            request.staged = true;
            Ok((seeder.clone(), request))
        }
    }
}

/// Fetches the copy once and stages it, returning its sidecar and the reservation to drop once
/// it is reported.
async fn fetch(inner: &Arc<Inner>, plan: &SeedPlan) -> Result<(Sidecar, Reservation), Fetch> {
    let (from, request) = request_for(inner, plan).await?;
    let address = inner
        .addresses
        .address(&from)
        .ok_or_else(|| Fetch::Failed(Status::unavailable(format!("no address for {from}"))))?;
    let channel = inner
        .peers
        .channel_to(&address)
        .map_err(|error| Fetch::Failed(Status::unavailable(error.to_string())))?;
    let mut chunks = replication_client(channel)
        .snapshot(request)
        .await
        .map_err(Fetch::from)?
        .into_inner();
    let first = match time::timeout(inner.settings.open_timeout, chunks.message()).await {
        Ok(Ok(Some(first))) => first,
        Ok(Ok(None)) => {
            return Err(Fetch::Failed(Status::data_loss(
                "the copy's stream was empty",
            )));
        }
        Ok(Err(status)) => return Err(Fetch::from(status)),
        Err(_) => {
            return Err(Fetch::Failed(Status::deadline_exceeded(
                "the copy's stream stalled",
            )));
        }
    };
    let total = first.total_bytes.ok_or_else(|| {
        Fetch::Failed(Status::failed_precondition(
            "the copy's stream doesn't say its size; the sender predates forks",
        ))
    })?;
    let reservation = inner
        .forks
        .admit(&plan.key, total)
        .map_err(Fetch::Refused)?;
    inner
        .forks
        .faults()
        .pass(FaultPoint::Admitted, plan.key.fork.operation)
        .await;
    let incoming = inner.forks.store().incoming(&plan.key);
    let staged = receive(inner, plan, &from, first, &mut chunks, total, &incoming).await;
    if staged.is_err() {
        let incoming = incoming.clone();
        let removed = blocking(inner.execution, move || {
            super::super::archive::remove_dir_if_present(&incoming)
        })
        .await;
        if let Ok(Err(error)) = removed {
            tracing::warn!(%error, "can't remove a failed fetch's files");
        }
    }
    staged.map(|sidecar| (sidecar, reservation))
}

/// Unpacks the stream that starts with `first` into `incoming`, checks it, and commits it as
/// a staged copy.
async fn receive(
    inner: &Arc<Inner>,
    plan: &SeedPlan,
    from: &NodeId,
    first: SnapshotChunk,
    chunks: &mut tonic::Streaming<SnapshotChunk>,
    total: u64,
    incoming: &std::path::Path,
) -> Result<Sidecar, Fetch> {
    let forks = &inner.forks;
    let name = &plan.key.fork.name;
    let progress = |bytes: u64| {
        forks.report_progress(
            name,
            Some(SeedProgress {
                fork: plan.key.fork.clone(),
                attempt: plan.key.attempt,
                bytes,
                total_bytes: total,
            }),
        );
    };
    progress(0);
    let mut unpacker = {
        let incoming = incoming.to_owned();
        blocking(inner.execution, move || Unpacker::create(&incoming))
            .await?
            .map_err(|error| {
                Fetch::Failed(Status::internal(format!("can't stage the copy: {error}")))
            })?
    };
    let limit = inner.settings.open_timeout;
    let mut chunk = Some(first);
    loop {
        let current = match chunk.take() {
            Some(current) => current,
            None => match time::timeout(limit, chunks.message()).await {
                Ok(Ok(Some(current))) => current,
                Ok(Ok(None)) => {
                    return Err(Fetch::Failed(Status::data_loss(
                        "the copy's stream ended early",
                    )));
                }
                Ok(Err(status)) => return Err(Fetch::from(status)),
                Err(_) => {
                    return Err(Fetch::Failed(Status::deadline_exceeded(
                        "the copy's stream stalled",
                    )));
                }
            },
        };
        if current.offset != unpacker.offset() {
            return Err(Fetch::Failed(Status::data_loss(format!(
                "a chunk of the copy starts at offset {}, after {} bytes",
                current.offset,
                unpacker.offset()
            ))));
        }
        if let Some(position) = current.position {
            if unpacker.offset() != total {
                return Err(Fetch::Failed(Status::data_loss(format!(
                    "the copy's stream ended after {} of the {total} bytes its first chunk \
                     announced",
                    unpacker.offset()
                ))));
            }
            return finish(
                inner,
                plan,
                from,
                unpacker,
                current.checksum,
                current.chain,
                current.source_incarnation,
                position,
                incoming,
            )
            .await;
        }
        let data = current.data;
        within_total(total, unpacker.offset(), data.len()).map_err(Fetch::Failed)?;
        let (returned, written) = blocking(inner.execution, move || {
            let written = unpacker.write(&data);
            (unpacker, written)
        })
        .await?;
        unpacker = returned;
        written.map_err(|error| {
            Fetch::Failed(Status::data_loss(format!("the copy is corrupt: {error}")))
        })?;
        progress(unpacker.offset());
    }
}

/// Refuses a chunk of `len` bytes at `offset` that would take a stream past the `total` bytes
/// its first chunk announced, and admitted room for: `DATA_LOSS`, which ends the fetch and
/// releases its reservation.
fn within_total(total: u64, offset: u64, len: usize) -> Result<(), Status> {
    let end = u64::try_from(len)
        .ok()
        .and_then(|len| offset.checked_add(len));
    if end.is_none_or(|end| end > total) {
        return Err(Status::data_loss(format!(
            "the copy's stream runs past the {total} bytes its first chunk announced"
        )));
    }
    Ok(())
}

/// Verifies the stream's end, checks the copy with the engine, and commits it.
#[expect(
    clippy::too_many_arguments,
    reason = "the last chunk's fields, each checked here"
)]
async fn finish(
    inner: &Arc<Inner>,
    plan: &SeedPlan,
    from: &NodeId,
    unpacker: Unpacker,
    checksum: Option<Vec<u8>>,
    chain: Option<Vec<u8>>,
    stamp: Option<u64>,
    position: Vec<u8>,
    incoming: &std::path::Path,
) -> Result<Sidecar, Fetch> {
    let failed = |message: String| Fetch::Failed(Status::data_loss(message));
    let checksum =
        checksum.ok_or_else(|| failed("the copy's last chunk has no checksum".to_owned()))?;
    let chain = chain
        .as_deref()
        .and_then(ChainHash::from_bytes)
        .ok_or_else(|| failed("the copy's last chunk has no chain hash".to_owned()))?;
    let stamp = stamp
        .map(Incarnation::new)
        .ok_or_else(|| failed("the copy's last chunk has no source incarnation".to_owned()))?;
    if stamp != plan.source_incarnation
        && !fleetring_control::mutation::Mutation::SkipSourceIncarnation.is_active()
    {
        return Err(Fetch::Failed(Status::failed_precondition(format!(
            "the copy is of source incarnation {stamp}, not {}",
            plan.source_incarnation
        ))));
    }
    let files: Vec<StagedFile> = unpacker
        .files()
        .iter()
        .map(|(name, len)| StagedFile {
            name: name.clone(),
            len: *len,
        })
        .collect();
    let size = unpacker.offset();
    let checksum_bytes: [u8; super::super::archive::CHECKSUM_LEN] = checksum
        .as_slice()
        .try_into()
        .map_err(|_| failed("the copy's checksum isn't 16 bytes".to_owned()))?;
    blocking(inner.execution, move || unpacker.finish(&checksum))
        .await?
        .map_err(|error| failed(format!("the copy is corrupt: {error}")))?;
    check_copy(inner, incoming, from)
        .await
        .map_err(Fetch::Failed)?;
    let sidecar = Sidecar {
        key: plan.key.clone(),
        position: Position::from(position),
        chain_hash: chain,
        source_incarnation: stamp,
        size,
        checksum: checksum_bytes,
        files,
    };
    for step in CommitStep::ALL {
        let committed = {
            let forks = Arc::clone(inner);
            let sidecar = sidecar.clone();
            blocking(inner.execution, move || {
                forks.forks.store().commit_step(&sidecar, step)
            })
            .await?
        };
        committed.map_err(|error| {
            Fetch::Failed(Status::internal(format!("can't stage the copy: {error}")))
        })?;
        let after = match step {
            CommitStep::Write => FaultPoint::StagingWritten,
            CommitStep::PlaceCopy => FaultPoint::StagingRenamed,
            CommitStep::PlaceSidecar => FaultPoint::StagingUnsynced,
            CommitStep::Sync => continue,
        };
        inner
            .forks
            .faults()
            .pass(after, plan.key.fork.operation)
            .await;
    }
    Ok(sidecar)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_never_runs_past_the_size_it_announced() {
        assert!(within_total(10, 0, 10).is_ok());
        assert!(within_total(10, 4, 6).is_ok());
        assert!(within_total(10, 10, 0).is_ok());
        let past = within_total(10, 4, 7).expect_err("one byte past");
        assert_eq!(past.code(), Code::DataLoss);
        assert!(within_total(0, 0, 1).is_err());
        assert!(within_total(u64::MAX, u64::MAX, 1).is_err());
    }
}
