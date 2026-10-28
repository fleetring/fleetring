//! The "not the leader, try X" reply (ADR-0017, point 4).
//!
//! A control node that isn't the leader answers with `UNAVAILABLE` and names the leader in the
//! [`LEADER_METADATA_KEY`] metadata entry as `host:port`. A single control node never sends it;
//! under Raft, followers will.

use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};

/// The metadata key that names the leader in a redirect: `fleetring-leader: <host:port>`.
pub const LEADER_METADATA_KEY: &str = "fleetring-leader";

/// Returns the status a non-leader replies with, naming `leader` as `host:port`.
///
/// If `leader` can't be carried in metadata (it isn't printable ASCII), the status carries no
/// redirect, and clients simply retry.
#[must_use]
pub fn not_leader(leader: &str) -> Status {
    let message = format!("not the leader; the leader is {leader}");
    let mut metadata = MetadataMap::new();
    match MetadataValue::try_from(leader) {
        Ok(value) => {
            metadata.insert(LEADER_METADATA_KEY, value);
        }
        Err(_) => tracing::warn!(leader, "leader address can't be sent as metadata"),
    }
    Status::with_metadata(Code::Unavailable, message, metadata)
}

/// Returns the leader a redirect names, or `None` if `status` isn't a redirect.
#[must_use]
pub fn leader_hint(status: &Status) -> Option<String> {
    if status.code() != Code::Unavailable {
        return None;
    }
    let leader = status.metadata().get(LEADER_METADATA_KEY)?.to_str().ok()?;
    (!leader.is_empty()).then(|| leader.to_owned())
}
