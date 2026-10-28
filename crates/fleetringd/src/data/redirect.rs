//! The "not the primary, try X" reply, which follows the control plane's leader redirect.
//!
//! A data node that doesn't hold a database's lease answers with `UNAVAILABLE` and names the
//! primary in the [`PRIMARY_METADATA_KEY`] metadata entry as `host:port`, as a control node that
//! isn't the leader does with [`LEADER_METADATA_KEY`](crate::control::LEADER_METADATA_KEY).

use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};

/// The metadata key that names a database's primary in a redirect:
/// `fleetring-primary: <host:port>`.
pub const PRIMARY_METADATA_KEY: &str = "fleetring-primary";

/// Returns the status a node that isn't `database`'s primary replies with, naming `primary` as
/// `host:port`.
///
/// If `primary` can't be carried in metadata (it isn't printable ASCII), the status carries no
/// redirect, and clients locate the primary again.
#[must_use]
pub fn not_primary(database: &str, primary: &str) -> Status {
    let message = format!("not the primary of {database}; the primary is {primary}");
    let mut metadata = MetadataMap::new();
    match MetadataValue::try_from(primary) {
        Ok(value) => {
            metadata.insert(PRIMARY_METADATA_KEY, value);
        }
        Err(_) => tracing::warn!(primary, "primary address can't be sent as metadata"),
    }
    Status::with_metadata(Code::Unavailable, message, metadata)
}

/// Returns the primary a redirect names, or `None` if `status` isn't a redirect.
#[must_use]
pub fn primary_hint(status: &Status) -> Option<String> {
    if status.code() != Code::Unavailable {
        return None;
    }
    let primary = status.metadata().get(PRIMARY_METADATA_KEY)?.to_str().ok()?;
    (!primary.is_empty()).then(|| primary.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_redirect_round_trips_and_other_statuses_are_not_redirects() {
        let status = not_primary("acme", "10.0.0.2:7400");
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(primary_hint(&status).as_deref(), Some("10.0.0.2:7400"));
        assert!(status.message().contains("acme"), "{status}");

        assert_eq!(primary_hint(&Status::unavailable("down")), None);
        let mut wrong_code = Status::not_found("gone");
        wrong_code
            .metadata_mut()
            .insert(PRIMARY_METADATA_KEY, MetadataValue::from_static("x:1"));
        assert_eq!(primary_hint(&wrong_code), None);
        assert_eq!(primary_hint(&not_primary("acme", "caf\u{e9}:1")), None);
    }
}
