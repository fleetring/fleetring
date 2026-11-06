//! The "try X" replies every client follows: a control node that isn't the leader names the
//! leader (ADR-0017, point 4), and a data node that isn't a database's primary names the primary.
//!
//! Both answer with `UNAVAILABLE` and put the address, `host:port`, in a metadata entry:
//! [`LEADER_METADATA_KEY`] or [`PRIMARY_METADATA_KEY`]. A client switches to that address and
//! repeats the call, so a write crosses one hop and a lost reply is never doubled by a proxy. The
//! daemon's nodes build the replies, and every client reads them, so both live here.

use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};

/// The metadata key that names the leader in a redirect: `fleetring-leader: <host:port>`.
pub const LEADER_METADATA_KEY: &str = "fleetring-leader";

/// The metadata key that names a database's primary in a redirect:
/// `fleetring-primary: <host:port>`.
pub const PRIMARY_METADATA_KEY: &str = "fleetring-primary";

/// Returns the status a non-leader replies with, naming `leader` as `host:port`.
///
/// If `leader` can't be carried in metadata (it isn't printable ASCII), the status carries no
/// redirect, and clients simply retry.
#[must_use]
pub fn not_leader(leader: &str) -> Status {
    redirect(
        format!("not the leader; the leader is {leader}"),
        LEADER_METADATA_KEY,
        leader,
    )
}

/// Returns the leader a redirect names, or `None` if `status` isn't a redirect.
#[must_use]
pub fn leader_hint(status: &Status) -> Option<String> {
    hint(status, LEADER_METADATA_KEY)
}

/// Returns the status a node that isn't `database`'s primary replies with, naming `primary` as
/// `host:port`.
///
/// If `primary` can't be carried in metadata (it isn't printable ASCII), the status carries no
/// redirect, and clients locate the primary again.
#[must_use]
pub fn not_primary(database: &str, primary: &str) -> Status {
    redirect(
        format!("not the primary of {database}; the primary is {primary}"),
        PRIMARY_METADATA_KEY,
        primary,
    )
}

/// Returns the primary a redirect names, or `None` if `status` isn't a redirect.
#[must_use]
pub fn primary_hint(status: &Status) -> Option<String> {
    hint(status, PRIMARY_METADATA_KEY)
}

/// An `UNAVAILABLE` status with `message`, naming `address` under `key` when it can.
fn redirect(message: String, key: &'static str, address: &str) -> Status {
    let mut metadata = MetadataMap::new();
    match MetadataValue::try_from(address) {
        Ok(value) => {
            metadata.insert(key, value);
        }
        Err(_) => tracing::warn!(
            address,
            key,
            "a redirect's address can't be sent as metadata"
        ),
    }
    Status::with_metadata(Code::Unavailable, message, metadata)
}

/// The non-empty address `status` names under `key`, if it is an `UNAVAILABLE` redirect.
fn hint(status: &Status, key: &str) -> Option<String> {
    if status.code() != Code::Unavailable {
        return None;
    }
    let address = status.metadata().get(key)?.to_str().ok()?;
    (!address.is_empty()).then(|| address.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_primary_redirect_round_trips_and_other_statuses_are_not_redirects() {
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

    #[test]
    fn a_leader_redirect_round_trips_and_is_not_a_primary_redirect() {
        let status = not_leader("10.0.0.1:7400");
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(leader_hint(&status).as_deref(), Some("10.0.0.1:7400"));
        assert_eq!(primary_hint(&status), None);
        assert_eq!(leader_hint(&not_leader("caf\u{e9}:1")), None);
    }
}
