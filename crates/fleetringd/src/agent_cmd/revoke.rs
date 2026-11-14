//! What `agent remove` and `agent renew` revoke, and as whom (ADR-0047, points 10 and 11;
//! `docs/cli.md`, section 16).
//!
//! A root agent removes itself: its principal and its own token ID, in one request, without the
//! `fleet` scope. A delegated agent is revoked as a principal by its nearest ancestor with an
//! identity here, and a renewed one's old delegation ID by its parent, both under the ancestor
//! rule. Every request is sent again while the answer is lost or the control plane unavailable,
//! as `agent delegate` sends its own. No token reaches stdout, a log, or an error.

use std::path::Path;

use fleetring_control::{Millis, Timestamp};
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{self as proto, RevokeTokenRequest, RevokeTokenResponse};
use fleetringd::auth::BearerToken;
use fleetringd::data::Client;
use fleetringd::fork_flow::ResendPolicy;
use tonic::{Code, Status};

use super::REVOKE_TIMEOUT;
use super::delegate::{Unanswered, send};
use crate::agent::{AgentDir, AgentError, AgentRef, DelegatedDir, DelegatedId, OldToken};
use crate::exit;

/// How long after a root principal's revocation a new token for it may be signed: nodes judge a
/// root token's `iat` with this much leeway (ADR-0047, point 11.5).
const ROOT_LEEWAY: Millis = Millis::new(30_000);

/// A request that revokes `token` by its ID, kept until the token expires. Without `fleet`, the
/// control plane chooses how long, and ignores `until_ms`.
pub(super) fn token_request(token: &OldToken) -> RevokeTokenRequest {
    RevokeTokenRequest {
        revocation: Some(proto::Revocation {
            revoked: Some(Revoked::TokenId(token.id.clone())),
            until_ms: token.until.as_millis(),
        }),
        principal: String::new(),
    }
}

/// A request that revokes `principal` and every agent below it, and, if given, `token` by its
/// ID after it (ADR-0047, point 11.7).
pub(super) fn principal_request(principal: &str, token: Option<&OldToken>) -> RevokeTokenRequest {
    RevokeTokenRequest {
        principal: principal.to_owned(),
        ..token.map_or_else(RevokeTokenRequest::default, token_request)
    }
}

/// Sends `request` through `client`, and sends it again while the answer is lost or the control
/// plane answers `UNAVAILABLE`, as `agent delegate` resends: at most 5 times within 60 seconds.
pub(super) async fn send_revocation(
    client: &Client,
    request: &RevokeTokenRequest,
) -> Result<RevokeTokenResponse, Unanswered> {
    send(ResendPolicy::default(), || async {
        tokio::time::timeout(
            REVOKE_TIMEOUT,
            client.control().revoke_token(request.clone()),
        )
        .await
        .unwrap_or_else(|_| {
            Err(Status::deadline_exceeded(format!(
                "the fleet didn't answer within {}s",
                REVOKE_TIMEOUT.as_secs()
            )))
        })
    })
    .await
}

/// What went wrong with a revocation, in words.
pub(super) fn describe(unanswered: &Unanswered) -> String {
    match unanswered {
        Unanswered::Refused(status) => exit::describe(status),
        Unanswered::Lost { sends, last } => format!(
            "the control plane stayed unavailable after {sends} sends: {}",
            exit::describe(last)
        ),
    }
}

/// Whether a root agent's revocation of itself failed only because the fleet can't use the
/// ancestor rule or principals yet, or doesn't accept the agent's own token, so that the token
/// is revoked with `fleet` instead, as before 0.8: a member lacks `delegation`
/// (`FAILED_PRECONDITION`), a control node predates it, or the token is refused.
pub(super) fn falls_back(unanswered: &Unanswered) -> bool {
    matches!(
        unanswered,
        Unanswered::Refused(status) if matches!(
            status.code(),
            Code::FailedPrecondition
                | Code::PermissionDenied
                | Code::Unauthenticated
                | Code::Unimplemented
        )
    )
}

/// The first moment a new root token for a principal revoked at `before` may be signed: the
/// first whole second whose `iat` nodes judge after `before` plus 30 seconds.
pub(crate) fn replacement_from(before: Timestamp) -> Timestamp {
    let last_refused = before.saturating_add(ROOT_LEEWAY).as_millis();
    Timestamp::from_millis((last_refused / 1_000 + 1) * 1_000)
}

/// The nearest ancestor of `id` with a usable identity here: its parent, else the parent's
/// parent, up to the root, with the token it presents. `None` if the root agent's identity, whose
/// certificate every agent of the lineage connects with, is missing, or no ancestor holds a
/// token still valid at `now`.
///
/// # Errors
///
/// The errors of reading the records and the token.
pub(super) fn nearest_ancestor(
    agents_dir: &Path,
    id: &DelegatedId,
    now: Timestamp,
) -> Result<Option<(AgentRef, BearerToken)>, AgentError> {
    let root = AgentDir::new(agents_dir, id.root().clone());
    if root.record()?.is_none() {
        return Ok(None);
    }
    let mut ancestor = id.parent();
    loop {
        match ancestor {
            AgentRef::Root(_) => {
                let Some(record) = root.record()? else {
                    return Ok(None);
                };
                if root.old_token(&record, now).until <= now {
                    return Ok(None);
                }
                return Ok(Some((ancestor, root.token()?)));
            }
            AgentRef::Delegated(parent) => {
                let dir = DelegatedDir::new(agents_dir, parent.clone());
                if let Some(record) = dir.record()?
                    && dir.old_token(&record, now).until > now
                {
                    return Ok(Some((AgentRef::Delegated(parent), dir.token()?)));
                }
                ancestor = parent.parent();
            }
        }
    }
}

/// The token agent `id` presents, from its own directory.
///
/// # Errors
///
/// The error of reading it.
pub(super) fn token_of(agents_dir: &Path, id: &AgentRef) -> Result<BearerToken, AgentError> {
    match id {
        AgentRef::Root(root) => AgentDir::new(agents_dir, root.clone()).token(),
        AgentRef::Delegated(delegated) => DelegatedDir::new(agents_dir, delegated.clone()).token(),
    }
}

/// The delegated agents below `id` with a directory here, in order: the identities a revocation
/// of `id`, or of the token they were minted from, ends.
pub(super) fn local_descendants(agents_dir: &Path, id: &str) -> Vec<String> {
    let prefix = format!("{id}.");
    let mut below: Vec<String> = std::fs::read_dir(agents_dir)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| name.starts_with(&prefix) && name.parse::<DelegatedId>().is_ok())
        .filter(|name| {
            agents_dir
                .join(name)
                .join(crate::agent::RECORD_FILE)
                .is_file()
        })
        .collect();
    below.sort();
    below
}

/// A note naming `below`, which an ended token takes with it, and how to renew or remove each;
/// `None` if there are none.
pub(super) fn ended_below(below: &[String], next: &str) -> Option<String> {
    if below.is_empty() {
        return None;
    }
    Some(format!(
        "note: this also ends {}, whose identities here no longer work; {next}",
        below.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_replacement_root_token_is_signed_after_the_leeway_in_whole_seconds() {
        assert_eq!(
            replacement_from(Timestamp::from_millis(1_000_500)),
            Timestamp::from_millis(1_031_000)
        );
        assert_eq!(
            replacement_from(Timestamp::from_millis(1_000_000)),
            Timestamp::from_millis(1_031_000)
        );
        assert_eq!(
            replacement_from(Timestamp::from_millis(999_999)),
            Timestamp::from_millis(1_030_000)
        );
    }

    #[test]
    fn a_root_and_its_token_are_revoked_in_one_request_principal_named() {
        let token = OldToken {
            id: "root-42".to_owned(),
            until: Timestamp::from_millis(5_000),
        };
        let request = principal_request("agent-42", Some(&token));
        assert_eq!(request.principal, "agent-42");
        assert_eq!(
            request.revocation,
            Some(proto::Revocation {
                revoked: Some(Revoked::TokenId("root-42".to_owned())),
                until_ms: 5_000,
            })
        );
        assert_eq!(principal_request("agent-42.c7", None).revocation, None);
    }

    #[test]
    fn only_a_fleet_that_cant_use_the_rule_falls_back_to_fleet() {
        for code in [
            Code::FailedPrecondition,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::Unimplemented,
        ] {
            assert!(falls_back(&Unanswered::Refused(Status::new(code, "x"))));
        }
        assert!(!falls_back(&Unanswered::Refused(
            Status::resource_exhausted("x")
        )));
        assert!(!falls_back(&Unanswered::Lost {
            sends: 6,
            last: Status::unavailable("x"),
        }));
    }

    #[test]
    fn descendants_are_the_dotted_identities_below_never_a_sibling_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        for name in [
            "42",
            "42.c7",
            "42.c7.r1",
            "42.c70",
            "42.c7.r1.x",
            "42.c7.r2",
        ] {
            std::fs::create_dir(root.path().join(name))?;
            if name != "42.c7.r2" {
                std::fs::write(root.path().join(name).join(crate::agent::RECORD_FILE), "")?;
            }
        }
        assert_eq!(
            local_descendants(root.path(), "42.c7"),
            ["42.c7.r1", "42.c7.r1.x"]
        );
        assert_eq!(
            local_descendants(root.path(), "42"),
            ["42.c7", "42.c7.r1", "42.c7.r1.x", "42.c70"]
        );
        Ok(())
    }
}
