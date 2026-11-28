//! The control group's RPCs (ADR-0035 point 11, ADR-0040 point 9): `AddControlNode`,
//! `SetControlVoters`, `RemoveControlNode`, and `DescribeControlGroup`.
//!
//! Each needs the `fleet` scope, and the boundary refuses node certificates, so a stolen node key
//! can neither add a node to the group nor change its voters. The leader checks a request's
//! arguments, then changes the group through [`ControlHandle`](crate::control::ControlHandle)'s
//! group methods, in a task of its own that a caller going away doesn't cancel, so a change
//! always runs to its end and writes its audit line, naming the caller, the change, and the
//! change's own outcome. The answer is the group as this node then knows it, read without a read
//! round: a change of voters can hand this node's leadership over, and a read that redirected the
//! caller would make it send the change again.

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;

use fleetring::NodeId;
use fleetring_client::redirect::not_leader;
use fleetring_proto::v1::{
    self as proto, AddControlNodeRequest, RemoveControlNodeRequest, SetControlVotersRequest,
};
use tonic::Status;

use super::{AUDIT, ControlService, parse, submit_status};
use crate::control::node::{GroupError, GroupStatus, MAX_VOTERS};
use crate::dev::{DevMode, check_loopback};
use crate::software::software_to_proto;

/// Who asks for a change, as its audit line names them: the principal and its credential.
pub(super) type Caller = (String, String);

/// Runs `change` in a task of its own and waits for it, so that a caller that goes away, or a
/// deadline that passes, doesn't drop the change halfway: between openraft's two steps, or before
/// its audit line.
///
/// # Errors
///
/// The change's status, or `INTERNAL` if its task panicked.
pub(super) async fn run_to_end<F>(change: F) -> Result<proto::ControlGroup, Status>
where
    F: Future<Output = Result<proto::ControlGroup, Status>> + Send + 'static,
{
    tokio::spawn(change).await.map_err(|error| {
        tracing::error!(%error, "a change of the control group failed");
        Status::internal("the change of the control group failed")
    })?
}

impl ControlService {
    /// Adds the node a request names to the control group, as a learner caught up with the log.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for an invalid node ID or address, `FAILED_PRECONDITION` for a non-
    /// loopback address on a dev mode node, and the status of [`group_status`] otherwise.
    pub(super) async fn add_member(
        self,
        caller: Caller,
        request: AddControlNodeRequest,
    ) -> Result<proto::ControlGroup, Status> {
        let node: NodeId = parse("node id", &request.node_id)?;
        let address = request.address.trim();
        if address.is_empty() {
            return Err(Status::invalid_argument(
                "missing address: name the host:port the control group dials the node at",
            ));
        }
        if self.dev == DevMode::On {
            // A dev node's control log never leaves the machine (ADR-0040 point 9).
            let parsed: SocketAddr = address.parse().map_err(|_| {
                Status::failed_precondition(format!(
                    "{address} isn't a loopback address, and dev mode serves loopback only \
                     (ADR-0039)"
                ))
            })?;
            check_loopback(parsed).map_err(Status::failed_precondition)?;
        }
        let changed = self
            .node
            .add_node(node.as_str(), address)
            .await
            .map_err(group_status);
        audit_group(
            &caller,
            "AddControlNode",
            &format!("add {node} at {address}"),
            &changed,
        );
        changed?;
        Ok(self.group())
    }

    /// Makes exactly the nodes a request names the control group's voters.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for an invalid node ID, a node named twice, an even count, or more
    /// than [`MAX_VOTERS`], and the status of [`group_status`] otherwise.
    pub(super) async fn change_voters(
        self,
        caller: Caller,
        request: SetControlVotersRequest,
    ) -> Result<proto::ControlGroup, Status> {
        let mut voters = BTreeSet::new();
        for text in &request.voters {
            let voter: NodeId = parse("voter", text)?;
            if !voters.insert(voter.to_string()) {
                return Err(Status::invalid_argument(format!(
                    "node '{voter}' is named twice"
                )));
            }
        }
        check_voter_count(voters.len())?;
        let voters: Vec<&str> = voters.iter().map(String::as_str).collect();
        let changed = self.node.set_voters(&voters).await.map_err(group_status);
        let change = format!("set voters {}", voters.join(","));
        audit_group(&caller, "SetControlVoters", &change, &changed);
        changed?;
        Ok(self.group())
    }

    /// Removes the learner a request names from the control group.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for an invalid node ID, and the status of [`group_status`] otherwise.
    pub(super) async fn remove_member(
        self,
        caller: Caller,
        request: RemoveControlNodeRequest,
    ) -> Result<proto::ControlGroup, Status> {
        let node: NodeId = parse("node id", &request.node_id)?;
        let changed = self
            .node
            .remove_node(node.as_str())
            .await
            .map_err(group_status);
        audit_group(
            &caller,
            "RemoveControlNode",
            &format!("remove {node}"),
            &changed,
        );
        changed?;
        Ok(self.group())
    }

    /// The control group as this node, its leader, knows it once a read round confirms it still
    /// leads (ADR-0040 point 2).
    ///
    /// # Errors
    ///
    /// A redirect off the leader, and `UNAVAILABLE` while it can't confirm its leadership.
    pub(super) async fn describe(&self) -> Result<proto::ControlGroup, Status> {
        self.node
            .ensure_linearizable()
            .await
            .map_err(submit_status)?;
        Ok(self.group())
    }

    /// The control group as this node knows it now, from one reading of its Raft metrics.
    fn group(&self) -> proto::ControlGroup {
        group_to_proto(self.node.group_status())
    }
}

/// `status` as the protocol carries it.
fn group_to_proto(status: GroupStatus) -> proto::ControlGroup {
    proto::ControlGroup {
        nodes: status
            .members
            .into_iter()
            .map(|member| proto::ControlNode {
                node_id: member.peer.node_id,
                address: member.peer.address,
                voter: member.voter,
                leader: member.leader,
                matched_index: member.matched_index,
                software: member.software.as_ref().map(software_to_proto),
            })
            .collect(),
        term: status.term,
        committed_index: status.committed_index,
        leader: status.leader.unwrap_or_default(),
        joint: status.joint,
    }
}

/// Refuses a voter count a control group can't have: even, which buys no fault tolerance over
/// one fewer and makes a split vote possible, or more than [`MAX_VOTERS`].
///
/// # Errors
///
/// `INVALID_ARGUMENT` naming the count.
fn check_voter_count(count: usize) -> Result<(), Status> {
    if count.is_multiple_of(2) {
        return Err(Status::invalid_argument(format!(
            "a control group needs an odd number of voters, not {count}"
        )));
    }
    if count > MAX_VOTERS {
        return Err(Status::invalid_argument(format!(
            "a control group has at most {MAX_VOTERS} voters, not {count}"
        )));
    }
    Ok(())
}

/// The status a group change's error is answered with:
///
/// - off the leader, a redirect to it when known, and `UNAVAILABLE` otherwise or once the node
///   has stopped;
/// - `FAILED_PRECONDITION` for a node the probe refused, a change the group's state rules out,
///   and one openraft refused;
/// - `ABORTED` for a change of voters left joint, which another `SetControlVoters` finishes, and
///   for a learner that hadn't caught up or a change not committed when the leader's wait
///   ended: the change may still take effect, and `DescribeControlGroup` says. Not
///   `DEADLINE_EXCEEDED`, which `ControlClient` reads as an address that hangs and moves off,
///   away from the leader that answered; nor `UNAVAILABLE`, which says to retry, and a retry of
///   an add or a removal would be refused as already done.
pub(super) fn group_status(error: GroupError) -> Status {
    match error {
        GroupError::NotLeader {
            leader: Some(ref leader),
        } => not_leader(leader),
        GroupError::NotLeader { leader: None } => {
            Status::unavailable("no control leader is known yet; retry")
        }
        GroupError::Stopped => Status::unavailable(error.to_string()),
        GroupError::Probe { .. } | GroupError::Invalid { .. } | GroupError::Refused { .. } => {
            Status::failed_precondition(error.to_string())
        }
        GroupError::Joint { .. } | GroupError::CatchingUp { .. } | GroupError::NotCommitted => {
            Status::aborted(error.to_string())
        }
    }
}

/// Writes a group change's audit line: the caller, the RPC, the change, and the outcome.
pub(super) fn audit_group<T>(
    (principal, credential): &Caller,
    rpc: &'static str,
    change: &str,
    result: &Result<T, Status>,
) {
    match result {
        Ok(_) => tracing::info!(
            target: AUDIT,
            principal,
            credential,
            change,
            result = "changed",
            rpc,
            "changed the control group",
        ),
        Err(status) => tracing::warn!(
            target: AUDIT,
            principal,
            credential,
            change,
            result = "refused",
            code = ?status.code(),
            reason = status.message(),
            rpc,
            "refused a change of the control group",
        ),
    }
}

#[cfg(test)]
mod tests {
    use fleetring_client::redirect::leader_hint;
    use tonic::Code;

    use super::*;

    #[test]
    fn group_errors_map_to_statuses() {
        let redirect = group_status(GroupError::NotLeader {
            leader: Some("10.0.0.2:7400".into()),
        });
        assert_eq!(redirect.code(), Code::Unavailable);
        assert_eq!(leader_hint(&redirect).as_deref(), Some("10.0.0.2:7400"));
        let unknown = group_status(GroupError::NotLeader { leader: None });
        assert_eq!(unknown.code(), Code::Unavailable);
        assert_eq!(leader_hint(&unknown), None);
        let cases = [
            (GroupError::Stopped, Code::Unavailable),
            (
                GroupError::Probe {
                    node: "c4".into(),
                    reason: "its store already holds Raft state".into(),
                },
                Code::FailedPrecondition,
            ),
            (
                GroupError::Invalid {
                    reason: "node 'c9' isn't a member".into(),
                },
                Code::FailedPrecondition,
            ),
            (
                GroupError::Refused {
                    reason: "in progress".into(),
                },
                Code::FailedPrecondition,
            ),
            (
                GroupError::Joint {
                    reason: "a leader change".into(),
                },
                Code::Aborted,
            ),
            (GroupError::CatchingUp { node: "c2".into() }, Code::Aborted),
            (GroupError::NotCommitted, Code::Aborted),
        ];
        for (error, code) in cases {
            let text = error.to_string();
            let status = group_status(error);
            assert_eq!(status.code(), code, "{text}");
            assert_eq!(status.message(), text);
            assert_eq!(leader_hint(&status), None, "{text}");
        }
    }

    #[test]
    fn a_voter_count_is_odd_and_at_most_seven() {
        for count in [1, 3, 5, 7] {
            assert!(check_voter_count(count).is_ok(), "{count}");
        }
        for count in [0, 2, 4, 9] {
            let refused = check_voter_count(count).expect_err("refused");
            assert_eq!(refused.code(), Code::InvalidArgument, "{count}");
        }
    }
}
