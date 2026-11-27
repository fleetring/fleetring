//! Health and readiness: what `/healthz` and `/readyz` report (ADR-0034 point 4).
//!
//! Every check reads local state: a probe never makes a network call, so a slow peer can't make
//! one time out.

use std::time::Duration;

use fleetring::NodeId;
use serde::Serialize;

use crate::Membership;
use crate::control::ControlHandle;

/// The state each check reads. A node runs one role or both, so each role's part is optional.
#[derive(Debug, Clone)]
pub(crate) struct Probes {
    /// This node.
    pub(crate) node: NodeId,
    /// The data role's membership, if the node hosts data.
    pub(crate) membership: Option<Membership>,
    /// How old the denylist may be before the node refuses tokens.
    pub(crate) denylist_max_age: Duration,
    /// The control node, if the node runs control.
    pub(crate) control: Option<ControlHandle>,
}

/// One readiness check's result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Check {
    pub(crate) name: &'static str,
    pub(crate) ok: bool,
    /// Why it fails; empty when it passes.
    pub(crate) detail: String,
}

impl Check {
    fn passed(name: &'static str) -> Self {
        Self {
            name,
            ok: true,
            detail: String::new(),
        }
    }

    fn judged(name: &'static str, failure: Option<String>) -> Self {
        match failure {
            None => Self::passed(name),
            Some(detail) => Self {
                name,
                ok: false,
                detail,
            },
        }
    }
}

/// What `/readyz` reports: whether every check passes, and every check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Readiness {
    pub(crate) ready: bool,
    pub(crate) checks: Vec<Check>,
}

/// What `/healthz` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Health {
    /// The process can serve.
    Serving,
    /// The control node stopped after a storage failure; only a restart recovers.
    ControlWriterStopped,
}

impl Probes {
    pub(crate) fn health(&self) -> Health {
        match &self.control {
            Some(control) if !control.is_running() => Health::ControlWriterStopped,
            _ => Health::Serving,
        }
    }

    /// Why the node refuses tokens for want of a fresh, readable denylist, if it does. The check
    /// reads the list the request boundary checks tokens against: on a node that runs control,
    /// the control state's, which is always current; otherwise the copy `membership`'s
    /// heartbeats bring, which an entry this node can't read makes unusable (ADR-0047, point 3).
    fn stale_denylist(&self, membership: &Membership) -> Option<String> {
        if self.control.is_some() {
            return None;
        }
        let denylist = membership.denylist();
        if let Err(unusable) = denylist.load() {
            let entry = unusable.entry();
            return Some(format!(
                "denylist entry {} is unreadable ({}); tokens are refused",
                entry.index(),
                entry.reason()
            ));
        }
        match denylist.age() {
            None => Some("no denylist yet".to_owned()),
            Some(age) if age > self.denylist_max_age => Some(format!(
                "denylist is {} ms old; tokens are refused",
                age.as_millis()
            )),
            Some(_) => None,
        }
    }

    /// Runs every check of the node's roles: the data role's first, then the control role's.
    pub(crate) fn readiness(&self) -> Readiness {
        let mut checks = Vec::new();
        if let Some(membership) = &self.membership {
            let view = membership.view();
            checks.push(Check::judged(
                "joined",
                (membership.epoch().get() == 0).then(|| "not joined to the fleet yet".to_owned()),
            ));
            checks.push(Check::judged(
                "ring",
                view.ring()
                    .is_none()
                    .then(|| "no ring published yet".to_owned()),
            ));
            checks.push(Check::judged("denylist", self.stale_denylist(membership)));
            checks.push(Check::judged(
                "draining",
                view.is_draining(&self.node)
                    .then(|| "the node is draining".to_owned()),
            ));
        }
        if let Some(control) = &self.control {
            // ADR-0040 point 10: the control log applies and its storage runs; a leader is known,
            // this node or another; and this node is a voter or learner of a control group.
            let stopped = control.halted().or_else(|| {
                (!control.is_running())
                    .then(|| "the control node stopped; restart the node".to_owned())
            });
            checks.push(Check::judged("control_log", stopped));
            checks.push(Check::judged(
                "leader",
                control
                    .leader()
                    .is_none()
                    .then(|| "no control leader known".to_owned()),
            ));
            let group = if control.group().contains(&control.own().node_id) {
                // A joint membership is a voter change openraft didn't finish, as when the leader
                // changed between its two steps; another `set_voters` finishes it.
                control.is_joint().then(|| {
                    "the control group's membership is joint: a change of voters didn't finish; \
                     run set-voters again"
                        .to_owned()
                })
            } else {
                Some(
                    "this node is no voter or learner of a control group: it waits to be added, \
                     or was removed"
                        .to_owned(),
                )
            };
            checks.push(Check::judged("control_group", group));
        }
        Readiness {
            ready: checks.iter().all(|check| check.ok),
            checks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_that_has_not_joined_is_not_ready() {
        let probes = Probes {
            node: "n1".parse().expect("a node id"),
            membership: Some(Membership::detached()),
            denylist_max_age: Duration::from_secs(5),
            control: None,
        };
        assert_eq!(probes.health(), Health::Serving);
        let readiness = probes.readiness();
        assert!(!readiness.ready);
        let failing: Vec<&str> = readiness
            .checks
            .iter()
            .filter(|check| !check.ok)
            .map(|check| check.name)
            .collect();
        assert_eq!(failing, ["joined", "ring", "denylist"]);
        let body = serde_json::to_value(&readiness).expect("readiness serializes");
        assert_eq!(body["ready"], serde_json::json!(false));
        assert_eq!(body["checks"][0]["name"], serde_json::json!("joined"));
        assert_eq!(body["checks"][3]["ok"], serde_json::json!(true));
        assert_eq!(body["checks"][3]["detail"], serde_json::json!(""));
    }

    /// A node that runs both roles checks tokens against the control state, which is always
    /// current, so a missing or stale heartbeat copy of the denylist doesn't make it unready.
    #[tokio::test]
    async fn a_node_that_runs_control_judges_the_denylist_it_checks_tokens_against() {
        let dir = tempfile::tempdir().expect("a directory");
        let options = crate::control::ControlOptions::default()
            .with_storage_io(crate::control::StorageIo::Inline);
        let node = crate::control::test_support::open_node(
            dir.path().join("control.db"),
            options,
            crate::clock::SimClock::new(fleetring_control::Timestamp::ZERO),
        )
        .await
        .expect("a control node");
        let both = Probes {
            node: "n1".parse().expect("a node id"),
            // A membership whose denylist never arrived.
            membership: Some(Membership::detached()),
            denylist_max_age: Duration::from_secs(5),
            control: Some(node.handle()),
        };
        let denylist = both
            .readiness()
            .checks
            .into_iter()
            .find(|check| check.name == "denylist")
            .expect("a denylist check");
        assert!(denylist.ok, "{denylist:?}");
        let data_only = Probes {
            control: None,
            ..both
        };
        let denylist = data_only
            .readiness()
            .checks
            .into_iter()
            .find(|check| check.name == "denylist")
            .expect("a denylist check");
        assert_eq!(denylist.detail, "no denylist yet");
        node.shutdown().await;
    }

    /// A denylist holding an entry the node can't read refuses every token however fresh it is,
    /// so the node isn't ready, and the check says which entry (ADR-0047, point 3).
    #[test]
    fn an_unusable_denylist_is_not_ready() {
        let membership = Membership::detached();
        let probes = Probes {
            node: "n1".parse().expect("a node id"),
            membership: Some(membership.clone()),
            denylist_max_age: Duration::from_secs(5),
            control: None,
        };
        membership
            .denylist()
            .receive(Err(crate::auth::UnreadableEntry::new(3, "unknown case")));
        let denylist = probes
            .readiness()
            .checks
            .into_iter()
            .find(|check| check.name == "denylist")
            .expect("a denylist check");
        assert!(!denylist.ok);
        assert_eq!(
            denylist.detail,
            "denylist entry 3 is unreadable (unknown case); tokens are refused"
        );
        membership
            .denylist()
            .replace(fleetring_control::Revocations::default());
        let denylist = probes
            .readiness()
            .checks
            .into_iter()
            .find(|check| check.name == "denylist")
            .expect("a denylist check");
        assert!(denylist.ok, "{denylist:?}");
    }

    /// A one-voter control group is ready: its node knows a leader, itself, and is a voter. A
    /// node that waits to join a group knows no leader and is no member (ADR-0040 point 10).
    #[tokio::test(start_paused = true)]
    async fn readiness_reports_leader_and_control_group() {
        let dir = tempfile::tempdir().expect("a directory");
        let options = crate::control::ControlOptions::default()
            .with_storage_io(crate::control::StorageIo::Inline);
        let node = crate::control::test_support::open_node(
            dir.path().join("control.db"),
            options.clone(),
            crate::clock::SimClock::new(fleetring_control::Timestamp::ZERO),
        )
        .await
        .expect("a control node");
        let probes = Probes {
            node: "control-1".parse().expect("a node id"),
            membership: None,
            denylist_max_age: Duration::from_secs(5),
            control: Some(node.handle()),
        };
        let readiness = probes.readiness();
        assert!(readiness.ready, "{readiness:?}");
        let names: Vec<&str> = readiness.checks.iter().map(|check| check.name).collect();
        assert_eq!(names, ["control_log", "leader", "control_group"]);
        node.shutdown().await;

        let joining = crate::control::test_support::open_node(
            dir.path().join("joining.db"),
            options.with_bootstrap(crate::control::Bootstrap::Join),
            crate::clock::SimClock::new(fleetring_control::Timestamp::ZERO),
        )
        .await
        .expect("a joining control node");
        let probes = Probes {
            control: Some(joining.handle()),
            ..probes
        };
        let readiness = probes.readiness();
        assert!(!readiness.ready);
        let failing: Vec<&str> = readiness
            .checks
            .iter()
            .filter(|check| !check.ok)
            .map(|check| check.name)
            .collect();
        assert_eq!(failing, ["leader", "control_group"]);
        joining.shutdown().await;
    }

    #[test]
    fn a_node_without_roles_is_ready() {
        let probes = Probes {
            node: "n1".parse().expect("a node id"),
            membership: None,
            denylist_max_age: Duration::from_secs(5),
            control: None,
        };
        assert_eq!(
            probes.readiness(),
            Readiness {
                ready: true,
                checks: Vec::new()
            }
        );
    }
}
