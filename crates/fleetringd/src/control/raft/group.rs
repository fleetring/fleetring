//! The control group's software, as its members' Raft messages report it, and the group gate
//! over it: the feature gate and the release window over the control group's members as well as
//! the fleet's (ADR-0040 point 4).
//!
//! - **Where a member's software comes from.** Every `RaftPayload`, request or reply, carries its
//!   sender's software and clock. The network records each reply, the `Raft` service each
//!   request, in [`GroupSoftware`], with when it was heard on `tokio::time`. A new term on this
//!   node clears it, so a new leader starts with no reports and hears from every reachable member
//!   within one Raft heartbeat.
//! - **A report goes stale.** It counts as known only while it was heard within one shortest
//!   election timeout. A report whose release or features differ from the previous one counts as
//!   unknown until every proposal the leader had made when it heard the change has been answered,
//!   so a command screened against the old report can't commit unnoticed.
//! - **The gate.** [`GroupGate`] refuses the first use of a fleet feature unless every voter's and
//!   learner's software is known and supports it, and a join whose release line can't run beside
//!   a group member's.
//! - **Clocks are watched, never used.** The largest offset between a member's clock and this
//!   node's is exported, with a warning at most once a minute above the failover margin.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{Action, Feature, Member, Millis, Rejection, Release, Software, State};
use tokio::time::Instant;

use crate::control::raft::{ControlPeer, RaftId, raft_id};
use crate::control::screen::Gate;
use crate::observe::ControlMetrics;

/// How often, at most, a control node warns that a member's clock is off.
const OFFSET_WARNING_EVERY: Duration = Duration::from_secs(60);

/// The release a refusal names for a member whose software isn't known: "hasn't reported its
/// software yet" (ADR-0040 point 4).
const UNREPORTED: Release = Release::new(0, 0, 0);

/// What this node last heard of a member's software.
#[derive(Debug, Clone)]
struct Heard {
    software: Software,
    /// The member's clock minus this node's when the message arrived, in milliseconds.
    offset_ms: i64,
    /// When it was heard.
    at: Instant,
    /// The proposal batch that must be answered before a changed report counts, if it changed.
    settles_after: Option<u64>,
}

#[derive(Debug, Default)]
struct Inner {
    heard: BTreeMap<RaftId, Heard>,
    /// The control group's voters and learners, as this node's membership names them.
    members: BTreeMap<RaftId, ControlPeer>,
    /// How many proposal batches this node has handed to openraft.
    proposed: u64,
    /// How many of them openraft has answered in full.
    answered: u64,
    /// When this node last warned of a member's clock.
    warned_at: Option<Instant>,
}

/// What a control node knows of its group members' software (ADR-0040 point 4). Cloning is
/// cheap; clones share what was heard.
#[derive(Debug, Clone)]
pub struct GroupSoftware {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    inner: Mutex<Inner>,
    own: RaftId,
    own_node: NodeId,
    own_software: Software,
    stale_after: Duration,
    offset_warning: Millis,
    metrics: ControlMetrics,
}

/// A member's software as the gate sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Report {
    /// Heard within the staleness bound, and settled.
    Known(Software),
    /// Never heard, heard too long ago, or changed and not yet settled.
    Unknown,
}

impl GroupSoftware {
    /// Returns what the node `own`, running `own_software`, knows of its group: nothing yet.
    /// Reports older than `stale_after` count as unknown, and a clock offset above
    /// `offset_warning` is warned of.
    pub(crate) fn new(
        own: NodeId,
        own_software: Software,
        stale_after: Duration,
        offset_warning: Millis,
        metrics: ControlMetrics,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                inner: Mutex::new(Inner::default()),
                own: raft_id(own.as_str()),
                own_node: own,
                own_software,
                stale_after,
                offset_warning,
                metrics,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.shared
            .inner
            .lock()
            .expect("group software lock poisoned")
    }

    /// The software this node reports to its group.
    #[must_use]
    pub fn own_software(&self) -> &Software {
        &self.shared.own_software
    }

    /// Records that the member `from` reported `software`, with its clock at `sent_at_ms` when
    /// this node's read `now_ms`.
    pub(crate) fn record(&self, from: RaftId, software: Software, sent_at_ms: u64, now_ms: u64) {
        if from == self.shared.own {
            return;
        }
        let offset = i128::from(sent_at_ms) - i128::from(now_ms);
        let offset_ms =
            i64::try_from(offset).unwrap_or(if offset < 0 { i64::MIN } else { i64::MAX });
        let now = Instant::now();
        let mut inner = self.lock();
        let proposed = inner.proposed;
        let settles_after = match inner.heard.get(&from) {
            Some(previous)
                if previous.software.release() != software.release()
                    || !previous.software.features().eq(software.features()) =>
            {
                Some(proposed)
            }
            Some(previous) => previous.settles_after,
            None => None,
        };
        inner.heard.insert(
            from,
            Heard {
                software,
                offset_ms,
                at: now,
                settles_after,
            },
        );
        let largest = inner
            .heard
            .values()
            .map(|heard| heard.offset_ms.unsigned_abs())
            .max()
            .unwrap_or_default();
        #[expect(
            clippy::cast_precision_loss,
            reason = "a gauge in seconds; milliseconds below 2^52 are exact"
        )]
        self.shared.metrics.clock_offset(largest as f64 / 1000.0);
        let due = inner
            .warned_at
            .is_none_or(|warned| now.duration_since(warned) >= OFFSET_WARNING_EVERY);
        if largest > self.shared.offset_warning.get() && due {
            inner.warned_at = Some(now);
            tracing::warn!(
                offset_ms = largest,
                margin_ms = self.shared.offset_warning.get(),
                "a control node's clock differs from this node's by more than the failover \
                 margin; check NTP on every control node",
            );
        }
    }

    /// Forgets every report: this node saw a new term begin.
    pub(crate) fn clear(&self) {
        let mut inner = self.lock();
        inner.heard.clear();
        inner.warned_at = None;
        drop(inner);
        self.shared.metrics.clock_offset(0.0);
    }

    /// Adds `peer` as a member before openraft's membership names it: a node being added, which
    /// the gate counts from then on.
    pub(crate) fn add_member(&self, id: RaftId, peer: ControlPeer) {
        self.lock().members.insert(id, peer);
    }

    /// Every voter and learner with the release it last reported this term, however long ago, in
    /// node ID order; `None` for one not heard this term. The release window reads these: a
    /// member's release doesn't change while it is quiet.
    pub(crate) fn releases(&self) -> Vec<(NodeId, Option<Release>)> {
        let inner = self.lock();
        let mut releases: Vec<(NodeId, Option<Release>)> = inner
            .members
            .iter()
            .map(|(id, peer)| {
                let name = peer
                    .node_id
                    .parse()
                    .unwrap_or_else(|_| self.shared.own_node.clone());
                let release = if *id == self.shared.own {
                    Some(self.shared.own_software.release().clone())
                } else {
                    inner
                        .heard
                        .get(id)
                        .map(|heard| heard.software.release().clone())
                };
                (name, release)
            })
            .collect();
        releases.sort_by(|left, right| left.0.cmp(&right.0));
        releases
    }

    /// Sets the group's voters and learners, as this node's membership names them.
    pub(crate) fn set_members(&self, members: BTreeMap<RaftId, ControlPeer>) {
        self.lock().members = members;
    }

    /// Notes that a batch of proposals goes to openraft.
    pub(crate) fn proposing(&self) {
        self.lock().proposed += 1;
    }

    /// Notes that openraft has answered every proposal handed to it so far.
    pub(crate) fn idle(&self) {
        let mut inner = self.lock();
        inner.answered = inner.proposed;
    }

    /// The software this node last heard from `member`, however long ago; this node's own for
    /// itself.
    #[must_use]
    pub fn last_heard(&self, member: RaftId) -> Option<Software> {
        if member == self.shared.own {
            return Some(self.shared.own_software.clone());
        }
        self.lock()
            .heard
            .get(&member)
            .map(|heard| heard.software.clone())
    }

    /// Every voter and learner with its report, in node ID order, named by its node ID; a
    /// member whose node ID doesn't parse, which no membership this build writes holds, is named
    /// as this node.
    pub(crate) fn reports(&self) -> Vec<(NodeId, Report)> {
        let mut inner = self.lock();
        let answered = inner.answered;
        let stale_after = self.shared.stale_after;
        let members: Vec<(RaftId, String)> = inner
            .members
            .iter()
            .map(|(id, peer)| (*id, peer.node_id.clone()))
            .collect();
        let mut reports: Vec<(NodeId, Report)> = members
            .into_iter()
            .map(|(id, node_id)| {
                let name = node_id
                    .parse()
                    .unwrap_or_else(|_| self.shared.own_node.clone());
                if id == self.shared.own {
                    return (name, Report::Known(self.shared.own_software.clone()));
                }
                let report = match inner.heard.get_mut(&id) {
                    None => Report::Unknown,
                    Some(heard) if heard.at.elapsed() > stale_after => Report::Unknown,
                    Some(heard) => match heard.settles_after {
                        Some(batch) if answered < batch => Report::Unknown,
                        Some(_) | None => {
                            heard.settles_after = None;
                            Report::Known(heard.software.clone())
                        }
                    },
                };
                (name, report)
            })
            .collect();
        reports.sort_by(|left, right| left.0.cmp(&right.0));
        reports
    }
}

/// The group gate of a control node (ADR-0040 point 4): the feature gate and the release window
/// over the control group's voters and learners, with the software their Raft messages carry.
///
/// A one-voter group's only member is this node, whose software supports every feature it can
/// log, so the fleet's own gate in `State::check_admission` decides alone.
#[derive(Debug, Clone)]
pub struct GroupGate {
    group: GroupSoftware,
}

impl GroupGate {
    /// Returns the gate over what `group` heard.
    pub(crate) fn new(group: GroupSoftware) -> Self {
        Self { group }
    }

    /// What the gate reads.
    #[must_use]
    pub fn group(&self) -> &GroupSoftware {
        &self.group
    }

    /// Whether every voter and learner is known to support `feature`, so that its first use may
    /// be logged.
    #[must_use]
    pub fn supports(&self, feature: Feature) -> bool {
        self.group.reports().iter().all(
            |(_, report)| matches!(report, Report::Known(software) if software.supports(feature)),
        )
    }

    /// Refuses `action` on `state` if it is the first use of a fleet feature some voter or
    /// learner doesn't support or hasn't reported, or a join whose release line can't run beside
    /// a member's known release.
    fn check_action(&self, action: &Action, state: &State) -> Result<(), Rejection> {
        let reports = self.group.reports();
        for feature in action.features() {
            if state.is_enabled(*feature) {
                continue;
            }
            for (id, report) in &reports {
                match report {
                    Report::Unknown => {
                        return Err(Rejection::FeatureUnsupported {
                            feature: *feature,
                            id: id.clone(),
                            release: UNREPORTED,
                        });
                    }
                    Report::Known(software) if !software.supports(*feature) => {
                        return Err(Rejection::FeatureUnsupported {
                            feature: *feature,
                            id: id.clone(),
                            release: software.release().clone(),
                        });
                    }
                    Report::Known(_) => {}
                }
            }
        }
        // A member that reports no software is taken to run 0.3.0 (ADR-0038 point 2), which no
        // supported release sends: only the fleet's own window judges it.
        // A member rejoining on the software recorded for it was admitted already, as
        // `check_admission` lets it rejoin. The window uses each control node's last-heard release
        // however old, and refuses while one hasn't been heard this term (ADR-0040 point 4).
        if let Action::Join { member } = action
            && let Some(joining) = member.software()
            && state.member(member.id()).and_then(Member::software) != Some(joining)
        {
            for (id, release) in self.group.releases() {
                let theirs = match release {
                    None => UNREPORTED,
                    Some(theirs) if joining.release().runs_beside(&theirs) => continue,
                    Some(theirs) => theirs,
                };
                return Err(Rejection::IncompatibleVersion {
                    id: member.id().clone(),
                    release: joining.release().clone(),
                    member: id,
                    member_release: theirs,
                });
            }
        }
        Ok(())
    }
}

impl Gate for GroupGate {
    fn check(&self, action: &Action, state: &State) -> Result<(), Rejection> {
        self.check_action(action, state)
    }
}

#[cfg(test)]
mod tests {
    use fleetring_control::{ControlConfig, Member};

    use super::*;

    const ELECTION: Duration = Duration::from_secs(1);

    fn every(release: Release) -> Software {
        Feature::ALL
            .into_iter()
            .fold(Software::new(release, 2), Software::with_feature)
    }

    fn without(release: Release, lacking: Feature) -> Software {
        Feature::ALL
            .into_iter()
            .filter(|feature| *feature != lacking)
            .fold(Software::new(release, 2), Software::with_feature)
    }

    fn group() -> GroupSoftware {
        let group = GroupSoftware::new(
            "c1".parse().expect("an id"),
            every(Release::new(0, 10, 0)),
            ELECTION,
            Millis::new(2_000),
            ControlMetrics::default(),
        );
        group.set_members(
            ["c1", "c2", "c3"]
                .into_iter()
                .map(|name| (raft_id(name), ControlPeer::new(name, "127.0.0.1:1")))
                .collect(),
        );
        group
    }

    fn undrain() -> Action {
        Action::Undrain {
            id: "data-1".parse().expect("an id"),
        }
    }

    fn named(rejection: &Rejection) -> Option<(&str, &Release)> {
        match rejection {
            Rejection::FeatureUnsupported { id, release, .. } => Some((id.as_str(), release)),
            _ => None,
        }
    }

    /// A member that hasn't reported holds back the first use of a feature, named with release
    /// 0.0.0, until it reports software that supports it.
    #[tokio::test(start_paused = true)]
    async fn the_first_use_of_a_feature_waits_for_every_member() {
        let group = group();
        let gate = GroupGate::new(group.clone());
        let state = State::new(ControlConfig::new());
        let refused = gate.check(&undrain(), &state).expect_err("nobody reported");
        assert_eq!(named(&refused), Some(("c2", &UNREPORTED)), "{refused:?}");
        group.record(raft_id("c2"), every(Release::new(0, 10, 0)), 0, 0);
        group.record(
            raft_id("c3"),
            without(Release::new(0, 10, 1), Feature::Undrain),
            0,
            0,
        );
        let refused = gate.check(&undrain(), &state).expect_err("c3 lacks it");
        assert_eq!(
            named(&refused),
            Some(("c3", &Release::new(0, 10, 1))),
            "{refused:?}"
        );
        assert!(!gate.supports(Feature::Undrain));
        group.record(raft_id("c3"), every(Release::new(0, 10, 1)), 0, 0);
        // A changed report settles at once while nothing is in flight.
        gate.check(&undrain(), &state)
            .expect("every member supports it");
        assert!(gate.supports(Feature::Undrain));
    }

    /// A report heard longer ago than one shortest election timeout counts as unknown.
    #[tokio::test(start_paused = true)]
    async fn a_report_older_than_an_election_timeout_counts_as_unknown() {
        let group = group();
        let gate = GroupGate::new(group.clone());
        let state = State::new(ControlConfig::new());
        for member in ["c2", "c3"] {
            group.record(raft_id(member), every(Release::new(0, 10, 0)), 0, 0);
        }
        gate.check(&undrain(), &state).expect("both reported");
        tokio::time::advance(ELECTION / 2).await;
        group.record(raft_id("c2"), every(Release::new(0, 10, 0)), 0, 0);
        tokio::time::advance(ELECTION / 2 + Duration::from_millis(1)).await;
        let refused = gate.check(&undrain(), &state).expect_err("c3 went quiet");
        assert_eq!(named(&refused), Some(("c3", &UNREPORTED)), "{refused:?}");
    }

    /// A report whose features changed while a proposal was in flight counts as unknown until
    /// that proposal has been answered, and then as what it says.
    #[tokio::test(start_paused = true)]
    async fn a_changed_report_settles_before_it_counts() {
        let group = group();
        let gate = GroupGate::new(group.clone());
        let state = State::new(ControlConfig::new());
        for member in ["c2", "c3"] {
            group.record(raft_id(member), every(Release::new(0, 10, 0)), 0, 0);
        }
        group.proposing();
        group.record(
            raft_id("c3"),
            without(Release::new(0, 10, 0), Feature::Undrain),
            0,
            0,
        );
        let refused = gate.check(&undrain(), &state).expect_err("unsettled");
        assert_eq!(named(&refused), Some(("c3", &UNREPORTED)), "{refused:?}");
        group.idle();
        let refused = gate
            .check(&undrain(), &state)
            .expect_err("settled, lacking");
        assert_eq!(
            named(&refused),
            Some(("c3", &Release::new(0, 10, 0))),
            "{refused:?}"
        );
    }

    /// A feature already enabled needs nothing more of the group, and a new term forgets every
    /// report.
    #[tokio::test(start_paused = true)]
    async fn an_enabled_feature_passes_and_a_new_term_forgets() {
        let group = group();
        let gate = GroupGate::new(group.clone());
        let mut state = State::new(ControlConfig::new());
        let joined = fleetring_control::apply(
            &mut state,
            fleetring_control::Command::new(
                fleetring_control::Timestamp::ZERO,
                Action::Join {
                    member: Member::new("data-1".parse().expect("an id"))
                        .with_software(every(Release::new(0, 10, 0))),
                },
            ),
        );
        assert!(joined.is_ok(), "{joined:?}");
        let _outcome = fleetring_control::apply(
            &mut state,
            fleetring_control::Command::new(
                fleetring_control::Timestamp::ZERO,
                Action::Drain {
                    id: "data-1".parse().expect("an id"),
                },
            ),
        );
        let _outcome = fleetring_control::apply(
            &mut state,
            fleetring_control::Command::new(fleetring_control::Timestamp::ZERO, undrain()),
        );
        assert!(state.is_enabled(Feature::Undrain));
        gate.check(&undrain(), &state)
            .expect("enabled, nothing to ask");
        group.record(raft_id("c2"), every(Release::new(0, 10, 0)), 0, 0);
        assert!(group.last_heard(raft_id("c2")).is_some());
        group.clear();
        let fresh = State::new(ControlConfig::new());
        assert!(gate.check(&undrain(), &fresh).is_err());
    }

    /// The window reads every control node's release: a join is refused, as unreported, while a
    /// member hasn't been heard this term, and a stale report still counts with the release it
    /// last gave, refusing a join two lines from it; a member rejoining on its recorded software
    /// passes, as `check_admission` lets it.
    #[tokio::test(start_paused = true)]
    async fn the_window_counts_unheard_and_stale_members() {
        let group = group();
        let gate = GroupGate::new(group.clone());
        let mut state = State::new(ControlConfig::new());
        let join = |release: Release| Action::Join {
            member: Member::new("data-1".parse().expect("an id")).with_software(every(release)),
        };
        group.record(raft_id("c2"), every(Release::new(0, 10, 0)), 0, 0);
        let refused = gate
            .check(&join(Release::new(0, 10, 0)), &state)
            .expect_err("c3 unheard");
        assert!(
            matches!(&refused, Rejection::IncompatibleVersion { member, member_release, .. }
                if member.as_str() == "c3" && *member_release == UNREPORTED),
            "{refused:?}"
        );
        group.record(raft_id("c3"), every(Release::new(0, 11, 0)), 0, 0);
        tokio::time::advance(ELECTION * 3).await;
        gate.check(&join(Release::new(0, 10, 0)), &state)
            .expect("stale, but within every member's window");
        let refused = gate
            .check(&join(Release::new(0, 9, 0)), &state)
            .expect_err("two lines from c3's last release");
        assert!(
            matches!(&refused, Rejection::IncompatibleVersion { member, .. } if member.as_str() == "c3"),
            "{refused:?}"
        );
        // A member already recorded with this software rejoins.
        let joined = fleetring_control::apply(
            &mut state,
            fleetring_control::Command::new(
                fleetring_control::Timestamp::ZERO,
                join(Release::new(0, 9, 0)),
            ),
        );
        assert!(joined.is_ok(), "{joined:?}");
        gate.check(&join(Release::new(0, 9, 0)), &state)
            .expect("a rejoin on the recorded software");
    }

    /// A join two release lines from a control node's known release is refused, naming it.
    #[tokio::test(start_paused = true)]
    async fn a_join_outside_a_control_nodes_window_is_refused() {
        let group = group();
        let gate = GroupGate::new(group.clone());
        group.record(raft_id("c2"), every(Release::new(0, 11, 0)), 0, 0);
        let state = State::new(ControlConfig::new());
        let join = Action::Join {
            member: Member::new("data-1".parse().expect("an id"))
                .with_software(every(Release::new(0, 9, 0))),
        };
        let refused = gate.check(&join, &state).expect_err("two lines from c2");
        assert!(
            matches!(&refused, Rejection::IncompatibleVersion { member, .. } if member.as_str() == "c2"),
            "{refused:?}"
        );
    }
}
