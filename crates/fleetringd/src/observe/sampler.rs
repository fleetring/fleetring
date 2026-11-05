//! The sampler: refreshes the gauges that summarize the node's databases and the fleet's members
//! (ADR-0034 point 7).
//!
//! A spread over databases is never a label: each tick counts, sums, and takes maxima over the
//! node's databases, reading only local state. Every labelled gauge sets each value of its closed
//! set, zeros included, so a state that empties reads 0 rather than vanishing.

use std::time::Duration;

use fleetring_control::Liveness;
use fleetring_proto::v1::ReplicaRole;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::changes::ChangeStreams;
use crate::control::ControlHandle;
use crate::observe::metrics::{ControlSample, DataSample, SampledMetrics};
use crate::replication::Replication;
use crate::{DatabaseStatuses as _, Membership};

// A follower's state, as its index in `FOLLOWER_STATES`.
const CAUGHT_UP: usize = 0;
const BEHIND: usize = 1;
const DISCONNECTED: usize = 2;
const RESEEDING: usize = 3;

/// What the sampler reads. A node runs one role or both, so each role's part is optional.
#[derive(Debug, Clone, Default)]
pub(crate) struct Sources {
    pub(crate) membership: Option<Membership>,
    pub(crate) replication: Option<Replication>,
    pub(crate) changes: Option<ChangeStreams>,
    /// Every change sink's name, so each sink's backlog reads 0 before it has a database.
    pub(crate) sinks: Vec<String>,
    pub(crate) control: Option<ControlHandle>,
}

/// Refreshes every sampled gauge at once, then every `interval`. Dropping it stops it.
#[derive(Debug)]
pub(crate) struct Sampler {
    task: JoinHandle<()>,
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Sampler {
    /// Starts sampling `sources` into `metrics` on the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// If called outside a Tokio runtime.
    pub(crate) fn start(sources: Sources, metrics: SampledMetrics, interval: Duration) -> Self {
        let task = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(interval);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticks.tick().await;
                sample(&sources, &metrics);
            }
        });
        Self { task }
    }
}

/// Takes one sample of every role the node runs.
pub(crate) fn sample(sources: &Sources, metrics: &SampledMetrics) {
    if let Some(membership) = &sources.membership {
        metrics.set_data(&data_sample(sources, membership));
    }
    if let Some(control) = &sources.control {
        metrics.set_control(&control_sample(control));
    }
}

fn data_sample(sources: &Sources, membership: &Membership) -> DataSample {
    let denylist = membership.denylist();
    let mut sample = DataSample {
        denylist_age: denylist.age().map(|age| age.as_secs_f64()),
        denylist_entries: denylist.load().len(),
        membership_epoch: membership.epoch().get(),
        ..DataSample::default()
    };
    if let Some(replication) = &sources.replication {
        for status in replication.statuses() {
            if status.role == i32::from(ReplicaRole::Primary) {
                sample.primaries += 1;
            } else {
                sample.followers += 1;
            }
        }
        sample.open = replication.registry().open_count();
        sample.orphaned = replication.orphan_count();
        let served: Vec<_> = replication.served().borrow().keys().cloned().collect();
        sample.leases_held = served.len();
        for database in &served {
            if replication
                .holders(database)
                .is_some_and(|holders| holders.is_under_replicated())
            {
                sample.under_replicated += 1;
            }
            let Some(status) = replication.primary_status(database) else {
                continue;
            };
            for follower in &status.followers {
                let state = if follower.needs_snapshot {
                    RESEEDING
                } else if !follower.connected {
                    DISCONNECTED
                } else if follower.reported && follower.acknowledged == status.committed {
                    CAUGHT_UP
                } else {
                    BEHIND
                };
                sample.follower_states[state] += 1;
            }
        }
    }
    let statuses = sources
        .changes
        .as_ref()
        .map(ChangeStreams::status)
        .unwrap_or_default();
    sample.backlogs = sources
        .sinks
        .iter()
        .map(|sink| {
            let backlogs = statuses
                .iter()
                .filter(|status| status.sink == *sink)
                .map(|status| status.backlog);
            let (total, max) = backlogs.fold((0_u64, 0_u64), |(total, max), backlog| {
                (total.saturating_add(backlog), max.max(backlog))
            });
            (sink.clone(), total, max)
        })
        .collect();
    sample
}

fn control_sample(control: &ControlHandle) -> ControlSample {
    let state = control.state();
    let now = state.now();
    let mut sample = ControlSample {
        mailbox_depth: control.queued(),
        databases: state.databases().len(),
        promotions_in_progress: state
            .databases()
            .filter(|database| database.promotion().is_some())
            .count(),
        // One control node is the control plane's leader until FR-064's Raft group.
        leader: control.is_running(),
        ..ControlSample::default()
    };
    for member in state.members() {
        match state.liveness(member.id(), now) {
            Some(Liveness::Live) => sample.live += 1,
            _ => sample.down += 1,
        }
        if state.is_draining(member.id()) {
            sample.draining += 1;
        }
    }
    sample
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::names::FOLLOWER_STATES;

    #[test]
    fn follower_states_index_the_closed_set() {
        assert_eq!(FOLLOWER_STATES[CAUGHT_UP], "caught_up");
        assert_eq!(FOLLOWER_STATES[BEHIND], "behind");
        assert_eq!(FOLLOWER_STATES[DISCONNECTED], "disconnected");
        assert_eq!(FOLLOWER_STATES[RESEEDING], "reseeding");
    }
}
