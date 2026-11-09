//! Fault hooks for the fork tests (FR-085): points where a test holds a data node's fork work
//! while it crashes the node or changes the fleet around it, and a stand-in for the free space
//! of its disk.
//!
//! The hooks exist only with the `fault-hooks` feature, which the integration tests enable and
//! release builds never do. Without it, [`ForkFaults`] is an empty value whose checks do
//! nothing, and a daemon can't be given one that does.

#[cfg(feature = "fault-hooks")]
use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "fault-hooks")]
use std::sync::Arc;
#[cfg(feature = "fault-hooks")]
use std::time::Duration;

use fleetring_control::fork::OperationId;
#[cfg(feature = "fault-hooks")]
use tokio::sync::watch;

/// A point in a data node's fork work where a test may hold it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum FaultPoint {
    /// A node is about to ask for a copy of a fork: the seeder from the source's primary, or a
    /// peer from the seeder.
    Fetch,
    /// A node has committed a staged copy to disk and hasn't made it known yet: heartbeats
    /// don't report it, and peers can't fetch it.
    Staged,
    /// A seeded fork's first live holder has checked its staged copy and hasn't asked for the
    /// fork's first lease yet: held, the fork stays seeded with no primary.
    FirstLease,
    /// A fork's primary has opened its copy and hasn't run the fork-reset write yet.
    BeforeReset,
    /// A fork's primary reports its fork-reset write running, and hasn't run it yet. The write is
    /// one transaction, so a crash here leaves no part of it, as before it.
    DuringReset,
    /// A fork's primary committed its fork-reset write locally, and hasn't waited for its
    /// bootstrap quorum yet. Passed only when the write ran: a primary whose copy holds the
    /// marker already passes [`AfterQuorumCommit`](Self::AfterQuorumCommit) alone.
    AfterLocalCommit,
    /// A fork's fork-reset write committed at its bootstrap quorum, and its primary hasn't
    /// reported it or served a client yet.
    AfterQuorumCommit,
    /// A follower of a fork's primary is about to look for the fork-reset write the primary
    /// reported committed in its durable log, and to report it there: held, the follower
    /// reports nothing, so the fork stays seeded while its primary serves.
    FollowerReset,
}

/// The fault hooks a daemon's fork work passes through.
///
/// Cloning is cheap; clones share the hooks, so a test keeps one and gives the daemon another.
#[derive(Debug, Clone, Default)]
pub struct ForkFaults {
    #[cfg(feature = "fault-hooks")]
    hooks: Option<Arc<Hooks>>,
}

/// Which fork work a hold applies to: all of it, or one fork operation's.
#[cfg(feature = "fault-hooks")]
type Scope = Option<[u8; 16]>;

#[cfg(feature = "fault-hooks")]
#[derive(Debug)]
struct Hooks {
    /// The points work is held at, each for all forks or one operation's.
    held: watch::Sender<BTreeSet<(FaultPoint, Scope)>>,
    /// How many times work has arrived at each point while it was held.
    arrived: watch::Sender<BTreeMap<FaultPoint, u64>>,
    /// How many times work has passed each point, held or not.
    passed: std::sync::Mutex<BTreeMap<FaultPoint, u64>>,
    /// The free space and the size of the node's data volume the node believes it has, in
    /// bytes, less what its data directory grows by from then on, in place of the volume's own.
    disk: std::sync::Mutex<Option<(u64, u64)>>,
}

#[cfg(feature = "fault-hooks")]
impl ForkFaults {
    /// Hooks that hold nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            hooks: Some(Arc::new(Hooks {
                held: watch::Sender::new(BTreeSet::new()),
                arrived: watch::Sender::new(BTreeMap::new()),
                passed: std::sync::Mutex::new(BTreeMap::new()),
                disk: std::sync::Mutex::new(None),
            })),
        }
    }

    fn set(&self, point: FaultPoint, scope: Scope, held: bool) {
        if let Some(hooks) = &self.hooks {
            hooks.held.send_modify(|holds| {
                if held {
                    holds.insert((point, scope));
                } else {
                    holds.remove(&(point, scope));
                }
            });
        }
    }

    /// Holds every piece of fork work that reaches `point`, until [`release`](Self::release).
    pub fn hold(&self, point: FaultPoint) {
        self.set(point, None, true);
    }

    /// Lets the work held at `point` go on, and lets later work pass it.
    pub fn release(&self, point: FaultPoint) {
        self.set(point, None, false);
    }

    /// Holds the work of the fork operation `operation` that reaches `point`, until
    /// [`release_operation`](Self::release_operation).
    pub fn hold_operation(&self, point: FaultPoint, operation: OperationId) {
        self.set(point, Some(operation.to_bytes()), true);
    }

    /// Lets the work of `operation` held at `point` go on.
    pub fn release_operation(&self, point: FaultPoint, operation: OperationId) {
        self.set(point, Some(operation.to_bytes()), false);
    }

    /// How many times work has arrived at `point` while it was held.
    #[must_use]
    pub fn arrivals(&self, point: FaultPoint) -> u64 {
        self.hooks.as_ref().map_or(0, |hooks| {
            hooks.arrived.borrow().get(&point).copied().unwrap_or(0)
        })
    }

    /// How many times work has passed `point`, held or not.
    #[must_use]
    pub fn passes(&self, point: FaultPoint) -> u64 {
        self.hooks.as_ref().map_or(0, |hooks| {
            hooks
                .passed
                .lock()
                .expect("fault hooks lock poisoned")
                .get(&point)
                .copied()
                .unwrap_or(0)
        })
    }

    /// Waits until work has arrived at `point` while it was held at least `count` times, or
    /// `limit` passes. Returns whether it has.
    pub async fn await_arrivals(&self, point: FaultPoint, count: u64, limit: Duration) -> bool {
        let Some(hooks) = &self.hooks else {
            return false;
        };
        let mut arrived = hooks.arrived.subscribe();
        let reached =
            arrived.wait_for(|arrived| arrived.get(&point).copied().unwrap_or(0) >= count);
        matches!(tokio::time::timeout(limit, reached).await, Ok(Ok(_)))
    }

    /// Makes the node believe its data volume holds `volume` bytes, of which `free` are free
    /// less what its data directory grows by from now on, in place of what the volume says.
    pub fn set_disk(&self, free: u64, volume: u64) {
        if let Some(hooks) = &self.hooks {
            *hooks.disk.lock().expect("fault hooks lock poisoned") = Some((free, volume));
        }
    }
}

impl ForkFaults {
    /// Passes `point` in the work of the fork operation `operation`, waiting while a test holds
    /// it.
    #[cfg_attr(
        not(feature = "fault-hooks"),
        expect(
            clippy::unused_async,
            reason = "the hooks wait here only with the `fault-hooks` feature"
        )
    )]
    pub(crate) async fn pass(&self, point: FaultPoint, operation: OperationId) {
        #[cfg(feature = "fault-hooks")]
        if let Some(hooks) = &self.hooks {
            *hooks
                .passed
                .lock()
                .expect("fault hooks lock poisoned")
                .entry(point)
                .or_default() += 1;
            let scope = Some(operation.to_bytes());
            let applies = |held: &BTreeSet<(FaultPoint, Scope)>| {
                held.contains(&(point, None)) || held.contains(&(point, scope))
            };
            if applies(&hooks.held.borrow()) {
                hooks.arrived.send_modify(|arrived| {
                    *arrived.entry(point).or_default() += 1;
                });
                let mut held = hooks.held.subscribe();
                // The sender lives as long as the hooks.
                let _ = held.wait_for(|held| !applies(held)).await;
            }
        }
        #[cfg(not(feature = "fault-hooks"))]
        let _ = (point, operation);
    }

    /// The free space and size of the data volume a test made the node believe it has, if it
    /// did.
    pub(crate) fn disk(&self) -> Option<(u64, u64)> {
        #[cfg(feature = "fault-hooks")]
        {
            self.hooks
                .as_ref()
                .and_then(|hooks| *hooks.disk.lock().expect("fault hooks lock poisoned"))
        }
        #[cfg(not(feature = "fault-hooks"))]
        None
    }
}
