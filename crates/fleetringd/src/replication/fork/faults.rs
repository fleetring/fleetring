//! Fault hooks for the fork tests (FR-085): points where a test holds a data node's fork work
//! while it crashes the node or changes the fleet around it, and a stand-in for the free space
//! of its disk.
//!
//! The hooks exist only with the `fault-hooks` feature, which the integration tests enable and
//! a build without debug assertions refuses to compile. Without it, [`ForkFaults`] is an empty value whose checks do
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
    /// A source's primary has pinned the source for a fork's seeder and hasn't sent the copy's
    /// first chunk yet: held, the seeder's stream is open and the pin is held.
    Streaming,
    /// A node has admitted a fork's copy on its disk, reserving room for it, and hasn't received
    /// its bytes yet.
    Admitted,
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
    /// A node is about to follow control state's view of a fork that is ready: held, a primary
    /// here keeps counting the replica set it saw before, as one that hasn't looked again yet
    /// does.
    FollowReady,
    /// A primary has brought its hash chain up to the log's end for a checkpoint and hasn't run
    /// the checkpoint yet: held, writes land in between, which the restarted log no longer
    /// holds once it runs. Held only for every database, with [`hold`](ForkFaults::hold).
    Checkpoint,
    /// A node has written a fork's copy and its sidecar to temporary files and synced them, and
    /// hasn't renamed either into place: a crash here leaves only temporary files.
    StagingWritten,
    /// A node has renamed a fork's copy into place and synced its directory, and hasn't renamed
    /// the sidecar that commits it: a crash here leaves a copy without a sidecar.
    StagingRenamed,
    /// A node has renamed the sidecar that commits a fork's copy into place, and hasn't synced
    /// the directory yet: a crash here leaves the copy committed, and a power loss may undo the
    /// rename.
    StagingUnsynced,
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
    /// How many times work has passed each point, held or not, for all forks or one
    /// operation's.
    passed: std::sync::Mutex<BTreeMap<(FaultPoint, Scope), u64>>,
    /// The free space and the size of the node's data volume the node believes it has, in
    /// bytes, less what its data directory grows by from then on, in place of the volume's own.
    disk: std::sync::Mutex<Option<(u64, u64)>>,
    /// Whether the control service fails the reply to the next fork request it applies.
    drop_reply: std::sync::atomic::AtomicBool,
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
                drop_reply: std::sync::atomic::AtomicBool::new(false),
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
                .iter()
                .filter(|((passed, _), _)| *passed == point)
                .map(|(_, count)| *count)
                .sum()
        })
    }

    /// How many times work has passed each point, held or not, by point and by the fork
    /// operation it belonged to, if it belonged to one.
    #[must_use]
    pub fn passes_by_operation(&self) -> BTreeMap<(FaultPoint, Option<OperationId>), u64> {
        self.hooks.as_ref().map_or_else(BTreeMap::new, |hooks| {
            hooks
                .passed
                .lock()
                .expect("fault hooks lock poisoned")
                .iter()
                .map(|((point, scope), count)| {
                    ((*point, scope.map(OperationId::from_bytes)), *count)
                })
                .collect()
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

    /// Makes the control service apply the next fork request it gets and then answer
    /// `UNAVAILABLE`, as a reply lost on the way would, once.
    pub fn drop_next_fork_reply(&self) {
        if let Some(hooks) = &self.hooks {
            hooks
                .drop_reply
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Whether the control service is still to drop the reply to a fork request: `false` once
    /// it dropped the one [`drop_next_fork_reply`](Self::drop_next_fork_reply) asked for.
    #[must_use]
    pub fn drops_next_fork_reply(&self) -> bool {
        self.hooks
            .as_ref()
            .is_some_and(|hooks| hooks.drop_reply.load(std::sync::atomic::Ordering::SeqCst))
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
    pub(crate) async fn pass(&self, point: FaultPoint, operation: OperationId) {
        self.pass_scoped(point, Some(operation)).await;
    }

    /// Passes `point` in work that belongs to no fork operation, waiting while a test holds it
    /// for every operation.
    pub(crate) async fn pass_any(&self, point: FaultPoint) {
        self.pass_scoped(point, None).await;
    }

    #[cfg_attr(
        not(feature = "fault-hooks"),
        expect(
            clippy::unused_async,
            reason = "the hooks wait here only with the `fault-hooks` feature"
        )
    )]
    async fn pass_scoped(&self, point: FaultPoint, operation: Option<OperationId>) {
        #[cfg(feature = "fault-hooks")]
        if let Some(hooks) = &self.hooks {
            let scope = operation.map(|operation| operation.to_bytes());
            *hooks
                .passed
                .lock()
                .expect("fault hooks lock poisoned")
                .entry((point, scope))
                .or_default() += 1;
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

    /// Whether the reply to the fork request just applied is to be dropped; true once per
    /// [`drop_next_fork_reply`](Self::drop_next_fork_reply).
    pub(crate) fn take_reply_drop(&self) -> bool {
        #[cfg(feature = "fault-hooks")]
        {
            self.hooks.as_ref().is_some_and(|hooks| {
                hooks
                    .drop_reply
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            })
        }
        #[cfg(not(feature = "fault-hooks"))]
        false
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
