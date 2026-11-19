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
    /// A seeded fork's first live holder has taken the fork's first lease and moved its staged
    /// copy into place, and hasn't asked control state to confirm the lease yet: held, its
    /// lease runs on while nothing is served.
    ConfirmFirstLease,
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
    /// A snapshot or a fork's seeding request found its database's hash chain lost, and is
    /// about to build it again: held, the chain stays loading. Held only for every database,
    /// with [`hold`](ForkFaults::hold).
    ChainRebuild,
    /// A primary building its hash chain has read where its log ends and hasn't read the log
    /// itself yet: held, writes land in between, so the log runs past the end it read. Held only
    /// for every database, with [`hold`](ForkFaults::hold).
    ChainEndRead,
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
    /// A node has moved a fork's staged copy into the database's place and synced it, and hasn't
    /// recorded the move beside the copy's sidecar, nor the copy's incarnation: a crash here
    /// leaves the copy in place with only the install's intent.
    InstallMoved,
    /// A replica has recorded a fence's epoch and holder, with the copy's incarnation, and
    /// hasn't read its copy's position yet: held, the fence's caller can give up, as one whose
    /// call ran out of time does, and fence again. Held only for every database, with
    /// [`hold`](ForkFaults::hold).
    FenceRecorded,
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

/// Whether a test holds work at a point, or lets it pass.
#[cfg(feature = "fault-hooks")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Hold {
    Held,
    Released,
}

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
    /// Whether the control service answers `Locate` as a control node from before replica sets
    /// were shown (0.7) does: without `placed_replicas` or `membership_epoch`.
    hide_replica_sets: std::sync::atomic::AtomicBool,
    /// How many of the next builds of a primary's hash chain fail.
    fail_chain_builds: std::sync::atomic::AtomicU32,
    /// How many of the next reads of a primary's log end fail.
    fail_position_reads: std::sync::atomic::AtomicU32,
}

/// Takes one from `count` unless it is 0. Returns whether it did.
#[cfg(feature = "fault-hooks")]
fn take_one(count: &std::sync::atomic::AtomicU32) -> bool {
    use std::sync::atomic::Ordering::SeqCst;
    let mut left = count.load(SeqCst);
    while left > 0 {
        match count.compare_exchange(left, left - 1, SeqCst, SeqCst) {
            Ok(_) => return true,
            Err(now) => left = now,
        }
    }
    false
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
                hide_replica_sets: std::sync::atomic::AtomicBool::new(false),
                fail_chain_builds: std::sync::atomic::AtomicU32::new(0),
                fail_position_reads: std::sync::atomic::AtomicU32::new(0),
            })),
        }
    }

    fn set(&self, point: FaultPoint, scope: Scope, hold: Hold) {
        if let Some(hooks) = &self.hooks {
            hooks.held.send_modify(|holds| match hold {
                Hold::Held => {
                    holds.insert((point, scope));
                }
                Hold::Released => {
                    holds.remove(&(point, scope));
                }
            });
        }
    }

    /// Holds every piece of fork work that reaches `point`, until [`release`](Self::release).
    pub fn hold(&self, point: FaultPoint) {
        self.set(point, None, Hold::Held);
    }

    /// Lets the work held at `point` go on, and lets later work pass it.
    pub fn release(&self, point: FaultPoint) {
        self.set(point, None, Hold::Released);
    }

    /// Holds the work of the fork operation `operation` that reaches `point`, until
    /// [`release_operation`](Self::release_operation).
    pub fn hold_operation(&self, point: FaultPoint, operation: OperationId) {
        self.set(point, Some(operation.to_bytes()), Hold::Held);
    }

    /// Lets the work of `operation` held at `point` go on.
    pub fn release_operation(&self, point: FaultPoint, operation: OperationId) {
        self.set(point, Some(operation.to_bytes()), Hold::Released);
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

    /// Makes the control service answer `Locate` as a control node from before replica sets
    /// were shown (0.7) does, without `placed_replicas` or `membership_epoch`, while `hidden`:
    /// how a scenario runs data nodes of this release under an older control node, and then
    /// upgrades it (ADR-0031, amendment 2).
    pub fn hide_replica_sets(&self, hidden: bool) {
        if let Some(hooks) = &self.hooks {
            hooks
                .hide_replica_sets
                .store(hidden, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Makes the next build of a primary's hash chain from its record and its log fail, once,
    /// as one whose reads a checkpoint restarted the log between did.
    pub fn fail_next_chain_build(&self) {
        self.fail_next_chain_builds(1);
    }

    /// Makes the next `count` builds of a primary's hash chain fail, as
    /// [`fail_next_chain_build`](Self::fail_next_chain_build) does the next one, in place of
    /// any count asked for before: 0 lets the next build run.
    pub fn fail_next_chain_builds(&self, count: u32) {
        if let Some(hooks) = &self.hooks {
            hooks
                .fail_chain_builds
                .store(count, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Makes the next `count` reads of where a primary's log ends fail, as an engine that can't
    /// answer does, in place of any count asked for before: 0 lets the next read run. A primary
    /// reads it when it starts serving, and each build of its hash chain reads it again.
    pub fn fail_next_position_reads(&self, count: u32) {
        if let Some(hooks) = &self.hooks {
            hooks
                .fail_position_reads
                .store(count, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Whether the next chain build is still to fail: `false` once every build
    /// [`fail_next_chain_builds`](Self::fail_next_chain_builds) asked to fail has failed.
    #[must_use]
    pub fn fails_next_chain_build(&self) -> bool {
        self.hooks.as_ref().is_some_and(|hooks| {
            hooks
                .fail_chain_builds
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        })
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

    /// Whether this chain build is to fail; true once per build
    /// [`fail_next_chain_builds`](Self::fail_next_chain_builds) asked to fail.
    pub(crate) fn take_chain_build_failure(&self) -> bool {
        #[cfg(feature = "fault-hooks")]
        {
            self.hooks
                .as_ref()
                .is_some_and(|hooks| take_one(&hooks.fail_chain_builds))
        }
        #[cfg(not(feature = "fault-hooks"))]
        false
    }

    /// Whether this read of a primary's log end is to fail; true once per read
    /// [`fail_next_position_reads`](Self::fail_next_position_reads) asked to fail.
    pub(crate) fn take_position_read_failure(&self) -> bool {
        #[cfg(feature = "fault-hooks")]
        {
            self.hooks
                .as_ref()
                .is_some_and(|hooks| take_one(&hooks.fail_position_reads))
        }
        #[cfg(not(feature = "fault-hooks"))]
        false
    }

    /// Whether the control service answers `Locate` without replica sets, as a 0.7 control node
    /// does ([`hide_replica_sets`](Self::hide_replica_sets)).
    pub(crate) fn hides_replica_sets(&self) -> bool {
        #[cfg(feature = "fault-hooks")]
        {
            self.hooks.as_ref().is_some_and(|hooks| {
                hooks
                    .hide_replica_sets
                    .load(std::sync::atomic::Ordering::SeqCst)
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
