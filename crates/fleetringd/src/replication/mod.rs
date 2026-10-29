//! Replication of frame-source databases: a primary's streams to its followers, and the
//! follower side of the `Replication` service (ADR-0016, ADR-0018).
//!
//! [`Replication::start`] builds both halves around one data node's [`Registry`]: the
//! [`ReplicatedSink`] the registry's actors publish every committed batch to, and the
//! [`ReplicationService`] that peers call. Everything here handles opaque [`Position`]s and
//! payloads only, and compares positions as bytes.
//!
//! # Protocol
//!
//! For each database it is primary for ([`Replication::serve_primary`]), a node keeps one
//! `Replicate` stream open to each follower in the database's replica set, which the ring picks
//! (`replicas_n`) from the published [`MembershipView`](crate::MembershipView):
//!
//! 1. The primary sends `ReplicateOpen` with the database and its epoch. The stream's
//!    [`PRIMARY_METADATA_KEY`] metadata names the primary.
//! 2. The follower checks the epoch against the highest it has recorded for the database,
//!    refuses a lower one, and records a higher one durably before it goes on.
//! 3. A follower with no copy fetches one from the primary through the `Snapshot` RPC and moves
//!    it into place (see [Snapshots](#snapshots)). It then answers with a first
//!    `ReplicateAck` that holds the position its copy is at.
//! 4. The primary sends `Batch`es from that position on. Each batch's `start` is exactly the
//!    position it continues from, so the follower checks continuity by comparing bytes. It applies
//!    each batch durably with [`FrameFollower::apply`](crate::engine::FrameFollower::apply), on a
//!    blocking thread, and acknowledges the position the batch ends at.
//! 5. After a checkpoint, the primary sends a `CheckpointMarker` to every follower that holds the
//!    log up to it, and the follower checkpoints its copy there.
//!
//! Up to [`ReplicationSettings::max_in_flight`] batches are in flight per follower. A commit
//! published while the pipeline is full joins the next batch: the stream reads every commit
//! since its last batch with one `committed_since` call (ADR-0016, point 11).
//!
//! # Write quorum and the replica floor
//!
//! A write's [`Acknowledgement`] completes once W − 1 followers have acknowledged a position at
//! or past the write's batch, where W comes from [`Quorums`](fleetring_control::Quorums) over
//! the replica set's size R in the database's [`Acks`](fleetring_control::Acks) mode
//! (ADR-0023): a majority for `quorum`, every replica for `all`, and the primary alone for
//! `local`.
//!
//! Before a write runs, the actor asks the sink ([`ReplicationSink::admit`]) whether at least
//! the database's `min_replicas` replicas, the primary included, are connected and live. If
//! not, the write is refused with [`ReplicationError::BelowFloor`] and nothing commits. A write
//! that passed the floor but finds fewer than W reachable when its batch is published fails at
//! once with [`ReplicationError::UnderReplicated`], and one whose quorum doesn't complete within
//! [`ReplicationSettings::ack_timeout`] fails too. In those two cases the write has committed on
//! the primary and may still replicate later; it just isn't acknowledged.
//!
//! # Checkpoints
//!
//! Once the batches published since the last checkpoint pass
//! [`ReplicationSettings::checkpoint_threshold`] bytes, and every follower has acknowledged
//! everything, the primary checkpoints (RESTART, ADR-0016 point 7). A follower that has been
//! behind for longer than [`ReplicationSettings::lag_limit`] stops blocking checkpoints. Once the
//! log restarts past it, it can no longer catch up from the log, and is flagged as needing a
//! snapshot ([`FollowerProgress::needs_snapshot`]). The engine also refuses to continue from an
//! earlier-generation position other than the one its last checkpoint recorded durably, so a
//! primary that restarted, and no longer knows its last checkpoint in memory, still finds such a
//! follower when it reads the log for it. A follower whose copy diverges from a new epoch's log
//! before a point it can be cut back to says so in its first acknowledgement (ADR-0022). In every
//! case, the primary sends the follower `Reseed`, and the follower re-seeds its copy from a
//! snapshot and acknowledges the snapshot's position.
//!
//! # Snapshots
//!
//! A follower never starts from an empty file. When a database's stream reaches a follower
//! without a copy, when the primary sends `Reseed`, or when [`Replication::seed`] is called, the
//! follower calls `Snapshot` on the primary. The primary pins a consistent copy through the
//! engine in one quick call on the database's thread, and streams the copy's files in chunks
//! from a task of its own, so writes go on meanwhile; checkpoints wait until the transfer ends.
//! The last chunk carries the copy's position and an XXH3-128 checksum of the stream, which
//! catches corruption, not tampering. The follower writes the chunks into a staging directory
//! as they arrive, verifies the checksum, syncs the files, and renames the directory into the
//! database's place, moving any old copy aside first and removing it after. An interrupted or
//! corrupt transfer is removed and restarts from the beginning; the database's place only ever
//! holds a complete copy, or none. Staging directories a crash left behind are removed when
//! replication starts.

mod archive;
mod epochs;
mod failover;
mod follower;
mod primary;
mod snapshot;

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, Durability, LeaseEpoch};
use fleetring_proto::v1::replication_server::ReplicationServer;
use fleetring_proto::v1::{DatabaseStatus, Role};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use self::epochs::Epochs;
use self::follower::FollowerSlot;
use self::primary::Primary;
use crate::clock::Clock;
use crate::engine::{
    CheckpointMarker, DatabaseEngine, EngineCapability, EngineKind, Execution, Position,
};
use crate::membership::DatabaseStatuses;
use crate::sink::{
    Acknowledged, Acknowledgement, CommittedBatch, ReplicationError, ReplicationSink,
};
use crate::transport::{Peers, replication_server};
use crate::{Membership, Registry};

pub use self::failover::{Fenced, PromotionRequest};
pub use self::follower::ReplicationService;

/// The metadata key on a `Replicate` stream that names the primary, so that a follower without a
/// copy knows whom to fetch one from.
pub const PRIMARY_METADATA_KEY: &str = "fleetring-primary";

/// Where a peer's `Replication` service listens: node ID to a `host:port` address.
///
/// The daemon uses its [`Membership`], whose members carry the addresses they advertised when
/// they joined. Tests may map node IDs to their own listeners instead.
pub trait PeerAddresses: fmt::Debug + Send + Sync + 'static {
    /// The `host:port` that `node`'s `Replication` service listens on, if it is known.
    fn address(&self, node: &NodeId) -> Option<String>;
}

impl PeerAddresses for BTreeMap<NodeId, String> {
    fn address(&self, node: &NodeId) -> Option<String> {
        self.get(node).cloned()
    }
}

/// A peer's address is the one it advertised when it joined, as the published membership view
/// holds it.
impl PeerAddresses for Membership {
    fn address(&self, node: &NodeId) -> Option<String> {
        self.view()
            .members()
            .find(|(member, _)| member.id() == node)
            .and_then(|(member, _)| member.address().map(str::to_owned))
    }
}

impl<T: PeerAddresses + ?Sized> PeerAddresses for Arc<T> {
    fn address(&self, node: &NodeId) -> Option<String> {
        (**self).address(node)
    }
}

/// How replication paces and bounds its work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSettings {
    /// How many batches may be in flight to one follower before new commits wait and are grouped
    /// into the next batch.
    pub max_in_flight: NonZeroUsize,
    /// How long a write waits for its quorum before it fails unacknowledged.
    pub ack_timeout: Duration,
    /// How long a stream with batches in flight may go without an acknowledgement before the
    /// primary drops its connection and dials a new one.
    pub stall_timeout: Duration,
    /// How long a stream may take to open, the follower's initial copy included.
    pub open_timeout: Duration,
    /// How long the primary waits before reopening a stream that failed, and how often it
    /// reconciles its streams with the replica set.
    pub retry_interval: Duration,
    /// How many bytes of batches may be published after a checkpoint before the next one is due.
    pub checkpoint_threshold: u64,
    /// How long a follower may stay behind before it stops blocking checkpoints.
    pub lag_limit: Duration,
    /// How many bytes of published batches the primary keeps for followers that haven't
    /// acknowledged them. Older batches are read back from the log.
    pub retained_bytes: usize,
}

impl ReplicationSettings {
    /// The defaults: 8 batches in flight, a 5 s acknowledgement timeout, a 2 s stall timeout,
    /// 30 s to open a stream, 200 ms between retries, a checkpoint every 4 MiB, a 60 s lag
    /// limit, and 64 MiB of retained batches.
    #[must_use]
    pub fn new() -> Self {
        Self {
            max_in_flight: NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
            ack_timeout: Duration::from_secs(5),
            stall_timeout: Duration::from_secs(2),
            open_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_millis(200),
            checkpoint_threshold: 4 * 1024 * 1024,
            lag_limit: Duration::from_secs(60),
            retained_bytes: 64 * 1024 * 1024,
        }
    }
}

impl Default for ReplicationSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// What [`Replication::start`] needs from the data node it runs on.
#[derive(Debug, Clone)]
pub struct ReplicationParts {
    /// This node's ID.
    pub node: NodeId,
    /// The engine the registry runs, for opening follower copies.
    pub engine: Arc<dyn DatabaseEngine>,
    /// The node's membership: placement and peer liveness come from [`Membership::view`].
    pub membership: Membership,
    /// The connection pool streams and snapshot transfers dial peers through.
    pub peers: Arc<Peers>,
    /// Where each peer's `Replication` service listens.
    pub addresses: Arc<dyn PeerAddresses>,
    /// The clock that times followers' lag.
    pub clock: Arc<dyn Clock>,
    /// Where blocking follower calls run: [`Execution::Inline`] under simulation.
    pub execution: Execution,
    /// A directory replication keeps its own durable state in, such as each database's highest
    /// epoch.
    pub state_dir: PathBuf,
    /// Pacing and bounds.
    pub settings: ReplicationSettings,
}

/// How a node serves a database as its primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryRole {
    /// The database's replication factor: the replica set is the ring's `replicas_n` for it.
    pub replication_factor: ReplicationFactor,
    /// The epoch every batch carries (ADR-0018).
    pub epoch: LeaseEpoch,
    /// The durability settings the lease was granted under: the
    /// [`Acks`](fleetring_control::Acks) mode that fixes W, and
    /// the floor on reachable replicas below which writes are refused (ADR-0023).
    pub durability: Durability,
}

impl PrimaryRole {
    /// The role for a database with `replication_factor` replicas at `epoch`, in
    /// [`Acks::Quorum`](fleetring_control::Acks::Quorum) with the floor at W.
    #[must_use]
    pub const fn new(replication_factor: ReplicationFactor, epoch: LeaseEpoch) -> Self {
        Self {
            replication_factor,
            epoch,
            durability: Durability::quorum(replication_factor),
        }
    }

    /// The same role under `durability`.
    #[must_use]
    pub const fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }
}

/// The error returned when [`Replication::seed`] can't seed a copy.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SeedError {
    /// The engine replicates itself, so there is no copy to seed.
    #[error("{engine} databases replicate themselves")]
    SelfReplicating {
        /// The engine.
        engine: EngineKind,
    },
    /// This node serves the database as its primary, or has it open.
    #[error("{database} is open as a primary on this node")]
    Primary {
        /// The database.
        database: DatabaseName,
    },
    /// The snapshot couldn't be fetched, verified, or installed. The old copy, if any, is still
    /// in place.
    #[error("seeding {database} failed: {reason}")]
    Failed {
        /// The database.
        database: DatabaseName,
        /// Why.
        reason: String,
    },
}

/// The error returned when a node can't serve a database as its primary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ServePrimaryError {
    /// The engine replicates itself, so there is nothing to ship.
    #[error("{engine} databases replicate themselves")]
    SelfReplicating {
        /// The engine.
        engine: EngineKind,
    },
    /// This node has seen a newer epoch than the one it was asked to lead at.
    #[error("epoch {epoch} is superseded: this node has seen epoch {highest}")]
    Superseded {
        /// The epoch it was asked to lead at.
        epoch: LeaseEpoch,
        /// The highest epoch it has seen.
        highest: LeaseEpoch,
    },
    /// The node couldn't record that it leads the database.
    #[error("can't record the lease: {reason}")]
    Failed {
        /// Why.
        reason: String,
    },
}

/// How far a primary's followers have got, as [`Replication::primary_status`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PrimaryStatus {
    /// The epoch the primary serves.
    pub epoch: LeaseEpoch,
    /// The end of the last committed batch the primary knows of.
    pub committed: Option<Position>,
    /// Where the primary last checkpointed, if it has since it started serving.
    pub checkpointed: Option<Position>,
    /// The followers it streams to, in ID order.
    pub followers: Vec<FollowerProgress>,
}

/// One follower's progress, as the primary sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FollowerProgress {
    /// The follower.
    pub node: NodeId,
    /// The last position it acknowledged durably, or `None` if it holds an empty log or hasn't
    /// reported yet.
    pub acknowledged: Option<Position>,
    /// Whether it has reported its position since its stream opened.
    pub reported: bool,
    /// Whether its stream is open.
    pub connected: bool,
    /// Whether the log can't catch it up, because the log restarted past it or its copy diverges
    /// from this epoch's log, so it is being re-seeded from a snapshot.
    pub needs_snapshot: bool,
}

/// A follower's copy of a database, as [`Replication::follower_status`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FollowerStatus {
    /// The highest epoch the follower has recorded for the database.
    pub epoch: Option<LeaseEpoch>,
    /// The position its copy is at, once a stream has opened it.
    pub position: Option<Position>,
    /// Where it last checkpointed at a primary's marker.
    pub checkpointed: Option<Position>,
    /// How many snapshots have replaced the copy since this node started.
    pub snapshots: u64,
}

/// One data node's replication: primary streams, follower copies, and the sink and service that
/// connect them to the [`Registry`].
///
/// Cloning is cheap; clones share everything.
#[derive(Debug, Clone)]
pub struct Replication {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    node: NodeId,
    engine: Arc<dyn DatabaseEngine>,
    membership: Membership,
    peers: Arc<Peers>,
    addresses: Arc<dyn PeerAddresses>,
    clock: Arc<dyn Clock>,
    execution: Execution,
    settings: ReplicationSettings,
    epochs: Epochs,
    /// Promotions the control node asked this node to carry out.
    promotions: watch::Sender<BTreeMap<DatabaseName, PromotionRequest>>,
    /// How many streams have ended, by the database they followed. A stream that ends may mean
    /// the database was dropped; the node asks the control plane (see [`ended_streams`]).
    ///
    /// [`ended_streams`]: Replication::ended_streams
    ended: watch::Sender<BTreeMap<DatabaseName, u64>>,
    /// The databases this node serves as primary, each with its epoch, for the change streams
    /// that follow them (FR-039).
    served: watch::Sender<BTreeMap<DatabaseName, LeaseEpoch>>,
    registry: OnceLock<Registry>,
    primaries: Mutex<BTreeMap<DatabaseName, Served>>,
    followers: Mutex<BTreeMap<DatabaseName, Arc<FollowerSlot>>>,
    /// Numbers staging directories, so that two transfers never share one.
    transfers: AtomicU64,
}

/// A database this node is primary for, and the task that drives its streams.
#[derive(Debug)]
struct Served {
    primary: Arc<Primary>,
    driver: JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

impl Replication {
    /// Builds a data node's replication, and its registry through `registry`, which receives the
    /// sink to pass to [`Registry::new`].
    ///
    /// This is the one entry point: the daemon serves [`server`](Self::server), uses
    /// [`registry`](Self::registry) for its databases, calls
    /// [`serve_primary`](Self::serve_primary) for each database it leads, and reports
    /// [`statuses`](DatabaseStatuses::statuses) in heartbeats through
    /// [`Membership::start_reporting`].
    pub fn start(
        parts: ReplicationParts,
        registry: impl FnOnce(ReplicatedSink) -> Registry,
    ) -> Self {
        let ReplicationParts {
            node,
            engine,
            membership,
            peers,
            addresses,
            clock,
            execution,
            state_dir,
            settings,
        } = parts;
        let inner = Arc::new(Inner {
            node,
            engine,
            membership,
            peers,
            addresses,
            clock,
            execution,
            settings,
            epochs: Epochs::new(state_dir.join("epochs")),
            promotions: watch::Sender::new(BTreeMap::new()),
            ended: watch::Sender::new(BTreeMap::new()),
            served: watch::Sender::new(BTreeMap::new()),
            registry: OnceLock::new(),
            primaries: Mutex::default(),
            followers: Mutex::default(),
            transfers: AtomicU64::new(0),
        });
        let built = registry(ReplicatedSink {
            inner: Arc::downgrade(&inner),
        });
        // `inner` is new, so nothing else can have set the registry.
        let _set = inner.registry.set(built);
        snapshot::sweep(inner.registry().root());
        Self { inner }
    }

    /// The registry the node's databases open through.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        self.inner.registry()
    }

    /// The membership view this node places by.
    pub(crate) fn membership_view(&self) -> Arc<crate::MembershipView> {
        self.inner.membership.view()
    }

    /// The membership this node places by, for a watcher of its changes.
    pub(crate) fn membership(&self) -> Membership {
        self.inner.membership.clone()
    }

    /// How replication paces itself.
    #[must_use]
    pub fn settings(&self) -> &ReplicationSettings {
        &self.inner.settings
    }

    /// How many streams following each database have ended since this node started. A watcher
    /// that sees a count grow checks whether the control plane still knows the database, since
    /// a dropped database's streams end and are never reopened.
    pub(crate) fn ended_streams(&self) -> watch::Receiver<BTreeMap<DatabaseName, u64>> {
        self.inner.ended.subscribe()
    }

    /// The `Replication` gRPC server, with FleetRing's message limits, for the node's router.
    #[must_use]
    pub fn server(&self) -> ReplicationServer<ReplicationService> {
        replication_server(self.service())
    }

    /// The `Replication` service itself, for a caller that wraps it before serving it.
    #[must_use]
    pub fn service(&self) -> ReplicationService {
        ReplicationService::new(Arc::clone(&self.inner))
    }

    /// Serves `database` as its primary at `role`'s epoch: opens a stream to each follower in its
    /// replica set, and from then on acknowledges its writes once the write quorum holds them.
    ///
    /// Serving it again replaces the previous role and reopens every stream. A node that holds a
    /// lease calls [`lead`](Self::lead) instead, which records the epoch durably first.
    ///
    /// # Errors
    ///
    /// [`ServePrimaryError::SelfReplicating`] if the engine replicates itself.
    ///
    /// # Panics
    ///
    /// If the replication state's lock was poisoned by a panic in another thread.
    pub fn serve_primary(
        &self,
        database: DatabaseName,
        role: PrimaryRole,
    ) -> Result<(), ServePrimaryError> {
        if let EngineCapability::SelfReplicating = self.inner.engine.capability() {
            return Err(ServePrimaryError::SelfReplicating {
                engine: self.inner.engine.kind(),
            });
        }
        let start = self
            .inner
            .epochs
            .record(&database)
            .start_of(role.epoch.get());
        let primary = Arc::new(Primary::new(database.clone(), role, start));
        let driver = tokio::spawn(primary::drive(
            Arc::clone(&self.inner),
            Arc::clone(&primary),
        ));
        let previous = self
            .inner
            .lock_primaries()
            .insert(database.clone(), Served { primary, driver });
        drop(previous);
        self.inner.served.send_modify(|served| {
            served.insert(database, role.epoch);
        });
        Ok(())
    }

    /// Stops serving `database` as its primary and closes its streams. Its writes are no longer
    /// acknowledged.
    ///
    /// # Panics
    ///
    /// If the replication state's lock was poisoned by a panic in another thread.
    pub fn stop_primary(&self, database: &DatabaseName) {
        let removed = self.inner.lock_primaries().remove(database);
        drop(removed);
        self.inner.served.send_modify(|served| {
            served.remove(database);
        });
    }

    /// Stops every primary's streams.
    ///
    /// # Panics
    ///
    /// If the replication state's lock was poisoned by a panic in another thread.
    pub fn stop(&self) {
        let primaries = std::mem::take(&mut *self.inner.lock_primaries());
        drop(primaries);
        self.inner.served.send_modify(BTreeMap::clear);
    }

    /// The databases this node serves as primary, each with its epoch, as they change. A change
    /// stream starts a pump for each one it is configured for (FR-039).
    pub(crate) fn served(&self) -> watch::Receiver<BTreeMap<DatabaseName, LeaseEpoch>> {
        self.inner.served.subscribe()
    }

    /// A feed of `database`'s acknowledged positions, if this node is its primary.
    #[must_use]
    pub(crate) fn primary_feed(&self, database: &DatabaseName) -> Option<PrimaryFeed> {
        let primary = self.inner.primary(database)?;
        let changes = primary.changes();
        Some(PrimaryFeed {
            inner: Arc::clone(&self.inner),
            primary,
            changes,
        })
    }

    /// Waits until `database`'s primary has enough followers connected, live, and reporting for
    /// a write to be admitted and reach its quorum, or until `limit` passes. Returns whether it
    /// has.
    ///
    /// A primary that has just started serving has no streams open yet, so its first writes
    /// would be refused; waiting here first spares them that.
    pub async fn await_writable(&self, database: &DatabaseName, limit: Duration) -> bool {
        self.await_followers(database, limit, |inner, primary, quorum| {
            quorum.admits(primary.reachable(inner, &quorum.followers))
        })
        .await
    }

    /// Waits until every follower of `database` that the membership view counts as live, and at
    /// least enough for a write to be admitted and reach its quorum, has its stream connected
    /// and reporting, or until `limit` passes. Returns whether they have.
    ///
    /// Once they have, a write isn't under-replicated unless a follower goes down or its stream
    /// drops. A follower that is down isn't waited for, unless the quorum or the floor needs it.
    pub async fn await_replicas(&self, database: &DatabaseName, limit: Duration) -> bool {
        self.await_followers(database, limit, |inner, primary, quorum| {
            let live = quorum
                .followers
                .iter()
                .filter(|node| inner.is_live(node))
                .count();
            let reachable = primary.reachable(inner, &quorum.followers);
            quorum.admits(reachable) && reachable >= live
        })
        .await
    }

    /// Waits until `follower` has acknowledged everything `database`'s primary has committed, or
    /// until `limit` passes. Returns whether it has, and `false` if this node isn't the primary.
    ///
    /// A node that hands a lease over waits for this first, so the new primary starts with the
    /// whole log and the promotion has nothing to fetch.
    pub async fn await_caught_up(
        &self,
        database: &DatabaseName,
        follower: &NodeId,
        limit: Duration,
    ) -> bool {
        self.await_followers(database, limit, |_, primary, quorum| {
            let status = primary.status(quorum);
            status.followers.iter().any(|progress| {
                progress.node == *follower
                    && progress.reported
                    && progress.acknowledged == status.committed
            })
        })
        .await
    }

    /// Whether `database`'s primary has enough followers connected, live, and reporting for a
    /// write to be admitted and reach its quorum now.
    pub(crate) fn is_writable(&self, database: &DatabaseName) -> bool {
        self.inner.primary(database).is_some_and(|primary| {
            let quorum = self.inner.followers_of(&primary);
            quorum.admits(primary.reachable(&self.inner, &quorum.followers))
        })
    }

    /// Waits until `ready` holds for `database`'s primary and its replica set, or until `limit`
    /// passes. Returns whether it does, and `false` if this node isn't the primary.
    async fn await_followers(
        &self,
        database: &DatabaseName,
        limit: Duration,
        ready: impl Fn(&Inner, &Primary, &Quorum) -> bool,
    ) -> bool {
        let Some(primary) = self.inner.primary(database) else {
            return false;
        };
        let poll = self.inner.settings.retry_interval;
        let holds = || ready(&self.inner, &primary, &self.inner.followers_of(&primary));
        tokio::time::timeout(limit, async {
            while !holds() {
                tokio::time::sleep(poll).await;
            }
        })
        .await
        .is_ok()
    }

    /// How many of `database`'s replicas hold everything its primary has committed, if this
    /// node is its primary.
    #[must_use]
    pub fn holders(&self, database: &DatabaseName) -> Option<Acknowledged> {
        let primary = self.inner.primary(database)?;
        let quorum = self.inner.followers_of(&primary);
        let status = primary.status(&quorum);
        let own = quorum.own;
        let caught_up = status
            .followers
            .iter()
            .filter(|follower| follower.reported && follower.acknowledged == status.committed)
            .count();
        let reachable = primary.reachable(&self.inner, &quorum.followers);
        let count = |replicas: usize| u32::try_from(replicas).unwrap_or(u32::MAX);
        Some(Acknowledged::new(
            count(own + caught_up),
            count(own + reachable),
            u32::from(primary.role().replication_factor.get()),
        ))
    }

    /// How far `database`'s followers have got, if this node is its primary.
    #[must_use]
    pub fn primary_status(&self, database: &DatabaseName) -> Option<PrimaryStatus> {
        self.inner
            .primary(database)
            .map(|primary| primary.status(&self.inner.followers_of(&primary)))
    }

    /// Where this node's copy of `database` is, if it follows it.
    #[must_use]
    pub fn follower_status(&self, database: &DatabaseName) -> Option<FollowerStatus> {
        let slot = self.inner.slot_if_any(database);
        let epoch = self.inner.epochs.highest(database);
        if slot.is_none() && epoch.is_none() {
            return None;
        }
        let snapshots = slot.as_ref().map_or(0, |slot| slot.snapshots());
        let (position, checkpointed) = slot.map(|slot| slot.positions()).unwrap_or_default();
        Some(FollowerStatus {
            epoch: epoch.map(LeaseEpoch::new),
            position,
            checkpointed,
            snapshots,
        })
    }

    /// Seeds this node's copy of `database` with a snapshot from `from`, its primary at `epoch`,
    /// replacing any copy here, and returns the position the copy is at.
    ///
    /// This is how a node newly added to a database's replica set gets its copy ahead of joining
    /// it. The primary's stream would also seed it on first contact, through the same transfer.
    /// Any stream applying to the copy here ends first; the primary reopens it from the new
    /// position.
    ///
    /// # Errors
    ///
    /// [`SeedError::SelfReplicating`] if the engine replicates itself, [`SeedError::Primary`] if
    /// this node has the database open, and [`SeedError::Failed`] if the epoch is stale or the
    /// transfer fails.
    pub async fn seed(
        &self,
        database: &DatabaseName,
        from: &NodeId,
        epoch: LeaseEpoch,
    ) -> Result<Option<Position>, SeedError> {
        snapshot::seed(&self.inner, database, from, epoch.get()).await
    }

    /// Checkpoints `database` now, if every follower holds everything, and ships the marker.
    ///
    /// # Errors
    ///
    /// [`ReplicationError::Failed`] if this node isn't its primary, a follower is behind, another
    /// checkpoint is running, or the checkpoint fails.
    pub async fn checkpoint(
        &self,
        database: &DatabaseName,
    ) -> Result<CheckpointMarker, ReplicationError> {
        let primary = self
            .inner
            .primary(database)
            .ok_or_else(|| failed(format!("{database} has no primary on this node")))?;
        let followers = self.inner.followers_of(&primary).followers;
        primary.begin_checkpoint(&followers)?;
        primary::checkpoint(&self.inner, &primary, Vec::new()).await
    }
}

impl DatabaseStatuses for Replication {
    fn statuses(&self) -> Vec<DatabaseStatus> {
        let mut statuses: BTreeMap<DatabaseName, DatabaseStatus> = BTreeMap::new();
        for (name, slot) in self.inner.lock_followers().iter() {
            let (Some(position), _) = slot.positions() else {
                continue;
            };
            statuses.insert(
                name.clone(),
                DatabaseStatus {
                    database: name.to_string(),
                    role: Role::Follower.into(),
                    epoch: self.inner.epochs.record(name).log_epoch(),
                    position: position.into_bytes(),
                },
            );
        }
        for (name, served) in self.inner.lock_primaries().iter() {
            let primary = &served.primary;
            statuses.insert(
                name.clone(),
                DatabaseStatus {
                    database: name.to_string(),
                    role: Role::Primary.into(),
                    epoch: primary.role().epoch.get(),
                    position: primary
                        .committed()
                        .map(Position::into_bytes)
                        .unwrap_or_default(),
                },
            );
        }
        statuses.into_values().collect()
    }
}

/// What a change stream's pump reads from a database's primary: how far the write quorum has
/// got, so that the pump never hands out a batch the quorum doesn't hold (FR-039).
///
/// The feed follows the [`Primary`] that served the database when it was taken. A primary that
/// is replaced, by a new epoch, or stopped, ends the feed: [`changed`](Self::changed) fails,
/// and the pump takes a fresh feed.
#[derive(Debug)]
pub(crate) struct PrimaryFeed {
    inner: Arc<Inner>,
    primary: Arc<Primary>,
    changes: watch::Receiver<u64>,
}

impl PrimaryFeed {
    /// The epoch the primary serves.
    pub(crate) fn epoch(&self) -> LeaseEpoch {
        self.primary.role().epoch
    }

    /// The highest position the write quorum holds, or `None` if it holds no batch yet.
    pub(crate) fn acknowledged(&self) -> Option<Position> {
        let quorum = self.inner.followers_of(&self.primary);
        self.primary.quorum_held(&quorum)
    }

    /// How many batches the primary has published since it started serving.
    pub(crate) fn published(&self) -> u64 {
        self.primary.published()
    }

    /// Records that `sink` has delivered up to `position`, so that the primary holds
    /// checkpoints back while the sink is behind, as it does for a follower.
    pub(crate) fn delivered(&self, sink: &str, position: Option<Position>) {
        self.primary
            .sink_delivered(sink, position, self.inner.clock.now());
    }

    /// Whether the primary this feed follows still serves the database.
    pub(crate) fn is_current(&self) -> bool {
        self.inner
            .primary(self.primary.name())
            .is_some_and(|current| Arc::ptr_eq(&current, &self.primary))
    }

    /// Waits until something changed: a batch was published or acknowledged, or a follower came
    /// or went. Fails once the primary is replaced or stopped.
    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.changes.changed().await
    }
}

/// The [`ReplicationSink`] that [`Replication::start`] hands the registry: it refuses a write
/// while fewer than the database's `min_replicas` are reachable, gives each committed batch to
/// the database's primary streams, and acknowledges it once the write quorum holds it.
#[derive(Debug, Clone)]
pub struct ReplicatedSink {
    inner: Weak<Inner>,
}

impl ReplicationSink for ReplicatedSink {
    /// Refuses the write if this node is the database's primary and fewer than its
    /// `min_replicas` replicas, this node included, are connected and live. A database this node
    /// doesn't serve is admitted here and refused when its batch is published, as before.
    fn admit(&self, database: &DatabaseName) -> Result<(), ReplicationError> {
        let Some(inner) = self.inner.upgrade() else {
            return Err(failed("replication has stopped".to_owned()));
        };
        let Some(primary) = inner.primary(database) else {
            return Ok(());
        };
        let quorum = inner.followers_of(&primary);
        let reachable = primary.reachable(&inner, &quorum.followers);
        if quorum.admits(reachable) {
            Ok(())
        } else {
            Err(ReplicationError::BelowFloor {
                min_replicas: quorum.floor,
                reachable: reachable + quorum.own,
            })
        }
    }

    fn publish(&self, batch: CommittedBatch) -> Acknowledgement {
        let Some(inner) = self.inner.upgrade() else {
            return refused(failed("replication has stopped".to_owned()));
        };
        let Some(primary) = inner.primary(&batch.database) else {
            return refused(failed(format!(
                "{} has no primary on this node",
                batch.database
            )));
        };
        primary::publish(&inner, &primary, batch.batch)
    }
}

/// The replica set's followers as this node sees them, how many must acknowledge a write, and
/// how many must be reachable for one to run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Quorum {
    /// Every member of the replica set but this node.
    followers: Vec<NodeId>,
    /// W, the replicas a write needs, the primary included.
    required: usize,
    /// How many of `followers` must acknowledge a write.
    needed: usize,
    /// How many replicas this node counts for: 1 when it is in the replica set, else 0.
    own: usize,
    /// The database's `min_replicas`: how many replicas, this node included, must be reachable
    /// for a write to be admitted (ADR-0023).
    floor: usize,
}

impl Quorum {
    /// Whether a write may run with `reachable` followers connected and live.
    fn admits(&self, reachable: usize) -> bool {
        reachable + self.own >= self.floor
    }
}

impl Inner {
    fn registry(&self) -> &Registry {
        self.registry
            .get()
            .expect("the registry is set before `Replication::start` returns")
    }

    fn lock_primaries(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, Served>> {
        self.primaries.lock().expect("replication lock poisoned")
    }

    fn lock_followers(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, Arc<FollowerSlot>>> {
        self.followers.lock().expect("replication lock poisoned")
    }

    fn primary(&self, database: &DatabaseName) -> Option<Arc<Primary>> {
        self.lock_primaries()
            .get(database)
            .map(|served| Arc::clone(&served.primary))
    }

    fn slot(&self, database: &DatabaseName) -> Arc<FollowerSlot> {
        Arc::clone(self.lock_followers().entry(database.clone()).or_default())
    }

    fn slot_if_any(&self, database: &DatabaseName) -> Option<Arc<FollowerSlot>> {
        self.lock_followers().get(database).cloned()
    }

    /// Counts one more ended stream of `database`.
    fn stream_ended(&self, database: &DatabaseName) {
        self.ended.send_modify(|ended| {
            *ended.entry(database.clone()).or_default() += 1;
        });
    }

    /// The replica set of `primary`'s database in the current membership view.
    fn followers_of(&self, primary: &Primary) -> Quorum {
        let role = primary.role();
        let factor = role.replication_factor;
        let view = self.membership.view();
        let members: Vec<NodeId> = view.ring().map_or_else(Vec::new, |ring| {
            ring.replicas_n(primary.name().as_str(), factor).to_ids()
        });
        let includes_self = members.is_empty() || members.contains(&self.node);
        let size = u8::try_from(members.len())
            .ok()
            .and_then(ReplicationFactor::new)
            .unwrap_or(factor);
        let required = usize::from(role.durability.quorums(size).write());
        let followers: Vec<NodeId> = members.into_iter().filter(|id| *id != self.node).collect();
        // Without a ring, the node can't know its followers, so a write needs them all.
        let own = usize::from(includes_self);
        let needed = required - own;
        Quorum {
            followers,
            required,
            needed,
            own,
            floor: usize::from(role.durability.min_replicas()),
        }
    }

    /// Whether the membership view counts `node` as live.
    fn is_live(&self, node: &NodeId) -> bool {
        self.membership.view().liveness(node) == Some(fleetring_control::Liveness::Live)
    }

    /// A fresh staging directory name under the registry's root. Database directories never
    /// contain `.`, so these never collide with one.
    fn staging(&self, kind: &str) -> PathBuf {
        let n = self.transfers.fetch_add(1, Ordering::Relaxed);
        self.registry().root().join(format!(".{kind}-{n}"))
    }
}

fn failed(reason: String) -> ReplicationError {
    ReplicationError::Failed { reason }
}

fn refused(error: ReplicationError) -> Acknowledgement {
    Acknowledgement::new(std::future::ready(Err(error)))
}

const _: () = {
    const fn assert_debug_send_sync<T: fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<Fenced>();
    assert_debug_send_sync::<FollowerProgress>();
    assert_debug_send_sync::<PromotionRequest>();
    assert_debug_send_sync::<FollowerStatus>();
    assert_debug_send_sync::<PrimaryFeed>();
    assert_debug_send_sync::<PrimaryRole>();
    assert_debug_send_sync::<PrimaryStatus>();
    assert_debug_send_sync::<ReplicatedSink>();
    assert_debug_send_sync::<Replication>();
    assert_debug_send_sync::<ReplicationParts>();
    assert_debug_send_sync::<ReplicationService>();
    assert_debug_send_sync::<ReplicationSettings>();
    assert_debug_send_sync::<SeedError>();
    assert_debug_send_sync::<ServePrimaryError>();
};
