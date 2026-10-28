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
//!    it into place (see [Initial copies](#initial-copies)). It then answers with a first
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
//! # Write quorum
//!
//! A write's [`Acknowledgement`] completes once W − 1 followers have
//! acknowledged a position at or past the write's batch, where W comes from
//! [`Quorums`] over the replica set's size R. If fewer followers
//! than that are connected and live when the batch is published, the acknowledgement fails at
//! once with [`ReplicationError::UnderReplicated`]. One that doesn't complete within
//! [`ReplicationSettings::ack_timeout`] fails too. Either way, the write has committed on the
//! primary and may still replicate later; it just isn't acknowledged.
//!
//! # Checkpoints
//!
//! Once the batches published since the last checkpoint pass
//! [`ReplicationSettings::checkpoint_threshold`] bytes, and every follower has acknowledged
//! everything, the primary checkpoints (RESTART, ADR-0016 point 7). A follower that has been
//! behind for longer than [`ReplicationSettings::lag_limit`] stops blocking checkpoints. Once the
//! log restarts past it, it can no longer catch up from the log, and is flagged as needing a
//! snapshot ([`FollowerProgress::needs_snapshot`]); snapshot catch-up is FR-032.
//!
//! # Initial copies
//!
//! A follower never starts from an empty file. The first time a database's stream reaches a
//! follower without a copy, the follower calls `Snapshot` on the primary, which streams a
//! [`DatabaseHandle::snapshot`](crate::DatabaseHandle::snapshot) of the database as one archive
//! of the snapshot directory's files. The follower unpacks it into a staging directory, syncs it,
//! and renames it into place. This is the minimal transfer FR-032 replaces with a checksummed,
//! resumable one built on SQLite's online backup.

mod archive;
mod epochs;
mod follower;
mod primary;

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, LeaseEpoch, Quorums};
use fleetring_proto::v1::replication_server::ReplicationServer;
use fleetring_proto::v1::{DatabaseStatus, Role};
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
    /// Whether the log restarted past it, so it can only catch up from a snapshot (FR-032).
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
        Self { inner }
    }

    /// The registry the node's databases open through.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        self.inner.registry()
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
    /// Serving it again replaces the previous role and reopens every stream.
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
        let primary = Arc::new(Primary::new(database.clone(), role));
        let driver = tokio::spawn(primary::drive(
            Arc::clone(&self.inner),
            Arc::clone(&primary),
        ));
        let previous = self
            .inner
            .lock_primaries()
            .insert(database, Served { primary, driver });
        drop(previous);
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
    }

    /// Stops every primary's streams.
    ///
    /// # Panics
    ///
    /// If the replication state's lock was poisoned by a panic in another thread.
    pub fn stop(&self) {
        let primaries = std::mem::take(&mut *self.inner.lock_primaries());
        drop(primaries);
    }

    /// Waits until `database`'s primary has enough followers connected, live, and reporting for
    /// a write to reach its quorum, or until `limit` passes. Returns whether it has.
    ///
    /// A primary that has just started serving has no streams open yet, so its first writes
    /// would fail as under-replicated; waiting here first spares them that.
    pub async fn await_writable(&self, database: &DatabaseName, limit: Duration) -> bool {
        let Some(primary) = self.inner.primary(database) else {
            return false;
        };
        let poll = self.inner.settings.retry_interval;
        let writable = || {
            let quorum = self.inner.followers_of(&primary);
            primary.reachable(&self.inner, &quorum.followers) >= quorum.needed
        };
        tokio::time::timeout(limit, async {
            while !writable() {
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
        let own = quorum.required - quorum.needed;
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
        let (position, checkpointed) = slot.map(|slot| slot.positions()).unwrap_or_default();
        Some(FollowerStatus {
            epoch: epoch.map(LeaseEpoch::new),
            position,
            checkpointed,
        })
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
                    epoch: self.inner.epochs.highest(name).unwrap_or(0),
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

/// The [`ReplicationSink`] that [`Replication::start`] hands the registry: it gives each
/// committed batch to the database's primary streams and acknowledges it once the write quorum
/// holds it.
#[derive(Debug, Clone)]
pub struct ReplicatedSink {
    inner: Weak<Inner>,
}

impl ReplicationSink for ReplicatedSink {
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

/// The replica set's followers as this node sees them, and how many must acknowledge a write.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Quorum {
    /// Every member of the replica set but this node.
    followers: Vec<NodeId>,
    /// W, the replicas a write needs, the primary included.
    required: usize,
    /// How many of `followers` must acknowledge a write.
    needed: usize,
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

    /// The replica set of `primary`'s database in the current membership view.
    fn followers_of(&self, primary: &Primary) -> Quorum {
        let factor = primary.role().replication_factor;
        let view = self.membership.view();
        let members: Vec<NodeId> = view.ring().map_or_else(Vec::new, |ring| {
            ring.replicas_n(primary.name().as_str(), factor).to_ids()
        });
        let includes_self = members.is_empty() || members.contains(&self.node);
        let size = u8::try_from(members.len())
            .ok()
            .and_then(ReplicationFactor::new)
            .unwrap_or(factor);
        let required = usize::from(Quorums::new(size).write());
        let followers: Vec<NodeId> = members.into_iter().filter(|id| *id != self.node).collect();
        // Without a ring, the node can't know its followers, so a write needs them all.
        let needed = if includes_self {
            required.saturating_sub(1)
        } else {
            required
        };
        Quorum {
            followers,
            required,
            needed,
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
    assert_debug_send_sync::<FollowerProgress>();
    assert_debug_send_sync::<FollowerStatus>();
    assert_debug_send_sync::<PrimaryRole>();
    assert_debug_send_sync::<PrimaryStatus>();
    assert_debug_send_sync::<ReplicatedSink>();
    assert_debug_send_sync::<Replication>();
    assert_debug_send_sync::<ReplicationParts>();
    assert_debug_send_sync::<ReplicationService>();
    assert_debug_send_sync::<ReplicationSettings>();
    assert_debug_send_sync::<ServePrimaryError>();
};
