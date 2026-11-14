//! A data node's side of forks (ADR-0046, points 4, 6, and 7): the staged copies it holds,
//! the seeds it fetches, the pins it serves as a source's primary, the disk it reserves, what
//! it reports about each fork in its heartbeats, and the readiness gate on a fork it opens.
//!
//! - **Seeding.** The attempt's seeder fetches the source from the source's primary, which
//!   pins it at the position its write quorum acknowledged; the fork's other replicas fetch the
//!   seeder's staged copy. Both run under the `seeding` binding, which compares the whole fork
//!   identity, the attempt, the source, and the source incarnation ([`binding`]).
//! - **Staging** is durable and atomic, in a directory named by the fork identity and the
//!   attempt ([`staged`]). Nothing reads a staged copy but a peer copying it and the node itself
//!   opening it, after checking it again against what control state recorded.
//! - **Admission.** A copy is admitted only with room for it and a tenth more beside the larger
//!   of 1 GiB and 1% of the volume, less what other admitted copies reserved; a node refusing
//!   reports it, and the control plane passes it over. Pins and downloads are bounded per node
//!   and per database.
//! - **Deletion** only on a positive signal for a staged copy's own identity (ADR-0042): a drop
//!   notice naming it, the identity seen failed, ready, or seeded without this node, or a later
//!   attempt. A staged copy whose identity this node can't confirm is an orphan.
//! - **Readiness.** A fork's primary serves nothing until its fork-reset write is committed at
//!   the bootstrap quorum: data calls and `WatchChanges` get `UNAVAILABLE` with a retry-after,
//!   and change streams, archives included, don't start ([`Forks::is_gated`]).
//!
//! [`binding`]: super::binding

pub(crate) mod faults;
pub(crate) mod seed;
pub(super) mod source;
pub(crate) mod staged;
pub(crate) mod view;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fleetring_control::fork::{
    CopyLossReason, CopyLost, ForkIdentity, PlacementCaughtUp, ResetReport, SeedFailure, StagedCopy,
};
use fleetring_control::{DatabaseName, HostedReplica, Incarnation, LeaseEpoch, ReplicaRole};
use fleetring_proto::v1::DatabaseStatus;
use tokio::task::JoinHandle;
use tonic::Status;
use tonic::metadata::MetadataValue;

use self::faults::ForkFaults;
use self::staged::{Held, Staged, StagedKey, StagedStore};
use crate::ForkConfig;
use crate::control::SeedProgress;
use crate::control::fork::{replica_to_proto, role_from_status};
use crate::engine::Position;
use crate::replication::ChainHash;

pub use self::faults::FaultPoint;
pub use self::view::{DatabaseView, ForkPhase, ForkView};

/// The metadata key that says, in milliseconds, how long a caller should wait before it retries
/// a call refused with `UNAVAILABLE` or `RESOURCE_EXHAUSTED` (ADR-0046, points 4.2, 6.2, and 7).
pub const RETRY_AFTER_KEY: &str = "fleetring-retry-after-ms";

/// What every refusal of a data call against a fork that isn't ready says, whichever node and
/// whichever path refuses it (ADR-0046, point 6.2).
pub const FORK_NOT_READY: &str = "fork not ready";

/// How long a client should wait before it retries a data call to a fork that isn't ready.
const FORK_NOT_READY_RETRY: Duration = Duration::from_millis(500);

/// The longest a data node's fork work waits for one answer from the control plane: the
/// `Locate` of a fork or its source, or a seeded fork's first lease (ADR-0046, point 12).
///
/// A call on a connection that stalled, behind link loss or a partition, would otherwise wait
/// for ever, and the node's fork work with it. A `Locate` is a read and the first lease one
/// logged command; the control client rides out a leader change within a `Locate`'s own
/// retries, about 1.6 s of backoff, which 5 s leaves room for. It is shorter than the 10 s
/// request timeout that bounds one try over TCP, so the node gives up on a stalled connection
/// first, and a twenty-fourth of `[fork] seeder_timeout` (2 min), so a seeder that hit one is
/// back long before the control plane passes it over.
pub(crate) const CONTROL_CALL_LIMIT: Duration = Duration::from_secs(5);

/// `UNAVAILABLE` "fork not ready", with a retry-after, for a data call against `name`, a fork
/// that isn't ready, `why` saying where it stands.
pub(crate) fn fork_not_ready(name: &DatabaseName, why: &str) -> Status {
    with_retry_after(
        Status::unavailable(format!("{FORK_NOT_READY}: database {name} {why}; retry")),
        FORK_NOT_READY_RETRY,
    )
}

/// The refusal of a data call against `name` that `placement` calls for before any node serves
/// it: `UNAVAILABLE` "fork not ready" for a fork seeding, or seeded with no primary yet, and
/// `FAILED_PRECONDITION` for a failed fork, which no node ever serves. `None` for anything else:
/// a database that isn't a fork, a ready fork, and a seeded fork with a primary, which decides
/// by its own readiness gate.
pub(crate) fn placement_refusal(
    name: &DatabaseName,
    placement: &fleetring_proto::v1::Placement,
) -> Option<Status> {
    use fleetring_proto::v1::ForkState;
    let fork = placement.fork.as_ref()?;
    match ForkState::try_from(fork.state) {
        Ok(ForkState::Seeding) => Some(fork_not_ready(
            name,
            &format!("is a fork still being seeded from {}", fork.source),
        )),
        Ok(ForkState::Seeded) if placement.primary.is_none() => Some(fork_not_ready(
            name,
            "is a fork that is seeded and waiting for its first primary",
        )),
        Ok(ForkState::Failed) => Some(Status::failed_precondition(format!(
            "database {name} is a fork that failed and is never served; `fleetring db drop \
             {name}` removes it"
        ))),
        _ => None,
    }
}

/// `status` with a retry-after of `after`.
pub(crate) fn with_retry_after(mut status: Status, after: Duration) -> Status {
    let millis = u64::try_from(after.as_millis()).unwrap_or(u64::MAX);
    if let Ok(value) = MetadataValue::try_from(millis.to_string()) {
        status.metadata_mut().insert(RETRY_AFTER_KEY, value);
    }
    status
}

/// The retry-after `status` carries, if it carries one.
pub(crate) fn retry_after(status: &Status) -> Option<Duration> {
    status
        .metadata()
        .get(RETRY_AFTER_KEY)?
        .to_str()
        .ok()?
        .parse()
        .ok()
        .map(Duration::from_millis)
}

/// `bytes` as lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a `String` can't fail.
        let _ = write!(text, "{byte:02x}");
    }
    text
}

/// The bytes lowercase hex `text` spells, if it spells any.
pub(crate) fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(text.get(at..at + 2)?, 16).ok())
        .collect()
}

/// The free space and the size of a data volume, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DiskSpace {
    pub(crate) free: u64,
    pub(crate) volume: u64,
}

impl DiskSpace {
    /// The space of the volume `path` is on, if the platform says.
    fn of(path: &Path) -> Option<Self> {
        #[cfg(unix)]
        {
            let mut existing = path;
            while !existing.exists() {
                existing = existing.parent()?;
            }
            let stats = rustix::fs::statvfs(existing).ok()?;
            Some(Self {
                free: stats.f_bavail.saturating_mul(stats.f_frsize),
                volume: stats.f_blocks.saturating_mul(stats.f_frsize),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            None
        }
    }
}

/// What a copy of `total_bytes` needs free on a volume of `volume` bytes, beyond what other
/// copies reserved (ADR-0046, point 7): the copy and a tenth more, beside the larger of
/// `[fork] admission_min_free_bytes` and `admission_min_free_percent` of the volume (1 GiB and
/// 1% by default).
pub(crate) fn admission_need(total_bytes: u64, volume: u64, config: &ForkConfig) -> u128 {
    let share = u128::from(volume) * u128::from(config.admission_min_free_percent) / 100;
    reservation(total_bytes) + share.max(u128::from(config.admission_min_free_bytes))
}

/// What admitting a copy of `total_bytes` reserves until it is staged: the copy and a tenth.
pub(crate) fn reservation(total_bytes: u64) -> u128 {
    u128::from(total_bytes) + u128::from(total_bytes / 10)
}

/// How a fork's replica set is frozen on this node, by name, and the fork identities whose
/// holders this node saw released.
#[derive(Debug, Default)]
struct Frozen {
    sets: BTreeMap<DatabaseName, (ForkIdentity, FrozenSet)>,
    released: BTreeMap<DatabaseName, ForkIdentity>,
}

/// A fork's recorded holders, and what a primary counts toward a write while they are its
/// replica set (ADR-0046, point 3.10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FrozenSet {
    /// Seeded: the holders alone are the replica set and the write quorum.
    Seeded(Vec<fleetring::NodeId>),
    /// Ready, holders kept: they are still the replica set a promotion fences, and a write
    /// needs both their write quorum and placement's until control state releases them.
    Handover(Vec<fleetring::NodeId>),
}

impl FrozenSet {
    /// The recorded holders.
    pub(crate) fn holders(&self) -> &[fleetring::NodeId] {
        match self {
            Self::Seeded(holders) | Self::Handover(holders) => holders,
        }
    }
}

/// What a node says about one fork name in its heartbeats, beside its copy.
#[derive(Debug, Clone, Default)]
struct NameReports {
    seed_failure: Option<SeedFailure>,
    copy_lost: Option<CopyLost>,
    reset: Option<ResetReport>,
    placement_caught_up: Option<PlacementCaughtUp>,
    progress: Option<SeedProgress>,
    /// The fork identity the node last saw in control state for the name.
    current: Option<ForkIdentity>,
}

/// The lease a fork's primary on this node opens its readiness gate under: the database's
/// incarnation and the lease's epoch. An epoch alone doesn't name a lease: a database created
/// again under the name, a fork made again after a drop among them, starts its epochs over
/// (ADR-0046, point 1), so its first lease has the epoch the dropped database's first lease had.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GateLease {
    /// The incarnation of the database the lease is on, if the lease named one.
    pub(crate) incarnation: Option<Incarnation>,
    /// The lease's epoch.
    pub(crate) epoch: LeaseEpoch,
}

/// Where opening a fork's primary stands on this node, under one lease.
#[derive(Debug)]
enum Opening {
    /// The gate is open under the lease.
    Open(GateLease),
    /// A task is opening it under the lease.
    Running(GateLease, JoinHandle<()>),
}

impl Opening {
    /// The lease the gate opened, or is opening, under.
    fn lease(&self) -> GateLease {
        match self {
            Self::Open(lease) | Self::Running(lease, _) => *lease,
        }
    }

    /// Stops the task opening the gate, if one runs.
    fn abort(&self) {
        if let Self::Running(_, handle) = self {
            handle.abort();
        }
    }
}

/// The reservation ledger and the downloads under way (ADR-0046, point 7.1). It isn't
/// persisted: a restart removes the temporary files the reservations stood for.
#[derive(Debug, Default)]
struct Ledger {
    reserved: BTreeMap<StagedKey, u128>,
    /// The copies being fetched, by name.
    seeds: BTreeMap<StagedKey, JoinHandle<()>>,
}

/// Why a copy wasn't admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refused {
    /// The volume hasn't room for it.
    Disk,
}

/// A copy's admitted reservation, released when dropped: once the copy is staged, its bytes are
/// on disk, and if it is abandoned, it needs nothing.
#[derive(Debug)]
pub(crate) struct Reservation {
    forks: Arc<Forks>,
    key: StagedKey,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.forks.lock_ledger().reserved.remove(&self.key);
        self.forks.publish_gauges();
    }
}

/// A data node's fork state; see the [module documentation](self).
#[derive(Debug)]
pub(crate) struct Forks {
    store: StagedStore,
    config: Mutex<ForkConfig>,
    faults: Mutex<ForkFaults>,
    ledger: Mutex<Ledger>,
    reports: Mutex<BTreeMap<DatabaseName, NameReports>>,
    pins: source::PinLedger,
    /// The forks whose primary on this node serves no client yet (ADR-0046, point 6.2).
    gated: Mutex<BTreeSet<DatabaseName>>,
    /// The epoch each fork's gate was last opened at on this node, and the task opening one.
    opening: Mutex<BTreeMap<DatabaseName, Opening>>,
    /// The replica set of each fork this node last saw seeded, or ready with its holders kept,
    /// as control state froze it: its recorded holders, whatever the ring says (ADR-0046, point
    /// 3.10). A promotion fences only them. While the fork is seeded, a primary here streams to
    /// them alone and counts only them toward a write quorum; once it is ready, the primary
    /// streams to them and to placement and counts both write quorums, until control state
    /// releases them. A fork this node saw released never freezes again.
    frozen: Mutex<Frozen>,
    /// The free space and volume the test hooks last said, with the data directory's size
    /// then, which what it grows by since comes off the free space.
    disk_baseline: Mutex<Option<((u64, u64), u64)>>,
    metrics: crate::observe::ReplicationMetrics,
}

impl Forks {
    /// The fork state of a node whose data directory holds `state_dir`, its replication
    /// state: staged copies live beside it, under `staged/`.
    pub(crate) fn new(state_dir: &Path, metrics: crate::observe::ReplicationMetrics) -> Self {
        let root = state_dir
            .parent()
            .map_or_else(|| state_dir.join("staged"), |dir| dir.join("staged"));
        Self {
            store: StagedStore::new(root),
            config: Mutex::new(ForkConfig::default()),
            faults: Mutex::new(ForkFaults::default()),
            ledger: Mutex::default(),
            reports: Mutex::default(),
            pins: source::PinLedger::default(),
            gated: Mutex::default(),
            opening: Mutex::default(),
            frozen: Mutex::default(),
            disk_baseline: Mutex::default(),
            metrics,
        }
    }

    /// The staged copies on disk.
    pub(crate) fn store(&self) -> &StagedStore {
        &self.store
    }

    /// The `[fork]` limits the node runs with.
    pub(crate) fn config(&self) -> ForkConfig {
        *self.config.lock().expect("fork config lock poisoned")
    }

    /// The fault hooks the node's fork work passes.
    pub(crate) fn faults(&self) -> ForkFaults {
        self.faults
            .lock()
            .expect("fork faults lock poisoned")
            .clone()
    }

    /// Runs with `config` and `faults` from now on.
    pub(crate) fn configure(&self, config: ForkConfig, faults: ForkFaults) {
        *self.config.lock().expect("fork config lock poisoned") = config;
        *self.faults.lock().expect("fork faults lock poisoned") = faults;
    }

    pub(super) fn pins(&self) -> &source::PinLedger {
        &self.pins
    }

    fn lock_ledger(&self) -> MutexGuard<'_, Ledger> {
        self.ledger.lock().expect("fork ledger lock poisoned")
    }

    fn lock_reports(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, NameReports>> {
        self.reports.lock().expect("fork reports lock poisoned")
    }

    fn lock_gated(&self) -> MutexGuard<'_, BTreeSet<DatabaseName>> {
        self.gated.lock().expect("fork gate lock poisoned")
    }

    /// The node's data volume as a fork's admission sees it: as the test hooks say, if they say,
    /// less what the data directory has grown by since they said it; else as the volume says.
    pub(crate) fn disk_space(&self) -> Option<DiskSpace> {
        if let Some(setting) = self.faults().disk() {
            let data_dir = self.store.root().parent().unwrap_or(self.store.root());
            let used = super::failover::size_on_disk(data_dir);
            let mut baseline = self
                .disk_baseline
                .lock()
                .expect("disk baseline lock poisoned");
            let base = match *baseline {
                Some((said, base)) if said == setting => base,
                _ => {
                    *baseline = Some((setting, used));
                    used
                }
            };
            return Some(DiskSpace {
                free: setting.0.saturating_sub(used.saturating_sub(base)),
                volume: setting.1,
            });
        }
        DiskSpace::of(self.store.root())
    }

    /// The free bytes on the node's data volume, for its heartbeats.
    pub(crate) fn disk_free_bytes(&self) -> Option<u64> {
        self.disk_space().map(|space| space.free)
    }

    /// Admits a copy of `total_bytes` for `key`, and reserves its room until the returned
    /// reservation is dropped. Two admissions never count the same free bytes: each compares
    /// the free space less every outstanding reservation. A volume whose space is unknown admits.
    pub(crate) fn admit(
        self: &Arc<Self>,
        key: &StagedKey,
        total_bytes: u64,
    ) -> Result<Reservation, Refused> {
        let space = self.disk_space();
        let config = self.config();
        {
            let mut ledger = self.lock_ledger();
            if let Some(space) = space {
                let reserved: u128 =
                    if fleetring_control::mutation::Mutation::DiskWithoutReservations.is_active() {
                        0
                    } else {
                        ledger
                            .reserved
                            .iter()
                            .filter(|(other, _)| *other != key)
                            .map(|(_, bytes)| *bytes)
                            .sum()
                    };
                let available = u128::from(space.free).saturating_sub(reserved);
                if available < admission_need(total_bytes, space.volume, &config) {
                    return Err(Refused::Disk);
                }
            }
            ledger
                .reserved
                .insert(key.clone(), reservation(total_bytes));
        }
        self.publish_gauges();
        Ok(Reservation {
            forks: Arc::clone(self),
            key: key.clone(),
        })
    }

    /// Updates the gauges of reserved disk.
    fn publish_gauges(&self) {
        let reserved: u128 = self.lock_ledger().reserved.values().sum();
        self.metrics
            .fork_reserved_bytes(u64::try_from(reserved).unwrap_or(u64::MAX));
    }

    /// Whether a fetch of `key` is under way.
    pub(crate) fn is_seeding(&self, key: &StagedKey) -> bool {
        self.lock_ledger()
            .seeds
            .get(key)
            .is_some_and(|task| !task.is_finished())
    }

    /// Starts `task` as the fetch of `key`, unless one is under way or the node already fetches
    /// `[fork] max_seeds_per_node` copies. Returns whether it started.
    pub(crate) fn start_seed(
        &self,
        key: &StagedKey,
        task: impl FnOnce() -> JoinHandle<()>,
    ) -> bool {
        let limit = self.config().max_seeds_per_node.get();
        let mut ledger = self.lock_ledger();
        ledger.seeds.retain(|_, task| !task.is_finished());
        if ledger.seeds.contains_key(key) || ledger.seeds.len() >= limit {
            return false;
        }
        ledger.seeds.insert(key.clone(), task());
        true
    }

    /// Stops every fetch of a copy of `name` that isn't for `keep`.
    pub(crate) fn abandon_seeds(&self, name: &DatabaseName, keep: Option<&StagedKey>) {
        let mut ledger = self.lock_ledger();
        ledger.seeds.retain(|key, task| {
            let stale = key.fork.name == *name && Some(key) != keep;
            if stale {
                task.abort();
            }
            !stale
        });
    }

    /// Stops every fetch of a copy of `fork`.
    pub(crate) fn abandon_fork_seeds(&self, fork: &ForkIdentity) {
        let mut ledger = self.lock_ledger();
        ledger.seeds.retain(|key, task| {
            let stale = key.fork == *fork;
            if stale {
                task.abort();
            }
            !stale
        });
    }

    /// Records the fork identity control state shows for `name` now, and forgets what this node
    /// reported about another.
    pub(crate) fn observe(&self, name: &DatabaseName, current: Option<&ForkIdentity>) {
        let mut reports = self.lock_reports();
        let entry = reports.entry(name.clone()).or_default();
        if entry.current.as_ref() != current {
            *entry = NameReports {
                current: current.cloned(),
                ..NameReports::default()
            };
        }
    }

    /// Reports `failure` for its fork from now on.
    pub(crate) fn report_failure(&self, failure: SeedFailure) {
        let name = failure.fork().name.clone();
        self.lock_reports().entry(name).or_default().seed_failure = Some(failure);
    }

    /// Whether this node reported `key`'s fetch failed.
    pub(crate) fn failed(&self, key: &StagedKey) -> bool {
        self.lock_reports()
            .get(&key.fork.name)
            .and_then(|reports| reports.seed_failure.as_ref())
            .is_some_and(|failure| *failure.fork() == key.fork && failure.attempt() == key.attempt)
    }

    /// Reports `lost` for its fork from now on.
    pub(crate) fn report_lost(&self, lost: CopyLost) {
        let name = lost.fork().name.clone();
        self.lock_reports().entry(name).or_default().copy_lost = Some(lost);
    }

    /// Reports `reset` for its fork from now on.
    pub(crate) fn report_reset(&self, reset: ResetReport) {
        let name = reset.fork().name.clone();
        self.lock_reports().entry(name).or_default().reset = Some(reset);
    }

    /// Reports, for `name`, that placement caught up with its primary here, or nothing.
    pub(crate) fn report_placement_caught_up(
        &self,
        name: &DatabaseName,
        caught_up: Option<PlacementCaughtUp>,
    ) {
        self.lock_reports()
            .entry(name.clone())
            .or_default()
            .placement_caught_up = caught_up;
    }

    /// What this node reports for `name` about placement catching up, if anything.
    pub(crate) fn placement_caught_up(&self, name: &DatabaseName) -> Option<PlacementCaughtUp> {
        self.lock_reports()
            .get(name)
            .and_then(|reports| reports.placement_caught_up.clone())
    }

    /// The reset this node reports for `name`, if it reports one.
    pub(crate) fn reset(&self, name: &DatabaseName) -> Option<ResetReport> {
        self.lock_reports()
            .get(name)
            .and_then(|reports| reports.reset.clone())
    }

    /// Reports `progress` for its fork, or none for `name`.
    pub(crate) fn report_progress(&self, name: &DatabaseName, progress: Option<SeedProgress>) {
        self.lock_reports()
            .entry(name.clone())
            .or_default()
            .progress = progress;
    }

    /// Closes the readiness gate on `name`: its primary here serves no client until
    /// [`open_gate`](Self::open_gate).
    pub(crate) fn close_gate(&self, name: &DatabaseName) {
        self.lock_gated().insert(name.clone());
    }

    /// Opens the readiness gate on `name`. Returns whether it was closed.
    pub(crate) fn open_gate(&self, name: &DatabaseName) -> bool {
        self.lock_gated().remove(name)
    }

    /// Whether `name`'s readiness gate is closed.
    pub(crate) fn is_gated(&self, name: &DatabaseName) -> bool {
        self.lock_gated().contains(name)
    }

    fn lock_frozen(&self) -> MutexGuard<'_, Frozen> {
        self.frozen.lock().expect("fork replica set lock poisoned")
    }

    /// `name`'s frozen replica set, if it has one: its recorded holders.
    pub(crate) fn frozen(&self, name: &DatabaseName) -> Option<Vec<fleetring::NodeId>> {
        self.frozen_set(name).map(|set| set.holders().to_vec())
    }

    /// How `name`'s replica set is frozen, if it is.
    pub(crate) fn frozen_set(&self, name: &DatabaseName) -> Option<FrozenSet> {
        self.lock_frozen()
            .sets
            .get(name)
            .map(|(_, set)| set.clone())
    }

    /// The names with a frozen replica set.
    pub(crate) fn frozen_names(&self) -> BTreeSet<DatabaseName> {
        self.lock_frozen().sets.keys().cloned().collect()
    }

    /// Follows what control state shows of `name`'s fork: its replica set is frozen at the
    /// recorded holders while it is seeded, handed over while it is ready with its holders kept,
    /// and follows the ring once they are released, and for anything else: seeding, failed, or
    /// no fork at all. A state this build doesn't know changes nothing.
    ///
    /// A view never moves the fork back: one that shows it seeded once this node saw it ready,
    /// or its holders kept once it saw them released, is an older view and changes nothing, so
    /// a primary here never goes back to counting the holders alone.
    pub(crate) fn follow_view(&self, name: &DatabaseName, fork: Option<&ForkView>) {
        let mut frozen = self.lock_frozen();
        let Some(fork) = fork else {
            frozen.sets.remove(name);
            frozen.released.remove(name);
            return;
        };
        let identity = &fork.identity;
        let released = frozen.released.get(name) == Some(identity);
        let handed_over = matches!(
            frozen.sets.get(name),
            Some((seen, FrozenSet::Handover(_))) if seen == identity
        );
        match &fork.phase {
            ForkPhase::Seeded { holders, .. } => {
                if !released && !handed_over {
                    let set = FrozenSet::Seeded(holders.iter().cloned().collect());
                    frozen.sets.insert(name.clone(), (identity.clone(), set));
                }
            }
            ForkPhase::Ready {
                holders: Some(holders),
            } => {
                if !released {
                    let set = FrozenSet::Handover(holders.iter().cloned().collect());
                    frozen.sets.insert(name.clone(), (identity.clone(), set));
                }
            }
            ForkPhase::Ready { holders: None } => {
                frozen.sets.remove(name);
                frozen.released.insert(name.clone(), identity.clone());
            }
            ForkPhase::Unknown => {}
            _ => {
                frozen.sets.remove(name);
            }
        }
    }

    /// Whether this node holds any fork state for `name`: a staged copy, a frozen replica set,
    /// a readiness gate or its opening, or a fork identity it reports about. A name with none
    /// is an ordinary database here, or a fork this node saw ready.
    pub(crate) fn tracks(&self, name: &DatabaseName) -> bool {
        !self.store.of_name(name).is_empty()
            || self.lock_frozen().sets.contains_key(name)
            || self.lock_gated().contains(name)
            || self
                .opening
                .lock()
                .expect("fork opening lock poisoned")
                .contains_key(name)
            || self
                .lock_reports()
                .get(name)
                .is_some_and(|reports| reports.current.is_some())
    }

    /// Whether `name`'s gate was opened under `lease` on this node: the same incarnation at the
    /// same epoch. A gate opened for an earlier database of the name, at whatever epoch, doesn't
    /// count.
    #[cfg(test)]
    pub(crate) fn opened_at(&self, name: &DatabaseName, lease: GateLease) -> bool {
        matches!(
            self.opening.lock().expect("fork opening lock poisoned").get(name),
            Some(Opening::Open(opened)) if *opened == lease
        )
    }

    /// Records that `name`'s gate opened under `lease`.
    #[cfg(test)]
    pub(crate) fn mark_opened(&self, name: &DatabaseName, lease: GateLease) {
        self.opening
            .lock()
            .expect("fork opening lock poisoned")
            .insert(name.clone(), Opening::Open(lease));
    }

    /// Starts `task` opening `name` under `lease`, unless one runs under that lease or the gate
    /// opened under it already. A task opening it under another lease, one on an earlier
    /// database of the name included, is aborted.
    pub(crate) fn start_opening(
        &self,
        name: &DatabaseName,
        lease: GateLease,
        task: impl FnOnce() -> JoinHandle<()>,
    ) {
        let mut opening = self.opening.lock().expect("fork opening lock poisoned");
        let busy = match opening.get(name) {
            Some(Opening::Open(opened)) => *opened == lease,
            Some(Opening::Running(running, handle)) => *running == lease && !handle.is_finished(),
            None => false,
        };
        if !busy {
            if let Some(previous) = opening.get(name) {
                previous.abort();
            }
            opening.insert(name.clone(), Opening::Running(lease, task()));
        }
    }

    /// Forgets how opening `name`'s gate stands, and stops a task opening it: this node's copy
    /// of the name is gone, so no lease on it opens the gate again, and a fork made again under
    /// the name starts with none.
    pub(crate) fn forget_opening(&self, name: &DatabaseName) {
        let removed = self
            .opening
            .lock()
            .expect("fork opening lock poisoned")
            .remove(name);
        if let Some(removed) = removed {
            removed.abort();
        }
        // A gate closed for the copy goes with it: a later database of the name that no fork
        // made would otherwise refuse every client as a fork that isn't ready.
        self.lock_gated().remove(name);
    }

    /// Forgets how opening `name`'s gate stands if that is for another database of the name than
    /// `incarnation`'s, and stops a task opening it: no lease on the database this node now
    /// serves can match it. A lease that names no incarnation changes nothing.
    pub(crate) fn forget_other_incarnation(
        &self,
        name: &DatabaseName,
        incarnation: Option<Incarnation>,
    ) {
        let Some(incarnation) = incarnation else {
            return;
        };
        let mut opening = self.opening.lock().expect("fork opening lock poisoned");
        if opening
            .get(name)
            .is_some_and(|entry| entry.lease().incarnation != Some(incarnation))
            && let Some(removed) = opening.remove(name)
        {
            removed.abort();
            // The gate it was opening was the other database's.
            self.lock_gated().remove(name);
        }
    }

    /// The names whose readiness gate is closed.
    pub(crate) fn gated(&self) -> BTreeSet<DatabaseName> {
        self.lock_gated().clone()
    }

    /// The staged copy of `name` this node reports: the one of the identity it last saw, at
    /// the highest attempt, or, before it has seen one, the highest attempt of any.
    fn reported_staged(
        &self,
        name: &DatabaseName,
        current: Option<&ForkIdentity>,
    ) -> Option<Staged> {
        let copies = self.store.of_name(name);
        let matching: Vec<&Staged> = copies
            .iter()
            .filter(|staged| staged.held != Held::Lost)
            .filter(|staged| current.is_none_or(|current| staged.sidecar.key.fork == *current))
            .collect();
        matching
            .into_iter()
            .max_by_key(|staged| staged.sidecar.key.attempt)
            .cloned()
    }

    /// Adds what this node reports about forks to `statuses`, one per database: each staged
    /// copy it holds at log epoch 0, unless it leads the name or its copy's log has frames of a
    /// later epoch (`log_epoch`), and every failure, loss, reset, and progress it reports.
    pub(crate) fn decorate(
        &self,
        statuses: &mut BTreeMap<DatabaseName, DatabaseStatus>,
        log_epoch: impl Fn(&DatabaseName) -> u64,
    ) {
        let reports = self.lock_reports().clone();
        let mut names: BTreeSet<DatabaseName> = self.store.names();
        names.extend(reports.keys().cloned());
        for name in names {
            let report = reports.get(&name).cloned().unwrap_or_default();
            let base = statuses.get(&name);
            let primary = base.is_some_and(|status| {
                status.role == fleetring_proto::v1::ReplicaRole::Primary as i32
            });
            let staged = (!primary && log_epoch(&name) == 0)
                .then(|| self.reported_staged(&name, report.current.as_ref()))
                .flatten();
            let nothing = staged.is_none()
                && report.seed_failure.is_none()
                && report.copy_lost.is_none()
                && report.reset.is_none()
                && report.placement_caught_up.is_none()
                && report.progress.is_none();
            if nothing {
                continue;
            }
            let (role, epoch, position) = match base {
                Some(status) => (
                    role_from_status(status.role),
                    status.epoch,
                    status.position.clone(),
                ),
                None => (
                    ReplicaRole::Follower,
                    0,
                    staged
                        .as_ref()
                        .map(|staged| staged.sidecar.position.as_bytes().to_vec())
                        .unwrap_or_default(),
                ),
            };
            let mut replica = HostedReplica::new(
                name.clone(),
                role,
                fleetring_control::Position::new(position),
            )
            .with_log_epoch(LeaseEpoch::new(epoch));
            if let Some(size) = base.and_then(|status| status.size_bytes) {
                replica = replica.with_size_bytes(size);
            }
            if let Some(staged) = &staged {
                let sidecar = &staged.sidecar;
                replica = replica.with_staged(StagedCopy::new(
                    sidecar.key.fork.clone(),
                    sidecar.key.attempt,
                    fleetring_control::Position::new(sidecar.position.as_bytes().to_vec()),
                    fleetring_control::fork::ChainHash::new(sidecar.chain_hash.as_bytes().to_vec()),
                    sidecar.source_incarnation,
                ));
            }
            if let Some(failure) = report.seed_failure {
                replica = replica.with_seed_failure(failure);
            }
            if let Some(lost) = report.copy_lost {
                replica = replica.with_copy_lost(lost);
            }
            if let Some(reset) = report.reset {
                replica = replica.with_reset(reset);
            }
            if let Some(caught_up) = report.placement_caught_up {
                replica = replica.with_placement_caught_up(caught_up);
            }
            statuses.insert(name, replica_to_proto(&replica, report.progress.as_ref()));
        }
    }
}

/// Installs this node's staged copy of `database` as its follower copy, if control state shows
/// the fork seeded with this node among its holders, and the copy matches what it recorded
/// (ADR-0046, point 6.2): the fork's own epoch then streams on from the copy's position,
/// instead of a full snapshot. Returns whether it did. A copy that fails its check is reported
/// lost, and the follower takes a snapshot from the primary as any follower does.
///
/// # Errors
///
/// The status of an install that failed after the check.
pub(super) async fn install_holder(
    inner: &Arc<super::Inner>,
    database: &DatabaseName,
) -> Result<bool, Status> {
    if inner.forks.store().of_name(database).is_empty() {
        return Ok(false);
    }
    let Ok(view) = inner.authority.view(database).await else {
        return Ok(false);
    };
    let Some(fork) = view.fork else {
        return Ok(false);
    };
    let ForkPhase::Seeded {
        attempt,
        at,
        chain_hash,
        holders,
        ..
    } = &fork.phase
    else {
        return Ok(false);
    };
    if !holders.contains(&inner.node) {
        return Ok(false);
    }
    let key = StagedKey {
        fork: fork.identity.clone(),
        attempt: *attempt,
    };
    let replication = super::Replication {
        inner: Arc::clone(inner),
    };
    match replication.verify_staged(&key, at, chain_hash).await {
        Ok(sidecar) => {
            replication
                .install_staged(&key, &sidecar, InstallAs::Follower)
                .await?;
            Ok(true)
        }
        Err(reason) => {
            tracing::warn!(fork = %key.fork, attempt = key.attempt, ?reason, "the staged copy fails its check; following from a snapshot instead");
            inner
                .forks
                .report_lost(CopyLost::new(key.fork, key.attempt, reason));
            Ok(false)
        }
    }
}

/// Records, at start, every staged copy that was moved into its database's place but whose
/// incarnation the node never recorded: a crash or a failed write between the move and the
/// record (`install_staged`) leaves such a copy, which a drop notice, the orphan check, and
/// `orphan remove` would otherwise not know. It blocks on the file system.
pub(super) fn record_installed_copies(inner: &super::Inner) {
    record_installed(&inner.epochs, inner.forks.store(), |name| {
        inner.registry().location(name)
    });
}

/// [`record_installed_copies`] over `epochs` and `store`, with `location` naming each
/// database's place. Only a copy whose epoch record was never written is the install's: a
/// record that exists, however it reads, belongs to a copy this node already knows, or to a
/// later database's copy, here since, and an unreadable one is left for an operator. Nothing
/// is moved or deleted here: a copy whose record can't be written is left in place and
/// logged, since the node can't tell, at start, what else the record's failure hides.
pub(super) fn record_installed(
    epochs: &super::epochs::Epochs,
    store: &staged::StagedStore,
    location: impl Fn(&DatabaseName) -> std::path::PathBuf,
) {
    for key in store.installed_keys() {
        let name = &key.fork.name;
        if !epochs.never_recorded(name) || !location(name).exists() {
            continue;
        }
        match epochs.record_install(name, key.fork.incarnation.get(), None) {
            Ok(()) => {
                tracing::warn!(target: "fleetringd::audit", fork = %key.fork, attempt = key.attempt, "recorded an installed staged copy a crash left unrecorded");
            }
            Err(error) => {
                tracing::error!(fork = %key.fork, %error, "can't record an installed staged copy a crash left unrecorded; left it in place");
            }
        }
    }
}

/// How a node installs a staged copy as its copy of the fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstallAs {
    /// As the fork's first primary, under its first lease at `epoch`, which opens it at its
    /// position.
    Primary {
        /// The first lease's epoch.
        epoch: LeaseEpoch,
    },
    /// As a follower of the fork's primary, which streams the fork's own epoch on from there.
    Follower,
}

impl super::Replication {
    /// Runs the node's fork work with `config`'s limits, through `faults`.
    pub(crate) fn configure_forks(&self, config: ForkConfig, faults: ForkFaults) {
        self.inner.forks.configure(config, faults);
    }

    /// The node's fork state.
    pub(crate) fn forks(&self) -> &Forks {
        &self.inner.forks
    }

    /// Starts fetching the copy `plan` names, unless a fetch of it is under way or the node
    /// fetches as many copies as `[fork] max_seeds_per_node` allows. Returns whether it started.
    pub(crate) fn seed_fork(&self, plan: seed::SeedPlan) -> bool {
        let key = plan.key.clone();
        let inner = Arc::clone(&self.inner);
        self.inner
            .forks
            .start_seed(&key, move || tokio::spawn(seed::run(inner, plan)))
    }

    /// Records that this node's copy of `database` belongs to `incarnation`, unless it recorded
    /// another already, and returns the one recorded (ADR-0046, point 1.3).
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` if the record can't be written.
    pub(crate) async fn record_incarnation(
        &self,
        database: &DatabaseName,
        incarnation: fleetring_control::Incarnation,
    ) -> Result<fleetring_control::Incarnation, Status> {
        let inner = Arc::clone(&self.inner);
        let database = database.clone();
        let recorded = super::follower::blocking(inner.execution, move || {
            inner
                .epochs
                .record_incarnation(&database, incarnation.get())
        })
        .await?;
        recorded
            .map(fleetring_control::Incarnation::new)
            .map_err(|error| Status::unavailable(format!("can't record the incarnation: {error}")))
    }

    /// Closes `database`'s readiness gate: its primary here serves no client, and no change
    /// stream starts on it, until [`open_gate`](Self::open_gate).
    pub(crate) fn close_gate(&self, database: &DatabaseName) {
        let _primaries = self.inner.lock_primaries();
        self.inner.forks.close_gate(database);
        self.inner.served.send_modify(|served| {
            served.remove(database);
        });
    }

    /// Closes `database`'s readiness gate unless it was opened under `lease` already, deciding
    /// and closing under the lock an opening marks itself open under: an opening under `lease`
    /// that finishes meanwhile either is seen open, or opens the gate after it was closed.
    pub(crate) fn close_gate_unless_opened(&self, database: &DatabaseName, lease: GateLease) {
        let opening = self
            .inner
            .forks
            .opening
            .lock()
            .expect("fork opening lock poisoned");
        if matches!(opening.get(database), Some(Opening::Open(opened)) if *opened == lease) {
            return;
        }
        self.close_gate(database);
        drop(opening);
    }

    /// Records that `database`'s gate opened under `lease`, and opens it, under the lock
    /// [`close_gate_unless_opened`](Self::close_gate_unless_opened) decides under: a start under
    /// another lease either closes the gate after this opened it, or sees it open under `lease`
    /// and leaves it.
    pub(crate) fn mark_opened_and_open(&self, database: &DatabaseName, lease: GateLease) {
        let mut opening = self
            .inner
            .forks
            .opening
            .lock()
            .expect("fork opening lock poisoned");
        opening.insert(database.clone(), Opening::Open(lease));
        self.open_gate(database);
        drop(opening);
    }

    /// Opens `database`'s readiness gate: its primary here, if it has one, joins the databases
    /// served, so that change streams start on it.
    pub(crate) fn open_gate(&self, database: &DatabaseName) {
        let primaries = self.inner.lock_primaries();
        self.inner.forks.open_gate(database);
        if let Some(served) = primaries.get(database) {
            let epoch = served.primary.role().epoch;
            self.inner.served.send_modify(|served| {
                served.insert(database.clone(), epoch);
            });
        }
    }

    /// Whether `database`'s readiness gate is closed.
    pub(crate) fn is_gated(&self, database: &DatabaseName) -> bool {
        self.inner.forks.is_gated(database)
    }

    /// How many replicas of `database`, its primary here included, hold its log up to
    /// `position`, if this node is its primary.
    pub(crate) fn held_by(&self, database: &DatabaseName, position: &Position) -> Option<usize> {
        let primary = self.inner.primary(database)?;
        let quorum = self.inner.followers_of(&primary);
        let own = usize::from(primary.committed().as_ref() >= Some(position)) * quorum.own;
        Some(own + primary.acknowledged_by(&quorum.followers, position))
    }

    /// Checks `key`'s staged copy against its sidecar and the position and chain hash control
    /// state recorded for it, reading every byte.
    ///
    /// # Errors
    ///
    /// Why the copy is lost.
    pub(crate) async fn verify_staged(
        &self,
        key: &StagedKey,
        at: &Position,
        chain_hash: &ChainHash,
    ) -> Result<staged::Sidecar, CopyLossReason> {
        let inner = Arc::clone(&self.inner);
        let (key, at, chain_hash) = (key.clone(), at.clone(), *chain_hash);
        super::follower::blocking(inner.execution, move || {
            inner.forks.store().verify(&key, (&at, &chain_hash))
        })
        .await
        .unwrap_or(Err(CopyLossReason::Damaged))
    }

    /// Moves `key`'s staged copy, which [`verify_staged`](Self::verify_staged) checked, into
    /// `database`'s place, which must hold no copy, and starts its hash chain at the copy's
    /// position: as the fork's first primary, or as a follower at log epoch 0 that the fork's
    /// own epoch continues from (ADR-0046, point 6.2). Returns the copy's position.
    ///
    /// # Errors
    ///
    /// `FAILED_PRECONDITION` if a copy is in the database's place or the database is open here,
    /// `DATA_LOSS` if the engine refuses the copy (#154), and `INTERNAL` if it can't be moved.
    pub(crate) async fn install_staged(
        &self,
        key: &StagedKey,
        sidecar: &staged::Sidecar,
        install: InstallAs,
    ) -> Result<Option<Position>, Status> {
        let inner = &self.inner;
        let database = &key.fork.name;
        if inner.primary(database).is_some() || inner.registry().is_open(database) {
            return Err(Status::failed_precondition(format!(
                "{database} is open on this node"
            )));
        }
        let copy = inner.forks.store().copy_dir(key);
        super::snapshot::check_copy(inner, &copy, &inner.node).await?;
        let location = inner.registry().location(database);
        let moved = {
            let inner = Arc::clone(inner);
            let key = key.clone();
            super::follower::blocking(inner.execution, move || {
                inner.forks.store().install(&key, &location)
            })
            .await?
        };
        moved.map_err(|error| match error.kind() {
            std::io::ErrorKind::AlreadyExists => Status::failed_precondition(format!(
                "this node holds a copy of {database} already: {error}"
            )),
            _ => Status::internal(format!("can't move the staged copy into place: {error}")),
        })?;
        // Recorded at once, before anything is served: a copy in place that no record names
        // would be unknown to a drop notice, an orphan check, and `orphan remove`, and a new
        // database of the name would open it as its own.
        let lease = match install {
            InstallAs::Primary { epoch } => Some(epoch.get()),
            InstallAs::Follower => None,
        };
        let recorded = {
            let inner = Arc::clone(inner);
            let database = database.clone();
            let incarnation = key.fork.incarnation.get();
            super::follower::blocking(inner.execution, move || {
                inner.epochs.record_install(
                    &database,
                    incarnation,
                    lease.map(|epoch| (epoch, &inner.node)),
                )
            })
            .await?
        };
        match recorded {
            Ok(()) => {}
            Err(
                error @ (super::epochs::EpochError::Stale { .. }
                | super::epochs::EpochError::OtherHolder { .. }),
            ) => {
                // The record names an epoch at least the lease's, so the copy is known; the
                // opening finds the lease superseded and serves nothing.
                tracing::info!(fork = %key.fork, %error, "the installed copy's record names a newer lease already");
            }
            Err(error) => {
                tracing::error!(fork = %key.fork, %error, "can't record the installed copy's incarnation and first lease; moving it back");
                let location = inner.registry().location(database);
                let undone = {
                    let inner = Arc::clone(inner);
                    let key = key.clone();
                    super::follower::blocking(inner.execution, move || {
                        inner.forks.store().uninstall(&key, &location)
                    })
                    .await?
                };
                if let Err(undo) = undone {
                    tracing::error!(fork = %key.fork, %undo, "can't move an unrecorded installed copy back");
                }
                return Err(super::failover::epoch_status(error));
            }
        }
        let position = super::primary::held(sidecar.position.as_bytes().to_vec());
        let anchor = super::chain::Anchor::new(
            position.as_ref(),
            sidecar.chain_hash,
            super::chain::Provenance::Recorded,
        );
        if install == InstallAs::Follower {
            super::failover::set_log_epoch(inner, database, 0).await?;
            let slot = inner.slot(database);
            slot.seeded(position.clone());
            slot.set_chain(Some(super::chain::Chain::new(anchor.clone())));
        }
        super::follower::record_anchor(inner, database, anchor).await;
        tracing::info!(target: "fleetringd::audit", fork = %key.fork, attempt = key.attempt, ?install, position = ?position, "opened the fork's staged copy");
        Ok(position)
    }

    /// Installs this node's staged copy of `database` as its copy, as [`install_holder`] does,
    /// if the node holds no copy: what a promoted holder that never followed the fork's first
    /// primary fences and ranks with (ADR-0046, point 6.2).
    ///
    /// # Errors
    ///
    /// The status of an install that failed after its check.
    pub(crate) async fn install_holder_copy(&self, database: &DatabaseName) -> Result<(), Status> {
        let location = self.inner.registry().location(database);
        let exists = super::follower::blocking(self.inner.execution, move || location.try_exists())
            .await?
            .map_err(|error| Status::internal(error.to_string()))?;
        if exists {
            return Ok(());
        }
        install_holder(&self.inner, database).await.map(drop)
    }

    /// Deletes every staged directory of `fork`, on a positive signal for its identity
    /// (ADR-0046, point 4.6), and stops fetching its copies.
    pub(crate) async fn delete_fork_copies(&self, fork: &ForkIdentity, signal: &str) {
        self.inner.forks.abandon_fork_seeds(fork);
        let inner = Arc::clone(&self.inner);
        let deleting = fork.clone();
        let deleted = super::follower::blocking(inner.execution, move || {
            inner.forks.store().delete_identity(&deleting)
        })
        .await;
        match deleted {
            Ok(Ok(())) => {
                tracing::info!(target: "fleetringd::audit", %fork, signal, "deleted the fork's staged copies");
            }
            Ok(Err(error)) => {
                tracing::warn!(%fork, %error, "can't delete the fork's staged copies")
            }
            Err(status) => tracing::warn!(%fork, %status, "can't delete the fork's staged copies"),
        }
    }

    /// Deletes `key`'s staged directory, on a positive signal for its identity (ADR-0046, point
    /// 4.6).
    pub(crate) async fn delete_staged(&self, key: &StagedKey, signal: &str) {
        let inner = Arc::clone(&self.inner);
        let deleting = key.clone();
        let deleted = super::follower::blocking(inner.execution, move || {
            inner.forks.store().delete(&deleting)
        })
        .await;
        match deleted {
            Ok(Ok(())) => {
                tracing::info!(target: "fleetringd::audit", fork = %key.fork, attempt = key.attempt, signal, "deleted a staged copy");
            }
            Ok(Err(error)) => {
                tracing::warn!(fork = %key.fork, attempt = key.attempt, %error, "can't delete a staged copy");
            }
            Err(status) => {
                tracing::warn!(fork = %key.fork, attempt = key.attempt, %status, "can't delete a staged copy");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips() {
        let bytes = [0, 1, 0x7f, 0xff];
        assert_eq!(hex(&bytes), "00017fff");
        assert_eq!(unhex("00017fff"), Some(bytes.to_vec()));
        assert_eq!(unhex("0"), None);
        assert_eq!(unhex("zz"), None);
    }

    #[test]
    fn admission_leaves_room_beside_the_copy() {
        // A tenth more than the copy, beside 1 GiB on a small volume and 1% on a large one.
        let defaults = ForkConfig::default();
        assert_eq!(admission_need(1_000, 1 << 20, &defaults), 1_100 + (1 << 30));
        assert_eq!(
            admission_need(1_000, 100 << 30, &defaults),
            1_100 + (1 << 30)
        );
        assert_eq!(
            admission_need(1_000, 500 << 30, &defaults),
            1_100 + (5 << 30)
        );
        // A dev node's small margin: 64 MiB, whatever the volume.
        let dev = ForkConfig {
            admission_min_free_bytes: 64 << 20,
            admission_min_free_percent: 0,
            ..ForkConfig::default()
        };
        assert_eq!(admission_need(1_000, 926 << 30, &dev), 1_100 + (64 << 20));
    }

    /// #165: a fork made again under a dropped fork's name has its first lease at the epoch the
    /// dropped fork's had. Its primary must not take the dropped fork's open gate for its own.
    #[tokio::test]
    async fn a_gate_opened_for_a_dropped_database_of_the_name_does_not_count() {
        let forks = Forks::new(
            Path::new("/nonexistent/state"),
            crate::observe::ReplicationMetrics::default(),
        );
        let name: DatabaseName = "dst".parse().expect("a name");
        let epoch = LeaseEpoch::new(1);
        let dropped = GateLease {
            incarnation: Some(Incarnation::new(0xd5d6)),
            epoch,
        };
        let again = GateLease {
            incarnation: Some(Incarnation::new(0xfd4c)),
            epoch,
        };
        forks.mark_opened(&name, dropped);
        assert!(forks.opened_at(&name, dropped));
        assert!(
            !forks.opened_at(&name, again),
            "the same epoch of another incarnation"
        );
        let mut started = false;
        forks.start_opening(&name, again, || {
            started = true;
            tokio::spawn(std::future::pending())
        });
        assert!(started, "the new incarnation's opening runs");
        let mut twice = false;
        forks.start_opening(&name, again, || {
            twice = true;
            tokio::spawn(async {})
        });
        assert!(!twice, "one opening per lease at a time");
    }

    /// A dropped name's gate state goes with its copy, and a lease on another database of the
    /// name clears what an earlier one left: neither leaks, nor makes the name look like a fork's.
    #[tokio::test]
    async fn gate_state_goes_with_the_copy_or_another_incarnation() {
        let forks = Forks::new(
            Path::new("/nonexistent/state"),
            crate::observe::ReplicationMetrics::default(),
        );
        let name: DatabaseName = "dst".parse().expect("a name");
        let dropped = GateLease {
            incarnation: Some(Incarnation::new(0xd5d6)),
            epoch: LeaseEpoch::new(1),
        };
        let again = Incarnation::new(0xfd4c);

        // The copy is dropped while a task opens its gate: the task stops, and nothing stays.
        let (_keep, held) = tokio::sync::oneshot::channel::<()>();
        let (running_tx, running) = tokio::sync::oneshot::channel::<()>();
        forks.start_opening(&name, dropped, || {
            tokio::spawn(async move {
                let _running = running_tx;
                let _never = held.await;
            })
        });
        assert!(forks.tracks(&name));
        forks.forget_opening(&name);
        assert!(!forks.tracks(&name), "the dropped name's entry stays");
        assert!(running.await.is_err(), "the opening task was stopped");

        // A lease on the same database keeps the open gate; one on another database clears it.
        forks.mark_opened(&name, dropped);
        forks.close_gate(&name);
        forks.forget_other_incarnation(&name, None);
        forks.forget_other_incarnation(&name, dropped.incarnation);
        assert!(forks.opened_at(&name, dropped), "the same incarnation");
        assert!(forks.is_gated(&name), "the same incarnation keeps its gate");
        forks.forget_other_incarnation(&name, Some(again));
        assert!(!forks.is_gated(&name), "another incarnation's gate goes");
        assert!(
            !forks.tracks(&name),
            "another incarnation's lease leaves it"
        );

        // A copy dropped before its reset leaves no closed gate behind.
        forks.close_gate(&name);
        forks.forget_opening(&name);
        assert!(!forks.is_gated(&name));
        assert!(!forks.tracks(&name));
    }

    #[test]
    fn a_retry_after_round_trips() {
        let status = with_retry_after(Status::unavailable("later"), Duration::from_millis(250));
        assert_eq!(retry_after(&status), Some(Duration::from_millis(250)));
        assert_eq!(retry_after(&Status::unavailable("later")), None);
    }
}
