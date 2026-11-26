//! The control node's failover timer: ticks, expired leases, and promotions (ADR-0018).
//!
//! Liveness and lease expiry are judged at the time of the last applied command, so on its own
//! the state machine notices nothing while no command arrives. The timer keeps it moving:
//!
//! 1. **Ticks.** When no command has arrived for a whole tick interval, it submits
//!    [`Action::Tick`], which only moves the state's time on. It also submits one, however busy
//!    the control plane is, once a delegation record's, operation's, or tombstone's `kept_until`
//!    has passed ([`delegation::tick_due`]), so an idle lineage's records go too (ADR-0047,
//!    point 6.2).
//! 2. **Expired leases.** It asks the state which promotions a `StartPromotion` would start now
//!    ([`State::promotions_due`]) and submits one for each. The state machine decides; the timer
//!    only triggers commands, and the control node stamps them as usual, so a replay gives the
//!    same result.
//! 3. **Candidates.** For each promotion in progress it tells the candidate through the
//!    candidate's `Replication.Promote`, at once and then again every notify interval until the
//!    promotion completes or is replaced. The candidate fences, catches up, and completes it.
//! 4. **Expired databases.** A task of its own, started and stopped with the timer, so that
//!    expiries never delay a tick, a promotion, or a candidate's notice (ADR-0041, point 4).
//!    Once a second it asks the state which databases' expiries have passed by this node's clock
//!    ([`State::expired_databases`]) and submits [`Action::ExpireDatabase`] for at most
//!    [`MAX_EXPIRIES_PER_SWEEP`] of them, all at once, so the writer logs them in one group.
//!    `apply` judges each one at its own stamp, so an expiry extended after the sweep looked
//!    wins, and an expired database is dropped as `DropDatabase` drops it, its replicas told
//!    through `Replication.DropCopy` (ADR-0042). A command refused or rejected is skipped; only
//!    a stopped control node ends the task. Expiry doesn't depend on
//!    leases, so it waits for no settling.
//! 5. **Forks.** After the settling below, on every tick it submits what the fork driver
//!    proposes ([`fork::actions`]): completions, retries, failures, readiness, and a tick when
//!    a fork's deadline or an operation's retention has passed (ADR-0046, point 3.11).
//! 6. **Kept replica sets.** After the forks, it releases every replica set a membership change
//!    kept whose primary reports that placement's write quorum holds its log
//!    ([`State::releases_due`]; ADR-0031, amendment 2).
//!
//! All of it runs only on the control group's ready leader (ADR-0040 point 6): the timer and the
//! sweep start when this node becomes ready, and pause for a tick, without ending, when a command
//! is refused for leadership (`NotLeader`, `CatchingUp`, or `Superseded`): they then resume if the
//! node is still ready under the same leader, keeping the settle window, or start again under a
//! new one. Every command they submit names the leader they run under, and the proposer refuses
//! it under any other. Only a stopped control node or a storage failure ends them.
//!
//! A leader that has just become ready gives primaries one lease duration plus the failover
//! margin to renew before it starts a promotion, a fork action, or a kept set's release: leases
//! look expired after an election or an outage only because nobody could renew them, and the
//! primaries kept serving meanwhile (ADR-0018, point 6). The window is measured on
//! `tokio::time::Instant`, which is monotonic, so a step of the leader's wall clock can't shorten
//! it. Notices for promotions already in progress go out at once. None of this affects safety,
//! which comes from epochs and quorums.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::{
    Action, Applied, DatabaseName, LeaseEpoch, Millis, State, Timestamp, delegation, fork,
};
use fleetring_proto::v1::PromoteRequest;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;

use crate::clock::Clock;
use crate::control::node::{Committed, ControlHandle, SubmitError};
use crate::control::notices::DropNotices;
use crate::control::raft::{LeaderRef, Readiness};
use crate::environment::Network;
use crate::transport::{Peers, replication_client};

/// How often the expiry sweep looks for databases whose expiry has passed.
const EXPIRY_SWEEP: Duration = Duration::from_secs(1);

/// The most `ExpireDatabase` commands one sweep submits, so that a burst of expiries, as after a
/// control node was down, is spread over several sweeps.
pub(crate) const MAX_EXPIRIES_PER_SWEEP: usize = 64;

/// How the failover timer paces itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FailoverSettings {
    /// How often the timer looks at the state, and how long the state may go without a command
    /// before a tick.
    pub tick_interval: Duration,
    /// How often a promotion's candidate is told again while the promotion is in progress, and
    /// how long one notice may take.
    pub notify_interval: Duration,
}

impl FailoverSettings {
    /// A 250 ms tick and a notice to each candidate every second.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tick_interval: Duration::from_millis(250),
            notify_interval: Duration::from_secs(1),
        }
    }

    /// Ticks every `tick_interval`.
    #[must_use]
    pub const fn with_tick_interval(mut self, tick_interval: Duration) -> Self {
        self.tick_interval = tick_interval;
        self
    }
}

impl Default for FailoverSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// The running failover timer of a control node, with its expiry sweep. Dropping it stops both.
#[derive(Debug)]
pub struct Failover {
    task: JoinHandle<()>,
}

impl Failover {
    /// Starts the timer and the expiry sweep for the control node behind `handle`, reading time
    /// from `clock` and reaching candidates over `network`. Both run whenever this node is the
    /// control group's ready leader.
    ///
    /// # Panics
    ///
    /// If called outside a Tokio runtime.
    #[must_use]
    pub fn start(
        handle: ControlHandle,
        clock: Arc<dyn Clock>,
        network: Network,
        settings: FailoverSettings,
    ) -> Self {
        let notices = DropNotices::new(network.clone());
        let task = tokio::spawn(run(handle, clock, network, settings, notices));
        Self { task }
    }

    /// Stops the timer and the sweep, and waits for them to end.
    pub async fn stop(mut self) {
        self.task.abort();
        // Aborted, so it can only end cancelled; the sweep ends with it.
        let _stopped = (&mut self.task).await;
    }
}

impl Drop for Failover {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Starts the expiry sweep for the control node behind `handle`, reading time from `clock`:
/// every [`EXPIRY_SWEEP`], it expires the databases whose expiry has passed, as
/// [`sweep`] does, and tells their replicas through `notices`. The task ends once the control
/// node stops or stops leading; the timer starts it again at the next readiness.
fn spawn_expiry_sweep(
    handle: ControlHandle,
    clock: Arc<dyn Clock>,
    notices: Option<DropNotices>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut sweeps = tokio::time::interval(EXPIRY_SWEEP);
        sweeps.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            sweeps.tick().await;
            let due = handle.state().expired_databases(clock.now());
            match sweep(&handle, due, notices.as_ref()).await {
                Sweep::Continue => {}
                Sweep::Pause | Sweep::Stop => return,
            }
        }
    })
}

/// Whether the timer, or the expiry sweep, goes on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sweep {
    /// It goes on at its next interval.
    Continue,
    /// This node no longer leads, so it pauses until the next readiness.
    Pause,
    /// The control node has stopped, or can't log, so it ends.
    Stop,
}

impl Sweep {
    /// What a submission's `error` means for the timer: only a stopped node or a storage
    /// failure ends it, a lost leadership pauses it, and a refusal skips one command (ADR-0040
    /// point 6).
    fn after(error: &SubmitError) -> Self {
        match error {
            SubmitError::Stopped | SubmitError::Storage(_) => Self::Stop,
            SubmitError::NotLeader { .. }
            | SubmitError::CatchingUp { .. }
            | SubmitError::Superseded => Self::Pause,
            SubmitError::Refused(_) => Self::Continue,
        }
    }

    /// The more final of `self` and `other`.
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Stop, _) | (_, Self::Stop) => Self::Stop,
            (Self::Pause, _) | (_, Self::Pause) => Self::Pause,
            (Self::Continue, Self::Continue) => Self::Continue,
        }
    }
}

/// Submits [`Action::ExpireDatabase`] for each of the first [`MAX_EXPIRIES_PER_SWEEP`] databases
/// of `due`, all at once, so that the writer logs them in one group, and logs what became of
/// each.
///
/// `due` is what the state said had expired when the sweep looked; `apply` judges each command
/// again at its own stamp, so a database whose expiry was extended or cleared since is rejected
/// as not expired and kept. The replicas of each database that expires are told through
/// `notices`.
pub(crate) async fn sweep(
    handle: &ControlHandle,
    due: Vec<DatabaseName>,
    notices: Option<&DropNotices>,
) -> Sweep {
    let mut submitted = JoinSet::new();
    for name in due.into_iter().take(MAX_EXPIRIES_PER_SWEEP) {
        let handle = handle.clone();
        submitted.spawn(async move {
            // The replicas as they were before the expiry, which takes the database's reports.
            let recipients = DropNotices::recipients(&handle.state(), &name);
            let result = handle
                .submit(Action::ExpireDatabase { name: name.clone() })
                .await;
            (name, recipients, result)
        });
    }
    let mut next = Sweep::Continue;
    while let Some(joined) = submitted.join_next().await {
        let Ok((name, recipients, result)) = joined else {
            continue;
        };
        if let (Some(notices), Ok(committed)) = (notices, &result)
            && let Ok(Applied::DatabaseExpired {
                database, epoch, ..
            }) = committed.outcome()
        {
            notices.send(recipients, database, *epoch);
        }
        next = next.and(expired(&name, result));
    }
    next
}

/// Logs what became of the expiry of `database`, and says whether the sweep goes on: it stops
/// with the control node, and pauses when this node no longer leads. A command refused before it
/// was logged, or rejected by `apply`, is skipped, and a storage failure is retried at the next
/// sweep.
pub(crate) fn expired(database: &DatabaseName, result: Result<Committed, SubmitError>) -> Sweep {
    match result {
        Ok(committed) => match committed.outcome() {
            Ok(Applied::DatabaseExpired {
                database,
                epoch,
                expires_at,
            }) => {
                tracing::info!(database = %database, epoch = epoch.get(), %expires_at, "database expired");
            }
            Ok(other) => {
                tracing::debug!(database = %database, ?other, "an expiry had an unexpected outcome");
            }
            // Extended or cleared since the sweep looked, or dropped meanwhile.
            Err(rejection) => {
                tracing::debug!(database = %database, %rejection, "the database didn't expire");
            }
        },
        Err(SubmitError::Refused(rejection)) => {
            tracing::debug!(database = %database, %rejection, "the expiry was refused; skipped");
        }
        Err(SubmitError::Storage(error)) => {
            tracing::warn!(database = %database, %error, "can't log an expiry; the next sweep tries again");
        }
        Err(SubmitError::Stopped) => return Sweep::Stop,
        Err(
            error @ (SubmitError::NotLeader { .. }
            | SubmitError::CatchingUp { .. }
            | SubmitError::Superseded),
        ) => {
            tracing::debug!(database = %database, %error, "the expiry wasn't logged; the next leader sweeps again");
            return Sweep::Pause;
        }
    }
    Sweep::Continue
}

/// Runs the timer and the sweep through every term this node leads, until the control node
/// stops.
async fn run(
    handle: ControlHandle,
    clock: Arc<dyn Clock>,
    network: Network,
    settings: FailoverSettings,
    notices: DropNotices,
) {
    let peers = Arc::new(Peers::with_network(network));
    let mut leadership = handle.watch_leadership();
    // The leader the timers last ran under, and when its settle window started.
    let mut term: Option<(LeaderRef, tokio::time::Instant)> = None;
    loop {
        let leader = loop {
            let current = leadership.borrow_and_update().clone();
            if let Readiness::Ready { leader } = current {
                break leader;
            }
            if leadership.changed().await.is_err() {
                return;
            }
        };
        // The settle window starts at readiness, on the monotonic clock, and again only under a
        // new leader: resuming after a pause in the same term keeps it (ADR-0040 point 6).
        let started = match term {
            Some((under, started)) if under == leader => started,
            Some(_) | None => tokio::time::Instant::now(),
        };
        term = Some((leader, started));
        let sweep = spawn_expiry_sweep(handle.clone(), Arc::clone(&clock), Some(notices.clone()));
        let ended = lead(
            &handle,
            &*clock,
            &peers,
            settings,
            (leader, started),
            &mut leadership,
        )
        .await;
        sweep.abort();
        match ended {
            Sweep::Continue => {}
            Sweep::Pause => {
                // A refusal pauses the timers for a tick; then they resume if this node is still
                // ready under the same leader, or start again under a new one.
                tracing::info!(leader = %leader, "control leader's timers paused for a tick");
                tokio::time::sleep(settings.tick_interval).await;
            }
            Sweep::Stop => return,
        }
    }
}

/// Runs the timer while this node is ready under `leader`, whose settle window started at
/// `started`, and says why it ended: `Continue` when readiness changed, `Pause` when a command
/// was refused for leadership, and `Stop` when the control node stopped. Every command it
/// submits is submitted under `leader`, so none reaches another term's log.
async fn lead(
    handle: &ControlHandle,
    clock: &dyn Clock,
    peers: &Arc<Peers>,
    settings: FailoverSettings,
    (leader, started): (LeaderRef, tokio::time::Instant),
    leadership: &mut tokio::sync::watch::Receiver<Readiness>,
) -> Sweep {
    let tick = millis(settings.tick_interval);
    let mut notified: BTreeMap<DatabaseName, (LeaseEpoch, Timestamp)> = BTreeMap::new();
    let mut ticks = tokio::time::interval(settings.tick_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            changed = leadership.changed() => {
                if changed.is_err() {
                    return Sweep::Stop;
                }
                if *leadership.borrow_and_update() != (Readiness::Ready { leader }) {
                    return Sweep::Continue;
                }
                continue;
            }
            _ = ticks.tick() => {}
        }
        let now = clock.now();
        let current = handle.state();
        let idle = now.as_millis().saturating_sub(current.now().as_millis()) >= tick.get();
        if (idle || delegation::tick_due(&current, now) || current.tombstones().purge_due(now))
            && let Err(error) = handle.submit_under(leader, Action::Tick).await
        {
            match Sweep::after(&error) {
                Sweep::Continue => {
                    tracing::debug!(%error, "a tick was refused");
                }
                next @ (Sweep::Pause | Sweep::Stop) => return next,
            }
        }
        let state = handle.state();
        let config = state.config();
        let window = Duration::from_millis(
            config
                .lease_duration()
                .saturating_add(config.failover_margin())
                .get(),
        );
        if started.elapsed() > window {
            for database in state.promotions_due(now) {
                match handle
                    .submit_under(
                        leader,
                        Action::StartPromotion {
                            database: database.clone(),
                        },
                    )
                    .await
                {
                    Ok(committed) => {
                        if let Ok(Applied::PromotionStarted {
                            epoch, candidate, ..
                        }) = committed.outcome()
                        {
                            tracing::info!(database = %database, epoch = epoch.get(), %candidate, "lease expired; promotion started");
                        }
                    }
                    Err(error) => match Sweep::after(&error) {
                        Sweep::Continue => {
                            tracing::debug!(database = %database, %error, "a promotion was refused");
                        }
                        next @ (Sweep::Pause | Sweep::Stop) => return next,
                    },
                }
            }
            match drive_forks(handle, leader, now).await {
                Sweep::Continue => {}
                next @ (Sweep::Pause | Sweep::Stop) => return next,
            }
            match release_kept_replicas(handle, leader).await {
                Sweep::Continue => {}
                next @ (Sweep::Pause | Sweep::Stop) => return next,
            }
        }
        notify_candidates(
            &handle.state(),
            now,
            peers,
            settings.notify_interval,
            &mut notified,
        );
    }
}

/// Submits what the fork driver proposes at `now` ([`fork::actions`]), one command at a time,
/// and logs what became of each (ADR-0046, point 3.11). The driver reads only the state, so a
/// restarted control node proposes the same commands; `apply` checks each one again, so a
/// proposal made stale by an earlier one is refused and changes nothing. Says whether the timer
/// goes on: it stops with the control node, and pauses when this node no longer leads.
async fn drive_forks(handle: &ControlHandle, leader: LeaderRef, now: Timestamp) -> Sweep {
    for action in fork::actions(&handle.state(), now) {
        match handle.submit_under(leader, action).await {
            Ok(committed) => match committed.outcome() {
                Ok(applied) => tracing::info!(?applied, "a fork moved on"),
                Err(rejection) => tracing::debug!(%rejection, "a fork proposal was refused"),
            },
            Err(SubmitError::Stopped) => return Sweep::Stop,
            Err(error @ (SubmitError::Storage(_) | SubmitError::Refused(_))) => {
                tracing::warn!(%error, "can't log a fork proposal; the next tick proposes again");
            }
            Err(
                SubmitError::NotLeader { .. }
                | SubmitError::CatchingUp { .. }
                | SubmitError::Superseded,
            ) => return Sweep::Pause,
        }
    }
    Sweep::Continue
}

/// The most releases of kept replica sets one tick submits: a join that moves many databases
/// releases them over several ticks rather than holding up the promotions the timer drives.
const RELEASES_PER_TICK: usize = 64;

/// Submits a release for every database whose replica set a membership change kept, once its
/// primary reports that placement's write quorum holds its log ([`State::releases_due`];
/// ADR-0031, amendment 2), up to [`RELEASES_PER_TICK`] at once, so that the writer logs them
/// together, and logs what became of each. `apply` checks each one again, so one made stale
/// meanwhile is refused and changes nothing; the rest wait for the next tick. Says whether the
/// timer goes on.
async fn release_kept_replicas(handle: &ControlHandle, leader: LeaderRef) -> Sweep {
    let mut releases = JoinSet::new();
    for action in handle
        .state()
        .releases_due()
        .into_iter()
        .take(RELEASES_PER_TICK)
    {
        let handle = handle.clone();
        releases.spawn(async move { handle.submit_under(leader, action).await });
    }
    let mut sweep = Sweep::Continue;
    while let Some(joined) = releases.join_next().await {
        match joined {
            Ok(Ok(committed)) => match committed.outcome() {
                Ok(applied) => {
                    tracing::info!(target: "fleetringd::audit", ?applied, "a kept replica set was released");
                }
                Err(rejection) => {
                    tracing::debug!(%rejection, "a kept replica set's release was refused");
                }
            },
            Ok(Err(SubmitError::Stopped)) => sweep = sweep.and(Sweep::Stop),
            Ok(Err(error @ (SubmitError::Storage(_) | SubmitError::Refused(_)))) => {
                tracing::warn!(%error, "can't log a release; the next tick proposes again");
            }
            Ok(Err(
                SubmitError::NotLeader { .. }
                | SubmitError::CatchingUp { .. }
                | SubmitError::Superseded,
            )) => sweep = sweep.and(Sweep::Pause),
            Err(error) => {
                tracing::warn!(%error, "a release's submission ended early; the next tick proposes again");
            }
        }
    }
    sweep
}

/// Tells the candidate of every promotion in progress, unless it was told less than
/// `interval` ago.
fn notify_candidates(
    state: &State,
    now: Timestamp,
    peers: &Arc<Peers>,
    interval: Duration,
    notified: &mut BTreeMap<DatabaseName, (LeaseEpoch, Timestamp)>,
) {
    let mut current = BTreeMap::new();
    for database in state.databases() {
        let Some(promotion) = database.promotion() else {
            continue;
        };
        let epoch = promotion.epoch();
        let name = database.name();
        let last = notified.get(name).copied();
        let due = match last {
            Some((told, at)) => told != epoch || now >= at.saturating_add(millis(interval)),
            None => true,
        };
        current.insert(
            name.clone(),
            if due {
                (epoch, now)
            } else {
                last.unwrap_or((epoch, now))
            },
        );
        if !due {
            continue;
        }
        let Some(address) = state
            .member(promotion.candidate())
            .and_then(|member| member.address())
            .map(str::to_owned)
        else {
            continue;
        };
        let request = PromoteRequest {
            database: name.to_string(),
            epoch: epoch.get(),
            replication_factor: u32::from(database.replication_factor().get()),
        };
        let candidate = promotion.candidate().clone();
        let peers = Arc::clone(peers);
        tokio::spawn(async move {
            let Ok(channel) = peers.channel_to(&address) else {
                return;
            };
            let sent =
                tokio::time::timeout(interval, replication_client(channel).promote(request)).await;
            match sent {
                Ok(Ok(_)) => {}
                Ok(Err(status)) => {
                    tracing::debug!(%candidate, %status, "candidate refused a promotion");
                }
                Err(_) => {
                    peers.forget(&address);
                    tracing::debug!(%candidate, "candidate didn't answer a promotion in time");
                }
            }
        });
    }
    *notified = current;
}

fn millis(duration: Duration) -> Millis {
    Millis::new(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use fleetring_control::{
        ControlConfig, Engine, ExpiryChange, Feature, Member, MetadataChange, MetadataSpec,
        Rejection, Release,
    };

    use super::*;
    use crate::clock::SimClock;
    use crate::control::{ControlNode, ControlOptions, StorageIo};

    const BASE: Timestamp = Timestamp::from_millis(1_000_000);

    async fn node(dir: &std::path::Path) -> ControlNode {
        let options = ControlOptions::new(ControlConfig::new()).with_storage_io(StorageIo::Inline);
        let node = crate::control::test_support::open_node(
            dir.join("control.db"),
            options,
            SimClock::new(BASE),
        )
        .await
        .expect("a new log opens");
        let member =
            Member::new("data-1".parse().expect("an id")).with_software(crate::software::running());
        node.handle()
            .submit(Action::Join { member })
            .await
            .expect("the join commits");
        node
    }

    async fn create_expiring(handle: &ControlHandle, name: &str, expires_at: Timestamp) {
        let created = handle
            .submit(Action::CreateDatabase {
                name: name.parse().expect("a name"),
                replication_factor: fleetring::ReplicationFactor::ONE,
                engine: Engine::Sqlite,
                durability: None,
                metadata: Some(MetadataSpec {
                    expires_at: Some(expires_at),
                    ..MetadataSpec::default()
                }),
                incarnation: fleetring_control::Incarnation::LEGACY,
                incarnation_rule: fleetring_control::IncarnationRule::OnceForked,
            })
            .await
            .expect("the creation commits");
        assert!(created.outcome().is_ok(), "{:?}", created.outcome());
    }

    /// A command refused before it is logged, as one needing a feature a member lacks, is
    /// skipped; so is one `apply` rejects. Only a stopped control node ends the sweep.
    #[test]
    fn a_refused_expiry_does_not_stop_the_sweep() {
        let database: DatabaseName = "scratch".parse().expect("a name");
        let refused = SubmitError::Refused(Rejection::FeatureUnsupported {
            feature: Feature::Expiry,
            id: "old".parse().expect("an id"),
            release: Release::new(0, 5, 0),
        });
        assert_eq!(expired(&database, Err(refused)), Sweep::Continue);
        assert_eq!(expired(&database, Err(SubmitError::Stopped)), Sweep::Stop);
    }

    /// The sweep submits what it read; a database extended since is rejected as not expired
    /// and kept, and the sweep goes on to expire the others.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_expires_what_is_due_and_keeps_what_was_extended_since() {
        let dir = tempfile::tempdir().expect("a directory");
        let node = node(dir.path()).await;
        let handle = node.handle();
        let expires_at = BASE.saturating_add(Millis::new(61_000));
        for name in ["a", "b", "c"] {
            create_expiring(&handle, name, expires_at).await;
        }
        tokio::time::sleep(Duration::from_secs(62)).await;
        let due = handle
            .state()
            .expired_databases(BASE.saturating_add(Millis::new(62_000)));
        assert_eq!(due.len(), 3);
        let extend = Action::UpdateDatabase {
            name: "b".parse().expect("a name"),
            change: MetadataChange {
                expiry: ExpiryChange::Set(BASE.saturating_add(Millis::new(3_600_000))),
                ..MetadataChange::default()
            },
            by: None,
            if_version: None,
        };
        let extended = handle.submit(extend).await.expect("the update commits");
        assert!(extended.outcome().is_ok(), "{:?}", extended.outcome());

        assert_eq!(sweep(&handle, due, None).await, Sweep::Continue);
        let state = handle.state();
        let left: Vec<&str> = state
            .databases()
            .map(|database| database.name().as_str())
            .collect();
        assert_eq!(left, ["b"]);
        // A sweep after the control node stopped ends.
        node.shutdown().await;
        assert_eq!(
            sweep(&handle, vec!["b".parse().expect("a name")], None).await,
            Sweep::Stop
        );
    }

    /// A `RecordDelegation` from root `agent-42`'s token, for a child that lasts a minute from
    /// `now`.
    fn delegation(now: Timestamp) -> Action {
        use fleetring_control::delegation::{
            CallerToken, CanonicalRequest, DelegationId, TokenClaims, TokenKind,
        };
        let parse = |texts: &[&str]| -> fleetring_control::scope::Scopes {
            texts
                .iter()
                .map(|text| text.parse().expect("a scope"))
                .collect()
        };
        let scopes = parse(&["admin:agent-42.c7-*"]);
        // The root may hand a child what it grants: `apply` holds a root's child to them.
        let held = parse(&["admin:agent-42.*", "grant:agent-42.*"]);
        let start = now.as_millis() - now.as_millis() % 1_000;
        let caller = CallerToken {
            claims: TokenClaims {
                fleet: "prod".into(),
                principal: "agent-42".parse().expect("a name"),
                id: "root-jti".parse().expect("an id"),
                issued_at: Some(Timestamp::ZERO),
                not_before: Timestamp::ZERO,
                expires_at: Timestamp::MAX,
                scopes: held,
                kind: Some(TokenKind::Agent),
                owner: Some("sam".parse().expect("a name")),
                parent: None,
                chain: Vec::new(),
            },
            kid: "authority".parse().expect("a key id"),
        };
        Action::RecordDelegation {
            operation: fork::OperationId::from_bytes([1; 16]),
            requested_by: caller.claims.principal.clone(),
            request: CanonicalRequest {
                segment: "c7".into(),
                scopes: Some(scopes.clone()),
                lifetime: None,
                not_before: None,
                caller_token: caller.claims.id.clone(),
            },
            caller,
            id: DelegationId::from_bytes([7; 16]),
            scopes,
            not_before: Timestamp::from_millis(start),
            expires_at: Timestamp::from_millis(start + 60_000),
        }
    }

    /// The timer proposes a `Tick` once a delegation record's `kept_until` passes, though a
    /// command arrives more often than the tick interval and so the idle tick never comes
    /// (ADR-0047, point 6.2).
    #[tokio::test(start_paused = true)]
    async fn timer_ticks_when_a_kept_until_passes_on_a_busy_control_plane() {
        let dir = tempfile::tempdir().expect("a directory");
        let options = ControlOptions::new(ControlConfig::new()).with_storage_io(StorageIo::Inline);
        let clock = SimClock::new(BASE);
        let node =
            crate::control::test_support::open_node(dir.path().join("control.db"), options, clock)
                .await
                .expect("a new log opens");
        let handle = node.handle();
        let software = Feature::ALL.into_iter().fold(
            fleetring_control::Software::new(Release::new(0, 8, 0), 1),
            fleetring_control::Software::with_feature,
        );
        let member = Member::new("data-1".parse().expect("an id")).with_software(software);
        handle
            .submit(Action::Join { member })
            .await
            .expect("the join commits");
        let recorded = handle
            .submit(delegation(clock.now()))
            .await
            .expect("the delegation commits");
        assert!(recorded.outcome().is_ok(), "{:?}", recorded.outcome());
        assert_eq!(handle.state().delegations().len(), 1);
        let kept_until = handle
            .state()
            .delegations()
            .map(|(_, record)| record.kept_until)
            .next()
            .expect("a record");

        let hour = Duration::from_secs(3_600);
        let timer = Failover::start(
            handle.clone(),
            Arc::new(clock),
            Network::tcp(crate::transport::TransportConfig::default()),
            FailoverSettings::new().with_tick_interval(hour),
        );
        // A heartbeat every half hour keeps the control plane busier than the tick interval.
        while clock.now() <= kept_until.saturating_add(Millis::new(3_600_000)) {
            let heartbeat = Action::Heartbeat {
                id: "data-1".parse().expect("an id"),
                replicas: Vec::new(),
                unknown_reports: fleetring_control::UnknownReports::Skip,
                software: None,
                disk_free_bytes: None,
            };
            handle
                .submit(heartbeat)
                .await
                .expect("the heartbeat commits");
            tokio::time::sleep(hour / 2).await;
        }
        assert_eq!(handle.state().delegations().len(), 0);
        timer.stop().await;
        node.shutdown().await;
    }

    /// The 0.9.0 fixture's commands, as 0.9.0's single writer logged them.
    fn fixture() -> Vec<fleetring_control::Command> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/control-0.9.0/commands.json");
        let text = std::fs::read_to_string(path).expect("readable");
        serde_json::from_str(&text).expect("decodes")
    }

    /// A clock that follows `tokio::time` from the fixture's first stamp, and that a test can
    /// step forward, as an operator's or NTP's correction steps a wall clock.
    #[derive(Debug, Clone)]
    struct SteppedClock {
        clock: SimClock,
        step: Arc<std::sync::atomic::AtomicU64>,
    }

    impl SteppedClock {
        fn new(base: Timestamp) -> Self {
            Self {
                clock: SimClock::new(base),
                step: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }
        }

        fn step(&self, by: Duration) {
            self.step
                .fetch_add(millis(by).get(), std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Clock for SteppedClock {
        fn now(&self) -> Timestamp {
            self.clock.now().saturating_add(Millis::new(
                self.step.load(std::sync::atomic::Ordering::SeqCst),
            ))
        }
    }

    /// A control node that has logged the fixture's actions through `upto` (enabling commands
    /// left out, the proposer adds them), each at its own time on `clock`, and the heartbeats
    /// among them, which keep the members live and their reports current.
    async fn fixture_node(
        dir: &std::path::Path,
        clock: SteppedClock,
        upto: usize,
    ) -> (ControlNode, Vec<Action>) {
        let commands = fixture();
        let base = commands[0].at;
        let started = tokio::time::Instant::now();
        let options = ControlOptions::new(ControlConfig::new()).with_storage_io(StorageIo::Inline);
        let node = crate::control::test_support::open_node(dir.join("control.db"), options, clock)
            .await
            .expect("opens");
        let mut heartbeats = Vec::new();
        for command in &commands[..=upto] {
            if matches!(
                command.action,
                Action::EnableHandover
                    | Action::EnableIncarnations
                    | Action::SetTombstoneWindow { .. }
            ) {
                continue;
            }
            let offset = command.at.as_millis().saturating_sub(base.as_millis());
            tokio::time::sleep_until(started + Duration::from_millis(offset)).await;
            node.handle()
                .submit(command.action.clone())
                .await
                .expect("logged as the writer logged it");
            if let Action::Heartbeat { id, .. } = &command.action {
                heartbeats.retain(
                    |beat| !matches!(beat, Action::Heartbeat { id: known, .. } if known == id),
                );
                heartbeats.push(command.action.clone());
            }
        }
        (node, heartbeats)
    }

    /// `heartbeats`, and one from each member that reports it follows, at a position, every
    /// leased database whose replica set holds it: candidates for a promotion.
    fn following(state: &State, mut heartbeats: Vec<Action>) -> Vec<Action> {
        for member in state.members() {
            let replicas: Vec<serde_json::Value> = state
                .databases()
                .filter(|database| {
                    database
                        .lease()
                        .is_some_and(|lease| lease.holder() != member.id())
                        && state
                            .replica_set(database.name())
                            .is_ok_and(|set| set.contains(member.id()))
                })
                .map(|database| {
                    serde_json::json!({
                        "database": database.name().as_str(),
                        "role": "follower",
                        "position": [0, 0, 0, 1],
                    })
                })
                .collect();
            if replicas.is_empty() {
                continue;
            }
            let heartbeat = serde_json::json!({
                "heartbeat": {
                    "id": member.id().as_str(),
                    "replicas": replicas,
                    "unknown_reports": "skip",
                }
            });
            heartbeats.push(serde_json::from_value(heartbeat).expect("a heartbeat"));
        }
        heartbeats
    }

    /// Keeps the members live for `span`: their last heartbeats again every second.
    async fn heartbeat_for(handle: &ControlHandle, heartbeats: &[Action], span: Duration) {
        let until = tokio::time::Instant::now() + span;
        while tokio::time::Instant::now() < until {
            for heartbeat in heartbeats {
                handle
                    .submit(heartbeat.clone())
                    .await
                    .expect("the heartbeat commits");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    fn promotions(handle: &ControlHandle) -> usize {
        handle
            .state()
            .databases()
            .filter(|database| database.promotion().is_some())
            .count()
    }

    /// The settle window of the default control configuration: a lease duration plus the
    /// failover margin.
    const WINDOW: Duration = Duration::from_secs(12);

    /// A leader that has just become ready starts no promotion for a settle window, however
    /// long its leases have looked expired, and starts them once the window has passed (ADR-0040
    /// point 6).
    #[tokio::test(start_paused = true)]
    async fn a_new_leader_waits_a_lease_duration_before_promoting() {
        let dir = tempfile::tempdir().expect("a directory");
        let clock = SteppedClock::new(fixture()[0].at);
        let (node, heartbeats) = fixture_node(dir.path(), clock.clone(), 27).await;
        let handle = node.handle();
        // Every lease expires, unrenewed, while nothing runs the timers.
        let heartbeats = following(&handle.state(), heartbeats);
        heartbeat_for(&handle, &heartbeats, WINDOW + Duration::from_secs(2)).await;
        assert!(!handle.state().promotions_due(clock.now()).is_empty());
        let timer = Failover::start(
            handle.clone(),
            Arc::new(clock.clone()),
            Network::tcp(crate::transport::TransportConfig::default()),
            FailoverSettings::new(),
        );
        heartbeat_for(&handle, &heartbeats, WINDOW - Duration::from_secs(1)).await;
        assert_eq!(promotions(&handle), 0, "no promotion inside the window");
        heartbeat_for(&handle, &heartbeats, Duration::from_secs(3)).await;
        assert!(promotions(&handle) > 0, "promotions after the window");
        timer.stop().await;
        node.shutdown().await;
    }

    /// A leader that has just become ready moves no fork and releases no kept replica set for a
    /// settle window, and does once it has passed (ADR-0040 point 6).
    #[tokio::test(start_paused = true)]
    async fn a_new_leader_waits_a_lease_duration_before_fork_actions_and_releases() {
        // The fork is seeded on every replica: the driver would complete it.
        let dir = tempfile::tempdir().expect("a directory");
        let clock = SteppedClock::new(fixture()[0].at);
        let (node, heartbeats) = fixture_node(dir.path(), clock.clone(), 27).await;
        let handle = node.handle();
        let seeding = |handle: &ControlHandle| {
            handle.state().databases().any(|database| {
                database.fork().is_some_and(|fork| {
                    matches!(
                        fork.state(),
                        fleetring_control::fork::ForkState::Seeding { .. }
                    )
                })
            })
        };
        assert!(seeding(&handle));
        assert!(!fork::actions(&handle.state(), clock.now()).is_empty());
        let timer = Failover::start(
            handle.clone(),
            Arc::new(clock.clone()),
            Network::tcp(crate::transport::TransportConfig::default()),
            FailoverSettings::new(),
        );
        heartbeat_for(&handle, &heartbeats, WINDOW - Duration::from_secs(1)).await;
        assert!(seeding(&handle), "no fork action inside the window");
        heartbeat_for(&handle, &heartbeats, Duration::from_secs(3)).await;
        assert!(!seeding(&handle), "the fork moved on after the window");
        timer.stop().await;
        node.shutdown().await;

        // A kept replica set whose primary reports the new placement's quorum holds its log.
        let dir = tempfile::tempdir().expect("a directory");
        let clock = SteppedClock::new(fixture()[0].at);
        let (node, heartbeats) = fixture_node(dir.path(), clock.clone(), 48).await;
        let handle = node.handle();
        assert!(!handle.state().releases_due().is_empty());
        let timer = Failover::start(
            handle.clone(),
            Arc::new(clock.clone()),
            Network::tcp(crate::transport::TransportConfig::default()),
            FailoverSettings::new(),
        );
        heartbeat_for(&handle, &heartbeats, WINDOW - Duration::from_secs(1)).await;
        assert!(
            !handle.state().releases_due().is_empty(),
            "no release inside the window"
        );
        heartbeat_for(&handle, &heartbeats, Duration::from_secs(3)).await;
        assert!(
            handle.state().releases_due().is_empty(),
            "the kept set was released after the window"
        );
        timer.stop().await;
        node.shutdown().await;
    }

    /// The settle window is measured on the monotonic clock: a wall clock that steps an hour
    /// forward as the leader becomes ready makes every lease look expired at once, and still no
    /// promotion starts before the window has passed.
    #[tokio::test(start_paused = true)]
    async fn a_wall_clock_step_does_not_shorten_the_settle_window() {
        let dir = tempfile::tempdir().expect("a directory");
        let clock = SteppedClock::new(fixture()[0].at);
        let (node, heartbeats) = fixture_node(dir.path(), clock.clone(), 27).await;
        let handle = node.handle();
        let heartbeats = following(&handle.state(), heartbeats);
        let timer = Failover::start(
            handle.clone(),
            Arc::new(clock.clone()),
            Network::tcp(crate::transport::TransportConfig::default()),
            FailoverSettings::new(),
        );
        clock.step(Duration::from_secs(3_600));
        heartbeat_for(&handle, &heartbeats, Duration::from_secs(1)).await;
        assert!(!handle.state().promotions_due(clock.now()).is_empty());
        heartbeat_for(&handle, &heartbeats, WINDOW - Duration::from_secs(2)).await;
        assert_eq!(promotions(&handle), 0, "no promotion inside the window");
        heartbeat_for(&handle, &heartbeats, Duration::from_secs(3)).await;
        assert!(promotions(&handle) > 0, "promotions after the window");
        timer.stop().await;
        node.shutdown().await;
    }

    /// The timers run only on a ready leader: a node that leads nothing, as one waiting to join
    /// a group, ticks nothing however long it waits, and once it becomes ready they run. (A
    /// one-voter group can't lose leadership to a peer; how each answer pauses or ends them is
    /// `only_stopped_and_storage_end_the_timers`.)
    #[tokio::test(start_paused = true)]
    async fn the_timers_pause_on_not_leader_and_resume_at_readiness() {
        let dir = tempfile::tempdir().expect("a directory");
        let options = ControlOptions::new(ControlConfig::new())
            .with_storage_io(StorageIo::Inline)
            .with_bootstrap(crate::control::Bootstrap::Join);
        let clock = SimClock::new(BASE);
        let joining =
            crate::control::test_support::open_node(dir.path().join("joining.db"), options, clock)
                .await
                .expect("opens");
        let handle = joining.handle();
        let timer = Failover::start(
            handle.clone(),
            Arc::new(clock),
            Network::tcp(crate::transport::TransportConfig::default()),
            FailoverSettings::new(),
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(handle.applied().get(), 0, "no tick while not leading");
        let peer = crate::control::test_support::own();
        let id = crate::control::raft::raft_id(&peer.node_id);
        let membership = crate::control::raft::Membership::new(
            vec![std::collections::BTreeSet::from([id])],
            std::collections::BTreeMap::from([(id, peer)]),
        )
        .expect("a membership");
        handle
            .raft()
            .initialize(membership)
            .await
            .expect("initialized");
        handle.raft().trigger().elect(false).await.expect("elects");
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(handle.state().now() > BASE, "the timers tick once it leads");
        timer.stop().await;
        joining.shutdown().await;
    }

    /// A timer command refused for leadership while the node stays ready under the same leader
    /// pauses the timers for a tick only: they resume, and keep the settle window, so promotions
    /// start when the window that began at readiness ends, not a window later.
    #[tokio::test(start_paused = true)]
    async fn a_refused_timer_command_pauses_the_timers_for_a_tick_and_keeps_the_window() {
        let dir = tempfile::tempdir().expect("a directory");
        let clock = SteppedClock::new(fixture()[0].at);
        let (node, heartbeats) = fixture_node(dir.path(), clock.clone(), 27).await;
        let handle = node.handle();
        let heartbeats = following(&handle.state(), heartbeats);
        heartbeat_for(&handle, &heartbeats, WINDOW + Duration::from_secs(2)).await;
        let before = handle.leadership();
        let timer = Failover::start(
            handle.clone(),
            Arc::new(clock.clone()),
            Network::tcp(crate::transport::TransportConfig::default()),
            FailoverSettings::new(),
        );
        heartbeat_for(&handle, &heartbeats, Duration::from_secs(6)).await;
        handle
            .faults()
            .refuse
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let ticked = handle.state().now();
        heartbeat_for(&handle, &heartbeats, Duration::from_secs(2)).await;
        assert_eq!(
            handle
                .faults()
                .refuse
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a timer command was refused"
        );
        assert_eq!(handle.leadership(), before, "the same leader throughout");
        assert!(handle.state().now() > ticked);
        assert_eq!(promotions(&handle), 0, "no promotion inside the window");
        heartbeat_for(
            &handle,
            &heartbeats,
            WINDOW - Duration::from_secs(8) + Duration::from_secs(2),
        )
        .await;
        assert!(
            promotions(&handle) > 0,
            "promotions once the window that began at readiness ended"
        );
        timer.stop().await;
        node.shutdown().await;
    }

    /// Only a stopped node or a storage failure ends the timers; a lost leadership pauses them,
    /// and a refused command is skipped (ADR-0040 point 6).
    #[test]
    fn only_stopped_and_storage_end_the_timers() {
        assert_eq!(Sweep::after(&SubmitError::Stopped), Sweep::Stop);
        assert_eq!(
            Sweep::after(&SubmitError::Storage(crate::control::LogError::Exhausted)),
            Sweep::Stop
        );
        for paused in [
            SubmitError::NotLeader { leader: None },
            SubmitError::CatchingUp { leader: None },
            SubmitError::Superseded,
        ] {
            assert_eq!(Sweep::after(&paused), Sweep::Pause, "{paused:?}");
        }
        let refused = SubmitError::Refused(Rejection::UnknownMember {
            id: "x".parse().expect("an id"),
        });
        assert_eq!(Sweep::after(&refused), Sweep::Continue);
    }
}
