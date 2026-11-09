//! The control node's failover timer: ticks, expired leases, and promotions (ADR-0018).
//!
//! Liveness and lease expiry are judged at the time of the last applied command, so on its own
//! the state machine notices nothing while no command arrives. The timer keeps it moving:
//!
//! 1. **Ticks.** When no command has arrived for a whole tick interval, it submits
//!    [`Action::Tick`], which only moves the state's time on.
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
//!
//! A control node that has just started gives primaries one lease duration plus the failover
//! margin to renew before it starts any promotion: leases look expired after an outage only
//! because nobody could renew them, and the primaries kept serving meanwhile (ADR-0018,
//! point 6). None of this affects safety, which comes from epochs and quorums.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::{
    Action, Applied, DatabaseName, LeaseEpoch, Millis, State, Timestamp, fork,
};
use fleetring_proto::v1::PromoteRequest;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;

use crate::clock::Clock;
use crate::control::node::{Committed, ControlHandle, SubmitError};
use crate::control::notices::DropNotices;
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
    sweep: JoinHandle<()>,
}

impl Failover {
    /// Starts the timer and the expiry sweep for the control node behind `handle`, reading time
    /// from `clock` and reaching candidates over `network`.
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
        let sweep = spawn_expiry_sweep(handle.clone(), Arc::clone(&clock), Some(notices));
        let task = tokio::spawn(run(handle, clock, network, settings));
        Self { task, sweep }
    }

    /// Stops the timer and the sweep, and waits for them to end.
    pub async fn stop(mut self) {
        self.task.abort();
        self.sweep.abort();
        // Aborted, so each can only end cancelled.
        let _stopped = (&mut self.task).await;
        let _swept = (&mut self.sweep).await;
    }
}

impl Drop for Failover {
    fn drop(&mut self) {
        self.task.abort();
        self.sweep.abort();
    }
}

/// Starts the expiry sweep for the control node behind `handle`, reading time from `clock`:
/// every [`EXPIRY_SWEEP`], it expires the databases whose expiry has passed, as
/// [`sweep`] does, and tells their replicas through `notices`. The task ends once the control
/// node stops.
pub(crate) fn spawn_expiry_sweep(
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
            if sweep(&handle, due, notices.as_ref()).await == Sweep::Stop {
                return;
            }
        }
    })
}

/// Whether the expiry sweep goes on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sweep {
    /// It sweeps again at its next interval.
    Continue,
    /// The control node has stopped, so it ends.
    Stop,
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
        if expired(&name, result) == Sweep::Stop {
            next = Sweep::Stop;
        }
    }
    next
}

/// Logs what became of the expiry of `database`, and says whether the sweep goes on: it stops
/// only with the control node. A command refused before it was logged, or rejected by `apply`,
/// is skipped, and a storage failure is retried at the next sweep.
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
    }
    Sweep::Continue
}

async fn run(
    handle: ControlHandle,
    clock: Arc<dyn Clock>,
    network: Network,
    settings: FailoverSettings,
) {
    let peers = Arc::new(Peers::with_network(network));
    let started = clock.now();
    let tick = millis(settings.tick_interval);
    let mut notified: BTreeMap<DatabaseName, (LeaseEpoch, Timestamp)> = BTreeMap::new();
    let mut ticks = tokio::time::interval(settings.tick_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let now = clock.now();
        if now
            .as_millis()
            .saturating_sub(handle.state().now().as_millis())
            >= tick.get()
            && handle.submit(Action::Tick).await.is_err()
        {
            return;
        }
        let state = handle.state();
        let config = state.config();
        let settled = started.saturating_add(
            config
                .lease_duration()
                .saturating_add(config.failover_margin()),
        );
        if now > settled {
            for database in state.promotions_due(now) {
                match handle
                    .submit(Action::StartPromotion {
                        database: database.clone(),
                    })
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
                    Err(_) => return,
                }
            }
            if drive_forks(&handle, now).await == Sweep::Stop {
                return;
            }
        }
        notify_candidates(
            &handle.state(),
            now,
            &peers,
            settings.notify_interval,
            &mut notified,
        );
    }
}

/// Submits what the fork driver proposes at `now` ([`fork::actions`]), one command at a time,
/// and logs what became of each (ADR-0046, point 3.11). The driver reads only the state, so a
/// restarted control node proposes the same commands; `apply` checks each one again, so a
/// proposal made stale by an earlier one is refused and changes nothing. Says whether the timer
/// goes on: it stops only with the control node.
async fn drive_forks(handle: &ControlHandle, now: Timestamp) -> Sweep {
    for action in fork::actions(&handle.state(), now) {
        match handle.submit(action).await {
            Ok(committed) => match committed.outcome() {
                Ok(applied) => tracing::info!(?applied, "a fork moved on"),
                Err(rejection) => tracing::debug!(%rejection, "a fork proposal was refused"),
            },
            Err(SubmitError::Stopped) => return Sweep::Stop,
            Err(error) => {
                tracing::warn!(%error, "can't log a fork proposal; the next tick proposes again");
            }
        }
    }
    Sweep::Continue
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
        let node = ControlNode::open(dir.join("control.db"), options, SimClock::new(BASE))
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
}
