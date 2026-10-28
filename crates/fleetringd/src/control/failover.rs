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
//!
//! A control node that has just started gives primaries one lease duration plus the failover
//! margin to renew before it starts any promotion: leases look expired after an outage only
//! because nobody could renew them, and the primaries kept serving meanwhile (ADR-0018,
//! point 6). None of this affects safety, which comes from epochs and quorums.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::{Action, Applied, DatabaseName, LeaseEpoch, Millis, State, Timestamp};
use fleetring_proto::v1::PromoteRequest;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::clock::Clock;
use crate::control::node::ControlHandle;
use crate::environment::Network;
use crate::transport::{Peers, replication_client};

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

/// The running failover timer of a control node. Dropping it stops it.
#[derive(Debug)]
pub struct Failover {
    task: JoinHandle<()>,
}

impl Failover {
    /// Starts the timer for the control node behind `handle`, reading time from `clock` and
    /// reaching candidates over `network`.
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
        let task = tokio::spawn(run(handle, clock, network, settings));
        Self { task }
    }

    /// Stops the timer and waits for it to end.
    pub async fn stop(mut self) {
        self.task.abort();
        // Aborted, so it can only end cancelled.
        let _stopped = (&mut self.task).await;
    }
}

impl Drop for Failover {
    fn drop(&mut self) {
        self.task.abort();
    }
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
