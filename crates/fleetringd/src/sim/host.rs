//! One simulated host's lifecycle, and the state hosts and clients share with the test.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use fleetring_control::{IncarnationRule, Liveness, Timestamp};
use tokio::sync::watch;

use crate::clock::{Clock, SimClock};
use crate::control::ControlHandle;
use crate::durability::ObjectStore;
use crate::observe::Metrics;
use crate::replication::ForkFaults;
use crate::replication::Replication;
use crate::router::{Router, RouterConfig};
use crate::sim::rng::SimEntropy;
use crate::sim::seeds::millis;
use crate::sim::trace::{Event, EventKind, Trace};
use crate::sim::workload::History;
use crate::tls::{Identity, Security};
use crate::{Config, Daemon, Environment, Membership, Network};

/// Whether a workload is still running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkloadState {
    Running,
    Done,
}

/// What hosts and clients share with the test, which reads it between steps.
#[derive(Debug, Clone, Default)]
pub(super) struct Shared {
    inner: Arc<Mutex<Inner>>,
    /// How far every host's clock is set ahead of simulated time, in milliseconds.
    clock_shift: Arc<AtomicU64>,
}

/// A host's clock: simulated time, set ahead by the fleet's shift ([`Shared::shift_clocks`]).
#[derive(Debug, Clone)]
struct ShiftedClock {
    inner: SimClock,
    shift: Arc<AtomicU64>,
}

impl Clock for ShiftedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(
            self.inner
                .now()
                .as_millis()
                .saturating_add(self.shift.load(Ordering::SeqCst)),
        )
    }
}

#[derive(Debug, Default)]
pub(super) struct Inner {
    pub(super) trace: Trace,
    pub(super) history: History,
    pub(super) running: BTreeMap<String, Running>,
    pub(super) stops: BTreeMap<String, watch::Sender<u64>>,
    pub(super) workloads: BTreeMap<String, WorkloadState>,
    /// The object stores every data host's archives write to, by archive name: outside the
    /// simulated network, shared by every host, and surviving every crash (ADR-0029, point 16).
    pub(super) object_stores: BTreeMap<String, Arc<dyn ObjectStore>>,
    /// The rule the control node logs creations under, when the test stands it in for one of a
    /// release before 0.8.1; `None` for this release's own.
    pub(super) incarnation_rule: Option<IncarnationRule>,
    /// Whether control hosts record each term they lead in the trace: in fleets with a control
    /// group, whose checks read it ([`check::one_leader_per_term`](crate::sim::check::one_leader_per_term)).
    pub(super) record_leaders: bool,
}

/// What a running host exposes.
#[derive(Debug, Clone)]
pub(super) struct Running {
    pub(super) control: Option<ControlHandle>,
    pub(super) membership: Option<Membership>,
    pub(super) replication: Option<Replication>,
    /// This incarnation's metrics: a restarted host starts counting from zero.
    pub(super) metrics: Metrics,
}

impl Shared {
    pub(super) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("simulation state lock poisoned")
    }

    /// Records an event from inside a host or client, at its simulated time.
    pub(super) fn record(&self, source: &str, kind: EventKind) {
        let at = turmoil::sim_elapsed().unwrap_or_default();
        self.lock().trace.push(Event::new(at, source, kind));
    }

    /// Sets every host's clock `by` further ahead of simulated time, as if that much time had
    /// passed between two steps.
    /// Returns how far ahead the clocks are then, in milliseconds.
    pub(super) fn shift_clocks(&self, by: std::time::Duration) -> u64 {
        let by = millis(by);
        self.clock_shift.fetch_add(by, Ordering::SeqCst) + by
    }

    /// Sets every host's clock `by` back, toward simulated time.
    /// Returns how far ahead the clocks are then, in milliseconds.
    pub(super) fn set_clocks_back(&self, by: std::time::Duration) -> u64 {
        let by = millis(by);
        // Only the test's driver changes the shift, between steps, so nothing races this.
        let shift = self.clock_shift.load(Ordering::SeqCst).saturating_sub(by);
        self.clock_shift.store(shift, Ordering::SeqCst);
        shift
    }

    /// How far ahead of simulated time every host's clock is, in milliseconds.
    pub(super) fn clock_shift(&self) -> u64 {
        self.clock_shift.load(Ordering::SeqCst)
    }

    fn clock(&self) -> ShiftedClock {
        let elapsed = turmoil::sim_elapsed().unwrap_or_default();
        ShiftedClock {
            inner: SimClock::new(Timestamp::from_millis(millis(elapsed))),
            shift: Arc::clone(&self.clock_shift),
        }
    }

    /// The handle of the running control node that is the control group's ready leader in the
    /// latest term, or of the first running control node while none is.
    pub(super) fn control_handle(&self) -> Option<ControlHandle> {
        let inner = self.lock();
        let handles = inner
            .running
            .values()
            .filter_map(|running| running.control.as_ref());
        let first = handles.clone().next();
        handles
            .filter_map(|handle| match handle.leadership() {
                crate::control::Readiness::Ready { leader } => Some((leader.term, handle)),
                crate::control::Readiness::Follower { .. }
                | crate::control::Readiness::CatchingUp { .. } => None,
            })
            .max_by_key(|(term, _)| *term)
            .map(|(_, handle)| handle)
            .or(first)
            .cloned()
    }

    /// The handle of the control node on host `name`, while it runs.
    pub(super) fn control_handle_of(&self, name: &str) -> Option<ControlHandle> {
        self.lock()
            .running
            .get(name)
            .and_then(|running| running.control.clone())
    }

    pub(super) fn signal_stop(&self, host: &str) {
        if let Some(stop) = self.lock().stops.get(host) {
            stop.send_modify(|generation| *generation += 1);
        }
    }

    pub(super) fn stop_signal(&self, host: &str) -> Option<watch::Receiver<u64>> {
        self.lock().stops.get(host).map(|stop| {
            let mut receiver = stop.subscribe();
            receiver.mark_unchanged();
            receiver
        })
    }
}

/// One incarnation of the router host: starts the router with `identity`, toward the fleet, and
/// serves until the test stops it or turmoil crashes it. Each incarnation draws its batons'
/// secrets from `seed` and the time it starts, so that a restarted router never repeats one.
pub(super) async fn run_router(
    name: String,
    config: RouterConfig,
    identity: Identity,
    seed: u64,
    shared: Shared,
) -> turmoil::Result {
    let mut stop = shared
        .stop_signal(&name)
        .ok_or_else(|| format!("no stop signal for {name}"))?;
    let elapsed = turmoil::sim_elapsed().unwrap_or_default();
    let clock = SimClock::new(Timestamp::from_millis(millis(elapsed)));
    let environment = Environment::simulated(clock)
        .with_network(Network::simulated().with_security(Security::Tls(identity)));
    let entropy = Arc::new(SimEntropy::new(seed ^ millis(elapsed)));
    let router = Router::start_with(config, environment, entropy).await?;
    shared.record(&name, EventKind::Started);
    // A dropped sender also means stop.
    let _stopped = stop.changed().await;
    router.stop().await;
    shared.record(&name, EventKind::Stopped);
    Ok(())
}

/// One incarnation of a host: starts the daemon with `identity`, publishes its handles, and
/// serves until the test stops it or turmoil crashes it. Each incarnation draws its randomness,
/// a control node's incarnations of databases among it, from `seed` and the time it starts, so
/// that a seed replays the same values and a restarted host never repeats them.
pub(super) async fn run_host(
    name: String,
    config: Config,
    identity: Identity,
    seed: u64,
    shared: Shared,
    faults: ForkFaults,
) -> turmoil::Result {
    let mut stop = shared
        .stop_signal(&name)
        .ok_or_else(|| format!("no stop signal for {name}"))?;
    let elapsed = turmoil::sim_elapsed().unwrap_or_default();
    let mut environment = Environment::simulated(shared.clock())
        .with_network(Network::simulated().with_security(Security::Tls(identity)))
        .with_entropy(Arc::new(SimEntropy::new(seed ^ millis(elapsed))));
    if let Some(rule) = shared.lock().incarnation_rule {
        environment = environment.with_incarnation_rule(rule);
    }
    let stores = shared.lock().object_stores.clone();
    for (archive, store) in stores {
        environment = environment.with_object_store(archive, store);
    }
    #[cfg(feature = "fault-hooks")]
    {
        environment = environment.with_fork_faults(faults);
    }
    #[cfg(not(feature = "fault-hooks"))]
    let _ = faults;
    let daemon = Daemon::start_with(config, environment).await?;
    let membership = daemon.membership();
    shared.lock().running.insert(
        name.clone(),
        Running {
            control: daemon.control(),
            membership: membership.clone(),
            replication: daemon.replication(),
            metrics: daemon.metrics().clone(),
        },
    );
    shared.record(&name, EventKind::Started);
    let record_leaders = shared.lock().record_leaders;
    if let Some(control) = daemon.control().filter(|_| record_leaders) {
        let shared = shared.clone();
        let name = name.clone();
        let mut leadership = control.watch_leadership();
        tokio::spawn(async move {
            loop {
                let current = leadership.borrow_and_update().clone();
                if let crate::control::Readiness::Ready { leader } = current {
                    shared.record(
                        &name,
                        EventKind::Leader {
                            term: leader.term,
                            leader: name.clone(),
                        },
                    );
                }
                if leadership.changed().await.is_err() {
                    return;
                }
            }
        });
    }
    if let Some(mut membership) = membership {
        let shared = shared.clone();
        let name = name.clone();
        tokio::spawn(async move {
            while let Some(view) = membership.changed().await {
                let down = view
                    .members()
                    .filter(|(_, liveness)| *liveness == Liveness::Down)
                    .map(|(member, _)| member.id().to_string())
                    .collect();
                shared.record(
                    &name,
                    EventKind::Published {
                        epoch: view.epoch().get(),
                        fingerprint: view.fingerprint(),
                        down,
                    },
                );
            }
        });
    }
    daemon
        .run_until(async move {
            // A dropped sender also means stop.
            let _stopped = stop.changed().await;
        })
        .await;
    shared.lock().running.remove(&name);
    shared.record(&name, EventKind::Stopped);
    Ok(())
}
