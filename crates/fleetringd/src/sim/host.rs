//! One simulated host's lifecycle, and the state hosts and clients share with the test.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use fleetring_control::{Liveness, Timestamp};
use tokio::sync::watch;

use crate::clock::SimClock;
use crate::control::ControlHandle;
use crate::sim::seeds::millis;
use crate::sim::trace::{Event, EventKind, Trace};
use crate::sim::workload::History;
use crate::{Config, Daemon, Environment, Membership};

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
}

#[derive(Debug, Default)]
pub(super) struct Inner {
    pub(super) trace: Trace,
    pub(super) history: History,
    pub(super) running: BTreeMap<String, Running>,
    pub(super) stops: BTreeMap<String, watch::Sender<u64>>,
    pub(super) workloads: BTreeMap<String, WorkloadState>,
}

/// What a running host exposes.
#[derive(Debug, Clone)]
pub(super) struct Running {
    pub(super) control: Option<ControlHandle>,
    pub(super) membership: Option<Membership>,
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

    pub(super) fn control_handle(&self) -> Option<ControlHandle> {
        self.lock()
            .running
            .values()
            .find_map(|running| running.control.clone())
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

/// One incarnation of a host: starts the daemon, publishes its handles, and serves until the
/// test stops it or turmoil crashes it.
pub(super) async fn run_host(name: String, config: Config, shared: Shared) -> turmoil::Result {
    let mut stop = shared
        .stop_signal(&name)
        .ok_or_else(|| format!("no stop signal for {name}"))?;
    let elapsed = turmoil::sim_elapsed().unwrap_or_default();
    let clock = SimClock::new(Timestamp::from_millis(millis(elapsed)));
    let daemon = Daemon::start_with(config, Environment::simulated(clock)).await?;
    let membership = daemon.membership();
    shared.lock().running.insert(
        name.clone(),
        Running {
            control: daemon.control(),
            membership: membership.clone(),
        },
    );
    shared.record(&name, EventKind::Started);
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
