//! FR-029: membership, heartbeats, and ring publication, under turmoil (ADR-0020).
//!
//! One control host and three data hosts. The test joins the data nodes, partitions one from the
//! control node, crashes one, removes it, restarts it, and takes the control node down, and
//! checks after each event that every live node publishes the control plane's ring, and the
//! control plane's view of who is live, within two heartbeat intervals.
#![cfg(feature = "simulation")]

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::{Fingerprint, NodeId};
use fleetring_control::{Action, ControlConfig, Liveness, Member, Millis, State, Timestamp};
use fleetringd::clock::SimClock;
use fleetringd::control::{
    ControlClient, ControlHandle, ControlNode, ControlOptions, ControlService, StorageIo,
};
use fleetringd::transport::{Backoff, control_server, sim};
use fleetringd::{Membership, MembershipSettings, MembershipView};

const CONTROL: &str = "control";
const OPERATOR: &str = "operator";
const PORT: u16 = 7400;
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const INTERVAL: Duration = Duration::from_millis(200);
const TIMEOUT: Duration = Duration::from_millis(1_000);
/// Every acceptance bound: two heartbeat intervals.
const BOUND: Duration = Duration::from_millis(400);

type TestResult = Result<(), Box<dyn Error>>;

/// What the hosts share with the test, which reads it between simulation steps.
#[derive(Debug, Clone, Default)]
struct Shared {
    control: Arc<Mutex<Option<ControlHandle>>>,
    nodes: Arc<Mutex<BTreeMap<String, Membership>>>,
    leaves: Arc<Mutex<Vec<NodeId>>>,
    trace: Arc<Mutex<Trace>>,
}

/// What a run did: its events, and what each node published, each in order.
///
/// Simulated times are left out. turmoil seeds the network, but Tokio's own random choices, such
/// as which ready branch of a `select!` in tonic's server runs first, are seeded only under
/// `--cfg tokio_unstable`, so they can shift an event by a few milliseconds between runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Trace {
    events: Vec<String>,
    published: BTreeMap<String, Vec<String>>,
}

impl Shared {
    /// The control node's state, or `None` while it is down.
    fn state(&self) -> Option<Arc<State>> {
        let control = self.control.lock().expect("lock");
        control.as_ref().map(ControlHandle::state)
    }

    fn record(&self, event: &str) {
        self.trace
            .lock()
            .expect("lock")
            .events
            .push(event.to_owned());
    }

    fn published(&self, node: &str, view: &MembershipView) {
        let down: Vec<String> = view
            .members()
            .filter(|(_, liveness)| *liveness == Liveness::Down)
            .map(|(member, _)| member.id().to_string())
            .collect();
        let line = format!(
            "epoch {} ring {:?} down {down:?}",
            view.epoch().get(),
            view.fingerprint().map(Fingerprint::get),
        );
        let mut trace = self.trace.lock().expect("lock");
        trace
            .published
            .entry(node.to_owned())
            .or_default()
            .push(line);
    }
}

fn millis(duration: Duration) -> Result<u64, Box<dyn Error>> {
    Ok(u64::try_from(duration.as_millis())?)
}

fn control_host(sim: &mut turmoil::Sim<'_>, path: PathBuf, shared: Shared) {
    sim.host(CONTROL, move || {
        let path = path.clone();
        let shared = shared.clone();
        async move {
            let elapsed = turmoil::sim_elapsed().unwrap_or_default();
            let config = ControlConfig::new().with_heartbeat_timeout(Millis::new(millis(TIMEOUT)?));
            let options = ControlOptions::new(config).with_storage_io(StorageIo::Inline);
            let clock = SimClock::new(Timestamp::from_millis(millis(elapsed)?));
            let node = ControlNode::open(path, options, clock).await?;
            *shared.control.lock().expect("lock") = Some(node.handle());
            let listener = turmoil::net::TcpListener::bind(("0.0.0.0", PORT)).await?;
            tonic::transport::Server::builder()
                .add_service(control_server(ControlService::new(node.handle())))
                .serve_with_incoming(sim::incoming(listener))
                .await?;
            Ok(())
        }
    });
}

fn data_host(sim: &mut turmoil::Sim<'_>, id: &'static str, shared: Shared) {
    sim.host(id, move || {
        let shared = shared.clone();
        async move {
            let backoff = Backoff {
                initial: Duration::from_millis(20),
                max: Duration::from_millis(100),
                attempts: 3,
            };
            let client =
                ControlClient::with_connector(format!("{CONTROL}:{PORT}"), sim::channel_to)
                    .with_backoff(backoff);
            let settings = MembershipSettings {
                heartbeat_interval_ms: NonZeroU64::new(millis(INTERVAL)?).ok_or("zero")?,
            };
            let (mut membership, _tasks) =
                Membership::start(Member::new(id.parse()?), client, &settings);
            shared
                .nodes
                .lock()
                .expect("lock")
                .insert(id.to_owned(), membership.clone());
            while let Some(view) = membership.changed().await {
                shared.published(id, &view);
            }
            Ok(())
        }
    });
}

/// Submits the leaves the test asks for, as an operator would.
fn operator(sim: &mut turmoil::Sim<'_>, shared: Shared) {
    sim.client(OPERATOR, async move {
        loop {
            let leaves: Vec<NodeId> = shared.leaves.lock().expect("lock").drain(..).collect();
            for id in leaves {
                let handle = shared
                    .control
                    .lock()
                    .expect("lock")
                    .clone()
                    .ok_or("no control")?;
                handle.submit(Action::Leave { id }).await?.into_outcome()?;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
}

/// Steps the simulation for `duration`.
fn advance(sim: &mut turmoil::Sim<'_>, duration: Duration) -> TestResult {
    let end = sim.elapsed() + duration;
    while sim.elapsed() < end {
        sim.step()?;
    }
    Ok(())
}

/// Steps the simulation until `done` holds, and returns how long that took in simulated time.
fn step_until(
    sim: &mut turmoil::Sim<'_>,
    limit: Duration,
    what: &str,
    mut done: impl FnMut() -> Result<bool, Box<dyn Error>>,
) -> Result<Duration, Box<dyn Error>> {
    let start = sim.elapsed();
    loop {
        if done()? {
            return Ok(sim.elapsed() - start);
        }
        if sim.elapsed() - start > limit {
            return Err(format!("{what}: not done within {limit:?}").into());
        }
        sim.step()?;
    }
}

/// Whether every node in `nodes` publishes the control plane's epoch and ring, and agrees with
/// it on every member's liveness.
fn converged(shared: &Shared, nodes: &[&str]) -> Result<bool, Box<dyn Error>> {
    let Some(state) = shared.state() else {
        return Ok(false);
    };
    let fingerprint = state.ring().ok().map(|ring| ring.fingerprint());
    let memberships = shared.nodes.lock().expect("lock");
    for id in nodes {
        let Some(membership) = memberships.get(*id) else {
            return Ok(false);
        };
        let view = membership.view();
        if view.epoch() != state.membership_epoch() || view.fingerprint() != fingerprint {
            return Ok(false);
        }
        for member in state.members() {
            if view.liveness(member.id()) != state.liveness(member.id(), state.now()) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn liveness(state: &State, id: &str) -> Result<Option<Liveness>, Box<dyn Error>> {
    Ok(state.liveness(&id.parse()?, state.now()))
}

/// Waits for the control plane to reach `condition`, then checks that `nodes` converge on it
/// within two heartbeat intervals.
fn after_event(
    sim: &mut turmoil::Sim<'_>,
    shared: &Shared,
    event: &str,
    limit: Duration,
    nodes: &[&str],
    mut condition: impl FnMut(&State) -> Result<bool, Box<dyn Error>>,
) -> TestResult {
    step_until(sim, limit, event, || match shared.state() {
        Some(state) => condition(&state),
        None => Ok(false),
    })?;
    shared.record(&format!("control: {event}"));
    let took = step_until(sim, BOUND, &format!("converging after {event}"), || {
        converged(shared, nodes)
    })?;
    assert!(took <= BOUND, "{event}: converged after {took:?}");
    shared.record(&format!("converged after {event}"));
    Ok(())
}

/// Runs the whole scenario and returns its trace.
fn scenario(seed: u64) -> Result<Trace, Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let shared = Shared::default();
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(120))
        .rng_seed(seed)
        .build();
    control_host(&mut sim, dir.path().join("control.db"), shared.clone());
    for id in DATA {
        data_host(&mut sim, id, shared.clone());
    }
    operator(&mut sim, shared.clone());

    // Joins.
    after_event(
        &mut sim,
        &shared,
        "three members",
        Duration::from_secs(5),
        &DATA,
        |state| Ok(state.membership_epoch().get() == 3),
    )?;

    // A partition: the control plane marks node-2 down, then up again when it heals.
    sim.partition("node-2", CONTROL);
    shared.record("partition node-2");
    let live_nodes = ["node-1", "node-3"];
    after_event(
        &mut sim,
        &shared,
        "node-2 down",
        TIMEOUT + BOUND,
        &live_nodes,
        |state| Ok(liveness(state, "node-2")? == Some(Liveness::Down)),
    )?;
    // Still down while the partition lasts.
    advance(&mut sim, TIMEOUT)?;
    let state = shared.state().ok_or("control is down")?;
    assert_eq!(liveness(&state, "node-2")?, Some(Liveness::Down));
    sim.repair("node-2", CONTROL);
    shared.record("repair node-2");
    after_event(
        &mut sim,
        &shared,
        "node-2 up",
        3 * INTERVAL,
        &DATA,
        |state| Ok(liveness(state, "node-2")? == Some(Liveness::Live)),
    )?;

    // A crash: node-3 is marked down, then removed.
    sim.crash("node-3");
    shared.record("crash node-3");
    let two = ["node-1", "node-2"];
    after_event(
        &mut sim,
        &shared,
        "node-3 down",
        TIMEOUT + BOUND,
        &two,
        |state| Ok(liveness(state, "node-3")? == Some(Liveness::Down)),
    )?;
    shared.leaves.lock().expect("lock").push("node-3".parse()?);
    after_event(
        &mut sim,
        &shared,
        "node-3 left",
        Duration::from_secs(1),
        &two,
        |state| Ok(state.membership_epoch().get() == 4),
    )?;

    // A restart: node-3 joins again, as a laptop waking from sleep would.
    sim.bounce("node-3");
    shared.record("restart node-3");
    after_event(
        &mut sim,
        &shared,
        "node-3 rejoined",
        Duration::from_secs(1),
        &DATA,
        |state| Ok(state.membership_epoch().get() == 5),
    )?;

    // A control node outage: every node is down when it returns, and up once heartbeats arrive.
    sim.crash(CONTROL);
    *shared.control.lock().expect("lock") = None;
    shared.record("crash control");
    advance(&mut sim, 2 * TIMEOUT)?;
    sim.bounce(CONTROL);
    let restarted = Timestamp::from_millis(millis(sim.elapsed())?);
    shared.record("restart control");
    after_event(
        &mut sim,
        &shared,
        "all heard from after the outage",
        4 * INTERVAL,
        &DATA,
        |state| {
            let mut heard = state.membership_epoch().get() == 5;
            for id in DATA {
                let id: NodeId = id.parse()?;
                heard &= state.last_heartbeat(&id).is_some_and(|at| at > restarted)
                    && state.liveness(&id, state.now()) == Some(Liveness::Live);
            }
            Ok(heard)
        },
    )?;

    let trace = shared.trace.lock().expect("lock").clone();
    Ok(trace)
}

#[test]
fn every_live_node_publishes_the_same_ring_within_two_heartbeats() -> TestResult {
    let first = scenario(29)?;
    let second = scenario(29)?;
    assert_eq!(first, second, "the same seed must give the same trace");
    Ok(())
}
