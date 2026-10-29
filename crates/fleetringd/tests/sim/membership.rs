//! FR-029 on the harness: membership, heartbeats, and ring publication.
//!
//! One control host and three data hosts. The test partitions a data node from the control
//! node, crashes one, removes it, restarts it, and takes the control node down, and checks after
//! each event that every live node publishes the control plane's ring, and the control plane's
//! view of who is live, within two heartbeat intervals.

use std::error::Error;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, Liveness, Role, State, Timestamp};
use fleetringd::sim::{
    Ack, Cluster, CreateAndWrite, Operation, Operator, Replay, Report, Timings, seeds,
};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const INTERVAL: Duration = Duration::from_millis(200);
const TIMEOUT: Duration = Duration::from_millis(1_000);
/// Every acceptance bound: two heartbeat intervals.
const BOUND: Duration = Duration::from_millis(400);

fn liveness(state: &State, id: &str) -> Option<Liveness> {
    let id: NodeId = id.parse().ok()?;
    state.liveness(&id, state.now())
}

/// Waits for the control plane to reach `condition`, then checks that `nodes` converge on it
/// within two heartbeat intervals.
fn after_event(
    cluster: &mut Cluster,
    event: &str,
    limit: Duration,
    nodes: &[&str],
    condition: impl Fn(&State) -> bool,
) -> TestResult {
    cluster.run_until(limit, event, |cluster| {
        cluster
            .control_state()
            .is_some_and(|state| condition(&state))
    })?;
    cluster.note(format!("control: {event}"));
    let took = cluster.await_ring_on(nodes, BOUND)?;
    assert!(took <= BOUND, "{event}: converged after {took:?}");
    cluster.note(format!("converged after {event}"));
    Ok(())
}

fn scenario(seed: u64) -> Result<Report, Box<dyn Error>> {
    let timings = Timings {
        heartbeat_interval: INTERVAL,
        heartbeat_timeout: TIMEOUT,
        ..Timings::default()
    };
    let mut cluster = Cluster::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .timings(timings)
        .build()?;
    let operator = Operator::new();
    cluster.client("operator", operator.clone());

    // Joins.
    after_event(
        &mut cluster,
        "three members",
        Duration::from_secs(5),
        &DATA,
        |state| state.membership_epoch().get() == 3,
    )?;

    // A partition: the control plane marks node-2 down, then up again when it heals.
    cluster.partition("node-2", CONTROL)?;
    let live = ["node-1", "node-3"];
    after_event(
        &mut cluster,
        "node-2 down",
        TIMEOUT + BOUND,
        &live,
        |state| liveness(state, "node-2") == Some(Liveness::Down),
    )?;
    // Still down while the partition lasts.
    cluster.run_for(TIMEOUT)?;
    let state = cluster.control_state().ok_or("control is down")?;
    assert_eq!(liveness(&state, "node-2"), Some(Liveness::Down));
    cluster.repair("node-2", CONTROL)?;
    after_event(&mut cluster, "node-2 up", 3 * INTERVAL, &DATA, |state| {
        liveness(state, "node-2") == Some(Liveness::Live)
    })?;

    // A crash: node-3 is marked down, then removed.
    cluster.crash("node-3")?;
    let two = ["node-1", "node-2"];
    after_event(
        &mut cluster,
        "node-3 down",
        TIMEOUT + BOUND,
        &two,
        |state| liveness(state, "node-3") == Some(Liveness::Down),
    )?;
    operator.push(Operation::Leave {
        node: "node-3".parse()?,
    });
    after_event(
        &mut cluster,
        "node-3 left",
        Duration::from_secs(1),
        &two,
        |state| state.membership_epoch().get() == 4,
    )?;

    // A restart: node-3 joins again, as a laptop waking from sleep would.
    cluster.restart("node-3")?;
    after_event(
        &mut cluster,
        "node-3 rejoined",
        Duration::from_secs(1),
        &DATA,
        |state| state.membership_epoch().get() == 5,
    )?;

    // A control node outage: every node is down when it returns, and up once heartbeats arrive.
    // Crashing the control host also checks that its log replays.
    cluster.crash(CONTROL)?;
    cluster.run_for(2 * TIMEOUT)?;
    cluster.restart(CONTROL)?;
    let restarted = Timestamp::from_millis(u64::try_from(cluster.elapsed().as_millis())?);
    let ids: Vec<NodeId> = DATA.iter().map(|id| id.parse()).collect::<Result<_, _>>()?;
    after_event(
        &mut cluster,
        "all heard from after the outage",
        4 * INTERVAL,
        &DATA,
        |state| {
            state.membership_epoch().get() == 5
                && ids.iter().all(|id| {
                    state.last_heartbeat(id).is_some_and(|at| at > restarted)
                        && state.liveness(id, state.now()) == Some(Liveness::Live)
                })
        },
    )?;
    assert!(operator.is_idle());
    Ok(cluster.finish()?)
}

#[test]
fn every_live_node_publishes_the_same_ring_within_two_heartbeats() {
    seeds::run(BINARY, &seeds::FIXED, |seed| -> TestResult {
        let first = scenario(seed)?;
        let second = scenario(seed)?;
        first.trace().compare(second.trace(), Replay::supported())?;
        Ok(())
    });
}

/// The primary that the acknowledged creation of `database` reported, if any.
fn opened_primary(cluster: &Cluster, database: &DatabaseName) -> Option<String> {
    cluster
        .history()
        .acknowledged()
        .find_map(|(operation, ack)| match (operation, ack) {
            (
                Operation::Create {
                    database: created, ..
                },
                Ack::Opened { primary, .. },
            ) if created == database => Some(primary.to_string()),
            _ => None,
        })
}

/// Replies slower than the heartbeat interval don't cost a node its liveness or its reports: the
/// control node records a heartbeat whether or not the reply gets back in time, and a join whose
/// reply is late is confirmed by the heartbeats after it rather than sent again.
#[test]
fn slow_replies_keep_nodes_live_and_their_reports_current() {
    seeds::run(BINARY, &seeds::FIXED, |seed| -> TestResult {
        let timings = Timings {
            heartbeat_interval: INTERVAL,
            heartbeat_timeout: TIMEOUT,
            ..Timings::default()
        };
        // Each message takes up to one interval, so most round trips outlast the time a node
        // gives a join or heartbeat.
        let mut cluster = Cluster::builder(seed)
            .control(CONTROL)
            .data(DATA[0])
            .data(DATA[1])
            .data(DATA[2])
            .timings(timings)
            .max_latency(INTERVAL)
            .operation_timeout(Duration::from_secs(5))
            .build()?;
        cluster.run_until(Duration::from_secs(10), "three members", |cluster| {
            cluster
                .control_state()
                .is_some_and(|state| state.membership_epoch().get() == 3)
        })?;
        cluster.client("writer", CreateAndWrite::new(1, 3, 3));
        cluster.await_workloads(Duration::from_secs(90))?;
        let database: DatabaseName = "db-0".parse()?;
        let primary: NodeId = opened_primary(&cluster, &database)
            .ok_or("db-0 was never opened")?
            .parse()?;
        cluster.run_until(Duration::from_secs(20), "the primary's report", |cluster| {
            cluster.control_state().is_some_and(|state| {
                state
                    .report(&primary, &database)
                    .is_some_and(|report| report.role() == Role::Primary)
            })
        })?;
        let state = cluster.control_state().ok_or("no control state")?;
        for node in DATA {
            assert_eq!(
                liveness(&state, node),
                Some(Liveness::Live),
                "{node} on seed {seed}"
            );
        }
        cluster.await_ring(Duration::from_secs(20))?;
        cluster.finish()?;
        Ok(())
    });
}
