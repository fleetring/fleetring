//! FR-064 on the harness: control groups of three and five control nodes (ADR-0035, ADR-0040).
//!
//! Every scenario runs data nodes beside a control group that the fleet forms as an operator
//! would, with `control add` and `control set-voters`, and ends with [`Fleet::finish`], which
//! replays every control host's log, checks that the logs agree (`ControlLogsAgree`), that at
//! most one host led each term (`OneLeaderPerTerm`), that the most complete log holds every
//! acknowledged control operation, and the history checker over every write. Faults crash,
//! isolate, and restart control hosts, the leader among them.

use std::error::Error;
use std::time::Duration;

use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetringd::sim::{CreateAndWrite, EventKind, Fleet, Replay, Report, Timings, seeds};

use crate::{BINARY, TestResult};

const THREE: [&str; 3] = ["c1", "c2", "c3"];
const FIVE: [&str; 5] = ["c1", "c2", "c3", "c4", "c5"];
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const ROUTER: &str = "router";

/// The harness's shortest election timeout; the longest is twice it.
const ELECTION_TIMEOUT: Duration = Duration::from_millis(250);

fn fleet(seed: u64, controls: &[&str]) -> Result<Fleet, Box<dyn Error>> {
    let mut builder = Fleet::builder(seed)
        .control_group(controls)
        .timings(Timings::default())
        .operation_timeout(Duration::from_secs(2));
    for data in DATA {
        builder = builder.data(data);
    }
    Ok(builder.build()?)
}

/// Every database's epoch is still the first lease's: no primary changed epoch.
fn no_epoch_changed(fleet: &Fleet) -> TestResult {
    let state = fleet.control_state().ok_or("no control state")?;
    for database in state.databases() {
        assert_eq!(
            database.epoch(),
            LeaseEpoch::new(1),
            "{} changed epoch across the control group's elections",
            database.name()
        );
    }
    Ok(())
}

/// Crashing the leader while a client writes elects a new leader within three of the longest
/// election timeouts; the leader comes back as a follower; every acknowledged create is in every
/// control node's log; and no primary changes epoch because of the election alone.
fn a_leader_crash_under_writes(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, &THREE)?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.client("app", CreateAndWrite::new(3, 3, 120));
    fleet.run_for(Duration::from_secs(2))?;
    let leader = fleet.control_leader().ok_or("no control leader")?;
    fleet.crash(&leader)?;
    let elected = fleet.run_until(Duration::from_secs(10), "a new control leader", |fleet| {
        fleet.control_leader().is_some()
    })?;
    assert!(
        elected <= ELECTION_TIMEOUT * 6,
        "a new leader took {elected:?} after the crash"
    );
    fleet.run_for(Duration::from_secs(2))?;
    fleet.restart(&leader)?;
    fleet.await_workloads(Duration::from_secs(120))?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.await_control_converged(Duration::from_secs(30))?;
    no_epoch_changed(&fleet)?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn a_leader_crash_under_writes_loses_no_control_command() {
    seeds::run(BINARY, &seeds::FIXED_HEAVY, a_leader_crash_under_writes);
}

/// A leader cut off from both other control nodes, though not from data nodes, commits nothing:
/// the others elect a leader of their own, clients and data nodes move to it, and once the
/// partition heals the old leader's log agrees with theirs.
fn an_isolated_leader(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, &THREE)?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.client("app", CreateAndWrite::new(2, 3, 120));
    fleet.run_for(Duration::from_secs(1))?;
    let isolated = fleet.control_leader().ok_or("no control leader")?;
    let others: Vec<&str> = THREE
        .iter()
        .copied()
        .filter(|host| *host != isolated)
        .collect();
    for other in &others {
        fleet.partition(&isolated, other)?;
    }
    let committed_index = |fleet: &Fleet| {
        fleet
            .control_handle_of(&isolated)
            .map(|handle| handle.committed_index())
    };
    let committed = committed_index(&fleet);
    fleet.run_until(Duration::from_secs(10), "another control leader", |fleet| {
        fleet
            .control_leader()
            .is_some_and(|leader| leader != isolated)
    })?;
    fleet.run_for(Duration::from_secs(3))?;
    assert_eq!(
        committed_index(&fleet),
        committed,
        "{isolated} committed entries while cut off from the other control nodes"
    );
    for other in &others {
        fleet.repair(&isolated, other)?;
    }
    fleet.await_workloads(Duration::from_secs(120))?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.await_control_converged(Duration::from_secs(30))?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn an_isolated_leader_commits_nothing() {
    seeds::run(BINARY, &seeds::FIXED_HEAVY, an_isolated_leader);
}

/// A five-node group keeps its quorum through two crashes, the leader's among them: a leader
/// among the other three, commands keep committing, and after the restarts the group converges.
fn five_control_nodes(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, &FIVE)?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.client("app", CreateAndWrite::new(2, 3, 120));
    fleet.run_for(Duration::from_secs(1))?;
    let leader = fleet.control_leader().ok_or("no control leader")?;
    let second = FIVE
        .iter()
        .copied()
        .find(|host| *host != leader)
        .ok_or("no other control host")?;
    fleet.crash(&leader)?;
    fleet.crash(second)?;
    fleet.run_until(
        Duration::from_secs(10),
        "a leader among the other three",
        |fleet| {
            fleet
                .control_leader()
                .is_some_and(|new| new != leader && new != second)
        },
    )?;
    fleet.run_for(Duration::from_secs(3))?;
    fleet.restart(&leader)?;
    fleet.restart(second)?;
    fleet.await_workloads(Duration::from_secs(120))?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.await_control_converged(Duration::from_secs(30))?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn five_control_nodes_survive_two_crashes() {
    seeds::run(BINARY, &[1], five_control_nodes);
}

/// Each control node stops gracefully and restarts in turn, the leader last. The leader hands
/// its leadership over as it stops, so a new leader is ready before an election timeout could
/// pass, and the writes go on throughout.
fn a_rolling_restart(seed: u64) -> TestResult {
    let mut fleet = fleet(seed, &THREE)?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.client("app", CreateAndWrite::new(2, 3, 200));
    fleet.run_for(Duration::from_secs(1))?;
    let leader = fleet.control_leader().ok_or("no control leader")?;
    let followers: Vec<&str> = THREE
        .iter()
        .copied()
        .filter(|host| *host != leader)
        .collect();
    for follower in followers {
        fleet.stop(follower)?;
        fleet.run_for(Duration::from_secs(1))?;
        fleet.restart(follower)?;
        fleet.await_control_converged(Duration::from_secs(30))?;
        fleet.run_for(Duration::from_secs(1))?;
    }
    fleet.stop(&leader)?;
    let taken = fleet.run_until(Duration::from_secs(10), "a new control leader", |fleet| {
        fleet.control_leader().is_some()
    })?;
    assert!(
        taken < ELECTION_TIMEOUT,
        "no leader for {taken:?} after the leader stopped: it didn't hand over"
    );
    fleet.run_for(Duration::from_secs(1))?;
    fleet.restart(&leader)?;
    fleet.await_workloads(Duration::from_secs(120))?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.await_control_converged(Duration::from_secs(30))?;
    no_epoch_changed(&fleet)?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn a_rolling_restart_keeps_the_control_plane() {
    seeds::run(BINARY, &seeds::FIXED_HEAVY, a_rolling_restart);
}

/// The router is configured with `c1`'s address alone, and learns the rest of the group from
/// `Locate` replies (ADR-0040 point 12, amendment 4). Once `c1`, the leader, crashes, the router
/// still finds the new leader, and serves writes to databases created after the crash, which it
/// must locate there.
fn the_router_learns_the_group(seed: u64) -> TestResult {
    let mut builder = Fleet::builder(seed)
        .control_group(&THREE)
        .router(ROUTER)
        .timings(Timings::default())
        .operation_timeout(Duration::from_secs(2));
    for data in DATA {
        builder = builder.data(data);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.client_via_router("app-1", CreateAndWrite::new(2, 3, 40))?;
    fleet.await_workloads(Duration::from_secs(120))?;
    assert_eq!(
        fleet.control_leader().as_deref(),
        Some(THREE[0]),
        "c1, which started the group, still leads"
    );
    fleet.crash(THREE[0])?;
    fleet.run_until(Duration::from_secs(10), "a new control leader", |fleet| {
        fleet.control_leader().is_some()
    })?;
    // `db-2` and `db-3` are created now, so the router locates them on the new leader.
    fleet.client_via_router("app-2", CreateAndWrite::new(4, 3, 80))?;
    fleet.await_workloads(Duration::from_secs(120))?;
    let late: DatabaseName = "db-3".parse()?;
    let served = fleet
        .history()
        .acknowledged_writes()
        .filter(|(database, _)| **database == late)
        .count();
    assert!(served > 0, "the router served no write to {late}");
    fleet.restart(THREE[0])?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.await_control_converged(Duration::from_secs(30))?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn the_router_learns_the_control_group_and_finds_a_new_leader() {
    seeds::run(BINARY, &seeds::FIXED_HEAVY, the_router_learns_the_group);
}

/// A three-node group's run, the leader crashing and restarting while a client writes, for
/// replaying twice.
fn crash_and_restart_the_leader(seed: u64) -> Result<Report, Box<dyn Error>> {
    let mut fleet = fleet(seed, &THREE)?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.client("app", CreateAndWrite::new(1, 3, 40));
    fleet.run_for(Duration::from_secs(1))?;
    let leader = fleet.control_leader().ok_or("no control leader")?;
    fleet.crash(&leader)?;
    fleet.run_for(Duration::from_secs(2))?;
    fleet.restart(&leader)?;
    fleet.await_workloads(Duration::from_secs(60))?;
    fleet.await_control_converged(Duration::from_secs(30))?;
    Ok(fleet.finish()?)
}

/// A control group's run replays exactly from its seed: both runs of
/// [`crash_and_restart_the_leader`] give the same trace, every event, the terms each host led
/// among them, at the same simulated time (ADR-0020, ADR-0040 point 1).
#[test]
fn a_control_group_seed_replays_the_same_run() {
    if !cfg!(tokio_unstable) {
        panic!(
            "this build can't replay a seed: Tokio isn't built with `--cfg tokio_unstable`, which \
             `.cargo/config.toml` sets and a `RUSTFLAGS` variable replaces (ADR-0020's amendment)"
        );
    }
    seeds::run(BINARY, &[1], |seed| -> TestResult {
        let first = crash_and_restart_the_leader(seed)?;
        let second = crash_and_restart_the_leader(seed)?;
        first.trace().compare(second.trace(), Replay::Exact)?;
        let leads = first
            .trace()
            .events()
            .iter()
            .filter(|event| matches!(event.kind(), EventKind::Leader { .. }))
            .count();
        assert!(leads >= 2, "the trace records no new leader");
        Ok(())
    });
}
