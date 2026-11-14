//! The replica-set hand-over on membership changes (FR-102; ADR-0031, amendment 2), beyond
//! #157's probe in `membership_lag`: a crash right after the release, a primary restarted during
//! the hand-over, a kept member that leaves before the primary is lost, a fleet whose replica
//! set grows, `local` acknowledgements, and a fleet with a member that lacks the feature.
//!
//! Every scenario runs the history checker, whose `AcknowledgedWritesFenced` property holds
//! each acknowledged write to a write quorum of the replica set promotions fence at every
//! promotion and at every change of that set. The ignored sweep runs them, and
//! `membership_lag`'s, over 300 seeds.

use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, Feature, Member, State, membership_ring};
use fleetringd::sim::{
    Ack, DurabilityMode, Fleet, Operation, Operator, Outcome, Script, Traffic, seeds,
};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const LIMIT: Duration = Duration::from_secs(60);
const SEEDS: [u64; 3] = [1, 157, 2_026];

/// The seeds the sweep runs, unless `FLEETRING_SIM_SWEEP_SEEDS` says how many.
const SWEEP: u64 = 300;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// One scenario, run at a seed.
type Scenario = fn(u64) -> TestResult;

/// A fleet of `nodes` data hosts and a control host, with `joiner` a data host too, cut off from
/// the control node until the scenario lets it join.
fn fleet(seed: u64, nodes: &[&str], joiner: &str) -> Result<Fleet> {
    let mut builder = Fleet::builder(seed).control(CONTROL);
    for node in nodes.iter().chain([&joiner]) {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.partition(joiner, CONTROL)?;
    fleet.await_ring_on(nodes, Duration::from_secs(10))?;
    Ok(fleet)
}

/// Where the ring over `members` places `name` with `factor` replicas.
fn placed(state: &State, members: &[Member], name: &str, factor: u8) -> Result<Vec<String>> {
    let ring = membership_ring(members, state.membership_epoch())?;
    let factor = ReplicationFactor::new(factor).ok_or("zero replicas")?;
    Ok(ring
        .replicas_n(name, factor)
        .to_ids()
        .iter()
        .map(ToString::to_string)
        .collect())
}

/// The first name `prefix-N` whose placement with `factor` replicas moves when `joiner` joins,
/// and satisfies `wanted(before, after)`. Returns it with both placements.
fn moved_by_join(
    fleet: &Fleet,
    joiner: &str,
    factor: u8,
    prefix: &str,
    wanted: impl Fn(&[String], &[String]) -> bool,
) -> Result<(DatabaseName, Vec<String>, Vec<String>)> {
    let state = fleet.control_state().ok_or("the control node is down")?;
    let now: Vec<Member> = state.members().cloned().collect();
    let mut joined = now.clone();
    joined.push(Member::new(joiner.parse()?));
    for n in 0..500 {
        let name = format!("{prefix}-{n}");
        let before = placed(&state, &now, &name, factor)?;
        let after = placed(&state, &joined, &name, factor)?;
        if after.iter().any(|node| node == joiner) && wanted(&before, &after) {
            return Ok((name.parse()?, before, after));
        }
    }
    Err(format!("no {prefix}-N is placed as the scenario needs").into())
}

/// The holder of `database`'s lease now.
fn leader(fleet: &Fleet, database: &DatabaseName) -> Option<String> {
    fleet
        .control_state()?
        .database(database)?
        .lease()
        .map(|lease| lease.holder().to_string())
}

/// Whether control state keeps a replica set for `database`.
fn keeps(fleet: &Fleet, database: &DatabaseName) -> bool {
    fleet
        .control_state()
        .and_then(|state| {
            state
                .database(database)
                .map(|db| db.kept_replicas().is_some())
        })
        .unwrap_or(false)
}

/// Steps until `database` has a primary, and returns it.
fn await_leader(fleet: &mut Fleet, database: &DatabaseName) -> Result<String> {
    let name = database.clone();
    fleet.run_until(LIMIT, "a primary", |fleet| leader(fleet, &name).is_some())?;
    Ok(leader(fleet, database).ok_or("no primary")?)
}

/// Steps until a primary other than `old` leads `database`.
fn await_promotion(fleet: &mut Fleet, database: &DatabaseName, old: &str) -> Result<()> {
    let name = database.clone();
    let old = old.to_owned();
    fleet.run_until(LIMIT, "a promotion", |fleet| {
        leader(fleet, &name).is_some_and(|holder| holder != old)
    })?;
    Ok(())
}

/// Steps until control state keeps a replica set for `database`, or, with `kept` false, keeps
/// none.
fn await_kept(fleet: &mut Fleet, database: &DatabaseName, kept: bool) -> Result<()> {
    let name = database.clone();
    let what = if kept {
        "a kept replica set"
    } else {
        "the kept replica set released"
    };
    fleet.run_until(LIMIT, what, |fleet| keeps(fleet, &name) == kept)?;
    Ok(())
}

/// Starts traffic on `database`, created with `replication_factor` replicas, and steps until
/// `writes` writes are acknowledged. Returns the handle that stops it.
fn traffic(
    fleet: &mut Fleet,
    database: &DatabaseName,
    replication_factor: u32,
    writes: usize,
) -> Result<fleetringd::sim::TrafficHandle> {
    let (traffic, handle) = Traffic::new([(database.clone(), replication_factor)], 0);
    fleet.client("traffic", traffic);
    let watched = handle.clone();
    let name = database.clone();
    fleet.run_until(LIMIT, "writes to the database", |_| {
        watched.acknowledged(&name) >= writes
    })?;
    Ok(handle)
}

/// Lets `joiner` join.
fn join(fleet: &mut Fleet, joiner: &str) -> Result<()> {
    fleet.repair(joiner, CONTROL)?;
    Ok(())
}

/// Stops `traffic`, waits for every client, and finishes the run with every check.
fn finish(mut fleet: Fleet, traffic: &fleetringd::sim::TrafficHandle) -> TestResult {
    traffic.stop();
    fleet.await_workloads(LIMIT)?;
    fleet.finish()?;
    Ok(())
}

/// A primary crashes right after control state released the kept set: the promotion fences a
/// quorum of placement, which every acknowledged write reached.
fn crash_right_after_release(seed: u64) -> TestResult {
    let nodes = ["n1", "n2", "n3", "n4"];
    let mut fleet = fleet(seed, &nodes, "n5")?;
    let (database, _, _) = moved_by_join(&fleet, "n5", 3, "release", |_, _| true)?;
    let traffic = traffic(&mut fleet, &database, 3, 20)?;
    join(&mut fleet, "n5")?;
    await_kept(&mut fleet, &database, true)?;
    await_kept(&mut fleet, &database, false)?;
    let primary = await_leader(&mut fleet, &database)?;
    fleet.crash(&primary)?;
    await_promotion(&mut fleet, &database, &primary)?;
    fleet.check_history()?;
    fleet.restart(&primary)?;
    finish(fleet, &traffic)
}

/// The primary restarts while control state keeps the old replica set, with writes going on:
/// whoever leads next learns the sets again before it serves, and the hand-over completes.
fn primary_restart_during_handover(seed: u64) -> TestResult {
    let nodes = ["n1", "n2", "n3", "n4"];
    let mut fleet = fleet(seed, &nodes, "n5")?;
    let (database, before, _) = moved_by_join(&fleet, "n5", 3, "restart", |_, _| true)?;
    let traffic = traffic(&mut fleet, &database, 3, 20)?;
    let primary = await_leader(&mut fleet, &database)?;
    // The joiner can't reach the primary, so placement can't catch up and the set stays kept.
    fleet.partition(&primary, "n5")?;
    join(&mut fleet, "n5")?;
    await_kept(&mut fleet, &database, true)?;
    fleet.note(format!(
        "{database} on {before:?}, led by {primary}, which restarts during the hand-over"
    ));
    fleet.restart(&primary)?;
    fleet.run_for(Duration::from_secs(2))?;
    fleet.repair(&primary, "n5")?;
    await_kept(&mut fleet, &database, false)?;
    fleet.check_history()?;
    finish(fleet, &traffic)
}

/// A member of the kept set fails and is removed, and the primary fails before the release:
/// the promotion waits, since only one member of the kept set is live, until the removed member
/// comes back and joins again, and then no acknowledged write is lost (the daemon README's
/// runbook).
fn kept_member_leaves_then_primary_lost(seed: u64) -> TestResult {
    let nodes = ["n1", "n2", "n3", "n4"];
    let mut fleet = fleet(seed, &nodes, "n5")?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    let members: Vec<Member> = state.members().cloned().collect();
    let name: DatabaseName = "left-0".parse()?;
    let set = placed(&state, &members, name.as_str(), 3)?;
    let traffic = traffic(&mut fleet, &name, 3, 20)?;
    let primary = await_leader(&mut fleet, &name)?;
    let leaving = set
        .iter()
        .find(|node| **node != primary)
        .cloned()
        .ok_or("no follower")?;
    let leaving_id: NodeId = leaving.parse()?;
    // The primary can't report, so the set stays kept.
    fleet.partition(&primary, CONTROL)?;
    fleet.crash(&leaving)?;
    let operator = Operator::new();
    fleet.client("operator", operator.clone());
    operator.push(Operation::Leave {
        node: leaving_id.clone(),
    });
    await_kept(&mut fleet, &name, true)?;
    fleet.note(format!(
        "{name} on {set:?}: {leaving} failed and was removed; {primary} fails before the release"
    ));
    fleet.crash(&primary)?;
    fleet.repair(&primary, CONTROL)?;
    // Only one member of the kept set is live: no promotion can complete.
    fleet.run_for(Duration::from_secs(10))?;
    if leader(&fleet, &name).is_some_and(|holder| holder != primary) {
        return Err(format!("{name} was promoted with one live member of its kept set").into());
    }
    // The removed member comes back, joins again, and counts in the kept set.
    fleet.restart(&leaving)?;
    await_promotion(&mut fleet, &name, &primary)?;
    fleet.check_history()?;
    fleet.restart(&primary)?;
    finish(fleet, &traffic)
}

/// The runbook's last resort: a member of the kept set fails, its disk with it, and is removed;
/// the primary fails before the release and doesn't come back. A node started under the removed
/// member's ID on an empty disk joins again and counts in the kept set: the promotion fences the
/// live member and it, and adopts the live member's log. Every acknowledged write the live
/// member held survives; here it holds all of them, since the writes stopped and the followers
/// caught up before the failures (a write only the lost disks held would be lost in any case).
fn kept_member_returns_on_an_empty_disk(seed: u64) -> TestResult {
    let nodes = ["n1", "n2", "n3", "n4"];
    let mut fleet = fleet(seed, &nodes, "n5")?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    let members: Vec<Member> = state.members().cloned().collect();
    let name: DatabaseName = "empty-0".parse()?;
    let set = placed(&state, &members, name.as_str(), 3)?;
    let traffic = traffic(&mut fleet, &name, 3, 20)?;
    traffic.stop();
    fleet.await_workloads(LIMIT)?;
    fleet.run_until(LIMIT, "the followers caught up", Fleet::replicas_caught_up)?;
    let primary = await_leader(&mut fleet, &name)?;
    let lost = set
        .iter()
        .find(|node| **node != primary)
        .cloned()
        .ok_or("no follower")?;
    let lost_id: NodeId = lost.parse()?;
    fleet.partition(&primary, CONTROL)?;
    fleet.crash(&lost)?;
    let dir = fleet.data_dir(&lost).ok_or("no data dir")?.to_path_buf();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    let operator = Operator::new();
    fleet.client("operator", operator.clone());
    operator.push(Operation::Leave {
        node: lost_id.clone(),
    });
    await_kept(&mut fleet, &name, true)?;
    fleet.note(format!(
        "{name} on {set:?}: {lost} lost its disk and was removed; {primary} fails for good"
    ));
    fleet.crash(&primary)?;
    fleet.run_for(Duration::from_secs(5))?;
    if leader(&fleet, &name).is_some_and(|holder| holder != primary) {
        return Err(format!("{name} was promoted with one live member of its kept set").into());
    }
    fleet.restart(&lost)?;
    await_promotion(&mut fleet, &name, &primary)?;
    fleet.check_history()?;
    fleet.repair(&primary, CONTROL)?;
    fleet.restart(&primary)?;
    fleet.await_ring_on(&[primary.as_str()], LIMIT)?;
    finish(fleet, &traffic)
}

/// A fleet of two data nodes places a database of three replicas on both; a third joins, and
/// the replica set grows from two to three: the kept set's write quorum is both nodes.
fn replica_set_grows(seed: u64) -> TestResult {
    let nodes = ["n1", "n2"];
    let mut fleet = fleet(seed, &nodes, "n3")?;
    let (database, before, after) = moved_by_join(&fleet, "n3", 3, "grow", |before, after| {
        before.len() < after.len()
    })?;
    let traffic = traffic(&mut fleet, &database, 3, 20)?;
    join(&mut fleet, "n3")?;
    await_kept(&mut fleet, &database, true)?;
    fleet.note(format!("{database} grows from {before:?} to {after:?}"));
    await_kept(&mut fleet, &database, false)?;
    let primary = await_leader(&mut fleet, &database)?;
    fleet.crash(&primary)?;
    await_promotion(&mut fleet, &database, &primary)?;
    fleet.check_history()?;
    fleet.restart(&primary)?;
    finish(fleet, &traffic)
}

/// A database with `local` acknowledgements hands over too: its primary alone is placement's
/// write quorum, so it reports at once, and control state releases the kept set.
fn local_acknowledgements(seed: u64) -> TestResult {
    let nodes = ["n1", "n2", "n3", "n4"];
    let mut fleet = fleet(seed, &nodes, "n5")?;
    let (database, _, _) = moved_by_join(&fleet, "n5", 3, "local", |_, _| true)?;
    let mut operations = vec![Operation::Create {
        database: database.clone(),
        replication_factor: 3,
        durability: DurabilityMode::Local,
    }];
    operations.extend((1..=10).map(|value| Operation::Write {
        database: database.clone(),
        value,
    }));
    fleet.client("writer", Script::new(operations));
    fleet.run_until(LIMIT, "the writes", Fleet::workloads_done)?;
    join(&mut fleet, "n5")?;
    let name = database.clone();
    // The set is kept, perhaps only for a moment, and then released.
    fleet.run_until(LIMIT, "placement moving", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .placed_replica_set(&name)
                .is_ok_and(|set| set.iter().any(|node| node.as_str() == "n5"))
        })
    })?;
    await_kept(&mut fleet, &database, false)?;
    fleet.client(
        "after",
        Script::new((11..=15).map(|value| Operation::Write {
            database: database.clone(),
            value,
        })),
    );
    fleet.await_workloads(LIMIT)?;
    let written = fleet
        .history()
        .records()
        .iter()
        .filter(|record| record.client() == "after")
        .filter(|record| matches!(record.outcome(), Outcome::Acknowledged(Ack::Written { .. })))
        .count();
    if written == 0 {
        return Err(format!("no write to {database} was acknowledged after the hand-over").into());
    }
    fleet.finish()?;
    Ok(())
}

/// A member that lacks `handover`, as a 0.7 node, keeps the feature off: a join then moves the
/// replica set at once, as before. Once that member leaves, every member supports the feature,
/// and the next membership change that moves a leased database keeps its set and enables it.
fn mixed_fleet(seed: u64) -> TestResult {
    let nodes = ["n1", "n2", "n3", "n4"];
    let mut fleet = fleet(seed, &nodes, "n5")?;
    let old: NodeId = "old".parse()?;
    let operator = Operator::new();
    fleet.client("operator", operator.clone());
    operator.push(Operation::JoinWithout {
        node: old.clone(),
        features: vec![Feature::Handover],
    });
    let joined = old.clone();
    fleet.run_until(LIMIT, "the old member joining", |fleet| {
        fleet
            .control_state()
            .is_some_and(|state| state.member(&joined).is_some())
    })?;
    fleet.await_ring_on(&nodes, LIMIT)?;
    let (database, _, _) = moved_by_join(&fleet, "n5", 3, "mixed", |before, after| {
        !before.iter().chain(after).any(|node| node == "old")
    })?;
    let traffic = traffic(&mut fleet, &database, 3, 20)?;
    join(&mut fleet, "n5")?;
    let name = database.clone();
    fleet.run_until(LIMIT, "placement moving", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .replica_set(&name)
                .is_ok_and(|set| set.iter().any(|node| node.as_str() == "n5"))
        })
    })?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    if state.is_enabled(Feature::Handover) || keeps(&fleet, &database) {
        return Err("a fleet with a member that lacks `handover` kept a replica set".into());
    }
    operator.push(Operation::Leave { node: old.clone() });
    fleet.run_until(LIMIT, "the old member leaving", |fleet| {
        fleet
            .control_state()
            .is_some_and(|state| state.member(&old).is_none())
    })?;
    // Every member supports the feature now; the next change that moves the database, the
    // joiner's removal, keeps its set and enables it.
    fleet.crash("n5")?;
    let joiner: NodeId = "n5".parse()?;
    operator.push(Operation::Leave {
        node: joiner.clone(),
    });
    fleet.run_until(LIMIT, "the joiner leaving", |fleet| {
        fleet
            .control_state()
            .is_some_and(|state| state.member(&joiner).is_none())
    })?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    if !state.is_enabled(Feature::Handover) {
        return Err("`handover` isn't enabled once every member supports it".into());
    }
    await_kept(&mut fleet, &database, false)?;
    fleet.check_history()?;
    fleet.restart("n5")?;
    finish(fleet, &traffic)
}

#[test]
fn a_crash_right_after_the_release_loses_nothing() {
    seeds::run(BINARY, &SEEDS, crash_right_after_release);
}

#[test]
fn a_primary_restarted_during_the_hand_over_completes_it() {
    seeds::run(BINARY, &SEEDS, primary_restart_during_handover);
}

#[test]
fn a_kept_set_short_of_its_quorum_waits_for_a_member_to_come_back() {
    seeds::run(BINARY, &SEEDS, kept_member_leaves_then_primary_lost);
}

#[test]
fn a_kept_member_back_on_an_empty_disk_lets_the_promotion_complete() {
    seeds::run(BINARY, &SEEDS, kept_member_returns_on_an_empty_disk);
}

#[test]
fn a_replica_set_that_grows_hands_over() {
    seeds::run(BINARY, &SEEDS, replica_set_grows);
}

#[test]
fn local_acknowledgements_hand_over() {
    seeds::run(BINARY, &SEEDS, local_acknowledgements);
}

#[test]
fn a_member_without_the_feature_keeps_it_off() {
    seeds::run(BINARY, &SEEDS, mixed_fleet);
}

/// The sweep FR-102's review asked to record: every hand-over scenario, and `membership_lag`'s,
/// over 300 seeds (or `FLEETRING_SIM_SWEEP_SEEDS`) from seed 1 (or `FLEETRING_SIM_SWEEP_FROM`),
/// failing with every seed that fails. Shards of seeds run in parallel as separate processes.
#[test]
#[ignore = "the sweep: 300 seeds per scenario, run with --ignored"]
fn sweep_handover() {
    let count: u64 = std::env::var("FLEETRING_SIM_SWEEP_SEEDS")
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(SWEEP);
    let from: u64 = std::env::var("FLEETRING_SIM_SWEEP_FROM")
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(1);
    let scenarios: Vec<(&str, Scenario)> = vec![
        ("crash_right_after_release", crash_right_after_release),
        (
            "primary_restart_during_handover",
            primary_restart_during_handover,
        ),
        (
            "kept_member_leaves_then_primary_lost",
            kept_member_leaves_then_primary_lost,
        ),
        (
            "kept_member_returns_on_an_empty_disk",
            kept_member_returns_on_an_empty_disk,
        ),
        ("replica_set_grows", replica_set_grows),
        ("local_acknowledgements", local_acknowledgements),
        ("mixed_fleet", mixed_fleet),
        ("membership_lag", crate::membership_lag::scenario),
        #[cfg(feature = "fault-hooks")]
        (
            "upgraded_control_node",
            crate::membership_lag::upgraded_control_node,
        ),
    ];
    let mut failures = Vec::new();
    for (name, scenario) in &scenarios {
        for seed in from..from + count {
            let result = std::panic::catch_unwind(|| scenario(seed));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(format!("{name} seed {seed}: {error}")),
                Err(_) => failures.push(format!("{name} seed {seed}: panicked")),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} runs failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
