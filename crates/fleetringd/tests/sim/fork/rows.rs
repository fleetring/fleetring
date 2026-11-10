//! Rows 1 to 11 of FR-087's matrix: the source and the seeding replicas under faults.
//!
//! Every row runs on real data nodes: the seeder fetches from the source's primary, the peers
//! from the seeder, a holder takes the first lease and runs the fork-reset write, all through
//! the daemon's own fork code. Only row 9 and row 10 add control-plane messages of their own: a
//! lease asked for by a node that isn't a holder, as no data node of this build asks.

use std::time::Duration;

use fleetring::NodeId;
use fleetringd::replication::FaultPoint;
use fleetringd::sim::{Expected, Operation, Outcome};

use super::matrix::{Point, Result, Run, Setup, after_losing, row};

/// Row 1: concurrent source writes and no fault, at each step: the baseline.
fn baseline(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    // The source takes writes while the fork's work waits.
    let source = run.source.clone();
    run.await_more_writes(&source, 5)?;
    run.release(point.hook(), operation)?;
    run.finish(operation, Expected::Ready)
}

row!(row_01_baseline, sweep_row_01_baseline, Point::ALL, baseline);

/// Row 2: the source's primary crashes at each step and fails over (ADR-0022), then comes back.
fn source_crash(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let primary = run.primary(&run.source.clone())?;
    run.crash(&primary)?;
    run.release(point.hook(), operation)?;
    run.wait(Duration::from_secs(4))?;
    run.restart(&primary)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_02_source_crash,
    sweep_row_02_source_crash,
    Point::ALL,
    source_crash
);

/// Row 2, with writes the source's primary committed and no quorum acknowledged when it was
/// pinned: its followers are cut off from it while clients write, the seeder pins, and the
/// primary crashes, so the source's failover loses those writes. The fork must hold none of
/// them (property b): the pin is at the acknowledged position, never at the tip.
fn source_unacknowledged_tail(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let primary = run.primary(&run.source.clone())?;
    let followers = run.source_followers()?;
    let target = {
        let followers = followers.clone();
        run.pick("dst", super::matrix::REPLICAS, |placed| {
            !followers.contains(&placed[0])
        })?
    };
    let operation = run.operation_id();
    run.hold(point.hook(), operation)?;
    run.fork_as(&target, operation)?;
    run.reach(operation, point)?;
    for follower in &followers {
        run.fleet.partition(&primary, follower)?;
    }
    // Writes reach the primary alone: committed there, never acknowledged.
    run.wait(Duration::from_millis(600))?;
    run.release(point.hook(), operation)?;
    run.hold(FaultPoint::Staged, operation)?;
    run.reach(operation, Point::AfterSeederStaged)?;
    run.crash(&primary)?;
    for follower in &followers {
        run.fleet.repair(&primary, follower)?;
    }
    run.release(FaultPoint::Staged, operation)?;
    run.wait(Duration::from_secs(4))?;
    run.restart(&primary)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_02_unacknowledged_tail,
    sweep_row_02_unacknowledged_tail,
    [Point::BeforePin],
    source_unacknowledged_tail
);

/// Row 3: the source's primary is drained at each step, handing its lease to a follower.
fn source_drain(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let primary = run.primary(&run.source.clone())?;
    run.operator.push(Operation::Drain {
        node: primary.parse()?,
    });
    run.wait(Duration::from_secs(2))?;
    run.release(point.hook(), operation)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_03_source_drain,
    sweep_row_03_source_drain,
    Point::ALL,
    source_drain
);

/// Row 4: the source is dropped at each step: a seeding fork fails as `SourceGone`, a seeded
/// one no longer needs it.
fn source_dropped(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let source = run.source.clone();
    run.traffic.remove(&source);
    run.script([Operation::Drop { database: source }])?;
    run.release(point.hook(), operation)?;
    let expected = if point.is_seeding() {
        Expected::Failed
    } else {
        Expected::Ready
    };
    run.finish(operation, expected)
}

row!(
    row_04_source_dropped,
    sweep_row_04_source_dropped,
    Point::ALL,
    source_dropped
);

/// Row 5: the source is dropped and created again under its name, with writes of its own, at
/// each step; at [`Point::BeforePin`], between `ForkDatabase` and the seeder's first request.
/// A seeding fork fails and copies nothing of the new source (property c); a seeded one keeps
/// the old source's rows.
fn source_recreated(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let source = run.source.clone();
    run.traffic.remove(&source);
    run.script([Operation::Drop {
        database: source.clone(),
    }])?;
    run.traffic.count_from(1_000_000);
    run.traffic.create(source.clone(), 3);
    run.await_more_writes(&source, 10)?;
    run.release(point.hook(), operation)?;
    let expected = if point.is_seeding() {
        Expected::Failed
    } else {
        Expected::Ready
    };
    run.finish(operation, expected)
}

row!(
    row_05_source_recreated,
    sweep_row_05_source_recreated,
    Point::ALL,
    source_recreated
);

/// Row 6: the seeder crashes at each step and restarts.
fn seeder_restart(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let seeder = run.seeder(operation)?;
    run.crash(&seeder)?;
    run.release(point.hook(), operation)?;
    run.wait(Duration::from_secs(1))?;
    run.restart(&seeder)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_06_seeder_restart,
    sweep_row_06_seeder_restart,
    Point::ALL,
    seeder_restart
);

/// Row 7: the seeder is lost for good at each step: the next attempt seeds from another replica,
/// or, once seeded, the other holders go on, if a bootstrap quorum of them remains.
fn seeder_lost(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let seeder = run.seeder(operation)?;
    let expected = if point.is_seeding() {
        Expected::Ready
    } else {
        after_losing(&run.holders(&target)?, &[seeder.clone()])
    };
    run.crash(&seeder)?;
    run.release(point.hook(), operation)?;
    run.finish(operation, expected)
}

row!(
    row_07_seeder_lost,
    sweep_row_07_seeder_lost,
    Point::ALL,
    seeder_lost
);

/// Row 8: a peer is lost for good before completion: the seeder and the other peer complete.
fn peer_lost(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, held) = run.fork_to(&target, point)?;
    let seeder = run.seeder(operation)?;
    let placed = run.placement(target.as_str(), super::matrix::REPLICAS)?;
    // At the peer copies, the peer held there; else the first peer.
    let peer = if point == Point::DuringPeerCopies {
        held
    } else {
        placed
            .iter()
            .find(|node| **node != seeder)
            .cloned()
            .ok_or("no peer")?
    };
    run.crash(&peer)?;
    run.release(point.hook(), operation)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_08_peer_lost,
    sweep_row_08_peer_lost,
    Point::SEEDING,
    peer_lost
);

/// Asks for `target`'s first lease for a node that isn't among its holders, as no data node
/// does: control state must refuse it (property a), and the fork is never served from an empty
/// replica.
fn non_holder_asks(run: &mut Run, target: &fleetring_control::DatabaseName) -> Result<()> {
    let holders = run.holders(target)?;
    let stranger = super::matrix::NODES
        .iter()
        .find(|node| !holders.iter().any(|holder| holder == *node) && !run.down.contains(**node))
        .ok_or("every live node holds the fork")?;
    let name = run.spawn_script([Operation::AcquireLease {
        database: target.clone(),
        node: stranger.parse::<NodeId>()?,
    }]);
    run.fleet.run_until(
        super::matrix::LIMIT,
        "the stranger's lease answered",
        |fleet| {
            fleet.history().records().iter().any(|record| {
                record.client() == name
                    && !matches!(record.outcome(), Outcome::Indeterminate { .. })
            })
        },
    )?;
    Ok(())
}

/// Row 9: every holder but one is lost after completion, before the first lease: the last one
/// takes it, but its reset can't reach a bootstrap quorum, so the fork stays seeded and serves
/// nothing. A node that isn't a holder asks for the first lease and is refused.
fn all_but_one_holder_lost(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, held) = run.fork_to(&target, point)?;
    let holders = run.holders(&target)?;
    let lost: Vec<String> = holders
        .iter()
        .filter(|holder| **holder != held)
        .cloned()
        .collect();
    for holder in &lost {
        run.crash(holder)?;
    }
    non_holder_asks(&mut run, &target)?;
    run.release(point.hook(), operation)?;
    run.finish(operation, after_losing(&holders, &lost))
}

row!(
    row_09_all_but_one_holder_lost,
    sweep_row_09_all_but_one_holder_lost,
    [Point::BeforeFirstLease],
    all_but_one_holder_lost
);

/// Row 10: every holder is lost after completion: the fork stays seeded, unavailable, and is
/// never served from an empty replica, though a node that isn't a holder asks for its lease.
fn all_holders_lost(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let holders = run.holders(&target)?;
    for holder in &holders {
        run.crash(holder)?;
    }
    non_holder_asks(&mut run, &target)?;
    run.release(point.hook(), operation)?;
    run.finish(operation, Expected::Seeded)
}

row!(
    row_10_all_holders_lost,
    sweep_row_10_all_holders_lost,
    [Point::BeforeFirstLease, Point::DuringReset],
    all_holders_lost
);

/// Row 11: the first lease holder crashes before the fork-reset write: another holder is
/// promoted, finds no marker, and runs it; the crashed one comes back as a follower.
fn first_primary_crash(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let operation = run.operation_id();
    let _ = point;
    run.hold(FaultPoint::BeforeReset, operation)?;
    run.fork_as(&target, operation)?;
    let primary = run.await_arrival(FaultPoint::BeforeReset, &std::collections::BTreeMap::new())?;
    run.crash(&primary)?;
    run.release(FaultPoint::BeforeReset, operation)?;
    run.wait(Duration::from_secs(4))?;
    run.restart(&primary)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_11_first_primary_crash,
    sweep_row_11_first_primary_crash,
    [Point::DuringReset],
    first_primary_crash
);
