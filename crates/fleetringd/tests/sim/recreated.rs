//! A database dropped and created again under its name, on the same replicas, while one
//! follower's control calls are slow (FR-106; ADR-0016, point 12; ADR-0046's amendment, item 9).
//!
//! Three data nodes hold `reused` at R = 3, led by P, with followers S and T. Its first life
//! takes enough writes to checkpoint several times, so S's position leads with a later log
//! generation than a young database's will reach. S's link to the control node then gets slow
//! and the name is dropped; T deletes its copy and is cut off from P; and the name is created
//! again: the new database restarts its epochs and is leased at epoch 1 on P again. S judges the drop notice, and the end of P's old
//! stream, against a control state that already holds the new database at epoch 1, so neither
//! deletes its copy, and P's new stream arrives at S under the same epoch and holder its record
//! names. Before the fix S acknowledged the dropped database's last position, P counted it
//! toward the new database's writes, and P alone held writes acknowledged by two; when P then
//! failed, the promotion took S's log by position and served the dropped rows as the new
//! database's.
//!
//! Now P compares S's chain hash at its first acknowledgement with its own log's and re-seeds
//! the copy; and, once forks have given databases random incarnations, S refuses a stream for
//! another incarnation than its copy records, and the drop notice deletes the copy, since the
//! copy records the dropped incarnation and control state holds another.
//!
//! Since FR-110 every database created once every member supports the `incarnations` fleet
//! feature has a random incarnation, forked fleet or not (ADR-0046's FR-110 amendment). The
//! scenarios of a fleet that never forked stand the control node in for one of 0.8.0 where only
//! the chain hash can tell the copies apart, as while it isn't upgraded yet.

use std::time::Duration;

use fleetring_control::fork::OperationId;
use fleetring_control::{DatabaseName, Feature, Incarnation, IncarnationRule};
use fleetringd::replication::ReplicationSettings;
use fleetringd::sim::{
    DurabilityMode, Fleet, History, Operation, ReplicaRows, Script, Timings, check, seeds,
};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: [&str; 3] = ["n1", "n2", "n3"];
const LIMIT: Duration = Duration::from_secs(60);
/// The delay on S's link to the control node: each of its control calls takes two, and each
/// heartbeat waits for its answer, so S counts as down after a few seconds of it.
const SLOW: Duration = Duration::from_secs(1);
/// The first life's writes: enough to cross several checkpoints.
const FIRST_LIFE: i64 = 40;
/// The second life's values start here, so they never meet the first's.
const SECOND_LIFE: i64 = 1_000;
const SECOND_WRITES: i64 = 6;
/// The forked database's one row, apart from every value the name's lives write, which the
/// fork properties tell apart by value.
const ORIGIN_VALUE: i64 = 9_000;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Whether the fleet has made a fork, so that a control node of 0.8.0 gives new databases
/// random incarnations.
#[derive(Debug, Clone, Copy)]
enum Forks {
    /// The fleet never forked and its control node runs 0.8.0, so every database has the legacy
    /// incarnation: only the chain hash tells the copies apart.
    Never,
    /// A fork enabled the `fork` feature before the name's first life.
    Made,
}

/// Which control node created the name's first life, in a fleet that never forked.
#[derive(Debug, Clone, Copy)]
enum FirstLife {
    /// This release's: the first life has a random incarnation too.
    Random,
    /// One of 0.8.0, upgraded before the drop: the first life keeps the legacy incarnation,
    /// and only the database created again has a random one.
    BeforeTheUpgrade,
}

fn reused() -> Result<DatabaseName> {
    Ok("reused".parse()?)
}

fn settings() -> ReplicationSettings {
    ReplicationSettings {
        stall_timeout: Duration::from_millis(400),
        ack_timeout: Duration::from_secs(2),
        open_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(100),
        // Small, so the first life crosses several checkpoints and log generations.
        checkpoint_threshold: 8 * 1024,
        ..ReplicationSettings::new()
    }
}

/// The node holding `database`'s lease at its current epoch.
fn leader(fleet: &Fleet, database: &DatabaseName) -> Option<String> {
    let state = fleet.control_state()?;
    let record = state.database(database)?;
    let lease = record.lease()?;
    (lease.epoch() == record.epoch()).then(|| lease.holder().to_string())
}

/// Runs `operations` on a client of their own, one after another, and steps until each is
/// answered.
fn run_script(fleet: &mut Fleet, client: &str, operations: Vec<Operation>) -> Result<()> {
    let count = operations.len();
    fleet.client(client, Script::new(operations));
    let name = client.to_owned();
    fleet.run_until(LIMIT, "the script answered", |fleet| {
        fleet
            .history()
            .records()
            .iter()
            .filter(|record| record.client() == name)
            .count()
            == count
    })?;
    Ok(())
}

/// Makes a fork, so that the `fork` feature is enabled and every database created from now on
/// has a random incarnation, and steps until it is ready.
fn make_a_fork(fleet: &mut Fleet) -> Result<()> {
    let origin: DatabaseName = "origin".parse()?;
    let copy: DatabaseName = "origin-copy".parse()?;
    run_script(
        fleet,
        "forker",
        vec![
            Operation::Create {
                database: origin.clone(),
                replication_factor: 3,
                durability: DurabilityMode::Quorum,
            },
            Operation::Write {
                database: origin.clone(),
                value: ORIGIN_VALUE,
            },
            Operation::Fork {
                source: origin,
                target: copy.clone(),
                operation: OperationId::from_bytes([0x5a; 16]),
                replication_factor: 3,
                expect_source_incarnation: None,
            },
        ],
    )?;
    fleet.run_until(LIMIT, "the fork ready", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state.is_enabled(Feature::Fork) && state.database(&copy).is_some_and(|db| db.is_ready())
        })
    })?;
    Ok(())
}

/// Waits until each of `hosts` keeps its copy of `database` as an orphan, then removes them, as
/// an operator would: a database of the legacy incarnation leaves no tombstone, so a drop deletes
/// none of its copies (ADR-0050, point 3).
fn remove_legacy_orphans(fleet: &mut Fleet, hosts: &[&str], database: &DatabaseName) -> Result<()> {
    let (watched, waiting) = (database.clone(), hosts.to_vec());
    fleet.run_until(LIMIT, "the legacy copies kept as orphans", |fleet| {
        waiting.iter().all(|host| {
            fleet
                .replication(host)
                .is_some_and(|replication| replication.is_orphan(&watched))
        })
    })?;
    let removals = hosts
        .iter()
        .map(|host| {
            Ok(Operation::RemoveOrphan {
                node: host.parse()?,
                database: database.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    run_script(fleet, "operator", removals)
}

/// The second life's acknowledged writes alone: the history checker leaves a dropped name out.
fn second_life(history: &History) -> History {
    let mut life = History::default();
    for record in history.records() {
        if let Operation::Write { value, .. } = record.operation()
            && (SECOND_LIFE..SECOND_LIFE + SECOND_WRITES).contains(value)
        {
            life.record(
                record.client(),
                record.invoked_at(),
                record.completed_at(),
                record.operation().clone(),
                record.outcome().clone(),
            );
        }
    }
    life
}

fn scenario(seed: u64, forks: Forks) -> TestResult {
    // A slow link holds each heartbeat for a round trip, so members stay live for longer than
    // a few of them; leases last longer than that too, and a promotion still comes in seconds.
    let timings = Timings {
        heartbeat_timeout: Duration::from_secs(5),
        lease_duration: Duration::from_secs(6),
        lease_renewal: Duration::from_secs(1),
        failover_margin: Duration::from_secs(1),
        ..Timings::default()
    };
    let mut builder = Fleet::builder(seed)
        .control(CONTROL)
        .timings(timings)
        .replication(settings())
        .operation_timeout(Duration::from_secs(4));
    if let Forks::Never = forks {
        builder = builder.incarnation_rule(IncarnationRule::OnceForked);
    }
    for node in NODES {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    match forks {
        Forks::Made => make_a_fork(&mut fleet)?,
        // The drop notice reaches S after control state holds the name at the dropped epoch
        // again, and a notice naming no random incarnation can't tell the two databases apart,
        // so S refuses it for its binding (ADR-0042, point 2), as it would a forged one.
        Forks::Never => fleet.allow_binding_refusals(),
    }
    let database = reused()?;

    // The first life, long enough to checkpoint several times, on every node.
    let mut first = vec![Operation::Create {
        database: database.clone(),
        replication_factor: 3,
        durability: DurabilityMode::Quorum,
    }];
    first.extend((0..FIRST_LIFE).map(|value| Operation::Write {
        database: database.clone(),
        value,
    }));
    run_script(&mut fleet, "first", first)?;
    fleet.await_replicas_converged(LIMIT)?;
    let primary = leader(&fleet, &database).ok_or("no primary for the first life")?;
    let mut followers = NODES.into_iter().filter(|node| *node != primary);
    let slow = followers.next().ok_or("no follower")?.to_owned();
    let cut = followers.next().ok_or("no second follower")?.to_owned();
    fleet.note(format!(
        "{database} led by {primary}; {slow}'s control calls are slow; {cut} is cut off from \
         {primary}"
    ));

    // S's control calls are slow when the name is dropped. T sees the drop and deletes its copy,
    // and is then cut off from P, so it can't follow the database created again.
    fleet.set_latency(&slow, CONTROL, SLOW)?;
    run_script(
        &mut fleet,
        "drop",
        vec![Operation::Drop {
            database: database.clone(),
        }],
    )?;
    let copy_on = |fleet: &Fleet, host: &str| {
        fleet
            .data_dir(host)
            .is_some_and(|dir| dir.join("databases").join("reused").exists())
    };
    if let Forks::Never = forks {
        remove_legacy_orphans(&mut fleet, &[primary.as_str(), cut.as_str()], &database)?;
    }
    let deleting = cut.clone();
    fleet.run_until(LIMIT, "T deleting its copy", |fleet| {
        !copy_on(fleet, &deleting)
    })?;
    if !copy_on(&fleet, &slow) {
        return Err(format!("{slow} deleted its copy before the name was created again").into());
    }
    fleet.partition(&primary, &cut)?;
    run_script(
        &mut fleet,
        "again",
        vec![Operation::Create {
            database: database.clone(),
            replication_factor: 3,
            durability: DurabilityMode::Quorum,
        }],
    )?;
    if leader(&fleet, &database).as_deref() != Some(primary.as_str()) {
        return Err(format!(
            "the second life is led by {:?}, not {primary}",
            leader(&fleet, &database)
        )
        .into());
    }
    // S has judged the drop notice and the old stream's end by now, each against the second
    // life at epoch 1. Its link heals before its heartbeats fall too far behind for it to count
    // as live.
    fleet.run_for(3 * SLOW)?;
    fleet.set_latency(&slow, CONTROL, Duration::from_millis(10))?;
    fleet.run_for(Duration::from_secs(2))?;
    run_script(
        &mut fleet,
        "second",
        (SECOND_LIFE..SECOND_LIFE + SECOND_WRITES)
            .map(|value| Operation::Write {
                database: database.clone(),
                value,
            })
            .collect(),
    )?;

    // Every write the second life acknowledged is on a write quorum of its copies.
    let rows = fleet.replica_rows(&database)?;
    let life = second_life(&fleet.history());
    check::acknowledged_writes_replicated(&life, &database, &rows, 2)?;
    let acknowledged: Vec<i64> = life.acknowledged_writes().map(|(_, value)| value).collect();
    if acknowledged.is_empty() {
        let outcomes: Vec<String> = life
            .records()
            .iter()
            .map(|record| format!("{}: {:?}", record.operation(), record.outcome()))
            .collect();
        return Err(
            format!("the second life acknowledged no write: {outcomes:?}; rows {rows:?}").into(),
        );
    }

    // P fails while T is still cut off from it: the promotion fences S and T.
    fleet.crash(&primary)?;
    let old = primary.clone();
    let name = database.clone();
    fleet.run_until(LIMIT, "a promotion", |fleet| {
        leader(fleet, &name).is_some_and(|holder| holder != old)
    })?;
    let promoted = leader(&fleet, &database).ok_or("no primary")?;
    let rows = fleet.replica_rows(&database)?;
    let served = rows.get(&promoted).cloned().unwrap_or_default();
    for value in &acknowledged {
        if !served.contains(value) {
            return Err(format!(
                "{promoted} was promoted without acknowledged write {value}: {rows:?}"
            )
            .into());
        }
    }
    if let Some(dropped) = served.iter().find(|value| **value < SECOND_LIFE) {
        return Err(format!("{promoted} serves the dropped database's row {dropped}").into());
    }

    fleet.repair(&primary, &cut)?;
    fleet.restart(&primary)?;
    fleet.await_ring(LIMIT)?;
    fleet.await_replicas_converged(LIMIT)?;
    fleet.finish()?;
    Ok(())
}

/// A forked fleet's name is dropped and created again while a follower, S, whose record names
/// T the holder of the dropped database's second epoch, is cut off from the control node and
/// from the new primary, P (ADR-0046's amendment, item 13).
///
/// The first life fails over from P to T, so S's record names T the holder of epoch 2, and its
/// copy records the first life's incarnation. S is cut off from the control node through the
/// drop, so it never hears of it and keeps its copy, and from P. The name is created again: the
/// new database is leased at epoch 1, on P, and P and T hold its writes. Then P fails, and the
/// promotion to epoch 2 names T, the only live candidate, which must fence S, since Q = 2.
/// Before the fix, S took the fence on its record alone, which names T at epoch 2, and reported
/// the dropped database's position at log epoch 2, which outranks every copy of the new
/// database: only the SQLite adapter's refusal of a log that doesn't continue T's kept T from
/// adopting it, and the promotion failed again and again. Now the fence names the new database's
/// incarnation, S refuses it and is never counted fenced, and the promotion waits for P.
fn recreated_while_a_follower_is_cut_off(seed: u64) -> TestResult {
    let timings = Timings {
        heartbeat_timeout: Duration::from_secs(5),
        lease_duration: Duration::from_secs(6),
        lease_renewal: Duration::from_secs(1),
        failover_margin: Duration::from_secs(1),
        ..Timings::default()
    };
    let mut builder = Fleet::builder(seed)
        .control(CONTROL)
        .timings(timings)
        .replication(settings())
        .operation_timeout(Duration::from_secs(4));
    for node in NODES {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    make_a_fork(&mut fleet)?;
    let database = reused()?;

    // The first life, which fails over from P to T.
    let mut first = vec![Operation::Create {
        database: database.clone(),
        replication_factor: 3,
        durability: DurabilityMode::Quorum,
    }];
    first.extend((0..FIRST_LIFE).map(|value| Operation::Write {
        database: database.clone(),
        value,
    }));
    run_script(&mut fleet, "first", first)?;
    fleet.await_replicas_converged(LIMIT)?;
    let primary = leader(&fleet, &database).ok_or("no primary for the first life")?;
    fleet.crash(&primary)?;
    let old = primary.clone();
    let name = database.clone();
    fleet.run_until(LIMIT, "the first life's promotion", |fleet| {
        leader(fleet, &name).is_some_and(|holder| holder != old)
    })?;
    let promoted = leader(&fleet, &database).ok_or("no primary")?;
    let epoch = fleet
        .control_state()
        .and_then(|state| state.database(&database).map(|db| db.epoch().get()))
        .ok_or("no epoch")?;
    if epoch != 2 {
        return Err(format!("the first life's promotion is to epoch {epoch}, not 2").into());
    }
    let stale = NODES
        .into_iter()
        .find(|node| *node != primary && *node != promoted)
        .ok_or("no third node")?
        .to_owned();
    fleet.restart(&primary)?;
    fleet.await_ring(LIMIT)?;
    run_script(
        &mut fleet,
        "first-again",
        (FIRST_LIFE..FIRST_LIFE + 5)
            .map(|value| Operation::Write {
                database: database.clone(),
                value,
            })
            .collect(),
    )?;
    fleet.await_replicas_converged(LIMIT)?;
    fleet.note(format!(
        "{database}'s first life failed over from {primary} to {promoted}; {stale} records \
         {promoted} at epoch 2 and is cut off from the control node and from {primary}"
    ));

    // S hears nothing of the drop, nor of the new database.
    fleet.partition(&stale, CONTROL)?;
    fleet.partition(&stale, &primary)?;
    run_script(
        &mut fleet,
        "drop",
        vec![Operation::Drop {
            database: database.clone(),
        }],
    )?;
    let copy_on = |fleet: &Fleet, host: &str| {
        fleet
            .data_dir(host)
            .is_some_and(|dir| dir.join("databases").join("reused").exists())
    };
    let (p, t) = (primary.clone(), promoted.clone());
    fleet.run_until(LIMIT, "P and T deleting their copies", |fleet| {
        !copy_on(fleet, &p) && !copy_on(fleet, &t)
    })?;
    run_script(
        &mut fleet,
        "again",
        vec![Operation::Create {
            database: database.clone(),
            replication_factor: 3,
            durability: DurabilityMode::Quorum,
        }],
    )?;
    if leader(&fleet, &database).as_deref() != Some(primary.as_str()) {
        return Err(format!(
            "the second life is led by {:?}, not {primary}",
            leader(&fleet, &database)
        )
        .into());
    }
    run_script(
        &mut fleet,
        "second",
        (SECOND_LIFE..SECOND_LIFE + SECOND_WRITES)
            .map(|value| Operation::Write {
                database: database.clone(),
                value,
            })
            .collect(),
    )?;
    let life = second_life(&fleet.history());
    let acknowledged: Vec<i64> = life.acknowledged_writes().map(|(_, value)| value).collect();
    if acknowledged.is_empty() {
        return Err("the second life acknowledged no write".into());
    }
    if !copy_on(&fleet, &stale) {
        return Err(format!("{stale} lost its copy of the first life").into());
    }

    // Whoever leads the new database serves every write it acknowledged and none of the dropped
    // database's rows, and no promotion counts S fenced while S holds the dropped copy.
    let stale_id: fleetring::NodeId = stale.parse()?;
    let checked = |fleet: &Fleet| -> Result<()> {
        let fenced = fleet.control_state().and_then(|state| {
            state
                .database(&database)?
                .promotion()
                .and_then(|promotion| promotion.fenced_position(&stale_id).cloned())
        });
        if let Some(position) = fenced {
            return Err(format!(
                "{stale} was fenced for the new database at {position:?}, with the dropped \
                 database's copy"
            )
            .into());
        }
        let Some(holder) = leader(fleet, &database).filter(|holder| *holder != primary) else {
            return Ok(());
        };
        let rows = fleet.replica_rows(&database)?;
        let served = rows.get(&holder).cloned().unwrap_or_default();
        if let Some(dropped) = served.iter().find(|value| **value < SECOND_LIFE) {
            return Err(format!(
                "{holder} serves the dropped database's row {dropped}, from {stale}'s copy: \
                 {rows:?}"
            )
            .into());
        }
        for value in &acknowledged {
            if !served.contains(value) {
                return Err(format!(
                    "{holder} was promoted without acknowledged write {value}: {rows:?}"
                )
                .into());
            }
        }
        Ok(())
    };

    // P fails: the promotion to epoch 2 names T, which must fence S.
    fleet.crash(&primary)?;
    for _ in 0..150 {
        fleet.run_for(Duration::from_millis(100))?;
        checked(&fleet)?;
    }

    // P comes back: the promotion fences it, and serves the new database's log.
    fleet.restart(&primary)?;
    let old = primary.clone();
    let name = database.clone();
    fleet.run_until(LIMIT, "the second life's promotion", |fleet| {
        leader(fleet, &name).is_some_and(|holder| holder != old)
    })?;
    checked(&fleet)?;
    // S keeps the dropped database's copy as an orphan, for `orphan remove` (ADR-0042), so it
    // never follows the new database; the rest of the fleet finishes as ever.
    fleet.repair(&stale, &primary)?;
    fleet.repair(&stale, CONTROL)?;
    fleet.await_ring(LIMIT)?;
    fleet.run_for(Duration::from_secs(5))?;
    checked(&fleet)?;
    fleet.finish()?;
    Ok(())
}

/// A never-forked fleet's name is dropped and created again quickly while a follower, S, handles
/// the drop notice late, and the new primary, P, fails before it streams to S (FR-110).
///
/// The first life checkpoints several times on P, S and T. S's control calls are slow when the
/// name is dropped: P and T delete their copies at once, S keeps its own while the name is
/// created again on P at epoch 1, and P is cut off from S, so the new database's writes reach P and T alone.
/// Then P fails, and the promotion to epoch 2 fences S and T. Before the fix both lives had the
/// legacy incarnation, 0: S judged the notice by its epoch against the new database at epoch 1
/// and kept the copy, the promotion's fence named 0 and S took it, and its position, whose log
/// generation leads, ranked first, so the promotion served the dropped rows and lost the writes
/// P and T acknowledged. Now every database created has a random incarnation, so the fence names
/// one S's copy doesn't record, and S refuses it.
///
/// A first life with a random incarnation of its own goes on the late notice too, which names
/// that incarnation while control state holds another. One created before the control node was
/// upgraded keeps the legacy incarnation, and S keeps its copy as an orphan: the notice, judged by
/// its epoch, is refused for its binding, as in a fleet that never forked; the fence of the new
/// database, which names a random incarnation, is refused for the copy's.
fn late_drop_notice_then_primary_lost(seed: u64, first_life: FirstLife) -> TestResult {
    let timings = Timings {
        heartbeat_timeout: Duration::from_secs(5),
        lease_duration: Duration::from_secs(6),
        lease_renewal: Duration::from_secs(1),
        failover_margin: Duration::from_secs(1),
        ..Timings::default()
    };
    let mut builder = Fleet::builder(seed)
        .control(CONTROL)
        .timings(timings)
        .replication(settings())
        .operation_timeout(Duration::from_secs(4));
    if let FirstLife::BeforeTheUpgrade = first_life {
        builder = builder.incarnation_rule(IncarnationRule::OnceForked);
    }
    for node in NODES {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    let database = reused()?;

    // The first life, long enough to checkpoint several times, on every node.
    let mut first = vec![Operation::Create {
        database: database.clone(),
        replication_factor: 3,
        durability: DurabilityMode::Quorum,
    }];
    first.extend((0..FIRST_LIFE).map(|value| Operation::Write {
        database: database.clone(),
        value,
    }));
    run_script(&mut fleet, "first", first)?;
    fleet.await_replicas_converged(LIMIT)?;
    if let FirstLife::BeforeTheUpgrade = first_life {
        // The control node is upgraded: the database it made keeps the legacy incarnation.
        fleet.set_incarnation_rule(None);
        fleet.stop(CONTROL)?;
        fleet.restart(CONTROL)?;
        fleet.await_ring(LIMIT)?;
        let kept = fleet
            .control_state()
            .and_then(|state| state.database(&database).map(|db| db.incarnation()));
        if kept != Some(Incarnation::LEGACY) {
            return Err(format!("the upgrade gave the first life incarnation {kept:?}").into());
        }
        // The late notice names the legacy incarnation, so S refuses it for its binding, as
        // in a fleet that never forked (ADR-0042, point 2).
        fleet.allow_binding_refusals();
        fleet.await_replicas_converged(LIMIT)?;
    }
    let primary = leader(&fleet, &database).ok_or("no primary for the first life")?;
    let mut followers = NODES.into_iter().filter(|node| *node != primary);
    let late = followers.next().ok_or("no follower")?.to_owned();
    let prompt = followers.next().ok_or("no second follower")?.to_owned();
    fleet.note(format!(
        "{database} led by {primary}; {late} handles the drop notice late and is cut off from \
         {primary}; {prompt} follows the new database"
    ));

    // S's control calls are slow when the name is dropped; P and T delete their copies at once.
    fleet.set_latency(&late, CONTROL, SLOW)?;
    run_script(
        &mut fleet,
        "drop",
        vec![Operation::Drop {
            database: database.clone(),
        }],
    )?;
    let copy_on = |fleet: &Fleet, host: &str| {
        fleet
            .data_dir(host)
            .is_some_and(|dir| dir.join("databases").join("reused").exists())
    };
    if let FirstLife::BeforeTheUpgrade = first_life {
        remove_legacy_orphans(&mut fleet, &[primary.as_str(), prompt.as_str()], &database)?;
    }
    let (p, t) = (primary.clone(), prompt.clone());
    fleet.run_until(LIMIT, "P and T deleting their copies", |fleet| {
        !copy_on(fleet, &p) && !copy_on(fleet, &t)
    })?;
    if !copy_on(&fleet, &late) {
        return Err(format!("{late} deleted its copy before the name was created again").into());
    }

    // The name is created again at once, on P, which never reaches S.
    fleet.partition(&primary, &late)?;
    run_script(
        &mut fleet,
        "again",
        vec![Operation::Create {
            database: database.clone(),
            replication_factor: 3,
            durability: DurabilityMode::Quorum,
        }],
    )?;
    if leader(&fleet, &database).as_deref() != Some(primary.as_str()) {
        return Err(format!(
            "the second life is led by {:?}, not {primary}",
            leader(&fleet, &database)
        )
        .into());
    }
    // Checked once the history has been: a property, not this, must catch the legacy
    // incarnation stored again.
    let created = fleet
        .control_state()
        .and_then(|state| state.database(&database).map(|db| db.incarnation()));
    // S judges the drop notice against the second life at epoch 1. Its link heals before its
    // heartbeats fall too far behind for it to count as live.
    fleet.run_for(3 * SLOW)?;
    fleet.set_latency(&late, CONTROL, Duration::from_millis(10))?;
    fleet.run_for(Duration::from_secs(2))?;
    run_script(
        &mut fleet,
        "second",
        (SECOND_LIFE..SECOND_LIFE + SECOND_WRITES)
            .map(|value| Operation::Write {
                database: database.clone(),
                value,
            })
            .collect(),
    )?;
    let life = second_life(&fleet.history());
    let acknowledged: Vec<i64> = life.acknowledged_writes().map(|(_, value)| value).collect();
    if acknowledged.is_empty() {
        return Err("the second life acknowledged no write".into());
    }
    check::acknowledged_writes_replicated(&life, &database, &fleet.replica_rows(&database)?, 2)?;

    // Whoever leads the new database holds every write it acknowledged, and none of the dropped
    // database's rows.
    let served = |fleet: &Fleet, holder: &str| -> Result<()> {
        let rows = fleet.replica_rows(&database)?;
        let held: ReplicaRows = rows
            .get(holder)
            .map(|values| (holder.to_owned(), values.clone()))
            .into_iter()
            .collect();
        check::acknowledged_writes_replicated(&life, &database, &held, 1)?;
        if let Some(dropped) = held.values().flatten().find(|value| **value < SECOND_LIFE) {
            return Err(
                format!("{holder} serves the dropped database's row {dropped}: {rows:?}").into(),
            );
        }
        Ok(())
    };

    // P fails before it has streamed anything to S: the promotion to epoch 2 fences S and T.
    fleet.crash(&primary)?;
    let mut promoted = None;
    for _ in 0..150 {
        fleet.run_for(Duration::from_millis(100))?;
        if let Some(holder) = leader(&fleet, &database).filter(|holder| *holder != primary) {
            served(&fleet, &holder)?;
            promoted = Some(holder);
            break;
        }
    }
    // A copy that outlived the notice refuses the fence, and the promotion waits for P.
    fleet.repair(&primary, &late)?;
    fleet.restart(&primary)?;
    if promoted.is_none() {
        let old = primary.clone();
        let name = database.clone();
        fleet.run_until(LIMIT, "the second life's promotion", |fleet| {
            leader(fleet, &name).is_some_and(|holder| holder != old)
        })?;
    }
    fleet.await_ring(LIMIT)?;
    match first_life {
        FirstLife::Random => {
            fleet.await_replicas_converged(LIMIT)?;
        }
        // S keeps the legacy copy as an orphan, for `orphan remove` (ADR-0042): the notice was
        // judged by its epoch, and the new database's streams and fences name another
        // incarnation, so S never follows it, as in a fleet that has forked. The rest of the
        // fleet finishes as ever.
        FirstLife::BeforeTheUpgrade => {
            let (host, name) = (late.clone(), database.clone());
            fleet.run_until(LIMIT, "S keeping its copy as an orphan", |fleet| {
                fleet
                    .replication(&host)
                    .is_some_and(|replication| replication.is_orphan(&name))
            })?;
            fleet.run_for(Duration::from_secs(5))?;
        }
    }
    let holder = leader(&fleet, &database).ok_or("no primary")?;
    served(&fleet, &holder)?;
    let rows = fleet.replica_rows(&database)?;
    check::acknowledged_writes_replicated(&life, &database, &rows, 2)?;
    fleet.finish()?;
    if created.is_none_or(|incarnation| incarnation == Incarnation::LEGACY) {
        return Err(format!("the name was created again at incarnation {created:?}").into());
    }
    Ok(())
}

/// A copy whose record has seen an epoch but names no incarnation, as one a release before
/// followers recorded incarnations left, is a copy of a database created before forks. When the
/// name was dropped while its node was down and created again with a random incarnation, the
/// node keeps that copy as an orphan at its next start, though the new database's epoch has
/// reached the copy's, rather than leave it to be ranked and fenced as the new database's
/// (ADR-0046's amendment, item 13).
fn unlabelled_copy_after_a_recreation(seed: u64) -> TestResult {
    let mut builder = Fleet::builder(seed)
        .control(CONTROL)
        .replication(settings());
    for node in NODES {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    make_a_fork(&mut fleet)?;
    let database = reused()?;
    let mut first = vec![Operation::Create {
        database: database.clone(),
        replication_factor: 3,
        durability: DurabilityMode::Quorum,
    }];
    first.extend((0..5).map(|value| Operation::Write {
        database: database.clone(),
        value,
    }));
    run_script(&mut fleet, "first", first)?;
    fleet.await_replicas_converged(LIMIT)?;
    let primary = leader(&fleet, &database).ok_or("no primary")?;
    let stale = NODES
        .into_iter()
        .find(|node| *node != primary)
        .ok_or("no follower")?
        .to_owned();

    // S is down through the drop and the creation again, so it hears of neither.
    fleet.crash(&stale)?;
    run_script(
        &mut fleet,
        "drop",
        vec![Operation::Drop {
            database: database.clone(),
        }],
    )?;
    fleet.run_for(Duration::from_secs(10))?;
    run_script(
        &mut fleet,
        "again",
        vec![Operation::Create {
            database: database.clone(),
            replication_factor: 3,
            durability: DurabilityMode::Quorum,
        }],
    )?;
    // S's record as a release before followers recorded incarnations wrote it.
    let record = fleet
        .data_dir(&stale)
        .ok_or("no data dir")?
        .join("replication")
        .join("epochs")
        .join("reused")
        .join("epoch");
    let mut written: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&record)?)?;
    let epoch = written["highest"].as_u64().ok_or("no epoch")?;
    written
        .as_object_mut()
        .ok_or("not an object")?
        .remove("incarnation");
    std::fs::write(&record, written.to_string())?;
    let reached = fleet
        .control_state()
        .and_then(|state| state.database(&database).map(|db| db.epoch().get()))
        .ok_or("no database")?;
    if reached < epoch {
        return Err(format!(
            "the new database is at epoch {reached}, below the copy's {epoch}, which an orphan \
             check catches anyway"
        )
        .into());
    }
    fleet.note(format!(
        "{stale} restarts with an unlabelled copy at epoch {epoch}; the new database is at {reached}"
    ));
    fleet.restart(&stale)?;
    let name = database.clone();
    let host = stale.clone();
    fleet.run_until(LIMIT, "S keeping its copy as an orphan", |fleet| {
        fleet
            .replication(&host)
            .is_some_and(|replication| replication.is_orphan(&name))
    })?;
    Ok(())
}

/// A copy that records no incarnation is kept as an orphan once a database created again under
/// its name holds a random one, whatever the epochs say.
#[test]
fn an_unlabelled_copy_is_an_orphan_once_the_name_has_a_random_incarnation() {
    seeds::run(BINARY, &seeds::FIXED, unlabelled_copy_after_a_recreation);
}

/// In a fleet that never forked, under a control node of 0.8.0, every database has the legacy
/// incarnation: the follower's copy of the dropped database is told apart by its chain hash at
/// its first acknowledgement, and re-seeded, before it counts toward any write.
#[test]
fn a_dropped_databases_copy_never_counts_toward_a_database_created_again() {
    seeds::run(BINARY, &seeds::FIXED, |seed| scenario(seed, Forks::Never));
}

/// Once forks give databases random incarnations, the follower refuses the new database's
/// stream for the copy it records as the dropped one's, and the drop notice deletes that copy
/// though control state holds the name at its epoch again, so the new database replicates there.
#[test]
fn a_dropped_databases_copy_is_neither_followed_nor_kept_for_one_created_again() {
    seeds::run(BINARY, &seeds::FIXED, |seed| scenario(seed, Forks::Made));
}

/// A follower cut off while a forked fleet's name is dropped and created again refuses the
/// promotion's fence, which names the new database's incarnation, though its record names the
/// candidate the holder of the fenced epoch, so the candidate never adopts the dropped log.
#[test]
fn a_dropped_databases_copy_refuses_a_fence_for_one_created_again() {
    seeds::run(BINARY, &seeds::FIXED, recreated_while_a_follower_is_cut_off);
}

/// In a fleet that never forked, a follower that handles a drop notice late, after the name was
/// created again, holds a copy of another incarnation than the new database's, so a promotion
/// after the new primary fails, before it streamed to that follower, never serves the dropped
/// rows nor loses the new database's acknowledged writes.
#[test]
fn a_late_drop_notice_never_lets_a_promotion_serve_the_dropped_database() {
    seeds::run(BINARY, &seeds::FIXED, |seed| {
        late_drop_notice_then_primary_lost(seed, FirstLife::Random)
    });
}

/// As above for a name whose first life a control node of 0.8.0 created, with the legacy
/// incarnation, before it was upgraded: the database created again has a random incarnation, so
/// `(name, 0)` names the first life alone, and its stale copy refuses the new database's fence.
#[test]
fn a_late_drop_notice_of_a_database_made_before_the_upgrade_never_serves_it() {
    seeds::run(BINARY, &seeds::FIXED, |seed| {
        late_drop_notice_then_primary_lost(seed, FirstLife::BeforeTheUpgrade)
    });
}
