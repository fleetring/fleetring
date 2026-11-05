//! FR-070 phase 3 on the harness: databases expire on their own (ADR-0041, point 4).
//!
//! One control host and three data hosts. An agent creates databases with staggered expiries,
//! and one that outlives the run, while a writer creates and writes to databases of its own.
//! The control node crashes and restarts before the first expiry passes, and the primary of one
//! expiring database crashes before its expiry and comes back after it. By the end:
//!
//! - every database whose expiry passed is gone from control state and from every host's disk,
//!   the crashed primary's copy included;
//! - none was dropped before its expiry by the simulated clock: every `ExpireDatabase` in the
//!   log is stamped at or after the expiry its database was created with, and nothing else
//!   dropped one;
//! - the database whose expiry hasn't passed is still there;
//! - the history checker in [`Fleet::finish`] passes for the writer's databases.

use std::collections::BTreeMap;
use std::time::Duration;

use fleetring_control::{Action, DatabaseName};
use fleetringd::sim::{Ack, CreateAndWrite, Fleet, Operation, Script, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const AGENT: &str = "agent";
const WRITER: &str = "writer";

/// Each expiring database, and how long after its creation it expires.
const EXPIRING: [(&str, u64); 4] = [
    ("scratch-0", 61),
    ("scratch-1", 75),
    ("scratch-2", 90),
    ("scratch-3", 105),
];

/// The database whose expiry is an hour away, so it outlives the run.
const KEPT: &str = "memory";

/// The databases the agent's creations acknowledged.
fn created(fleet: &Fleet) -> Vec<DatabaseName> {
    fleet
        .history()
        .acknowledged()
        .filter_map(|(operation, ack)| match (operation, ack) {
            (Operation::CreateExpiring { database, .. }, Ack::Opened { .. }) => {
                Some(database.clone())
            }
            _ => None,
        })
        .collect()
}

/// The node holding `database`'s lease at its current epoch, if any.
fn primary(fleet: &Fleet, database: &DatabaseName) -> Option<String> {
    let state = fleet.control_state()?;
    let database = state.database(database)?;
    let lease = database.lease()?;
    (lease.epoch() == database.epoch()).then(|| lease.holder().to_string())
}

/// Whether some data host still has a copy of `database`, or a record of its epochs.
fn on_some_disk(fleet: &Fleet, database: &str) -> bool {
    DATA.iter().any(|host| {
        fleet.data_dir(host).is_some_and(|dir| {
            dir.join("databases").join(database).exists()
                || dir
                    .join("replication")
                    .join("epochs")
                    .join(database)
                    .exists()
        })
    })
}

/// Whether control state no longer has `database`.
fn dropped(fleet: &Fleet, database: &str) -> bool {
    database.parse::<DatabaseName>().is_ok_and(|name| {
        fleet
            .control_state()
            .is_some_and(|state| state.database(&name).is_none())
    })
}

fn expiries(seed: u64) -> TestResult {
    let mut fleet = Fleet::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .duration(Duration::from_secs(600))
        .build()?;
    fleet.await_ring(Duration::from_secs(5))?;
    let mut operations = Vec::new();
    for (database, seconds) in EXPIRING.iter().copied().chain([(KEPT, 3_600)]) {
        operations.push(Operation::CreateExpiring {
            database: database.parse()?,
            replication_factor: 3,
            expires_in: Duration::from_secs(seconds),
        });
    }
    fleet.client(AGENT, Script::new(operations));
    fleet.client(WRITER, CreateAndWrite::new(2, 3, 30));
    fleet.run_until(
        Duration::from_secs(20),
        "every expiring database created",
        |fleet| created(fleet).len() == EXPIRING.len() + 1,
    )?;

    // The control node restarts before the first expiry passes; its sweep starts over.
    fleet.run_for(Duration::from_secs(20))?;
    fleet.crash(CONTROL)?;
    fleet.run_for(Duration::from_millis(500))?;
    fleet.restart(CONTROL)?;

    // The primary of the third expiring database crashes before its expiry, and comes back
    // after it: it deletes its copy when it recovers.
    let (crashing, _) = EXPIRING[2];
    let crashing_name: DatabaseName = crashing.parse()?;
    fleet.run_until(
        Duration::from_secs(30),
        "a primary for the third",
        |fleet| primary(fleet, &crashing_name).is_some(),
    )?;
    fleet.run_until(Duration::from_secs(60), "the first expiry", |fleet| {
        dropped(fleet, EXPIRING[0].0)
    })?;
    let primary = primary(&fleet, &crashing_name).ok_or("no primary for the third")?;
    fleet.crash(&primary)?;
    fleet.run_until(Duration::from_secs(60), "the third expiry", |fleet| {
        dropped(fleet, crashing)
    })?;
    fleet.run_for(Duration::from_secs(1))?;
    fleet.restart(&primary)?;

    // A follower of the database whose primary crashed before it noticed the drop has no
    // stream to end; it checks once the control plane has called the database unknown for
    // longer than the flush limit.
    let gone = fleet.run_until(
        Duration::from_secs(90),
        "every expired database gone from control state and every disk",
        |fleet| {
            EXPIRING
                .iter()
                .all(|(database, _)| dropped(fleet, database) && !on_some_disk(fleet, database))
        },
    );
    if gone.is_err() {
        for (database, _) in EXPIRING {
            let left: Vec<&str> = DATA
                .iter()
                .copied()
                .filter(|host| {
                    fleet
                        .data_dir(host)
                        .is_some_and(|dir| dir.join("databases").join(database).exists())
                })
                .collect();
            eprintln!(
                "{database}: dropped {}, copies on {left:?}",
                dropped(&fleet, database)
            );
        }
    }
    gone?;
    assert!(!dropped(&fleet, KEPT), "{KEPT} expires in an hour");
    assert!(on_some_disk(&fleet, KEPT));
    fleet.await_workloads(Duration::from_secs(60))?;
    fleet.await_ring(Duration::from_secs(5))?;
    let report = fleet.finish()?;

    // Every expiring database was dropped by an expiry stamped at or after the expiry it was
    // created with, and by nothing else.
    let mut expiries: BTreeMap<DatabaseName, u64> = BTreeMap::new();
    let mut expired: BTreeMap<DatabaseName, usize> = BTreeMap::new();
    for command in report.commands() {
        match &command.action {
            Action::CreateDatabase {
                name,
                metadata: Some(spec),
                ..
            } => {
                if let Some(at) = spec.expires_at {
                    expiries.insert(name.clone(), at.as_millis());
                }
            }
            Action::ExpireDatabase { name } => {
                let expires_at = expiries
                    .get(name)
                    .ok_or_else(|| format!("{name} expired without an expiry"))?;
                assert!(
                    command.at.as_millis() >= *expires_at,
                    "{name} expired at {} before its expiry {expires_at}",
                    command.at
                );
                *expired.entry(name.clone()).or_default() += 1;
            }
            Action::DropDatabase { name } => {
                return Err(format!("{name} was dropped, not expired").into());
            }
            _ => {}
        }
    }
    for (database, _) in EXPIRING {
        let name: DatabaseName = database.parse()?;
        assert!(expired.contains_key(&name), "{database} never expired");
        assert!(report.state().database(&name).is_none());
    }
    let kept: DatabaseName = KEPT.parse()?;
    assert!(!expired.contains_key(&kept));
    assert!(report.state().database(&kept).is_some());
    Ok(())
}

#[test]
fn databases_expire_through_a_control_restart_and_a_primary_crash() {
    seeds::run(BINARY, &seeds::FIXED_HEAVY, expiries);
}
