//! Replicas that missed a drop or an expiry learn of it from its tombstone (FR-077; ADR-0050).
//!
//! Three data nodes hold a database at R = 3. Its primary crashes, and both followers are cut off
//! from the control node, so no drop notice can reach them and no stream ends to make them ask.
//! The database is then dropped, or expires. Once the followers reach the control node again,
//! their heartbeats bring the tombstone, and they delete their copies without a restart; the
//! primary does the same when it comes back. The name is then created again on the same nodes,
//! and the old tombstone, which every heartbeat still carries, deletes nothing.

use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::sim::{DurabilityMode, Fleet, Operation, Script, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: [&str; 3] = ["n1", "n2", "n3"];
const LIMIT: Duration = Duration::from_secs(120);
const DATABASE: &str = "stranded";
/// Longer than the control node goes on telling an unreachable node of a drop.
const NOTICES_GIVEN_UP: Duration = Duration::from_secs(12);

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// How the database goes away while its followers are cut off.
#[derive(Debug, Clone, Copy)]
enum Ending {
    Dropped,
    Expired,
}

fn on_disk(fleet: &Fleet, host: &str, database: &DatabaseName) -> bool {
    fleet
        .data_dir(host)
        .is_some_and(|dir| dir.join("databases").join(database.as_str()).exists())
}

fn copies(fleet: &Fleet, database: &DatabaseName) -> usize {
    NODES
        .into_iter()
        .filter(|host| on_disk(fleet, host, database))
        .count()
}

/// Runs `operations` on a client of their own until each is answered, and fails unless every one
/// was acknowledged.
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
    let history = fleet.history();
    let refused: Vec<String> = history
        .records()
        .iter()
        .filter(|record| record.client() == name && record.outcome().ack().is_none())
        .map(|record| format!("{:?}: {:?}", record.operation(), record.outcome()))
        .collect();
    if refused.is_empty() {
        Ok(())
    } else {
        Err(format!("{client} wasn't acknowledged: {refused:?}").into())
    }
}

fn gone_from_control(fleet: &Fleet, database: &DatabaseName) -> bool {
    fleet
        .control_state()
        .is_some_and(|state| state.database(database).is_none())
}

fn stranded(seed: u64, ending: Ending) -> TestResult {
    let mut builder = Fleet::builder(seed)
        .control(CONTROL)
        .duration(Duration::from_secs(600));
    for node in NODES {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    let database: DatabaseName = DATABASE.parse()?;
    let create = match ending {
        Ending::Dropped => Operation::Create {
            database: database.clone(),
            replication_factor: 3,
            durability: DurabilityMode::Quorum,
        },
        Ending::Expired => Operation::CreateExpiring {
            database: database.clone(),
            replication_factor: 3,
            expires_in: Duration::from_secs(61),
        },
    };
    // The history checker counts a write as lost unless a drop it saw took the database, so a
    // database that expires takes none.
    let writes = (0..5)
        .filter(|_| matches!(ending, Ending::Dropped))
        .map(|value| Operation::Write {
            database: database.clone(),
            value,
        });
    run_script(
        &mut fleet,
        "first",
        std::iter::once(create).chain(writes).collect(),
    )?;
    fleet.await_replicas_converged(LIMIT)?;
    let watched = database.clone();
    fleet.run_until(LIMIT, "a copy on every node", |fleet| {
        copies(fleet, &watched) == NODES.len()
    })?;

    // The primary crashes, and neither follower can hear from the control node.
    let state = fleet.control_state().ok_or("no control state")?;
    let primary = state
        .database(&database)
        .and_then(|db| db.lease())
        .ok_or("no lease")?
        .holder()
        .to_string();
    let followers: Vec<&str> = NODES.into_iter().filter(|node| *node != primary).collect();
    fleet.crash(&primary)?;
    for follower in &followers {
        fleet.partition(follower, CONTROL)?;
    }
    match ending {
        Ending::Dropped => run_script(
            &mut fleet,
            "drop",
            vec![Operation::Drop {
                database: database.clone(),
            }],
        )?,
        Ending::Expired => {
            let watched = database.clone();
            fleet.run_until(LIMIT, "the expiry", |fleet| {
                gone_from_control(fleet, &watched)
            })?;
        }
    }
    assert!(gone_from_control(&fleet, &database));
    let tombstones = fleet.control_state().ok_or("no control state")?;
    assert_eq!(tombstones.tombstones().of(&database).len(), 1);

    // No notice reaches them, and nothing else may delete their copies.
    fleet.run_for(NOTICES_GIVEN_UP)?;
    for follower in &followers {
        assert!(
            on_disk(&fleet, follower, &database),
            "{follower} kept its copy while it couldn't learn of the drop"
        );
    }

    // Their heartbeats bring the tombstone, with no restart.
    for follower in &followers {
        fleet.repair(follower, CONTROL)?;
    }
    let (watched, hosts) = (database.clone(), followers.clone());
    fleet.run_until(LIMIT, "both followers deleting their copies", |fleet| {
        hosts.iter().all(|host| !on_disk(fleet, host, &watched))
    })?;

    // So does the primary, once it is back.
    fleet.restart(&primary)?;
    let watched = database.clone();
    fleet.run_until(LIMIT, "every copy deleted, none an orphan", |fleet| {
        copies(fleet, &watched) == 0
            && NODES.into_iter().all(|host| {
                fleet
                    .replication(host)
                    .is_some_and(|replication| !replication.is_orphan(&watched))
            })
    })?;
    fleet.await_ring(Duration::from_secs(10))?;

    // The name again, on the same nodes: the old tombstone deletes none of it.
    let again = std::iter::once(Operation::Create {
        database: database.clone(),
        replication_factor: 3,
        durability: DurabilityMode::Quorum,
    })
    .chain((1_000..1_005).map(|value| Operation::Write {
        database: database.clone(),
        value,
    }))
    .collect();
    run_script(&mut fleet, "again", again)?;
    fleet.await_replicas_converged(LIMIT)?;
    fleet.run_for(Duration::from_secs(5))?;
    assert_eq!(copies(&fleet, &database), NODES.len());
    fleet.finish()?;
    Ok(())
}

/// A dropped database's followers delete their copies, though its primary crashed before the
/// drop and no notice reached them, once they learn its tombstone.
#[test]
fn followers_of_a_crashed_primary_learn_a_drop_from_its_tombstone() {
    seeds::run(BINARY, &seeds::FIXED, |seed| {
        stranded(seed, Ending::Dropped)
    });
}

/// The same for a database that expires.
#[test]
fn followers_of_a_crashed_primary_learn_an_expiry_from_its_tombstone() {
    seeds::run(BINARY, &seeds::FIXED, |seed| {
        stranded(seed, Ending::Expired)
    });
}
