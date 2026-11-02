//! FR-042: the lease watch changes before the primary it describes does.
//!
//! A database is in a node's [`LeaseWatch`] before its primary can take a batch, and out of it
//! before the primary is removed. Read in either order around a running stream of serves and
//! stops, a primary is never seen without its lease, and a lease that stays is never seen
//! without its primary.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetringd::replication::{LeaseWatch, PrimaryRole, Replication};

use crate::common::{Cluster, TestResult, name, settings, sqlite};

/// How many times the primary is served at a new epoch and stopped.
const ROUNDS: u64 = 4_000;

fn role(epoch: u64) -> PrimaryRole {
    PrimaryRole::new(ReplicationFactor::ONE, LeaseEpoch::new(epoch))
}

/// Serves `database` at epochs 1, 2, …, stopping it between, and yields after each step so
/// that an observer on another worker sees the steps interleaved.
async fn churn(replication: Replication, database: DatabaseName) -> TestResult {
    for epoch in 1..=ROUNDS {
        replication.serve_primary(database.clone(), role(epoch))?;
        tokio::task::yield_now().await;
        replication.stop_primary(&database);
        tokio::task::yield_now().await;
    }
    Ok(())
}

/// Reads the primary and the watch around each other until `done`, and returns the first pair
/// of reads that contradicts the ordering, if any.
fn observe(
    replication: &Replication,
    watch: &mut LeaseWatch,
    database: &DatabaseName,
    done: &AtomicBool,
) -> Option<String> {
    let epoch_of = |held: &BTreeMap<DatabaseName, LeaseEpoch>| held.get(database).copied();
    while !done.load(Ordering::SeqCst) {
        // A primary seen before and after a read of the watch, at one epoch, was there
        // throughout; the watch must have held its lease.
        let before = replication.serving(database).map(|role| role.epoch);
        let held = epoch_of(&watch.held());
        let after = replication.serving(database).map(|role| role.epoch);
        if let (Some(first), Some(second)) = (before, after)
            && first == second
            && held != Some(first)
        {
            return Some(format!(
                "the primary served epoch {} throughout, but the watch held {held:?}",
                first.get()
            ));
        }
        // A lease in the watch before and after a read of the primary, at one epoch, was there
        // throughout; the primary must have been serving it.
        let before = epoch_of(&watch.held());
        let serving = replication.serving(database).map(|role| role.epoch);
        let after = epoch_of(&watch.held());
        if let (Some(first), Some(second)) = (before, after)
            && first == second
            && serving != Some(first)
        {
            return Some(format!(
                "the watch held epoch {} throughout, but the primary served {serving:?}",
                first.get()
            ));
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lease_watch_changes_before_the_primary_does() -> TestResult {
    let cluster = Cluster::start(&["a"], sqlite(), settings(), BTreeMap::new()).await?;
    let node = cluster.node("a");
    let database = name("churned");
    node.registry().create(&database).await?;
    let replication = node.replication.clone();
    let done = Arc::new(AtomicBool::new(false));

    let observer = {
        let replication = replication.clone();
        let mut watch = replication.leases();
        let database = database.clone();
        let done = Arc::clone(&done);
        tokio::task::spawn_blocking(move || observe(&replication, &mut watch, &database, &done))
    };
    let churned = churn(replication.clone(), database.clone()).await;
    done.store(true, Ordering::SeqCst);
    let contradiction = observer.await?;
    churned?;
    assert_eq!(contradiction, None);
    assert_eq!(replication.serving(&database), None);
    assert!(replication.leases().held().is_empty());
    Ok(())
}
