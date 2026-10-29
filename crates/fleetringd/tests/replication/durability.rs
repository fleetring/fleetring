//! FR-035: the `acks` modes and the replica floor on a primary's own replication (ADR-0023).
//!
//! Nodes `b` and `c` sit behind proxies that can hold their acknowledgements back or cut their
//! streams, so a test sees exactly which acknowledgements a write waited for, and what a primary
//! does when too few replicas are reachable.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{Acks, Durability};
use fleetringd::engine::Position;
use fleetringd::replication::{PrimaryRole, Replication};
use fleetringd::{DatabaseError, ReplicationError};

use crate::common::{
    Cluster, TestResult, Wrap, eventually, guarded, name, put, role, routes, settings, sqlite,
};
use crate::proxy::{Event, Log, Proxy, Switches};

/// Starts nodes `a`, `b`, and `c`, with `b` and `c` behind proxies.
async fn proxied() -> Result<(Cluster, Switches, Switches, Log), Box<dyn std::error::Error>> {
    let log = Log::default();
    let b = Switches::new();
    let c = Switches::new();
    let mut wraps: BTreeMap<String, Wrap> = BTreeMap::new();
    for (node, switches) in [("b", b.clone()), ("c", c.clone())] {
        let log = log.clone();
        wraps.insert(
            node.to_owned(),
            Box::new(move |replication| {
                routes(Proxy {
                    node: node.to_owned(),
                    inner: replication.service(),
                    switches,
                    log,
                })
            }),
        );
    }
    let cluster = Cluster::start(&["a", "b", "c"], sqlite(), settings(), wraps).await?;
    Ok((cluster, b, c, log))
}

/// The role of a three-replica database in `acks`, with the floor at the mode's W.
fn three(acks: Acks) -> PrimaryRole {
    let factor = ReplicationFactor::new(3).expect("nonzero");
    role(3).with_durability(Durability::new(acks, factor))
}

/// Where `log` first shows `node` acknowledging `end` or later.
fn acked_at(events: &[Event], node: &str, end: &Position) -> Option<usize> {
    events.iter().position(
        |event| matches!(event, Event::Acked { node: by, position } if by == node && position >= end),
    )
}

/// The bytes of a database's files on disk: the main file and its log.
fn files(dir: &Path) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    let main = std::fs::read(dir.join("data.db"))?;
    let wal = dir.join("data.db-wal");
    let wal = if wal.try_exists()? {
        std::fs::read(wal)?
    } else {
        Vec::new()
    };
    Ok((main, wal))
}

/// Waits until the primary sees `node`'s stream closed.
async fn disconnected(
    replication: &Replication,
    db: &fleetring_control::DatabaseName,
    node: &str,
) -> TestResult {
    eventually("the stream closing", || {
        replication.primary_status(db).is_some_and(|status| {
            status
                .followers
                .iter()
                .any(|follower| follower.node.as_str() == node && !follower.connected)
        })
    })
    .await
}

/// Writes until the primary refuses a write below its floor, and returns the refusal.
async fn until_refused(
    replication: &Replication,
    db: &fleetring_control::DatabaseName,
    first_key: i64,
) -> Result<ReplicationError, Box<dyn std::error::Error>> {
    let registry = replication.registry().clone();
    let mut key = first_key;
    guarded("a write refused below the floor", async {
        loop {
            match registry.execute(db, put(key, 10)).await {
                Err(DatabaseError::Refused { source, .. }) => return source,
                // A write that slipped in before the primary noticed the cut.
                Ok(_) | Err(_) => {
                    key += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_waits_for_every_replica_and_refuses_writes_without_one() -> TestResult {
    let (cluster, b, c, log) = proxied().await?;
    let db = name("everyone");
    cluster.create_as("a", &db, three(Acks::All)).await?;
    let primary = &cluster.node("a").replication;
    let before = primary.primary_status(&db).ok_or("not primary")?.committed;

    // Both followers apply and fsync the write, but their acknowledgements are held back.
    b.pass_acks.send_replace(false);
    c.pass_acks.send_replace(false);
    let write = {
        let registry = cluster.node("a").registry().clone();
        let db = db.clone();
        let log = log.clone();
        tokio::spawn(async move {
            let result = registry.execute(&db, put(1, 100)).await;
            log.push(Event::Written);
            result
        })
    };
    for follower in ["b", "c"] {
        let replication = &cluster.node(follower).replication;
        eventually("the follower applying the write", || {
            replication
                .follower_status(&db)
                .is_some_and(|status| status.position > before)
        })
        .await?;
    }
    assert!(
        !write.is_finished(),
        "a write returned before any acknowledgement"
    );
    let end = primary
        .primary_status(&db)
        .ok_or("not primary")?
        .committed
        .ok_or("no commit")?;

    // One acknowledgement is a majority, but `all` needs both.
    b.pass_acks.send_replace(true);
    eventually("b's acknowledgement", || {
        acked_at(&log.events(), "b", &end).is_some()
    })
    .await?;
    assert!(
        !write.is_finished(),
        "a write in `all` returned after one of two followers acknowledged it"
    );
    assert!(!log.events().contains(&Event::Written));
    c.pass_acks.send_replace(true);
    let stored = guarded("the write completing", write).await???;
    assert_eq!(stored.acknowledged.map(|ack| ack.acknowledged_by), Some(3));
    let events = log.events();
    let written = events
        .iter()
        .position(|event| *event == Event::Written)
        .ok_or("no write")?;
    for follower in ["b", "c"] {
        let acked = acked_at(&events, follower, &end);
        assert!(
            acked.is_some_and(|acked| acked < written),
            "the write returned before {follower} acknowledged it: {events:?}"
        );
    }
    cluster.await_caught_up("a", &db).await?;

    // With one replica gone, `all` can't be met: the floor (W = 3) refuses writes before they
    // run, and the primary's files don't change.
    c.cut.send_replace(true);
    disconnected(primary, &db, "c").await?;
    let refused = until_refused(primary, &db, 2).await?;
    assert_eq!(
        refused,
        ReplicationError::BelowFloor {
            min_replicas: 3,
            reachable: 2
        }
    );
    let dir = cluster.node("a").database_dir("everyone");
    let committed = primary.primary_status(&db).ok_or("not primary")?.committed;
    let snapshot = files(&dir)?;
    for key in 100..103 {
        let error = cluster
            .node("a")
            .registry()
            .execute(&db, put(key, 10))
            .await
            .expect_err("still below the floor");
        assert!(matches!(error, DatabaseError::Refused { .. }), "{error}");
    }
    assert_eq!(files(&dir)?, snapshot, "refused writes changed the files");
    assert_eq!(
        primary.primary_status(&db).ok_or("not primary")?.committed,
        committed
    );

    // Once the replica is back, writes go through again.
    c.cut.send_replace(false);
    guarded("a write after the replica returns", async {
        loop {
            if cluster
                .node("a")
                .registry()
                .execute(&db, put(3, 10))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    cluster.await_caught_up("a", &db).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_below_the_replica_floor_are_refused_before_they_run() -> TestResult {
    let (cluster, b, c, _log) = proxied().await?;
    let db = name("floor");
    let factor = ReplicationFactor::new(3).expect("nonzero");
    // A majority acknowledges, but every replica must be reachable for a write to run.
    let strict = role(3).with_durability(Durability::with_min_replicas(Acks::Quorum, 3, factor)?);
    cluster.create_as("a", &db, strict).await?;
    let primary = &cluster.node("a").replication;
    let registry = cluster.node("a").registry().clone();
    let stored = registry.execute(&db, put(1, 10)).await?;
    assert_eq!(stored.acknowledged.map(|ack| ack.reachable), Some(3));

    // One replica unreachable: a majority is still there, but the floor isn't met.
    c.cut.send_replace(true);
    disconnected(primary, &db, "c").await?;
    let refused = until_refused(primary, &db, 2).await?;
    assert_eq!(
        refused,
        ReplicationError::BelowFloor {
            min_replicas: 3,
            reachable: 2
        }
    );
    assert!(
        refused.to_string().contains("nothing committed"),
        "{refused}"
    );
    let dir = cluster.node("a").database_dir("floor");
    let snapshot = files(&dir)?;
    let committed = primary.primary_status(&db).ok_or("not primary")?.committed;
    for key in 100..105 {
        let error = registry
            .execute(&db, put(key, 500))
            .await
            .expect_err("still below the floor");
        assert!(matches!(error, DatabaseError::Refused { .. }), "{error}");
    }
    assert_eq!(files(&dir)?, snapshot, "refused writes changed the files");
    assert_eq!(
        primary.primary_status(&db).ok_or("not primary")?.committed,
        committed,
        "refused writes moved the log"
    );
    // The floor counts the primary: with both followers gone, two are missing.
    b.cut.send_replace(true);
    disconnected(primary, &db, "b").await?;
    let refused = until_refused(primary, &db, 200).await?;
    assert_eq!(
        refused,
        ReplicationError::BelowFloor {
            min_replicas: 3,
            reachable: 1
        }
    );

    b.cut.send_replace(false);
    c.cut.send_replace(false);
    guarded("a write once every replica is back", async {
        loop {
            if registry.execute(&db, put(3, 10)).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    cluster.await_caught_up("a", &db).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_local_acknowledges_after_the_primarys_own_fsync() -> TestResult {
    let (cluster, b, c, log) = proxied().await?;
    let db = name("alone");
    cluster.create_as("a", &db, three(Acks::Local)).await?;
    let primary = &cluster.node("a").replication;

    // No follower acknowledges, and the write still returns, held by the primary alone.
    b.pass_acks.send_replace(false);
    c.pass_acks.send_replace(false);
    let stored = guarded(
        "a local write",
        cluster.node("a").registry().execute(&db, put(1, 100)),
    )
    .await??;
    log.push(Event::Written);
    let acknowledged = stored.acknowledged.ok_or("no acknowledgement")?;
    assert_eq!(acknowledged.acknowledged_by, 1, "{acknowledged:?}");
    assert_eq!(acknowledged.replicas, 3);
    assert!(!acknowledged.is_under_replicated());
    let end = primary
        .primary_status(&db)
        .ok_or("not primary")?
        .committed
        .ok_or("no commit")?;
    let events = log.events();
    let written = events
        .iter()
        .position(|event| *event == Event::Written)
        .ok_or("no write")?;
    for follower in ["b", "c"] {
        assert!(
            acked_at(&events[..written], follower, &end).is_none(),
            "{follower} acknowledged before the write returned: {events:?}"
        );
    }

    // The floor is 1, so even without any follower the primary keeps taking writes.
    b.pass_acks.send_replace(true);
    c.pass_acks.send_replace(true);
    cluster.await_caught_up("a", &db).await?;
    b.cut.send_replace(true);
    c.cut.send_replace(true);
    eventually("both streams cut", || {
        primary
            .primary_status(&db)
            .is_some_and(|status| status.followers.iter().all(|follower| !follower.connected))
    })
    .await?;
    let stored = cluster
        .node("a")
        .registry()
        .execute(&db, put(2, 10))
        .await?;
    let acknowledged = stored.acknowledged.ok_or("no acknowledgement")?;
    assert_eq!(
        (acknowledged.acknowledged_by, acknowledged.reachable),
        (1, 1)
    );
    assert!(acknowledged.is_under_replicated());
    b.cut.send_replace(false);
    c.cut.send_replace(false);
    cluster.await_caught_up("a", &db).await
}
