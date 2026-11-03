//! The write quorum: a write is acknowledged only once W replicas have stored it durably, and
//! fails clearly when too few replicas are reachable.

use std::collections::BTreeMap;

use std::time::Duration;

use fleetringd::DatabaseError;
use fleetringd::ReplicationError;
use fleetringd::replication::ReplicationSettings;

use crate::common::{
    Fleet, TestResult, Wrap, eventually, guarded, name, put, routes, settings, sqlite,
};
use crate::proxy::{Event, Log, Proxy, Switches};

/// Starts nodes `a`, `b`, and `c`, with `b` and `c` behind proxies.
async fn proxied() -> Result<(Fleet, Switches, Switches, Log), Box<dyn std::error::Error>> {
    proxied_with(settings()).await
}

/// As [`proxied`], with the nodes running `settings`.
async fn proxied_with(
    settings: ReplicationSettings,
) -> Result<(Fleet, Switches, Switches, Log), Box<dyn std::error::Error>> {
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
    let fleet = Fleet::start(&["a", "b", "c"], sqlite(), settings, wraps).await?;
    Ok((fleet, b, c, log))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_write_is_acknowledged_before_the_write_quorum_has_fsynced_it() -> TestResult {
    let (fleet, b, c, log) = proxied().await?;
    let db = name("quorum");
    fleet.create("a", &db, 3).await?;
    let primary = &fleet.node("a").replication;
    let before = primary.primary_status(&db).ok_or("not primary")?.committed;

    // Both followers apply and fsync the write, but their acknowledgements are held back.
    b.pass_acks.send_replace(false);
    c.pass_acks.send_replace(false);
    let write = {
        let registry = fleet.node("a").registry().clone();
        let db = db.clone();
        let log = log.clone();
        tokio::spawn(async move {
            let result = registry.execute(&db, put(1, 100)).await;
            log.push(Event::Written);
            result
        })
    };
    for follower in ["b", "c"] {
        let replication = &fleet.node(follower).replication;
        eventually("the follower applying the write", || {
            replication
                .follower_status(&db)
                .is_some_and(|status| status.position > before)
        })
        .await?;
    }
    assert!(
        !write.is_finished(),
        "a write returned before any follower acknowledged it"
    );
    let end = primary
        .primary_status(&db)
        .ok_or("not primary")?
        .committed
        .ok_or("no commit")?;
    assert!(!log.events().contains(&Event::Written));

    // R = 3, so W = 2: one follower's acknowledgement completes the write.
    b.pass_acks.send_replace(true);
    guarded("the write completing", write).await???;
    let events = log.events();
    let written = events
        .iter()
        .position(|event| *event == Event::Written)
        .ok_or("no write")?;
    let acked_by_b = events.iter().position(
        |event| matches!(event, Event::Acked { node, position } if node == "b" && *position >= end),
    );
    assert!(
        acked_by_b.is_some_and(|acked| acked < written),
        "the write returned before b acknowledged it: {events:?}"
    );
    assert!(
        !events[..written].iter().any(|event| {
            matches!(event, Event::Acked { node, position } if node == "c" && *position >= end)
        }),
        "c's acknowledgement was held back: {events:?}"
    );
    c.pass_acks.send_replace(true);
    fleet.await_caught_up("a", &db).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_are_refused_without_a_reachable_quorum() -> TestResult {
    // This test does rely on the acknowledgement timeout: a write that slips in just before the
    // primary sees `b`'s stream close commits unacknowledged and ends only at the timeout, after
    // which the next write is refused. At the hang guard that one write would use up the guard
    // below, so keep the old 10 seconds here.
    let (fleet, b, c, _log) = proxied_with(ReplicationSettings {
        ack_timeout: Duration::from_secs(10),
        ..settings()
    })
    .await?;
    let db = name("under");
    fleet.create("a", &db, 3).await?;
    let registry = fleet.node("a").registry().clone();

    // One follower is enough for R = 3.
    c.cut.send_replace(true);
    guarded("a write with one follower", async {
        loop {
            if registry.execute(&db, put(1, 10)).await.is_ok() {
                return;
            }
        }
    })
    .await?;

    // With the floor at W, a write that arrives once the primary has seen the stream close is
    // refused before it runs; one that slips in just before commits unacknowledged.
    b.cut.send_replace(true);
    let error = guarded("a write refused without a quorum", async {
        loop {
            match registry.execute(&db, put(2, 10)).await {
                Err(DatabaseError::Replication(
                    error @ ReplicationError::UnderReplicated { .. },
                )) => {
                    assert_eq!(
                        error,
                        ReplicationError::UnderReplicated {
                            required: 2,
                            reachable: 1
                        }
                    );
                    assert!(error.to_string().contains("under-replicated"), "{error}");
                }
                Err(DatabaseError::Refused { source, .. }) => return source,
                _ => {}
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    assert_eq!(
        error,
        ReplicationError::BelowFloor {
            min_replicas: 2,
            reachable: 1
        }
    );
    assert!(error.to_string().contains("nothing committed"), "{error}");

    // Once the followers are back, the unacknowledged write replicates too.
    b.cut.send_replace(false);
    c.cut.send_replace(false);
    fleet.await_caught_up("a", &db).await?;
    registry.execute(&db, put(3, 10)).await?;
    Ok(())
}

/// A write waiting for its quorum when the node stops being the primary, as a drain or a newer
/// epoch does, fails at once: no follower will acknowledge it to a primary that is gone, so
/// waiting out the acknowledgement timeout only holds the client up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_waiting_for_its_quorum_fails_when_its_primary_steps_down() -> TestResult {
    // An acknowledgement timeout far beyond the hang guard, so that only the step down can end
    // the wait inside it.
    let (fleet, b, c, _log) = proxied_with(ReplicationSettings {
        ack_timeout: Duration::from_secs(3_600),
        ..settings()
    })
    .await?;
    let db = name("stepdown");
    fleet.create("a", &db, 3).await?;
    let primary = &fleet.node("a").replication;
    let before = primary.primary_status(&db).ok_or("not primary")?.committed;

    b.pass_acks.send_replace(false);
    c.pass_acks.send_replace(false);
    let write = {
        let registry = fleet.node("a").registry().clone();
        let db = db.clone();
        tokio::spawn(async move { registry.execute(&db, put(1, 100)).await })
    };
    // The write has committed and been published once the primary's log moves on.
    eventually("the write committing", || {
        primary
            .primary_status(&db)
            .is_some_and(|status| status.committed > before)
    })
    .await?;
    assert!(!write.is_finished(), "the write returned without a quorum");

    primary.stop_primary(&db);
    match guarded("the write failing", write).await?? {
        Err(DatabaseError::Replication(ReplicationError::Failed { reason })) => {
            assert!(reason.contains("stopped being"), "{reason}");
            Ok(())
        }
        other => Err(format!("expected the write to fail, got {other:?}").into()),
    }
}
