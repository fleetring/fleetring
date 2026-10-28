//! The write quorum: a write is acknowledged only once W replicas have stored it durably, and
//! fails clearly when too few replicas are reachable.

use std::collections::BTreeMap;

use fleetringd::DatabaseError;
use fleetringd::ReplicationError;

use crate::common::{
    Cluster, TestResult, Wrap, eventually, guarded, name, put, routes, settings, sqlite,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_write_is_acknowledged_before_the_write_quorum_has_fsynced_it() -> TestResult {
    let (cluster, b, c, log) = proxied().await?;
    let db = name("quorum");
    cluster.create("a", &db, 3).await?;
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
    cluster.await_caught_up("a", &db).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_fail_as_under_replicated_without_a_reachable_quorum() -> TestResult {
    let (cluster, b, c, _log) = proxied().await?;
    let db = name("under");
    cluster.create("a", &db, 3).await?;
    let registry = cluster.node("a").registry().clone();

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

    b.cut.send_replace(true);
    let error = guarded("a write failing as under-replicated", async {
        loop {
            match registry.execute(&db, put(2, 10)).await {
                Err(DatabaseError::Replication(
                    error @ ReplicationError::UnderReplicated { .. },
                )) => {
                    return error;
                }
                _ => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }
    })
    .await?;
    assert_eq!(
        error,
        ReplicationError::UnderReplicated {
            required: 2,
            reachable: 1
        }
    );
    assert!(error.to_string().contains("under-replicated"), "{error}");

    // Once the followers are back, the unacknowledged write replicates too.
    b.cut.send_replace(false);
    c.cut.send_replace(false);
    cluster.await_caught_up("a", &db).await?;
    registry.execute(&db, put(3, 10)).await?;
    Ok(())
}
