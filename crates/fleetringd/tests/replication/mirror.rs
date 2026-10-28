//! Followers mirror the primary byte for byte.

use std::collections::BTreeMap;
use std::fs;

use fleetringd::engine::{Statement, Transaction, Value};
use fleetringd::replication::ReplicationSettings;
use tokio::task::JoinSet;

use crate::common::{Cluster, TestResult, eventually, name, put, settings, sqlite};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caught_up_followers_are_byte_identical_to_the_primary_once_both_checkpoint() -> TestResult
{
    // A small threshold, so the workload crosses several automatic checkpoints.
    let settings = ReplicationSettings {
        checkpoint_threshold: 16 * 1024,
        ..settings()
    };
    let cluster = Cluster::start(&["a", "b", "c"], sqlite(), settings, BTreeMap::new()).await?;
    let db = name("mirrored");
    cluster.create("a", &db, 3).await?;

    let mut writers = JoinSet::new();
    for writer in 0..4_i64 {
        let registry = cluster.node("a").registry().clone();
        let db = db.clone();
        writers.spawn(async move {
            for i in 0..120_i64 {
                let k = writer * 1_000 + i;
                let len = usize::try_from(1 + (i * 37) % 700).expect("small");
                registry.execute(&db, put(k, len)).await?;
                if i % 7 == 0 {
                    registry
                        .execute(
                            &db,
                            Transaction::from(Statement::with_params(
                                "DELETE FROM kv WHERE k = ?1",
                                [Value::Integer(k - 3)],
                            )),
                        )
                        .await?;
                }
                if i % 11 == 0 {
                    registry.execute(&db, put(writer * 1_000, 2_000)).await?;
                }
            }
            Ok::<_, fleetringd::DatabaseError>(())
        });
    }
    while let Some(written) = writers.join_next().await {
        written??;
    }
    cluster.await_caught_up("a", &db).await?;
    let primary = &cluster.node("a").replication;
    let status = primary.primary_status(&db).ok_or("not primary")?;
    assert!(
        status.checkpointed.is_some(),
        "the workload crossed automatic checkpoints"
    );

    let marker = primary.checkpoint(&db).await?;
    for follower in ["b", "c"] {
        let replication = &cluster.node(follower).replication;
        eventually("the follower checkpointing at the marker", || {
            replication
                .follower_status(&db)
                .is_some_and(|status| status.checkpointed.as_ref() == Some(&marker.position))
        })
        .await?;
    }
    let file = |node: &str| fs::read(cluster.node(node).database_dir("mirrored").join("data.db"));
    let expected = file("a")?;
    for follower in ["b", "c"] {
        let copy = file(follower)?;
        assert_eq!(copy.len(), expected.len(), "{follower}'s database size");
        assert!(
            copy == expected,
            "{follower}'s database differs from the primary's"
        );
    }
    Ok(())
}
