//! Followers mirror the primary byte for byte, `vec0` vector tables included.

use std::collections::BTreeMap;
use std::fs;

use fleetringd::engine::{DatabaseEngine as _, Interrupt, OpenMode, Statement, Transaction, Value};
use fleetringd::replication::ReplicationSettings;
use tokio::task::JoinSet;

use crate::common::{Fleet, TestResult, checkpoint, eventually, name, put, settings, sqlite};

/// An eight-dimensional vector for row `i`, as the JSON text `vec0` takes, from integers alone.
fn embedding(i: i64) -> String {
    let parts: Vec<String> = (1..=8_i64)
        .map(|d| ((i * (2 * d + 1) + d * d) % 17 - 8).to_string())
        .collect();
    format!("[{}]", parts.join(", "))
}

/// The ten rows of `vectors` nearest row 7's vector, nearest first, with their distances.
fn nearest() -> Statement {
    Statement::with_params(
        "SELECT rowid, distance FROM vectors WHERE embedding MATCH ?1 AND k = 10 ORDER BY distance",
        [Value::from(embedding(7))],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vec0_table_replicates_and_answers_the_same_k_nearest_query_on_every_follower()
-> TestResult {
    let fleet = Fleet::start(&["a", "b", "c"], sqlite(), settings(), BTreeMap::new()).await?;
    let db = name("vectors");
    fleet.create("a", &db, 3).await?;
    let registry = fleet.node("a").registry();
    // Small chunks, so the vectors span several of the module's chunk rows.
    registry
        .execute(
            &db,
            Transaction::from(Statement::new(
                "CREATE VIRTUAL TABLE vectors USING vec0(embedding float[8], chunk_size = 8)",
            )),
        )
        .await?;
    for batch in 0..4_i64 {
        let inserts = (0..16_i64).map(|offset| {
            let i = batch * 16 + offset;
            Statement::with_params(
                "INSERT INTO vectors (rowid, embedding) VALUES (?1, ?2)",
                [Value::Integer(i), Value::from(embedding(i))],
            )
        });
        registry.execute(&db, Transaction::new(inserts)).await?;
    }
    registry
        .execute(
            &db,
            Transaction::new([
                Statement::new("DELETE FROM vectors WHERE rowid % 9 = 0"),
                Statement::with_params(
                    "UPDATE vectors SET embedding = ?1 WHERE rowid = 20",
                    [Value::from(embedding(7))],
                ),
            ]),
        )
        .await?;
    let expected = registry.query(&db, nearest()).await?;
    assert_eq!(expected.rows.len(), 10);
    assert_eq!(
        expected.rows[0][0],
        Value::Integer(7),
        "row 7 is nearest itself"
    );

    fleet.await_caught_up("a", &db).await?;
    let marker = checkpoint(&fleet.node("a").replication, &db).await?;
    for follower in ["b", "c"] {
        let replication = &fleet.node(follower).replication;
        eventually("the follower checkpointing at the marker", || {
            replication
                .follower_status(&db)
                .is_some_and(|status| status.checkpointed.as_ref() == Some(&marker.position))
        })
        .await?;
    }
    let file = |node: &str| fs::read(fleet.node(node).database_dir("vectors").join("data.db"));
    let primary = file("a")?;
    for follower in ["b", "c"] {
        let copy = file(follower)?;
        assert!(
            copy == primary,
            "{follower}'s database differs from the primary's"
        );
        // The follower's copy, opened as a promotion would open it, answers the same query.
        let scratch = tempfile::tempdir()?;
        fs::write(scratch.path().join("data.db"), &copy)?;
        let mut opened = sqlite().open(scratch.path(), OpenMode::Existing)?;
        let answer = opened.query(&nearest(), &Interrupt::new())?;
        assert_eq!(answer, expected, "{follower}'s k-nearest answer");
        opened.close()?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caught_up_followers_are_byte_identical_to_the_primary_once_both_checkpoint() -> TestResult
{
    // A small threshold, so the workload crosses several automatic checkpoints.
    let settings = ReplicationSettings {
        checkpoint_threshold: 16 * 1024,
        ..settings()
    };
    let fleet = Fleet::start(&["a", "b", "c"], sqlite(), settings, BTreeMap::new()).await?;
    let db = name("mirrored");
    fleet.create("a", &db, 3).await?;

    let mut writers = JoinSet::new();
    for writer in 0..4_i64 {
        let registry = fleet.node("a").registry().clone();
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
    fleet.await_caught_up("a", &db).await?;
    let primary = &fleet.node("a").replication;
    let status = primary.primary_status(&db).ok_or("not primary")?;
    assert!(
        status.checkpointed.is_some(),
        "the workload crossed automatic checkpoints"
    );

    let marker = checkpoint(primary, &db).await?;
    for follower in ["b", "c"] {
        let replication = &fleet.node(follower).replication;
        eventually("the follower checkpointing at the marker", || {
            replication
                .follower_status(&db)
                .is_some_and(|status| status.checkpointed.as_ref() == Some(&marker.position))
        })
        .await?;
    }
    let file = |node: &str| fs::read(fleet.node(node).database_dir("mirrored").join("data.db"));
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
