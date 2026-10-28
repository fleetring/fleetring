//! The SQLite frame source: one batch per commit, no unrequested checkpoints, and followers.

use std::fs;
use std::time::Duration;

use fleetring_wal::{Batch, Position as WalPosition};
use fleetringd::engine::{
    DatabaseEngine, EngineCapability, EngineError, Execution, FrameFollower, Position,
    SqliteEngine, Statement, Transaction, Value,
};
use fleetringd::{CommittedBatch, DatabaseError};
use rusqlite::Connection;

use crate::common::{Fixture, Kind, PAGE_SIZE, TestResult, config, name, wal_file};

const LONG: Duration = Duration::from_secs(3600);

fn sqlite(execution: Execution) -> Fixture {
    Fixture::new(Kind::Sqlite, config(execution, 16, LONG))
}

/// Decodes a published batch and checks that it holds exactly one transaction.
fn decode_one_transaction(published: &CommittedBatch) -> Batch {
    let batch = Batch::from_bytes(published.batch.payload.as_bytes()).expect("valid payload");
    assert_eq!(published.batch.start.as_bytes(), batch.start().to_bytes());
    assert_eq!(published.batch.end.as_bytes(), batch.end().to_bytes());
    let commits: Vec<bool> = batch
        .frame_headers()
        .map(|frame| frame.is_commit())
        .collect();
    assert_eq!(
        commits.iter().filter(|&&commit| commit).count(),
        1,
        "exactly one commit frame, at the end: {commits:?}"
    );
    assert_eq!(commits.last(), Some(&true));
    batch
}

#[tokio::test]
async fn each_committed_transaction_publishes_exactly_its_frames() -> TestResult {
    let fx = sqlite(Execution::Threads);
    let db = name("orders");
    fx.create(&db).await?;
    let wal = wal_file(fx.dir.path(), "orders");
    let created = fx.sink.take();
    assert_eq!(created.len(), 1, "the CREATE TABLE commit");
    let mut previous = decode_one_transaction(&created[0]);

    let transactions = [
        Transaction::from(fx.put("a", 1)),
        Transaction::new((0..20).map(|i| fx.put(&format!("big-{i}"), vec![7u8; 900]))),
        Transaction::new([
            Statement::new("UPDATE kv SET v = 'x' WHERE k LIKE 'big-%'"),
            Statement::new("DELETE FROM kv WHERE k = 'a'"),
        ]),
        Transaction::from(fx.put("b", "text")),
    ];
    for transaction in transactions {
        fx.registry.execute(&db, transaction).await?;
        let published = fx.sink.take();
        assert_eq!(published.len(), 1, "one batch per commit");
        assert_eq!(published[0].database, db);
        let batch = decode_one_transaction(&published[0]);
        assert_eq!(batch.start(), previous.end(), "batches are contiguous");
        assert_eq!(batch.header(), None, "no checkpoint, so no new generation");
        let on_disk = fs::read(&wal)?;
        let start = usize::try_from(batch.start().offset())?;
        assert_eq!(
            &on_disk[start..],
            batch.frame_bytes(),
            "the batch is exactly the frames the transaction appended"
        );
        assert!(published[0].batch.start < published[0].batch.end);
        previous = batch;
    }

    fx.registry
        .execute(&db, Transaction::from(fx.get("b")))
        .await?;
    assert!(
        fx.sink.take().is_empty(),
        "a transaction that writes nothing commits no frames"
    );
    assert_eq!(fx.value(&db, "b").await?, Some(Value::from("text")));
    Ok(())
}

#[tokio::test]
async fn rollbacks_and_failed_statements_publish_nothing() -> TestResult {
    let fx = sqlite(Execution::Threads);
    let db = name("ledger");
    fx.create(&db).await?;
    fx.registry
        .execute(&db, Transaction::from(fx.put("k", 1)))
        .await?;
    let handle = fx.registry.open(&db).await?;
    let before = handle.position().await?;
    let _ = fx.sink.take();

    let failing = [
        // A later statement fails, so the earlier insert rolls back.
        Transaction::new([fx.put("k", 2), fx.failing()]),
        // A constraint violation.
        Transaction::from(Statement::with_params(
            "INSERT INTO kv (k, v) VALUES (?1, 3)",
            [Value::from("k")],
        )),
        // A syntax error.
        Transaction::from(Statement::new("INSERT INTO")),
        // Statements may not end the transaction themselves.
        Transaction::new([fx.put("k", 4), Statement::new("ROLLBACK")]),
        // Pages spill into the WAL before the failure, and must not be shipped.
        Transaction::new(
            [Statement::new("PRAGMA cache_size = 2")]
                .into_iter()
                .chain((0..200).map(|i| fx.put(&format!("spill-{i}"), vec![1u8; 800])))
                .chain([fx.failing()]),
        ),
    ];
    for transaction in failing {
        let error = fx
            .registry
            .execute(&db, transaction)
            .await
            .expect_err("the transaction fails");
        assert!(
            matches!(
                error.engine_error(),
                Some(EngineError::Statement { .. } | EngineError::TransactionControl)
            ),
            "{error:?}"
        );
        assert!(fx.sink.take().is_empty(), "no batch after {error}");
        assert_eq!(handle.position().await?, before);
    }
    assert_eq!(fx.value(&db, "k").await?, Some(Value::Integer(1)));

    fx.registry
        .execute(&db, Transaction::from(fx.put("k", 5)))
        .await?;
    let published = fx.sink.take();
    assert_eq!(published.len(), 1);
    let batch = decode_one_transaction(&published[0]);
    assert_eq!(Some(Position::new(batch.start().to_bytes())), before);
    Ok(())
}

#[tokio::test]
async fn the_wal_grows_across_thousands_of_writes_until_a_checkpoint_is_requested() -> TestResult {
    const WRITES: usize = 2000;
    let fx = sqlite(Execution::Threads);
    let db = name("growing");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    // An append-only table adds about one frame per write, which keeps the test quick.
    handle
        .execute(Transaction::from(Statement::new(
            "CREATE TABLE log (v INTEGER)",
        )))
        .await?;
    let _ = fx.sink.take();
    let wal = wal_file(fx.dir.path(), "growing");
    let mut len = fs::metadata(&wal)?.len();
    for i in 0..WRITES {
        handle
            .execute(Transaction::from(Statement::with_params(
                "INSERT INTO log VALUES (?1)",
                [Value::Integer(i64::try_from(i)?)],
            )))
            .await?;
        let grown = fs::metadata(&wal)?.len();
        assert!(grown > len, "write {i} appended to the wal");
        len = grown;
    }
    let frame_len = 24 + u64::from(PAGE_SIZE);
    assert!(len >= 32 + frame_len * WRITES as u64, "{len} bytes");
    let batches = fx.sink.take();
    assert_eq!(batches.len(), WRITES);
    assert!(
        batches.iter().all(
            |published| Batch::from_bytes(published.batch.payload.as_bytes())
                .is_ok_and(|batch| batch.header().is_none())
        ),
        "every batch continues the first generation"
    );
    assert!(
        batches
            .windows(2)
            .all(|pair| pair[0].batch.end == pair[1].batch.start),
        "positions advance contiguously"
    );

    let marker = handle.checkpoint().await?;
    assert_eq!(Some(marker.position.clone()), handle.position().await?);
    handle
        .execute(Transaction::from(fx.put("after", 1)))
        .await?;
    let restarted = fx.sink.take();
    assert_eq!(restarted.len(), 1);
    let batch = Batch::from_bytes(restarted[0].batch.payload.as_bytes())?;
    assert!(
        batch.header().is_some(),
        "the next write starts a new generation"
    );
    assert!(
        restarted[0].batch.start > marker.position,
        "the new generation orders later"
    );
    assert_eq!(
        fs::metadata(&wal)?.len(),
        batch.end().offset(),
        "the wal shrank to the new generation"
    );
    Ok(())
}

#[tokio::test]
async fn a_follower_mirrors_the_primary_across_checkpoints_and_reopens() -> TestResult {
    let fx = sqlite(Execution::Threads);
    let db = name("mirrored");
    fx.create(&db).await?;
    fx.registry
        .execute(&db, Transaction::from(fx.put("seed", 0)))
        .await?;
    let handle = fx.registry.open(&db).await?;
    let follower_dir = fx.path("follower");
    let snapshot = handle.snapshot(&follower_dir).await?;
    assert_eq!(snapshot.position, handle.position().await?);
    let _ = fx.sink.take();

    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let EngineCapability::FrameSource(frames) = engine.capability() else {
        panic!("sqlite is a frame source");
    };
    let mut follower = frames.open_follower(&follower_dir)?;
    assert_eq!(follower.position(), snapshot.position);
    let ship = |follower: &mut Box<dyn FrameFollower>| -> TestResult {
        for published in fx.sink.take() {
            let end = follower.apply(&published.batch)?;
            assert_eq!(end, published.batch.end);
        }
        Ok(())
    };

    for round in 0..3_i64 {
        for i in 0..5_i64 {
            fx.registry
                .execute(
                    &db,
                    Transaction::from(fx.put(&format!("r{round}-{i}"), round * i)),
                )
                .await?;
        }
        ship(&mut follower)?;
        let marker = fx.registry.open(&db).await?.checkpoint().await?;
        follower.checkpoint(&marker)?;
        if round == 1 {
            // Closing and reopening keeps the log's order: the next generation still sorts later.
            fx.registry.close_all().await;
            assert_eq!(fx.registry.open_count(), 0);
        }
    }
    fx.registry
        .execute(&db, Transaction::from(fx.put("last", 9)))
        .await?;
    ship(&mut follower)?;
    assert_eq!(
        follower.position(),
        fx.registry.open(&db).await?.position().await?
    );
    drop(follower);

    let primary = fx
        .registry
        .query(&db, Statement::new("SELECT k, v FROM kv ORDER BY k"))
        .await?;
    let copy = Connection::open(follower_dir.join("data.db"))?;
    let mut statement = copy.prepare("SELECT k, v FROM kv ORDER BY k")?;
    let mirrored: Vec<(String, i64)> = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(mirrored.len(), primary.rows.len());
    for (row, (k, v)) in primary.rows.iter().zip(&mirrored) {
        assert_eq!(row, &vec![Value::from(k.as_str()), Value::Integer(*v)]);
    }
    let integrity: String = copy.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    assert_eq!(integrity, "ok");
    Ok(())
}

#[tokio::test]
async fn a_follower_rejects_a_batch_that_skips_ahead() -> TestResult {
    let fx = sqlite(Execution::Threads);
    let db = name("gaps");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    handle.snapshot(fx.path("follower")).await?;
    let _ = fx.sink.take();
    for i in 0..2 {
        handle
            .execute(Transaction::from(fx.put(&format!("k{i}"), i)))
            .await?;
    }
    let batches = fx.sink.take();
    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let EngineCapability::FrameSource(frames) = engine.capability() else {
        panic!("sqlite is a frame source");
    };
    let mut follower = frames.open_follower(&fx.path("follower"))?;
    let error = follower.apply(&batches[1].batch).expect_err("a gap");
    assert!(matches!(error, EngineError::InvalidPayload(_)), "{error:?}");
    follower.apply(&batches[0].batch)?;
    follower.apply(&batches[1].batch)?;

    let caught_up = handle
        .committed_since(Some(batches[0].batch.start.clone()))
        .await?;
    let caught_up = caught_up.expect("two transactions to catch up on");
    assert_eq!(
        caught_up.end, batches[1].batch.end,
        "catch-up joins the batches"
    );
    let bogus = Position::new(WalPosition::default().to_bytes());
    let error = handle
        .committed_since(Some(bogus))
        .await
        .expect_err("not in this log");
    assert!(
        matches!(
            error.engine_error(),
            Some(EngineError::InvalidPosition { .. })
        ),
        "{error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_follower_truncates_a_divergent_tail_and_serves_its_frames() -> TestResult {
    let fx = sqlite(Execution::Threads);
    let db = name("divergent");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    handle.snapshot(fx.path("follower")).await?;
    let _ = fx.sink.take();
    for i in 0..3 {
        handle
            .execute(Transaction::from(fx.put(&format!("k{i}"), i)))
            .await?;
    }
    let batches = fx.sink.take();
    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let EngineCapability::FrameSource(frames) = engine.capability() else {
        panic!("sqlite is a frame source");
    };
    let mut follower = frames.open_follower(&fx.path("follower"))?;
    for published in &batches {
        follower.apply(&published.batch)?;
    }

    // A promoted member reads what it holds after a position, as a candidate fetches it.
    let fetched = follower
        .committed_since(Some(&batches[0].batch.end))
        .expect("readable")
        .expect("two transactions");
    assert_eq!(fetched.start, batches[1].batch.start);
    assert_eq!(fetched.end, batches[2].batch.end);
    assert!(
        follower
            .committed_since(follower.position().as_ref())?
            .is_none()
    );

    // The last two transactions were never acknowledged: cut them off, durably.
    let adopted = batches[0].batch.end.clone();
    follower.truncate_to(&adopted)?;
    assert_eq!(follower.position(), Some(adopted.clone()));
    drop(follower);
    let mut follower = frames.open_follower(&fx.path("follower"))?;
    assert_eq!(
        follower.position(),
        Some(adopted),
        "the cut survives a reopen"
    );
    // The log continues from there, with the same frames or with others.
    follower.apply(&batches[1].batch)?;
    assert_eq!(follower.position(), Some(batches[1].batch.end.clone()));

    // A position that isn't in the copy's log can't be cut back to.
    let bogus = Position::new(WalPosition::default().to_bytes());
    let error = follower.truncate_to(&bogus).expect_err("not in this log");
    assert!(
        matches!(error, EngineError::InvalidPosition { .. }),
        "{error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_follower_never_truncates_to_or_below_its_last_checkpoint() -> TestResult {
    let fx = sqlite(Execution::Threads);
    let db = name("checkpointed");
    fx.create(&db).await?;
    let handle = fx.registry.open(&db).await?;
    handle.snapshot(fx.path("follower")).await?;
    let start = handle.position().await?;
    handle.execute(Transaction::from(fx.put("a", 1))).await?;
    let behind = handle.position().await?.ok_or("a position")?;
    handle.execute(Transaction::from(fx.put("b", 2))).await?;
    let engine = SqliteEngine::new(PAGE_SIZE)?;
    let EngineCapability::FrameSource(frames) = engine.capability() else {
        panic!("sqlite is a frame source");
    };
    let mut follower = frames.open_follower(&fx.path("follower"))?;
    follower.apply(
        &handle
            .committed_since(start)
            .await?
            .ok_or("the first generation")?,
    )?;
    let marker = handle.checkpoint().await?;
    handle.execute(Transaction::from(fx.put("c", 3))).await?;
    handle.execute(Transaction::from(fx.put("d", 4))).await?;
    let next = handle
        .committed_since(Some(marker.position.clone()))
        .await?
        .ok_or("the next generation")?;
    // The follower records the primary's checkpoint as the next generation reaches it.
    follower.apply(&next)?;
    drop(follower);
    let mut follower = frames.open_follower(&fx.path("follower"))?;

    // It serves the next generation from the checkpoint, as the primary does, and refuses every
    // other earlier-generation position.
    let served = follower
        .committed_since(Some(&marker.position))?
        .ok_or("the next generation")?;
    assert_eq!(served.end, next.end);
    let error = follower
        .committed_since(Some(&behind))
        .expect_err("behind the checkpoint");
    assert!(
        matches!(error, EngineError::InvalidPosition { .. }),
        "{error:?}"
    );

    // The frames up to the checkpoint are in the database file now, so the copy can't be cut
    // back to it or behind it: only a snapshot would do.
    for position in [&marker.position, &behind] {
        let error = follower
            .truncate_to(position)
            .expect_err("at or behind the checkpoint");
        assert!(
            matches!(error, EngineError::InvalidPosition { .. }),
            "{error:?}"
        );
    }
    assert_eq!(follower.position(), Some(next.end), "nothing was cut");
    Ok(())
}

#[tokio::test]
async fn queries_run_on_the_read_connection_and_cannot_write() -> TestResult {
    let fx = sqlite(Execution::Threads);
    let db = name("reads");
    fx.create(&db).await?;
    let error = fx
        .registry
        .query(&db, Statement::new("INSERT INTO kv VALUES ('x', 1)"))
        .await
        .expect_err("queries are read-only");
    assert!(
        matches!(error.engine_error(), Some(EngineError::Statement { .. })),
        "{error:?}"
    );
    fx.registry
        .execute(&db, Transaction::from(fx.put("x", 2)))
        .await?;
    let rows = fx
        .registry
        .query(&db, Statement::new("SELECT k, v FROM kv"))
        .await?;
    assert_eq!(rows.columns, vec!["k".to_owned(), "v".to_owned()]);
    assert_eq!(rows.rows, vec![vec![Value::from("x"), Value::Integer(2)]]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn inline_execution_publishes_the_same_batches() -> TestResult {
    let fx = sqlite(Execution::Inline);
    let db = name("inline");
    fx.create(&db).await?;
    for i in 0..10 {
        fx.registry
            .execute(&db, Transaction::from(fx.put(&format!("k{i}"), i)))
            .await?;
    }
    let batches = fx.sink.take();
    assert_eq!(batches.len(), 11);
    for published in &batches {
        decode_one_transaction(published);
    }
    let missing = fx
        .registry
        .open(&name("missing"))
        .await
        .expect_err("no such database");
    assert!(matches!(missing, DatabaseError::Open { .. }), "{missing:?}");
    Ok(())
}
