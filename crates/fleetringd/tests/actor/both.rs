//! Actor behavior that holds for every engine shape, run on SQLite and on the memory engine.

use std::time::Duration;

use tokio::time::Instant;

use fleetringd::engine::{Execution, Shape, Transaction, Value};

use crate::common::{Fixture, Kind, TestResult, config, name};

const LONG: Duration = Duration::from_secs(3600);
const KINDS: [Kind; 2] = [Kind::Sqlite, Kind::Memory];

#[tokio::test]
async fn writes_are_serialized_and_reads_see_every_commit() -> TestResult {
    for kind in KINDS {
        let fx = Fixture::new(kind, config(Execution::Threads, 8, LONG));
        let db = name("counter");
        fx.create(&db).await?;
        let handle = fx.registry.open(&db).await?;
        let writers: Vec<_> = (0..50)
            .map(|i| {
                let handle = handle.clone();
                let put = fx.put(&format!("k{i}"), i);
                tokio::spawn(async move { handle.execute(Transaction::from(put)).await })
            })
            .collect();
        for writer in writers {
            assert_eq!(writer.await??.rows_affected, 1);
        }
        for i in 0..50 {
            assert_eq!(
                fx.value(&db, &format!("k{i}")).await?,
                Some(Value::Integer(i)),
                "{kind:?}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn positions_advance_once_per_commit_and_never_on_failure() -> TestResult {
    for kind in KINDS {
        let fx = Fixture::new(kind, config(Execution::Threads, 8, LONG));
        let db = name("positions");
        fx.create(&db).await?;
        let handle = fx.registry.open(&db).await?;
        let mut last = handle.position().await?;
        for i in 0..5 {
            handle.execute(Transaction::from(fx.put("k", i))).await?;
            let position = handle.position().await?;
            assert!(position > last, "{kind:?}: {position:?} after {last:?}");
            last = position;
            let failed = handle
                .execute(Transaction::new([fx.put("k", 100), fx.failing()]))
                .await;
            assert!(failed.is_err(), "{kind:?}");
            assert_eq!(handle.position().await?, last, "{kind:?}");
        }
        assert_eq!(
            fx.value(&db, "k").await?,
            Some(Value::Integer(4)),
            "{kind:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_slow_query_on_one_database_does_not_delay_writes_to_another() -> TestResult {
    // A few durable writes against a long query: fsync is slow on some CI runners (about 100 ms on
    // Windows), so many writes could outlast a short query without ever waiting on it.
    const SLOW_MS: i64 = 3000;
    const WRITES: i64 = 3;
    for kind in KINDS {
        let fx = Fixture::new(kind, config(Execution::Threads, 8, LONG));
        let (slow, busy) = (name("slow"), name("busy"));
        fx.create(&slow).await?;
        fx.create(&busy).await?;
        let started = Instant::now();
        let query = {
            let registry = fx.registry.clone();
            let statement = fx.slow_query(SLOW_MS);
            tokio::spawn(async move { registry.query(&slow, statement).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        for i in 0..WRITES {
            fx.registry
                .execute(&busy, Transaction::from(fx.put(&format!("k{i}"), i)))
                .await?;
        }
        let writes_done = started.elapsed();
        assert!(
            !query.is_finished(),
            "{kind:?}: the slow query finished before the writes, after {writes_done:?}"
        );
        query.await??;
        let query_done = started.elapsed();
        assert!(
            writes_done * 2 < query_done,
            "{kind:?}: writes took {writes_done:?}, the query {query_done:?}"
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn idle_databases_close_and_reopen_on_next_use() -> TestResult {
    for kind in KINDS {
        let fx = Fixture::new(kind, config(Execution::Inline, 8, Duration::from_secs(60)));
        let db = name("sleepy");
        fx.create(&db).await?;
        fx.registry
            .execute(&db, Transaction::from(fx.put("k", 1)))
            .await?;
        assert!(fx.registry.is_open(&db));
        tokio::time::sleep(Duration::from_secs(59)).await;
        assert!(
            fx.registry.is_open(&db),
            "{kind:?}: not idle long enough yet"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            !fx.registry.is_open(&db),
            "{kind:?}: closed after the idle timeout"
        );
        assert_eq!(
            fx.value(&db, "k").await?,
            Some(Value::Integer(1)),
            "{kind:?}"
        );
        assert!(fx.registry.is_open(&db), "{kind:?}: reopened on use");
    }
    Ok(())
}

#[tokio::test]
async fn the_least_recently_used_database_is_evicted_at_the_cap() -> TestResult {
    for kind in KINDS {
        let fx = Fixture::new(kind, config(Execution::Threads, 2, LONG));
        let (a, b, c) = (name("a"), name("b"), name("c"));
        fx.create(&a).await?;
        fx.create(&b).await?;
        fx.registry
            .execute(&a, Transaction::from(fx.put("k", 1)))
            .await?;
        fx.create(&c).await?;
        assert!(
            fx.registry.is_open(&a),
            "{kind:?}: used most recently before c"
        );
        assert!(!fx.registry.is_open(&b), "{kind:?}: least recently used");
        assert!(fx.registry.is_open(&c), "{kind:?}");
        assert_eq!(fx.registry.open_count(), 2, "{kind:?}");

        fx.registry
            .execute(&b, Transaction::from(fx.put("k", 2)))
            .await?;
        assert!(
            !fx.registry.is_open(&a),
            "{kind:?}: a was now the least recently used"
        );
        assert_eq!(
            fx.value(&a, "k").await?,
            Some(Value::Integer(1)),
            "{kind:?}"
        );
        assert_eq!(
            fx.value(&b, "k").await?,
            Some(Value::Integer(2)),
            "{kind:?}"
        );
        assert_eq!(fx.registry.open_count(), 2, "{kind:?}");

        let error = fx.registry.create(&a).await.expect_err("a exists");
        assert!(
            error.to_string().contains("could not be opened"),
            "{kind:?}: {error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn shape_tells_callers_which_requests_apply() -> TestResult {
    for kind in KINDS {
        let fx = Fixture::new(kind, config(Execution::Threads, 8, LONG));
        let db = name("shaped");
        fx.create(&db).await?;
        let handle = fx.registry.open(&db).await?;
        match handle.shape() {
            Shape::FrameSource => {
                assert_eq!(kind, Kind::Sqlite);
                assert!(handle.replica_positions().await.is_err());
                handle.checkpoint().await?;
            }
            Shape::SelfReplicating => {
                assert_eq!(kind, Kind::Memory);
                assert!(handle.checkpoint().await.is_err());
                assert!(handle.committed_since(None).await.is_err());
                handle.replica_positions().await?;
            }
            other => panic!("unexpected shape {other:?}"),
        }
    }
    Ok(())
}
