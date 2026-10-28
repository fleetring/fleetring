//! The registry at scale, and its configuration.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use fleetringd::engine::{EngineConfig, Execution, SqliteEngine, Statement, Transaction, Value};
use fleetringd::{Acknowledgement, CommittedBatch, Config, Registry, ReplicationSink};

use crate::common::{PAGE_SIZE, TestResult, config, name};

/// Counts batches without keeping them.
#[derive(Debug, Clone, Default)]
struct CountingSink(Arc<AtomicUsize>);

impl ReplicationSink for CountingSink {
    fn publish(&self, _batch: CommittedBatch) -> Acknowledgement {
        self.0.fetch_add(1, Ordering::Relaxed);
        Acknowledgement::done()
    }
}

// About ten seconds, bound by creating 30,000 files and fsyncing them, so it stays out of the
// debug and emulated runs. Run it with `cargo test -p fleetringd --test actor -- --ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "creates 10,000 databases on disk; run with --ignored"]
async fn one_registry_creates_and_writes_ten_thousand_databases_with_256_open() -> TestResult {
    fill(10_000, 256).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_registry_creates_and_writes_many_more_databases_than_it_keeps_open() -> TestResult {
    fill(600, 16).await
}

/// Creates `databases` databases from 64 concurrent writers, with at most `cap` open at once,
/// writes one row to each, and reads a sample back after they've been evicted.
async fn fill(databases: usize, cap: usize) -> TestResult {
    const WORKERS: usize = 64;
    let dir = tempfile::tempdir()?;
    let sink = CountingSink::default();
    let registry = Registry::new(
        SqliteEngine::new(PAGE_SIZE)?,
        sink.clone(),
        dir.path().join("databases"),
        config(Execution::Threads, cap, Duration::from_secs(3600)),
    );
    let most_open = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let workers: Vec<_> = (0..WORKERS)
        .map(|worker| {
            let registry = registry.clone();
            let most_open = Arc::clone(&most_open);
            tokio::spawn(async move {
                for i in (worker..databases).step_by(WORKERS) {
                    let db = name(&format!("tenant-{i}"));
                    // The handle `create` returns may already be evicted by the time it's used,
                    // so write through the registry, which reopens as needed.
                    let _handle = registry.create(&db).await?;
                    registry
                        .execute(
                            &db,
                            Transaction::new([
                                Statement::new("CREATE TABLE t (v INTEGER)"),
                                Statement::with_params(
                                    "INSERT INTO t VALUES (?1)",
                                    [Value::from(i64::try_from(i)?)],
                                ),
                            ]),
                        )
                        .await?;
                    most_open.fetch_max(registry.open_count(), Ordering::Relaxed);
                }
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            })
        })
        .collect();
    for worker in workers {
        worker.await?.map_err(|error| error.to_string())?;
    }
    let elapsed = started.elapsed();
    assert_eq!(
        sink.0.load(Ordering::Relaxed),
        databases,
        "one batch per database"
    );
    assert!(
        most_open.load(Ordering::Relaxed) <= cap,
        "{most_open:?} open at once"
    );
    assert!(registry.open_count() <= cap);
    for i in (0..databases).step_by(databases / 10 - 3) {
        let rows = registry
            .query(
                &name(&format!("tenant-{i}")),
                Statement::new("SELECT v FROM t"),
            )
            .await?;
        assert_eq!(rows.rows, vec![vec![Value::Integer(i64::try_from(i)?)]]);
    }
    registry.close_all().await;
    assert_eq!(registry.open_count(), 0);
    println!("created and wrote {databases} databases in {elapsed:?} with {cap} open");
    Ok(())
}

#[test]
fn engine_config_has_defaults_and_reads_the_engine_section() -> TestResult {
    let minimal = r#"
data_dir = "/tmp/fleetring-test"
listen = "127.0.0.1:7400"

[node]
id = "laptop-1"
"#;
    let config = Config::from_toml(minimal)?;
    assert_eq!(config.engine, EngineConfig::default());
    assert_eq!(config.engine.max_open.get(), 256);
    assert_eq!(config.engine.idle_timeout, Duration::from_secs(300));
    assert_eq!(config.engine.execution, Execution::Threads);

    let tuned = format!(
        "{minimal}\n[engine]\nexecution = \"inline\"\nmax_open = 16\nidle_timeout_secs = 5\n"
    );
    let config = Config::from_toml(&tuned)?;
    assert_eq!(config.engine.execution, Execution::Inline);
    assert_eq!(config.engine.max_open.get(), 16);
    assert_eq!(config.engine.idle_timeout, Duration::from_secs(5));
    assert_eq!(config.engine.page_size, 4096);

    for bad in ["max_open = 0", "execution = \"fibers\"", "idle_timeout = 5"] {
        let text = format!("{minimal}\n[engine]\n{bad}\n");
        assert!(Config::from_toml(&text).is_err(), "{bad}");
    }
    Ok(())
}
