//! Throughput and acknowledgement latency of small replicated transactions, for `docs/perf.md`.
//!
//! Run it in release mode on the machine being measured:
//!
//! ```sh
//! cargo test -p fleetringd --release --test replication -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::Instant;

use fleetringd::replication::ReplicationSettings;

use crate::common::{Cluster, TestResult, name, put, settings, sqlite};

/// Transactions per configuration, split across its writers.
const TRANSACTIONS: usize = 2_000;

fn percentile(sorted: &[Duration], per_mille: usize) -> Duration {
    let index = (sorted.len() * per_mille / 1_000).min(sorted.len() - 1);
    sorted[index]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement, not a check; run it in release mode"]
async fn small_transactions_throughput_and_ack_latency() -> TestResult {
    let small_log = ReplicationSettings {
        checkpoint_threshold: 256 * 1024,
        ..settings()
    };
    println!(
        "| R | Checkpoint every | Writers | Transactions | Throughput (tx/s) | p50 ack | p99 ack |"
    );
    println!("|---|---|---|---|---|---|---|");
    for (settings, label) in [(settings(), "4 MiB"), (small_log, "256 KiB")] {
        let cluster = Cluster::start(&["a", "b", "c"], sqlite(), settings, BTreeMap::new()).await?;
        for (replicas, writers) in [(1_u8, 1_i64), (3, 1), (3, 4), (3, 16), (3, 64)] {
            measure(&cluster, label, replicas, writers).await?;
        }
    }
    Ok(())
}

async fn measure(cluster: &Cluster, label: &str, replicas: u8, writers: i64) -> TestResult {
    let per_writer = TRANSACTIONS / usize::try_from(writers)?;
    {
        let db = name(&format!("perf-{replicas}-{writers}"));
        cluster.create("a", &db, replicas).await?;
        let started = Instant::now();
        let mut tasks = JoinSet::new();
        for writer in 0..writers {
            let registry = cluster.node("a").registry().clone();
            let db = db.clone();
            tasks.spawn(async move {
                let mut latencies = Vec::with_capacity(per_writer);
                for i in 0..per_writer {
                    let k = writer * 1_000_000 + i64::try_from(i).expect("small");
                    let begun = Instant::now();
                    registry.execute(&db, put(k, 64)).await?;
                    latencies.push(begun.elapsed());
                }
                Ok::<_, fleetringd::DatabaseError>(latencies)
            });
        }
        let mut latencies = Vec::new();
        while let Some(done) = tasks.join_next().await {
            latencies.extend(done??);
        }
        let elapsed = started.elapsed();
        latencies.sort();
        let count = latencies.len();
        let rate = u128::try_from(count)? * 1_000_000 / elapsed.as_micros().max(1);
        println!(
            "| {replicas} | {label} | {writers} | {count} | {rate} | {:.2?} | {:.2?} |",
            percentile(&latencies, 500),
            percentile(&latencies, 990),
        );
    }
    Ok(())
}
