//! A slow or failing sink never delays a write's acknowledgement.
//!
//! Timing isn't measured. Instead the history is causal: every write completes while the sink
//! is provably stuck (it has taken nothing, or has failed everything so far), so no
//! acknowledgement waited for it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fleetringd::changes::{
    Change, ChangeSink, ConfiguredSink, Format, MemorySink, Published, SinkError,
};
use tokio::sync::watch;

use crate::common::{Harness, TestResult, eventually, every_database, insert, name};

/// Takes nothing until the gate opens.
#[derive(Debug)]
struct GatedSink {
    inner: MemorySink,
    gate: watch::Receiver<bool>,
    offered: Arc<AtomicU64>,
}

impl ChangeSink for GatedSink {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn publish(&self, change: Change) -> Published {
        self.offered.fetch_add(1, Ordering::SeqCst);
        let mut gate = self.gate.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            // An error means the test dropped the gate; the publish never completes then.
            if gate.wait_for(|open| *open).await.is_err() {
                std::future::pending::<()>().await;
            }
            inner.publish(change).await
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stuck_sink_delays_no_acknowledgement() -> TestResult {
    let harness = Harness::start().await?;
    let (open, gate) = watch::channel(false);
    let inner = MemorySink::new("slow");
    let offered = Arc::new(AtomicU64::new(0));
    let node = harness
        .node(vec![ConfiguredSink::new(
            GatedSink {
                inner: inner.clone(),
                gate,
                offered: Arc::clone(&offered),
            },
            Format::Logical,
            every_database(),
        )])
        .await?;
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    // Every write is acknowledged while the sink has taken nothing at all.
    for value in 1..=40 {
        node.execute(&ledger, insert(value)).await?;
        assert!(
            inner.changes().is_empty(),
            "the sink took something while gated"
        );
    }
    let status = node.streams.status();
    assert_eq!(status[0].position, None);
    assert!(status[0].acknowledged.is_some());
    // The pump offered the sink one change and is waiting on it: it holds no backlog in
    // memory, and reads the rest from the log once the sink is ready.
    eventually("the first offer", || offered.load(Ordering::SeqCst) >= 1).await?;
    assert_eq!(offered.load(Ordering::SeqCst), 1);

    open.send_replace(true);
    node.await_caught_up().await?;
    let delivered: Vec<i64> = inner
        .changes()
        .iter()
        .flat_map(|change| match &change.kind {
            fleetringd::changes::ChangeKind::Logical(rows) => rows.clone(),
            fleetringd::changes::ChangeKind::Physical(_) => Vec::new(),
        })
        .filter(|row| row.table == "t")
        .filter_map(|row| match row.values.first().map(|column| &column.value) {
            Some(fleetringd::engine::Value::Integer(value)) => Some(*value),
            _ => None,
        })
        .collect();
    assert_eq!(delivered, (1..=40).collect::<Vec<_>>());
    assert!(
        inner.changes().len() <= 2,
        "the backlog was read from the log as one batch: {} changes",
        inner.changes().len()
    );
    node.kill().await;
    Ok(())
}

/// Fails the first `failures` publishes, then takes everything.
#[derive(Debug)]
struct FlakySink {
    inner: MemorySink,
    remaining: AtomicU64,
    failed: Arc<AtomicU64>,
}

impl ChangeSink for FlakySink {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn publish(&self, change: Change) -> Published {
        let fail = loop {
            let left = self.remaining.load(Ordering::SeqCst);
            if left == 0 {
                break false;
            }
            let taken =
                self.remaining
                    .compare_exchange(left, left - 1, Ordering::SeqCst, Ordering::SeqCst);
            if taken.is_ok() {
                break true;
            }
        };
        if fail {
            self.failed.fetch_add(1, Ordering::SeqCst);
            return Box::pin(std::future::ready(Err(SinkError::Failed {
                reason: "not now".to_owned(),
            })));
        }
        self.inner.publish(change)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_sink_delays_no_acknowledgement_and_is_retried() -> TestResult {
    let harness = Harness::start().await?;
    let inner = MemorySink::new("flaky");
    let failed = Arc::new(AtomicU64::new(0));
    let node = harness
        .node(vec![ConfiguredSink::new(
            FlakySink {
                inner: inner.clone(),
                remaining: AtomicU64::new(3),
                failed: Arc::clone(&failed),
            },
            Format::Logical,
            every_database(),
        )])
        .await?;
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    for value in 1..=10 {
        node.execute(&ledger, insert(value)).await?;
    }
    node.await_caught_up().await?;
    assert_eq!(failed.load(Ordering::SeqCst), 3);
    let status = node.streams.status();
    assert_eq!(status[0].failures, 3);
    assert_eq!(
        status[0].last_error.as_deref(),
        Some("sink failed: not now")
    );
    assert_eq!(status[0].position, status[0].acknowledged);
    let rows: usize = inner
        .changes()
        .iter()
        .map(|change| match &change.kind {
            fleetringd::changes::ChangeKind::Logical(rows) => {
                rows.iter().filter(|row| row.table == "t").count()
            }
            fleetringd::changes::ChangeKind::Physical(_) => 0,
        })
        .sum();
    assert_eq!(rows, 10, "every write arrived once the sink recovered");
    node.kill().await;
    Ok(())
}
