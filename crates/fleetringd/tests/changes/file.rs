//! The file sink: every write arrives in commit order, exactly once after a clean run, and at
//! least once across a pump that is killed mid-stream and restarted.

use std::path::Path;
use std::sync::Arc;

use fleetring::ReplicationFactor;
use fleetring_control::LeaseEpoch;
use fleetringd::changes::{
    Change, ChangeKind, ChangeRecord, ChangeSink, ConfiguredSink, FileSink, Format, MarkHome,
    MemorySink, Published, RecordContent,
};
use fleetringd::engine::{CaptureMode, RowOperation, Value};
use fleetringd::replication::PrimaryRole;
use tokio::sync::Notify;

use crate::common::{Harness, NoMarks, TestResult, every_database, guarded, insert, name};

/// The records in `path`, in file order.
pub(crate) fn records(path: &Path) -> Result<Vec<ChangeRecord>, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    text.lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

/// The inserted values in `records`, in order.
pub(crate) fn inserted(records: &[ChangeRecord]) -> Vec<i64> {
    records
        .iter()
        .flat_map(|record| match &record.content {
            RecordContent::Logical { rows } => rows.clone(),
            RecordContent::Physical { .. } => Vec::new(),
        })
        .filter(|row| row.table == "t" && row.operation == RowOperation::Insert)
        .filter_map(
            |row| match row.values().iter().find(|column| column.name == "value") {
                Some(column) => match column.value {
                    Value::Integer(value) => Some(value),
                    _ => None,
                },
                None => None,
            },
        )
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_arrive_in_commit_order_exactly_once_after_a_clean_run() -> TestResult {
    let harness = Harness::start().await?;
    let sink = FileSink::new("archive", harness.root().join("archive"));
    let path = sink.database_file(&name("ledger"));
    let node = harness
        .node(vec![ConfiguredSink::new(
            sink,
            Format::Logical,
            every_database(),
        )])
        .await?;
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    for value in 1..=50 {
        node.execute(&ledger, insert(value)).await?;
    }
    node.await_caught_up().await?;

    let records = records(&path)?;
    assert_eq!(inserted(&records), (1..=50).collect::<Vec<_>>());
    // Each record continues the one before: no gap and no overlap.
    for pair in records.windows(2) {
        assert_eq!(pair[1].start.as_deref(), Some(pair[0].position.as_str()));
        assert_eq!(pair[1].database, "ledger");
        assert_eq!(pair[1].epoch, 1);
    }
    assert_eq!(
        records[0].start, None,
        "the first record reads from the log's beginning"
    );
    let status = node.streams.status();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].sink, "archive");
    assert_eq!(status[0].database, ledger);
    assert_eq!(status[0].position, status[0].acknowledged);
    assert_eq!((status[0].failures, status[0].resets), (0, 0));
    node.kill().await;
    Ok(())
}

/// Writes through to a file sink, then hangs forever on the change that inserts `stick_at`:
/// the change is in the file, but the pump never records its position.
#[derive(Debug)]
struct StickySink {
    inner: FileSink,
    stick_at: i64,
    stuck: Arc<Notify>,
}

impl ChangeSink for StickySink {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn publish(&self, change: Change) -> Published {
        let stick = match &change.kind {
            ChangeKind::Logical(rows) => rows.iter().any(|row| {
                row.values()
                    .iter()
                    .any(|column| column.value == Value::Integer(self.stick_at))
            }),
            ChangeKind::Physical(_) => false,
        };
        let written = self.inner.publish(change);
        let stuck = Arc::clone(&self.stuck);
        Box::pin(async move {
            written.await?;
            if stick {
                stuck.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pump_killed_mid_stream_delivers_everything_at_least_once_after_a_restart() -> TestResult
{
    let harness = Harness::start().await?;
    let dir = harness.root().join("archive");
    let path = FileSink::new("archive", &dir).database_file(&name("ledger"));
    let stuck = Arc::new(Notify::new());
    let sticky = StickySink {
        inner: FileSink::new("archive", &dir),
        stick_at: 6,
        stuck: Arc::clone(&stuck),
    };
    let node = harness
        .node(vec![ConfiguredSink::new(
            sticky,
            Format::Logical,
            every_database(),
        )])
        .await?;
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    for value in 1..=5 {
        node.execute(&ledger, insert(value)).await?;
        node.await_caught_up().await?;
    }
    // The sixth change is written and then sticks; the writes after it are acknowledged
    // regardless, and never reach the file while the pump is stuck.
    node.execute(&ledger, insert(6)).await?;
    guarded("the sink sticking", stuck.notified()).await?;
    for value in 7..=20 {
        node.execute(&ledger, insert(value)).await?;
    }
    let before = records(&path)?;
    assert_eq!(inserted(&before), (1..=6).collect::<Vec<_>>());
    node.kill().await;

    // A fresh pump resumes from the last recorded position, 5: it delivers 6 again, with
    // everything after it, as one batch.
    let node = harness
        .node(vec![ConfiguredSink::new(
            FileSink::new("archive", &dir),
            Format::Logical,
            every_database(),
        )])
        .await?;
    node.replication.serve_primary(
        ledger.clone(),
        fleetringd::replication::PrimaryRole::new(
            fleetring::ReplicationFactor::ONE,
            fleetring_control::LeaseEpoch::new(2),
        ),
    )?;
    node.await_caught_up().await?;
    let after = records(&path)?;
    let delivered = inserted(&after);
    assert_eq!(delivered.len(), 21, "6 is delivered twice: {delivered:?}");
    let mut once = delivered.clone();
    once.dedup();
    assert_eq!(once, (1..=20).collect::<Vec<_>>());
    let resumed = &after[before.len()];
    // The record of 5, found by its value: whether the schema's batch was delivered on its own or
    // together with the first insert depends on how quickly the pump started, so the records'
    // indices are not fixed.
    let of_five = before
        .iter()
        .find(|record| inserted(std::slice::from_ref(record)) == [5])
        .ok_or("a record that inserted 5")?;
    assert_eq!(resumed.start.as_deref(), Some(of_five.position.as_str()));
    assert_eq!(resumed.epoch, 2);
    match &resumed.content {
        RecordContent::Logical { rows } => assert_eq!(rows.len(), 15),
        RecordContent::Physical { .. } => panic!("a logical sink"),
    }
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_physical_sink_receives_the_opaque_payload() -> TestResult {
    let harness = Harness::start().await?;
    let sink = fleetringd::changes::MemorySink::new("mirror");
    let node = harness
        .node(vec![ConfiguredSink::new(
            sink.clone(),
            Format::Physical,
            every_database(),
        )])
        .await?;
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    node.execute(&ledger, insert(1)).await?;
    node.await_caught_up().await?;
    let changes = sink.changes();
    assert!(!changes.is_empty());
    for change in &changes {
        match &change.kind {
            ChangeKind::Physical(payload) => assert!(!payload.is_empty()),
            ChangeKind::Logical(_) => panic!("a physical sink"),
        }
    }
    node.kill().await;
    Ok(())
}

/// Each change in `sink` that carries rows, in delivery order.
fn row_changes(sink: &MemorySink) -> Vec<Change> {
    sink.changes()
        .into_iter()
        .filter(|change| matches!(&change.kind, ChangeKind::Logical(rows) if !rows.is_empty()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pump_restarted_on_the_same_node_resumes_from_the_mark() -> TestResult {
    let harness = Harness::start().await?;
    let first = MemorySink::new("stream");
    let node = harness
        .node(vec![ConfiguredSink::new(
            first.clone(),
            Format::Logical,
            every_database(),
        )])
        .await?;
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    node.await_caught_up().await?;
    // Each write carries the mark of the delivery before it: the third write records the end of
    // the second change, and the last delivery's mark is staged with no write to record it.
    for value in 1..=2 {
        node.execute(&ledger, insert(value)).await?;
        node.await_caught_up().await?;
    }
    let delivered = row_changes(&first);
    assert_eq!(delivered.len(), 2, "{delivered:?}");
    node.kill().await;

    let second = MemorySink::new("stream");
    let node = harness
        .node(vec![ConfiguredSink::new(
            second.clone(),
            Format::Logical,
            every_database(),
        )])
        .await?;
    node.replication.serve_primary(
        ledger.clone(),
        PrimaryRole::new(ReplicationFactor::ONE, LeaseEpoch::new(2)),
    )?;
    node.await_caught_up().await?;
    let resumed = second.changes();
    assert_eq!(resumed.len(), 1, "one change repeated: {resumed:?}");
    assert_eq!(
        resumed[0].start.as_ref(),
        Some(&delivered[0].position),
        "the stream resumes from the mark, the end of the first insert"
    );
    assert_eq!(resumed[0].position, delivered[1].position);
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn status_reports_where_each_sinks_position_lives() -> TestResult {
    let ledger = name("ledger");
    for (kept, expected) in [
        (Kept::InDatabase, MarkHome::Replicated),
        (Kept::OnNode, MarkHome::Local),
    ] {
        let harness = Harness::start().await?;
        let sinks = vec![ConfiguredSink::new(
            MemorySink::new("stream"),
            Format::Logical,
            every_database(),
        )];
        let node = match kept {
            Kept::InDatabase => harness.node(sinks).await?,
            Kept::OnNode => {
                harness
                    .node_on(NoMarks::new(), sinks, CaptureMode::default())
                    .await?
            }
        };
        node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
            .await?;
        node.execute(&ledger, insert(1)).await?;
        node.await_caught_up().await?;
        let status = node.streams.status();
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].home, Some(expected), "{status:?}");
        node.kill().await;
    }
    Ok(())
}

/// Where a test's engine keeps sink positions.
#[derive(Debug, Clone, Copy)]
enum Kept {
    InDatabase,
    OnNode,
}

#[tokio::test(flavor = "multi_thread")]
async fn an_engine_without_marks_keeps_positions_on_the_node() -> TestResult {
    let harness = Harness::start().await?;
    let node = harness
        .node_on(
            NoMarks::new(),
            vec![ConfiguredSink::new(
                MemorySink::new("stream"),
                Format::Logical,
                every_database(),
            )],
            CaptureMode::default(),
        )
        .await?;
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    node.execute(&ledger, insert(1)).await?;
    node.await_caught_up().await?;
    let status = node.streams.status();
    let position = status[0].position.clone().ok_or("a delivered position")?;
    let file = harness.root().join("changes").join("stream").join("ledger");
    assert_eq!(std::fs::read(&file)?, position.as_bytes(), "{file:?}");
    node.kill().await;
    Ok(())
}
