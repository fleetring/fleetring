//! FR-041: a change stream's position replicates with the database, so a new primary resumes the
//! stream where the old one stopped (ADR-0028). The tests move the primary by hand: three nodes
//! replicate a database over real TCP, the first is killed, and the second serves it at the next
//! epoch.
//!
//! With three replicas the write quorum is two, so the promoted node and the third still
//! acknowledge writes; with two, the survivor alone could not, and its pumps would deliver
//! nothing.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::changes::{
    Change, ChangeKind, ChangeSink, ConfiguredSink, Format, MemorySink, Published, SinkError,
};
use fleetringd::engine::{Position, RowOperation, Value};
use rusqlite::{Connection, OpenFlags};
use tokio::sync::watch;

use crate::common::{Fleet, TestResult, eventually, every_database, guarded, insert, name};

const SCHEMA: &str = "CREATE TABLE t (value INTEGER NOT NULL)";
const SINK: &str = "stream";

fn logical(sink: impl ChangeSink) -> ConfiguredSink {
    ConfiguredSink::new(sink, Format::Logical, every_database())
}

/// The values the changes in `sink` inserted into `t`, in delivery order.
fn values(sink: &MemorySink) -> Vec<i64> {
    sink.changes().iter().flat_map(inserted).collect()
}

fn inserted(change: &Change) -> Vec<i64> {
    let ChangeKind::Logical(rows) = &change.kind else {
        return Vec::new();
    };
    rows.iter()
        .filter(|row| row.table == "t" && row.operation == RowOperation::Insert)
        .filter_map(|row| {
            row.values()
                .iter()
                .find(|column| column.name == "value")
                .and_then(|column| match column.value {
                    Value::Integer(value) => Some(value),
                    _ => None,
                })
        })
        .collect()
}

/// The changes in `sink` that carry rows.
fn with_rows(sink: &MemorySink) -> Vec<Change> {
    sink.changes()
        .into_iter()
        .filter(|change| matches!(&change.kind, ChangeKind::Logical(rows) if !rows.is_empty()))
        .collect()
}

/// Three nodes, `a` primary at epoch 1, with a memory sink named [`SINK`] on `a` and on `b`.
struct Streams {
    fleet: Fleet,
    ledger: DatabaseName,
    on_a: MemorySink,
    on_b: MemorySink,
}

impl Streams {
    /// Starts the nodes and creates the database, and has `a` deliver two inserts: `1`, whose end
    /// is the position the mark records, and `2`, whose mark no write follows. Returns once both
    /// followers hold everything.
    async fn start() -> Result<(Self, Position), Box<dyn std::error::Error>> {
        let on_a = MemorySink::new(SINK);
        let on_b = MemorySink::new(SINK);
        let fleet = Fleet::start(&["a", "b", "c"], |node| match node {
            "a" => vec![logical(on_a.clone())],
            "b" => vec![logical(on_b.clone())],
            _ => Vec::new(),
        })
        .await?;
        let ledger = name("ledger");
        fleet.create("a", &ledger, SCHEMA).await?;
        fleet.node("a").await_caught_up().await?;
        // Each write carries the mark of the delivery before it. The delivery of `1` ends at the
        // position the second write records as the mark; the delivery of `2` is the one a
        // failover repeats.
        for value in 1..=2 {
            fleet.execute("a", &ledger, insert(value)).await?;
            fleet.node("a").await_caught_up().await?;
        }
        fleet.await_followers("a", &ledger).await?;
        let delivered = with_rows(&on_a);
        assert_eq!(delivered.len(), 2, "{delivered:?}");
        let mark = delivered[0].position.clone();
        Ok((
            Self {
                fleet,
                ledger,
                on_a,
                on_b,
            },
            mark,
        ))
    }

    /// Kills `a` and serves the database on `b` at epoch 2.
    async fn fail_over(&mut self) -> TestResult {
        self.fleet.kill("a").await;
        self.fleet.promote("b", &self.ledger, 2)?;
        self.fleet.node("b").await_caught_up().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_primary_resumes_from_the_mark_and_repeats_one_delivery() -> TestResult {
    let (mut streams, mark) = Streams::start().await?;
    assert_eq!(values(&streams.on_a), vec![1, 2]);
    streams.fail_over().await?;

    let resumed = with_rows(&streams.on_b);
    assert_eq!(resumed.len(), 1, "one delivery repeated: {resumed:?}");
    assert_eq!(resumed[0].start.as_ref(), Some(&mark));
    assert_eq!(resumed[0].epoch.get(), 2);
    assert_eq!(
        inserted(&resumed[0]),
        vec![2],
        "the write after the mark alone"
    );
    let status = streams.fleet.node("b").streams.status();
    assert_eq!(status[0].resets, 0, "{status:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mark_outlives_the_data_directory_that_wrote_it() -> TestResult {
    let (mut streams, mark) = Streams::start().await?;
    streams.fleet.kill("a").await;
    std::fs::remove_dir_all(streams.fleet.root("a"))?;
    streams.fleet.promote("b", &streams.ledger, 2)?;
    streams.fleet.node("b").await_caught_up().await?;

    let resumed = with_rows(&streams.on_b);
    assert_eq!(resumed.len(), 1, "{resumed:?}");
    assert_eq!(resumed[0].start.as_ref(), Some(&mark));
    assert_eq!(inserted(&resumed[0]), vec![2]);
    Ok(())
}

/// Fails every publish until its switch is turned on, then takes everything.
#[derive(Debug)]
struct Switched {
    inner: MemorySink,
    on: watch::Receiver<bool>,
    offered: Arc<AtomicU64>,
}

impl ChangeSink for Switched {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn publish(&self, change: Change) -> Published {
        self.offered.fetch_add(1, Ordering::SeqCst);
        if *self.on.borrow() {
            self.inner.publish(change)
        } else {
            Box::pin(std::future::ready(Err(SinkError::Failed {
                reason: "switched off".to_owned(),
            })))
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mark_behind_a_restarted_log_restarts_from_the_beginning() -> TestResult {
    let on_a = MemorySink::new(SINK);
    let on_b = MemorySink::new(SINK);
    let (switch, on) = watch::channel(false);
    let offered = Arc::new(AtomicU64::new(0));
    let b_sink = Switched {
        inner: on_b.clone(),
        on,
        offered: Arc::clone(&offered),
    };
    let b_sink = std::sync::Mutex::new(Some(b_sink));
    let mut fleet = Fleet::start(&["a", "b", "c"], |node| match node {
        "a" => vec![logical(on_a.clone())],
        "b" => vec![logical(
            b_sink.lock().expect("lock").take().expect("one sink"),
        )],
        _ => Vec::new(),
    })
    .await?;
    let ledger = name("ledger");
    fleet.create("a", &ledger, SCHEMA).await?;
    fleet.node("a").await_caught_up().await?;
    for value in 1..=2 {
        fleet.execute("a", &ledger, insert(value)).await?;
        fleet.node("a").await_caught_up().await?;
    }
    fleet.await_followers("a", &ledger).await?;

    // `b` takes over, and its pump starts at the mark but can't deliver: the sink is off.
    fleet.kill("a").await;
    fleet.promote("b", &ledger, 2)?;
    eventually("b's pump offering the sink a change", || {
        offered.load(Ordering::SeqCst) >= 1
    })
    .await?;
    // The log restarts past the mark. The checkpoint goes through the database directly, since
    // replication's own waits for `a`, which is gone, to hold everything; and no sink holds it
    // back, as the replication settings' `lag_limit` is zero.
    let database = fleet.node("b").registry().open(&ledger).await?;
    guarded("the checkpoint", async {
        loop {
            match database.checkpoint().await {
                Err(error) if error.to_string().contains("could not complete") => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => return result,
            }
        }
    })
    .await??;
    // The log's new generation begins with the next write.
    fleet.execute("b", &ledger, insert(3)).await?;
    switch.send_replace(true);
    fleet.node("b").await_caught_up().await?;

    let status = fleet.node("b").streams.status();
    assert_eq!(
        status[0].resets, 1,
        "the pump counted the restart: {status:?}"
    );
    assert_eq!(status[0].position, status[0].acknowledged);
    let delivered = with_rows(&on_b);
    assert_eq!(delivered.len(), 1, "{delivered:?}");
    assert_eq!(delivered[0].start, None, "from the log's beginning");
    assert_eq!(inserted(&delivered[0]), vec![3]);
    Ok(())
}

/// Takes nothing until its gate opens.
#[derive(Debug)]
struct Gated {
    inner: MemorySink,
    gate: watch::Receiver<bool>,
}

impl ChangeSink for Gated {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn publish(&self, change: Change) -> Published {
        let mut gate = self.gate.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            if gate.wait_for(|open| *open).await.is_err() {
                std::future::pending::<()>().await;
            }
            inner.publish(change).await
        })
    }
}

/// How many transactions pass between prunes of delivered row changes, and then some.
const WRITES_PAST_A_PRUNE: i64 = 100;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pruning_waits_for_the_marks_of_every_logical_sink() -> TestResult {
    let fast_on_a = MemorySink::new("fast");
    let fast_on_b = MemorySink::new("fast");
    let slow_on_a = MemorySink::new("slow");
    let slow_on_b = MemorySink::new("slow");
    let (stall, gate_a) = watch::channel(true);
    let (_open, gate_b) = watch::channel(true);
    let sinks = |node: &str| match node {
        "a" => vec![
            logical(fast_on_a.clone()),
            logical(Gated {
                inner: slow_on_a.clone(),
                gate: gate_a.clone(),
            }),
        ],
        "b" => vec![
            logical(fast_on_b.clone()),
            logical(Gated {
                inner: slow_on_b.clone(),
                gate: gate_b.clone(),
            }),
        ],
        _ => Vec::new(),
    };
    let mut fleet = Fleet::start(&["a", "b", "c"], sinks).await?;
    let ledger = name("ledger");
    fleet.create("a", &ledger, SCHEMA).await?;
    fleet.node("a").await_caught_up().await?;
    for value in 1..=10 {
        fleet.execute("a", &ledger, insert(value)).await?;
        fleet.node("a").await_caught_up().await?;
    }

    // `slow` stops after 10; `fast` goes on. The recorder prunes in the first write, after 64
    // transactions, that finds a release to act on, and a release follows a delivery of `fast`'s
    // asynchronously. So the writes come in two rounds, with `fast` caught up between them: by the
    // second round the recorder is past 64 transactions and a release is on its way, whatever the
    // speed of the machine.
    stall.send_replace(false);
    let halfway = 10 + WRITES_PAST_A_PRUNE * 7 / 10;
    let last = 10 + WRITES_PAST_A_PRUNE;
    for value in 11..=halfway {
        fleet.execute("a", &ledger, insert(value)).await?;
    }
    eventually("the fast sink delivering the first round", || {
        values(&fast_on_a).last() == Some(&halfway)
    })
    .await?;
    for value in halfway + 1..=last {
        fleet.execute("a", &ledger, insert(value)).await?;
    }
    fleet.await_followers("a", &ledger).await?;
    // Wait on what the sink holds: a pump's status keeps the quorum position from its last look,
    // which can equal its delivered position while a later write is still to be delivered.
    eventually("the fast sink delivering everything", || {
        values(&fast_on_a).last() == Some(&last)
    })
    .await?;
    assert_eq!(values(&slow_on_a), (1..=10).collect::<Vec<_>>());
    let slow_stopped_at = with_rows(&slow_on_a)
        .last()
        .map(|change| change.position.clone())
        .ok_or("the slow sink delivered something")?;

    fleet.kill("a").await;
    // The primary did prune: the oldest rows its change table holds are not its first.
    let copy = Connection::open_with_flags(
        fleet
            .root("a")
            .join("databases")
            .join("ledger")
            .join("data.db"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let oldest: i64 = copy.query_row("SELECT min(change_id) FROM _fleetring_cdc", [], |row| {
        row.get(0)
    })?;
    assert!(oldest > 1, "nothing was pruned: the oldest row is {oldest}");
    drop(copy);

    // The new primary's `slow` pump starts at the replicated mark, and the rows after it are all
    // still in the copy.
    fleet.promote("b", &ledger, 2)?;
    fleet.node("b").await_caught_up().await?;
    let resumed = with_rows(&slow_on_b);
    assert_eq!(
        resumed.first().and_then(|change| change.start.as_ref()),
        Some(&slow_stopped_at),
        "{resumed:?}"
    );
    let mut every: Vec<i64> = values(&slow_on_a);
    every.extend(values(&slow_on_b));
    every.dedup();
    assert_eq!(every, (1..=last).collect::<Vec<_>>());
    assert_eq!(values(&slow_on_b), (11..=last).collect::<Vec<_>>());
    Ok(())
}
