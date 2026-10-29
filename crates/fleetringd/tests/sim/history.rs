//! FR-036: the history checker catches planted bugs.
//!
//! Each test builds a history by hand, the way a run records it, plants one bug in it or in the
//! replicas' final rows, and checks that [`history::check`] reports the right invariant with an
//! excerpt that shows the operations involved. A correct history passes.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetringd::sim::check::{Invariant, ReplicaRows, Violation};
use fleetringd::sim::history::{self, Guarantee, Replicated};
use fleetringd::sim::{Ack, History, Operation, OperationId, Outcome};
use tonic::Code;

use crate::TestResult;

const WRITER: &str = "writer";
const READER: &str = "reader";

fn db() -> Result<DatabaseName, Box<dyn std::error::Error>> {
    Ok("db-0".parse()?)
}

fn ms(at: u64) -> Duration {
    Duration::from_millis(at)
}

fn written_by(primary: &str, epoch: u64) -> Result<Outcome, Box<dyn std::error::Error>> {
    Ok(Outcome::Acknowledged(Ack::Written {
        acknowledged_by: 2,
        replicas: 3,
        under_replicated: false,
        primary: Some(primary.parse()?),
        epoch: LeaseEpoch::new(epoch),
    }))
}

fn refused() -> Outcome {
    Outcome::Rejected {
        code: Code::FailedPrecondition,
        message: "not the primary".into(),
    }
}

fn unknown() -> Outcome {
    Outcome::Indeterminate {
        code: Some(Code::Aborted),
    }
}

fn read(values: impl IntoIterator<Item = i64>) -> Outcome {
    Outcome::Acknowledged(Ack::Read {
        values: values.into_iter().collect(),
    })
}

/// A history that records, at `at`, operations lasting 5 ms each.
struct Builder {
    history: History,
    at: u64,
}

impl Builder {
    fn new() -> Self {
        Self {
            history: History::default(),
            at: 100,
        }
    }

    fn write(&mut self, client: &str, value: i64, outcome: Outcome) -> TestResult {
        self.op(
            client,
            Operation::Write {
                database: db()?,
                value,
            },
            outcome,
        )
    }

    fn read_all(&mut self, client: &str, outcome: Outcome) -> TestResult {
        self.op(
            client,
            Operation::Read {
                database: db()?,
                value: None,
            },
            outcome,
        )
    }

    fn op(&mut self, client: &str, operation: Operation, outcome: Outcome) -> TestResult {
        self.history
            .record(client, ms(self.at), ms(self.at + 5), operation, outcome);
        self.at += 10;
        Ok(())
    }

    fn id_of_write(&self, value: i64) -> Option<OperationId> {
        self.history
            .records()
            .iter()
            .find(|record| {
                matches!(record.operation(), Operation::Write { value: written, .. } if *written == value)
            })
            .map(fleetringd::sim::Record::id)
    }
}

/// Three replicas that all hold `values`.
fn replicas(values: impl IntoIterator<Item = i64> + Clone) -> ReplicaRows {
    ["node-1", "node-2", "node-3"]
        .into_iter()
        .map(|host| (host.to_owned(), values.clone().into_iter().collect()))
        .collect()
}

fn databases(
    rows: ReplicaRows,
) -> Result<BTreeMap<DatabaseName, Replicated>, Box<dyn std::error::Error>> {
    let three = ReplicationFactor::new(3).ok_or("three replicas")?;
    Ok(BTreeMap::from([(
        db()?,
        Replicated::new(Guarantee::quorum(three), rows),
    )]))
}

/// A correct run: acknowledged writes everywhere, a refused and an unknown write nowhere or
/// anywhere, and reads that see what they should.
fn correct() -> Result<(Builder, ReplicaRows), Box<dyn std::error::Error>> {
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    run.write(WRITER, 2, written_by("node-1", 1)?)?;
    run.read_all(WRITER, read([1, 2]))?;
    run.write(WRITER, 3, refused())?;
    run.write(WRITER, 4, unknown())?;
    run.read_all(READER, read([1, 2, 4]))?;
    run.write(WRITER, 5, written_by("node-2", 2)?)?;
    run.read_all(WRITER, read([1, 2, 5]))?;
    run.read_all(READER, read([1, 2, 5]))?;
    // Replica rows: the unknown write 4 was truncated with the old primary's tail.
    let mut rows = replicas([1, 2, 5]);
    rows.get_mut("node-3").ok_or("node-3")?.extend([4]);
    Ok((run, rows))
}

fn expect_violation(
    run: &Builder,
    rows: ReplicaRows,
    invariant: Invariant,
    involved: &[OperationId],
) -> Result<Violation, Box<dyn std::error::Error>> {
    let violation = history::check(&run.history, &databases(rows)?)
        .err()
        .ok_or("the planted bug went unnoticed")?;
    assert_eq!(violation.invariant(), invariant, "{violation}");
    let marked: BTreeSet<String> = violation
        .excerpt()
        .iter()
        .filter(|line| line.starts_with('>'))
        .map(|line| {
            line.split_whitespace()
                .nth(2)
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    for id in involved {
        assert!(
            marked.contains(&id.to_string()),
            "the excerpt doesn't mark {id}:\n{violation}"
        );
    }
    let text = violation.to_string();
    assert!(text.contains("history around it"), "{text}");
    Ok(violation)
}

#[test]
fn a_correct_history_passes() -> TestResult {
    let (run, rows) = correct()?;
    history::check(&run.history, &databases(rows)?)?;
    Ok(())
}

#[test]
fn a_dropped_acknowledged_write_is_caught() -> TestResult {
    let (run, mut rows) = correct()?;
    // Write 2 was acknowledged by a quorum, but only one replica still holds it.
    for host in ["node-1", "node-2"] {
        rows.get_mut(host).ok_or(host)?.remove(&2);
    }
    let lost = run.id_of_write(2).ok_or("write 2")?;
    let violation = expect_violation(&run, rows, Invariant::AcknowledgedWritesReplicated, &[lost])?;
    assert!(violation.detail().contains("write 2"), "{violation}");
    assert!(
        violation.detail().contains("[\"node-3\"]"),
        "names the replica that still holds it: {violation}"
    );
    Ok(())
}

#[test]
fn two_primaries_acknowledging_in_one_epoch_are_caught() -> TestResult {
    let (mut run, rows) = correct()?;
    // node-1 acknowledges at epoch 2, which node-2 already served.
    run.write(WRITER, 6, written_by("node-1", 2)?)?;
    run.read_all(WRITER, read([1, 2, 5, 6]))?;
    let mut rows = rows;
    for held in rows.values_mut() {
        held.insert(6);
    }
    let first = run.id_of_write(5).ok_or("write 5")?;
    let second = run.id_of_write(6).ok_or("write 6")?;
    let violation = expect_violation(&run, rows, Invariant::OnePrimaryPerEpoch, &[first, second])?;
    assert!(
        violation.detail().contains("node-2 and node-1"),
        "{violation}"
    );
    Ok(())
}

#[test]
fn a_stale_read_is_caught() -> TestResult {
    // The writer's own read after write 5 was acknowledged doesn't see it.
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    run.write(WRITER, 5, written_by("node-1", 1)?)?;
    run.read_all(WRITER, read([1]))?;
    let write = run.id_of_write(5).ok_or("write 5")?;
    let violation = expect_violation(&run, replicas([1, 5]), Invariant::ReadYourWrites, &[write])?;
    assert!(violation.detail().contains("write 5"), "{violation}");

    // Another client's read that began after the acknowledgement doesn't see it either.
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    run.write(WRITER, 5, written_by("node-1", 1)?)?;
    run.read_all(READER, read([1]))?;
    let write = run.id_of_write(5).ok_or("write 5")?;
    expect_violation(
        &run,
        replicas([1, 5]),
        Invariant::AcknowledgedWritesRead,
        &[write],
    )?;

    // A read that began before the write completed may miss it.
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    run.history.record(
        READER,
        ms(200),
        ms(260),
        Operation::Read {
            database: db()?,
            value: None,
        },
        read([1]),
    );
    run.history.record(
        WRITER,
        ms(210),
        ms(250),
        Operation::Write {
            database: db()?,
            value: 5,
        },
        written_by("node-1", 1)?,
    );
    history::check(&run.history, &databases(replicas([1, 5]))?)?;
    Ok(())
}

#[test]
fn a_read_that_loses_an_acknowledged_write_it_saw_is_caught() -> TestResult {
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    // The reader sees write 2 while it is in flight, then it is acknowledged, then a later
    // read by the same reader doesn't see it.
    run.history.record(
        READER,
        ms(200),
        ms(205),
        Operation::Read {
            database: db()?,
            value: None,
        },
        read([1, 2]),
    );
    run.history.record(
        WRITER,
        ms(195),
        ms(230),
        Operation::Write {
            database: db()?,
            value: 2,
        },
        written_by("node-1", 1)?,
    );
    run.history.record(
        READER,
        ms(220),
        ms(225),
        Operation::Read {
            database: db()?,
            value: None,
        },
        read([1]),
    );
    let violation = expect_violation(&run, replicas([1, 2]), Invariant::MonotonicReads, &[])?;
    assert!(violation.detail().contains("write 2"), "{violation}");

    // An unknown write may be seen once and never again: a truncated divergent tail.
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    run.write(WRITER, 2, unknown())?;
    run.read_all(READER, read([1, 2]))?;
    run.read_all(READER, read([1]))?;
    history::check(&run.history, &databases(replicas([1]))?)?;
    Ok(())
}

#[test]
fn a_phantom_failed_write_is_caught() -> TestResult {
    // On a replica.
    let (run, mut rows) = correct()?;
    rows.get_mut("node-2").ok_or("node-2")?.insert(3);
    let phantom = run.id_of_write(3).ok_or("write 3")?;
    let violation = expect_violation(&run, rows, Invariant::NoPhantomWrites, &[phantom])?;
    assert!(violation.detail().contains("node-2"), "{violation}");

    // In a read.
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    run.write(WRITER, 3, refused())?;
    run.read_all(READER, read([1, 3]))?;
    let phantom = run.id_of_write(3).ok_or("write 3")?;
    let violation = expect_violation(&run, replicas([1]), Invariant::NoPhantomWrites, &[phantom])?;
    assert!(violation.detail().contains("read"), "{violation}");
    Ok(())
}

#[test]
fn a_keyed_read_speaks_only_for_its_value() -> TestResult {
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    run.write(WRITER, 2, written_by("node-1", 1)?)?;
    // Looking for 2 and finding it says nothing about 1.
    run.op(
        WRITER,
        Operation::Read {
            database: db()?,
            value: Some(2),
        },
        read([2]),
    )?;
    history::check(&run.history, &databases(replicas([1, 2]))?)?;
    // Looking for 1 and not finding it is a stale read.
    run.op(
        WRITER,
        Operation::Read {
            database: db()?,
            value: Some(1),
        },
        read([]),
    )?;
    expect_violation(&run, replicas([1, 2]), Invariant::ReadYourWrites, &[])?;
    Ok(())
}

#[test]
fn an_operation_still_in_flight_counts_as_unknown() -> TestResult {
    let mut run = Builder::new();
    run.write(WRITER, 1, written_by("node-1", 1)?)?;
    run.history.begin(
        WRITER,
        ms(300),
        Operation::Write {
            database: db()?,
            value: 2,
        },
    );
    // Whether or not the replicas hold 2, the history is fine.
    history::check(&run.history, &databases(replicas([1]))?)?;
    history::check(&run.history, &databases(replicas([1, 2]))?)?;
    assert_eq!(run.history.pending().count(), 1);
    Ok(())
}
