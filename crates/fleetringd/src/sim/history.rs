//! The history checker: what every client saw, checked against what each database promises.
//!
//! Jepsen checks etcd and Kafka by recording every client operation as an invocation and a
//! completion, then asking whether some sequence of effects explains the whole history. This
//! module does the same for FleetRing, over the [`History`] a run records and the rows each
//! replica holds on disk at the end:
//!
//! - **Durability.** Every `ok` write is on at least W of its database's replicas
//!   ([`Invariant::AcknowledgedWritesReplicated`]) and in every `ok` read that began after it
//!   completed ([`Invariant::AcknowledgedWritesRead`]), unless the database's
//!   [`DurabilityMode`] gives that up.
//! - **No phantom writes.** A `fail` write is on no replica and in no read
//!   ([`Invariant::NoPhantomWrites`]). An `info` write may or may not be.
//! - **Single writer.** At most one node acknowledges writes to a database in any epoch
//!   ([`Invariant::OnePrimaryPerEpoch`], from [`check::one_primary_per_epoch`]).
//! - **Read your writes.** A read sees every write its own client had acknowledged when the read
//!   began ([`Invariant::ReadYourWrites`]).
//! - **Monotonic reads.** A client never sees an acknowledged write disappear that one of its
//!   earlier reads saw ([`Invariant::MonotonicReads`]). Reads are served by the primary, and a
//!   primary cut off from its followers commits writes it can't acknowledge, which a promotion
//!   truncates (ADR-0022): such a write may be seen once and never again, so only acknowledged
//!   writes are held to this.
//!
//! A violation carries an excerpt of the history around the operations involved, a few events
//! before and after each, and [`seeds::run`](crate::sim::seeds::run) prints the command that
//! reruns the seed.
//!
//! Each database carries a [`Guarantee`]: its replication factor and its [`DurabilityMode`].
//! Today every database is in [`DurabilityMode::Quorum`] (ADR-0018). ADR-0023 adds `all` and
//! `local` per database; `local` gives up durability across failover, and the checker will then
//! hold such a database only to the invariants that still apply.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Quorums};

use crate::sim::check::{self, Invariant, ReplicaRows, Violation};
use crate::sim::workload::{
    Ack, History, HistoryEvent, Operation, OperationId, OutcomeClass, Record,
};

/// How many events before and after each operation involved in a violation the excerpt shows.
pub const CONTEXT: usize = 3;

/// What a database promises about acknowledged writes (ADR-0023).
///
/// Only [`DurabilityMode::Quorum`] exists today. The variants ADR-0023 proposes extend this enum,
/// and the checker asks it, not the variant, what to hold a database to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[non_exhaustive]
pub enum DurabilityMode {
    /// A write is acknowledged once a majority of the replica set holds it: W = ⌊R/2⌋ + 1. No
    /// acknowledged write is lost while a majority survives (ADR-0018).
    #[default]
    Quorum,
}

impl DurabilityMode {
    /// Returns how many replicas, the primary included, must hold a write before it is
    /// acknowledged, for a database with `replicas` replicas.
    #[must_use]
    pub fn write_quorum(self, replicas: ReplicationFactor) -> usize {
        match self {
            Self::Quorum => usize::from(Quorums::new(replicas).write()),
        }
    }

    /// Returns whether an acknowledged write must survive every fault the harness injects: on
    /// a write quorum, in every later read, and for every later read by a client that saw it.
    #[must_use]
    pub fn acknowledged_writes_survive(self) -> bool {
        match self {
            Self::Quorum => true,
        }
    }
}

impl fmt::Display for DurabilityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Quorum => "quorum",
        })
    }
}

/// What one database promises: its replication factor and its durability mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Guarantee {
    replicas: ReplicationFactor,
    mode: DurabilityMode,
}

impl Guarantee {
    /// A database with `replicas` replicas in `mode`.
    #[must_use]
    pub fn new(replicas: ReplicationFactor, mode: DurabilityMode) -> Self {
        Self { replicas, mode }
    }

    /// A database with `replicas` replicas in [`DurabilityMode::Quorum`], as every database is
    /// created today.
    #[must_use]
    pub fn quorum(replicas: ReplicationFactor) -> Self {
        Self::new(replicas, DurabilityMode::Quorum)
    }

    /// Returns R, the size of the database's replica set.
    #[must_use]
    pub fn replicas(&self) -> ReplicationFactor {
        self.replicas
    }

    /// Returns the database's durability mode.
    #[must_use]
    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }

    /// Returns W: how many replicas must hold an acknowledged write.
    #[must_use]
    pub fn write_quorum(&self) -> usize {
        self.mode.write_quorum(self.replicas)
    }
}

/// What the checker knows about one database besides the history: its [`Guarantee`] and the
/// rows each of its replicas holds on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replicated {
    guarantee: Guarantee,
    rows: ReplicaRows,
}

impl Replicated {
    /// A database with `guarantee` whose replicas hold `rows`.
    #[must_use]
    pub fn new(guarantee: Guarantee, rows: ReplicaRows) -> Self {
        Self { guarantee, rows }
    }

    /// Returns what the database promises.
    #[must_use]
    pub fn guarantee(&self) -> &Guarantee {
        &self.guarantee
    }

    /// Returns the rows each replica holds, by host.
    #[must_use]
    pub fn rows(&self) -> &ReplicaRows {
        &self.rows
    }

    fn holders(&self, value: i64) -> Vec<&str> {
        self.rows
            .iter()
            .filter(|(_, rows)| rows.contains(&value))
            .map(|(host, _)| host.as_str())
            .collect()
    }
}

/// A write in the history, with what it wrote.
#[derive(Debug, Clone, Copy)]
struct Write<'h> {
    record: &'h Record,
    database: &'h DatabaseName,
    value: i64,
}

/// An acknowledged read in the history, with what it read.
#[derive(Debug, Clone, Copy)]
struct Read<'h> {
    record: &'h Record,
    database: &'h DatabaseName,
    /// The one value it looked for, or `None` if it read every value.
    scope: Option<i64>,
    values: &'h BTreeSet<i64>,
}

impl Read<'_> {
    /// Whether the read says anything about `value`.
    fn covers(&self, value: i64) -> bool {
        self.scope.is_none_or(|looked_for| looked_for == value)
    }

    fn saw(&self, value: i64) -> bool {
        self.values.contains(&value)
    }
}

/// Checks `history` against what each database in `databases` promises, and against the rows
/// its replicas hold.
///
/// A database the history writes to but `databases` doesn't name is held to the invariants
/// that need no replica rows, as a quorum database.
///
/// # Errors
///
/// The first [`Violation`], with the history around it.
pub fn check(
    history: &History,
    databases: &BTreeMap<DatabaseName, Replicated>,
) -> Result<(), Violation> {
    if let Some(conflict) = check::conflicting_primaries(history) {
        return Err(conflict.violation().with_excerpt(excerpt(
            history,
            &[conflict.first.id(), conflict.second.id()],
        )));
    }
    let mut ok_writes = Vec::new();
    let mut failed_writes = Vec::new();
    let mut reads = Vec::new();
    for record in history.records() {
        match (record.operation(), record.outcome().ack()) {
            (Operation::Write { database, value }, ack) => {
                let write = Write {
                    record,
                    database,
                    value: *value,
                };
                match record.class() {
                    OutcomeClass::Ok if matches!(ack, Some(Ack::Written { .. })) => {
                        ok_writes.push(write);
                    }
                    OutcomeClass::Ok | OutcomeClass::Info => {}
                    OutcomeClass::Fail => failed_writes.push(write),
                }
            }
            (
                Operation::Read {
                    database,
                    value: scope,
                },
                Some(Ack::Read { values }),
            ) => reads.push(Read {
                record,
                database,
                scope: *scope,
                values,
            }),
            _ => {}
        }
    }
    let survive = |database: &DatabaseName| {
        databases
            .get(database)
            .is_none_or(|replicated| replicated.guarantee.mode.acknowledged_writes_survive())
    };

    acknowledged_writes_replicated(history, &ok_writes, databases)?;
    acknowledged_writes_read(history, &ok_writes, &reads, &survive)?;
    no_phantom_writes(history, &failed_writes, &reads, databases)?;
    monotonic_reads(history, &ok_writes, &reads, &survive)?;
    Ok(())
}

/// Every `ok` write is on at least W replicas.
fn acknowledged_writes_replicated(
    history: &History,
    ok_writes: &[Write<'_>],
    databases: &BTreeMap<DatabaseName, Replicated>,
) -> Result<(), Violation> {
    for write in ok_writes {
        let Some(replicated) = databases.get(write.database) else {
            continue;
        };
        if !replicated.guarantee.mode.acknowledged_writes_survive() {
            continue;
        }
        let required = replicated.guarantee.write_quorum();
        let holders = replicated.holders(write.value);
        if holders.len() < required {
            return Err(Violation::new(
                Invariant::AcknowledgedWritesReplicated,
                format!(
                    "acknowledged write {} to {} is on {holders:?}, fewer than {required} replicas",
                    write.value, write.database
                ),
            )
            .with_excerpt(excerpt(history, &[write.record.id()])));
        }
    }
    Ok(())
}

/// Every `ok` write is in every `ok` read of it that began after the write completed.
fn acknowledged_writes_read(
    history: &History,
    ok_writes: &[Write<'_>],
    reads: &[Read<'_>],
    survive: &impl Fn(&DatabaseName) -> bool,
) -> Result<(), Violation> {
    for read in reads {
        if !survive(read.database) {
            continue;
        }
        let missing = ok_writes.iter().find(|write| {
            write.database == read.database
                && read.covers(write.value)
                && write.record.completed_at() <= read.record.invoked_at()
                && !read.saw(write.value)
        });
        if let Some(write) = missing {
            let same_client = write.record.client() == read.record.client();
            let invariant = if same_client {
                Invariant::ReadYourWrites
            } else {
                Invariant::AcknowledgedWritesRead
            };
            return Err(Violation::new(
                invariant,
                format!(
                    "write {} to {} was acknowledged to {} at {} ms, but {}'s read {} that began at {} ms doesn't see it",
                    write.value,
                    write.database,
                    write.record.client(),
                    write.record.completed_at().as_millis(),
                    read.record.client(),
                    read.record.id(),
                    read.record.invoked_at().as_millis(),
                ),
            )
            .with_excerpt(excerpt(
                history,
                &[write.record.id(), read.record.id()],
            )));
        }
    }
    Ok(())
}

/// A `fail` write is on no replica and in no `ok` read.
fn no_phantom_writes(
    history: &History,
    failed_writes: &[Write<'_>],
    reads: &[Read<'_>],
    databases: &BTreeMap<DatabaseName, Replicated>,
) -> Result<(), Violation> {
    for write in failed_writes {
        let holders = databases
            .get(write.database)
            .map(|replicated| replicated.holders(write.value))
            .unwrap_or_default();
        if !holders.is_empty() {
            return Err(Violation::new(
                Invariant::NoPhantomWrites,
                format!(
                    "write {} to {} was refused, but {holders:?} hold it",
                    write.value, write.database
                ),
            )
            .with_excerpt(excerpt(history, &[write.record.id()])));
        }
        let seen = reads.iter().find(|read| {
            read.database == write.database && read.covers(write.value) && read.saw(write.value)
        });
        if let Some(read) = seen {
            return Err(Violation::new(
                Invariant::NoPhantomWrites,
                format!(
                    "write {} to {} was refused, but {}'s read {} saw it",
                    write.value,
                    write.database,
                    read.record.client(),
                    read.record.id()
                ),
            )
            .with_excerpt(excerpt(history, &[write.record.id(), read.record.id()])));
        }
    }
    Ok(())
}

/// A client's reads of a database never lose an acknowledged write an earlier read of its saw.
fn monotonic_reads(
    history: &History,
    ok_writes: &[Write<'_>],
    reads: &[Read<'_>],
    survive: &impl Fn(&DatabaseName) -> bool,
) -> Result<(), Violation> {
    let acknowledged: BTreeSet<(&DatabaseName, i64)> = ok_writes
        .iter()
        .map(|write| (write.database, write.value))
        .collect();
    // Per client and database: each acknowledged value an earlier read saw, and the read.
    let mut seen: BTreeMap<(&str, &DatabaseName), BTreeMap<i64, OperationId>> = BTreeMap::new();
    for read in reads {
        if !survive(read.database) {
            continue;
        }
        let seen = seen
            .entry((read.record.client(), read.database))
            .or_default();
        let lost = seen
            .iter()
            .find(|(value, _)| read.covers(**value) && !read.saw(**value));
        if let Some((value, earlier)) = lost {
            return Err(Violation::new(
                Invariant::MonotonicReads,
                format!(
                    "{}'s read {earlier} of {} saw acknowledged write {value}, but its later read {} doesn't",
                    read.record.client(),
                    read.database,
                    read.record.id()
                ),
            )
            .with_excerpt(excerpt(history, &[*earlier, read.record.id()])));
        }
        for value in read.values {
            if acknowledged.contains(&(read.database, *value)) {
                seen.entry(*value).or_insert(read.record.id());
            }
        }
    }
    Ok(())
}

/// The events of `history` within [`CONTEXT`] of each end of the operations in `involved`, one
/// per line, with `>` before the involved operations' own events and `…` where events are
/// skipped.
#[must_use]
pub fn excerpt(history: &History, involved: &[OperationId]) -> Vec<String> {
    let events = history.events();
    let mut shown = vec![false; events.len()];
    for (index, event) in events.iter().enumerate() {
        if involved.contains(&event.id()) {
            let first = index.saturating_sub(CONTEXT);
            let last = (index + CONTEXT).min(events.len().saturating_sub(1));
            for flag in &mut shown[first..=last] {
                *flag = true;
            }
        }
    }
    let mut lines = Vec::new();
    let mut skipped = 0_usize;
    for (event, show) in events.iter().zip(&shown) {
        if *show {
            if skipped > 0 {
                lines.push(format!("  … {skipped} events …"));
                skipped = 0;
            }
            lines.push(line(event, involved));
        } else {
            skipped += 1;
        }
    }
    if skipped > 0 {
        lines.push(format!("  … {skipped} events …"));
    }
    lines
}

fn line(event: &HistoryEvent<'_>, involved: &[OperationId]) -> String {
    let mark = if involved.contains(&event.id()) {
        '>'
    } else {
        ' '
    };
    format!("{mark} {event}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fleetring_control::LeaseEpoch;

    use super::*;
    use crate::sim::workload::Outcome;

    fn ms(at: u64) -> Duration {
        Duration::from_millis(at)
    }

    fn db() -> DatabaseName {
        "db".parse().expect("name")
    }

    fn written() -> Outcome {
        Outcome::Acknowledged(Ack::Written {
            acknowledged_by: 2,
            replicas: 3,
            under_replicated: false,
            primary: Some("a".parse().expect("id")),
            epoch: LeaseEpoch::new(1),
        })
    }

    #[test]
    fn the_excerpt_shows_the_events_around_the_operations_involved_and_elides_the_rest() {
        let mut history = History::default();
        for value in 0..20 {
            let at = 10 * value;
            history.record(
                "writer",
                ms(u64::try_from(at).expect("positive")),
                ms(u64::try_from(at + 5).expect("positive")),
                Operation::Write {
                    database: db(),
                    value,
                },
                written(),
            );
        }
        let lost = history
            .records()
            .iter()
            .find(|record| matches!(record.operation(), Operation::Write { value: 10, .. }))
            .expect("write 10")
            .id();
        let lines = excerpt(&history, &[lost]);
        // The write's two adjacent events, three on either side of the pair, and two elisions.
        assert_eq!(lines.len(), 3 + 2 + 3 + 2, "{lines:#?}");
        assert!(lines[0].starts_with("  …"), "{lines:#?}");
        assert!(lines[lines.len() - 1].starts_with("  …"), "{lines:#?}");
        let marked: Vec<&String> = lines.iter().filter(|line| line.starts_with('>')).collect();
        assert_eq!(marked.len(), 2, "{lines:#?}");
        assert!(marked[0].contains("invoke write 10 to db"), "{}", marked[0]);
        assert!(marked[1].contains("ok     write 10 to db"), "{}", marked[1]);
    }

    #[test]
    fn an_operation_still_in_flight_is_an_invocation_without_a_completion() {
        let mut history = History::default();
        history.begin(
            "writer",
            ms(1),
            Operation::Write {
                database: db(),
                value: 1,
            },
        );
        let events = history.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].phase(), crate::sim::workload::Phase::Invoke);
        assert!(history.is_empty());
        assert_eq!(history.pending().count(), 1);
    }
}
