//! The event trace: what happened in a run, in order, for comparing runs and for debugging.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use fleetring::Fingerprint;

use crate::sim::fault::Fault;
use crate::sim::workload::{Operation, Outcome};

/// The source of the events the test itself records: faults and notes.
pub const DRIVER: &str = "sim";

/// One thing that happened in a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    at: Duration,
    source: String,
    kind: EventKind,
}

impl Event {
    pub(crate) fn new(at: Duration, source: impl Into<String>, kind: EventKind) -> Self {
        Self {
            at,
            source: source.into(),
            kind,
        }
    }

    /// Returns the simulated time the event happened at, since the run started.
    #[must_use]
    pub fn at(&self) -> Duration {
        self.at
    }

    /// Returns who recorded the event: a host, a client, or [`DRIVER`] for the test itself.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns what happened.
    #[must_use]
    pub fn kind(&self) -> &EventKind {
        &self.kind
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:>8}ms {}: {}",
            self.at.as_millis(),
            self.source,
            self.kind
        )
    }
}

/// What an [`Event`] records.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventKind {
    /// The test injected a fault, or repaired one.
    Fault(Fault),
    /// A host's daemon started.
    Started,
    /// A host's daemon finished a graceful shutdown.
    Stopped,
    /// A data node published a membership view.
    Published {
        /// The membership epoch.
        epoch: u64,
        /// The ring's fingerprint, or `None` while there are no members.
        fingerprint: Option<Fingerprint>,
        /// The members the view marks down, in ID order.
        down: Vec<String>,
    },
    /// A client finished an operation.
    Operation {
        /// The operation.
        operation: Operation,
        /// How it ended.
        outcome: Outcome,
    },
    /// A client's workload finished.
    WorkloadDone,
    /// A free-form note from the test.
    Note(String),
}

impl fmt::Display for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fault(fault) => write!(f, "fault {fault}"),
            Self::Started => f.write_str("started"),
            Self::Stopped => f.write_str("stopped"),
            Self::Published {
                epoch,
                fingerprint,
                down,
            } => {
                write!(f, "published epoch {epoch} ring ")?;
                match fingerprint {
                    Some(fingerprint) => write!(f, "{fingerprint}")?,
                    None => f.write_str("none")?,
                }
                write!(f, " down {down:?}")
            }
            Self::Operation { operation, outcome } => write!(f, "{operation} -> {outcome}"),
            Self::WorkloadDone => f.write_str("workload done"),
            Self::Note(note) => write!(f, "note {note}"),
        }
    }
}

/// How closely two runs of the same seed must agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Replay {
    /// The same events, in the same order, at the same simulated times.
    Exact,
    /// The test's own events (faults and notes) in the same order, ignoring times.
    ///
    /// Hosts and clients are left out: when two events land a millisecond apart instead of
    /// together, a node may publish two membership epochs where the other run published one, or
    /// an operation may fail with a different status. The test's events still have to match,
    /// so each run reached every checkpoint, through the same faults, in the same order.
    Order,
}

impl Replay {
    /// The strongest comparison this build supports.
    ///
    /// turmoil seeds Tokio's scheduler, which picks among ready branches of a `select!`, only when
    /// Tokio is built with `--cfg tokio_unstable`. With it, a seed replays exactly; without it,
    /// events can shift by a few simulated milliseconds, so only the test's own events are
    /// compared.
    #[must_use]
    pub const fn supported() -> Self {
        if cfg!(tokio_unstable) {
            Self::Exact
        } else {
            Self::Order
        }
    }
}

/// An ordered record of a run's events.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trace {
    events: Vec<Event>,
}

impl Trace {
    pub(crate) fn push(&mut self, event: Event) {
        self.events.push(event);
    }

    /// Returns every event, in the order they were recorded.
    #[must_use]
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Returns how many events were recorded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns whether no event was recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns each source's events without their times, in order, keyed by source.
    #[must_use]
    pub fn by_source(&self) -> BTreeMap<&str, Vec<String>> {
        let mut sources: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for event in &self.events {
            sources
                .entry(event.source.as_str())
                .or_default()
                .push(event.kind.to_string());
        }
        sources
    }

    /// Checks that `other`, a run of the same seed, agrees with this one as `replay` asks.
    ///
    /// # Errors
    ///
    /// [`Divergence`] names the first event where the runs differ.
    pub fn compare(&self, other: &Self, replay: Replay) -> Result<(), Divergence> {
        match replay {
            Replay::Exact => {
                let ours: Vec<String> = self.events.iter().map(ToString::to_string).collect();
                let theirs: Vec<String> = other.events.iter().map(ToString::to_string).collect();
                first_difference("the whole run", &ours, &theirs)
            }
            Replay::Order => {
                let empty = Vec::new();
                let ours = self.by_source();
                let theirs = other.by_source();
                first_difference(
                    DRIVER,
                    ours.get(DRIVER).unwrap_or(&empty),
                    theirs.get(DRIVER).unwrap_or(&empty),
                )
            }
        }
    }
}

impl fmt::Display for Trace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for event in &self.events {
            writeln!(f, "{event}")?;
        }
        Ok(())
    }
}

fn first_difference(scope: &str, ours: &[String], theirs: &[String]) -> Result<(), Divergence> {
    let length = ours.len().max(theirs.len());
    for index in 0..length {
        let first = ours.get(index);
        let second = theirs.get(index);
        if first != second {
            return Err(Divergence {
                scope: scope.to_owned(),
                index,
                first: first.cloned(),
                second: second.cloned(),
            });
        }
    }
    Ok(())
}

/// Where two runs of one seed stopped agreeing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "runs diverge in {scope} at event {index}: {} vs {}",
    .first.as_deref().unwrap_or("(none)"),
    .second.as_deref().unwrap_or("(none)")
)]
pub struct Divergence {
    scope: String,
    index: usize,
    first: Option<String>,
    second: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(at: u64, source: &str, text: &str) -> Event {
        Event::new(
            Duration::from_millis(at),
            source,
            EventKind::Note(text.into()),
        )
    }

    #[test]
    fn order_compares_the_driver_without_times_and_exact_compares_everything() {
        let mut first = Trace::default();
        first.push(note(1, DRIVER, "x"));
        first.push(note(2, "node-1", "y"));
        let mut second = Trace::default();
        second.push(note(3, DRIVER, "x"));
        second.push(note(4, "node-1", "z"));
        assert!(first.compare(&second, Replay::Order).is_ok());
        assert!(first.compare(&second, Replay::Exact).is_err());
        assert!(first.compare(&first.clone(), Replay::Exact).is_ok());

        second.push(note(5, DRIVER, "w"));
        let divergence = first
            .compare(&second, Replay::Order)
            .expect_err("an extra event");
        assert!(
            divergence.to_string().contains("in sim at event 1"),
            "{divergence}"
        );
    }
}
