//! The workload: ten clients writing to and reading from eight databases through
//! [`fleetringd::data::Client`], every operation recorded in a [`History`] when it is sent and
//! when it ends (ADR-0036, point 5).

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::DatabaseName;
use fleetringd::Network;
use fleetringd::data::Client;
use fleetringd::sim::{
    DurabilityMode, Guarantee, History, Operation, Outcome, SimRng, data_operation,
};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout};

use crate::Mode;
use crate::node::Node;
use crate::schedule::database_label;

pub use crate::schedule::DATABASES;

/// How many clients run at once.
pub const CLIENTS: usize = 10;
/// The share of a client's operations that are writes, in percent; the rest are reads.
pub const WRITE_PERCENT: u64 = 70;
/// How long a client waits after each operation before it sends the next.
pub const PAUSE_BETWEEN_OPERATIONS: Duration = Duration::from_millis(50);
/// How long an operation may take before it is recorded as indeterminate.
pub const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
/// How many replicas each database has.
pub const REPLICATION_FACTOR: u32 = 3;
/// How long the setup and the final reads retry, in all.
pub const RETRY_FOR: Duration = Duration::from_secs(60);
/// How long the setup and the final reads wait between attempts.
const RETRY_EVERY: Duration = Duration::from_millis(500);
/// How many writes one client's values are apart: client `k` writes `k * VALUE_STRIDE + n`.
const VALUE_STRIDE: i64 = 1_000_000_000;

/// Returns the database with index `index`: `chaos/db-<index>`.
///
/// # Panics
///
/// Never for the indexes a run uses; the name is valid for any index.
#[must_use]
pub fn database_name(index: usize) -> DatabaseName {
    database_label(index)
        .parse()
        .unwrap_or_else(|error| panic!("chaos/db-{index} isn't a database name: {error}"))
}

/// Returns how database `index` is created: quorum for 0..6, all for 6 and 7; local for every
/// database in the self-test.
#[must_use]
pub fn created_durability(index: usize, mode: Mode) -> DurabilityMode {
    match mode {
        Mode::SelfTest => DurabilityMode::Local,
        Mode::Normal => checked_guarantee(index).mode(),
    }
}

/// Returns what the checker holds database `index` to: quorum for 0..6 (and for every
/// database in the self-test), all for 6 and 7.
#[must_use]
pub fn checked_guarantee(index: usize) -> Guarantee {
    let replicas = u8::try_from(REPLICATION_FACTOR)
        .ok()
        .and_then(ReplicationFactor::new)
        .unwrap_or(ReplicationFactor::ONE);
    let mode = if index < 6 {
        DurabilityMode::Quorum
    } else {
        DurabilityMode::All
    };
    Guarantee::new(replicas, mode)
}

/// Returns the value client `client` writes as its `n`th write: unique across the run.
#[must_use]
pub fn written_value(client: usize, n: i64) -> i64 {
    i64::try_from(client).unwrap_or(i64::MAX / VALUE_STRIDE) * VALUE_STRIDE + n
}

/// The shared history, with the instant its times count from.
#[derive(Debug, Clone)]
pub struct Recorder {
    history: Arc<Mutex<History>>,
    t0: Instant,
}

impl Recorder {
    /// Returns an empty history whose times count from `t0`.
    #[must_use]
    pub fn new(t0: Instant) -> Self {
        Self {
            history: Arc::default(),
            t0,
        }
    }

    /// Sends `operation` through `data`, recording it when sent and when it ends; an
    /// operation without an outcome within [`OPERATION_TIMEOUT`] ends indeterminate, and the
    /// client's connections are dropped, since one may have stalled.
    pub async fn perform(&self, client: &str, data: &Client, operation: Operation) -> Outcome {
        let id = self
            .lock()
            .begin(client, self.t0.elapsed(), operation.clone());
        let outcome = match timeout(OPERATION_TIMEOUT, data_operation(data, &operation)).await {
            Ok(Some(outcome)) => outcome,
            Ok(None) => Outcome::Rejected {
                code: tonic::Code::Internal,
                message: "not a data operation".to_owned(),
            },
            Err(_) => {
                data.reconnect();
                Outcome::Indeterminate { code: None }
            }
        };
        self.lock().complete(id, self.t0.elapsed(), outcome.clone());
        outcome
    }

    /// Returns how long it has been since the instant the history's times count from.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.t0.elapsed()
    }

    /// Returns a copy of the history so far.
    #[must_use]
    pub fn history(&self) -> History {
        self.lock().clone()
    }

    fn lock(&self) -> MutexGuard<'_, History> {
        self.history.lock().expect("history lock poisoned")
    }
}

/// Creates the eight databases and their writes tables, retrying each until it is
/// acknowledged, for up to [`RETRY_FOR`] in all.
///
/// # Errors
///
/// [`WorkloadError::NotCreated`] naming the first database not acknowledged in time.
pub async fn create_databases(
    recorder: &Recorder,
    data: &Client,
    mode: Mode,
) -> Result<(), WorkloadError> {
    let give_up = Instant::now() + RETRY_FOR;
    for index in 0..DATABASES {
        let database = database_name(index);
        let operation = Operation::Create {
            database: database.clone(),
            replication_factor: REPLICATION_FACTOR,
            durability: created_durability(index, mode),
        };
        retry(recorder, "setup", data, &operation, give_up)
            .await
            .map_err(|()| WorkloadError::NotCreated { database })?;
    }
    Ok(())
}

/// Runs the ten clients until `deadline`. Client `k` is named `client-k`, takes the `k`th fork
/// of `root`, and has its own [`Client`] over `network`.
pub async fn run_clients(
    recorder: Recorder,
    network: Network,
    root: &mut SimRng,
    deadline: Instant,
) {
    let mut clients = JoinSet::new();
    for index in 0..CLIENTS {
        let rng = root.fork();
        let data = Client::new(Node::CONTROL.address(), network.clone());
        clients.spawn(run_client(recorder.clone(), index, data, rng, deadline));
    }
    while clients.join_next().await.is_some() {}
}

/// One client's loop: a write (70 %) or a read (30 %) of a random database, then a pause.
async fn run_client(
    recorder: Recorder,
    index: usize,
    data: Client,
    mut rng: SimRng,
    deadline: Instant,
) {
    let name = format!("client-{index}");
    let mut writes = 0;
    while Instant::now() < deadline {
        let database = database_name(rng.index(DATABASES));
        let operation = if rng.chance(WRITE_PERCENT, 100) {
            writes += 1;
            Operation::Write {
                database,
                value: written_value(index, writes),
            }
        } else {
            Operation::Read {
                database,
                value: None,
            }
        };
        recorder.perform(&name, &data, operation).await;
        sleep(PAUSE_BETWEEN_OPERATIONS).await;
    }
}

/// Reads every database once more, retrying each until it is acknowledged, for up to
/// [`RETRY_FOR`] in all.
///
/// # Errors
///
/// [`WorkloadError::NotRead`] naming the first database not read in time.
pub async fn final_reads(recorder: &Recorder, data: &Client) -> Result<(), WorkloadError> {
    let give_up = Instant::now() + RETRY_FOR;
    for index in 0..DATABASES {
        let database = database_name(index);
        let operation = Operation::Read {
            database: database.clone(),
            value: None,
        };
        retry(recorder, "final", data, &operation, give_up)
            .await
            .map_err(|()| WorkloadError::NotRead { database })?;
    }
    Ok(())
}

/// Performs `operation` every [`RETRY_EVERY`] until it is acknowledged, or fails once
/// `give_up` passes.
async fn retry(
    recorder: &Recorder,
    client: &str,
    data: &Client,
    operation: &Operation,
    give_up: Instant,
) -> Result<(), ()> {
    loop {
        let outcome = recorder.perform(client, data, operation.clone()).await;
        if matches!(outcome, Outcome::Acknowledged(_)) {
            return Ok(());
        }
        if Instant::now() + RETRY_EVERY > give_up {
            return Err(());
        }
        sleep(RETRY_EVERY).await;
    }
}

/// Why the workload couldn't set up or finish.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WorkloadError {
    /// A database wasn't created in time.
    #[error("database {database} wasn't created within 60 s")]
    NotCreated {
        /// The database.
        database: DatabaseName,
    },
    /// A database couldn't be read in time.
    #[error("database {database} couldn't be read within 60 s")]
    NotRead {
        /// The database.
        database: DatabaseName,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn self_tests_create_local_databases_but_check_them_as_quorum() {
        for index in 0..6 {
            assert_eq!(
                created_durability(index, Mode::SelfTest),
                DurabilityMode::Local
            );
            assert_eq!(checked_guarantee(index).mode(), DurabilityMode::Quorum);
            assert_eq!(
                created_durability(index, Mode::Normal),
                DurabilityMode::Quorum
            );
        }
        for index in 6..8 {
            assert_eq!(
                created_durability(index, Mode::SelfTest),
                DurabilityMode::Local
            );
        }
    }

    #[test]
    fn two_databases_acknowledge_on_every_replica() {
        for index in [6, 7] {
            assert_eq!(created_durability(index, Mode::Normal), DurabilityMode::All);
            assert_eq!(checked_guarantee(index).mode(), DurabilityMode::All);
            assert_eq!(checked_guarantee(index).write_quorum(), 3);
        }
        assert_eq!(checked_guarantee(0).write_quorum(), 2);
    }

    #[test]
    fn values_are_unique_across_clients() {
        let mut seen = BTreeSet::new();
        for client in 0..CLIENTS {
            for n in [1, 2, 999_999_999] {
                assert!(seen.insert(written_value(client, n)), "{client} {n}");
            }
        }
        // The largest value of one client stays below the smallest of the next.
        for client in 0..CLIENTS - 1 {
            assert!(written_value(client, 999_999_999) < written_value(client + 1, 1));
        }
    }

    #[test]
    fn databases_are_named_under_the_fleet() {
        assert_eq!(database_name(0).as_str(), "chaos/db-0");
        assert_eq!(database_name(7).as_str(), "chaos/db-7");
    }
}
