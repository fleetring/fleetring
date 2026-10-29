//! Workloads: clients that issue operations and record which ones were acknowledged.
//!
//! A [`Workload`] decides what a client does next; the harness runs the operation, records it in
//! the [`History`] and the trace, and tells the workload how it ended. Control operations go to
//! the control plane; [`Operation::Create`], [`Operation::Write`], and [`Operation::Read`] go
//! through the data plane's `Database` service, as the command line's do.
//!
//! The history records each operation twice, as the Jepsen-style checkers in
//! [`history`](crate::sim::history) need it: once when the client sends it and once when the
//! outcome arrives. An outcome is `ok` ([`Outcome::Acknowledged`]: the effect is durable),
//! `fail` ([`Outcome::Rejected`]: definitely not applied), or `info` ([`Outcome::Indeterminate`]
//! or never completed: unknown).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, LeaseEpoch};
use tonic::Code;

use crate::sim::SimRng;

/// One operation a client sends to the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operation {
    /// Joins `node` to the cluster, with default placement attributes.
    Join {
        /// The node.
        node: NodeId,
    },
    /// Sends a heartbeat for `node`, reporting no databases.
    Heartbeat {
        /// The node.
        node: NodeId,
    },
    /// Removes `node`, as an operator on the control host would: it is submitted to the control
    /// node in process, not through the `Leave` RPC a node calls when it shuts down.
    Leave {
        /// The node.
        node: NodeId,
    },
    /// Creates a SQLite database.
    CreateDatabase {
        /// The database.
        database: DatabaseName,
        /// How many replicas it should have.
        replication_factor: u32,
    },
    /// Asks where a database lives.
    Locate {
        /// The database.
        database: DatabaseName,
    },
    /// Takes a database's first lease for `node`.
    AcquireLease {
        /// The database.
        database: DatabaseName,
        /// The node that asks to be primary.
        node: NodeId,
    },
    /// Renews `node`'s lease at `epoch`.
    RenewLease {
        /// The database.
        database: DatabaseName,
        /// The lease holder.
        node: NodeId,
        /// The lease's epoch.
        epoch: LeaseEpoch,
    },
    /// Creates a SQLite database through the data plane, as `fleetringd db create` does: the
    /// control plane places it, and the first live member of its replica set takes the lease and
    /// opens it. Then creates its [`WRITES_TABLE`]. A database that already exists counts as
    /// created once its table is there, so the operation can be repeated until it succeeds.
    Create {
        /// The database.
        database: DatabaseName,
        /// How many replicas it should have.
        replication_factor: u32,
    },
    /// Inserts `value` into the database's [`WRITES_TABLE`] on its primary, through the data
    /// plane, and waits for the write quorum to hold it.
    Write {
        /// The database.
        database: DatabaseName,
        /// The value, unique within the run.
        value: i64,
    },
    /// Reads the database's [`WRITES_TABLE`] on its primary, through the data plane's `Query`:
    /// every value, or whether one value is there.
    Read {
        /// The database.
        database: DatabaseName,
        /// The one value to look for, or `None` to read every value.
        value: Option<i64>,
    },
}

impl Operation {
    /// Returns the database a data-plane operation acts on, or `None` for a control operation.
    #[must_use]
    pub fn database(&self) -> Option<&DatabaseName> {
        match self {
            Self::Create { database, .. }
            | Self::Write { database, .. }
            | Self::Read { database, .. }
            | Self::CreateDatabase { database, .. }
            | Self::Locate { database }
            | Self::AcquireLease { database, .. }
            | Self::RenewLease { database, .. } => Some(database),
            Self::Join { .. } | Self::Heartbeat { .. } | Self::Leave { .. } => None,
        }
    }
}

/// The table [`Operation::Create`] makes and [`Operation::Write`] inserts into: one `value`
/// column, one row per acknowledged write.
pub const WRITES_TABLE: &str = "writes";

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Join { node } => write!(f, "join {node}"),
            Self::Heartbeat { node } => write!(f, "heartbeat {node}"),
            Self::Leave { node } => write!(f, "leave {node}"),
            Self::CreateDatabase {
                database,
                replication_factor,
            } => write!(f, "create {database} rf {replication_factor}"),
            Self::Locate { database } => write!(f, "locate {database}"),
            Self::AcquireLease { database, node } => write!(f, "acquire {database} for {node}"),
            Self::RenewLease {
                database,
                node,
                epoch,
            } => write!(f, "renew {database} for {node} at epoch {}", epoch.get()),
            Self::Create {
                database,
                replication_factor,
            } => write!(f, "open {database} rf {replication_factor}"),
            Self::Write { database, value } => write!(f, "write {value} to {database}"),
            Self::Read {
                database,
                value: Some(value),
            } => write!(f, "read {value} from {database}"),
            Self::Read {
                database,
                value: None,
            } => write!(f, "read {database}"),
        }
    }
}

/// How an operation ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// The cluster acknowledged it: the effect is durable and must never be lost.
    Acknowledged(Ack),
    /// The cluster refused it; it had no effect.
    Rejected {
        /// The gRPC status code.
        code: Code,
        /// The status message.
        message: String,
    },
    /// No answer arrived: the operation may or may not have taken effect.
    Indeterminate {
        /// The status code of the failure, or `None` if the call timed out.
        code: Option<Code>,
    },
}

impl Outcome {
    /// Returns the acknowledgement, if the operation was acknowledged.
    #[must_use]
    pub fn ack(&self) -> Option<&Ack> {
        match self {
            Self::Acknowledged(ack) => Some(ack),
            _ => None,
        }
    }

    /// Returns the outcome's class: what the checkers may assume about the operation's effect.
    #[must_use]
    pub fn class(&self) -> OutcomeClass {
        match self {
            Self::Acknowledged(_) => OutcomeClass::Ok,
            Self::Rejected { .. } => OutcomeClass::Fail,
            Self::Indeterminate { .. } => OutcomeClass::Info,
        }
    }

    pub(crate) fn from_status(status: &tonic::Status) -> Self {
        match status.code() {
            Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Cancelled
            | Code::Unknown
            | Code::Internal
            | Code::Aborted
            | Code::ResourceExhausted => Self::Indeterminate {
                code: Some(status.code()),
            },
            code => Self::Rejected {
                code,
                message: status.message().to_owned(),
            },
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acknowledged(ack) => write!(f, "ok {ack}"),
            Self::Rejected { code, message } => write!(f, "rejected {code:?}: {message}"),
            Self::Indeterminate { code: Some(code) } => write!(f, "indeterminate {code:?}"),
            Self::Indeterminate { code: None } => f.write_str("indeterminate timeout"),
        }
    }
}

/// What a checker may assume about an operation's effect, from how it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutcomeClass {
    /// The operation was acknowledged: its effect is durable and must never be lost.
    Ok,
    /// The operation was refused: it definitely had no effect.
    Fail,
    /// The operation got no answer, or an answer that leaves its effect unknown: it may or may
    /// not have taken effect. An operation still in flight is `info` too.
    Info,
}

impl fmt::Display for OutcomeClass {
    /// Honors a width, so excerpt columns line up.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(match self {
            Self::Ok => "ok",
            Self::Fail => "fail",
            Self::Info => "info",
        })
    }
}

/// What an acknowledged operation returned.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Ack {
    /// The node is a member, at this membership epoch.
    Joined {
        /// The membership epoch after the join.
        membership_epoch: u64,
    },
    /// The heartbeat was recorded.
    HeartbeatRecorded,
    /// The node was removed.
    Left,
    /// The database was created.
    Created,
    /// Where the database lives.
    Located {
        /// The database's epoch.
        epoch: LeaseEpoch,
        /// Its lease holder, if it has one.
        primary: Option<NodeId>,
    },
    /// A lease was granted or renewed.
    Lease {
        /// The lease's epoch.
        epoch: LeaseEpoch,
        /// When it expires, in control-plane milliseconds.
        expires_at_ms: u64,
    },
    /// The database exists, with a primary that serves it.
    Opened {
        /// The primary.
        primary: NodeId,
        /// The primary's lease epoch.
        epoch: LeaseEpoch,
    },
    /// A write committed and a write quorum stored it: it must never be lost.
    Written {
        /// How many replicas, the primary included, held it when it was acknowledged: W or
        /// more.
        acknowledged_by: u32,
        /// R, the size the database's replica set should have.
        replicas: u32,
        /// Whether fewer than R of the database's replicas were reachable.
        under_replicated: bool,
        /// The primary that acknowledged it, if it said.
        primary: Option<NodeId>,
        /// The epoch it acknowledged it at, or 0 if it didn't say.
        epoch: LeaseEpoch,
    },
    /// What a read found in the database's [`WRITES_TABLE`]: every value it holds, or, for a
    /// read of one value, that value if it is there.
    Read {
        /// The values found, in order.
        values: BTreeSet<i64>,
    },
}

impl fmt::Display for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Joined { membership_epoch } => write!(f, "joined at epoch {membership_epoch}"),
            Self::HeartbeatRecorded => f.write_str("recorded"),
            Self::Left => f.write_str("left"),
            Self::Created => f.write_str("created"),
            Self::Located { epoch, primary } => match primary {
                Some(primary) => write!(f, "epoch {} primary {primary}", epoch.get()),
                None => write!(f, "epoch {} no primary", epoch.get()),
            },
            Self::Lease {
                epoch,
                expires_at_ms,
            } => write!(f, "lease epoch {} until {expires_at_ms}", epoch.get()),
            Self::Opened { primary, epoch } => {
                write!(f, "opened on {primary} at epoch {}", epoch.get())
            }
            Self::Written {
                acknowledged_by,
                replicas,
                under_replicated,
                primary,
                epoch,
            } => {
                write!(f, "written to {acknowledged_by} of {replicas}")?;
                if *under_replicated {
                    f.write_str(", under-replicated")?;
                }
                if let Some(primary) = primary {
                    write!(f, " by {primary} at epoch {}", epoch.get())?;
                }
                Ok(())
            }
            Self::Read { values } => {
                const SHOWN: usize = 6;
                write!(f, "read {} values", values.len())?;
                if values.len() <= SHOWN {
                    write!(f, " {values:?}")
                } else {
                    let first: Vec<&i64> = values.iter().take(SHOWN).collect();
                    write!(f, " {first:?}…")
                }
            }
        }
    }
}

/// Identifies one operation in a [`History`]: the order its client sent it in, across clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationId(u64);

impl OperationId {
    /// Returns the operation's number, counted from 1 in the order operations were sent.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// One finished operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    id: OperationId,
    client: String,
    invoked_at: Duration,
    completed_at: Duration,
    operation: Operation,
    outcome: Outcome,
}

impl Record {
    /// Returns the operation's ID.
    #[must_use]
    pub fn id(&self) -> OperationId {
        self.id
    }

    /// Returns the client that sent the operation.
    #[must_use]
    pub fn client(&self) -> &str {
        &self.client
    }

    /// Returns the simulated time the operation was sent.
    #[must_use]
    pub fn invoked_at(&self) -> Duration {
        self.invoked_at
    }

    /// Returns the simulated time the outcome arrived.
    #[must_use]
    pub fn completed_at(&self) -> Duration {
        self.completed_at
    }

    /// Returns the operation.
    #[must_use]
    pub fn operation(&self) -> &Operation {
        &self.operation
    }

    /// Returns how it ended.
    #[must_use]
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// Returns what the checkers may assume about the operation's effect.
    #[must_use]
    pub fn class(&self) -> OutcomeClass {
        self.outcome.class()
    }
}

/// An operation a client sent and hasn't heard back about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pending {
    client: String,
    invoked_at: Duration,
    operation: Operation,
}

impl Pending {
    /// Returns the client that sent the operation.
    #[must_use]
    pub fn client(&self) -> &str {
        &self.client
    }

    /// Returns the simulated time the operation was sent.
    #[must_use]
    pub fn invoked_at(&self) -> Duration {
        self.invoked_at
    }

    /// Returns the operation.
    #[must_use]
    pub fn operation(&self) -> &Operation {
        &self.operation
    }
}

/// Which end of an operation a [`HistoryEvent`] marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase<'h> {
    /// The client sent the operation.
    Invoke,
    /// The outcome arrived.
    Complete(&'h Outcome),
}

/// One end of an operation, as [`History::events`] lists them: Jepsen's `:invoke` and `:ok`,
/// `:fail`, or `:info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryEvent<'h> {
    at: Duration,
    client: &'h str,
    id: OperationId,
    operation: &'h Operation,
    phase: Phase<'h>,
}

impl<'h> HistoryEvent<'h> {
    /// Returns the simulated time of the event.
    #[must_use]
    pub fn at(&self) -> Duration {
        self.at
    }

    /// Returns the client that sent the operation.
    #[must_use]
    pub fn client(&self) -> &'h str {
        self.client
    }

    /// Returns the operation's ID.
    #[must_use]
    pub fn id(&self) -> OperationId {
        self.id
    }

    /// Returns the operation.
    #[must_use]
    pub fn operation(&self) -> &'h Operation {
        self.operation
    }

    /// Returns which end of the operation this is.
    #[must_use]
    pub fn phase(&self) -> Phase<'h> {
        self.phase
    }
}

impl fmt::Display for HistoryEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:>8}ms {} {:>6} ",
            self.at.as_millis(),
            self.id,
            self.client
        )?;
        match self.phase {
            Phase::Invoke => write!(f, "invoke {}", self.operation),
            Phase::Complete(outcome) => {
                write!(f, "{:<6} {} -> {outcome}", outcome.class(), self.operation)
            }
        }
    }
}

/// Every operation clients sent: the finished ones in completion order, and the ones still in
/// flight.
///
/// The harness fills it as clients run; a test builds one by hand with [`History::begin`] and
/// [`History::complete`], or [`History::record`], to exercise the checkers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    next_id: u64,
    records: Vec<Record>,
    pending: BTreeMap<OperationId, Pending>,
}

impl History {
    /// Records that `client` sent `operation` at `at`, and returns the ID to complete it with.
    pub fn begin(
        &mut self,
        client: impl Into<String>,
        at: Duration,
        operation: Operation,
    ) -> OperationId {
        self.next_id += 1;
        let id = OperationId(self.next_id);
        self.pending.insert(
            id,
            Pending {
                client: client.into(),
                invoked_at: at,
                operation,
            },
        );
        id
    }

    /// Records that the operation `id` ended with `outcome` at `at`.
    ///
    /// # Panics
    ///
    /// If `id` isn't in flight: it was never begun, or it was completed already.
    pub fn complete(&mut self, id: OperationId, at: Duration, outcome: Outcome) {
        let pending = self
            .pending
            .remove(&id)
            .unwrap_or_else(|| panic!("operation {id} isn't in flight"));
        self.records.push(Record {
            id,
            client: pending.client,
            invoked_at: pending.invoked_at,
            completed_at: at,
            operation: pending.operation,
            outcome,
        });
    }

    /// Records a finished operation in one step, and returns its ID.
    pub fn record(
        &mut self,
        client: impl Into<String>,
        invoked_at: Duration,
        completed_at: Duration,
        operation: Operation,
        outcome: Outcome,
    ) -> OperationId {
        let id = self.begin(client, invoked_at, operation);
        self.complete(id, completed_at, outcome);
        id
    }

    /// Returns every finished operation, in completion order.
    #[must_use]
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Returns the finished operation with ID `id`, if it has finished.
    #[must_use]
    pub fn record_by_id(&self, id: OperationId) -> Option<&Record> {
        self.records.iter().find(|record| record.id == id)
    }

    /// Returns every operation still in flight, with its ID, in the order they were sent.
    pub fn pending(&self) -> impl Iterator<Item = (OperationId, &Pending)> {
        self.pending.iter().map(|(id, pending)| (*id, pending))
    }

    /// Returns both ends of every operation, in time order. At one time, invocations come
    /// before completions, and earlier operations first.
    #[must_use]
    pub fn events(&self) -> Vec<HistoryEvent<'_>> {
        let mut events: Vec<HistoryEvent<'_>> = Vec::with_capacity(2 * self.records.len());
        for record in &self.records {
            events.push(HistoryEvent {
                at: record.invoked_at,
                client: &record.client,
                id: record.id,
                operation: &record.operation,
                phase: Phase::Invoke,
            });
            events.push(HistoryEvent {
                at: record.completed_at,
                client: &record.client,
                id: record.id,
                operation: &record.operation,
                phase: Phase::Complete(&record.outcome),
            });
        }
        for (id, pending) in &self.pending {
            events.push(HistoryEvent {
                at: pending.invoked_at,
                client: &pending.client,
                id: *id,
                operation: &pending.operation,
                phase: Phase::Invoke,
            });
        }
        events.sort_by_key(|event| {
            (
                event.at,
                matches!(event.phase, Phase::Complete(_)),
                event.id,
            )
        });
        events
    }

    /// Returns the acknowledged operations with their acknowledgements, in completion order.
    pub fn acknowledged(&self) -> impl Iterator<Item = (&Operation, &Ack)> {
        self.records
            .iter()
            .filter_map(|record| Some((&record.operation, record.outcome.ack()?)))
    }

    /// Returns every acknowledged write, as its database and value, in completion order. Each
    /// must be found on the database after any fault.
    pub fn acknowledged_writes(&self) -> impl Iterator<Item = (&DatabaseName, i64)> {
        self.acknowledged()
            .filter_map(|(operation, ack)| match (operation, ack) {
                (Operation::Write { database, value }, Ack::Written { .. }) => {
                    Some((database, *value))
                }
                _ => None,
            })
    }

    /// Returns how many operations finished.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns whether no operation finished.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// What a client does next.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Step {
    /// Send an operation and wait for its outcome.
    Run(Operation),
    /// Wait for a while.
    Sleep(Duration),
    /// Stop: the workload is finished.
    Done,
}

/// Decides what a simulated client does.
///
/// The harness calls [`Workload::next`] for each step, with a generator forked from the run's
/// seed, and [`Workload::observe`] with each outcome. A workload must take randomness only from
/// that generator, so a seed replays it.
pub trait Workload: fmt::Debug + 'static {
    /// Returns the client's next step.
    fn next(&mut self, rng: &mut SimRng) -> Step;

    /// Learns how an operation ended. The default ignores it.
    fn observe(&mut self, operation: &Operation, outcome: &Outcome) {
        let _ = (operation, outcome);
    }

    /// Whether the workload runs until the simulation ends rather than finishing on its own.
    /// [`Cluster::await_workloads`](crate::sim::Cluster::await_workloads) doesn't wait for such
    /// a workload. The default is `false`.
    fn is_endless(&self) -> bool {
        false
    }
}

/// How long a script waits before it repeats an operation that got no answer.
const SCRIPT_RETRY: Duration = Duration::from_millis(20);

/// Runs a fixed list of operations in order, repeating each one until it gets an answer.
///
/// An operation counts as answered once it is acknowledged or rejected; `ALREADY_EXISTS` after a
/// lost reply is such an answer.
#[derive(Debug, Clone, Default)]
pub struct Script {
    pending: VecDeque<Operation>,
    waiting: bool,
}

impl Script {
    /// A script of `operations`.
    #[must_use]
    pub fn new(operations: impl IntoIterator<Item = Operation>) -> Self {
        Self {
            pending: operations.into_iter().collect(),
            waiting: false,
        }
    }
}

impl Workload for Script {
    fn next(&mut self, _rng: &mut SimRng) -> Step {
        if self.waiting {
            self.waiting = false;
            return Step::Sleep(SCRIPT_RETRY);
        }
        match self.pending.front() {
            Some(operation) => Step::Run(operation.clone()),
            None => Step::Done,
        }
    }

    fn observe(&mut self, _operation: &Operation, outcome: &Outcome) {
        if matches!(outcome, Outcome::Indeterminate { .. }) {
            self.waiting = true;
        } else {
            self.pending.pop_front();
        }
    }
}

/// How often an idle operator checks its queue.
const OPERATOR_POLL: Duration = Duration::from_millis(10);

/// Operations a test queues while the simulation runs, as an operator would type them.
///
/// It never finishes. Each queued operation runs once, whatever its outcome. Clones share the
/// queue, so the test keeps one and gives another to [`Cluster::client`](crate::sim::Cluster::client).
#[derive(Debug, Clone, Default)]
pub struct Operator {
    queue: Arc<Mutex<VecDeque<Operation>>>,
}

impl Operator {
    /// An operator with nothing queued.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues `operation`.
    ///
    /// # Panics
    ///
    /// If the queue's lock was poisoned by a panic in another thread.
    pub fn push(&self, operation: Operation) {
        self.queue
            .lock()
            .expect("operator queue lock poisoned")
            .push_back(operation);
    }

    /// Returns whether every queued operation has been sent.
    ///
    /// # Panics
    ///
    /// If the queue's lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.queue
            .lock()
            .expect("operator queue lock poisoned")
            .is_empty()
    }
}

impl Workload for Operator {
    fn next(&mut self, _rng: &mut SimRng) -> Step {
        let next = self
            .queue
            .lock()
            .expect("operator queue lock poisoned")
            .pop_front();
        next.map_or(Step::Sleep(OPERATOR_POLL), Step::Run)
    }

    fn is_endless(&self) -> bool {
        true
    }
}

/// The longest pause between two operations of [`CreateAndAcquire`], in milliseconds.
const MAX_PAUSE_MS: u64 = 60;

/// Creates databases, takes each one's first lease for a random node, and renews leases.
///
/// Every database is created with the given replication factor, so with as many data nodes as
/// replicas, any live node may take the lease. An acquire that gets no answer, or is refused,
/// is followed by a `Locate` that learns whether the lease was granted after all. The workload
/// finishes once every database is leased and the renewals are spent.
#[derive(Debug, Clone)]
pub struct CreateAndAcquire {
    nodes: Vec<NodeId>,
    replication_factor: u32,
    databases: Vec<(DatabaseName, Progress)>,
    renewals: u32,
    pause: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Progress {
    Planned,
    Created,
    Uncertain,
    Leased { node: NodeId, epoch: LeaseEpoch },
}

impl CreateAndAcquire {
    /// Creates `databases` databases named `db-0`, `db-1`, …, with `replication_factor`
    /// replicas, leases them to nodes among `nodes`, and then renews leases `renewals` times.
    ///
    /// # Panics
    ///
    /// If `db-N` isn't a valid database name, which it always is.
    #[must_use]
    pub fn new(
        nodes: impl IntoIterator<Item = NodeId>,
        databases: u32,
        replication_factor: u32,
        renewals: u32,
    ) -> Self {
        Self {
            nodes: nodes.into_iter().collect(),
            replication_factor,
            databases: (0..databases)
                .map(|k| {
                    let name = format!("db-{k}");
                    let name = name
                        .parse()
                        .unwrap_or_else(|error| panic!("'{name}' is a valid name: {error}"));
                    (name, Progress::Planned)
                })
                .collect(),
            renewals,
            pause: false,
        }
    }

    fn progress(&mut self, database: &DatabaseName) -> Option<&mut Progress> {
        self.databases
            .iter_mut()
            .find(|(name, _)| name == database)
            .map(|(_, progress)| progress)
    }
}

impl Workload for CreateAndAcquire {
    fn next(&mut self, rng: &mut SimRng) -> Step {
        if self.pause {
            self.pause = false;
            return Step::Sleep(Duration::from_millis(10 + rng.below(MAX_PAUSE_MS - 10)));
        }
        let planned = self
            .databases
            .iter()
            .find(|(_, progress)| *progress == Progress::Planned)
            .map(|(database, _)| database.clone());
        let open: Vec<&(DatabaseName, Progress)> = self
            .databases
            .iter()
            .filter(|(_, progress)| matches!(progress, Progress::Created | Progress::Uncertain))
            .collect();
        let leased: Vec<&(DatabaseName, Progress)> = self
            .databases
            .iter()
            .filter(|(_, progress)| matches!(progress, Progress::Leased { .. }))
            .collect();
        let renewable = self.renewals > 0 && !leased.is_empty();

        let operation = match planned {
            // Creates go in order, interleaved with work on databases already created.
            Some(database) if open.is_empty() || rng.chance(1, 2) => Operation::CreateDatabase {
                database,
                replication_factor: self.replication_factor,
            },
            _ if !open.is_empty() && (!renewable || rng.chance(2, 3)) => {
                let (database, progress) = open[rng.index(open.len())];
                match (progress, self.nodes.get(rng.index(self.nodes.len()))) {
                    (Progress::Created, Some(node)) => Operation::AcquireLease {
                        database: database.clone(),
                        node: node.clone(),
                    },
                    _ => Operation::Locate {
                        database: database.clone(),
                    },
                }
            }
            _ if renewable => {
                self.renewals -= 1;
                match leased[rng.index(leased.len())] {
                    (database, Progress::Leased { node, epoch }) => Operation::RenewLease {
                        database: database.clone(),
                        node: node.clone(),
                        epoch: *epoch,
                    },
                    (database, _) => Operation::Locate {
                        database: database.clone(),
                    },
                }
            }
            Some(database) => Operation::CreateDatabase {
                database,
                replication_factor: self.replication_factor,
            },
            None => return Step::Done,
        };
        self.pause = true;
        Step::Run(operation)
    }

    fn observe(&mut self, operation: &Operation, outcome: &Outcome) {
        match (operation, outcome) {
            (Operation::CreateDatabase { database, .. }, Outcome::Acknowledged(_))
            | (
                Operation::CreateDatabase { database, .. },
                Outcome::Rejected {
                    code: Code::AlreadyExists,
                    ..
                },
            ) => {
                if let Some(progress) = self.progress(database) {
                    *progress = Progress::Created;
                }
            }
            (
                Operation::AcquireLease { database, node },
                Outcome::Acknowledged(Ack::Lease { epoch, .. }),
            ) => {
                let leased = Progress::Leased {
                    node: node.clone(),
                    epoch: *epoch,
                };
                if let Some(progress) = self.progress(database) {
                    *progress = leased;
                }
            }
            (Operation::AcquireLease { database, .. }, _) => {
                if let Some(progress) = self.progress(database) {
                    *progress = Progress::Uncertain;
                }
            }
            (
                Operation::Locate { database },
                Outcome::Acknowledged(Ack::Located { epoch, primary }),
            ) => {
                let next = match primary {
                    Some(node) => Progress::Leased {
                        node: node.clone(),
                        epoch: *epoch,
                    },
                    None => Progress::Created,
                };
                if let Some(progress) = self.progress(database) {
                    *progress = next;
                }
            }
            _ => {}
        }
    }
}

/// The longest pause between two operations of [`CreateAndWrite`], in milliseconds.
const MAX_WRITE_PAUSE_MS: u64 = 30;

/// Creates databases through the data plane, then writes to them and reads them back.
///
/// It creates every database first, repeating each [`Operation::Create`] until it is
/// acknowledged, then sends the given number of [`Operation::Write`]s, each to a random database
/// and with the next value: 0, 1, 2, …. A write is sent once, whatever its outcome, so the
/// history tells which values must survive. One operation in four between the writes is an
/// [`Operation::Read`] instead: of a random database's every value, or of the last value the
/// workload saw acknowledged, so the history has the reads the checkers want. After the last
/// write it reads every database once more, then finishes.
#[derive(Debug, Clone)]
pub struct CreateAndWrite {
    replication_factor: u32,
    databases: Vec<(DatabaseName, bool)>,
    writes: u32,
    next_value: i64,
    last_acknowledged: Option<(DatabaseName, i64)>,
    final_reads: Vec<DatabaseName>,
    pause: bool,
}

impl CreateAndWrite {
    /// Creates `databases` databases named `db-0`, `db-1`, …, with `replication_factor`
    /// replicas, then sends `writes` writes, with reads between them.
    ///
    /// # Panics
    ///
    /// If `db-N` isn't a valid database name, which it always is.
    #[must_use]
    pub fn new(databases: u32, replication_factor: u32, writes: u32) -> Self {
        let databases: Vec<(DatabaseName, bool)> = (0..databases)
            .map(|k| {
                let name = format!("db-{k}");
                let name = name
                    .parse()
                    .unwrap_or_else(|error| panic!("'{name}' is a valid name: {error}"));
                (name, false)
            })
            .collect();
        Self {
            replication_factor,
            final_reads: databases
                .iter()
                .rev()
                .map(|(name, _)| name.clone())
                .collect(),
            databases,
            writes,
            next_value: 0,
            last_acknowledged: None,
            pause: false,
        }
    }

    fn read(&self, rng: &mut SimRng) -> Operation {
        match &self.last_acknowledged {
            Some((database, value)) if rng.chance(1, 3) => Operation::Read {
                database: database.clone(),
                value: Some(*value),
            },
            _ => {
                let (database, _) = &self.databases[rng.index(self.databases.len())];
                Operation::Read {
                    database: database.clone(),
                    value: None,
                }
            }
        }
    }
}

impl Workload for CreateAndWrite {
    fn next(&mut self, rng: &mut SimRng) -> Step {
        if self.pause {
            self.pause = false;
            return Step::Sleep(Duration::from_millis(1 + rng.below(MAX_WRITE_PAUSE_MS)));
        }
        self.pause = true;
        if let Some((database, _)) = self.databases.iter().find(|(_, created)| !created) {
            return Step::Run(Operation::Create {
                database: database.clone(),
                replication_factor: self.replication_factor,
            });
        }
        if self.databases.is_empty() {
            return Step::Done;
        }
        if self.writes == 0 {
            return match self.final_reads.pop() {
                Some(database) => Step::Run(Operation::Read {
                    database,
                    value: None,
                }),
                None => Step::Done,
            };
        }
        if self.next_value > 0 && rng.chance(1, 4) {
            return Step::Run(self.read(rng));
        }
        self.writes -= 1;
        let (database, _) = &self.databases[rng.index(self.databases.len())];
        let value = self.next_value;
        self.next_value += 1;
        Step::Run(Operation::Write {
            database: database.clone(),
            value,
        })
    }

    fn observe(&mut self, operation: &Operation, outcome: &Outcome) {
        match (operation, outcome) {
            (Operation::Create { database, .. }, Outcome::Acknowledged(_)) => {
                if let Some((_, created)) =
                    self.databases.iter_mut().find(|(name, _)| name == database)
                {
                    *created = true;
                }
            }
            (Operation::Write { database, value }, Outcome::Acknowledged(_)) => {
                self.last_acknowledged = Some((database.clone(), *value));
            }
            _ => {}
        }
    }
}
