//! The per-database actor: one task that owns an open database (ADR-0013, point 6).
//!
//! The actor serializes writes, sends reads to the engine's read path, and turns each committed
//! write transaction of a frame-source engine into exactly one batch for the
//! [`ReplicationSink`]. It closes the database when it has been idle for the configured time, or
//! when the registry evicts it.

mod executor;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::DatabaseName;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;

use self::executor::{Executor, OpenRequest};
use crate::engine::{
    CheckpointMarker, Database, DatabaseCapability, DatabaseEngine, EngineError, Executed,
    Execution, FrameBatch, FrameSource, OpenMode, Position, ReplicaPosition, Rows, Shape, Snapshot,
    Statement, Transaction,
};
use crate::sink::{Acknowledgement, CommittedBatch, ReplicationError, ReplicationSink};

type Reply<T> = oneshot::Sender<Result<T, DatabaseError>>;

/// A request in an actor's mailbox.
pub(crate) enum Request {
    Ready(Reply<()>),
    Execute(Transaction, Reply<(Executed, Option<Acknowledgement>)>),
    Query(Statement, Reply<Rows>),
    Position(Reply<Option<Position>>),
    CommittedSince(Option<Position>, Reply<Option<FrameBatch>>),
    Checkpoint(Reply<CheckpointMarker>),
    Snapshot(PathBuf, Reply<Snapshot>),
    ReplicaPositions(Reply<Vec<ReplicaPosition>>),
}

impl Request {
    /// Answers the request with `error` without running it.
    fn fail(self, error: DatabaseError) {
        match self {
            Self::Ready(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Execute(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Query(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Position(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::CommittedSince(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Checkpoint(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Snapshot(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::ReplicaPositions(reply) => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ready(_) => "Ready",
            Self::Execute(..) => "Execute",
            Self::Query(..) => "Query",
            Self::Position(_) => "Position",
            Self::CommittedSince(..) => "CommittedSince",
            Self::Checkpoint(_) => "Checkpoint",
            Self::Snapshot(..) => "Snapshot",
            Self::ReplicaPositions(_) => "ReplicaPositions",
        })
    }
}

/// The error returned by a request to an open database.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DatabaseError {
    /// The database closed before it accepted the request, which never ran. Open it again.
    #[error("database {name} is closed")]
    Closed {
        /// The database.
        name: DatabaseName,
    },
    /// The database stopped after accepting the request, so it may or may not have run.
    #[error("database {name} stopped before replying")]
    Interrupted {
        /// The database.
        name: DatabaseName,
    },
    /// The database couldn't be opened.
    #[error("database {name} could not be opened")]
    Open {
        /// The database.
        name: DatabaseName,
        /// Why.
        source: Arc<EngineError>,
    },
    /// The database refuses writes because it can no longer read its own log. The write that
    /// found out may have committed.
    #[error("database {name} can no longer capture commits")]
    Failed {
        /// The database.
        name: DatabaseName,
        /// Why.
        source: Arc<EngineError>,
    },
    /// The operation needs an engine of another shape.
    #[error("{operation} needs a {needed} engine")]
    Unsupported {
        /// The operation.
        operation: &'static str,
        /// The shape it needs.
        needed: Shape,
    },
    /// The engine refused or failed the request.
    #[error(transparent)]
    Engine(#[from] EngineError),
    /// The write committed locally, but replication couldn't store it.
    #[error(transparent)]
    Replication(#[from] ReplicationError),
}

impl DatabaseError {
    /// The engine error behind this error, if there is one.
    #[must_use]
    pub fn engine_error(&self) -> Option<&EngineError> {
        match self {
            Self::Open { source, .. } | Self::Failed { source, .. } => Some(source),
            Self::Engine(source) => Some(source),
            _ => None,
        }
    }
}

/// A cheap, cloneable handle to an open database's actor.
///
/// A handle stops working when its database closes, after an idle timeout or an eviction:
/// requests then fail with [`DatabaseError::Closed`], and the caller opens the database again
/// through the [`Registry`](crate::Registry).
#[derive(Debug, Clone)]
pub struct DatabaseHandle {
    name: DatabaseName,
    shape: Shape,
    sender: mpsc::Sender<Request>,
}

impl DatabaseHandle {
    /// The database's name.
    #[must_use]
    pub fn name(&self) -> &DatabaseName {
        &self.name
    }

    /// How the database replicates. Callers check it before frame-source or self-replicating
    /// requests.
    #[must_use]
    pub fn shape(&self) -> Shape {
        self.shape
    }

    /// Whether the database has closed, so that this handle no longer works.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Runs `transaction` as one write transaction, then waits for replication to acknowledge
    /// its batch.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed, a statement fails (nothing commits), or
    /// replication fails (the write committed locally).
    pub async fn execute(&self, transaction: Transaction) -> Result<Executed, DatabaseError> {
        let permit = self.reserve().await.ok_or_else(|| self.closed())?;
        self.execute_reserved(permit, transaction).await
    }

    /// Runs a read-only query on the read path, against the latest commit.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed or the query fails.
    pub async fn query(&self, statement: Statement) -> Result<Rows, DatabaseError> {
        let permit = self.reserve().await.ok_or_else(|| self.closed())?;
        self.query_reserved(permit, statement).await
    }

    /// The position just past the last committed transaction, or `None` for an empty log.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed or the engine can't report it.
    pub async fn position(&self) -> Result<Option<Position>, DatabaseError> {
        self.call(Request::Position).await
    }

    /// Every committed transaction after `after`, as one batch, for a follower catching up.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine is a frame source, and [`DatabaseError`]
    /// if the database is closed or `after` doesn't fit its log.
    pub async fn committed_since(
        &self,
        after: Option<Position>,
    ) -> Result<Option<FrameBatch>, DatabaseError> {
        self.call(|reply| Request::CommittedSince(after, reply))
            .await
    }

    /// Checkpoints a frame-source database's log, and returns the marker for followers.
    ///
    /// This is the only way a database checkpoints: the engine never does on its own.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine is a frame source, and [`DatabaseError`]
    /// if the database is closed or the checkpoint can't complete.
    pub async fn checkpoint(&self) -> Result<CheckpointMarker, DatabaseError> {
        self.call(Request::Checkpoint).await
    }

    /// Writes a consistent copy of the database into `destination`.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed or the copy can't be written.
    pub async fn snapshot(
        &self,
        destination: impl Into<PathBuf>,
    ) -> Result<Snapshot, DatabaseError> {
        let destination = destination.into();
        self.call(|reply| Request::Snapshot(destination, reply))
            .await
    }

    /// How far each replica of a self-replicating database has got.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine replicates itself, and
    /// [`DatabaseError`] if the database is closed or the engine can't report them.
    pub async fn replica_positions(&self) -> Result<Vec<ReplicaPosition>, DatabaseError> {
        self.call(Request::ReplicaPositions).await
    }

    /// Waits until the database has opened.
    pub(crate) async fn ready(&self) -> Result<(), DatabaseError> {
        self.call(Request::Ready).await
    }

    /// Reserves a mailbox slot, or returns `None` if the database has closed.
    pub(crate) async fn reserve(&self) -> Option<mpsc::Permit<'_, Request>> {
        self.sender.reserve().await.ok()
    }

    pub(crate) async fn execute_reserved(
        &self,
        permit: mpsc::Permit<'_, Request>,
        transaction: Transaction,
    ) -> Result<Executed, DatabaseError> {
        let (reply, result) = oneshot::channel();
        permit.send(Request::Execute(transaction, reply));
        let (executed, acknowledgement) = self.await_reply(result).await?;
        if let Some(acknowledgement) = acknowledgement {
            acknowledgement.wait().await?;
        }
        Ok(executed)
    }

    pub(crate) async fn query_reserved(
        &self,
        permit: mpsc::Permit<'_, Request>,
        statement: Statement,
    ) -> Result<Rows, DatabaseError> {
        let (reply, result) = oneshot::channel();
        permit.send(Request::Query(statement, reply));
        self.await_reply(result).await
    }

    async fn call<T>(&self, request: impl FnOnce(Reply<T>) -> Request) -> Result<T, DatabaseError> {
        let permit = self.reserve().await.ok_or_else(|| self.closed())?;
        let (reply, result) = oneshot::channel();
        permit.send(request(reply));
        self.await_reply(result).await
    }

    async fn await_reply<T>(
        &self,
        result: oneshot::Receiver<Result<T, DatabaseError>>,
    ) -> Result<T, DatabaseError> {
        result.await.map_err(|_| DatabaseError::Interrupted {
            name: self.name.clone(),
        })?
    }

    fn closed(&self) -> DatabaseError {
        DatabaseError::Closed {
            name: self.name.clone(),
        }
    }
}

/// What the registry needs to start an actor.
#[derive(Debug)]
pub(crate) struct ActorConfig {
    pub(crate) name: DatabaseName,
    pub(crate) engine: Arc<dyn DatabaseEngine>,
    pub(crate) sink: Arc<dyn ReplicationSink>,
    pub(crate) location: PathBuf,
    pub(crate) mode: OpenMode,
    pub(crate) execution: Execution,
    pub(crate) idle_timeout: Duration,
    pub(crate) mailbox: usize,
    pub(crate) thread_name: String,
}

/// A started actor, as the registry holds it.
#[derive(Debug)]
pub(crate) struct Spawned {
    pub(crate) handle: DatabaseHandle,
    /// Asks the actor to finish its queued requests and close.
    pub(crate) close: oneshot::Sender<()>,
    /// Turns true once the actor has closed its database.
    pub(crate) closed: watch::Receiver<bool>,
}

/// Starts an actor for one database. `on_exit` runs after the database has closed, just before
/// `closed` turns true.
pub(crate) fn spawn(config: ActorConfig, on_exit: impl FnOnce() + Send + 'static) -> Spawned {
    let (sender, mailbox) = mpsc::channel(config.mailbox);
    let (close, close_requested) = oneshot::channel();
    let (closed_tx, closed) = watch::channel(false);
    let handle = DatabaseHandle {
        name: config.name.clone(),
        shape: config.engine.capability().shape(),
        sender,
    };
    tokio::spawn(async move {
        run(config, mailbox, close_requested).await;
        on_exit();
        let _ = closed_tx.send(true);
    });
    Spawned {
        handle,
        close,
        closed,
    }
}

async fn run(
    config: ActorConfig,
    mut mailbox: mpsc::Receiver<Request>,
    close_requested: oneshot::Receiver<()>,
) {
    let ActorConfig {
        name,
        engine,
        sink,
        location,
        mode,
        execution,
        idle_timeout,
        thread_name,
        ..
    } = config;
    let opened = Executor::open(OpenRequest {
        engine,
        location,
        mode,
        execution,
        thread_name,
    })
    .await;
    let executor = match opened {
        Ok(executor) => executor,
        Err(error) => {
            let source = Arc::new(error);
            tracing::debug!(database = %name, error = %source, "database could not be opened");
            mailbox.close();
            while let Some(request) = mailbox.recv().await {
                request.fail(DatabaseError::Open {
                    name: name.clone(),
                    source: Arc::clone(&source),
                });
            }
            return;
        }
    };
    let mut actor = Actor {
        name,
        executor,
        sink,
        last: None,
        failed: None,
    };
    actor.start().await;
    actor
        .serve(&mut mailbox, close_requested, idle_timeout)
        .await;
    mailbox.close();
    while let Some(request) = mailbox.recv().await {
        actor.handle(request).await;
    }
    let name = actor.name.clone();
    if let Err(error) = actor.executor.close().await {
        tracing::error!(database = %name, %error, "closing the database failed");
    }
}

/// The state an actor keeps while its database is open.
struct Actor {
    name: DatabaseName,
    executor: Executor,
    sink: Arc<dyn ReplicationSink>,
    /// The end of the last batch captured, for a frame source.
    last: Option<Position>,
    /// Why the database stopped taking writes, if it has.
    failed: Option<Arc<EngineError>>,
}

impl Actor {
    /// Finds where the log ends, so the first write's batch holds only that write.
    async fn start(&mut self) {
        let position = self
            .executor
            .run(|database| match database.capability() {
                DatabaseCapability::FrameSource(frames) => frames.position(),
                _ => Ok(None),
            })
            .await;
        match position {
            Ok(Ok(position)) => self.last = position,
            Ok(Err(error)) => drop(self.fail(error)),
            Err(stopped) => drop(self.fail(EngineError::Storage(stopped.into()))),
        }
    }

    async fn serve(
        &mut self,
        mailbox: &mut mpsc::Receiver<Request>,
        mut close_requested: oneshot::Receiver<()>,
        idle_timeout: Duration,
    ) {
        loop {
            let request = tokio::select! {
                biased;
                _ = &mut close_requested => return,
                received = time::timeout(idle_timeout, mailbox.recv()) => match received {
                    Ok(Some(request)) => request,
                    Ok(None) | Err(_) => return,
                },
            };
            self.handle(request).await;
        }
    }

    async fn handle(&mut self, request: Request) {
        match request {
            Request::Ready(reply) => {
                let _ = reply.send(Ok(()));
            }
            Request::Execute(transaction, reply) => {
                let result = self.execute(transaction).await;
                let _ = reply.send(result);
            }
            Request::Query(statement, reply) => {
                let result = self
                    .call(move |database| Ok(database.query(&statement)?))
                    .await;
                let _ = reply.send(result);
            }
            Request::Position(reply) => {
                let last = self.last.clone();
                let result = self
                    .call(move |database| match database.capability() {
                        DatabaseCapability::FrameSource(_) => Ok(last),
                        DatabaseCapability::SelfReplicating(engine) => Ok(engine.position()?),
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::CommittedSince(after, reply) => {
                let result = self
                    .call(move |database| {
                        Ok(frame_source(database, "reading committed batches")?
                            .committed_since(after.as_ref())?)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::Checkpoint(reply) => {
                let result = self
                    .call(|database| Ok(frame_source(database, "checkpointing")?.checkpoint()?))
                    .await;
                let _ = reply.send(result);
            }
            Request::Snapshot(destination, reply) => {
                let result = self
                    .call(move |database| Ok(database.snapshot(&destination)?))
                    .await;
                let _ = reply.send(result);
            }
            Request::ReplicaPositions(reply) => {
                let result = self
                    .call(|database| match database.capability() {
                        DatabaseCapability::SelfReplicating(engine) => {
                            Ok(engine.replica_positions()?)
                        }
                        _ => Err(DatabaseError::Unsupported {
                            operation: "reading replica positions",
                            needed: Shape::SelfReplicating,
                        }),
                    })
                    .await;
                let _ = reply.send(result);
            }
        }
    }

    /// Runs one write transaction and captures its batch, if it committed one.
    async fn execute(
        &mut self,
        transaction: Transaction,
    ) -> Result<(Executed, Option<Acknowledgement>), DatabaseError> {
        if let Some(source) = &self.failed {
            return Err(DatabaseError::Failed {
                name: self.name.clone(),
                source: Arc::clone(source),
            });
        }
        let last = self.last.clone();
        let outcome = self
            .executor
            .run(move |database| {
                let result = database.execute(&transaction);
                // Capture runs whatever the outcome, so a commit is never missed. A rolled-back
                // transaction leaves no committed frames, so it yields no batch.
                let captured = match database.capability() {
                    DatabaseCapability::FrameSource(frames) => {
                        Some(frames.committed_since(last.as_ref()))
                    }
                    _ => None,
                };
                (result, captured)
            })
            .await;
        let (result, captured) = match outcome {
            Ok(outcome) => outcome,
            Err(stopped) => {
                self.fail(EngineError::Storage(stopped.into()));
                return Err(DatabaseError::Interrupted {
                    name: self.name.clone(),
                });
            }
        };
        let acknowledgement = match captured {
            None | Some(Ok(None)) => None,
            Some(Ok(Some(batch))) => {
                self.last = Some(batch.end.clone());
                Some(self.sink.publish(CommittedBatch {
                    database: self.name.clone(),
                    batch,
                }))
            }
            Some(Err(error)) => {
                let source = self.fail(error);
                return Err(DatabaseError::Failed {
                    name: self.name.clone(),
                    source,
                });
            }
        };
        Ok((result?, acknowledgement))
    }

    async fn call<T: Send + 'static>(
        &mut self,
        call: impl FnOnce(&mut dyn Database) -> Result<T, DatabaseError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        match self.executor.run(call).await {
            Ok(result) => result,
            Err(stopped) => {
                self.fail(EngineError::Storage(stopped.into()));
                Err(DatabaseError::Interrupted {
                    name: self.name.clone(),
                })
            }
        }
    }

    fn fail(&mut self, error: EngineError) -> Arc<EngineError> {
        tracing::error!(database = %self.name, %error, "database stopped taking writes");
        let source = Arc::new(error);
        self.failed = Some(Arc::clone(&source));
        source
    }
}

fn frame_source<'a>(
    database: &'a mut dyn Database,
    operation: &'static str,
) -> Result<&'a mut dyn FrameSource, DatabaseError> {
    match database.capability() {
        DatabaseCapability::FrameSource(frames) => Ok(frames),
        _ => Err(DatabaseError::Unsupported {
            operation,
            needed: Shape::FrameSource,
        }),
    }
}
