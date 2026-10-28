//! Where an open database's blocking calls run: a dedicated thread, or inline (ADR-0013, ADR-0020).

use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;

use tokio::sync::oneshot;

use crate::engine::{Database, DatabaseEngine, EngineError, Execution, OpenMode};

type Job = Box<dyn FnOnce(&mut dyn Database) + Send>;

/// The engine's thread, or the engine itself when it runs inline.
#[derive(Debug)]
pub(super) enum Executor {
    Inline(Box<dyn Database>),
    Thread {
        jobs: mpsc::Sender<Job>,
        closed: oneshot::Receiver<Result<(), EngineError>>,
    },
}

/// The database's thread stopped, so a call never ran or never replied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("database thread stopped")]
pub(super) struct ThreadStopped;

/// What an executor needs to open its database.
#[derive(Debug)]
pub(super) struct OpenRequest {
    pub(super) engine: Arc<dyn DatabaseEngine>,
    pub(super) location: PathBuf,
    pub(super) mode: OpenMode,
    pub(super) execution: Execution,
    pub(super) thread_name: String,
}

impl Executor {
    /// Opens the database where `request.execution` says calls run.
    pub(super) async fn open(request: OpenRequest) -> Result<Self, EngineError> {
        let OpenRequest {
            engine,
            location,
            mode,
            execution,
            thread_name,
        } = request;
        match execution {
            Execution::Inline => Ok(Self::Inline(engine.open(&location, mode)?)),
            Execution::Threads => {
                let (opened_tx, opened_rx) = oneshot::channel();
                let (jobs, job_rx) = mpsc::channel::<Job>();
                let (closed_tx, closed) = oneshot::channel();
                thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || {
                        let mut database = match engine.open(&location, mode) {
                            Ok(database) => {
                                let _ = opened_tx.send(Ok(()));
                                database
                            }
                            Err(error) => {
                                let _ = opened_tx.send(Err(error));
                                return;
                            }
                        };
                        while let Ok(job) = job_rx.recv() {
                            job(database.as_mut());
                        }
                        let _ = closed_tx.send(database.close());
                    })
                    .map_err(|error| EngineError::Storage(error.into()))?;
                match opened_rx.await {
                    Ok(Ok(())) => Ok(Self::Thread { jobs, closed }),
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(EngineError::Storage(ThreadStopped.into())),
                }
            }
        }
    }

    /// Runs `call` against the database and returns its result.
    pub(super) async fn run<T: Send + 'static>(
        &mut self,
        call: impl FnOnce(&mut dyn Database) -> T + Send + 'static,
    ) -> Result<T, ThreadStopped> {
        match self {
            Self::Inline(database) => Ok(call(database.as_mut())),
            Self::Thread { jobs, .. } => {
                let (reply, result) = oneshot::channel();
                let job: Job = Box::new(move |database| drop(reply.send(call(database))));
                jobs.send(job).map_err(|_| ThreadStopped)?;
                result.await.map_err(|_| ThreadStopped)
            }
        }
    }

    /// Closes the database, after every call already sent has run.
    pub(super) async fn close(self) -> Result<(), EngineError> {
        match self {
            Self::Inline(database) => database.close(),
            Self::Thread { jobs, closed } => {
                drop(jobs);
                closed
                    .await
                    .map_err(|_| EngineError::Storage(ThreadStopped.into()))?
            }
        }
    }
}
