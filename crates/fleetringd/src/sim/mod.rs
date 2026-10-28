//! The simulation harness: whole clusters of real daemons on turmoil's simulated network and
//! clock, in one test process (ADR-0020).
//!
//! It lives in the library, behind the `simulation` feature, rather than in a test-support
//! module, so that every test binary and every later ticket can build on one copy, and so that
//! it is linted and documented like the daemon it drives.
//!
//! - [`Cluster`] runs one turmoil host per node, each running the real [`Daemon`](crate::Daemon)
//!   in the control role, the data role, or both, through
//!   [`Daemon::start_with`](crate::Daemon::start_with) and
//!   [`Environment::simulated`](crate::Environment::simulated). A host's data directory is a real
//!   temporary directory that survives crashes; its memory doesn't.
//! - [`Fault`]s partition and repair links, add latency or loss, and crash, stop, or restart
//!   hosts.
//! - [`Workload`]s run on client hosts and record every operation and its [`Outcome`] in a
//!   [`History`].
//! - [`check`] holds the invariants; the cluster runs them during and after a run.
//! - The [`Trace`] records faults, publications, and operations in order, so two runs of a seed
//!   can be compared with [`Trace::compare`].
//! - [`seeds`] runs a scenario over the fixed CI seeds, or the one in `FLEETRING_SIM_SEED`, and
//!   prints the command that reruns a failing seed.
//!
//! # Determinism
//!
//! One seed drives turmoil's network and, through [`SimRng`], every workload. Daemon code reads
//! time only from `tokio::time` and the injected clock. Built with
//! `RUSTFLAGS="--cfg tokio_unstable"`, turmoil also seeds Tokio's scheduler, and a seed replays
//! the same trace with the same simulated times ([`Replay::Exact`]). Without it, Tokio's
//! `select!` picks among ready branches at random, so events can shift by a few simulated
//! milliseconds; only the test's own faults and notes are compared, in order and without times
//! ([`Replay::Order`]).
//!
//! # Example
//!
//! ```
//! use std::time::Duration;
//!
//! use fleetringd::sim::{Cluster, CreateAndAcquire};
//!
//! let mut cluster = Cluster::builder(7)
//!     .control("control")
//!     .data("node-1")
//!     .data("node-2")
//!     .build()?;
//! cluster.await_ring(Duration::from_secs(5))?;
//! let nodes = ["node-1".parse()?, "node-2".parse()?];
//! cluster.client("client", CreateAndAcquire::new(nodes, 4, 2, 4));
//! cluster.partition("node-2", "control")?;
//! cluster.run_for(Duration::from_secs(2))?;
//! cluster.repair("node-2", "control")?;
//! cluster.crash("control")?;
//! cluster.restart("control")?;
//! cluster.await_workloads(Duration::from_secs(30))?;
//! cluster.await_ring(Duration::from_secs(5))?;
//! let report = cluster.finish()?;
//! println!("{}", report.trace());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod check;
mod client;
mod cluster;
mod fault;
mod host;
mod rng;
pub mod seeds;
mod trace;
mod workload;

pub use check::{ControlLog, Invariant, Violation};
pub use cluster::{Cluster, ClusterBuilder, PORT, Report, SimError, TRACE_VAR, Timings};
pub use fault::{Fault, LossRate};
pub use rng::SimRng;
pub use trace::{DRIVER, Divergence, Event, EventKind, Replay, Trace};
pub use workload::{
    Ack, CreateAndAcquire, History, Operation, Operator, Outcome, Record, Script, Step, Workload,
};
