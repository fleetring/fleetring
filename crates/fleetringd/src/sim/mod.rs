//! The simulation harness: whole fleets of real daemons on turmoil's simulated network and
//! clock, in one test process (ADR-0020).
//!
//! It lives in the library, behind the `simulation` feature, rather than in a test-support
//! module, so that every test binary and every later ticket can build on one copy, and so that
//! it is linted and documented like the daemon it drives.
//!
//! - [`Fleet`] runs one turmoil host per node, each running the real [`Daemon`](crate::Daemon)
//!   in the control role, the data role, or both, through
//!   [`Daemon::start_with`](crate::Daemon::start_with) and
//!   [`Environment::simulated`](crate::Environment::simulated). A host's data directory is a real
//!   temporary directory that survives crashes; its memory doesn't.
//! - [`Fault`]s partition and repair links, add latency or loss, and crash, stop, or restart
//!   hosts.
//! - [`Workload`]s run on client hosts and record every operation, as it is sent and as its
//!   [`Outcome`] arrives, in a [`History`].
//! - [`check`] holds the invariants; the fleet runs them during and after a run. [`history`]
//!   checks the whole history against what each database promises, Jepsen-style, and
//!   [`Fleet::finish`] runs it for every scenario.
//! - A lease watcher per data host follows the host's `WatchLeases` stream (FR-042) and records
//!   what it learns in the [`History`]; [`Fleet::finish`] checks it against the leases the
//!   control log granted ([`check::lease_events_match_control`]).
//! - The [`Trace`] records faults, publications, and operations in order, so two runs of a seed
//!   can be compared with [`Trace::compare`].
//! - [`seeds`] runs a scenario over the fixed CI seeds, or the one in `FLEETRING_SIM_SEED`, and
//!   prints the command that reruns a failing seed.
//!
//! # Determinism
//!
//! One seed drives turmoil's network and, through [`SimRng`], every workload. Daemon code reads
//! time only from `tokio::time` and the injected clock, and a simulated node's data volume is
//! simulated too. Built with `--cfg tokio_unstable`, which the workspace's `.cargo/config.toml`
//! sets, turmoil also seeds Tokio's own randomness, and a seed replays the same trace with the
//! same simulated times, the same history, and the same control log ([`Replay::Exact`]). A
//! `RUSTFLAGS` variable replaces the configured flags; in a build without the cfg, Tokio's
//! `select!` picks among ready branches, and a `watch` channel wakes its receivers, in an order
//! that differs from run to run, so events shift by a few simulated milliseconds and a seed's
//! outcome can change; only the test's own faults and notes are compared, in order and without
//! times ([`Replay::Order`]). SQLite draws its log generations' salts from its own generator,
//! which the process shares, so positions differ in those bytes between runs (ADR-0020's
//! amendment).
//!
//! # Example
//!
//! ```
//! use std::time::Duration;
//!
//! use fleetringd::sim::{Fleet, CreateAndAcquire};
//!
//! let mut fleet = Fleet::builder(7)
//!     .control("control")
//!     .data("node-1")
//!     .data("node-2")
//!     .build()?;
//! fleet.await_ring(Duration::from_secs(5))?;
//! let nodes = ["node-1".parse()?, "node-2".parse()?];
//! fleet.client("client", CreateAndAcquire::new(nodes, 4, 2, 4));
//! fleet.partition("node-2", "control")?;
//! fleet.run_for(Duration::from_secs(2))?;
//! fleet.repair("node-2", "control")?;
//! fleet.crash("control")?;
//! fleet.restart("control")?;
//! fleet.await_workloads(Duration::from_secs(30))?;
//! fleet.await_ring(Duration::from_secs(5))?;
//! let report = fleet.finish()?;
//! println!("{}", report.trace());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod check;
mod client;
mod fault;
mod fleet;
pub mod forks;
pub mod history;
mod host;
mod hrana;
mod rng;
pub mod seeds;
mod trace;
mod workload;

pub use check::{ControlLog, Invariant, ReplicaChains, ReplicaRows, Violation};
pub use fault::{Fault, LossRate};
pub use fleet::{
    Fleet, FleetBuilder, OTHER_PRINCIPAL, PORT, ROUTER_PORT, Report, SimError, TRACE_VAR, Timings,
    lease_watcher, written_values,
};
pub use forks::{CopyOnDisk, Expected, ForkDisk, ForkExpectations};
pub use history::{DurabilityMode, Guarantee, Replicated};
pub use rng::SimRng;
pub use trace::{DRIVER, Divergence, Event, EventKind, Replay, Trace};
pub use workload::{
    Ack, CreateAndAcquire, CreateAndWrite, Delegations, FencedSeen, ForkMarkerSeen, ForkPass,
    ForkPhaseKind, ForkTransition, History, HistoryEvent, LeaseObservation, LeaseSeen, Operation,
    OperationId, Operator, Outcome, OutcomeClass, Pending, Phase, Record, Script, SeedingProbe,
    Step, Traffic, TrafficHandle, WRITES_TABLE, Workload,
};
