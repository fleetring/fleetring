//! Simulation tests: whole clusters of real daemons under turmoil (ADR-0020, FR-024).
//!
//! Each test runs its scenario over the fixed seeds in `fleetringd::sim::seeds::FIXED`. Set
//! `FLEETRING_SIM_SEED` to run one seed; a failing test prints the command that does that.

mod cluster;
mod control;
mod database;
mod drain;
mod durability;
mod failover;
mod history;
mod membership;
mod replication;

use std::error::Error;

/// The name of this test binary, for rerun commands.
const BINARY: &str = "sim";

type TestResult = Result<(), Box<dyn Error>>;
