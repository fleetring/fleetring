//! Simulation tests: whole fleets of real daemons under turmoil (ADR-0020, FR-024).
//!
//! Each test runs its scenario over the fixed seeds in `fleetringd::sim::seeds::FIXED`. Set
//! `FLEETRING_SIM_SEED` to run one seed; a failing test prints the command that does that.

mod archive;
mod binding;
mod changes;
mod control;
mod database;
mod delegation;
mod drain;
mod durability;
mod expiry;
mod failover;
mod fleet;
mod fork;
mod handover;
mod history;
mod membership;
mod membership_lag;
mod observe;
mod replication;
mod router;
mod snapshot_chain;

use std::error::Error;

/// The name of this test binary, for rerun commands.
const BINARY: &str = "sim";

type TestResult = Result<(), Box<dyn Error>>;
