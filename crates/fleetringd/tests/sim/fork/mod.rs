//! Fork scenarios (ADR-0046, FR-085, FR-087).
//!
//! - [`control_only`]: FR-085's hooks, a fork driven to `ready` by reports fed without data
//!   nodes.
//! - [`matrix`] and the row modules: FR-087's failure matrix, every row of ADR-0046 point 12's
//!   recovery table at each seeding step, on real data nodes, with the fork properties of the
//!   history checker ([`fleetringd::sim::forks`]) run over every scenario. The matrix needs the
//!   `fault-hooks` feature, which holds a data node's fork work at each step.

mod control_only;
#[cfg(feature = "fault-hooks")]
mod matrix;
#[cfg(feature = "fault-hooks")]
mod rows;
#[cfg(feature = "fault-hooks")]
mod rows_late;
