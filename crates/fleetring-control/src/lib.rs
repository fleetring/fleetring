//! The FleetRing control plane's state machine (ADR-0017).
//!
//! Every change to control state is a command, applied in order by one pure function. The crate
//! does no I/O and reads no clock: each command carries the time the control node stamped on it,
//! so replaying a command log anywhere gives the same state. That is what lets the control plane
//! move from one node to Raft without changing its logic.
#![deny(clippy::float_arithmetic)]
