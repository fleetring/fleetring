//! Integration tests for `fleetring-control`, compiled as one binary so they link once and share
//! the command generator.

mod common;
mod durability;
mod fork;
mod fork_lifecycle;
mod fork_replay;
mod metadata;
mod properties;
mod replay;
mod rollback;
mod scenarios;
mod software;
