//! Integration tests for `fleetring-control`, compiled as one binary so they link once and share
//! the command generator.

mod common;
mod durability;
mod metadata;
mod properties;
mod replay;
mod rollback;
mod scenarios;
mod software;
