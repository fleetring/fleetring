//! Integration tests for `fleetring-control`, compiled as one binary so they link once and share
//! the command generator.

mod common;
mod delegation;
mod delegation_replay;
mod durability;
mod fork;
mod fork_lifecycle;
mod fork_replay;
mod fork_review;
mod handover;
mod lineage;
mod metadata;
mod properties;
mod replay;
mod revocation;
mod rollback;
mod scenarios;
mod software;
mod tokens_vectors;
mod upgrade;
