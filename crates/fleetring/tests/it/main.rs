//! Integration tests for `fleetring`, compiled as one binary so they link once and share
//! fixtures.

mod bounded;
mod cells;
mod cells_spec;
mod common;
mod diagnostics;
mod diff;
mod hasher;
mod labels;
mod placement;
mod ring;
mod spec;
mod vectors;
mod vnodes;
