//! FR-039: change streams. Sinks receive each database's acknowledged changes, beside
//! replication and never in its acknowledgement path.

mod common;
mod daemon;
mod file;
mod isolation;
mod logical;
mod webhook;
