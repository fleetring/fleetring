//! What every FleetRing client shares, whatever surface draws it (ADR-0033, ADR-0043 point 4.10).
//!
//! - [`context`] holds the contexts file and its format, lock and selection; [`dirs`] finds the
//!   directories it lives in.
//! - [`redirect`] is the "try X" reply a control node that isn't the leader, or a data node that
//!   isn't a database's primary, answers with, and that every client follows.
//! - [`output`] holds the typed results the read commands print, so the CLI's JSON, the TUI's
//!   rows and the desktop app's views agree on every field's name and meaning, and the escaping
//!   that keeps a name a principal chose from passing for layout.

pub mod context;
pub mod dirs;
pub mod output;
pub mod redirect;

const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<context::Context>();
    assert_debug_send_sync::<context::ContextError>();
    assert_debug_send_sync::<context::ContextName>();
    assert_debug_send_sync::<context::ContextStore>();
    assert_debug_send_sync::<context::Contexts>();
    assert_debug_send_sync::<context::DevContextOutcome>();
    assert_debug_send_sync::<context::SelectedBy>();
    assert_debug_send_sync::<context::WasCurrent>();
    assert_debug_send_sync::<dirs::DirsError>();
    assert_debug_send_sync::<output::DatabaseReport>();
    assert_debug_send_sync::<output::MetadataReport>();
    assert_debug_send_sync::<output::NodeList>();
    assert_debug_send_sync::<output::NodeReport>();
    assert_debug_send_sync::<output::PlacementReport>();
    assert_debug_send_sync::<output::ReplicaReport>();
    assert_debug_send_sync::<output::SchemaObjectReport>();
    assert_debug_send_sync::<output::SchemaReport>();
};
