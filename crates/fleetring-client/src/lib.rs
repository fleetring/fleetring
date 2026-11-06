//! What a FleetRing client command needs before it dials a node (ADR-0043, point 4.10).
//!
//! [`context`] holds the contexts file and its format, lock and selection; [`dirs`] finds the
//! directories it lives in. FR-056 grows this crate with the clients and the typed results.

pub mod context;
pub mod dirs;

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
};
