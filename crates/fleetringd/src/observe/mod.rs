//! What an operator collects from a running daemon: metrics, probes, and request spans
//! (ADR-0034).
//!
//! - **The admin listener.** Apart from the gRPC port, the daemon serves HTTP/1.1 on
//!   `[admin] listen` (`127.0.0.1:7401` by default): `/metrics` in the OpenMetrics text format,
//!   `/healthz`, and `/readyz`. A non-loopback address needs `[admin] tls = true`, which serves
//!   the node's own certificate and limits `/metrics` to the client certificates of the principals
//!   `[admin] scrapers` lists, or an explicit `[admin] allow_plaintext = true`. The probes never
//!   need a certificate.
//! - **Names.** Every family is in [`FAMILIES`]: the `fleetring_` prefix, lexicon words, base
//!   units in the name, `_total` on counters.
//! - **Labels.** Label names come from [`ALLOWED_LABELS`], and values from closed sets fixed by
//!   the code or from the node's configuration (a sink's name). No label value is a database,
//!   principal, token, or peer node. Spreads over databases are gauges a sampler refreshes.
//! - **One registry per daemon.** [`Metrics`] holds the registry and a handle to every family;
//!   nothing is global, so two daemons in one process keep separate counts.
//! - **Spans.** [`TraceLayer`] opens an `rpc` span around every request with its
//!   [`RequestId`], counts and times it, and counts refusals. The request ID travels on the
//!   calls a node makes while serving the request.

pub(crate) mod admin;
pub(crate) mod codes;
pub(crate) mod health;
mod logs;
mod metrics;
mod names;
pub(crate) mod request_id;
pub(crate) mod sampler;
mod trace;

pub use logs::{LogColour, log_subscriber};
pub(crate) use metrics::DataMetrics;
pub use metrics::{
    ChangeMetrics, ControlMetrics, MembershipMetrics, Metrics, NodeInfo, ReplicationMetrics,
};
pub use names::{ALLOWED_LABELS, FAMILIES, FamilySpec, INFO_LABELS, Kind, closed_values};
pub use request_id::{REQUEST_ID_KEY, RequestId};
pub use trace::{TraceLayer, TraceService};
