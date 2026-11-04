---
id: FR-063
title: Metrics, health and readiness endpoints, structured logs, and a span per request
status: todo
priority: P1
size: L
milestone: M7 Durability and operations
depends_on: [FR-046, FR-047]
adrs: [ADR-0020, ADR-0025, ADR-0026, ADR-0034]
---

# FR-063: Metrics, health and readiness endpoints, structured logs, and a span per request

## Context

The roadmap's "Operable" criterion asks for metrics, health checks, and logs an operator can collect. Today a running `fleetringd` offers `tracing` text on stderr and the `status` commands, which answer one question at a time: nothing can scrape it, a load balancer or orchestrator can't ask whether a node is ready, and nothing ties a request's log lines together. ADR-0034 decides the endpoints, who may read them, the metric naming scheme, the cardinality limits (a node can hold 10,000 databases, so per-database labels are out), and the log format.

## Scope

- **An admin listener**, separate from the gRPC port (`[admin] listen`, `127.0.0.1:7401` by default): `/metrics` in the Prometheus text format, `/healthz`, and `/readyz`. Plaintext on loopback by default; any other address needs `[admin] tls = true` (the node's own certificate, with `/metrics` limited to the client certificates of the principals listed in `[admin] scrapers`) or an explicit `[admin] allow_plaintext = true`.
- **Metrics** from one registry per daemon (`prometheus-client`, no global state), named by ADR-0034's scheme: the `fleetring_` prefix, lexicon words, base units in the name (`_seconds`, `_bytes`), `_total` on counters. Families for RPCs, refusals, membership, writes and queries, replication, leases and promotions, the control plane, and change streams.
- **Cardinality limits:** label values only from closed sets or configuration (RPC, status code, replica role, sink name, outcome). No database, principal, token, or peer node labels; distributions over databases are histograms.
- **Readiness** per node role: joined with a ring and a fresh denylist on a data node, a recovered log and a known leader on a control node; `503` while draining.
- **Logs:** `[log] format = "text" | "json"`, `--log-format`, and `FLEETRING_LOG_FORMAT`; an `rpc` span around every request with a request ID, taken from `x-request-id` or generated, and sent on the calls a node makes for that request.
- **Not in scope:** OpenTelemetry export, opt-in per-database metrics, dashboards, and the router's own metrics (FR-066 adds them to this registry).

## Acceptance criteria

- [ ] ADR-0034 is accepted and listed in `docs/adr/README.md`.
- [ ] A daemon started with the default configuration serves every metric family in ADR-0034's table at `http://127.0.0.1:7401/metrics`, and the daemon README lists them with their labels.
- [ ] `/healthz` answers `200` while the process runs. `/readyz` answers `200` only when every check of the node's roles passes, and otherwise `503` with a JSON body naming each failing check; a draining node answers `503` with the `draining` check failing.
- [ ] A configuration whose `[admin] listen` isn't a loopback address, without `tls = true` or `allow_plaintext = true`, fails validation with a message naming both fields.
- [ ] With `tls = true`, `/metrics` answers `401` without a client certificate and `403` for a principal not in `scrapers`, and serves a listed principal; `/healthz` and `/readyz` answer without a certificate.
- [ ] A test that hosts 200 databases on one node finds no label value equal to a database name and fewer than 1,500 series in the exposition.
- [ ] `--log-format json` writes one JSON object per line; every log line a request causes carries the `rpc` span with its `request_id`, an incoming `x-request-id` is kept, and the control call a data node makes while serving the request carries the same ID.
- [ ] Two daemons in one process (the integration tests and the simulation harness) keep separate metrics, and the simulation suite passes unchanged.
- [ ] `deploy/fleetringd.toml` documents `[admin]` and `[log]`; the daemon README has an "Observability" section; the lexicon and `docs/roadmap.md` are updated.

Two notes on reading the criteria against ADR-0034: "ADR-0034's table" is the family table in this plan (step 1, `observe/names.rs`), which the ADR points to; and "distributions over databases are histograms" is implemented as sampled gauges (counts, maxima, sums over the node's databases), which ADR-0034 point 7 decides, because a histogram over databases would have to be rebuilt from scratch at every sample anyway.

## Implementation plan

Each step is about one commit (`feat(fleetringd): …` unless it says otherwise, with a `Refs: FR-063` trailer) and leaves the workspace building with every test passing. Every path is under `crates/fleetringd/` unless it says otherwise. New public types derive `Debug`, are `Send + Sync`, and are added to the `assert_debug_send_sync` list in `src/lib.rs`; error enums are `#[non_exhaustive]` with lower-case messages; no `unwrap()` outside tests; no `bool` parameters; no `HashMap` or `HashSet`; time only through `tokio::time` or `clock::Clock` (`clippy.toml` bans `std::time::Instant::now`, so durations use `tokio::time::Instant::now()`).

### Step 1: dependencies, the `observe` module, and the family table

Commit: `feat(fleetringd): add a per-daemon metrics registry and the metric family table`.

- `Cargo.toml` (the crate's): add `prometheus-client = "0.23"` (or the latest 0.x at implementation time; Apache-2.0 OR MIT, which `deny.toml` allows); `http-body = "1"`; change `hyper` to `features = ["client", "http1", "server"]`; change `tracing-subscriber` to `features = ["env-filter", "fmt", "json"]`. Run `cargo deny check` and record any new licence in `deny.toml` (none is expected).
- `src/lib.rs`: `pub mod observe;`.
- `src/observe/mod.rs`: module docs summarizing ADR-0034 (admin listener, names, labels, spans); `mod names; mod metrics; mod codes;` and re-exports:

  ```rust
  pub use metrics::{Metrics, NodeInfo};
  pub use names::{ALLOWED_LABELS, FAMILIES, FamilySpec, Kind};
  ```

- `src/observe/names.rs`: the single table every registration reads and every test checks.

  ```rust
  /// What kind of metric a family is.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  #[non_exhaustive]
  pub enum Kind { Counter, Gauge, Histogram }

  /// One metric family: its registered name, kind, label names, and help text.
  ///
  /// Counters are registered without `_total`; the exposition adds it (OpenMetrics).
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct FamilySpec {
      pub name: &'static str,
      pub kind: Kind,
      pub labels: &'static [&'static str],
      pub help: &'static str,
  }

  /// Every label name a family may use (ADR-0034 point 7).
  pub const ALLOWED_LABELS: &[&str] = &[
      "service", "method", "code", "replica_role", "liveness", "outcome", "reason", "action",
      "state", "sink", "version", "protocol", "node_id", "node_role", "fleet",
  ];

  /// The labels only the two info families may use.
  pub const INFO_LABELS: &[&str] = &["version", "protocol", "node_id", "node_role", "fleet"];

  /// Every family the daemon registers, in exposition order.
  pub const FAMILIES: &[FamilySpec] = &[ /* the table below, in order */ ];
  ```

  The table (exposed name in the first column; for counters `name` in `FAMILIES` is the same without `_total`). "Sampled" means the sampler task of step 3 sets it every `sample_interval_ms`.

  | Exposed name | Kind | Labels | Help | Updated in |
  |---|---|---|---|---|
  | `fleetring_build_info` | gauge | `version`, `protocol` | The daemon's version and protocol version; always 1. | `Metrics::new` |
  | `fleetring_node_info` | gauge | `node_id`, `node_role`, `fleet` | This node's ID, node role, and fleet name; always 1. | `Metrics::new` |
  | `fleetring_rpc_requests_total` | counter | `service`, `method`, `code` | RPCs this node served, by final status code. | `observe/trace.rs`, when a response ends |
  | `fleetring_rpc_duration_seconds` | histogram | `service`, `method` | Time to serve unary RPCs, from request to final status. | `observe/trace.rs`, unary RPCs only |
  | `fleetring_rpc_in_flight` | gauge | `service`, `method` | RPCs being served now, streams included. | `observe/trace.rs` |
  | `fleetring_auth_refusals_total` | counter | `reason` | Requests refused at the request boundary or by a handler's scope or binding check. | `observe/trace.rs`, from the response |
  | `fleetring_denylist_age_seconds` | gauge | none | Seconds since the token denylist last arrived; +Inf before the first. | sampled (data role) |
  | `fleetring_denylist_entries` | gauge | none | Revoked tokens and signing keys on the denylist. | sampled |
  | `fleetring_membership_epoch` | gauge | none | The membership epoch this node last published. | sampled (data role) |
  | `fleetring_heartbeats_total` | counter | `outcome` | Heartbeats sent to the control plane, by outcome. | `membership.rs`, `heartbeat_loop` |
  | `fleetring_heartbeat_duration_seconds` | histogram | none | Time from sending a heartbeat to its reply. | `membership.rs`, `heartbeat_loop` |
  | `fleetring_members` | gauge | `liveness` | Members of the fleet, by liveness, as control state has them. | sampled (control role) |
  | `fleetring_members_draining` | gauge | none | Members marked as draining. | sampled (control role) |
  | `fleetring_databases` | gauge | `replica_role` | Databases this node hosts, by replica role. | sampled (data role) |
  | `fleetring_databases_open` | gauge | none | Databases this node has open. | sampled (data role) |
  | `fleetring_databases_under_replicated` | gauge | none | Databases this node is primary for with fewer replicas reachable than the replication factor. | sampled (data role) |
  | `fleetring_leases_held` | gauge | none | Database leases this node holds. | sampled (data role) |
  | `fleetring_writes_total` | counter | `outcome` | `Execute` calls, by outcome. | `data/service.rs`, `execute` |
  | `fleetring_write_duration_seconds` | histogram | none | Time from receiving an acknowledged write to replying, write quorum included. | `data/service.rs`, `execute` |
  | `fleetring_queries_total` | counter | `outcome` | `Query` calls, by outcome. | `data/service.rs`, `query` |
  | `fleetring_query_duration_seconds` | histogram | none | Time to answer a query that succeeded. | `data/service.rs`, `query` |
  | `fleetring_replication_batches_sent_total` | counter | none | Batches this node sent to followers. | `replication/primary.rs`, `stream` |
  | `fleetring_replication_sent_bytes_total` | counter | none | Batch payload bytes this node sent to followers. | `replication/primary.rs`, `stream` |
  | `fleetring_replication_batches_applied_total` | counter | none | Batches this node applied as a follower. | `replication/follower.rs` |
  | `fleetring_replication_snapshots_sent_total` | counter | none | Database snapshots this node served to followers. | `replication/snapshot.rs` |
  | `fleetring_replication_reseeds_total` | counter | none | Followers this node told to re-seed from a database snapshot. | `replication/primary.rs`, `reseed` |
  | `fleetring_replication_followers` | gauge | `state` | Followers of the databases this node is primary for, by state. | sampled (data role) |
  | `fleetring_lease_renewals_total` | counter | `outcome` | Lease renewals this node asked for, by outcome. | `data/lease.rs`, `renew_all` |
  | `fleetring_lease_transfers_total` | counter | `outcome` | Leases this node handed over while draining, by outcome. | `data/drain.rs`, `transfer` |
  | `fleetring_promotions_total` | counter | `outcome` | Promotions this node carried out as candidate, by outcome. | `data/lease.rs`, `promote` |
  | `fleetring_promotion_duration_seconds` | histogram | none | Time from a promotion notice to serving as primary. | `data/lease.rs`, `promote` |
  | `fleetring_control_commands_total` | counter | `action`, `outcome` | Control commands logged, by action and whether `apply` accepted them. | `control/node.rs`, `apply_logged` |
  | `fleetring_control_log_index` | gauge | none | The index of the last logged control command. | `control/node.rs`, `apply_logged` |
  | `fleetring_control_log_write_duration_seconds` | histogram | none | Time to log one group of control commands, its sync included. | `control/node.rs`, `commit_group` |
  | `fleetring_control_mailbox_depth` | gauge | none | Control commands waiting for the writer. | sampled (control role) |
  | `fleetring_control_snapshot_index` | gauge | none | The log index of the last control snapshot saved; 0 before the first. | `control/node.rs`, `apply_logged` |
  | `fleetring_control_promotions_in_progress` | gauge | none | Databases with a promotion in progress. | sampled (control role) |
  | `fleetring_control_databases` | gauge | none | Databases control state records. | sampled (control role) |
  | `fleetring_control_leader` | gauge | none | 1 if this node is the control plane's leader. | sampled (control role) |
  | `fleetring_change_deliveries_total` | counter | `sink`, `outcome` | Change stream deliveries, by sink and outcome. | `changes/pump.rs` |
  | `fleetring_change_resets_total` | counter | `sink` | Times a sink's stream skipped ahead because the log moved past it. | `changes/pump.rs` |
  | `fleetring_change_backlog_batches` | gauge | `sink` | Batches published but not yet delivered, summed over databases. | sampled (data role) |
  | `fleetring_change_backlog_batches_max` | gauge | `sink` | The largest backlog of any one database. | sampled (data role) |

  Closed label values, as `&'static str` constants in `names.rs` (`pub const OUTCOMES_WRITE: &[&str]`, and so on), each used by its family and by the tests:

  - `service`: `control`, `replication`, `database`, `unknown` (a path `auth::RPCS` doesn't list).
  - `method`: an `Rpc::method()` from `auth::RPCS`, or `unknown`.
  - `code`: a gRPC code in lower snake case (`observe/codes.rs`): `ok`, `cancelled`, `unknown`, `invalid_argument`, `deadline_exceeded`, `not_found`, `already_exists`, `permission_denied`, `resource_exhausted`, `failed_precondition`, `aborted`, `out_of_range`, `unimplemented`, `internal`, `unavailable`, `data_loss`, `unauthenticated`.
  - `reason` (`fleetring_auth_refusals_total`): `scope`, `binding`, `unauthenticated`, `denylist_stale`, `no_requirement`.
  - `outcome`: heartbeats `recorded`, `failed`; writes `acknowledged`, `refused`, `aborted`, `redirected`, `failed`; queries `ok`, `redirected`, `failed`; renewals `renewed`, `refused`, `failed`; transfers `completed`, `failed`; promotions `completed`, `superseded`, `failed`; control commands `applied`, `rejected`; change deliveries `delivered`, `failed`.
  - `action`: every `fleetring_control::Action` variant in snake case, as its serde name: `join`, `leave`, `heartbeat`, `create_database`, `change_durability`, `drop_database`, `acquire_lease`, `renew_lease`, `start_promotion`, `record_fenced`, `complete_promotion`, `tick`, `drain`, `transfer_lease`, `revoke_token`; `other` for a variant added later before this list is extended.
  - `state` (`fleetring_replication_followers`): `caught_up`, `behind`, `disconnected`, `reseeding`.
  - `replica_role`: `primary`, `follower`. `liveness`: `live`, `down`.
  - `sink`: a `[[changes.sink]]` entry's `name`.
  - `node_role`: `data`, `control`, `both`.

- `src/observe/codes.rs`: `pub(crate) fn code_label(code: tonic::Code) -> &'static str` (the list above) and `pub(crate) fn action_label(action: &fleetring_control::Action) -> &'static str` (an exhaustive `match` with a `_ => "other"` arm, needed because `Action` is `#[non_exhaustive]`).
- `src/observe/metrics.rs`:

  ```rust
  /// What the two info families say about this node.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct NodeInfo {
      pub node_id: NodeId,
      pub node_role: NodeRole,
      /// The fleet name from the node's certificate; `None` in plaintext.
      pub fleet: Option<FleetName>,
  }

  /// One daemon's metrics: the registry and a handle to every family (ADR-0034).
  ///
  /// Cloning is cheap and shares the counts. Nothing is global: two daemons in one process
  /// have two `Metrics`.
  #[derive(Debug, Clone)]
  pub struct Metrics { registry: Arc<Registry>, /* one field per group below */ }

  impl Metrics {
      /// Registers every family in [`FAMILIES`] and sets the info families.
      #[must_use]
      pub fn new(info: &NodeInfo) -> Self;
      /// The exposition, in OpenMetrics text.
      #[must_use]
      pub fn encode(&self) -> String;
  }
  ```

  Handles are grouped into `pub(crate)` structs, each `Clone` and `Default` (a `Default` handle is a working metric that isn't registered anywhere, for tests and for components built without a daemon): `RpcMetrics`, `AuthMetrics`, `MembershipMetrics`, `DataMetrics` (writes, queries, renewals, transfers, promotions), `ReplicationMetrics`, `ControlMetrics`, `ChangeMetrics`, `SampledMetrics` (every "sampled" gauge), each exposed as `pub(crate) fn rpc(&self) -> &RpcMetrics` and so on. Labelled families are `Family<L, Counter>` (or `Gauge`, `Histogram`) with label structs deriving `prometheus_client::encoding::EncodeLabelSet` whose fields are `&'static str`, so a label value can't be a runtime string except `sink`, a `String` taken from configuration, and the `node_id` and `fleet` of `fleetring_node_info`, which is one series set once in `Metrics::new`. `encode` calls `prometheus_client::encoding::text::encode` into a `String`; an encoding error (only possible from `fmt::Write`) yields the text encoded so far. Histogram buckets: `exponential_buckets(0.0005, 2.0, 16)` (0.5 ms to about 16 s) for every `_seconds` family.

  `Metrics::new` registers each family with the name, help, and labels from `FAMILIES`; a `debug_assert!` checks the registration order matches the table.
- Tests in `src/observe/names.rs`:
  - `every_family_uses_only_allowed_labels`: each `FAMILIES` entry's labels are in `ALLOWED_LABELS`, and only `fleetring_build_info` and `fleetring_node_info` use `INFO_LABELS`.
  - `names_follow_the_scheme`: every name starts with `fleetring_`, is lower snake case, uses no word from the lexicon's "Words we don't use" table (`cluster`, `master`, `slave`, `leader` outside `fleetring_control_leader`, `shard`, `up`, `alive`, `dead`), counters don't end in `_total` (the encoder adds it), histograms end in `_seconds` or `_bytes`, and no name ends in `_ms` or `_milliseconds`.
  - `names_are_unique`: no two entries share a name.
- Tests in `src/observe/metrics.rs`:
  - `the_exposition_lists_every_family`: `Metrics::new(&info).encode()` has a `# TYPE <exposed name>` line for every family, `fleetring_build_info{version="<CARGO_PKG_VERSION>",protocol="1.0"} 1`, and ends with `# EOF`.
  - `two_registries_keep_separate_counts`: incrementing `fleetring_writes_total{outcome="acknowledged"}` on one `Metrics` leaves the other's at 0.
  - `codes_are_lower_snake_case`: `code_label` for every `tonic::Code` is in the closed list, and `action_label` covers every `Action` example in `fleetring_control`'s docs.

### Step 2: the admin listener, its configuration, and the three endpoints

Commit: `feat(fleetringd): serve /metrics, /healthz, and /readyz on an admin listener`.

- `src/config.rs`:

  ```rust
  /// The `[admin]` table: the HTTP listener for metrics and probes (ADR-0034).
  ///
  /// ```toml
  /// [admin]
  /// enabled = true                 # false: no admin listener
  /// listen = "127.0.0.1:7401"
  /// tls = false                    # true: HTTPS with the node's [tls] certificate
  /// scrapers = ["prometheus"]      # principals whose client certificates may read /metrics
  /// allow_plaintext = false        # true: plaintext on a non-loopback address
  /// sample_interval_ms = 10000     # how often sampled gauges are refreshed
  /// ```
  #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
  #[serde(default, deny_unknown_fields)]
  pub struct AdminConfig {
      pub enabled: AdminListener,
      pub listen: SocketAddr,
      pub tls: AdminTls,
      pub scrapers: Vec<Principal>,
      pub allow_plaintext: AdminPlaintext,
      pub sample_interval_ms: NonZeroU64,
  }
  ```

  with three enums in the style of `TokenForwarding` (`#[serde(from = "bool")]`, `#[non_exhaustive]`, `From<bool>`, `Default`): `AdminListener { #[default] Enabled, Disabled }`, `AdminTls { #[default] Off, NodeIdentity }`, `AdminPlaintext { #[default] Refused, Allowed }`. Defaults: enabled, `127.0.0.1:7401`, off, `[]`, refused, `10000`. `Config` gains `#[serde(default)] pub admin: AdminConfig`. Re-export `AdminConfig`, `AdminListener`, `AdminTls`, `AdminPlaintext` from `src/lib.rs` and add them to the assertion list.
- `Config::validate` gains, after the existing checks:
  - if `admin.enabled == Enabled`, `!admin.listen.ip().is_loopback()`, `admin.tls == Off`, and `admin.allow_plaintext == Refused`: `ConfigError::Invalid { field: "admin.listen", reason: format!("{listen} isn't a loopback address; set admin.tls = true, or admin.allow_plaintext = true on a network you control") }`;
  - duplicate names in `admin.scrapers`: `ConfigError::Invalid { field: "admin.scrapers", reason: "'<name>' is listed more than once" }`.
- `src/daemon.rs`:
  - `Daemon` gains `metrics: Metrics` and `admin: Option<RunningAdmin>`; `pub fn metrics(&self) -> &Metrics` and `pub fn admin_address(&self) -> Option<SocketAddr>`.
  - In `start_with`, after `security(...)`: `let metrics = Metrics::new(&NodeInfo { node_id, node_role: config.role, fleet })` where `fleet` comes from `Security::Tls(identity) => Some(identity.fleet())` (make `Identity::fleet` `pub(crate)` if it isn't reachable). After `serve(...)`: start the admin listener with `observe::admin::start(...)` when `config.admin.enabled == Enabled` and `network.is_tcp()`; a bind failure is `StartError::Listen { address: config.admin.listen, source }`. If `admin.tls == NodeIdentity` and the security is plaintext: `StartError::Config(ConfigError::Missing { field: "tls", reason: "admin.tls = true serves the node's certificate; add a [tls] table" })`.
  - In `run_until`, stop the admin listener right after `self.server.stop.send(())`, before the control writer stops, so `/healthz` never reports a stopping writer as failed.
  - The startup `tracing::info!` gains `admin = ?admin_address`.
- `src/observe/admin.rs`:

  ```rust
  /// The running admin listener; dropping it stops it.
  #[derive(Debug)]
  pub(crate) struct RunningAdmin { address: SocketAddr, stop: oneshot::Sender<()>, task: JoinHandle<()> }

  pub(crate) struct AdminParts {
      pub(crate) listener: tokio::net::TcpListener,
      pub(crate) metrics: Metrics,
  }
  // Step 3 adds `pub(crate) probes: Probes`, and step 4 `pub(crate) tls: Option<AdminTlsParts>`.

  pub(crate) fn start(parts: AdminParts) -> io::Result<RunningAdmin>;
  impl RunningAdmin { pub(crate) fn address(&self) -> SocketAddr; pub(crate) async fn stop(self); }
  ```

  The accept loop spawns one task per connection, serving it with `hyper::server::conn::http1::Builder::new().serve_connection(hyper_util::rt::TokioIo::new(stream), service_fn(...))`. Routing, in `fn route(method: &Method, path: &str) -> Route`:
  - `GET` or `HEAD` `/metrics`: `200`, `content-type: application/openmetrics-text; version=1.0.0; charset=utf-8`, body `metrics.encode()` (empty for `HEAD`).
  - `GET` or `HEAD` `/healthz`: `200` `ok\n`, or `503` `control writer stopped\n` (step 3); `content-type: text/plain; charset=utf-8`.
  - `GET` or `HEAD` `/readyz`: step 3; this step answers `200` with `{"ready":true,"checks":[]}`.
  - Any other method on those paths: `405` with `allow: GET, HEAD`. Any other path: `404`.
  - Query strings are ignored. Request bodies are never read. Each connection is closed after 30 s idle (`http1::Builder::timer` with `hyper_util::rt::TokioTimer` and `header_read_timeout(Duration::from_secs(30))`).
- Tests:
  - `src/config.rs`: `the_admin_listener_defaults_to_loopback_plaintext` (an empty `[admin]` gives the defaults above); `a_non_loopback_admin_address_needs_tls_or_plaintext` (`listen = "0.0.0.0:7401"` alone fails, and the message contains `admin.tls` and `admin.allow_plaintext`; with either field set it passes; with `enabled = false` it passes); `duplicate_scrapers_are_refused`.
  - `tests/observe.rs` (new integration test file, `mod support;` like the others): `metrics_health_and_readiness_on_the_admin_listener`: a `both` daemon with a test authority (`support::Tls`) and `[admin] listen = "127.0.0.1:0"`; with a minimal `hyper` HTTP/1 client to `Daemon::admin_address()`: `/healthz` is `200` `ok`; `/metrics` is `200` with the OpenMetrics content type and has a `# TYPE` line for every `FAMILIES` entry; `POST /metrics` is `405`; `/nope` is `404`; `HEAD /metrics` is `200` with an empty body.
  - `tests/deploy.rs`: the example config still loads, with the admin defaults.

### Step 3: health, readiness, and the sampler

Commit: `feat(fleetringd): report readiness per node role and sample gauges over databases`.

- `src/control/node.rs`: `ControlHandle::is_running(&self) -> bool` (`!self.mailbox.is_closed()`) and `ControlHandle::queued(&self) -> usize` (`self.mailbox.max_capacity() - self.mailbox.capacity()`).
- `src/observe/health.rs`:

  ```rust
  /// The state each check reads. Every field is optional: a node runs one role or both.
  #[derive(Debug, Clone)]
  pub(crate) struct Probes {
      pub(crate) node: NodeId,
      pub(crate) membership: Option<Membership>,
      pub(crate) denylist_max_age: Duration,     // daemon.rs's denylist_max_age(config)
      pub(crate) control: Option<ControlHandle>,
  }

  /// One readiness check's result.
  #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
  pub(crate) struct Check { pub(crate) name: &'static str, pub(crate) ok: bool, pub(crate) detail: String }

  #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
  pub(crate) struct Readiness { pub(crate) ready: bool, pub(crate) checks: Vec<Check> }

  /// What `/healthz` reports.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub(crate) enum Health { Serving, ControlWriterStopped }

  impl Probes {
      pub(crate) fn health(&self) -> Health;
      pub(crate) fn readiness(&self) -> Readiness;
  }
  ```

  `health` returns `ControlWriterStopped` when `control` is `Some` and `!is_running()`, else `Serving`. Checks, in this order, data role first (when `membership` is `Some`), then control role (when `control` is `Some`):

  | Name | Passes when | `detail` when it fails |
  |---|---|---|
  | `joined` | `membership.epoch().get() > 0` | `not joined to the fleet yet` |
  | `ring` | `membership.ring().is_some()` | `no ring published yet` |
  | `denylist` | `membership.denylist().age()` is `Some(age)` with `age <= denylist_max_age` | `no denylist yet` or `denylist is <n> ms old; tokens are refused` |
  | `draining` | `!membership.view().is_draining(&node)` | `the node is draining` |
  | `control_log` | `control.is_running()` | `the control writer stopped; restart the node` |
  | `leader` | always, until FR-064 | (never fails) |

  A passing check's `detail` is the empty string. `ready` is the conjunction of every `ok`.
- `src/observe/admin.rs`: `AdminParts` gains `pub(crate) probes: Probes`; `/healthz` answers `200` `ok` for `Health::Serving` and `503` `control writer stopped` for `Health::ControlWriterStopped`; `/readyz` answers `200` or `503` with `serde_json::to_string(&readiness)` and `content-type: application/json`.
- `src/observe/sampler.rs`:

  ```rust
  pub(crate) struct Sources {
      pub(crate) membership: Option<Membership>,
      pub(crate) replication: Option<Replication>,
      pub(crate) changes: Option<ChangeStreams>,
      pub(crate) control: Option<ControlHandle>,
  }
  /// Refreshes every sampled gauge at once, then every `interval`. Dropping it stops it.
  #[derive(Debug)]
  pub(crate) struct Sampler { task: JoinHandle<()> }
  impl Sampler { pub(crate) fn start(sources: Sources, metrics: SampledMetrics, interval: Duration) -> Self; }
  ```

  Each tick (a `tokio::time::interval` with `MissedTickBehavior::Delay`), with no network call:
  - data role: `fleetring_membership_epoch` from `membership.epoch()`; `fleetring_denylist_age_seconds` from `denylist().age()` (`f64::INFINITY` when `None`) and `fleetring_denylist_entries` from `denylist().load()`'s length; `fleetring_databases{replica_role}` by counting `DatabaseStatuses::statuses(&replication)` by role; `fleetring_databases_open` from `replication.registry().open_count()`; `fleetring_leases_held` from the length of `replication.served().borrow()`; `fleetring_databases_under_replicated` by counting the served databases whose `replication.holders(db)` `is_under_replicated()`; `fleetring_replication_followers{state}` by classifying every follower in `replication.primary_status(db)` of each served database: `reseeding` if it is being re-seeded, else `disconnected` if `!connected`, else `caught_up` if `reported && acknowledged == committed`, else `behind`; `fleetring_change_backlog_batches{sink}` and `..._max{sink}` from `changes.status()` (sum and maximum of `backlog` per sink).
  - control role: from `control.state()` at `state.now()`: `fleetring_members{liveness}` with `State::liveness`, `fleetring_members_draining`, `fleetring_control_databases`, `fleetring_control_promotions_in_progress` (databases whose `promotion()` is `Some`), `fleetring_control_mailbox_depth` from `queued()`, `fleetring_control_leader` set to 1.
  - Every labelled gauge sets every value of its closed set each tick, zeros included, so a state that empties reads 0 rather than vanishing.
- `src/daemon.rs`: build `Probes` and `Sources` from the roles; start the `Sampler` (stored in `Daemon`, dropped in `run_until` before the roles stop) in every environment, simulated included; pass `Probes` into `AdminParts`.
- Tests in `tests/observe.rs`:
  - `readiness_lists_every_check_of_both_roles`: the `both` daemon of step 2, once its ring is published, answers `/readyz` `200` with checks named `joined`, `ring`, `denylist`, `draining`, `control_log`, `leader` in that order, all `ok`.
  - `a_node_is_not_ready_until_it_joins`: a `data` daemon whose `[control] address` is a closed local port answers `/healthz` `200` and `/readyz` `503` with `joined` and `ring` failing and the body's `ready` false.
  - `a_draining_node_is_not_ready`: a control node and two data nodes over TCP; `Client::drain_node` on the second; within 5 s its `/readyz` is `503` with only `draining` failing, while the first stays `200`.
  - `sampled_gauges_count_the_databases`: with `[admin] sample_interval_ms = 100`, after creating three databases (replication factor 1) the exposition holds `fleetring_databases{replica_role="primary"} 3`, `fleetring_leases_held 3`, and `fleetring_control_databases 3`.

### Step 4: TLS on the admin listener, and scrapers

Commit: `feat(fleetringd): serve the admin listener over TLS and limit /metrics to scrapers`.

- `src/tls/identity.rs`: build a second server configuration beside `server` in `from_pem`: the same roots and certificate, `WebPkiClientVerifier::builder_with_provider(Arc::clone(&roots), provider()).allow_unauthenticated().build()?`, TLS 1.3 only, `alpn_protocols = vec![b"http/1.1".to_vec()]`. Store it as `admin_server: Arc<ServerConfig>` with `pub(crate) fn admin_server_config(&self) -> Arc<ServerConfig>`. It reloads with the identity, since `Identity::current()` rebuilds both.
- `src/tls/conn.rs`: factor the end-entity check of `Security::accept` (subject URI, fleet match) into `pub(crate) fn peer_of(certificates: &[CertificateDer<'_>], fleet: &FleetName) -> Result<Peer, HandshakeError>`, used by both paths; `Security::accept` behaves exactly as before.
- `src/observe/admin.rs`:

  ```rust
  pub(crate) struct AdminTlsParts { pub(crate) identity: Identity, pub(crate) scrapers: Vec<Principal> }
  ```

  With `tls` set, each accepted connection does a `tokio_rustls::TlsAcceptor::from(identity.current().admin_server_config())` handshake under the same `HANDSHAKE_TIMEOUT` as `Security::accept`; a failed handshake drops the connection with a debug line. The connection's peer is `None` when the client sent no certificate, else `peer_of(...)` (a certificate that chains but names nothing of this fleet ends the connection, as on the gRPC port). The router passes the peer to `/metrics` only:
  - no certificate: `401` `a client certificate is required\n`;
  - `Peer::Principal(name)` with `name` in `scrapers`: `200` with the exposition;
  - anything else (another principal, any node): `403` `'<principal or node:<id>>' may not read metrics\n`, logged once on `fleetringd::audit` at warn with `principal`, `rpc = "admin /metrics"`, and `"denied"`.
  - `/healthz` and `/readyz` ignore the peer.
  Without `tls`, every endpoint answers without a certificate.
- `src/daemon.rs`: pass `AdminTlsParts { identity, scrapers: config.admin.scrapers.clone() }` when `config.admin.tls == NodeIdentity`.
- Tests in `tests/observe.rs`: `admin_tls_requires_a_listed_scraper`: a daemon with `[admin] tls = true`, `scrapers = ["prometheus"]`, `listen = "127.0.0.1:0"`; a test HTTPS client (rustls `ClientConfig` trusting `ca.crt`, with or without a client certificate from `support::Tls::client_identity`, ALPN `http/1.1`, over `hyper::client::conn::http1`):
  - no certificate: `/metrics` `401`, `/healthz` `200`, `/readyz` `200`;
  - `client.ops` certificate: `/metrics` `403`;
  - the node's own certificate: `/metrics` `403`;
  - `client.prometheus` certificate: `/metrics` `200` with the exposition;
  - a plaintext HTTP request to the TLS port gets no HTTP answer (the handshake fails).

### Step 5: the trace layer, RPC metrics, refusals, and request IDs

Commit: `feat(fleetringd): open a span per request, count RPCs, and carry request IDs`.

- `src/observe/request_id.rs`:

  ```rust
  /// A request's ID: kept from `x-request-id`, or generated.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct RequestId(Arc<str>);

  /// The metadata key a request ID travels in.
  pub const REQUEST_ID_KEY: &str = "x-request-id";

  /// Makes request IDs for one daemon: `<node-id>-<16 hex digits>` from a counter.
  #[derive(Debug)]
  pub(crate) struct RequestIds { node: NodeId, next: AtomicU64 }

  impl RequestIds {
      pub(crate) fn new(node: NodeId) -> Self;
      /// `incoming` if it is 1 to 64 characters of `[A-Za-z0-9._-]`, else a new ID.
      pub(crate) fn resolve(&self, incoming: Option<&str>) -> RequestId;
  }

  tokio::task_local! {
      /// The ID of the request the current task serves, if any.
      pub(crate) static CURRENT: RequestId;
  }

  /// The current task's request ID, if it serves a request.
  pub(crate) fn current() -> Option<RequestId>;   // CURRENT.try_with(Clone::clone).ok()
  ```

  The counter starts at 1 and is formatted `{:016x}`; it is a plain counter, so a seed replays the same IDs (ADR-0020).
- `src/transport.rs`: `impl Interceptor for Credentials` also inserts `x-request-id` from `request_id::current()` when the request doesn't already carry one, so every control and database call made from a request's task carries the request's ID. Replication clients (`replication_client`) don't carry it: their calls come from stream and promotion tasks, not from a request's task.
- `src/observe/trace.rs`:

  ```rust
  /// The outermost layer on every service: an `rpc` span, RPC metrics, refusal counts, and the
  /// request ID (ADR-0034 point 8).
  #[derive(Debug, Clone)]
  pub struct TraceLayer { ids: Arc<RequestIds>, metrics: RpcMetrics, auth: AuthMetrics }

  impl TraceLayer { pub(crate) fn new(node: NodeId, metrics: &Metrics) -> Self; }
  impl<S> Layer<S> for TraceLayer { type Service = TraceService<S>; /* ... */ }

  #[derive(Debug, Clone)]
  pub struct TraceService<S> { inner: S, /* the layer's fields */ }
  ```

  `TraceService::call(request: http::Request<B>)`:
  1. `let rpc = auth::rpc_at(path)`; `service` is `control`, `replication`, or `database` from `Rpc::service()` (`fleetring.v1.Control` → `control`), else `unknown`; `method` is `rpc.method()` or `unknown`.
  2. `let id = ids.resolve(header x-request-id)`; insert the resolved value back into the request's headers, so handlers and audit lines see it.
  3. `let span = tracing::info_span!("rpc", service, method, request_id = %id, principal = Empty, node = Empty, database = Empty, code = Empty, duration_ms = Empty);`
  4. Increment `fleetring_rpc_in_flight{service, method}`; `let started = tokio::time::Instant::now();`
  5. `let future = span.in_scope(|| inner.call(request));` then return `CURRENT.scope(id, future.instrument(span.clone()))` mapped over the response: wrap the body in `ObservedBody`.
  - `ObservedBody` implements `http_body::Body<Data = Bytes, Error = tonic::Status>` around `tonic::body::Body` and is put back with `tonic::body::Body::new(observed)`. The final code is: `grpc-status` in the response headers (a trailers-only response, which is how a handler's error and every boundary refusal arrive), else `grpc-status` in the trailers frame, else, if the body is dropped before either, `cancelled`. When it is known (or on drop): record `code` and `duration_ms` in the span; increment `fleetring_rpc_requests_total{service, method, code}`; observe `fleetring_rpc_duration_seconds` unless the method streams; decrement the in-flight gauge (exactly once, guarded by an `Option` taken on first use); emit `tracing::debug!(parent: &span, "rpc finished")`.
  - Streaming methods are a constant, `const STREAMING: &[&str] = &["WatchMembership", "Replicate", "Snapshot", "WatchLeases"];`, with a test that reads `fleetring-proto/proto/fleetring/v1/fleetring.proto` and checks that exactly these RPCs take or return `stream`.
  - Refusals: `fleetring_auth_refusals_total{reason}` is incremented when the final code is `permission_denied` (reason `binding` if the response carries the `fleetring-binding` header, else `scope`), `unauthenticated` (`unauthenticated`), or when the response carries the `Refusal` extension below (its reason).
- `src/auth/boundary.rs`:
  - A response extension, `#[derive(Debug, Clone, Copy)] pub(crate) enum Refusal { NoRequirement, DenylistStale }`, inserted by `AuthService::call` into the `http::Response` it builds from a refusal: `NoRequirement` when `rpc_at` found nothing, `DenylistStale` when the token couldn't be checked for want of a fresh denylist (the `UNAVAILABLE` `with_revocations` returns). `TraceService` reads it before the code rules above, so a stale denylist counts as `denylist_stale`, not as an `unavailable` with no reason.
  - In `Authorizer::admit`, once the caller is resolved: `Span::current().record("principal", name)` for a principal and `.record("node", id)` for a node. In `Authorized::check`, when a database is checked: `.record("database", database.as_str())`. Credentials are never recorded.
- `src/daemon.rs`, `serve`: replace `.layer(AuthLayer::new(authorizer))` with `.layer(tower::ServiceBuilder::new().layer(TraceLayer::new(node.clone(), &metrics)).layer(AuthLayer::new(authorizer)).into_inner())`, so the trace layer is outermost (`ServiceBuilder` applies the first layer added outermost).
- Tests:
  - `src/observe/request_id.rs`: `an_incoming_request_id_is_kept_when_valid` (`abc-1.2_X` kept); `an_invalid_request_id_is_replaced` (65 characters, a space, a `/`, and an empty value each give `<node>-0000000000000001`, then `…02`); `generated_ids_count_up_per_daemon` (two `RequestIds` for different nodes count independently).
  - `src/observe/trace.rs`: `streaming_methods_match_the_proto`; `a_trailers_only_error_is_counted_with_its_code` (a test service returning `Status::not_found` counts `code="not_found"` once and leaves in-flight at 0); `a_dropped_stream_counts_as_cancelled`.
  - `tests/observe.rs`: `rpcs_and_refusals_are_counted`: on a `both` daemon, one `Control.Locate` for a missing database counts `fleetring_rpc_requests_total{service="control",method="Locate",code="not_found"} 1`; a `Database.Execute` with a principal certificate that has no scope counts `fleetring_auth_refusals_total{reason="scope"} 1`; a `Replication.Fence` from a client certificate counts a refusal (`scope`, since principals can't make node calls); `fleetring_rpc_in_flight` reads 0 for each afterwards.
  - `tests/observe.rs`: `a_request_id_reaches_the_control_plane` (`#[tokio::test(flavor = "current_thread")]`, with a `tracing_subscriber::registry()` plus a small test `Layer` that records each `rpc` span's `method` and `request_id` fields into an `Arc<Mutex<Vec<(String, String)>>>`, installed with `tracing::subscriber::set_default`): a raw `DatabaseClient` sends `Database.Create` for a new database with metadata `x-request-id: req-42`; the recorded spans include `("Create", "req-42")` and `("AcquireLease", "req-42")`, the second being the control call the data node made while serving the first.

### Step 6: data plane, leases, promotions, and replication instrumentation

Commit: `feat(fleetringd): count writes, queries, renewals, promotions, and replication`.

- `src/data/node.rs`: `DataNode::new` gains a final parameter `metrics: DataMetrics`, stored, with `pub(crate) fn metrics(&self) -> &DataMetrics`. Update every call site (`src/daemon.rs`, and any test helper that builds one) to pass `metrics.data().clone()` or `DataMetrics::default()`.
- `src/data/service.rs`, `execute`: start a `tokio::time::Instant` at entry; on the result:
  - `Ok`: `outcome="acknowledged"`, and observe `fleetring_write_duration_seconds`;
  - `Err(status)` with `primary_hint(&status).is_some()`: `redirected`;
  - `Err(status)` with code `Unavailable` (no primary hint) or `NotFound` (unknown database): `refused`; `Aborted`: `aborted`; anything else: `failed`.
  `query`: `ok` (observe `fleetring_query_duration_seconds`), `redirected`, `failed`, by the same rules.
- `src/data/lease.rs`:
  - `renew_all`: for each renewal, `renewed` on success; `refused` when the control plane answered with a status that demotes (the existing branch that calls `demote` or `discard_if_dropped`); `failed` when the call itself failed (unreachable control plane, timeout).
  - `promote`: start an `Instant` on entry; `completed` (observe `fleetring_promotion_duration_seconds`) when the node starts serving at the epoch; `superseded` when it gives up because control state has moved past the epoch (the `FAILED_PRECONDITION` paths); `failed` otherwise.
- `src/data/drain.rs`, `transfer`: `completed` when the database has a new primary within the hand-over limit, `failed` otherwise.
- `src/replication/mod.rs`: `ReplicationParts` gains `pub metrics: ReplicationMetrics`; update the construction sites in `src/daemon.rs`, `src/data/leases.rs`, `src/replication/mod.rs` (docs and tests), `tests/changes/common.rs`, `tests/replication/common.rs`, and `tests/replication/binding.rs` (tests pass `ReplicationMetrics::default()`; `ReplicationMetrics` is re-exported as `pub` from `observe` for this, documented as a handle the daemon passes in).
- `src/replication/primary.rs`: in `stream`, after a batch is written to a follower's stream, increment `fleetring_replication_batches_sent_total` and add the payload length to `fleetring_replication_sent_bytes_total`; in `reseed`, increment `fleetring_replication_reseeds_total`.
- `src/replication/snapshot.rs`: increment `fleetring_replication_snapshots_sent_total` when a snapshot's last chunk is sent.
- `src/replication/follower.rs`: increment `fleetring_replication_batches_applied_total` after a batch applies durably, before its acknowledgement.
- Tests in `tests/observe.rs`:
  - `writes_and_queries_are_counted`: a control node and two data nodes, a database with replication factor 2; three writes and one query through `Client` against the primary, one write sent to the follower's address with a raw `DatabaseClient`; the primary's exposition has `fleetring_writes_total{outcome="acknowledged"} 3`, `fleetring_queries_total{outcome="ok"} 1`, `fleetring_replication_batches_sent_total` at least 3, and the follower's has `fleetring_writes_total{outcome="redirected"} 1` and `fleetring_replication_batches_applied_total` at least 3.
  - `a_drain_counts_a_transfer_and_a_promotion`: the same fleet with replication factor 2; drain the primary; its exposition has `fleetring_lease_transfers_total{outcome="completed"} 1`, and the follower's has `fleetring_promotions_total{outcome="completed"} 1` and a `fleetring_promotion_duration_seconds_count 1`.

### Step 7: control plane, membership, and change streams

Commit: `feat(fleetringd): count control commands, heartbeats, and change deliveries`.

- `src/control/node.rs`: `ControlNode::open_with_metrics(path, options, clock, metrics: ControlMetrics)`; `ControlNode::open` calls it with `ControlMetrics::default()`. The writer keeps the handle:
  - `commit_group`: time `self.log.append_all(...)` and observe `fleetring_control_log_write_duration_seconds` for successful groups.
  - `apply_logged`: increment `fleetring_control_commands_total{action, outcome}` (`applied` for `Ok`, `rejected` for `Err`), set `fleetring_control_log_index` to `index`, and set `fleetring_control_snapshot_index` after a successful `save_snapshot`.
  - `src/daemon.rs`, `open_control`: call `open_with_metrics` with `metrics.control().clone()`.
- `src/membership.rs`: `Membership::start_reporting` gains a final parameter `metrics: MembershipMetrics`, passed to `heartbeat_loop`, which times each heartbeat (from sending to reply) into `fleetring_heartbeat_duration_seconds` and counts `fleetring_heartbeats_total{outcome="recorded"}` or `{outcome="failed"}`. Update every call site; tests pass `MembershipMetrics::default()`.
- `src/changes/mod.rs`: `ChangeParts` gains `pub metrics: ChangeMetrics`; update the four construction sites. `src/changes/pump.rs`: increment `fleetring_change_deliveries_total{sink, outcome="delivered"}` where a delivery succeeds, `{outcome="failed"}` where `state.failures += 1`, and `fleetring_change_resets_total{sink}` where `state.resets += 1`.
- Tests:
  - `src/control/node.rs` (unit, `StorageIo::Inline` in a paused-time Tokio test): `control_commands_are_counted_by_action_and_outcome`: a join, a creation, and a `Leave` of an unknown member give `fleetring_control_commands_total{action="join",outcome="applied"} 1`, `{action="create_database",outcome="applied"} 1`, `{action="leave",outcome="rejected"} 1`, and `fleetring_control_log_index 3`.
  - `tests/observe.rs`: `heartbeats_and_sink_deliveries_are_counted`: a `both` daemon with a `file` change sink named `audit`; after one write and two heartbeat intervals, `fleetring_heartbeats_total{outcome="recorded"}` is at least 2 and `fleetring_change_deliveries_total{sink="audit",outcome="delivered"}` at least 1.

### Step 8: JSON logs and the log format flag

Commit: `feat(fleetringd): write logs as JSON with --log-format`.

- `src/config.rs`:

  ```rust
  /// The `[log]` table.
  ///
  /// ```toml
  /// [log]
  /// format = "json"   # or "text", the default
  /// ```
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
  #[serde(default, deny_unknown_fields)]
  pub struct LogConfig { pub format: LogFormat }

  /// How log lines are written.
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
  #[serde(rename_all = "lowercase")]
  #[non_exhaustive]
  pub enum LogFormat { #[default] Text, Json }
  ```

  `Config` gains `#[serde(default)] pub log: LogConfig`; `LogFormat` implements `FromStr` (`text`, `json`; anything else is an error naming both values) for the flag and the variable. Re-export both from `src/lib.rs`.
- `src/main.rs`:
  - `Cli` gains `#[arg(long, global = true, value_parser = parse_log_format)] log_format: Option<LogFormat>`, documented as "Writes log lines as `text` or `json`. Overrides FLEETRING_LOG_FORMAT and the config file's `[log] format`."
  - A constant `LOG_FORMAT_VAR: &str = "FLEETRING_LOG_FORMAT"`.
  - `main` resolves the format before it installs the subscriber: the flag; else the variable (an invalid value prints `fleetringd: invalid FLEETRING_LOG_FORMAT '<value>'; use text or json` and exits with code 2); else, for the daemon only, `[log] format` from the configuration file (read with `Config::load` before logging starts; a load error is reported as today, without a subscriber); else `Text`.
  - `Json` installs `tracing_subscriber::fmt().json().with_current_span(true).with_span_list(true).with_env_filter(...).with_writer(std::io::stderr)`; `Text` keeps today's subscriber.
  - `run_daemon` takes the already-loaded `Config` instead of loading it again.
- Tests:
  - `src/main.rs` `#[cfg(test)]`: `log_format_is_accepted_before_and_after_a_subcommand` (`Cli::try_parse_from(["fleetringd", "--log-format", "json", "status", "--control", "127.0.0.1:1"])` and the same with the flag after `status` both parse to `Some(LogFormat::Json)`; this also checks that the global flag isn't refused by `args_conflicts_with_subcommands`); `an_unknown_log_format_is_refused`.
  - `src/config.rs`: `the_log_format_defaults_to_text_and_reads_json`.
  - `tests/observe.rs`: `json_logs_carry_the_rpc_span` (`current_thread` runtime; a `tracing_subscriber::fmt().json().with_current_span(true).with_span_list(true)` subscriber writing to a shared buffer, set with `set_default`): after one `Database.Execute` with `x-request-id: req-7`, every buffered line parses as a JSON object, and at least one line has `span.name == "rpc"` with `span.request_id == "req-7"` and `span.method == "Execute"`.

### Step 9: the cardinality budget and the simulation hook

Commit: `test(fleetringd): hold metrics to the series budget and expose them in simulation`.

- `src/sim/host.rs`: `Running` gains `metrics: Metrics`, set from `daemon.metrics().clone()` in `run_host`.
- `src/sim/fleet.rs`: `pub fn metrics(&self, host: &str) -> Option<String>`: the exposition of a running host, `None` if it isn't running.
- `tests/observe.rs`: `two_hundred_databases_stay_within_the_series_budget`: one `both` daemon, `[admin] sample_interval_ms = 100`, 200 databases named `tenant:t<k>/db` with replication factor 1, one write each; after one sample, parse the exposition (every non-comment line up to `# EOF`): fewer than 1,500 sample lines; no label value equals any of the 200 names or contains `tenant:`; every label name is in `ALLOWED_LABELS`; `fleetring_databases{replica_role="primary"} 200`. Creations run eight at a time to keep the debug run short.
- `tests/observe.rs`: `two_daemons_in_one_process_keep_separate_metrics`: two `both` daemons; three writes to the first; the second's `fleetring_writes_total{outcome="acknowledged"}` reads 0.
- `tests/sim/observe.rs` (add `mod observe;` to `tests/sim/main.rs`): `metrics_count_the_acknowledged_writes` over `seeds::FIXED`: a control host, three data hosts, the writer workload `tests/sim/history.rs` uses; after `await_workloads`, the sum over data hosts of `fleetring_writes_total{outcome="acknowledged"}` is at least the number of acknowledged writes in `fleet.history()`, and `fleet.finish()` passes. The admin listener isn't started under simulation; the sampler is, and runs on simulated time.
- Run the whole simulation suite under `RUSTFLAGS="--cfg tokio_unstable"` and confirm the fixed seeds still pass; the sampler adds timers but no behaviour.

### Step 10: documentation

Commit: `docs: document metrics, probes, and JSON logs`.

- `crates/fleetringd/README.md`: an "Observability" section after "Principals and scopes": the admin listener and its defaults; how to expose it (`tls = true` with a `cert client prometheus` certificate and `scrapers = ["prometheus"]`, with a Prometheus `scrape_configs` example using `scheme: https` and `tls_config` with `ca_file`, `cert_file`, `key_file`); `/healthz` and `/readyz` with the check table of step 3 and a Kubernetes probe example; the metric family table of step 1 with exposed names; the label rules and why there are no per-database labels (use `fleetringd status --node`); JSON logs, the `rpc` span's fields, and `x-request-id`.
- `deploy/fleetringd.toml`: a commented `[admin]` table with every field and default, noting that inside a container the default loopback address is reachable only from the container, and a commented `[log]` table.
- `docs/lexicon.md`, new entries in alphabetical order:
  - **Admin listener.** "The HTTP listener a daemon serves `/metrics`, `/healthz`, and `/readyz` on, apart from the gRPC port; loopback and plaintext by default (ADR-0034). **Applies to:** daemon, CLI. **Code:** `[admin]`, `AdminConfig`, `observe::admin`. **CLI:** none. **Not:** status port, management port, HTTP port, debug server."
  - **Readiness.** "Whether a node should be sent work now: `/readyz` passes when every check of its node roles passes (`joined`, `ring`, `denylist`, `draining`, `control_log`, `leader`). Health (`/healthz`) only says the process can serve. **Applies to:** daemon, UI. **Code:** `observe::Probes`, `Readiness`, `Check`. **CLI:** none. **Not:** liveness (a member's liveness is the control plane's view; see [liveness](#liveness)), status, up."
  - **Request ID.** "The identifier of one request, kept from `x-request-id` or made as `<node-id>-<counter>`, carried in the `rpc` span and on the calls a node makes for the request. **Applies to:** daemon. **Code:** `observe::RequestId`, `x-request-id`. **CLI:** none. **Not:** trace ID (until traces are exported), correlation ID."
  - **Scraper.** "A principal whose client certificate may read `/metrics` on an admin listener with TLS (`[admin] scrapers`). It has no scope on the gRPC port unless a `[[principal]]` entry gives it one. **Applies to:** daemon. **Code:** `AdminConfig::scrapers`. **CLI:** `fleetringd cert client <principal>` issues its certificate. **Not:** collector, exporter, monitor."
  - The "Words we don't use" table gains a row: "health (for a member's liveness) → [liveness](#liveness): `/healthz` is about the process; liveness is the control plane's view."
- `docs/cli.md`: section 13 (Logging) gains "`--log-format text|json` (or `FLEETRING_LOG_FORMAT`, or the daemon's `[log] format`) chooses the format; JSON is one object per line." The "Where `fleetringd` stands" table gains no row.
- `docs/roadmap.md`: E2's "Metrics (Prometheus)…" line links FR-063 ([#66](https://github.com/fleetring/fleetring/issues/66)) and ADR-0034.
- `docs/adr/README.md`: ADR-0034's status when accepted.
- `SECURITY.md`: one paragraph: the admin listener is loopback and plaintext by default; over TLS, `/metrics` is limited to scrapers and the probes are open; metrics carry no database names, principals, or token IDs.
- This plan's Progress section, and the issue.

## Documentation

Step 10 lists every file. The README's metric table is generated by hand from `observe::FAMILIES`; a test in `tests/observe.rs`, `the_readme_lists_every_family`, reads `crates/fleetringd/README.md` and checks that every exposed name appears in it, as the scope table's test does for RPCs.

## Owner decisions (2026-11-03)

The open questions this plan raised were decided as recommended:

- Per-database metrics stay out, and so do an opt-in for them, OpenTelemetry export, and a gRPC health service.
- `allow_plaintext` on a non-loopback admin address stays, with a warning at every start.

## Open risks

- **Metric names become an interface.** Alerts and dashboards are built on them, so a rename breaks someone's pager. Renames and removals are breaking changes listed in the release notes, and `the_readme_lists_every_family` keeps the documented list honest.
- **The sampler at 10,000 databases.** One tick walks every hosted database's status (`statuses`, `holders`, `primary_status`), which takes the replication state's locks. At 10,000 databases that is tens of milliseconds every 10 s; if `docs/perf.md`'s benchmarks show contention on the write path, the sampler should keep running totals updated on change instead.
- **Tracing on hot paths.** A span per RPC and a body wrapper per response add allocations to every replication batch ack and every write. `Replicate` is a stream, so its batches don't each pay for a span, but unary calls do. Measure with the existing `capture` bench and a write benchmark before and after; if the cost is visible, record `code` and `duration_ms` only at debug level.
- **Per-database questions go unanswered by metrics.** An operator chasing one slow tenant has `status --node` and the logs, not a time series. Opt-in per-database labels are a later decision (ADR-0034, Consequences).
- **The admin port in containers.** The default loopback address isn't reachable from outside a container; operators must set `listen` and either TLS or `allow_plaintext`. The example configuration says so.
- **`prometheus-client` versions.** Its 0.x API has changed between minor versions (label set derive, `Collector`). The plan uses only `Registry`, `Family`, `Counter`, `Gauge`, `Histogram`, `EncodeLabelSet`, and `text::encode`, which have been stable since 0.21.

## Progress

- 2026-11-04: implemented on the `metrics` branch, steps 1 to 10, in four commits rather than ten: the source and its unit tests, the documentation, the integration and simulation tests, and the measurements. The steps share files (`daemon.rs` wires every one), and a commit per step would have left dead code that `clippy -D warnings` refuses between them.
- Acceptance criteria, and the tests that hold them:
  - ADR-0034 is accepted and listed (it was accepted on 2026-11-03); a dated amendment records the details below.
  - Every family at `/metrics` on a default daemon: `the_admin_listener_defaults_to_loopback_plaintext` (the default is `127.0.0.1:7401`), `metrics_health_and_readiness_on_the_admin_listener` (a `TYPE` line per family, served on a port the test binds as 0, since a fixed port collides with tests running in parallel), `the_exposition_lists_every_family`, and `the_readme_lists_every_family`.
  - `/healthz` and `/readyz`: `readiness_lists_every_check_of_both_roles`, `a_node_is_not_ready_until_it_joins`, `a_draining_node_is_not_ready`, and the unit tests in `observe::health`.
  - The non-loopback validation: `a_non_loopback_admin_address_needs_tls_or_plaintext` and `a_non_loopback_admin_address_fails_validation`; `admin_tls_needs_the_node_s_certificate` covers `tls = true` without `[tls]`.
  - TLS and scrapers: `admin_tls_requires_a_listed_scraper` (401, 403 for another principal and for a node, 200 for `prometheus`, probes without a certificate, no HTTP answer to plaintext) and `only_listed_principals_may_scrape_over_tls`.
  - Cardinality: `two_hundred_databases_stay_within_the_series_budget` (also checks every label name against the allow-list and every closed label value against its set, `observe::closed_values`).
  - Logs and request IDs: `json_logs_carry_the_rpc_span`, `a_request_id_reaches_the_control_plane`, `the_daemon_writes_json_lines_with_log_format_json` (the binary, with the flag and with the variable), and the `observe::request_id` and `observe::trace` unit tests.
  - Separate registries and the simulation: `two_daemons_in_one_process_keep_separate_metrics`, `two_registries_keep_separate_counts`, and `metrics_count_the_acknowledged_writes` in the simulation suite, which otherwise passes unchanged.
  - Documentation: `deploy/fleetringd.toml`, the README's "Observability" section, the lexicon, `docs/cli.md`, `docs/roadmap.md`, and `SECURITY.md`.
- Deviations from the plan, and why:
  - `prometheus-client` 0.25 (the latest; Apache-2.0 OR MIT), not 0.23. It adds `dtoa` and `prometheus-client-derive-encode` (both MIT OR Apache-2.0), and `tracing-subscriber`'s `json` feature adds `tracing-serde` (MIT); `itoa` and `parking_lot` were already in the tree. `hyper` gains its `server` feature and the crate depends on `http-body` directly; both were already in the tree.
  - Every family is listed from the first scrape. `prometheus-client` leaves a labelled family out until it has a series, so a wrapper registers each one as always present; closed outcome and reason sets, each configured sink's series, and each control action and outcome start at 0. The RPC families get series as RPCs arrive.
  - `Membership::start_reporting` keeps its signature; the daemon calls a crate-private `start_observed` that takes the handle, so tests that start membership by hand don't change. `ControlNode::open_with_metrics` is crate-private for the same reason. `ReplicationParts` and `ChangeParts` gain `metrics` as planned.
  - `TraceLayer::new` is public, for the `observe` bench and for the router (FR-066).
  - The boundary's `Refusal` extension is decided in `AuthService::call` from the path and the status `admit` returned (only a token without a fresh denylist yields `UNAVAILABLE` there), which leaves `admit` unchanged.
  - `--log-format` before a command's name was refused by clap's `args_conflicts_with_subcommands`, as the plan's test anticipated; `main` now checks the daemon-options-with-a-subcommand conflict itself (`daemon_options_are_refused_next_to_a_subcommand`).
  - `fleetring_denylist_age_seconds` before the first denylist is written `inf`, which is how `prometheus-client` encodes +Inf; Prometheus parses it.
  - Every integration test's daemon gets `[admin] listen = "127.0.0.1:0"` (`support::ADMIN`, appended by `Tls::node_table`), so no test contends for 7401.
  - Hot-path cost (open risk 3): the trace layer costs about 1.7 µs per request at info level, and 1.35 µs once `code` and `duration_ms` are recorded only when debug logging is on, which is the plan's fallback, taken. End to end that is about 2% of a `Locate` and within noise for an `Execute`; `docs/perf.md` has the numbers under "Request path". The `capture` bench doesn't reach the request path and didn't move.
