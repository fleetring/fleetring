# ADR-0034: Metrics, health, and readiness on a separate admin listener, with bounded labels and a span per request

- **Status:** Accepted (2026-11-03)
- **Date:** 2026-11-03
- **Related:** ADR-0013 (async boundaries), ADR-0015 (daemon layout), ADR-0020 (simulation), ADR-0025 and ADR-0026 (principals, scopes, enforcement), ADR-0033 (surfaces); tickets FR-063

## Context

The roadmap's "Operable" criterion asks for metrics, health checks, and logs an operator can collect. A running `fleetringd` offers `tracing` text on stderr and the `status` commands. Neither can be scraped, a load balancer or an orchestrator has nothing to probe, and the log lines one request causes, on the node it reached and on the nodes that node called, share nothing that ties them together.

Five forces shape the answer:

- **The gRPC port is closed to strangers.** Every connection to it is mutual TLS with a certificate from the fleet's authority, and every RPC has a requirement in `auth::RPCS` (ADR-0025, ADR-0026). Prometheus can present a client certificate, but a kubelet's HTTP probe can't, and neither speaks gRPC to a scope table.
- **Scale.** A node can host 10,000 databases and a fleet 1,000 nodes (roadmap E4). One series per database per family is millions of series per fleet: the usual way a Prometheus server falls over. Peer node IDs as labels have the same shape at fleet scale.
- **Database names are tenant data.** A name such as `tenant:acme/billing` says who the customers are. Labels travel to every system that stores metrics, with weaker access control than the fleet's scopes.
- **Determinism.** Daemon code reads time only from `tokio::time` or a `Clock`, and randomness only from an injected generator (ADR-0020). Several daemons run in one process in tests and in the simulation, so metrics can't live in process-global state.
- **Lexicon.** Metric and label names are an interface operators build alerts and dashboards on, and the lexicon is the single source of names (ADR-0033 point 5).

Precedents: etcd serves `/metrics` and `/health` on `--listen-metrics-urls`, separate from its client port; CockroachDB serves `/_status/vars` and `/health?ready=1` on its HTTP port, apart from its SQL and RPC port; Kubernetes components expose `/healthz`, `/livez`, and `/readyz` with per-check detail; Prometheus' naming guide asks for a prefix, base units, and `_total` on counters, and warns against unbounded label values.

## Decision

1. **A separate admin listener.** The daemon serves HTTP/1.1 on `[admin] listen`, `127.0.0.1:7401` by default, apart from the gRPC port. `[admin] enabled = false` turns it off. It serves only `GET` and `HEAD` on three paths, answers `405` for other methods and `404` for other paths, and is started by the daemon itself, so a node needs nothing else installed. Under simulation, whose network is turmoil's, the listener isn't started; the metrics exist all the same and the harness reads them directly. `Daemon::admin_address()` returns the bound address, so tests bind port 0.

2. **Plaintext on loopback, TLS anywhere else.** A `listen` address that isn't loopback is refused at validation unless `[admin] tls = true` or `[admin] allow_plaintext = true`; the error names both fields. With `tls = true` the listener presents the node's own certificate from `[tls]`, reloaded when its files change exactly as the gRPC port's is, and asks for a client certificate from the fleet's authority without requiring one at the handshake. `allow_plaintext` exists for networks where the operator controls who can connect (a pod network with a network policy) and logs a warning at every start, as `insecure_plaintext` does.

3. **Who may scrape.** `/metrics` over TLS needs a client certificate whose principal is listed in `[admin] scrapers`: `401` without a certificate, `403` for any other principal, node certificates included. Scrapers are configuration, like `[[principal]]` entries; no scope action is added, so ADR-0025's four actions and the `fleet` scope stay as they are, and a scraper's certificate grants nothing on the gRPC port unless a `[[principal]]` entry also lists it. `/healthz` and `/readyz` never need a certificate: they say only whether the node is up and ready, which a probe must be able to learn. On a plaintext listener every endpoint is open to whoever can connect, which is why plaintext is loopback-only by default.

4. **Health and readiness.**
   - `/healthz` answers `200` with `ok` while the process can serve, and `503` once the control writer has stopped after a storage failure, the one state from which only a restart recovers (`SubmitError::Storage`).
   - `/readyz` answers `200` when every check of the node's roles passes and `503` otherwise, with a JSON body: `{"ready": <bool>, "checks": [{"name": "<check>", "ok": <bool>, "detail": "<text>"}]}`, every check listed, failing or not.
   - Data role checks: `joined` (the node has joined the fleet), `ring` (it has published a ring), `denylist` (its token denylist is no older than the stale bound of ADR-0026's amendment, so it can accept tokens), and `draining` (fails while the node drains, so a load balancer stops sending it work).
   - Control role checks: `control_log` (the log recovered and the writer runs) and `leader` (a leader is known; always true for one control node, and FR-064's Raft group gives it meaning).
   - Every check reads local state. A probe never makes a network call, so a slow peer can't make a probe time out.

5. **One registry per daemon, with `prometheus-client`.** A new `fleetringd::observe` module owns a `Metrics` value: the registry and typed handles to every family. The daemon creates it at start and passes clones of the handles it needs to each component. Nothing is global, so two daemons in one process keep separate counts. Histograms are `f64`, which the daemon allows; the float ban is the placement crate's. Durations are measured with `tokio::time::Instant`, so they follow simulated time under turmoil. The exposition is OpenMetrics text, content type `application/openmetrics-text; version=1.0.0; charset=utf-8`, which Prometheus reads natively.

6. **Names.** Every metric is `fleetring_<subsystem>_<thing>_<unit>`:
   - the prefix `fleetring_`; subsystems `rpc`, `auth`, `denylist`, `membership`, `heartbeat`, `databases`, `writes`, `queries`, `replication`, `lease`, `promotion`, `control`, `change`, plus `build_info` and `node_info`;
   - base units in the name (`_seconds`, `_bytes`), never milliseconds or kibibytes;
   - `_total` on every counter, and plural nouns for counts (`fleetring_databases`, `fleetring_members`);
   - lexicon words only: primary and follower, lease, epoch, promotion, transfer, batch, sink, denylist, member, replica, heartbeat, live and down. A metric that needs a new word adds it to the lexicon first.

   The full list of families, with types, labels, and where each is updated, is in FR-063's plan and the daemon README; a renamed or removed family is a breaking change, listed in the release notes like a renamed CLI flag (`docs/cli.md`, section 14).

7. **Labels from closed sets only.** Label names come from one allow-list: `service`, `method`, `code`, `replica_role`, `liveness`, `outcome`, `reason`, `action`, `state`, `sink`, and, on the two info metrics only, `version`, `protocol`, `node_id`, `node_role`, and `fleet`. Every value comes from a closed set fixed by the code (an RPC in `auth::RPCS`, a gRPC status code in lower snake case, a control `Action` variant, an outcome a family documents) or from the node's own configuration (a sink's name). No label value is ever a database name, principal, token or signing key ID, peer node ID, address, or SQL text. Questions about one database, one follower, or one sink's progress on one database are answered by `status --node` and `NodeStatus`, which scopes protect. Spreads over databases become sampled gauges: a background task refreshes counts, maxima, and sums over the node's databases every `[admin] sample_interval_ms` (10 s by default). Tests hold every family to the allow-list, and a node with 200 databases to fewer than 1,500 series with no database name among the label values.

8. **Logs: text or JSON, and a span per request.**
   - `[log] format = "text" | "json"` (text by default), `--log-format text|json` on the daemon and on every command, and `FLEETRING_LOG_FORMAT`, resolved in the CLI guide's order: flag, environment, configuration, default. JSON is one object per line from `tracing-subscriber`'s JSON formatter, with the current span and its parents.
   - A tower layer, `TraceLayer`, sits outside `AuthLayer` on every service and opens an `rpc` span for each request with `service`, `method`, and `request_id`, and with `principal` or `node`, `database`, `code`, and `duration_ms` recorded when they become known. The same layer counts and times the RPC metrics, so the two can't disagree.
   - **Request IDs:** an incoming `x-request-id` of at most 64 characters from `[A-Za-z0-9._-]` is kept; any other value, or none, is replaced by `<node-id>-<16 hex digits>` from a per-daemon counter, which is deterministic and needs no randomness. The ID is held in a task-local for the request's task, and the interceptor every control and database client already runs (`Credentials`) sends it on each call made from that task, so a data node's control call on behalf of a request carries the request's ID. Work a request hands to another task (replication streams, change pumps) doesn't inherit it.
   - Audit lines (`fleetringd::audit`) keep their fields and now also sit inside the `rpc` span. Credentials are never span fields.

9. **Out of scope now:** OpenTelemetry trace export (the span layout is the one `tracing-opentelemetry` would export), per-database metrics as an opt-in, push gateways, and dashboards. The router (FR-066) registers its families on this registry and follows these rules.

## Alternatives considered

- **Metrics and health on the gRPC port.** One port to open, and tonic can route HTTP paths beside gRPC. But every connection to that port must be mutual TLS with an authority certificate, so a kubelet probe can't reach it, and plain HTTP/1.1 handlers would have to bypass `AuthLayer`, the one place every request is checked. Rejected.
- **The gRPC health checking protocol only** (`grpc.health.v1`). Load balancers that speak gRPC understand it, but Prometheus doesn't, kubelet needs a gRPC probe and a certificate, and it says nothing about why a node isn't ready. A later ticket can add it on the gRPC port beside `/readyz`; it doesn't replace the admin listener.
- **Bearer tokens for scrapers.** Prometheus supports them, and tokens already exist. But a token is verified against the denylist that only data nodes hold, so a control-only node couldn't check one, and tokens carry scopes this endpoint doesn't use. A client certificate from the same authority, named in configuration, is simpler and works on every node role.
- **A new `metrics` scope action.** Consistent with the gRPC port, but it changes ADR-0025's four actions for one HTTP endpoint and makes scraping a matter of token issuance. Rejected in favour of `scrapers`.
- **Per-database labels, limited by a cap or an allow-list of prefixes.** Useful for a handful of important databases, and CockroachDB-style "top N" exports exist. But any cap is a cliff (series appear and vanish as databases cross it), and the names leak into metric stores. Deferred: the sampled gauges and `status --node` cover the questions without either problem.
- **The `metrics` facade with `metrics-exporter-prometheus`.** Instrumentation through macros is pleasant, but the facade is a process-global recorder, so two daemons in one process (every integration test and the simulation) would share counts. The `prometheus` crate (TiKV's) has a global default registry and pulls in protobuf. `prometheus-client` has neither and implements OpenMetrics directly.
- **A push gateway.** For batch jobs, not for long-running daemons that a scraper can reach. Rejected.
- **Random request IDs (UUIDs).** Conventional, but daemon code takes randomness only from an injected generator (ADR-0020), and a counter prefixed with the node ID is unique within a fleet's logs and replays identically in the simulation.

## Consequences

- An operator gets a scrape target and two probes with no extra process, on loopback by default; exposing them needs a decision (`tls = true` and a scraper's certificate, or `allow_plaintext`), which is the same posture as `insecure_plaintext` on the gRPC port.
- Metric names and labels become an interface. Renames are breaking changes, and new families follow these rules and the lexicon.
- Per-database detail stays behind scopes (`status --node`); metrics answer fleet-level questions (how many under-replicated databases, how far behind the worst sink is), not "which database".
- Every component that is instrumented takes a handle from `Metrics`, which adds a parameter to several constructors; tests build `Metrics::new` for their own components.
- `tracing-subscriber` gains its `json` feature, and the daemon gains `prometheus-client` and hyper's server feature; `deny.toml` already allows their licences.
- The simulation is unaffected: metrics have no effect on behaviour, the admin listener doesn't start, and the harness reads each host's exposition directly.
- Follow-up decisions: opt-in per-database metrics, OpenTelemetry export, and the gRPC health service, each in its own ticket if asked for.

## Amendment (2026-11-04): details the implementation settled

FR-063's implementation adds these details; the decision above stands.

- **Every family is listed from the first scrape.** `prometheus-client` leaves a labelled family out of the exposition until it has a series, so a node would list some families only after the first event that updates them. The daemon lists every family in the table from the start, with its `HELP` and `TYPE` lines, and creates every series of a closed outcome or reason set, of each configured sink, and of each control action and outcome at zero. The RPC families get a series per RPC as RPCs arrive, since one per RPC and status code would spend most of the series budget on combinations that never happen.
- **Infinity is written `inf`.** `fleetring_denylist_age_seconds` before the first denylist is +Inf, which `prometheus-client`'s float encoder writes as `inf`; Prometheus parses both spellings.
- **`--log-format` and the daemon's options.** clap's rule that refuses the daemon's options next to a subcommand also refuses a global flag given before the subcommand's name, so the binary checks that conflict itself and `--log-format` works on either side of a command's name.
