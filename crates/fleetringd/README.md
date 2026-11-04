# fleetringd

The FleetRing daemon. It places, replicates, and fails over SQLite databases across machines you own. One binary runs a data node, the control node, or both (a one-machine fleet), and its subcommands talk to a running fleet.

> **Status:** early development. Not production-ready.

## Run it

Every connection is mutual TLS (ADR-0025): nodes and clients present certificates issued by the fleet's own certificate authority. Create the authority once, then a certificate for the node and one for yourself:

```sh
fleetringd cert init --fleet home --dir /etc/fleetring/tls
fleetringd cert node node-1 --addresses 127.0.0.1 --dir /etc/fleetring/tls
fleetringd cert client admin --dir /etc/fleetring/tls
export FLEETRING_TLS_DIR=/etc/fleetring/tls   # or pass --ca, --cert, and --key to each command
```

`cert init` writes `ca.crt` and `ca.key`; the key issues every certificate and belongs on no node. `cert node` writes `node.node-1.crt` and `node.node-1.key`, naming the node's ID and the addresses (IP addresses or DNS names) it is reached at; `cert client` writes `client.admin.crt` and `client.admin.key`, naming the principal the commands act as. The commands read `$FLEETRING_TLS_DIR/ca.crt`, `client.crt`, and `client.key`. To act as another principal, name it once with `--as <principal>` (or `FLEETRING_AS`), which reads `client.<principal>.crt` and `client.<principal>.key` from that directory instead, and fails naming both files if either is missing. `--ca`, `--cert`, and `--key` (or `FLEETRING_CA`, `FLEETRING_CERT`, and `FLEETRING_KEY`) name a file outright. For each file the first of these wins: its flag, its environment variable, `--as` or `FLEETRING_AS`, and the default in `FLEETRING_TLS_DIR`; `--help` repeats the order. Node certificates last 30 days and client certificates 7 by default (`--ttl 90d`); a node reloads its files when they change, so renewing one needs no restart, and `ca.crt` may hold two authority certificates while a rotation overlaps. Certificates from Vault, cert-manager, or SPIRE work the same way: the daemon only needs a certificate, its key, and the authority certificates to trust.

Write a configuration file. This one runs a one-node fleet, with its databases in `/var/lib/fleetring`:

```toml
data_dir = "/var/lib/fleetring"
listen = "127.0.0.1:7400"
role = "both"

[node]
id = "node-1"

[tls]
ca = "/etc/fleetring/tls/ca.crt"
cert = "/etc/fleetring/tls/node.node-1.crt"
key = "/etc/fleetring/tls/node.node-1.key"

[[principal]]
name = "admin"
scopes = ["admin:*", "stream:*", "fleet"]
```

The `[[principal]]` entry lets the `admin` certificate administer, write, read, and stream every database and run fleet operations such as draining; a principal the file doesn't list may do nothing with its certificate alone (see [Principals and scopes](#principals-and-scopes)).

Then start the daemon, and talk to it from another terminal:

```sh
fleetringd --config fleetringd.toml

fleetringd status --config fleetringd.toml
fleetringd db create notes --replication-factor 1 --config fleetringd.toml
fleetringd db exec notes "CREATE TABLE note (body TEXT)" --config fleetringd.toml
fleetringd db exec notes "INSERT INTO note VALUES (?)" "hello" --config fleetringd.toml
fleetringd db query notes "SELECT body FROM note" --config fleetringd.toml
fleetringd db locate notes --config fleetringd.toml
fleetringd db list --config fleetringd.toml
fleetringd status --address 127.0.0.1:7400
fleetringd status --node node-1 --config fleetringd.toml
```

An application gets its own certificate and a short-lived token that carries its scopes, so it can write `notes` and nothing else. Issue the token where `ca.key` is; the node verifies it with `ca.crt` alone:

```sh
fleetringd cert client notes-app --dir /etc/fleetring/tls
export FLEETRING_TOKEN=$(fleetringd token issue --principal notes-app --scope write:notes --ttl 1h --dir /etc/fleetring/tls)
fleetringd db exec notes "INSERT INTO note VALUES (?)" "from the app" --config fleetringd.toml --as notes-app
unset FLEETRING_TOKEN
fleetringd db drop notes --yes --config fleetringd.toml
```

**Statements.** `db exec` and `db query` run SQL against the one database they name, and statements are confined to it: a statement that attaches or detaches a database, loads an extension, or runs a pragma other than read-only introspection (`table_info`, `table_xinfo`, `index_list`, `index_info`, `index_xinfo`, `foreign_key_list`, `foreign_key_check`, `integrity_check`, `quick_check`, `compile_options`, and the read forms of `user_version`, `application_id`, `schema_version`, `page_size`, `page_count`, and `freelist_count`) is refused with `INVALID_ARGUMENT`. The daemon owns journaling, syncing, and checkpoints.

`db list` shows every database with its engine, epoch, replication factor, primary, and replicas, and ends with how many databases each node is primary for; `--node <id>` keeps the databases one node is primary for. `status --address <host:port>` asks one data node what it believes on its own, so its ring fingerprint can be compared with `status`; `status --node <id>` does the same, finding the node's address in the control plane's membership. `--node` always takes a node ID, and `--address` an address: `db exec` and `db query` take either, to send a statement through a particular node. `db drop` removes a database for good: every replica deletes its copy within a few seconds. It asks first on a terminal; `--yes` skips the question.

Each database chooses its durability when it is created (ADR-0023). `--acks quorum` (the default) acknowledges a write once a majority of its replicas hold it; `--acks all` waits for every replica, so one replica down blocks writes; `--acks local` acknowledges after the primary's own fsync, which is fastest but may lose acknowledged writes when the primary fails over. `--min-replicas <n>` sets the floor: a write that arrives while fewer than `n` replicas, the primary included, are reachable is refused with `UNAVAILABLE` before anything commits, so it can be retried cleanly; it must lie between the write quorum and the replication factor, and defaults to the write quorum. `db locate` and `db list` show the settings, `db set-durability` changes them from the database's next epoch on, and `[replication] acks` and `min_replicas` in the control node's configuration set the defaults for databases created without them.

**Drain.** `fleetringd node drain <id>` hands every database the node is primary for to a follower, through the same epoch and fencing rules as a failover but without waiting for a lease to expire: clients see a brief `UNAVAILABLE` and a redirect instead of a failover's pause. A database with no other replica has nowhere to go; the node logs `no other replica; the database is unavailable until this node returns` for it and keeps serving it until the node stops. The node gets no new primaries afterwards and stays a follower until it stops or restarts, or until `fleetringd node undrain <id>` clears the mark once every member supports it (see [Upgrading](#upgrading)). A graceful stop (`Ctrl-C`, SIGTERM) with `[membership] on_shutdown = "leave"`, the default, drains the same way before it leaves; `on_shutdown = "stay"` keeps the node's leases for a quick restart and does neither. `SIGHUP`, which a closing terminal also sends, doesn't stop the daemon: it restarts it in place (see [Upgrading](#upgrading)).

**Change streams.** A `[[changes.sink]]` entry in a data node's configuration sends every acknowledged batch of the databases it names to a sink: a `file` sink appends one JSON line per batch to a file per database, and a `webhook` sink POSTs each batch as JSON. `format = "logical"` (the default) sends the rows each transaction inserted, updated, or deleted; `format = "physical"` sends the opaque batch for a mirror or an archive. A logical sink's `capture` says how much of each row it gets: `id` the key alone, `after` (the default) the key and the new values of inserted and updated rows, `full` also the old values of updated and deleted rows and the names of the updated columns; a database named by several sinks records in the strongest mode asked for. `databases` lists exact names or prefixes ending in `*`, such as `["tenant:*", "ledger"]`. Sinks run beside replication and only see batches the write quorum holds, so a slow or failing sink never delays a write; delivery is at least once, in commit order. Row changes are recorded inside the database, in a table named `_fleetring_cdc` that replicates with the data so a promoted follower continues the stream, and so is each sink's position, a mark in a table named `_fleetring_sinks` that the database's next write carries after a delivery: a new primary resumes each stream at the newest mark the write quorum holds, and a failover repeats only what the old primary delivered after it, usually the last batch or two, never a gap. An engine that can't keep marks inside its database keeps positions under `<data_dir>/changes/` on that node alone, and a failover then repeats everything since the last checkpoint. `status --node` (or `--address`) shows each sink's position, whether it is `replicated` or `local`, and its backlog. Configure the same sinks on every node that may become a primary. Names beginning with `_fleetring_` are reserved, and `db exec` and `db query` refuse statements that touch them. Schema changes aren't streamed.

**Following leases.** `fleetringd lease watch --node <id>` (or `--address <host:port>`) follows the leases one data node holds, from the node's own view: it prints `held <database> epoch <n>` for each database the node serves when the stream opens, then `gained <database> epoch <n>` and `lost <database> epoch <n>` lines as leases move, until the stream ends. The node reports a lease before it acknowledges a write under that epoch, and reports its loss before it stops answering for the database, so a worker beside the node (a queue consumer, a scheduler, a cache warmer) can act on exactly the databases the node is primary for and stop the moment one moves, without polling. Events for a database arrive in epoch order; a subscriber that falls behind is cut off with `RESOURCE_EXHAUSTED` and opens the stream again, which starts with the leases held then. The stream is one node's; a client that wants every node subscribes to each.

**Verifying replicas.** Every replica keeps a hash chain over the replication log: one BLAKE3 hash per transaction, each folding in the hash before it, so the hash at a position commits every byte of the log before it. The primary computes each batch's hash as it ships the batch, and a follower recomputes it before applying the batch; one that doesn't match is refused, and the follower re-seeds from a snapshot rather than apply a log that isn't the primary's. `fleetringd db verify <database>` asks every replica for its hash at the latest position they all hold, each recomputed from the replica's log on disk, and prints the one hash and `all 3 replicas agree`, or names the first replica whose hash differs or whose log can no longer be read back to that position. `--at <position>` picks a position `db locate` printed instead, and `--expect <hash>` reports `nothing changed since <position>` when the log still hashes to a hash recorded earlier. The chain covers the log, not pages already checkpointed into the database file (FR-045), and proves consistency, not provenance: it isn't signed.

**Backups and restore.** Replicas don't protect against a bad `DROP TABLE`, which replicates in a round trip, or against losing the fleet; an archive does (ADR-0029). An `[[archive]]` entry in a data node's configuration writes the databases it names to an object store outside the fleet: an S3-compatible bucket (AWS S3, MinIO, Cloudflare R2, Garage) or a directory, such as a mounted volume. An archive is a physical change stream: each primary writes one immutable segment per delivery, holding every batch the write quorum acknowledged since the last one, at most one every `segment_interval_ms` (10 s by default), plus a base snapshot when the archive has no path to a delivery's start and then once a day or every GiB of segments. Its recovery point is its lag, which `status --node` shows as the archive's backlog like any sink's; a write never waits for the archive. Every segment records the hash chain at both its ends, and a restore recomputes the chain over every batch it applies, so a restored copy is proven to hold the log the fleet held; a segment or snapshot chunk that changed in the store fails the restore with `DATA_LOSS` naming the object, and a missing segment is refused rather than skipped. Retention keeps the newest `keep_snapshots` base snapshots (7) and the segments that continue them. Configure the same entries on every node that may become a primary: a new primary continues the archive from the mark the old one replicated, without a new snapshot.

`fleetringd db restore <new> --archive <name> --from <source>` creates a new database from the archive's copy of `source`, which needn't exist any more: the first live member of the new database's replica set restores it, takes the first lease, and serves it, and its followers seed from it. `--at-position <hex>` restores exactly the log up to a transaction boundary, and `--at-time <RFC 3339>` every segment archived at or before that time, by the archiving nodes' clocks: a time only picks which acknowledged prefix is restored, and the restore may stop up to one segment interval before it. A restore never overwrites a database; to restore under the old name, drop it first. It needs `admin` on both the new database and the source. `fleetringd db archive show <source> --archive <name>` lists the archive's snapshots and how far its segments reach, and `db archive verify`, which needs `admin` on the database since it costs a data node a whole copy on its disk, restores into a scratch copy there, verifying every batch, then asks each live replica for its hash at the restored position and prints `archive agrees with every replica` or the replicas that differ.

A data node runs one restore or verification at a time, and answers another with `UNAVAILABLE` until it is done. For disaster recovery, with no fleet at all, `fleetringd archive restore --config <file> --archive <name> --from <source> --to <dir>` reads the store from a configuration file's `[[archive]]` entry and writes `<dir>/data.db`, a plain SQLite database that `sqlite3` opens directly. It takes the same `--at-position` and `--at-time`.

S3 credentials are static keys: `credentials = "env"` (the default) reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN` when the daemon starts; `credentials = "file"` reads a TOML file with `access_key_id`, `secret_access_key`, and optionally `session_token`, which only its owner may read, and is re-read when it changes. Secrets are never logged. `endpoint` defaults to AWS's for the `region`; `path_style = true` suits MinIO; `ca` adds a private endpoint's authority to the platform's; and `allow_http = true` permits a plain `http://` endpoint, for a local MinIO only. The store must give read-after-write and list-after-write consistency, as these services do. An archive holds every row of the databases it names, unencrypted: grant its bucket the same care as the nodes' disks. `deploy/fleetringd.toml` documents every setting.

The subcommands take `--control <host:port>`, the address of the node that runs the control role, or `--config <file>` to read it from a daemon's configuration. A control call sent to a data-only node answers with the control node's address.

**Plaintext.** A daemon without a `[tls]` table refuses to start. `--insecure-plaintext` (or `insecure_plaintext = true` in the file) lets it run without one, warning at every start, and the subcommands take the same flag to connect to it; nothing on the wire is then authenticated or encrypted, so it is for local experiments only. A node whose certificate names another ID is refused on `Join` and `Heartbeat` with `PERMISSION_DENIED`, and only nodes may make node-to-node calls such as replication. In plaintext no certificate names the caller, so a request without a token may do anything, and a token is refused, since there is no authority to verify it against.

`fleetringd --help` lists every flag and subcommand. [`deploy/fleetringd.toml`](https://github.com/fleetring/fleetring/blob/main/deploy/fleetringd.toml) documents the main settings, including a multi-node fleet's.

## Principals and scopes

Every request comes from a node or a principal (ADR-0025). Nodes, named by their certificates, make the node-to-node calls, and act on themselves only otherwise: a node may follow the membership, leave or drain itself, locate databases, and hand over leases it holds, and nothing more; it can't read, write, or administer a database, drain or remove another member, or revoke a token (ADR-0026). A principal may do what its scopes allow and nothing else: a request no scope allows is refused with `PERMISSION_DENIED`, before it touches any state, with a message that names the action and the database it needed, never the caller's scopes.

A scope is `<action>:<pattern>`: the action is `read`, `write`, `admin`, or `stream`, and the pattern an exact database name or a prefix ending in `*` (`read:tenant:acme/*`, `write:notes`, `admin:*`). `write` implies `read`, and `admin` (create, drop, change durability, hand a lease over) implies both; `stream` stands alone, so a change consumer can't read tables. The word `fleet` is the scope for operations that name no database: membership, draining, revoking tokens.

A principal gets its scopes in one of two ways:

- **Client certificate.** `[[principal]]` entries in the configuration, identical on every node, list each principal with its `scopes`; the daemon checks them when it starts.
- **Bearer token.** `fleetringd token issue --principal <name> --scope <scope>… --ttl <duration> --dir <authority dir>` signs a token (an Ed25519 JWT whose header names the signing key in `kid`, with the claims `iss`, `sub`, `jti`, `iat`, `nbf`, `exp`, and `fleetring.scopes`) with the authority's key, valid one hour by default and 30 days at most; nodes refuse a token that lasts longer from `nbf` to `exp`, whatever minted it. A client sends it as `authorization: Bearer <token>` (`--token` or `FLEETRING_TOKEN` on the command line), and the request then acts with the token's scopes alone. Nodes verify it with the trusted `ca.crt`, with no call to the control plane, and refuse an expired, tampered, or revoked token with `UNAUTHENTICATED`. The certificate on the same connection must name the same principal, unless its `[[principal]]` entry sets `forward_tokens = true`, as a router that relays its callers' tokens does.

`fleetringd token revoke --token-id <id>` (the ID `token issue` prints) or `--key-id <kid>` adds a token, or every token a signing key issued, to a denylist the control plane keeps; data nodes receive it with every heartbeat reply, so each refuses the token within one heartbeat interval. A revoked token's entry is forgotten once the token would have expired anyway.

Each request a principal makes is logged at debug on the `fleetringd::audit` target, and each refusal as a warning, with the principal, the credential (`certificate`, or `token:<id>`), the action, the database, and the RPC; never the token itself. A refused token's line names its ID whenever the token's signature verified (a revoked, expired, or mismatched token), so the ID `token revoke` takes is in the log; a token that never verified is logged as `token`, since nothing in it can be believed.

What each RPC needs: `node` is a node certificate, and principals are refused; `fleet` is a node or the `fleet` scope; a listing shows each database the caller may read, and everything to `fleet`, except that `NodeStatus` shows a change sink's progress to a principal that may `stream` its database. A request for an RPC this table doesn't list is refused. The binding is what a node making the call must be for the database or node the request names (ADR-0031): `self` (the request names the caller), `replica` (in the database's replica set), `holder` (it holds the database's lease), `candidate` (the candidate of the promotion to the request's epoch), `holder or candidate` (either, at the stream's epoch), or `follower` (one the primary streams the database to).

A node checks every node call against its binding before it touches any state, judging the node the caller's certificate names. The control node checks against its own state. A data node asks the control plane who leads a new epoch once, through `Locate`, and then trusts its durable epoch record, so it keeps accepting its recorded primary while the control plane is down; a node it hasn't recorded gets `UNAVAILABLE` until the control plane is back. A call that fails its binding is refused with `PERMISSION_DENIED`, a `fleetring-binding` metadata entry naming the binding, and a warning on `fleetringd::audit`. So a stolen node key can act as that node and nothing more: serve, follow, and lead the databases placed on it at the epochs the control plane gives it, report its own positions, and join, drain, or leave itself. It can't stream to, fence, fetch from, snapshot, or promote any other database. Revoking it means removing the node and no longer trusting its certificate.

A token is checked when a request arrives, and a stream opened with one (`WatchLeases`, `WatchMembership`) is checked again before each message and every second between them, so it ends with `UNAUTHENTICATED` within about a second of the token's expiry, or of its revocation reaching the node. A data node checks tokens against the denylist from its last join or heartbeat reply; until the first one arrives, and whenever the last is older than `control.heartbeat_timeout_ms` (or three heartbeat intervals, if longer), it refuses every token with `UNAVAILABLE`, which a client retries, and keeps serving client certificates. Keep a data-only node's `[control]` timings equal to the control node's.

| RPC | Needs | Binding |
|---|---|---|
| `Control.Join` | node | self |
| `Control.Heartbeat` | node | self |
| `Control.WatchMembership` | fleet | — |
| `Control.CreateDatabase` | admin on the database | — |
| `Control.DropDatabase` | admin on the database | — |
| `Control.ChangeDurability` | admin on the database | — |
| `Control.Locate` | read on the database, or fleet | — |
| `Control.ListDatabases` | read per database listed, or fleet | — |
| `Control.AcquireLease` | node | self |
| `Control.RenewLease` | node | self |
| `Control.Leave` | fleet | self |
| `Control.StartPromotion` | node | replica |
| `Control.RecordFenced` | node | candidate |
| `Control.CompletePromotion` | node | candidate |
| `Control.Drain` | fleet | self |
| `Control.Undrain` | fleet | — |
| `Control.TransferLease` | admin on the database | holder |
| `Control.RevokeToken` | fleet | — |
| `Replication.Replicate` | node | holder or candidate |
| `Replication.Fence` | node | candidate |
| `Replication.Snapshot` | node | follower |
| `Replication.Fetch` | node | candidate |
| `Replication.Promote` | node | candidate |
| `Database.Execute` | write on the database | — |
| `Database.Query` | read on the database | — |
| `Database.Create` | admin on the database | — |
| `Database.NodeStatus` | read or stream per database listed, or fleet | — |
| `Database.WatchLeases` | read per database listed, or fleet | — |
| `Database.Verify` | read on the database, or fleet | — |
| `Database.Restore` | admin on the database | — |
| `Database.DescribeArchive` | read on the database, or fleet | — |
| `Database.VerifyArchive` | admin on the database | — |

## Observability

A running daemon offers three things an operator collects: metrics, health and readiness probes, and logs (ADR-0034).

### The admin listener

Apart from the gRPC port, the daemon serves HTTP/1.1 on `[admin] listen`, `127.0.0.1:7401` by default: `/metrics` in the OpenMetrics text format (`application/openmetrics-text; version=1.0.0`), `/healthz`, and `/readyz`. It answers `GET` and `HEAD` on those three paths, `405` for any other method, and `404` for any other path. `[admin] enabled = false` turns it off; the daemon keeps its metrics either way. It serves at most 64 connections at once and closes any beyond them as soon as it accepts them.

On loopback it serves plaintext, and every endpoint is open to whoever can connect to the machine. Any other address fails validation unless one of two fields says how to expose it:

- **`tls = true`.** The listener presents the node's own `[tls]` certificate, reloaded when its files change, and asks for a client certificate from the fleet's authority without requiring one. `/metrics` then answers only a principal listed in `[admin] scrapers`: `401` without a client certificate, `403` for any other principal or a node's certificate (logged on `fleetringd::audit`). `/healthz` and `/readyz` answer anyone, since a probe must learn whether the node is up. A scraper's certificate grants nothing on the gRPC port unless a `[[principal]]` entry also lists it.
- **`allow_plaintext = true`.** For a network you control, such as a pod network behind a network policy: every endpoint is open to whoever can connect, and the daemon warns at every start.

Inside a container the default loopback address is reachable only from the container, so set `listen` and one of the two. To let Prometheus scrape over TLS, issue it a client certificate and list it:

```sh
fleetringd cert client prometheus --dir ./tls
```

```toml
[admin]
listen = "0.0.0.0:7401"
tls = true
scrapers = ["prometheus"]
```

```yaml
scrape_configs:
  - job_name: fleetring
    scheme: https
    tls_config:
      ca_file: /etc/prometheus/fleetring/ca.crt
      cert_file: /etc/prometheus/fleetring/client.prometheus.crt
      key_file: /etc/prometheus/fleetring/client.prometheus.key
    static_configs:
      - targets: ["192.0.2.10:7401", "192.0.2.11:7401"]
```

The node certificate must name the address Prometheus dials (`cert node --addresses`).

### Health and readiness

`/healthz` answers `200` with `ok` while the process can serve, and `503` with `control writer stopped` once a control node's writer has stopped after a storage failure, the one state only a restart recovers from.

`/readyz` answers `200` when every check of the node's roles passes, and `503` otherwise, with every check in a JSON body: `{"ready": false, "checks": [{"name": "joined", "ok": false, "detail": "not joined to the fleet yet"}, …]}`. Every check reads local state; a probe never waits on another node.

| Check | Role | Passes when | Detail when it fails |
|---|---|---|---|
| `joined` | data | the node has joined the fleet | `not joined to the fleet yet` |
| `ring` | data | it has published a ring | `no ring published yet` |
| `denylist` | data | its token denylist is no older than the stale bound, so it accepts tokens | `no denylist yet`, or `denylist is <n> ms old; tokens are refused` |
| `draining` | data | the node isn't draining | `the node is draining` |
| `control_log` | control | the control log recovered and its writer runs | `the control writer stopped; restart the node` |
| `leader` | control | a control leader is known; always, until the Raft control group (FR-064) | — |

A draining node answers `503`, so a load balancer stops sending it work while its leases move. In Kubernetes:

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 7401 }
readinessProbe:
  httpGet: { path: /readyz, port: 7401 }
```

With `[admin] tls = true`, add `scheme: HTTPS` to both; the kubelet doesn't verify the certificate.

### Metrics

Every family carries the `fleetring_` prefix, lexicon words, and base units in its name (`_seconds`, `_bytes`); counters end in `_total`. Durations are histograms with buckets from 0.5 ms doubling to about 16 s. Families a node's roles never update stay at 0, or have no series; every family is listed from the first scrape. Renaming or removing a family is a breaking change, listed in the release notes.

| Family | Type | Labels | What it says |
|---|---|---|---|
| `fleetring_build_info` | gauge | `version`, `protocol` | The daemon's version and protocol version; always 1. |
| `fleetring_node_info` | gauge | `node_id`, `node_role`, `fleet` | This node's ID, node role, and fleet name; always 1. |
| `fleetring_rpc_requests_total` | counter | `service`, `method`, `code` | RPCs this node served, by final status code. |
| `fleetring_rpc_duration_seconds` | histogram | `service`, `method` | Time to serve unary RPCs, from request to final status. |
| `fleetring_rpc_in_flight` | gauge | `service`, `method` | RPCs being served now, streams included. |
| `fleetring_auth_refusals_total` | counter | `reason` | Requests refused at the request boundary or by a handler's scope or binding check. |
| `fleetring_denylist_age_seconds` | gauge | — | Seconds since the token denylist last arrived; +Inf (written `inf`) before the first. |
| `fleetring_denylist_entries` | gauge | — | Revoked tokens and signing keys on the denylist. |
| `fleetring_membership_epoch` | gauge | — | The membership epoch this node last published. |
| `fleetring_heartbeats_total` | counter | `outcome` | Heartbeats sent to the control plane, by outcome. |
| `fleetring_heartbeat_duration_seconds` | histogram | — | Time from sending a heartbeat to its reply. |
| `fleetring_members` | gauge | `liveness` | Members of the fleet, by liveness, as control state has them. |
| `fleetring_members_draining` | gauge | — | Members marked as draining. |
| `fleetring_databases` | gauge | `replica_role` | Databases this node hosts, by replica role. |
| `fleetring_databases_open` | gauge | — | Databases this node has open. |
| `fleetring_databases_under_replicated` | gauge | — | Databases this node is primary for with fewer replicas reachable than the replication factor. |
| `fleetring_leases_held` | gauge | — | Database leases this node holds. |
| `fleetring_writes_total` | counter | `outcome` | `Execute` calls, by outcome. |
| `fleetring_write_duration_seconds` | histogram | — | Time from receiving an acknowledged write to replying, write quorum included. |
| `fleetring_queries_total` | counter | `outcome` | `Query` calls, by outcome. |
| `fleetring_query_duration_seconds` | histogram | — | Time to answer a query that succeeded. |
| `fleetring_replication_batches_sent_total` | counter | — | Batches this node sent to followers. |
| `fleetring_replication_sent_bytes_total` | counter | — | Batch payload bytes this node sent to followers. |
| `fleetring_replication_batches_applied_total` | counter | — | Batches this node applied as a follower. |
| `fleetring_replication_snapshots_sent_total` | counter | — | Database snapshots this node served to followers. |
| `fleetring_replication_reseeds_total` | counter | — | Followers this node told to re-seed from a database snapshot. |
| `fleetring_replication_followers` | gauge | `state` | Followers of the databases this node is primary for, by state. |
| `fleetring_lease_renewals_total` | counter | `outcome` | Lease renewals this node asked for, by outcome. |
| `fleetring_lease_transfers_total` | counter | `outcome` | Leases this node handed over while draining, by outcome. |
| `fleetring_promotions_total` | counter | `outcome` | Promotions this node carried out as candidate, by outcome. |
| `fleetring_promotion_duration_seconds` | histogram | — | Time from a promotion notice to serving as primary. |
| `fleetring_control_commands_total` | counter | `action`, `outcome` | Control commands logged, by action and whether `apply` accepted them. |
| `fleetring_control_log_index` | gauge | — | The index of the last logged control command. |
| `fleetring_control_log_write_duration_seconds` | histogram | — | Time to log one group of control commands, its sync included. |
| `fleetring_control_mailbox_depth` | gauge | — | Control commands waiting for the writer. |
| `fleetring_control_snapshot_index` | gauge | — | The log index of the last control snapshot saved; 0 before the first. |
| `fleetring_control_promotions_in_progress` | gauge | — | Databases with a promotion in progress. |
| `fleetring_control_databases` | gauge | — | Databases control state records. |
| `fleetring_control_leader` | gauge | — | 1 if this node is the control plane's leader. |
| `fleetring_change_deliveries_total` | counter | `sink`, `outcome` | Change stream deliveries, by sink and outcome. |
| `fleetring_change_resets_total` | counter | `sink` | Times a sink's stream skipped ahead because the log moved past it. |
| `fleetring_change_backlog_batches` | gauge | `sink` | Batches published but not yet delivered, summed over databases. |
| `fleetring_change_backlog_batches_max` | gauge | `sink` | The largest backlog of any one database. |

Label values come from closed sets the code fixes, or from the node's own configuration:

- `service`: `control`, `replication`, `database`, or `unknown`; `method`: an RPC of the table above, or `unknown`; `code`: a gRPC status code in lower snake case (`ok`, `not_found`, `permission_denied`, …).
- `reason`: `scope`, `binding`, `unauthenticated`, `denylist_stale`, `no_requirement`.
- `outcome`: heartbeats `recorded`, `failed`; writes `acknowledged`, `refused`, `aborted`, `redirected`, `failed`; queries `ok`, `redirected`, `failed`; renewals `renewed`, `refused`, `failed`; transfers `completed`, `failed`; promotions `completed`, `superseded`, `failed`; control commands `applied`, `rejected`; change deliveries `delivered`, `failed`.
- `action`: a control command's action in snake case (`join`, `heartbeat`, `create_database`, …), or `other`.
- `state`: `caught_up`, `behind`, `disconnected`, `reseeding`; `replica_role`: `primary`, `follower`; `liveness`: `live`, `down`; `node_role`: `data`, `control`, `both`.
- `sink`: a `[[changes.sink]]` or `[[archive]]` entry's name.

No label is ever a database name, a principal, a token or key ID, a peer node, an address, or SQL. A node can hold 10,000 databases, and a database's name is tenant data, so per-database questions (which database, which follower, how far one sink is on one database) are answered by `fleetringd status --node <id>`, which scopes protect; spreads over databases are gauges a sampler refreshes every `[admin] sample_interval_ms` (10 s by default), as counts, sums, and maxima.

### Logs and request IDs

Log lines go to stderr, filtered by `RUST_LOG` (`info` for the daemon, `warn` for commands). `--log-format text|json`, then `FLEETRING_LOG_FORMAT`, then the daemon's `[log] format` choose the format; text is the default. JSON is one object per line, with the event's fields, its current span (`span`), and the spans it sits in (`spans`).

Every request runs in an `rpc` span with `service`, `method`, and `request_id`, and with `principal` or `node`, `database`, `code`, and `duration_ms` once they are known; audit lines sit inside it. Credentials are never span fields, but principals and database names are, so treat logs as sensitive (`SECURITY.md`). The request ID is the incoming `x-request-id` when it is 1 to 64 characters of `[A-Za-z0-9._-]`, and otherwise `<node-id>-<16 hex digits>` from a counter. The calls a node makes while serving the request, such as a data node's `AcquireLease` for a `Create`, carry the same `x-request-id`, so `grep` for one ID finds the request's lines on every node it reached. Work handed to another task (replication streams, change stream pumps) gets IDs of its own.

## Install

- **Release archives** for Linux (glibc 2.36 or newer), macOS, and Windows, on x86_64 and ARM64 (Windows on x86_64 only), are on the [releases page](https://github.com/fleetring/fleetring/releases), with a `SHA256SUMS` file.
- **The install script** picks the archive for the machine, verifies its checksum, and installs the binary:

  ```sh
  curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh | sh
  ```

- **The container image**, for `linux/amd64` and `linux/arm64`: `ghcr.io/fleetring/fleetringd`.

## Upgrading

**Which releases run together** (ADR-0038). A release's line is its `MAJOR.MINOR`. A fleet runs at most two adjacent lines at once, such as 0.3.x beside 0.4.x, and never 0.3.x beside 0.5.x, so upgrade one line at a time; patch releases of a line always mix. Every node reports its release, protocol, control-log format, and fleet features in `Join` and `Heartbeat`, and `fleetringd status` prints each member's `version=`. A node that reports nothing runs 0.3.0, the first release a rolling upgrade starts from. The control plane refuses a join outside the window with `FAILED_PRECONDITION`, naming both nodes and releases; the refused node keeps retrying and logs why. Moving from 0.2 to 0.3 isn't a rolling upgrade: upgrade every node, then reissue fleet-scoped tokens, as the 0.3.0 release notes say.

**New commands wait for every member.** A command a release adds that older members can't read or act on is a fleet feature; the first is `fleetringd node undrain <node-id>`, which clears a drain mark without a restart. While any member, live or down, runs a release without the feature, the control plane refuses the command with `FAILED_PRECONDITION` and names that member, rather than apply it on some nodes only. The first such command it accepts enables the feature for good, and from then on a node whose release lacks it can't join, and a control node on such a release couldn't read the control log: `fleetringd upgrade` refuses to install, or roll back to, a release that lacks an enabled feature, before it swaps anything. Until a new command is first used, rolling back is safe.

**A member that is gone for good holds the fleet back.** Down members still count: one that died on an old release keeps every new command refused and every upgrade past the next line out. `fleetringd node remove <node-id>` takes it out of the fleet, as a node that leaves on shutdown does, and its replicas are placed elsewhere. It refuses a live member unless `--force` (a live node joins again at its next heartbeat anyway), and asks first on a terminal; `--yes` skips the question.

**`fleetringd upgrade`.** It needs `cosign` 2.4.2 or newer on `PATH` ([install it](https://docs.sigstore.dev/cosign/system_config/installation/)), and refuses to install anything without it.

```sh
fleetringd upgrade --check --config /etc/fleetring/fleetringd.toml   # what's newer, and does the fleet allow it?
fleetringd upgrade --config /etc/fleetring/fleetringd.toml           # the newest release, then restart this node
fleetringd upgrade --version 0.4.1 --config /etc/fleetring/fleetringd.toml
fleetringd upgrade --rollback --config /etc/fleetring/fleetringd.toml
```

1. It finds the newest final release on GitHub, or the one `--version` names (naming the installed one reinstalls it), for the platform the binary was built for. `--base-url <url>` reads a mirror laid out as `install.sh --base-url` expects and needs `--version`; `--releases-url <url>` reads a mirror's copy of GitHub's releases list.
2. With `--config` (or `--control`), it asks the control plane for every member's release and the fleet features it has enabled, and refuses a release the window forbids, before downloading anything.
3. It downloads the archive, `SHA256SUMS`, and `SHA256SUMS.sigstore.json`, and verifies the signature with `cosign verify-blob` against the release workflow's identity (the one [`RELEASING.md`](https://github.com/fleetring/fleetring/blob/main/RELEASING.md#verify-what-was-published) names), then the archive's checksum, then that the binary inside says it is the release asked for, and that it supports every enabled fleet feature (`fleetringd upgrade --software` prints what a binary supports). Any failure refuses, says why, and changes nothing.
4. It writes the new binary beside the installed one and renames it into place, so the path always holds a whole binary, and keeps the installed one as `fleetringd.previous`.
5. With `--config` naming a running node, and unless `--no-restart`, it drains the node, restarts it in place, waits until the control plane lists it as live on the new release, and waits until each of its replicas reaches the position its primary had when it rejoined. `--timeout` bounds each step (5 minutes by default).

`--check` prints the installed release, the newest one (or the one `--version` names), and whether the fleet allows it, and changes nothing; it exits 1 when the fleet doesn't allow it. Once the fleet has enabled a feature, checking a release older than the installed one downloads and verifies it, since only its binary says which features it supports. The command acts as your principal, like every other command, so drain and status need the `fleet` scope. Upgrade the data nodes one at a time, then the control node, each after the last has rejoined.

**Restarting in place.** A running daemon writes `<data_dir>/fleetringd.process` with its process ID and binary. `SIGHUP` restarts it: it drains as a stop does, stays a member, and executes the same binary path with the same arguments, so it keeps its process ID and loads the new binary.

- **systemd:** run `fleetringd upgrade --config …` as the binary's owner (usually root). The unit needs no change, since the main process ID stays the same. To restart through systemd instead, pass `--no-restart` and run `systemctl restart fleetringd`; the stop drains and leaves, so give `TimeoutStopSec` room for a drain.
- **The container image:** the binary belongs to root and the daemon runs as UID 65532, so `fleetringd upgrade` can't replace it, and the image is the release anyway. Upgrade by running the new tag on the same volume: `docker stop --time 60 fleetringd` (SIGTERM drains and leaves), then `docker run` `ghcr.io/fleetring/fleetringd:X.Y.Z` as before. `fleetringd upgrade --check` works from anywhere.
- **A plain process:** `fleetringd upgrade --config …` restarts it in place in its terminal; or press Ctrl-C and start the new binary. Closing the terminal sends the daemon `SIGHUP` too, which now restarts it rather than stopping it; run it under `nohup` or a supervisor, or stop it with Ctrl-C first, if the terminal may close.

A daemon that wrote no process file, such as 0.3.0, can't restart in place: the command replaces the binary and says to restart the node with its supervisor.

**Rolling back.** `fleetringd upgrade --rollback --config …` swaps `fleetringd` and `fleetringd.previous` and restarts the node as an upgrade does; a second rollback rolls forward again. If the new binary doesn't run at all, `fleetringd.previous upgrade --rollback` does the same. A release without the command, such as 0.3.0, rolls back by hand: `mv fleetringd.previous fleetringd`, then restart. A rollback to a release that lacks an enabled fleet feature is refused before the swap, naming the feature: the fleet never gives a feature up.

**By hand**, without the command, for each node in turn, the control node last:

1. Check what the fleet runs (`fleetringd status`) and the release notes.
2. Download the release and verify it as [`RELEASING.md`](https://github.com/fleetring/fleetring/blob/main/RELEASING.md#verify-what-was-published) shows, or with `install.sh` on a machine with cosign.
3. Drain the node: `fleetringd node drain <node-id>`, then wait until `fleetringd db list --node <node-id>` lists nothing.
4. Replace the binary by writing the new one beside it and renaming it into place, as `install.sh` does.
5. Restart the node: `systemctl restart fleetringd`, a new container, or Ctrl-C and start. The stop drains again and leaves; the node rejoins when it starts.
6. Wait until `fleetringd status` lists the node as live on the new version, and `fleetringd db locate` shows its replicas at their primaries' positions, before the next node.

## License

Dual-licensed under MIT (`LICENSE-MIT`) or Apache-2.0 (`LICENSE-APACHE`), at your option.

[fleetring.dev](https://fleetring.dev) · [GitHub](https://github.com/fleetring/fleetring)
