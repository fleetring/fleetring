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

**Drain.** `fleetringd node drain <id>` hands every database the node is primary for to a follower, through the same epoch and fencing rules as a failover but without waiting for a lease to expire: clients see a brief `UNAVAILABLE` and a redirect instead of a failover's pause. A database with no other replica has nowhere to go; the node logs `no other replica; the database is unavailable until this node returns` for it and keeps serving it until the node stops. The node gets no new primaries afterwards and stays a follower until it stops or restarts. A graceful stop (`Ctrl-C`, SIGTERM) with `[membership] on_shutdown = "leave"`, the default, drains the same way before it leaves; `on_shutdown = "stay"` keeps the node's leases for a quick restart and does neither.

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

## Install

- **Release archives** for Linux (glibc 2.36 or newer), macOS, and Windows, on x86_64 and ARM64 (Windows on x86_64 only), are on the [releases page](https://github.com/fleetring/fleetring/releases), with a `SHA256SUMS` file.
- **The install script** picks the archive for the machine, verifies its checksum, and installs the binary:

  ```sh
  curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh | sh
  ```

- **The container image**, for `linux/amd64` and `linux/arm64`: `ghcr.io/fleetring/fleetringd`.

## License

Dual-licensed under MIT (`LICENSE-MIT`) or Apache-2.0 (`LICENSE-APACHE`), at your option.

[fleetring.dev](https://fleetring.dev) · [GitHub](https://github.com/fleetring/fleetring)
