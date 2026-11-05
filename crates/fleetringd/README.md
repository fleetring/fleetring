# fleetringd

The FleetRing daemon. It places, replicates, and fails over SQLite databases across machines you own. One binary runs a data node, the control node, or both (a one-machine fleet), and its subcommands talk to a running fleet.

> **Status:** early development. Not production-ready.

## Dev mode

`fleetringd dev` runs one node on a laptop with no setup ([ADR-0039](https://github.com/fleetring/fleetring/blob/main/docs/adr/0039-dev-mode.md)):

```sh
fleetringd dev                           # in one terminal; Ctrl-C stops it

. ~/.fleetring/env                       # in another: points the commands at it
fleetringd status
fleetringd db create notes               # one replica, a dev node's default
fleetringd db exec notes "CREATE TABLE note (body TEXT)"
fleetringd db exec notes "INSERT INTO note VALUES (?)" "hello"
fleetringd db query notes "SELECT body FROM note"
tail -f ~/.fleetring/changes/notes.ndjson   # every database's changes, one JSON line per transaction
```

The first run creates, in `~/.fleetring` (or `--data-dir <dir>`): an authority for the fleet `dev` (`tls/ca.crt`, `tls/ca.key`), a certificate for the node `dev` at `127.0.0.1`, `::1`, and `localhost`, a certificate for the principal `admin`, which the configuration gives every scope, and `fleetringd.toml`. Certificates last a year. Later runs reuse every file and never overwrite one: a missing certificate is issued again from the existing authority, and anything that can't be completed safely, such as `ca.crt` without `ca.key` or an expired certificate, fails naming the file and what to do. A directory that isn't dev mode's, one whose `fleetringd.toml` lacks `dev = true` or that holds anything but `tls/`, `data/`, `changes/`, and `env`, is refused before anything is written to it. No token is stored; the admin certificate needs none, and `fleetringd token issue --dir ~/.fleetring/tls` issues one for an application. On Unix the directories are `0700` and the keys `0600`.

The node serves gRPC on `127.0.0.1:7400` and its admin listener on `127.0.0.1:7401`; `--port <n>` picks another port on the first run, and the admin listener takes the next one. It is the normal daemon running `~/.fleetring/fleetringd.toml`, so `fleetringd --config ~/.fleetring/fleetringd.toml` runs the same node, and the file may be edited: `dev = true` in it makes the daemon refuse any `listen`, `node.advertise`, `control.address`, or `[admin] listen` that isn't loopback, any role but `both`, and plaintext, however it is started. The configuration keeps the node's databases placed on it across a stop (`[membership] on_shutdown = "stay"`), creates databases with one replica unless `db create` names a `--replication-factor` (`[replication] replication_factor = 1`), and sends every database's row changes to a file sink, `dev`, in `~/.fleetring/changes/`.

`~/.fleetring/env` sets `FLEETRING_CONTROL` (the control address, which `--control` and `--config` override), `FLEETRING_TLS_DIR`, and `FLEETRING_AS=admin`; dev mode rewrites it at every start. `status` against a dev node starts with `dev mode: one node on loopback, for development only`, and doesn't warn that its one-node ring has fewer nodes than a fleet's replication factor. A dev node holds one copy of each database, so a larger `--replication-factor` leaves writes short of their quorum; `db create` accepts it and warns.

To run it at login, [`deploy/dev/`](https://github.com/fleetring/fleetring/tree/main/deploy/dev) has a Homebrew `service do` block (`homebrew-service.rb`, with the launchd agent it becomes) and a systemd user unit, `fleetringd-dev.service`. The authority's key and the admin key sit unencrypted in the directory: dev mode is for local development only ([SECURITY.md](https://github.com/fleetring/fleetring/blob/main/SECURITY.md#dev-mode)).

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

**Statements.** `db exec` and `db query` run SQL against the one database they name, and statements are confined to it: a statement that attaches or detaches a database, loads an extension, or runs a pragma other than read-only introspection (`table_info`, `table_xinfo`, `index_list`, `index_info`, `index_xinfo`, `foreign_key_list`, `foreign_key_check`, `integrity_check`, `quick_check`, `compile_options`, and the read forms of `user_version`, `application_id`, `schema_version`, `page_size`, `page_count`, `freelist_count`, and `data_version`, which FTS5 runs to search and update a full-text table) is refused with `INVALID_ARGUMENT`. The daemon owns journaling, syncing, and checkpoints.

`db list` shows every database with its engine, epoch, replication factor, primary, and replicas, and ends with how many databases each node is primary for; `--node <id>` keeps the databases one node is primary for. `status --address <host:port>` asks one data node what it believes on its own, so its ring fingerprint can be compared with `status`; `status --node <id>` does the same, finding the node's address in the control plane's membership. `--node` always takes a node ID, and `--address` an address: `db exec` and `db query` take either, to send a statement through a particular node. `db drop` removes a database for good: every replica deletes its copy within a few seconds. It asks first on a terminal; `--yes` skips the question. The primary learns of the drop at its next lease renewal and stops admitting writes; before it deletes its copy, it flushes the database's change streams for up to 30 seconds, so an archive receives every write the database acknowledged (see Expiry and Backups and restore, below). Until the old copy is deleted, that node answers a `db create` or `db restore` of the same name with `UNAVAILABLE`, which the commands retry for up to 45 seconds. A node deletes a copy only after it saw the drop itself: one holding a copy of an earlier database of the name that it didn't see dropped, as after `control.db` was lost and the name created again, keeps it and refuses the creation with `FAILED_PRECONDITION`, naming the copy; restarting that node deletes it (ADR-0041, amendment 7). A follower whose primary was down when the drop happened keeps its copy until it restarts (ADR-0041, amendment 7).

Each database chooses its durability when it is created (ADR-0023). `--acks quorum` (the default) acknowledges a write once a majority of its replicas hold it; `--acks all` waits for every replica, so one replica down blocks writes; `--acks local` acknowledges after the primary's own fsync, which is fastest but may lose acknowledged writes when the primary fails over. `--min-replicas <n>` sets the floor: a write that arrives while fewer than `n` replicas, the primary included, are reachable is refused with `UNAVAILABLE` before anything commits, so it can be retried cleanly; it must lie between the write quorum and the replication factor, and defaults to the write quorum. `db locate` and `db list` show the settings, `db set-durability` changes them from the database's next epoch on, and `[replication] acks` and `min_replicas` in the control node's configuration set the defaults for databases created without them.

The replication factor is chosen at creation too. `db create` and `db restore` without `--replication-factor` take the fleet's default, `[replication] replication_factor` in the control node's configuration: 3 unless it says otherwise, and 1 on a dev node. The control node fills it in before it logs the creation, so every database records its own factor, and changing the default later changes no existing database. A factor larger than the fleet's data nodes is accepted, since nodes may join later and placement fills the replica set as they do; `db create` then warns on stderr, naming both numbers and whether writes can reach their floor meanwhile. A control node before 0.5.0 has no default: the commands then ask it for 3, as they did before.

**Database metadata.** A database may carry a description, labels, and an expiry, kept in control state beside its placement, so that another agent or a person can find it, tell what it holds, and know when it goes away (ADR-0041):

```sh
fleetringd db create notes --description "Memory for agent 42.
Tables: facts, episodes." --label purpose=memory --label agent=42
fleetringd db list --label purpose=memory          # or --label purpose, for any value; --prefix agent-42-
fleetringd db show notes                           # placement, description, labels, creator, expiry, version, schema
fleetringd db show notes --json                    # one object: database, placement, metadata, schema
fleetringd db show notes --no-schema --count-limit 0 --all
fleetringd db set-labels notes --label task=7 --remove agent
fleetringd db set-description notes --description - --if-version 2 < description.txt
fleetringd db set-description notes --clear
fleetringd db create agent-42-task-7f3a --expires-in 24h --label purpose=task
fleetringd db set-expiry agent-42-task-7f3a --expires-in 48h   # or --expires-at <RFC 3339>, or --never
```

A description is at most 1 KiB of text: tab and line feed are allowed, while other control characters, invisible format characters (zero-width characters, bidirectional overrides, tag characters), and the line and paragraph separators are refused. `--description -` reads it from stdin, without its trailing line feeds. Labels follow the Kubernetes rules: a key is an optional DNS-subdomain prefix and a name of up to 63 letters, digits, `-`, `_`, and `.`, starting and ending with a letter or digit, and a value is empty or a name; keys under `fleetring.dev/` are reserved, and a database has at most 32. The control node records who created the database (`creator`) and who last set the description (`description by`), from the request's caller, never from the request; `db show` prints a description as a block, every line indented and prefixed with `| `, and marks one that another principal wrote, or that nobody can be named for, as untrusted. Updates are patches: `db set-labels` changes only the keys it names, so two agents labelling one database both succeed, and `--if-version <n>` refuses the change, naming the version, if the metadata moved on since `db show` printed version `n`. `db list --label` filters among the databases the caller may read, so it never reveals another's names or labels. Changing metadata needs `admin` on the database, as creating and dropping it do.

**Schema.** `db show` also asks the database's primary for its schema (ADR-0041): each table with its row count, then its indexes and triggers, then the views and virtual tables, each with the engine's own definition (for SQLite, the `CREATE` statement). It needs only `read` on the database. Row counts are exact up to a limit and a lower bound beyond it: `--count-limit <n>` (default 10000; 0 counts nothing; the command refuses more than 1,000,000, which is also the most rows one request counts across all tables, leaving the tables after that uncounted) prints a larger table as `more than 10000 rows`, and views and virtual tables, such as full-text indexes, aren't counted. The count reads through the read connection, so it holds no write lock, but the database's thread does the counting: **writes queued on that database wait for it**, for as long as the scan takes, which the budget of 1,000,000 rows per request bounds. The list stops at 1,000 objects, saying so. SQLite's own tables and a virtual table's shadow tables are left out unless `--all`; the daemon's reserved `_fleetring_` objects are never shown. A definition is text the database's writers chose, so it prints as a block like a description, every line escaped, indented, and prefixed with `| `, and `--json` carries it unchanged in `schema.objects[]` (`kind`, `name`, `table`, `definition`, `rows`, `rows_at_least`). `--no-schema` skips the call. A primary that runs an older release (0.5 or before the schema call) can't answer it: `db show` then prints the placement and metadata and `schema: unavailable (the primary runs an older fleetringd, which can't report schemas)`, with the reason in `schema_unavailable` in JSON.

Metadata is a fleet feature, `metadata` (see [Upgrading](#upgrading)): until every member runs a release that supports it, `db create --description` or `--label`, `db set-description`, and `db set-labels` are refused with `FAILED_PRECONDITION` naming the member that lacks it, before anything is logged, and a plain `db create` works as before. A plain `db create` records its creator only once the feature is enabled, so the first ordinary creation after an upgrade doesn't close the rollback window. A control node of a release without metadata refuses the calls outright, and the commands say `the control plane runs a release without database metadata; upgrade it first`.

**Expiry.** `--expires-in <n>d|h|m|s` or `--expires-at <RFC 3339>` on `db create` and `db restore`, and `db set-expiry`, give a database an expiry: the time after which the control plane drops it, exactly as `db drop` does. A duration counts from the control node's clock, or from the control plane's latest command if that is later, and the control node turns it into a time before it logs the command, so the log holds only times; an expiry less than a minute ahead is refused with `INVALID_ARGUMENT`, and one the control plane's time passed before it was logged with `UNAVAILABLE`, which a retry resolves. `db set-expiry --never` removes it, and a later time extends it: an extension made after the control node looked still wins, since the command that drops the database is judged at its own time. `db show` prints `expires: <time> (in 23h 59m)`, or `(expired 5s ago, dropping)` in the moment before the drop. Once a second, the control node's expiry sweep drops at most 64 of the databases whose expiry has passed, each with a command of its own; a control node that was down drops everything that expired meanwhile once it is back. Expiry is a fleet feature of its own, `expiry`, which needs `metadata` too: until every member reports it, `--expires-in`, `--expires-at`, and `db set-expiry` are refused as metadata is.

What an expiry keeps is what a drop keeps. The primary flushes the database's change streams before it deletes its copy, for up to 30 seconds, and an archive puts its open segment at once, whatever its `segment_interval_ms`: with a flush that completes, the archive holds every write the database acknowledged, and `db restore --from` brings it back. A flush that runs out of time, or a primary that is down or cut off when the drop happens, leaves the archive as of its last archived segment, and the writes acknowledged after it are lost; the primary logs `the dropped database's change streams didn't catch up in time` when that happens. Without an archive, an expiry loses the database entirely. Set expiries generously and extend them while the work goes on; leave a scratch prefix out of `[[archive]] databases` to keep it out of archives.

The expiry is judged by the control plane's time, which never moves back: a database can expire early by the largest lead over real time that any control node's clock has ever had, and a control node whose clock jumps forward drops everything due within the jump at once. The one-minute minimum doesn't bound this. Expiry never decides safety, which comes from epochs and quorums; keep control nodes' clocks synchronized, and keep an archive for any database whose loss would matter. A dev node stopped over a weekend drops the databases that expired meanwhile when it starts again.

**Drain.** `fleetringd node drain <id>` hands every database the node is primary for to a follower, through the same epoch and fencing rules as a failover but without waiting for a lease to expire: clients see a brief `UNAVAILABLE` and a redirect instead of a failover's pause. A database with no other replica has nowhere to go; the node logs `no other replica; the database is unavailable until this node returns` for it and keeps serving it until the node stops. The node gets no new primaries afterwards and stays a follower until it stops or restarts, or until `fleetringd node undrain <id>` clears the mark once every member supports it (see [Upgrading](#upgrading)). A graceful stop (`Ctrl-C`, SIGTERM) with `[membership] on_shutdown = "leave"`, the default, drains the same way before it leaves; `on_shutdown = "stay"` keeps the node's leases for a quick restart and does neither. `SIGHUP`, which a closing terminal also sends, doesn't stop the daemon: it restarts it in place (see [Upgrading](#upgrading)).

**Change streams.** A `[[changes.sink]]` entry in a data node's configuration sends every acknowledged batch of the databases it names to a sink: a `file` sink appends one JSON line per batch to a file per database, and a `webhook` sink POSTs each batch as JSON. `format = "logical"` (the default) sends the rows each transaction inserted, updated, or deleted; `format = "physical"` sends the opaque batch for a mirror or an archive. A logical sink's `capture` says how much of each row it gets: `id` the key alone, `after` (the default) the key and the new values of inserted and updated rows, `full` also the old values of updated and deleted rows and the names of the updated columns; a database named by several sinks records in the strongest mode asked for. `databases` lists exact names or prefixes ending in `*`, such as `["tenant:*", "ledger"]`. A webhook's `url` is `https://`, over TLS 1.2 or 1.3, and trusts the platform's root certificates as an `[[archive]]` does; `ca = "<path>"` adds the authorities in a PEM bundle to them, for a receiver with a private certificate, and doesn't replace them. For a receiver that asks for a client certificate, `cert` and `key` (PEM files, both or neither) present one. A bearer token is sent as `Authorization: Bearer <token>` when `token_file` (a file only its owner may read, which the daemon refuses otherwise) or `token_env` (an environment variable) names it, at most one; the token is read when the sink starts, is never in the configuration file, is zeroed in memory when dropped, and is never logged or printed. A certificate the sink doesn't trust, or any other TLS or connection failure, is a failed delivery like a `5xx`: the batch is retried with backoff and never dropped or skipped. An `http://` `url` fails configuration validation unless the sink sets `allow_http = true`, because every change would otherwise leave the node in plaintext; since 0.5.0 an existing `http://` webhook needs it. Sinks run beside replication and only see batches the write quorum holds, so a slow or failing sink never delays a write; delivery is at least once, in commit order. Row changes are recorded inside the database, in a table named `_fleetring_cdc` that replicates with the data so a promoted follower continues the stream, and so is each sink's position, a mark in a table named `_fleetring_sinks` that the database's next write carries after a delivery: a new primary resumes each stream at the newest mark the write quorum holds, and a failover repeats only what the old primary delivered after it, usually the last batch or two, never a gap. An engine that can't keep marks inside its database keeps positions under `<data_dir>/changes/` on that node alone, and a failover then repeats everything since the last checkpoint. `status --node` (or `--address`) shows each sink's position, whether it is `replicated` or `local`, and its backlog. Configure the same sinks on every node that may become a primary. Names beginning with `_fleetring_` are reserved, and `db exec` and `db query` refuse statements that touch them. Schema changes aren't streamed.

**Following leases.** `fleetringd lease watch --node <id>` (or `--address <host:port>`) follows the leases one data node holds, from the node's own view: it prints `held <database> epoch <n>` for each database the node serves when the stream opens, then `gained <database> epoch <n>` and `lost <database> epoch <n>` lines as leases move, until the stream ends. The node reports a lease before it acknowledges a write under that epoch, and reports its loss before it stops answering for the database, so a worker beside the node (a queue consumer, a scheduler, a cache warmer) can act on exactly the databases the node is primary for and stop the moment one moves, without polling. Events for a database arrive in epoch order; a subscriber that falls behind is cut off with `RESOURCE_EXHAUSTED` and opens the stream again, which starts with the leases held then. The stream is one node's; a client that wants every node subscribes to each.

**Verifying replicas.** Every replica keeps a hash chain over the replication log: one BLAKE3 hash per transaction, each folding in the hash before it, so the hash at a position commits every byte of the log before it. The primary computes each batch's hash as it ships the batch, and a follower recomputes it before applying the batch; one that doesn't match is refused, and the follower re-seeds from a snapshot rather than apply a log that isn't the primary's. `fleetringd db verify <database>` asks every replica for its hash at the latest position they all hold, each recomputed from the replica's log on disk, and prints the one hash and `all 3 replicas agree`, or names the first replica whose hash differs or whose log can no longer be read back to that position. `--at <position>` picks a position `db locate` printed instead, and `--expect <hash>` reports `nothing changed since <position>` when the log still hashes to a hash recorded earlier. The chain covers the log, not pages already checkpointed into the database file (FR-045), and proves consistency, not provenance: it isn't signed.

**Backups and restore.** Replicas don't protect against a bad `DROP TABLE`, which replicates in a round trip, or against losing the fleet; an archive does (ADR-0029). An `[[archive]]` entry in a data node's configuration writes the databases it names to an object store outside the fleet: an S3-compatible bucket (AWS S3, MinIO, Cloudflare R2, Garage) or a directory, such as a mounted volume. An archive is a physical change stream: each primary writes one immutable segment per delivery, holding every batch the write quorum acknowledged since the last one, at most one every `segment_interval_ms` (10 s by default), plus a base snapshot when the archive has no path to a delivery's start and then once a day or every GiB of segments. Its recovery point is its lag, which `status --node` shows as the archive's backlog like any sink's; a write never waits for the archive. Every segment records the hash chain at both its ends, and a restore recomputes the chain over every batch it applies, so a restored copy is proven to hold the log the fleet held; a segment or snapshot chunk that changed in the store fails the restore with `DATA_LOSS` naming the object, and a missing segment is refused rather than skipped. Retention keeps the newest `keep_snapshots` base snapshots (7) and the segments that continue them. Configure the same entries on every node that may become a primary: a new primary continues the archive from the mark the old one replicated, without a new snapshot. A dropped or expired database keeps its archive, and its primary puts the open segment at once before it deletes its copy (see Expiry, above).

`fleetringd db restore <new> --archive <name> --from <source>` creates a new database from the archive's copy of `source`, which needn't exist any more: the first live member of the new database's replica set restores it, takes the first lease, and serves it, and its followers seed from it. `--at-position <hex>` restores exactly the log up to a transaction boundary, and `--at-time <RFC 3339>` every segment archived at or before that time, by the archiving nodes' clocks: a time only picks which acknowledged prefix is restored, and the restore may stop up to one segment interval before it. A restore never overwrites a database; to restore under the old name, drop it first. It needs `admin` on both the new database and the source. `fleetringd db archive show <source> --archive <name>` lists the archive's snapshots and how far its segments reach, and `db archive verify`, which needs `admin` on the database since it costs a data node a whole copy on its disk, restores into a scratch copy there, verifying every batch, then asks each live replica for its hash at the restored position and prints `archive agrees with every replica` or the replicas that differ.

**Metadata in archives.** A base snapshot of a database that has metadata (a description, labels, a creator) is archived with one more object, `<prefix>/.metadata/<db>/<snapshot id>_<checksum>.json`, written before the snapshot's descriptor. It sits outside the database's own `<prefix>/<db>/`, so a release that predates it never lists it: no damaged entries in `db archive show`, and nothing left behind by its retention. Retention in this release deletes a snapshot's metadata object with the snapshot, and any object whose snapshot is gone and older than the oldest kept one. The primary reads the metadata from the control plane when it completes the snapshot; if that fails, the snapshot is archived without it and the next one carries it, so an archive's metadata is as of its newest snapshot. `<checksum>` is the object's XXH3-128, which a restore checks: an object that is missing, damaged, too large (64 KiB; a description is at most 1 KiB and a database has at most 32 labels), or of another database is skipped with a warning and the data still restores. The checksum detects damage and nothing more: archives aren't signed, and whoever can write the bucket can write the object and its checksum too. `db archive show` prints `metadata: yes` or `no` for each snapshot.

`db restore` sets the archived labels and description on the new database, and each `--label` or `--description` you give wins over the archive's, per key. **A restored description records no author**, whoever restores it and whoever the archive says wrote it, so every reader treats it as untrusted; one given with `--description` is attributed to you as usual. The restore's report prints the archived description as a block, and the archive's claims about its author and creator (`archived description by: … (the archive's claim; not recorded)`), which are kept nowhere. The creator is the principal that restored, and **the expiry is never restored**: `--expires-in` or `--expires-at` gives the new database one, set last, after the archived description and labels and only once the restore has returned, so a short one can't drop it mid-restore. If setting it fails, the database is restored without one, and the command prints `warning: the database was restored without its expiry: <reason>` with the `db set-expiry` command that sets it, and exits 1; that exit wins when the archived metadata failed too, whose failure alone is a warning and exits 0. If the fleet can't take metadata (a member lacks the `metadata` feature, or the control node runs a release without it), the database is restored and the command prints `warning: the archived description and labels weren't restored: <reason>`. An archive written before this release restores as it always did. Offline, `archive restore` writes the object, byte for byte, as `<dir>/metadata.json` beside `data.db`.

A data node runs one restore or verification at a time, and answers another with `UNAVAILABLE` until it is done. For disaster recovery, with no fleet at all, `fleetringd archive restore --config <file> --archive <name> --from <source> --to <dir>` reads the store from a configuration file's `[[archive]]` entry and writes `<dir>/data.db`, a plain SQLite database that `sqlite3` opens directly. It takes the same `--at-position` and `--at-time`.

S3 credentials are static keys: `credentials = "env"` (the default) reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN` when the daemon starts; `credentials = "file"` reads a TOML file with `access_key_id`, `secret_access_key`, and optionally `session_token`, which only its owner may read, and is re-read when it changes. Secrets are never logged. `endpoint` defaults to AWS's for the `region`; `path_style = true` suits MinIO; `ca` adds a private endpoint's authority to the platform's; and `allow_http = true` permits a plain `http://` endpoint, for a local MinIO only. The store must give read-after-write and list-after-write consistency, as these services do. An archive holds every row of the databases it names, unencrypted: grant its bucket the same care as the nodes' disks. `deploy/fleetringd.toml` documents every setting.

The subcommands take `--control <host:port>`, the address of the node that runs the control role, or `--config <file>` to read it from a daemon's configuration, or else `FLEETRING_CONTROL`. A control call sent to a data-only node answers with the control node's address.

**Plaintext.** A daemon without a `[tls]` table refuses to start. `--insecure-plaintext` (or `insecure_plaintext = true` in the file) lets it run without one, warning at every start, and the subcommands take the same flag to connect to it; nothing on the wire is then authenticated or encrypted, so it is for local experiments only. A node whose certificate names another ID is refused on `Join` and `Heartbeat` with `PERMISSION_DENIED`, and only nodes may make node-to-node calls such as replication. In plaintext no certificate names the caller, so a request without a token may do anything, and a token is refused, since there is no authority to verify it against.

`fleetringd --help` lists every flag and subcommand. [`deploy/fleetringd.toml`](https://github.com/fleetring/fleetring/blob/main/deploy/fleetringd.toml) documents the main settings, including a multi-node fleet's.

## Principals and scopes

Every request comes from a node or a principal (ADR-0025). Nodes, named by their certificates, make the node-to-node calls, and act on themselves only otherwise: a node may follow the membership, leave or drain itself, locate databases, and hand over leases it holds, and nothing more; it can't read, write, or administer a database, drain or remove another member, or revoke a token (ADR-0026). A principal may do what its scopes allow and nothing else: a request no scope allows is refused with `PERMISSION_DENIED`, before it touches any state, with a message that names the action and the database it needed, never the caller's scopes.

A scope is `<action>:<pattern>`: the action is `read`, `write`, `admin`, or `stream`, and the pattern an exact database name or a prefix ending in `*` (`read:tenant:acme/*`, `write:notes`, `admin:*`). `write` implies `read`, and `admin` (create, drop, describe and label, change durability, hand a lease over) implies both; `stream` stands alone, so a change consumer can't read tables. The word `fleet` is the scope for operations that name no database: membership, draining, revoking tokens.

A prefix is bytes, not a path: `admin:agent-42*` covers `agent-42-notes`, and `agent-420` too. End every prefix with a separator, as in `admin:agent-42-*` or `write:tenant:acme/*`; `token issue` warns on stderr, and the daemon logs a warning when it loads a `[[principal]]` entry, for a prefix whose last character before `*` is a letter or digit (ADR-0041, point 5). Such a principal needs no `fleet` scope to manage its own databases: with `admin:agent-42-*` alone it creates, describes, labels, uses, lists, shows, and drops `agent-42-*` databases, and is refused everything else.

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
| `Control.CreateDatabaseWithMetadata` | admin on the database | — |
| `Control.UpdateDatabase` | admin on the database | — |
| `Replication.Replicate` | node | holder or candidate |
| `Replication.Fence` | node | candidate |
| `Replication.Snapshot` | node | follower |
| `Replication.Fetch` | node | candidate |
| `Replication.Promote` | node | candidate |
| `Database.Execute` | write on the database | — |
| `Database.Query` | read on the database | — |
| `Database.Schema` | read on the database | — |
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

**New commands wait for every member.** A command a release adds that older members can't read or act on is a fleet feature; the first is `fleetringd node undrain <node-id>`, which clears a drain mark without a restart. While any member, live or down, runs a release without the feature, the control plane refuses the command with `FAILED_PRECONDITION` and names that member, rather than apply it on some nodes only. The first such command it accepts enables the feature for good, and from then on a node whose release lacks it can't join, and a control node on such a release couldn't read the control log: given `--config` or `--control`, so that it can ask the control plane, `fleetringd upgrade` refuses to install, or roll back to, a release that lacks an enabled feature, before it swaps anything. Without either it can't check the fleet, so pass one. Until a new command is first used, rolling back is safe.

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
3. It downloads the archive, `SHA256SUMS`, and `SHA256SUMS.sigstore.json`, and verifies the signature with `cosign verify-blob` against the release workflow's identity (the one [`RELEASING.md`](https://github.com/fleetring/fleetring/blob/main/RELEASING.md#verify-what-was-published) names), then the archive's checksum, then that the binary inside says it is the release asked for, and, with `--config` or `--control`, that it supports every enabled fleet feature (`fleetringd upgrade --software` prints what a binary supports). Any failure refuses, says why, and changes nothing.
4. It writes the new binary beside the installed one and renames it into place, so the path always holds a whole binary, and keeps the installed one as `fleetringd.previous`.
5. With `--config` naming a running node, and unless `--no-restart`, it drains the node, restarts it in place, waits until the control plane lists it as live on the new release, and waits until each of its replicas reaches the position its primary had when it rejoined. `--timeout` bounds each step (5 minutes by default).

`--check` prints the installed release, the newest one (or the one `--version` names), and whether the fleet allows it, and changes nothing; it exits 1 when the fleet doesn't allow it. Once the fleet has enabled a feature, checking a release older than the installed one downloads and verifies it, since only its binary says which features it supports. The command acts as your principal, like every other command, so drain and status need the `fleet` scope. Upgrade the data nodes one at a time, then the control node, each after the last has rejoined.

**Restarting in place.** A running daemon writes `<data_dir>/fleetringd.process` with its process ID and binary. `SIGHUP` restarts it: it drains as a stop does, stays a member, and executes the same binary path with the same arguments, so it keeps its process ID and loads the new binary.

- **systemd:** run `fleetringd upgrade --config …` as the binary's owner (usually root). The unit needs no change, since the main process ID stays the same. To restart through systemd instead, pass `--no-restart` and run `systemctl restart fleetringd`; the stop drains and leaves, so give `TimeoutStopSec` room for a drain.
- **The container image:** the binary belongs to root and the daemon runs as UID 65532, so `fleetringd upgrade` can't replace it, and the image is the release anyway. Upgrade by running the new tag on the same volume: `docker stop --time 60 fleetringd` (SIGTERM drains and leaves), then `docker run` `ghcr.io/fleetring/fleetringd:X.Y.Z` as before. `fleetringd upgrade --check` works from anywhere.
- **A plain process:** `fleetringd upgrade --config …` restarts it in place in its terminal; or press Ctrl-C and start the new binary. Closing the terminal sends the daemon `SIGHUP` too, which now restarts it rather than stopping it; run it under `nohup` or a supervisor, or stop it with Ctrl-C first, if the terminal may close.

A daemon that wrote no process file, such as 0.3.0, can't restart in place: the command replaces the binary and says to restart the node with its supervisor.

**Several nodes on one host.** From this release on, every node serves the admin listener on `127.0.0.1:7401` unless its `[admin]` table says otherwise, so a second node on the same host fails to bind it and exits. 0.3.0 refuses an `[admin]` table, so the table can't be added before the upgrade. On such a host, run `fleetringd upgrade --no-restart --config …`, give each node's configuration an `[admin]` table with a `listen` port of its own (or `enabled = false`), then restart the nodes one at a time.

**Rolling back.** `fleetringd upgrade --rollback --config …` swaps `fleetringd` and `fleetringd.previous` and restarts the node as an upgrade does; a second rollback rolls forward again. If the new binary doesn't run at all, `fleetringd.previous upgrade --rollback` does the same. A release without the command, such as 0.3.0, rolls back by hand: `mv fleetringd.previous fleetringd`, then restart. With `--config` or `--control`, a rollback to a release that lacks an enabled fleet feature is refused before the swap, naming the feature: the fleet never gives a feature up. Without either, the command can't ask the control plane and doesn't check the fleet: a data node rolled back that way is refused at its join, and a control node can't read the log.

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
