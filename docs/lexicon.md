# Lexicon

The names FleetRing uses, in code, docs, the CLI, the TUI and desktop app, and tickets. This page is the single source of truth: when two places disagree, this page wins, and the other place is fixed or gets a rename ticket. A new concept gets its entry here before it gets a type, a flag, or a column header.

Each entry gives a definition, where the word applies, its canonical spelling in code and in the CLI, and the words not to use for it. "Applies to" uses four places:

- **crate**: the `fleetring` placement crate, which says "key", never "database" (ADR-0011).
- **daemon**: the node process (`fleetring serve`; the `fleetringd` crate) and the `fleetring-control`, `fleetring-wal` and `fleetring-proto` crates.
- **CLI**: the `fleetring` commands (see the [CLI style guide](cli.md)). The binary is also run as `fleet`, its permanent short name (ADR-0043); that `fleet` is the program, not the [fleet](#fleet) noun.
- **UI**: the TUI and the desktop app (ADR-0033).

A word marked *planned* names something decided but not yet built. Accepted ADRs and finished tickets keep the words they were written with; [renamed in 0.3.0](#renamed-in-030) and [renamed in 0.7.0](#renamed-in-070) map the old words to these.

## Hierarchy

```text
fleet                 one control plane, one certificate authority, one fleet name
└── cell              optional; partitions keys: each key lives in exactly one cell
    │                 (cells nest: a region can be a cell of cells)
    └── zone          a failure-domain label; a cell's ring spreads replicas across zones first
        └── host      a machine label; then across hosts
            └── node  one fleetring serve process, a member of the fleet and of its cell's ring
```

- **Cells partition; zones and hosts spread** (ADR-0012, fact 1). A directory or a ring picks a key's cell, and the cell's ring picks its nodes. Zones and hosts are labels on a ring's nodes, not layers: one key's replicas sit in several zones of one cell.
- **A host belongs to exactly one zone**, and a node without a host label is its own host (ADR-0006, point 2).
- **A fleet without cells is one cell**: one ring over all its nodes. That is the only shape the daemon runs today; cells exist in the placement crate (FR-020).
- **A ring places keys onto nodes.** In the daemon the key is the database's name. The ring's preference list for a key starts with its replica set, the first R nodes; the first of those is the preferred primary. A lease makes one replica the primary at an epoch, and the other replicas are its followers.

```text
database "notes" ──(name is the key)──▶ ring ──▶ replica set [n3, n1, n7]
                                                  │    └── followers: n1, n7
                                                  └── primary (holds the lease at epoch 4): n3
```

## Words we don't use

| Instead of | Say | Why |
|---|---|---|
| cluster | [fleet](#fleet) | ADR-0032; renamed in 0.3.0 (FR-059). |
| master, leader (for a database) | [primary](#primary) | "Leader" is kept for the control plane ([leader](#leader)). |
| slave, secondary, standby, read replica | [follower](#follower) | |
| server, instance, box | [node](#node), or [host](#host) for the machine | |
| shard, partition | [database](#database) in the daemon, [key](#key) in the crate | "Partition" is kept for network partitions. |
| tenant (as an object) | a key prefix, such as `tenant:acme/` | [Tenant](#tenant) is a naming convention, not a type. |
| region, pool, datacenter (as a level) | [cell](#cell) or [zone](#zone) | A region is a cell layer; a datacenter is usually a zone value. |
| CA (in prose) | [authority](#authority) | `ca.crt`, `ca.key`, and `--ca` keep the short form. |
| blocklist, revocation list (for tokens) | [denylist](#denylist) | "Certificate revocation list" names a thing FleetRing doesn't have. |
| up, alive, healthy, dead | live, down ([liveness](#liveness)) | |
| decommission, depart, departure | [leave](#leave) | FR-038's title keeps "departure"; ticket titles are history. |
| host (for where a node is dialed) | [address](#address) | A [host](#host) is the machine label. |
| changefeed, CDC (in names) | [change stream](#change-stream) | `_fleetring_cdc` keeps its name; it is an on-disk table. |
| backup (as a noun for the mechanism), durability sink | [archive](#archive) | "Backup" is fine in prose for what an archive is for. ADR-0016 says durability sink; it is accepted, so it keeps the word. |
| sink (for where committed batches go to be replicated) | [batch publisher](#batch-publisher) | A [sink](#sink) is only a change stream's destination. |
| snapshot (alone) | [database snapshot, base snapshot, or control snapshot](#snapshot) | Say which state is copied. |
| key (for a database, in the daemon) | [database](#database) | "Key" alone is the placement key; other keys say which: private key, signing key, lease key. |
| role (alone) | [node role](#node-role), [replica role](#replica-role), or [access role](#access-role) | |
| capability (for a scope or an action) | [scope](#scope), [action](#action) | Fine in prose about capability-based security in general. |
| branch, copy (for a fork) | [fork](#fork) | "Clone" is fine for the file-system mechanism. |
| checkpoint (for a co-signed chain head) | [signed head](#signed-head) | A [checkpoint](#checkpoint) is the engine's. |
| release (for a package's version) | package version ([package](#package)) | A [release](#release) is a version of the `fleetring` binary. |
| API key, password | [token](#token) or [certificate](#certificate) | |
| health (for a member's liveness) | [liveness](#liveness) | `/healthz` is about the process; liveness is the control plane's view. |
| tag | [label](#label) | ADR-0041. |
| owner (of a database) | [creator](#creator) | Being the creator grants nothing; scopes decide. "Owner" is refused for a lease holder and a primary too; it is kept for an agent's [owner](#owner) (planned). |
| TTL (as a field or flag) | [expiry](#expiry) | "TTL" is fine in prose for the duration. |
| connection, stream (alone, for a Hrana stream) | [Hrana stream](#hrana-stream) | A [change stream](#change-stream) and a replication stream are other things. |

## Terms

### Access role

*Planned* (ADR-0044). A named bundle of scope templates, such as `developer = ["admin:team-{team}-*", "read:org-*"]`, assigned to principals and groups in the identity registry. The issuer expands a principal's access roles into plain scopes when it issues a token; nodes never see one.

- **Applies to:** daemon, CLI, UI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** role (alone), permission set, policy, profile.

### Acknowledgement mode

How many replicas must store a write before it is acknowledged: `quorum` (a majority, the default), `all`, or `local` (the primary alone; a failover may lose acknowledged writes). Set per database (ADR-0023).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Acks`, `ACKS_QUORUM` in the protocol. **CLI:** `--acks quorum|all|local`; config `[replication] acks`.
- **Not:** consistency level, write concern, sync mode.

### Action

The verb half of a [scope](#scope): `read`, `write`, `fork`, `admin`, `stream`, or `grant` (ADR-0025, ADR-0044 point 2). `write` and `fork` each imply `read`; `admin` implies `write`, `fork`, and `read`; `stream` and `grant` stand alone, implying nothing and implied by nothing. `read` doesn't imply `fork`. `fork` lets a principal fork *from* a database; forking *into* a name needs `admin`. `grant` authorizes no request: it bounds the scopes an agent's token may hand a child, narrowed to its pattern, and a `[[principal]]` entry refuses it. Since 0.8.0; nodes before 0.8 refuse a token carrying `fork` or `grant`.

- **Applies to:** daemon, CLI, UI.
- **Code:** `auth::Action`. **CLI:** the part before `:` in `--scope read:notes`.
- **Not:** permission, privilege, role, verb.

### Adapter

The engine adapter: the code behind the `DatabaseEngine` trait that turns one [engine](#engine)'s storage into opaque [positions](#position) and payloads. Two shapes: a *frame source*, which FleetRing replicates (SQLite now), and a *self-replicating* engine, which replicates itself (Postgres, Turso sync) (ADR-0016). Everything engine-specific lives below it.

- **Applies to:** daemon.
- **Code:** `DatabaseEngine`, `FrameSourceEngine`; "SQLite adapter". **CLI:** none.
- **Not:** driver, backend, plugin, connector.

### Address

Where a node is dialed: `host:port`, or the IP addresses and DNS names its certificate names. A member advertises one (`[node] advertise`), and placements carry it for each replica.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Member::address`, protocol `Node.address` and `Replica.address`, `[node] advertise`. **CLI:** `--address <host:port>` where a command dials one node (`status`, `lease watch`, `db exec`, `db query`); `cert node --addresses` for the names a node certificate carries; `address=` after each member in `status`.
- **Not:** host (that is the machine label), endpoint, URL, `--node` (which takes a node ID).

### Admin listener

The HTTP listener a daemon serves `/metrics`, `/healthz`, and `/readyz` on, apart from the gRPC port; loopback and plaintext by default (ADR-0034).

- **Applies to:** daemon, CLI.
- **Code:** `[admin]`, `AdminConfig`, `observe::admin`. **CLI:** none.
- **Not:** status port, management port, HTTP port, debug server.

### Agent

An AI agent's identity in a fleet: the [principal](#principal) `agent-<id>`, with a client [certificate](#certificate) and a [token](#token) scoped to its own databases (`admin:agent-<id>-*` and `stream:agent-<id>-*` by default, or `read:agent-<id>-*` and `stream:agent-<id>-*` for a read-only agent, from 0.8; `admin:` alone before), kept in a directory of its own, `<agents dir>/<id>/` (ADR-0043, point 8; FR-081). The *agent ID* is 1 to 32 lowercase letters, digits, and `_`, so the `-` after it in `agent-<id>-*` always ends it, and `.` is left for [delegation](#delegation). The directory, created `0700`, holds `ca.crt`, the certificate and its key, the token, and `identity.toml`, the *identity record*, which holds no secret: the principal, scopes (with `scopes_from = "flags"` when `--scope` chose them, so `agent renew` never widens them), expiries, `issued_by`, the [owner](#owner), and the [parent](#parent). The *agents directory* is `<dev_dir>/agents` for the `dev` context, a context's `agents_dir` if it sets one, and `$XDG_DATA_HOME/fleetring/agents/<context>/` for any other. In [dev mode](#dev-mode), `fleetring mcp --agent <id>` mints the identity on first use and renews it at start; elsewhere `agent add --authority` mints it where the [authority](#authority) is. A name that starts with `agent-` and holds `.` is a *reserved dotted agent name*, which no command issues and no node accepts in 0.7 (ADR-0044, point 1.8). Without an [agent launcher](#agent-launcher), agents run in [trusted local development](#trusted-local-development).

- **Applies to:** daemon, CLI, UI.
- **Code:** `AgentId`, `AgentDir`, `AgentRecord` in the CLI; `Principal::is_reserved_agent_name`. **CLI:** `fleetring agent add|list|show|renew|remove`, `--agent <id>` and `FLEETRING_AGENT` on every client command, `mcp --agent`.
- **Not:** bot, assistant, robot, service account, agent user.

### Agent launcher

*Planned* (ADR-0044). The trusted program that starts agents in enforced sandboxes, gives each only its own credential or a *credential proxy* that keeps holder keys outside the sandbox, spawns children within their parent's ceiling and the user's saved policy, and exposes a restricted spawn and report endpoint instead of a terminal's control socket. A secure multi-agent mode needs one; without it, agents run in [trusted local development](#trusted-local-development).

- **Applies to:** docs, CLI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** harness (fine for the program an agent runs in), supervisor, runner.

### Archive

A database's history kept for recovery: a physical [change stream](#change-stream) plus base [snapshots](#snapshot), written as immutable objects to an object store, with point-in-time [restore](#restore) (ADR-0029, FR-049). Separate from replicas: replicas give availability, an archive gives recovery. Its objects are [segments](#segment), and each base snapshot's *chunks* and *descriptor*; an object's name is its *object key* ([key](#key)).

- **Applies to:** daemon, CLI, UI.
- **Code:** `[[archive]]` in config, `durability::Archive`, `ArchiveConfig`, `ArchiveKeys`, `ObjectStore`, `S3Store`, `DirectoryStore`, `MemoryStore`. **CLI:** the `archive` group (`archive show`, `archive verify`, `archive restore`), and `--archive <name>` on those and on `db restore`.
- **Not:** backup (as the mechanism's name), dump, export, durability sink. ADR-0016, point 3, says "durability sink" for the same idea; the ADR is accepted and keeps its words, and everything else says archive.

### Audit line

One log line on the `fleetringd::audit` target per request a principal makes: debug when allowed, a warning when refused. It names the principal, the credential (`certificate` or `token:<id>`), the action, the database, and the RPC, never the token.

- **Applies to:** daemon, UI.
- **Code:** target `fleetringd::audit`. **CLI:** none; read it from the node's logs.
- **Not:** audit event, access log, audit trail (for a single line).

### Authority

The fleet's certificate authority: an Ed25519 key (`ca.key`) and its certificate (`ca.crt`). It issues every node and client certificate and signs every token. Its key belongs on no node.

- **Applies to:** daemon, CLI, UI.
- **Code:** `tls::Authority`. **CLI:** `fleetring cert init`; files `ca.crt` and `ca.key`; flag `--ca`.
- **Not:** CA (in prose; fine in file names and flags), root, issuer (the token claim `iss` names the fleet, not the authority), PKI.

### Batch

One committed write transaction's changes as the engine ships them: an opaque payload with a start and end [position](#position). Replication, change streams, and archives all move batches.

- **Applies to:** daemon.
- **Code:** `FrameBatch`, protocol `Batch`, `CommittedBatch`. **CLI:** "batch" in `status --node` backlogs.
- **Not:** frame (a frame is SQLite's unit inside a batch, and stays below the adapter), transaction (in replication code), chunk (a [snapshot](#snapshot) is sent in chunks), record, event.

### Batch publisher

Where a database's actor hands each committed batch to be replicated, right after the commit; it also decides, before a write runs, whether enough replicas are reachable for it to run at all (ADR-0023). On a data node it is replication's primary streams; in tests, a recording.

- **Applies to:** daemon.
- **Code:** `BatchPublisher` (`admit`, `publish`), `ReplicatingPublisher`, `RecordingPublisher`, module `fleetringd::publisher`. **CLI:** none.
- **Not:** sink (a [sink](#sink) receives a change stream, after the write quorum holds a batch), feed, outbox, queue.

### Baton

The opaque string that continues a [Hrana stream](#hrana-stream): the [router](#router) returns one with each response and accepts it once, on the stream's next request, from the token that opened the stream (ADR-0037). It is `base64url(stream ID ‖ 16 random bytes ‖ sequence)`; the random bytes are the stream's secret, and the sequence moves on with every request, so a replayed or forked baton is refused.

- **Applies to:** daemon.
- **Code:** `router::streams::Baton`. **CLI:** none.
- **Not:** session ID, cursor, ticket, token (a [token](#token) is a credential).

### Binding

What a node making a node-to-node call must be for the database or node the request names: `self`, `replica`, `holder`, `candidate`, `holder or candidate`, `follower`, or `dropped` (ADR-0031, ADR-0042). A call that fails its binding is refused before it touches state.

- ***Seeding binding*** (ADR-0046): `seeding`, under which a [fork](#fork)'s seeder fetches the source's database snapshot from the source's primary, and the fork's other replicas fetch the [staged copy](#staged-copy) from the seeder, on `Replication.Snapshot`. Each side checks the caller against the full fork identity, the attempt, and the source's [incarnation](#incarnation) in its own view of control state.

- **Applies to:** daemon.
- **Code:** `replication::binding`, metadata `fleetring-binding`, the `Binding:` lines in the protocol, `Binding::Seeding` and `Binding::FollowerOrSeeding` (`Replication.Snapshot`'s), `Unbound::NotSeeding`, `SeedingMismatch`. **CLI:** none.
- **Not:** check, guard, constraint, policy.

### Candidate

The replica a promotion makes the next primary. It fences replicas, adopts the most advanced log, and completes the promotion (ADR-0022).

- **Applies to:** daemon, UI.
- **Code:** the candidate of a `Promotion`, binding `candidate`. **CLI:** none.
- **Not:** successor, next leader, heir.

### Cell

A partition of the key space with its own ring. A directory, a ring over the cells, or both pick a key's cell, and that cell's ring picks its nodes. Cells nest: `Cells<Cells<Ring>>` is region → cell → nodes (ADR-0012). Each key lives in exactly one cell.

- **Applies to:** crate now; daemon, CLI, UI *planned* (roadmap E6).
- **Code:** `Cells`, `CellId`, `CellPlacement`, `CellsSpec`. **CLI:** *planned*, `--cell <id>`.
- **Not:** region (a region is a layer of cells, or a cell's name), pool, shard, partition, cluster, availability zone.

### Certificate

An X.509 certificate from the fleet's [authority](#authority). A *node certificate* names a node, `fleetring://<fleet>/node/<id>`; a *client certificate* names a [principal](#principal), `fleetring://<fleet>/principal/<name>` (ADR-0025). Every connection is mutual TLS with them.

- **Applies to:** daemon, CLI, UI.
- **Code:** `tls::Identity` (a certificate, its private key, and the trusted authorities), `SubjectUri`. **CLI:** `fleetring cert node`, `fleetring cert client`; files `node.<id>.crt`, `client.<principal>.crt`; flags `--cert`, and `--key` for the certificate's private key.
- **Not:** cert (fine in commands and files, not in prose), key pair (the certificate and its private key together are an *identity*), credential (a credential is a certificate *or* a token).

### Change stream

The ordered, at-least-once flow of one database's acknowledged batches to a [sink](#sink), beside replication and never in a write's acknowledgement path (FR-039). *Logical* format sends row changes; *physical* sends the opaque batches.

- **Applies to:** daemon, CLI, UI.
- **Code:** `changes::`, `ChangeSink`, `Format::Logical`/`Physical`, `CaptureMode`. **CLI:** `[[changes.sink]]` in config; sink progress in `status --node`; a [watcher](#watcher) follows the recorded row changes with `db watch`.
- **Not:** changefeed, CDC (in names), event stream, replication stream (that is node-to-node).

### Checkpoint

The engine folding its log into the main database file (SQLite's WAL checkpoint). A follower behind its primary's last checkpoint needs a snapshot. Above the adapter it is only an opaque marker.

- **Applies to:** daemon.
- **Code:** `CheckpointMarker`. **CLI:** none.
- **Not:** compaction (reserved for the control log), flush, sync.

### Client

Anything that calls the fleet's RPCs as a [principal](#principal): the CLI, the TUI, the desktop app, an application, the router. Clients share one crate (ADR-0033).

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetringd::data::Client` today; the `fleetring-client` crate holds contexts, the redirect replies and the typed results a client prints (`fleetring_client::output`), and the clients move into it once the types they share with the daemon do (ADR-0033, amendment). **CLI:** the `fleetring` commands; `fleetring cert client` issues a client's certificate.
- **Not:** user, consumer (a change stream has consumers), caller (fine in prose about one RPC).

### Context

A named set of client settings: a fleet name, the control addresses, the TLS files, a default principal, and optionally a token file, so a command needs `--context home` instead of repeating flags. It holds paths and names, never a secret. Contexts live in `contexts.toml` in the configuration directory (ADR-0043, point 4).

- **Applies to:** CLI, UI.
- **Code:** `fleetring_client::context::Context`. **CLI:** `fleetring context …`, `switch`, `--context`, `FLEETRING_CONTEXT`.
- **See:** [Current context](#current-context).
- **Not:** profile, target, environment, connection.

### Control log

The control plane's durable, ordered list of commands, and the *control snapshot* it compacts into. Replaying it with `apply` rebuilds control state exactly (ADR-0017). It holds no key material.

- **Applies to:** daemon.
- **Code:** `fleetring_control::Command`, `State`, `apply`. **CLI:** none.
- **Not:** journal, WAL (a WAL belongs to a database), event log, Raft log (until Raft exists).

### Control node

A node that runs the control role: it holds the control log and answers control RPCs. Today a fleet has one; with Raft it will have three or five, of which one is the [leader](#leader).

- **Applies to:** daemon, CLI, UI.
- **Code:** `NodeRole::Control` or `NodeRole::Both`. **CLI:** `--control <host:port>`, `role = "control"`.
- **Not:** master, coordinator, manager, controller, metadata server.

### Control plane

The control service as a whole: membership, placement records, epochs, leases, promotions, and the token denylist, whatever node or nodes run it.

- **Applies to:** daemon, UI.
- **Code:** `control::`, protocol service `Control`. **CLI:** "the control plane" in messages.
- **Not:** control node (when you mean the service, not a machine), management plane, brain.

### Creator

The [principal](#principal) that created a database, which the control node records from the request's caller, never from the request (ADR-0041). Being the creator grants nothing: what a principal may do comes from its [scopes](#scope) alone. A database created before `metadata` was enabled, or by an anonymous caller, has none.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Metadata::created_by`, protocol `DatabaseMetadata.created_by`. **CLI:** `creator:` in `db show`; `created_by` in its JSON.
- **Not:** owner (being the creator grants nothing), author (that is a description's), holder (that is a lease's).

### Current context

The [context](#context) that client commands use when no `--context` flag or `FLEETRING_CONTEXT` names another: the file's `current`. `fleetring switch` and `context use` change it, and keep the one they replace as the previous context, for `switch -`. Environment variables for a single setting override it (ADR-0043, point 4.4).

- **Applies to:** CLI, UI.
- **Code:** `Contexts::current`. **CLI:** `fleetring context current`, `fleetring switch`.
- **Not:** default context, active context.

### Data node

A node that runs the data role: it hosts replicas and serves database calls. A node with `role = "both"` is a data node and a control node.

- **Applies to:** daemon, CLI, UI.
- **Code:** `NodeRole::Data`, `role = "data"`. **CLI:** "data node" in help; `status --node <node-id>`.
- **Not:** storage node, worker, server.

### Database

One SQLite database (later libSQL, Turso, or Postgres) that FleetRing places, replicates, and fails over as a unit. Its name is its placement [key](#key), but the daemon, its scopes, and its messages say "database": `admin on the database`, `auth::Target::Database`. The placement crate never says "database".

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Database`, `DatabaseName`. **CLI:** the noun `db` (`fleetring db create`), the argument `<database>`; "database" in output headers.
- **Not:** shard, partition, tenant, keyspace, namespace, table, instance.

### Delegation

*Planned* (ADR-0044). An agent handing a narrower copy of its token to a child, offline and without the authority: it appends a block to its biscuit token that names the child's principal (its own, `.`, one segment), itself as parent, and the child's holder key, with scopes and an expiry that can only narrow, and signs the block with its own holder key as a biscuit third-party block, so copied token bytes can't delegate. It needs the `grant` action, and FleetRing's own verifier, not biscuit's Datalog, enforces it. A *delegated token* is one with such a block; *attenuation* is appending a block that narrows without naming a new holder.

- **Applies to:** daemon, CLI, UI.
- **Code:** none yet; the token vocabulary is specified in `docs/spec/tokens-v1.md` when built. **CLI:** none yet.
- **Not:** impersonation, sub-token, child token, token exchange (an online protocol FleetRing doesn't use for this).

### Denylist

The set of revoked tokens and signing keys the control plane keeps and publishes to data nodes with every heartbeat reply. A node refuses a token on it. *Planned* (ADR-0044): it also holds [revocation IDs](#revocation-id) of token blocks, and principals whose earlier tokens are revoked.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Revocations`, `Revoked`, protocol `Revocation`. **CLI:** `fleetring token revoke`.
- **Not:** blocklist, blacklist, revocation list, CRL.

### Description

Free text, at most 1 KiB, saying what a database holds and how it was set up; part of its [metadata](#metadata) (ADR-0041). Tab and line feed are allowed; other control characters, invisible format characters, and the line and paragraph separators are refused. It records who last set it (`description_by`), and text output marks one written by another principal, or by nobody that can be named, as untrusted.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Description`, `DescriptionChange`, protocol `DatabaseMetadata.description`. **CLI:** `--description` on `db create`, `db restore`, and `db edit`; `--clear-description`; `description:` in `db show`.
- **Not:** comment, note, summary.

### Dev mode

One node on loopback, with a local [authority](#authority) and an `admin` [principal](#principal) with every [scope](#scope), that `fleetring dev` creates once in `~/.fleetring` and reuses, for development only (ADR-0039). It is the normal daemon with `dev = true` in its configuration, which keeps every address on loopback; `status` prints `dev mode` for it.

- **Applies to:** daemon, CLI, UI.
- **Code:** `dev::DevMode`, `dev::prepare`, protocol `MembershipUpdate.dev_mode`. **CLI:** `fleetring dev [--data-dir <dir>] [--port <n>] [-d]`, which writes the `dev` [context](#context); `fleetring dev stop|status|logs`; `dev = true` in config; `~/.fleetring/env`, for scripts.
- **Not:** local mode, sandbox, playground, standalone, single-node mode (a fleet of one node needn't be dev mode).

### Directory

In cells: the explicit rules, by longest prefix or exact key, that pick a key's cell before the ring does, for residency and pinning (ADR-0012). Elsewhere the word means a filesystem directory (`--dir`, `data_dir`).

- **Applies to:** crate.
- **Code:** `Directory`. **CLI:** *planned*, with cells.
- **Not:** routing table, mapping, catalog, lookup table.

### Drain

Marking a member as draining: it gets no new leases and is never a promotion's candidate, and it hands every lease it holds to a follower through a [transfer](#transfer) (FR-038). A drained node stays a follower until it stops, restarts, or is *undrained*: its mark cleared, a [fleet feature](#fleet-feature).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Control.Drain`, `Control.Undrain`, `Action::Undrain`, protocol `Member.draining`. **CLI:** `fleetring node drain <node-id>`, `fleetring node undrain <node-id>`; `[membership] on_shutdown = "leave"`.
- **Not:** evacuate, cordon, decommission, step down.

### Durability

A database's acknowledgement mode and replica floor together (ADR-0023). The *replica floor* (`min_replicas`) is how many replicas, the primary included, must be reachable for a write to run.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Durability`, `Acks`, `min_replicas`. **CLI:** `fleetring db edit`, `--acks`, `--min-replicas`.
- **Not:** consistency, safety level, replication mode.

### Engine

The storage engine behind a database: `sqlite` today. Every database records its engine, so a migration can change it (ADR-0021).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Engine::Sqlite`, protocol `ENGINE_SQLITE`. **CLI:** the `ENGINE` column in `db list`.
- **Not:** backend, driver, database type, flavor.

### Epoch

A number that only grows, used to order and fence. Always say which one:

- **Membership epoch:** the version of the fleet's membership and its ring; it grows with every join, leave, or drain (`fleetring::Epoch` in the crate, `membership_epoch` in the daemon).
- **Lease epoch:** the version of one database's lease; each promotion or transfer starts a new one, and replicas refuse anything from an older one (`LeaseEpoch`; "epoch" in `db list` and `db locate`).
- **Log epoch:** the lease epoch of the primary that wrote a replica's latest log entries; promotions rank logs by it, then by position (ADR-0022).

- **Applies to:** crate (membership epoch, as the ring's version), daemon, CLI, UI.
- **Code and CLI:** as above; output headers say `epoch` only where the kind is clear from the command.
- **Not:** term, generation (a generation is SQLite's, below the adapter), version (for a lease), round.

### Exit code

The number a `fleetring` command ends with, the same for every command: 0 success, 1 failed, 2 usage, 3 refused, 4 unavailable (a retry may succeed), 130 interrupted (`docs/cli.md`, section 8). A failed call's gRPC status chooses it in one place.

- **Applies to:** CLI.
- **Code:** the binary's `exit::exit_code` and `exit::code_of`. **CLI:** listed in `fleetring --help`.
- **Not:** status (a gRPC status is what a call returns), return code, error code.

### Expiry

The time after which the control plane drops a database, as `db drop` does; part of its [metadata](#metadata) (ADR-0041, point 4). A duration is turned into a time by the control node before it logs the command, at least a minute ahead. The control node's sweep stamps the command that drops it with its own clock, so a control node whose clock runs ahead expires databases early by as much. Before the primary deletes its copy, it flushes the database's change streams, its [archive](#archive) included, for up to 30 seconds. A replica that misses the drop keeps its copy as an [orphan](#orphan).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Metadata::expires_at`, `Action::ExpireDatabase`, `Feature::Expiry`, `control::MIN_EXPIRY`, `data::Expiry`, `ChangeStreams::flush_database`. **CLI:** `--expires-in`, `--expires-at` on `db create`, `db restore`, and `db edit`; `--clear-expiry`; `expires` in `db show`.
- **Not:** TTL (fine in prose for the duration), retention (an archive's), lifetime (a token's), deadline.

### Failover

A promotion after a primary's lease expires: the control plane starts it, and a candidate completes it (ADR-0022). A planned move of the lease is a [transfer](#transfer), not a failover.

- **Applies to:** daemon, CLI, UI.
- **Code:** `replication::failover`. **CLI:** "fails over" in messages.
- **Not:** failback, switchover (for an unplanned move), election.

### Fencing

Making a replica record a newer lease epoch, so it refuses anything an older primary sends. Epochs fence; no clock decides safety (ADR-0018).

- **Applies to:** daemon.
- **Code:** `Replication.Fence`, `RecordFenced`. **CLI:** none.
- **Not:** locking, isolating, STONITH, killing.

### Fingerprint

A 64-bit hash of a ring's or cell layer's canonical encoding. Two peers with equal fingerprints place every key identically (ADR-0008).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Fingerprint`. **CLI:** "ring fingerprint" in `status`.
- **Not:** checksum, hash (alone), version, digest.

### Fleet

The set of nodes that one control plane and one certificate authority govern, under one [fleet name](#fleet-name). A fleet is made of cells, zones, hosts, and nodes (see the [hierarchy](#hierarchy)). The README's "fleets of SQLite databases" uses the word in its everyday sense, for many databases; in names, flags, and types, "fleet" means only this.

- **Applies to:** crate (test fixtures, such as `fixtures::fleet(12, 3)`), daemon, CLI, UI.
- **Code:** `Fleet` (simulation harness), `FleetName`. **CLI:** `--fleet <name>`.
- **Not:** cluster (0.2's word, renamed in 0.3.0 by FR-059), deployment, ring (a fleet can hold many rings), mesh, realm.

### Fleet feature

A control command that not every member can read or act on, refused until every member, live or down, reports it in its [software](#software). The first such command the fleet accepts enables the feature for good, and from then on a member that lacks it can't join (ADR-0038). The features are `undrain`, `metadata` (a creation with a description or labels, and every metadata update), and `expiry` (a command that sets or changes an expiry, and the one that expires a database), ADR-0041, and `fork`, for the fork commands, which enabling also starts assigning [incarnations](#incarnation) (ADR-0046), and which releases report since data nodes seed and open forks.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Feature`, `Action::features`, `State::features`, `State::check_features`, `State::check_admission`, `Rejection::FeatureUnsupported`, `Rejection::MissingFeature`; protocol `Software.features`.
- **Not:** feature flag (alone), capability, cluster version, finalization.

### Fleet name

The name `cert init --fleet` gives the authority. Every certificate it issues carries it in its `fleetring://<fleet>/…` URI, every token carries it as `iss`, and a node refuses a certificate or token for another fleet.

- **Applies to:** daemon, CLI, UI.
- **Code:** `FleetName`; `fleet()` on `SubjectUri`, `Identity`, and `Authority`. **CLI:** `cert init --fleet <name>`.
- **Not:** cluster name, trust domain, realm, namespace.

### Fleet scope

The scope for operations that name no database: membership, draining, removing members, revoking tokens. A principal without it sees only the databases its other scopes cover.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Scope::fleet()`, `auth::Target::Fleet`, `Requirement::Fleet`. **CLI:** `--scope fleet`, `scopes = ["fleet"]`.
- **Not:** cluster (0.2's spelling; refused since 0.3.0), operator, superuser, root, global.

### Follower

A replica that isn't the primary: it applies the primary's batches and can be promoted.

- **Applies to:** daemon, CLI, UI.
- **Code:** `ReplicaRole::Follower`, `replication::follower`. **CLI:** "followers" in `db locate`.
- **Not:** slave, secondary, standby, read replica, backup, replica (when only non-primaries are meant).

### Fork

*Planned* (ADR-0044, ADR-0046). A new database seeded from a source database's snapshot at a position its source's write quorum acknowledged, independent afterwards, that records its source in `forked_from`: the source, the source's [incarnation](#incarnation), the position, its chain hash, and the time (a signed head or a package manifest, later). Needs `fork` on the source and `admin` on the target. Also the [action](#action) `fork`, which implies `read` and which scopes accept since 0.8.0.

- ***Fork identity:*** the triple (name, incarnation, operation): the fork's database name, its own incarnation, and its [operation ID](#operation-id). Every command, report, `Snapshot` request, and staging directory about a fork carries it, so nothing meant for a dropped fork can act on a fork created again under the same name.
- ***Fork states:***
  - **`seeding`:** being copied, in numbered *attempts*. Each attempt has one *seeder*, the replica that fetches the database snapshot from the source's primary; the fork's other replicas copy the seeder's [staged copy](#staged-copy). Never leased, never served. A node *excluded as seeder* (it was down, or its attempt timed out) isn't chosen as seeder again while another can be, and may still hold a copy; a node that *refused on disk* is neither seeder nor holder. Only disk refusals can fail a fork for disk.
  - **`seeded`:** the fork's *completion quorum*, a majority of its replicas and at least its write quorum, durably holds the staged copy. Those *holders* are the fork's replica set until it is ready, whatever placement says, and only a holder whose copy is still verified may take the first lease. Nothing is served yet. The CLI prints it as `opening: not yet usable`; JSON and MCP say `seeded`.
  - **`ready`:** the fork-reset write has committed. Clients, `db watch`, and sinks are served, and it is an ordinary database from then on. Its holders stay its replica set until they are *released*: its primary counts both their write quorum and placement's toward every write, reports once placement's holds its log, and the control plane then moves it onto placement.
  - **`failed`:** it stopped, with a reason, and stays until `db drop` removes it.

  A database no fork made has no fork state, and counts as seeded and ready. `ready` is a fork's state, not a node's [readiness](#readiness).
- ***Fork reset*** and ***reset marker:*** the fork-reset write is one conditional transaction, run by the fork's primary before it serves anything, that clears the change record and the sink positions the fork inherited from its source, and makes the reset marker the only row of the reserved `_fleetring_fork` table: a row naming the fork identity (a fork of a fork drops the row it inherited). It runs only when the marker for this identity is absent, and counts as committed only once a majority of the fork's replicas, and at least its write quorum, holds it, `acks = local` included, so it happens exactly once in the database's surviving history.
- **Cancelling** a fork is dropping it while it is seeding or seeded, or letting its expiry pass then; there is no cancel command. An operation's terminal outcomes are exactly `ready`, `failed`, and `cancelled`.
- **Applies to:** daemon, CLI, UI.
- **Code:** `ForkDatabase`, `CompleteFork`, `RetryForkSeed`, `RecordForkDiskRefusal`, `FailFork`, `DropForkHolder`, `MarkForkReady`, `ReleaseForkHolders`, `PlacementCaughtUp`, `Fork`, `ForkState`, `ForkFailure`, `RetryCause`, `Feature::Fork`, protocol `Control.ForkDatabase` and `Placement.fork` (`ForkStatus`), `[fork]`, `ForkMarker` (the reset marker), `ForkReset`, `Database::fork_reset`, `Database::fork_marker`, the `_fleetring_fork` table; on data nodes `replication::fork`, `data::ForkKeeper`, the readiness gate, and the metadata `fleetring-retry-after-ms` (*planned:* `Metadata::forked_from`). **CLI:** none yet (*planned:* `db fork <source> <target>`, the fork lines of `db show`, the `FORK` column of `db list`, and `fork` and `forked_from` in their JSON; MCP `db_fork`).
- **Not:** branch, clone (kept for the file-system mechanism, which is deferred), copy (a fork is a database; a *copy* is a node's files of one), snapshot; for the states, not *unseeded*, *pending*, *done*, or *live* (a member's [liveness](#liveness)).

### Group

*Planned* (ADR-0044). A named set of principals in the identity registry, to which access roles are assigned. Membership is expanded into a token's scopes at issuance, never looked up by a node.

- **Applies to:** daemon, CLI, UI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** team (a team is a prefix tier, `team-<team>-`, usually with a group of the same name), role.

### Hash chain

One BLAKE3 hash per transaction over the replication log, each folding in the one before, so the hash at a position commits every byte before it (FR-044). One value of it is a *chain hash*.

- **Applies to:** daemon, CLI, UI.
- **Code:** `ChainHash`, `replication::chain`. **CLI:** `fleetring db verify`, `--expect <hash>`.
- **Not:** Merkle tree (a [page hash tree](adr/0030-page-hash-trees.md) is one; the chain isn't), checksum, signature (the chain isn't signed).

### Heartbeat

A data node's periodic call to the control plane. It reports the replicas the node hosts and their positions, keeps the node live, and brings back the denylist.

- **Applies to:** daemon.
- **Code:** `Control.Heartbeat`, `[control] heartbeat_interval_ms`. **CLI:** none.
- **Not:** ping, keepalive, health check, gossip.

### Holder

Who holds a lease: for a database lease, the primary; for a keyed lease, the named holder that requested it (ADR-0027).

- **Applies to:** daemon, UI.
- **Code:** `Lease::holder`, binding `holder`. **CLI:** none yet.
- **Not:** owner, leader, master, lessee.

### Holder key

*Planned* (ADR-0044). The Ed25519 key a token is bound to: a root agent's is its certificate's key; a child generates its own on its machine, its parent's delegation block names the public key, and the child presents it as a self-signed client certificate, which a node accepts only with `[tls] accept_holder_keys` on and never reads a name from. A node accepts the token only on a connection that proves that key, or, for a bearer delegation, through a relay with `forward_tokens`. Delegating from a token needs a signature by its holder key.

- **Applies to:** daemon, CLI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** private key (alone), session key, proof key.

### Hrana stream

A client's sequence of Hrana requests that share one [session](#session), continued across HTTP requests by a [baton](#baton) (ADR-0037). Always say *Hrana stream*: a change stream and a replication stream are other things. A stream idle past the router's `[streams] idle_timeout_ms` expires.

- **Applies to:** daemon.
- **Code:** `router::streams::HranaStream`, `[streams]` in the router's configuration. **CLI:** none.
- **Not:** stream (alone), connection, session (that is the node's side).

### Host

A placement label for the machine a node runs on, when several nodes share one; replicas spread across hosts after zones (ADR-0006). "Host" also keeps its network sense inside `host:port`, but the names a node is dialed at are its [addresses](#address) (`cert node --addresses`).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Host`, `[node] host`. **CLI:** `host=` after each member in `status`.
- **Not:** node (a host can run several nodes), server, machine (fine in prose), instance, VM.

### Identity registry

*Planned* (ADR-0044). The issuer's state: principals' kinds, owners, and login mappings, groups, access roles, and organizations, kept in the reserved database `_fleetring/identity`. Nodes never read it.

- **Applies to:** daemon, CLI, UI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** user store, directory (a [directory](#directory) maps prefixes to cells), IAM.

### Incarnation

*Planned* (ADR-0046). One life of a database name: `(name, incarnation)` identifies one database. Each new database, a [fork](#fork) included, gets 64 random bits, never 0, drawn by the control node that proposes its creation and carried in the command, as time is; so a name dropped and created again has a different incarnation, even where its [lease epochs](#epoch) start again, and even if `control.db` was replaced. No rule rests on a counter. A database created before the `fork` [fleet feature](#fleet-feature) was enabled has incarnation 0. A fork records its source's incarnation, so it never copies a database created again under the source's name. Printed as 16 lowercase hex digits, and a string of them in JSON.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Incarnation`, `Database::incarnation`, the `incarnation` of `CreateDatabase` and `ForkDatabase`, protocol `Placement.incarnation`. **CLI:** none yet (*planned:* `incarnation` in `db show`, `db locate` and `db fork`; `<name> (incarnation 3f9a0c6e21d4b870)`).
- **Not:** generation (SQLite's, below the adapter), epoch (a lease epoch counts leases within one incarnation), version (metadata's), instance, ID.

### Issuer

*Planned* (ADR-0044). The service that turns a login (OIDC, later passkeys or a wallet) into a FleetRing token, signing it with an issuer key that the authority certifies as a constrained trust anchor. It runs in the router first. The token claim `iss` names the fleet, not the issuer.

- **Applies to:** daemon, CLI.
- **Code:** none yet. **CLI:** none yet (`fleetring login`).
- **Not:** authority, identity provider (that is the OIDC side), auth server.

### Join

A node becoming a member: its first call to the control plane, which records it and gives it the membership and ring.

- **Applies to:** daemon, CLI.
- **Code:** `Control.Join`. **CLI:** "joined the fleet" in logs.
- **Not:** register, enroll, bootstrap, add (the operator doesn't add a node; it joins).

### Key

The placement key: in the placement crate, the opaque bytes that name a placeable thing; the crate places keys and knows nothing else (ADR-0011). In the daemon, a database's name is its key, and names, scopes, and messages there say [database](#database). "Key" alone means only this. Other keys always get their qualifier: *private key* (`--key`, `ca.key`), *signing key* (`--key-id`, the token header's `kid`), *lease key* (a keyed lease's name), *object key* (in an archive).

- **Applies to:** crate (only this word), daemon.
- **Code:** `&[u8]` or `impl AsRef<[u8]>` in placement; `DatabaseName` in the daemon. **CLI:** `<database>` where the key is a database name; `--key <path>` is the client certificate's private key, and `--key-id` a signing key's ID, as their help says.
- **Not:** database (in the crate), key alone for a private or signing key, shard key, partition key, ID, item.

### Keyed lease

*Planned* (FR-043). A fenced, renewable lease on any key, requested by a named holder and kept in control state, for things that have no database (ADR-0027).

- **Applies to:** daemon, CLI, UI.
- **Code:** *planned*. **CLI:** *planned*, `fleetring lease acquire|renew|release|watch`.
- **Not:** lock, mutex, distributed lock.

### Label

A `key=value` pair on a database, by the Kubernetes rules, for finding it again: `db list --label purpose=memory`. At most 32 per database; keys under `fleetring.dev/` are reserved. Part of its [metadata](#metadata) (ADR-0041). *Placement label* is the qualified name for a [zone](#zone) or [host](#host), and the placement crate's `label` module names validated identifiers (ADR-0005); say which.

- **Applies to:** daemon, CLI, UI.
- **Code:** `LabelKey`, `LabelValue`, `Labels`, `data::LabelRequirement`, protocol `Label`. **CLI:** `--label` on `db create`, `db restore`, `db list`, and `db edit`; `--remove-label`; `labels:` in `db show`.
- **Not:** tag, annotation, attribute.

### Leader

The control node that accepts control commands. Today the only control node; with Raft, the elected one, and others redirect to it. The word is reserved for the control plane.

- **Applies to:** daemon.
- **Code:** `not_leader`, `LEADER_METADATA_KEY`. **CLI:** none.
- **Not:** primary (that is a database's), master, coordinator.

### Lease

The right of one node to write one database at one lease epoch, granted and renewed by the control plane (ADR-0018). A lease expires unless renewed; safety still never depends on clocks, because epochs fence.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Lease`, `AcquireLease`, `RenewLease`, `WatchLeases`, `HeldLeases`, `LeaseUpdate::Held`. **CLI:** `fleetring lease watch --node <node-id>` or `--address <host:port>`, which prints `held`, `gained`, and `lost` lines.
- **Not:** lock, ownership, token, claim.

### Leave

A member removing itself from the fleet's membership, as a node does when it shuts down with `on_shutdown = "leave"`. An operator with the fleet scope makes the same call to remove another member.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Control.Leave`, `Client::remove_node`. **CLI:** `on_shutdown = "leave"`; "left the fleet" in logs; `fleetring node remove <node-id>` removes another member, refusing a live one unless `--force`.
- **Not:** depart, departure (FR-038's title keeps it), decommission, deregister, kick.

### Liveness

Whether a member is `live` (its last heartbeat is within the heartbeat timeout) or `down`. A down node isn't removed: placement filters it out and traffic fails over around it, but its data doesn't move (ADR-0006, point 6).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Liveness::Live`, `Liveness::Down`, protocol `Member.live`. **CLI:** `live` or `down` after each member in `status`, and after each replica in `db locate`.
- **Not:** up, alive, healthy, dead, failed, unreachable (fine in prose about a network).

### Mark

A [sink](#sink)'s delivered [position](#position), kept inside the database so that it replicates with the data and a new primary resumes the change stream where the old one stopped (FR-041, ADR-0028). A sink whose engine can't keep marks keeps its position in a file on the node instead, and `status --node` says which.

- **Applies to:** daemon, CLI.
- **Code:** `SinkMarks`, `MarkHome`, the `_fleetring_sinks` table. **CLI:** `replicated` or `local` after a sink's position in `status --node`.
- **Not:** checkpoint (that is the log's, see [checkpoint](#checkpoint)), high-water mark, cursor, offset.

### MCP server

`fleetring mcp`: the Model Context Protocol over stdio, acting as one [principal](#principal) with that principal's [scopes](#scope), so an agent can create, use, follow, and drop its own databases (ADR-0041, point 9). Its tools are `db_create`, `db_list`, `db_show`, `query`, `changes`, `exec`, `db_update`, and `db_drop`; text principals wrote reaches a result only in JSON string fields, and a [description](#description) carries its author and whether it is untrusted.

- **Applies to:** daemon, CLI.
- **Code:** `fleetringd::mcp`. **CLI:** `fleetring mcp`.
- **Not:** agent server, plugin, connector, bridge.

### Member

A node as the fleet's membership records it: its ID, placement attributes, address, liveness, and whether it is draining. "Member" is the record; "node" is the process.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Member`, protocol `Member`, `WatchMembership`. **CLI:** "members" in `status`.
- **Not:** peer (fine in prose for "another node"), participant, server.

### Metadata

Database metadata: a database's [description](#description), [labels](#label), [creator](#creator), creation time, and [expiry](#expiry), kept in control state with a version that grows with every update (ADR-0041). Updates are patches applied in log order; `--if-version` makes one conditional. A creation or update with metadata is a [fleet feature](#fleet-feature), `metadata`. *Planned* (ADR-0046): `forked_from`, a [fork](#fork)'s provenance, set only by the fork command and granting nothing.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Metadata`, `MetadataSpec`, `MetadataChange`, `Feature::Metadata`; protocol `DatabaseMetadata`, `CreateDatabaseWithMetadata`, `UpdateDatabase`. **CLI:** `db show`, `db edit`; `metadata` in `db show --json`.
- **Not:** annotations, properties, attributes.

### Node

One daemon process (`fleetring serve`, or `fleetring dev`): a member of the fleet, a ring member, a data node, a control node, or both. In the placement crate, a ring member with an ID, a weight, a zone, and a host.

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Node`, `NodeId`, `[node]`. **CLI:** the noun `node` (`fleetring node drain`, `fleetring node list`), `<node-id>`, and `--node <node-id>` in every command that takes one; a node's [address](#address) is `--address`.
- **Not:** server, instance, host (unless the machine is meant), worker, replica, peer (as a name), daemon (fine for the binary).

### Node role

What a daemon runs: `data`, `control`, or `both` (ADR-0015).

- **Applies to:** daemon, CLI.
- **Code:** `fleetringd::NodeRole`, `role` in config. **CLI:** `--role data|control|both`.
- **Not:** role alone (say whose), mode, type, kind, function.

### Operation ID

*Planned* (ADR-0046). The identifier of one *operation*: one fork request, from its first call until it is `ready`, `failed`, or `cancelled`, across every retry. It is 16 random bytes the client chooses, printed as 26 lowercase base32 characters, and the CLI prints it before it calls.

- ***Caller intent*** is the request as given, a relative expiry included; the ***resolved state*** is what the first request settled: the source's incarnation, the fork's incarnation, the absolute expiry. Neither is decided twice.
- ***Resuming*** reads the operation by its ID and follows it; nothing is sent or resolved again.
- ***Resubmitting*** sends the request again under a known ID, and compares the caller intent only: from the same principal and unchanged, it returns the existing operation; from another principal, it is refused without revealing the operation; changed, it is refused, naming the field.
- **Retention.** Control state keeps the operation's record, and with it the ID, while the fork is pending and for 7 days after its terminal outcome; after that the ID is unknown, and the same request starts a new fork.
- **Applies to:** daemon, CLI, UI.
- **Code:** `OperationId`, `ForkIntent`, `OperationRecord`, `State::fork_operations`, `Control.GetForkOperation` and `ForkOperationStatus`, `Rejection::OperationConflict`, `Rejection::OperationNotYours`. **CLI:** none yet (*planned:* `--operation-id` on `db fork` and on `orphan remove` for a staged copy; `operation_id` in JSON; MCP `operation_id`).
- **Not:** [request ID](#request-id) (one call, for tracing), job ID, task ID, idempotency key, transaction ID.

### Organization

*Planned* (ADR-0044). A namespace of principals and databases in a fleet that serves several: its databases are under `<org>/`, its principals are named `<org>_<name>` (never with `.`, which is reserved for lineage), and its administrators edit its groups and access roles. A fleet that serves one organization needs none. `org-` in a database name is the organization-wide tier.

- **Applies to:** daemon, CLI, UI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** tenant (a naming convention for an application's customers), account, workspace, project.

### Orphan

A copy of a database that a data node keeps on disk, closed, because the control plane doesn't know the database, or holds the name only at a lower epoch, and the node saw no drop (ADR-0042). A node deletes a copy only after a control node tells it of the drop; an orphan stays until an operator removes it. It is neither served nor replicated, is listed in `status --node`, and counts in `fleetring_databases_orphaned`; a creation or restore of its name on that node is refused. A [staged copy](#staged-copy) whose fork the node can't confirm is kept as an orphan too, including when a placement shows the name with another [incarnation](#incarnation); it is listed and removed by its name, incarnation, operation ID, and attempt (ADR-0046).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Replication::orphan`, `data::Orphan`, `data::StagedOrphan`, `Client::remove_staged_orphan`, protocol `OrphanStatus`, `StagedOrphan`, and `Database.RemoveOrphan`. **CLI:** `fleetring orphan list`, `fleetring orphan remove <database> --node <id>`.
- **Not:** stale copy, leftover, stray, zombie, dangling replica.

### Overlay

*Planned* (ADR-0044). A reader's own database beside an installed package, holding its notes, corrections, and private constraints about it. It is never merged into the package; upgrading the package replaces the package's database and leaves the overlay alone.

- **Applies to:** docs, CLI, UI.
- **Code:** none. **CLI:** none.
- **Not:** patch, layer, fork (an overlay starts empty).

### Owner

*Planned* (ADR-0044). The principal an agent acts for and answers to: the principal the command that created the root agent ran as, or `authority` when only the authority's key was used (ADR-0043). A child agent inherits its parent's owner. Recorded in the agent's identity record and its token; it grants nothing. Only agents have owners: a database has a [creator](#creator), a lease a [holder](#holder).

- **Applies to:** daemon, CLI, UI.
- **Code:** `AgentRecord::owner`, `owner` in `identity.toml` (ADR-0043); none yet in tokens. **CLI:** `agent show`.
- **Not:** creator, sponsor, user (as a field).

### Package

*Planned* (ADR-0044). A database a publisher releases in versions for others to read or fork. A *package version* is described by a *package manifest*: the publisher, the package, the version, the engine, the digests of its files, its position and chain hash, and the previous version's manifest digest, signed by the publisher's key and verifiable offline with that key alone. Each manifest has a sequence that only grows and an expiry, and a publisher may sign a *latest statement*, valid for a day at most, naming its newest manifests, so readers detect rollback and freeze. "Verified" means the publisher's identity, never the content's truth or safety, and a package's content is untrusted data.

- **Applies to:** daemon, CLI, UI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** release (a [release](#release) is a version of the `fleetring` binary), dataset, bundle, image.

### Parent

*Planned* (ADR-0044). The agent that spawned an agent, recorded by agent ID in the child's `identity.toml` and by principal in the delegation block that created its token. A child's ID extends its parent's with `.` (`42.c7`), so its databases, `agent-42.c7-…`, fall under its parent's subtree scope `agent-42.*`. It grants nothing by itself.

- **Applies to:** daemon, CLI, UI.
- **Code:** `AgentRecord::parent`, `parent` in `identity.toml` as the parent's agent ID (ADR-0043), always absent in 0.7. **CLI:** `agent show`.
- **Not:** creator, owner, ancestor (fine in prose for any agent above).

### Placement

The deterministic mapping from a key to its replica set (and, with cells, its cell). Placement is a wire format: changing which nodes a key maps to needs a new spec version (ADR-0008).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Placement` trait, `CellPlacement`, protocol `Placement`. **CLI:** `fleetring db locate`.
- **Not:** sharding, allocation, scheduling, routing (routing sends a request where placement says).

### Position

A point in a database's log, defined by the engine and opaque above the adapter. Positions compare byte-wise, so control code orders them without knowing the engine.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Position`, `engine::Position`. **CLI:** positions in `db locate`, `db verify --at <position>`.
- **Not:** offset, LSN, frame number, index (all engine-specific), cursor.

### Preference list

A key's nodes in placement order: zone-diverse first, then host-diverse, then the rest (ADR-0006). The replica set is its first R entries, and failover order follows it.

- **Applies to:** crate.
- **Code:** `Ring::preference`, `Preference`. **CLI:** none.
- **Not:** candidate list (candidates are a promotion's), ranking.

### Preferred primary

The first node of a key's replica set: the primary when it is live and holds the lease. The lease, not the ring, decides the actual primary.

- **Applies to:** crate, daemon.
- **Code:** `ReplicaSet::primary` in the crate. **CLI:** none.
- **Not:** owner, home node, leader.

### Primary

The replica that holds a database's lease at its current lease epoch, and the only one that accepts writes.

- **Applies to:** daemon, CLI, UI.
- **Code:** `ReplicaRole::Primary`. **CLI:** the `PRIMARY` column in `db list`; `db list --node <node-id>` lists one node's primaries.
- **Not:** leader, master, writer (fine in prose about write ownership), owner, main.

### Principal

A named identity a client acts as, with the scopes its `[[principal]]` entry or its token grants (ADR-0025). *Planned* (ADR-0044): every principal has a *principal kind*, `user`, `agent`, or `service` (the default); a node is not a principal. An agent records an [owner](#owner) and, if another agent spawned it, a [parent](#parent).

- **Applies to:** daemon, CLI, UI.
- **Code:** `tls::Principal`, `[[principal]]`. **CLI:** `cert client <principal>`, `token issue --principal`, *planned* `--as <principal>` (FR-055).
- **Not:** user, account, role, service account, subject (the token claim `sub` holds it).

### Process file

`fleetringd.process` in a node's data directory, which a running node writes and removes when it stops: its process ID, binary, release, arguments, the directory it started in (from 0.8), restart count, and, when they apply, its [supervisor](#supervisor) and `started_by = "detach"` for a node `dev -d` started (ADR-0038, ADR-0043 point 5). A node runs when its process file names a process that holds the data directory's lock; a file whose process doesn't is stale. `upgrade` restarts a node through it, after the new release's `serve --check` accepts the node's arguments, run in that directory; `stop` and `dev stop` stop one. Its name is kept until 1.0.

- **Applies to:** daemon, CLI.
- **Code:** `upgrade::ProcessFile`, `upgrade::running`, `upgrade::Running`. **CLI:** `fleetring stop (--data-dir <dir> | --config <file>)`, `dev stop`, `dev status`.
- **Not:** PID file, lock file (that is `fleetringd.lock`), state file.

### Promotion

Making a follower the primary at the next lease epoch: the control plane starts it, the candidate fences a quorum, adopts the most advanced log, and completes it (ADR-0022). Failovers and transfers are both promotions.

- **Applies to:** daemon, UI.
- **Code:** `Promotion`, `StartPromotion`, `CompletePromotion`, `Replication.Promote`. **CLI:** "promotion" in `db locate`.
- **Not:** election, takeover, failover (when it is a transfer).

### Publisher

*Planned* (ADR-0044). An Ed25519 key with a name, held by a principal of the publishing fleet, that signs package manifests. A reader pins publisher keys in its trust store as provenance anchors; a publisher key never authorizes a request in any fleet.

- **Applies to:** daemon, CLI, UI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** author (that is a description's), vendor, maintainer, signer (alone).

### Quorum

A number of replicas that must agree: the *write quorum* W acknowledges a write, and the *promotion quorum* Q = R − W + 1 must be fenced before a candidate takes over. W + Q > R is what keeps acknowledged writes (ADR-0018).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Quorums`. **CLI:** `--acks quorum`.
- **Not:** majority (only for `acks = quorum`'s W), consensus, replica set.

### Readiness

Whether a node should be sent work now: `/readyz` passes when every check of its node roles passes (`joined`, `ring`, `denylist`, `draining`, `control_log`, `leader`). Health (`/healthz`) only says the process can serve.

- **Applies to:** daemon, UI.
- **Code:** `observe::health::Probes`, `Readiness`, `Check`. **CLI:** none.
- **Not:** liveness (a member's liveness is the control plane's view; see [liveness](#liveness)), status, up.

### Release

A published version of the `fleetring` binary (the `fleetringd` package), `MAJOR.MINOR.PATCH` with an optional prerelease such as `-rc.1`, built and signed by the release workflow from a `fleetringd-vX.Y.Z` tag. Its *release line* is `MAJOR.MINOR`; a fleet runs at most two adjacent lines at once (ADR-0038).

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Release`, `Release::line`, `Release::runs_beside`. **CLI:** `--version` (of `upgrade`), `version=` in `fleetring status`.
- **Not:** build, cluster version, version (alone, for a line).

### Replica

One database's copy on one node, the primary's copy included.

- **Applies to:** daemon, CLI, UI.
- **Code:** `HostedReplica`, protocol `Replica`. **CLI:** the `REPLICAS` column in `db list`.
- **Not:** follower (a follower is a replica that isn't the primary), copy (fine in prose), shard, instance, node.

### Replica role

Whether a replica is its database's primary or a follower, as the node hosting it reports.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::ReplicaRole`, protocol enum `ReplicaRole` (`REPLICA_ROLE_PRIMARY`, `REPLICA_ROLE_FOLLOWER`). **CLI:** `primary` or `follower` after each database in `status --node`.
- **Not:** role alone (say whose), state, mode.

### Replica set

The ordered list of distinct nodes that hold a key's replicas: the first R nodes of its preference list (ADR-0006).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `ReplicaSet`. **CLI:** "replicas" in output.
- **Not:** quorum (a subset), replica group, shard group, cohort.

### Replication factor

R: how many replicas a database has, the primary included. A replica set with fewer than R nodes is *under-replicated*. Set per database at creation; a creation that names none takes the fleet's default, 3 unless the control node's configuration says otherwise, and 1 in dev mode (ADR-0039).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `ReplicationFactor`. **CLI:** `--replication-factor`; the `RF` column in `db list`; config `[replication] replication_factor`, the fleet's default.
- **Not:** replica count, copies, redundancy.

### Request ID

The identifier of one request, kept from `x-request-id` or made as `<node-id>-<counter>`, carried in the `rpc` span and on the calls a node makes for the request (ADR-0034).

- **Applies to:** daemon.
- **Code:** `observe::RequestId`, `x-request-id`. **CLI:** none.
- **Not:** trace ID (until traces are exported), correlation ID, [operation ID](#operation-id) (one fork request across many calls).

### Restore

Building a database from an [archive](#archive) at a *restore point*: its latest point, a [position](#position), or a time (ADR-0029, point 10). A restore starts from a base [snapshot](#snapshot) and applies [segments](#segment), verifying each against the [hash chain](#hash-chain). Online, it creates a new database and never overwrites one; offline, it writes a directory with a plain SQLite database.

A restore also returns the [metadata](#metadata) the archive holds for the snapshot it started from, as the archive's claim: `db restore` sets the description without an author and the labels, and never the expiry (ADR-0041, point 10).

- **Applies to:** daemon, CLI, UI.
- **Code:** `durability::restore_into`, `RestoreTarget`, `Restored`, `RestorePlan`, `ArchivedMetadata`; protocol `Database.Restore`, `RestorePoint`, `RestoreResponse.archived_metadata`. **CLI:** `db restore`, `archive restore`, `--at-position`, `--at-time`.
- **Not:** recover (that is what a failover does), import, rollback, rewind.

### Revocation ID

*Planned* (ADR-0044). The SHA-256 of one block's signature in a biscuit token, base64url. Putting it on the denylist refuses every token that contains the block, so revoking a parent's token, or the block that delegated to a child, revokes everything derived from it.

- **Applies to:** daemon, CLI.
- **Code:** none yet (`Revoked::Block`). **CLI:** none yet.
- **Not:** token ID (that is `jti`), key ID.

### Ring

The placement structure over a set of nodes, with a strategy (weighted rendezvous by default, or a vnode ring), a seed, and a fingerprint. One per cell; one per fleet when there are no cells.

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Ring`, `RingBuilder`, `RingSpec`. **CLI:** "ring fingerprint" in `status`.
- **Not:** hash ring (fine in prose), cluster, topology, circle, keyspace.

### Rollback

Returning a node to the binary its last [upgrade](#upgrade) replaced, kept beside it as `<name>.previous`: `fleetring.previous` on a fresh install, and `fleetringd.previous` on a host upgraded from 0.6, whose real file is still `fleetringd` (ADR-0043, point 2). A rollback swaps the two, so a second one rolls forward; the fleet refuses one below a release whose [fleet feature](#fleet-feature) is enabled.

- **Applies to:** daemon, CLI, UI.
- **Code:** `upgrade::Mode::Rollback`. **CLI:** `fleetring upgrade --rollback`; the kept binary can also roll back on its own, as `<name>.previous upgrade --rollback`.
- **Not:** downgrade (for this), revert, restore (that builds a database from an archive).

### Router

The front door that speaks libSQL's Hrana protocol over HTTP, so existing libSQL and Turso clients reach each database's [primary](#primary) unchanged (ADR-0037). A separate process with its own identity: a client certificate for a principal every data node lists with `forward_tokens = true` and no scopes. It verifies each request's token and checks `read` before it calls any node, then forwards the token; each [Hrana stream](#hrana-stream) runs on one [session](#session) on the primary.

- **Applies to:** daemon, CLI.
- **Code:** `fleetringd::router`, `RouterConfig`; `forward_tokens = true` in its `[[principal]]` entry. **CLI:** `fleetring router --config <file>`.
- **Not:** proxy (fine in prose), gateway, load balancer, sidecar (fine for how it is deployed).

### Schema

A database's tables, views, indexes, triggers, and virtual tables with their definitions, as its engine adapter reports them, and capped row counts (ADR-0041, point 6). The primary answers; the engine's own tables and reserved objects are left out unless asked for.

- **Applies to:** daemon, CLI, UI.
- **Code:** `engine::Schema`, `Database.Schema`, `data::SchemaView`, `report::SchemaReport`. **CLI:** the schema section of `db show`, with `--no-schema`, `--count-limit`, and `--all`.
- **Not:** catalog, DDL (fine in prose), spec (that's placement's).

### Scope

`<action>:<pattern>`, where the pattern is an exact database name or a prefix ending in `*`, or the [fleet scope](#fleet-scope). Default deny (ADR-0025). What an RPC needs is stated in the same words: `admin on the database`, `read per database listed, or fleet`.

- **Applies to:** daemon, CLI, UI.
- **Code:** `auth::Scope`, `Scopes`; narrowing is `Scopes::intersect` and `Scopes::grant_step` (`grant_step_strict` refuses rather than narrow). **CLI:** `--scope`, `scopes = […]` in `[[principal]]`.
- **Not:** permission, role, policy, ACL. A token's contents are its *token spec* (`auth::TokenSpec`), not a "grant"; `grant` is the [action](#action).

### Scraper

A principal whose client certificate may read `/metrics` on an [admin listener](#admin-listener) with TLS (`[admin] scrapers`). It has no scope on the gRPC port unless a `[[principal]]` entry gives it one (ADR-0034).

- **Applies to:** daemon.
- **Code:** `AdminConfig::scrapers`. **CLI:** `fleetring cert client <principal>` issues its certificate.
- **Not:** collector, exporter, monitor.

### Segment

One immutable archive object holding a run of delivered batches between two positions (ADR-0029): one delivery of an archive's change stream, with the hash chain at both ends.

- **Applies to:** daemon, CLI.
- **Code:** `durability::SegmentName`, `SegmentHeader`, `encode_segment`; objects under `<prefix>/<db>/segments/`. **CLI:** `segments` in `archive show` and in a restore's report.
- **Not:** WAL file, log file, chunk, part.

### Session

The data plane's half of a [Hrana stream](#hrana-stream): one `Database.Session` stream from a [router](#router) to a database's [primary](#primary), in which statements run with SQLite's own transaction semantics on the primary's writer, so a transaction can span several requests (ADR-0037). A session without `write` on its database is read-only. Its open transaction holds the writer, within `[sessions]`, and rolls back when the session ends.

- **Applies to:** daemon.
- **Code:** `Database.Session`, `engine::SqlSessions`, `engine::SessionCall`, `SessionId`, `[sessions]`. **CLI:** none.
- **Not:** connection (the HTTP or gRPC connection under it), transaction (a session can hold several, one at a time), cursor.

### Signed head

*Planned* (FR-074, ADR-0044). A database's chain hash at a position, signed by the node keys of a write quorum of its replicas, so anyone with the fleet's authority certificate can check offline that the fleet held that history. FR-074's ticket calls it a co-signed checkpoint; "checkpoint" stays the engine's.

- **Applies to:** daemon, CLI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** checkpoint, attestation, seal.

### Sink

Where a [change stream](#change-stream) delivers: a `file`, a `webhook`, or an archive. A sink's *position* is how far it has delivered (ADR-0028). Only change streams have sinks; replication hands batches to a [batch publisher](#batch-publisher).

- **Applies to:** daemon, CLI, UI.
- **Code:** `ChangeSink`, `[[changes.sink]]`, protocol `ChangeSinkStatus`. **CLI:** sink progress in `status --node`.
- **Not:** consumer (a consumer reads from a sink's destination), target, destination (fine in prose), subscriber, exporter, publisher.

### Skill

The page an AI agent reads to use FleetRing well, [`docs/agents/SKILL.md`](agents/SKILL.md), in the Agent Skills format: when to create a [database](#database) rather than a table, naming, [labels](#label), [descriptions](#description), [expiry](#expiry), and SQL patterns (ADR-0041, point 11). Its SQL runs through the SQLite adapter in the tests. Say *agent skill* where "skill" alone is unclear.

- **Applies to:** daemon.
- **Code:** `docs/agents/SKILL.md`, with `name: fleetring` in its front matter; `tests/agents_skill.rs`. **CLI:** none.
- **Not:** prompt, playbook, instructions (the MCP server's `instructions` are a different, shorter text), guide (that is the [quickstart](agents/quickstart.md)'s job).

### Snapshot

A consistent copy of state at one point. Always say which: a *database snapshot* seeds or re-seeds a replica (FR-032); a *base snapshot* starts an archive's restore chain (ADR-0029); a *control snapshot* compacts the control log. The leases a node holds when a `WatchLeases` stream opens aren't a snapshot; they are its *held leases*.

- **Applies to:** daemon, UI.
- **Code:** `engine::DatabaseSnapshot`, `Replication.Snapshot` (whose messages, `SnapshotRequest` and `SnapshotChunk`, only ever carry a database snapshot); `durability::SnapshotDescriptor`, `SnapshotEntry`, and protocol `ArchivedSnapshot` for base snapshots; the control log's `snapshots` table and its "control snapshot" errors; protocol `HeldLeases` for the leases. **CLI:** `snapshots` in `archive show`, and `snapshot` in a restore's report, both base snapshots.
- **Not:** backup, dump, image, checkpoint.

### Software

What a member reports it runs: its [release](#release), the newest protocol and control-log format it speaks, and the [fleet features](#fleet-feature) it supports. A member that reports none runs 0.3.0 (ADR-0038).

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Software`, `Member::software`, `Member::running`; protocol `Software` on `JoinRequest`, `HeartbeatRequest`, and `Member`. **CLI:** `version=` in `fleetring status`.
- **Not:** build info, version info, capabilities.

### Spec

The placement spec: the normative description of placement that every implementation, in any language, must reproduce, with golden vectors (ADR-0008, [`spec/placement-v1.md`](spec/placement-v1.md)). Also the serializable form of a ring (`RingSpec`) or of cells (`CellsSpec`).

- **Applies to:** crate.
- **Code:** `RingSpec`, `CellsSpec`, `NodeSpec`. **CLI:** none.
- **Not:** schema, config, manifest.

### Staged copy

A [fork](#fork)'s database snapshot as one of its replicas holds it before the fork is ready (ADR-0046): its files written to a temporary directory under `<data_dir>/staged/`, in a directory named by the fork identity and the attempt, checked against the stream's checksum and by the engine, fsynced and renamed into place, with a sidecar naming the fork identity, the attempt, the position, the source primary's chain hash at it, the source's [incarnation](#incarnation), the stream's size and checksum, the files, and [log epoch](#epoch) 0; the sidecar's rename commits it. A node reports it only once it is in place. It is never served. It becomes a database's files only when its holder, after checking it again against the fork's recorded position and chain hash, takes the fork's first lease, is promoted, or installs it as a follower on its first stream; a copy that fails that check is reported lost. Promotion ranks it at log epoch 0. A node deletes one only on a positive signal for its own fork identity (a drop notice naming it, or that identity failed, ready, completed without this node, or moved to a later attempt), and keeps any other as an [orphan](#orphan).

- **Applies to:** daemon, CLI.
- **Code:** `StagedCopy`, `CopyLost`, the `staged` and `copy_lost` reports in heartbeats, `SnapshotRequest.staged`, `DropCopy`'s incarnation and operation; on data nodes `replication::fork::staged` (`StagedStore`, `StagedKey`, `Sidecar`), `replication::fork::seed`, and `data::ForkKeeper`. **CLI:** none yet (*planned:* `holders` in `db show`; `kind` `staged` in `orphan list`).
- **Not:** partial copy (a staged copy is whole, or not reported), temporary copy, snapshot (alone), fork (the fork is the database).

### Statement deadline

When a write, query, or schema read must stop on its [primary](#primary): the data node's `[limits] statement_timeout_ms` after the node takes the request, or, if earlier, the call's own deadline less a small margin, so the node answers first (FR-078). At the deadline, or once the call is cancelled, the node raises the request's *interrupt*, a flag the engine checks while the statement runs; a write it stops rolls back. Time stays with whoever raises the flag, so engines never read a clock.

- **Applies to:** daemon, CLI.
- **Code:** `engine::Interrupt`, `engine::Interruption`, `EngineError::Interrupted`, `LimitsConfig::statement_timeout_ms`, `Client::with_call_timeout`, `mcp::STATEMENT_TIMEOUT`. **CLI:** `--timeout` on `db exec` and `db query`; `statement interrupted: …` in messages.
- **Not:** query timeout (it covers writes too), statement timeout in prose (the setting's name), kill, abort (a gRPC code that means the outcome is unknown).

### Supervisor

The service manager that started a node and restarts it: systemd (a system or user unit) or launchd (a Homebrew service). A node records one in its [process file](#process-file) only when the supervisor's variable is set *and* the supervisor is its parent, since shells inherit the variables. `stop` and `dev stop` refuse a supervised node, naming the supervisor's own stop command, unless `dev -d` started it (ADR-0043, point 5.5).

- **Applies to:** daemon, CLI.
- **Code:** `upgrade::Supervisor`, `ProcessFile::supervisor`. **CLI:** the hint `systemctl stop <unit>` or `brew services stop fleetring`.
- **Not:** service manager (in prose about one product, its own name), init system, daemon manager.

### Surface

One of the three ways people use FleetRing: the CLI, the TUI, and the desktop app. All three use the same client crate, RPCs, and words (ADR-0033).

- **Applies to:** CLI, UI.
- **Code:** the `fleetring` commands, in the `fleetringd` crate; *planned* `fleetring ui`, `apps/desktop`. **CLI:** `fleetring ui` (*planned*, FR-057).
- **Not:** frontend, client (a client is any caller), interface, dashboard (fine for one view).

### Tenant

Not a FleetRing object. A tenant is a naming convention for keys, `tenant:<name>/…`, that scopes and directories can match by prefix.

- **Applies to:** docs and examples.
- **Code:** none: no tenant type, flag, or RPC. **CLI:** none.
- **Not:** a synonym for database, principal, or cell.

### Token

A bearer credential: an Ed25519-signed JWT from the authority that names a principal, its scopes, and its lifetime. Its ID is `jti`; the signing key's ID is `kid`. *Planned* (ADR-0044): a token may also be a biscuit in FleetRing's fixed vocabulary, which a holder can narrow and [delegate](#delegation) without the authority.

- **Applies to:** daemon, CLI, UI.
- **Code:** `auth::BearerToken`, `TokenSpec` (what a token says: principal, scopes, ID, lifetime), `TokenId`, `KeyId`. **CLI:** `fleetring token issue|revoke`, `--token`, `FLEETRING_TOKEN`, `--token-id`, `--key-id`.
- **Not:** API key, password, secret, session, JWT (fine where the format matters).

### Transfer

Handing a database's lease from its holder to a follower at the next epoch without waiting for expiry, as a drain does. A transfer is a promotion, not a failover.

- **Applies to:** daemon, UI.
- **Code:** `Control.TransferLease`. **CLI:** none yet ("hands over" in drain output).
- **Not:** handoff (as a name; "hand over" is fine in prose), switchover, migration (moving the data itself), step down.

### Trust anchor

*Planned* (ADR-0044). A public key a fleet or a reader trusts, with a purpose and a constraint. Authorization anchors sign tokens: the fleet's authority (unconstrained), issuer keys, and other fleets' authorities (each limited to the patterns and actions it may vouch for). Provenance anchors, publisher keys, only verify content and never authorize.

- **Applies to:** daemon, CLI.
- **Code:** none yet. **CLI:** none yet.
- **Not:** root (alone), CA (an [authority](#authority) is one kind of anchor), trust root.


### Trusted local development

Running agents without an [agent launcher](#agent-launcher) (ADR-0044, point 11): each agent's FleetRing requests are scoped, but agents running as the same OS user can read each other's credential files and database files, so it isn't secure separation between agents. 0.7's `fleetring mcp --agent` runs in this mode, and says so.

- **Applies to:** docs, CLI.
- **Code:** none. **CLI:** the line `trusted local development: agents on this machine can read each other's credentials`, which `mcp --agent` prints at start and `agent add` and `agent show` print; the help of `mcp` and `agent`.
- **Not:** sandbox, secure mode, isolated mode.
### Upgrade

Moving a node to another [release](#release): verify the release's signature and checksum, swap the binary atomically, and restart the node in place after a [drain](#drain), within the fleet's window of release lines (ADR-0038). A *rolling upgrade* moves a fleet one node at a time.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetringd::upgrade`, `upgrade::run`, `ProcessFile`, `Stop::Restart`. **CLI:** `fleetring upgrade`, `--version`, `--check`, `--no-restart`, `--rollback`, `--software`; `SIGHUP` restarts a daemon in place. A 0.6 binary installs 0.7 under its old name, `fleetringd`, and a 0.7 `upgrade` then adds the `fleetring` link beside it (ADR-0043, point 3).
- **Not:** update, self-update, patch (a patch is one kind of release).

### Watcher

A client following one database's recorded row changes live, through `db watch` (ADR-0045): served by the database's primary from the same change table logical [sinks](#sink) read, one committed transaction at a time, live or after a [position](#position) the primary still holds every change after. Unlike a sink, a watcher keeps no [mark](#mark) and holds back no [checkpoint](#checkpoint); while it is connected it holds back the pruning of the changes it hasn't read. Its position is the last transaction it printed. The change table's *floor* is the position from which the primary holds every recorded change. The [MCP server](#mcp-server)'s `changes` tool is a watcher for the length of one call, at most 2 seconds, and returns the position to continue from, which nothing holds between calls (FR-081).

- **Applies to:** daemon, CLI.
- **Code:** `changes::Watcher`, `ChangeStreams::watch`, `MAX_WATCHERS`, `LogicalChanges::transactions_after`, protocol `Database.WatchChanges`. **CLI:** `fleetring db watch <db> [--from <position>]`; MCP `changes`.
- **Not:** subscriber, consumer (a consumer reads what a watcher prints), tail, listener, cursor.

### Weight

A node's share of keys relative to the others in its ring; a cell's share in its selector ring is set explicitly, never summed from its nodes (ADR-0012).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Weight`, `[node] weight`. **CLI:** `weight=` after each member in `status`.
- **Not:** capacity (fine in prose), size, priority.

### Zone

A placement label for a failure domain (a room, a rack, a cloud availability zone); a cell's ring spreads replicas across zones before hosts. A node without one is in the default zone. Regions are cells, not zones (ADR-0006, point 2).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Zone`, `[node] zone`. **CLI:** `zone=` after each member in `status`.
- **Not:** region, datacenter (as the label's name; fine as a value), AZ (as the label's name), rack (same), cell.

## Renamed in 0.3.0

0.3.0 gave each thing one name, with no aliases (ADR-0032; FR-059 and FR-062, whose breaking-changes list is the full record). Accepted ADRs and finished tickets keep the old words; read them with this table.

| Old word | Word now |
|---|---|
| cluster, `ClusterName`, `--cluster`, the `cluster` scope, `sim::Cluster` | [fleet](#fleet), `FleetName`, `--fleet`, the `fleet` scope, `sim::Fleet` |
| `fleetringd::Role` | [node role](#node-role), `NodeRole` |
| `fleetring_control::Role`, protocol `Role` | [replica role](#replica-role), `ReplicaRole` |
| `--node <host:port>` | `--address <host:port>`; `--node` takes a node ID |
| `watch-leases` | `lease watch` |
| `cert node --hosts` | `cert node --addresses` |
| `ReplicationSink` | [batch publisher](#batch-publisher), `BatchPublisher` |
| durability sink (ADR-0016) | [archive](#archive) |
| `engine::Snapshot` | `engine::DatabaseSnapshot` |
| `LeaseSnapshot`, `LeaseUpdate::Snapshot` | `HeldLeases`, `LeaseUpdate::Held` |
| "admin on the key", `Target::Key`, `require_key` | "admin on the database", `Target::Database`, `require_database` |
| departure | [leave](#leave) |
| up | [live](#liveness) |

`Node` and `Member` in the protocol were never a conflict: a [member](#member) is the record of a [node](#node).

## Renamed in 0.7.0

0.7.0 named the binary `fleetring` (ADR-0043). These are clean breaks, apart from two compatibility names kept until 1.0 (`fleet`, a third name for the binary, is permanent and not one of them): `fleetringd` as a second name for the binary, and a node started with `--config <file>` and no `serve`. Each old spelling fails with a `hint:` naming the new one.

| Old word | Word now |
|---|---|
| `fleetringd …` (the binary) | `fleetring …`; `fleetringd` stays a name for it until 1.0 |
| `fleetringd --config <file> [daemon flags]` | `fleetring serve --config <file> [daemon flags]`; the old form still runs a node until 1.0, with a warning |
| `db archive show <db> --archive <a>` | `archive show <db> --archive <a>` |
| `db archive verify <db> --archive <a>` | `archive verify <db> --archive <a>` |
| `archive restore --from <db> --to <dir>` | `archive restore <db> --to <dir>` |
| `ghcr.io/fleetring/fleetringd` | `ghcr.io/fleetring/fleetring`; the old name is pushed too until 1.0 |
| `deploy/dev/fleetringd-dev.service`, `homebrew.mxcl.fleetringd` | `deploy/dev/fleetring-dev.service`, `homebrew.mxcl.fleetring` |
| `fleetringd.previous` (the kept binary) | `<name>.previous`, beside the real file |
| sourcing `~/.fleetring/env` to reach a dev node | the `dev` [context](#context); the `env` file is still written, for scripts |

Kept through 0.x: the `fleetringd` package and crate (`-p fleetringd`, `fleetringd::…`, the `fleetringd::audit` log target), the `fleetringd-v` release tags, the `fleetringd-<version>-<target>` archives, `fleetringd.toml`, `<data_dir>/fleetringd.process`, `<data_dir>/fleetringd.lock`, `FLEETRINGD_RESTARTS`, and `FLEETRINGD_BASELINE_VERSION` and `FLEETRINGD_BASELINE_BIN` in the rolling-upgrade test (ADR-0043, point 1.7).
