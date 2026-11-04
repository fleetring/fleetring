# Lexicon

The names FleetRing uses, in code, docs, the CLI, the TUI and desktop app, and tickets. This page is the single source of truth: when two places disagree, this page wins, and the other place is fixed or gets a rename ticket. A new concept gets its entry here before it gets a type, a flag, or a column header.

Each entry gives a definition, where the word applies, its canonical spelling in code and in the CLI, and the words not to use for it. "Applies to" uses four places:

- **crate**: the `fleetring` placement crate, which says "key", never "database" (ADR-0011).
- **daemon**: `fleetringd` and the `fleetring-control`, `fleetring-wal` and `fleetring-proto` crates.
- **CLI**: the `fleetringd` subcommands (see the [CLI style guide](cli.md)).
- **UI**: the TUI and the desktop app (ADR-0033).

A word marked *planned* names something decided but not yet built. Accepted ADRs and finished tickets keep the words they were written with; [renamed in 0.3.0](#renamed-in-030) maps the old words to these.

## Hierarchy

```text
fleet                 one control plane, one certificate authority, one fleet name
└── cell              optional; partitions keys: each key lives in exactly one cell
    │                 (cells nest: a region can be a cell of cells)
    └── zone          a failure-domain label; a cell's ring spreads replicas across zones first
        └── host      a machine label; then across hosts
            └── node  one fleetringd process, a member of the fleet and of its cell's ring
```

- **Cells partition; zones and hosts spread** (ADR-0012, fact 1). A directory or a ring picks a key's cell, and the cell's ring picks its nodes. Zones and hosts are labels on a ring's nodes, not layers: one key's replicas sit in several zones of one cell.
- **A host belongs to exactly one zone**, and a node without a host label is its own host (ADR-0006, point 2).
- **A fleet without cells is one cell**: one ring over all its nodes. That is the only shape `fleetringd` runs today; cells exist in the placement crate (FR-020).
- **A ring places keys onto nodes.** In `fleetringd` the key is the database's name. The ring's preference list for a key starts with its replica set, the first R nodes; the first of those is the preferred primary. A lease makes one replica the primary at an epoch, and the other replicas are its followers.

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
| role (alone) | [node role](#node-role) or [replica role](#replica-role) | |
| API key, password | [token](#token) or [certificate](#certificate) | |
| health (for a member's liveness) | [liveness](#liveness) | `/healthz` is about the process; liveness is the control plane's view. |

## Terms

### Acknowledgement mode

How many replicas must store a write before it is acknowledged: `quorum` (a majority, the default), `all`, or `local` (the primary alone; a failover may lose acknowledged writes). Set per database (ADR-0023).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Acks`, `ACKS_QUORUM` in the protocol. **CLI:** `--acks quorum|all|local`; config `[replication] acks`.
- **Not:** consistency level, write concern, sync mode.

### Action

The verb half of a [scope](#scope): `read`, `write`, `admin`, or `stream`. `write` implies `read`, `admin` implies both, and `stream` stands alone (ADR-0025).

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

### Archive

A database's history kept for recovery: a physical [change stream](#change-stream) plus base [snapshots](#snapshot), written as immutable objects to an object store, with point-in-time [restore](#restore) (ADR-0029, FR-049). Separate from replicas: replicas give availability, an archive gives recovery. Its objects are [segments](#segment), and each base snapshot's *chunks* and *descriptor*; an object's name is its *object key* ([key](#key)).

- **Applies to:** daemon, CLI, UI.
- **Code:** `[[archive]]` in config, `durability::Archive`, `ArchiveConfig`, `ArchiveKeys`, `ObjectStore`, `S3Store`, `DirectoryStore`, `MemoryStore`. **CLI:** `--archive <name>` on `db restore`, `db archive show`, `db archive verify`, and `archive restore`.
- **Not:** backup (as the mechanism's name), dump, export, durability sink. ADR-0016, point 3, says "durability sink" for the same idea; the ADR is accepted and keeps its words, and everything else says archive.

### Audit line

One log line on the `fleetringd::audit` target per request a principal makes: debug when allowed, a warning when refused. It names the principal, the credential (`certificate` or `token:<id>`), the action, the database, and the RPC, never the token.

- **Applies to:** daemon, UI.
- **Code:** target `fleetringd::audit`. **CLI:** none; read it from the node's logs.
- **Not:** audit event, access log, audit trail (for a single line).

### Authority

The fleet's certificate authority: an Ed25519 key (`ca.key`) and its certificate (`ca.crt`). It issues every node and client certificate and signs every token. Its key belongs on no node.

- **Applies to:** daemon, CLI, UI.
- **Code:** `tls::Authority`. **CLI:** `fleetringd cert init`; files `ca.crt` and `ca.key`; flag `--ca`.
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

### Binding

What a node making a node-to-node call must be for the database or node the request names: `self`, `replica`, `holder`, `candidate`, `holder or candidate`, or `follower` (ADR-0031). A call that fails its binding is refused before it touches state.

- **Applies to:** daemon.
- **Code:** `replication::binding`, metadata `fleetring-binding`, the `Binding:` lines in the protocol. **CLI:** none.
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
- **Code:** `tls::Identity` (a certificate, its private key, and the trusted authorities), `SubjectUri`. **CLI:** `fleetringd cert node`, `fleetringd cert client`; files `node.<id>.crt`, `client.<principal>.crt`; flags `--cert`, and `--key` for the certificate's private key.
- **Not:** cert (fine in commands and files, not in prose), key pair (the certificate and its private key together are an *identity*), credential (a credential is a certificate *or* a token).

### Change stream

The ordered, at-least-once flow of one database's acknowledged batches to a [sink](#sink), beside replication and never in a write's acknowledgement path (FR-039). *Logical* format sends row changes; *physical* sends the opaque batches.

- **Applies to:** daemon, CLI, UI.
- **Code:** `changes::`, `ChangeSink`, `Format::Logical`/`Physical`, `CaptureMode`. **CLI:** `[[changes.sink]]` in config; sink progress in `status --node`.
- **Not:** changefeed, CDC (in names), event stream, replication stream (that is node-to-node).

### Checkpoint

The engine folding its log into the main database file (SQLite's WAL checkpoint). A follower behind its primary's last checkpoint needs a snapshot. Above the adapter it is only an opaque marker.

- **Applies to:** daemon.
- **Code:** `CheckpointMarker`. **CLI:** none.
- **Not:** compaction (reserved for the control log), flush, sync.

### Client

Anything that calls the fleet's RPCs as a [principal](#principal): the CLI, the TUI, the desktop app, an application, the router. Clients share one crate (ADR-0033).

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetringd::data::Client` today; `fleetring-client` *planned* (FR-056). **CLI:** `fleetringd cert client`.
- **Not:** user, consumer (a change stream has consumers), caller (fine in prose about one RPC).

### Context

*Planned* (FR-056). A named set of client settings: a fleet name, the control address, the TLS files, and a default principal, so a command needs `--context home` instead of repeating flags.

- **Applies to:** CLI, UI.
- **Code:** `fleetring_client::Context`. **CLI:** `fleetringd context add|use|list`, `--context`, `FLEETRING_CONTEXT`.
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

### Data node

A node that runs the data role: it hosts replicas and serves database calls. A node with `role = "both"` is a data node and a control node.

- **Applies to:** daemon, CLI, UI.
- **Code:** `NodeRole::Data`, `role = "data"`. **CLI:** "data node" in help; `status --node <node-id>`.
- **Not:** storage node, worker, server.

### Database

One SQLite database (later libSQL, Turso, or Postgres) that FleetRing places, replicates, and fails over as a unit. Its name is its placement [key](#key), but the daemon, its scopes, and its messages say "database": `admin on the database`, `auth::Target::Database`. The placement crate never says "database".

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Database`, `DatabaseName`. **CLI:** the noun `db` (`fleetringd db create`), the argument `<database>`; "database" in output headers.
- **Not:** shard, partition, tenant, keyspace, namespace, table, instance.

### Denylist

The set of revoked tokens and signing keys the control plane keeps and publishes to data nodes with every heartbeat reply. A node refuses a token on it.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Revocations`, `Revoked`, protocol `Revocation`. **CLI:** `fleetringd token revoke`.
- **Not:** blocklist, blacklist, revocation list, CRL.

### Directory

In cells: the explicit rules, by longest prefix or exact key, that pick a key's cell before the ring does, for residency and pinning (ADR-0012). Elsewhere the word means a filesystem directory (`--dir`, `data_dir`).

- **Applies to:** crate.
- **Code:** `Directory`. **CLI:** *planned*, with cells.
- **Not:** routing table, mapping, catalog, lookup table.

### Drain

Marking a member as draining: it gets no new leases and is never a promotion's candidate, and it hands every lease it holds to a follower through a [transfer](#transfer) (FR-038). A drained node stays a follower until it stops, restarts, or is *undrained*: its mark cleared, a [fleet feature](#fleet-feature).

- **Applies to:** daemon, CLI, UI.
- **Code:** `Control.Drain`, `Control.Undrain`, `Action::Undrain`, protocol `Member.draining`. **CLI:** `fleetringd node drain <node-id>`, `fleetringd node undrain <node-id>`; `[membership] on_shutdown = "leave"`.
- **Not:** evacuate, cordon, decommission, step down.

### Durability

A database's acknowledgement mode and replica floor together (ADR-0023). The *replica floor* (`min_replicas`) is how many replicas, the primary included, must be reachable for a write to run.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Durability`, `Acks`, `min_replicas`. **CLI:** `fleetringd db set-durability`, `--acks`, `--min-replicas`.
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

A control command that not every member can read or act on, refused until every member, live or down, reports it in its [software](#software). The first such command the fleet accepts enables the feature for good, and from then on a member that lacks it can't join (ADR-0038). The first is `undrain`.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Feature`, `Action::feature`, `State::features`, `State::check_features`, `Rejection::FeatureUnsupported`, `Rejection::MissingFeature`; protocol `Software.features`.
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

### Hash chain

One BLAKE3 hash per transaction over the replication log, each folding in the one before, so the hash at a position commits every byte before it (FR-044). One value of it is a *chain hash*.

- **Applies to:** daemon, CLI, UI.
- **Code:** `ChainHash`, `replication::chain`. **CLI:** `fleetringd db verify`, `--expect <hash>`.
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

### Host

A placement label for the machine a node runs on, when several nodes share one; replicas spread across hosts after zones (ADR-0006). "Host" also keeps its network sense inside `host:port`, but the names a node is dialed at are its [addresses](#address) (`cert node --addresses`).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Host`, `[node] host`. **CLI:** `host=` after each member in `status`.
- **Not:** node (a host can run several nodes), server, machine (fine in prose), instance, VM.

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
- **Code:** *planned*. **CLI:** *planned*, `fleetringd lease acquire|renew|release|watch`.
- **Not:** lock, mutex, distributed lock.

### Leader

The control node that accepts control commands. Today the only control node; with Raft, the elected one, and others redirect to it. The word is reserved for the control plane.

- **Applies to:** daemon.
- **Code:** `not_leader`, `LEADER_METADATA_KEY`. **CLI:** none.
- **Not:** primary (that is a database's), master, coordinator.

### Lease

The right of one node to write one database at one lease epoch, granted and renewed by the control plane (ADR-0018). A lease expires unless renewed; safety still never depends on clocks, because epochs fence.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Lease`, `AcquireLease`, `RenewLease`, `WatchLeases`, `HeldLeases`, `LeaseUpdate::Held`. **CLI:** `fleetringd lease watch --node <node-id>` or `--address <host:port>`, which prints `held`, `gained`, and `lost` lines.
- **Not:** lock, ownership, token, claim.

### Leave

A member removing itself from the fleet's membership, as a node does when it shuts down with `on_shutdown = "leave"`. An operator with the fleet scope makes the same call to remove another member.

- **Applies to:** daemon, CLI, UI.
- **Code:** `Control.Leave`, `Client::remove_node`. **CLI:** `on_shutdown = "leave"`; "left the fleet" in logs; `fleetringd node remove <node-id>` removes another member, refusing a live one unless `--force`.
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

### Member

A node as the fleet's membership records it: its ID, placement attributes, address, liveness, and whether it is draining. "Member" is the record; "node" is the process.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Member`, protocol `Member`, `WatchMembership`. **CLI:** "members" in `status`.
- **Not:** peer (fine in prose for "another node"), participant, server.

### Node

One `fleetringd` process: a member of the fleet, a ring member, a data node, a control node, or both. In the placement crate, a ring member with an ID, a weight, a zone, and a host.

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Node`, `NodeId`, `[node]`. **CLI:** the noun `node` (`fleetringd node drain`), `<node-id>`, and `--node <node-id>` in every command that takes one; a node's [address](#address) is `--address`.
- **Not:** server, instance, host (unless the machine is meant), worker, replica, peer (as a name), daemon (fine for the binary).

### Node role

What a daemon runs: `data`, `control`, or `both` (ADR-0015).

- **Applies to:** daemon, CLI.
- **Code:** `fleetringd::NodeRole`, `role` in config. **CLI:** `--role data|control|both`.
- **Not:** role alone (say whose), mode, type, kind, function.

### Placement

The deterministic mapping from a key to its replica set (and, with cells, its cell). Placement is a wire format: changing which nodes a key maps to needs a new spec version (ADR-0008).

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Placement` trait, `CellPlacement`, protocol `Placement`. **CLI:** `fleetringd db locate`.
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

A named identity a client acts as, with the scopes its `[[principal]]` entry or its token grants (ADR-0025).

- **Applies to:** daemon, CLI, UI.
- **Code:** `tls::Principal`, `[[principal]]`. **CLI:** `cert client <principal>`, `token issue --principal`, *planned* `--as <principal>` (FR-055).
- **Not:** user, account, role, service account, subject (the token claim `sub` holds it).

### Promotion

Making a follower the primary at the next lease epoch: the control plane starts it, the candidate fences a quorum, adopts the most advanced log, and completes it (ADR-0022). Failovers and transfers are both promotions.

- **Applies to:** daemon, UI.
- **Code:** `Promotion`, `StartPromotion`, `CompletePromotion`, `Replication.Promote`. **CLI:** "promotion" in `db locate`.
- **Not:** election, takeover, failover (when it is a transfer).

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

A published version of `fleetringd`, `MAJOR.MINOR.PATCH` with an optional prerelease such as `-rc.1`, built and signed by the release workflow from a `fleetringd-vX.Y.Z` tag. Its *release line* is `MAJOR.MINOR`; a fleet runs at most two adjacent lines at once (ADR-0038).

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Release`, `Release::line`, `Release::runs_beside`. **CLI:** `--version` (of `upgrade`), `version=` in `fleetringd status`.
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

R: how many replicas a database has, the primary included. A replica set with fewer than R nodes is *under-replicated*.

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `ReplicationFactor`. **CLI:** `--replication-factor`; the `RF` column in `db list`.
- **Not:** replica count, copies, redundancy.

### Request ID

The identifier of one request, kept from `x-request-id` or made as `<node-id>-<counter>`, carried in the `rpc` span and on the calls a node makes for the request (ADR-0034).

- **Applies to:** daemon.
- **Code:** `observe::RequestId`, `x-request-id`. **CLI:** none.
- **Not:** trace ID (until traces are exported), correlation ID.

### Restore

Building a database from an [archive](#archive) at a *restore point*: its latest point, a [position](#position), or a time (ADR-0029, point 10). A restore starts from a base [snapshot](#snapshot) and applies [segments](#segment), verifying each against the [hash chain](#hash-chain). Online, it creates a new database and never overwrites one; offline, it writes a directory with a plain SQLite database.

- **Applies to:** daemon, CLI, UI.
- **Code:** `durability::restore_into`, `RestoreTarget`, `Restored`, `RestorePlan`; protocol `Database.Restore`, `RestorePoint`. **CLI:** `db restore`, `archive restore`, `--at-position`, `--at-time`.
- **Not:** recover (that is what a failover does), import, rollback, rewind.

### Ring

The placement structure over a set of nodes, with a strategy (weighted rendezvous by default, or a vnode ring), a seed, and a fingerprint. One per cell; one per fleet when there are no cells.

- **Applies to:** crate, daemon, CLI, UI.
- **Code:** `Ring`, `RingBuilder`, `RingSpec`. **CLI:** "ring fingerprint" in `status`.
- **Not:** hash ring (fine in prose), cluster, topology, circle, keyspace.

### Rollback

Returning a node to the binary its last [upgrade](#upgrade) replaced, kept beside it as `fleetringd.previous`. A rollback swaps the two, so a second one rolls forward; the fleet refuses one below a release whose [fleet feature](#fleet-feature) is enabled.

- **Applies to:** daemon, CLI, UI.
- **Code:** `upgrade::Mode::Rollback`. **CLI:** `fleetringd upgrade --rollback`.
- **Not:** downgrade (for this), revert, restore (that builds a database from an archive).

### Router

*Planned* (MVP step 4). The front door that speaks the libSQL/Hrana protocol, so existing libSQL and Turso clients reach the right primary unchanged. It forwards its callers' tokens.

- **Applies to:** daemon, CLI.
- **Code:** *planned*; `forward_tokens = true` in its `[[principal]]` entry. **CLI:** *planned*.
- **Not:** proxy (fine in prose), gateway, load balancer, sidecar (fine for how it is deployed).

### Scope

`<action>:<pattern>`, where the pattern is an exact database name or a prefix ending in `*`, or the [fleet scope](#fleet-scope). Default deny (ADR-0025). What an RPC needs is stated in the same words: `admin on the database`, `read per database listed, or fleet`.

- **Applies to:** daemon, CLI, UI.
- **Code:** `auth::Scope`, `Scopes`. **CLI:** `--scope`, `scopes = […]` in `[[principal]]`.
- **Not:** permission, grant (a `Grant` is a token's content), role, policy, ACL.

### Scraper

A principal whose client certificate may read `/metrics` on an [admin listener](#admin-listener) with TLS (`[admin] scrapers`). It has no scope on the gRPC port unless a `[[principal]]` entry gives it one (ADR-0034).

- **Applies to:** daemon.
- **Code:** `AdminConfig::scrapers`. **CLI:** `fleetringd cert client <principal>` issues its certificate.
- **Not:** collector, exporter, monitor.

### Segment

One immutable archive object holding a run of delivered batches between two positions (ADR-0029): one delivery of an archive's change stream, with the hash chain at both ends.

- **Applies to:** daemon, CLI.
- **Code:** `durability::SegmentName`, `SegmentHeader`, `encode_segment`; objects under `<prefix>/<db>/segments/`. **CLI:** `segments` in `db archive show` and in a restore's report.
- **Not:** WAL file, log file, chunk, part.

### Sink

Where a [change stream](#change-stream) delivers: a `file`, a `webhook`, or an archive. A sink's *position* is how far it has delivered (ADR-0028). Only change streams have sinks; replication hands batches to a [batch publisher](#batch-publisher).

- **Applies to:** daemon, CLI, UI.
- **Code:** `ChangeSink`, `[[changes.sink]]`, protocol `ChangeSinkStatus`. **CLI:** sink progress in `status --node`.
- **Not:** consumer (a consumer reads from a sink's destination), target, destination (fine in prose), subscriber, exporter, publisher.

### Snapshot

A consistent copy of state at one point. Always say which: a *database snapshot* seeds or re-seeds a replica (FR-032); a *base snapshot* starts an archive's restore chain (ADR-0029); a *control snapshot* compacts the control log. The leases a node holds when a `WatchLeases` stream opens aren't a snapshot; they are its *held leases*.

- **Applies to:** daemon, UI.
- **Code:** `engine::DatabaseSnapshot`, `Replication.Snapshot` (whose messages, `SnapshotRequest` and `SnapshotChunk`, only ever carry a database snapshot); `durability::SnapshotDescriptor`, `SnapshotEntry`, and protocol `ArchivedSnapshot` for base snapshots; the control log's `snapshots` table and its "control snapshot" errors; protocol `HeldLeases` for the leases. **CLI:** `snapshots` in `db archive show`, and `snapshot` in a restore's report, both base snapshots.
- **Not:** backup, dump, image, checkpoint.

### Software

What a member reports it runs: its [release](#release), the newest protocol and control-log format it speaks, and the [fleet features](#fleet-feature) it supports. A member that reports none runs 0.3.0 (ADR-0038).

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetring_control::Software`, `Member::software`, `Member::running`; protocol `Software` on `JoinRequest`, `HeartbeatRequest`, and `Member`. **CLI:** `version=` in `fleetringd status`.
- **Not:** build info, version info, capabilities.

### Spec

The placement spec: the normative description of placement that every implementation, in any language, must reproduce, with golden vectors (ADR-0008, [`spec/placement-v1.md`](spec/placement-v1.md)). Also the serializable form of a ring (`RingSpec`) or of cells (`CellsSpec`).

- **Applies to:** crate.
- **Code:** `RingSpec`, `CellsSpec`, `NodeSpec`. **CLI:** none.
- **Not:** schema, config, manifest.

### Surface

One of the three ways people use FleetRing: the CLI, the TUI, and the desktop app. All three use the same client crate, RPCs, and words (ADR-0033).

- **Applies to:** CLI, UI.
- **Code:** `fleetringd` subcommands; *planned* `fleetringd ui`, `apps/desktop`. **CLI:** `fleetringd ui` (*planned*, FR-057).
- **Not:** frontend, client (a client is any caller), interface, dashboard (fine for one view).

### Tenant

Not a FleetRing object. A tenant is a naming convention for keys, `tenant:<name>/…`, that scopes and directories can match by prefix.

- **Applies to:** docs and examples.
- **Code:** none: no tenant type, flag, or RPC. **CLI:** none.
- **Not:** a synonym for database, principal, or cell.

### Token

A bearer credential: an Ed25519-signed JWT from the authority that names a principal, its scopes, and its lifetime. Its ID is `jti`; the signing key's ID is `kid`.

- **Applies to:** daemon, CLI, UI.
- **Code:** `auth::BearerToken`, `TokenId`, `KeyId`. **CLI:** `fleetringd token issue|revoke`, `--token`, `FLEETRING_TOKEN`, `--token-id`, `--key-id`.
- **Not:** API key, password, secret, session, JWT (fine where the format matters).

### Transfer

Handing a database's lease from its holder to a follower at the next epoch without waiting for expiry, as a drain does. A transfer is a promotion, not a failover.

- **Applies to:** daemon, UI.
- **Code:** `Control.TransferLease`. **CLI:** none yet ("hands over" in drain output).
- **Not:** handoff (as a name; "hand over" is fine in prose), switchover, migration (moving the data itself), step down.

### Upgrade

Moving a node to another [release](#release): verify the release's signature and checksum, swap the binary atomically, and restart the node in place after a [drain](#drain), within the fleet's window of release lines (ADR-0038). A *rolling upgrade* moves a fleet one node at a time.

- **Applies to:** daemon, CLI, UI.
- **Code:** `fleetringd::upgrade`, `upgrade::run`, `ProcessFile`, `Stop::Restart`. **CLI:** `fleetringd upgrade`, `--version`, `--check`, `--no-restart`, `--rollback`, `--software`; `SIGHUP` restarts a daemon in place.
- **Not:** update, self-update, patch (a patch is one kind of release).

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
