---
id: FR-062
title: One word per thing: resolve the lexicon's naming conflicts in code, CLI, protocol, and docs
status: in-progress
priority: P1
size: M
milestone: M11 Operator experience
depends_on: [FR-059]
adrs: [ADR-0032]
---

# FR-062: One word per thing: resolve the lexicon's naming conflicts in code, CLI, protocol, and docs

## Goal

The [lexicon](../lexicon.md) listed places where the code used one word for two things, or two words for one: `Role`, `--node`, `host`, `sink`, `snapshot`, `key`, and a few words in prose. Each meant a reader had to know which sense was meant. This ticket picks one name per thing, applies it everywhere at once, and empties the lexicon's "Open conflicts" table. It ships with FR-059 in 0.3.0, which is a clean break: no aliases, no deprecation paths, and the protocol renamed where its names conflicted (ADR-0032, as revised).

## Decisions

| Conflict | Chosen name | Why |
|---|---|---|
| `Role` for what a daemon runs and what a replica is | `fleetringd::NodeRole` (data, control, both); `fleetring_control::ReplicaRole` and protocol enum `ReplicaRole` (`REPLICA_ROLE_PRIMARY`, `REPLICA_ROLE_FOLLOWER`); the `DatabaseRole` import alias is gone | Each names whose role it is. The config key and flag stay `role` and `--role`, which only ever meant the node's. The control log stores the replica role as a lowercase variant name under the field `role`, which doesn't change. |
| `--node` took a node ID in `db list` and an address in `status`, `watch-leases`, and `db exec` | `--node <node-id>` everywhere; `--address <host:port>` where a command dials a node directly | One flag, one kind of value (CLI guide, section 2). `status --node`, `lease watch --node`, `db exec --node` and `db query --node` now look the node's address up in the control plane's membership. Node IDs may contain `:` and `.`, so a value isn't told apart by its form; when `--node` names no member and parses as an address, the error says to use `--address`. |
| `watch-leases`, a top-level verb-noun command | `lease watch` | The `noun verb` structure (CLI guide, section 1), and the noun `lease` that keyed leases (FR-043) will share. Done here rather than in FR-056, since `--node` changed under it anyway. |
| `host` for the machine label and for dial addresses | `host` stays the machine label (`[node] host`); `cert node --hosts` becomes `cert node --addresses` | The label is in config, in the placement crate, and on the wire (`Node.host`); the flag was the odd one out. "Address" already means `host:port` or a name a node is dialed at everywhere else. |
| `sink` for change-stream destinations and for where an actor's batches go | `sink` means only a change stream's destination (and later an archive); `ReplicationSink` becomes `BatchPublisher`, `RecordingSink` becomes `RecordingPublisher`, and the module `fleetringd::sink` becomes `fleetringd::publisher` | The trait's one required method is already `publish`, and it hands batches on to be replicated rather than storing them. `ReplicationFeed` reads as something consumed, and `BatchOutbox` suggests a durable queue it isn't. |
| Durability sink and archive for one thing | **archive** | Config already says `[[archive]]`, ADR-0029 builds archives, and "sink" is now the change stream's word. ADR-0016 keeps its wording, being accepted; the lexicon notes the mapping. |
| `snapshot` for four things | `engine::DatabaseSnapshot` (catch-up, was `engine::Snapshot`); "base snapshot" for an archive's restore start; "control snapshot" for the control log's compaction; the watch stream's first message is `HeldLeases` on the wire (was `LeaseSnapshot`) and `LeaseUpdate::Held` in Rust, matching the `held` lines `lease watch` prints | Each says which state it copies. `Replication.Snapshot`, `SnapshotRequest`, and `SnapshotChunk` keep their names: they only ever carry a database snapshot, inside the replication service. The control log has no snapshot type to rename; its docs and errors say "control snapshot". |
| `key` for the placement key, private keys, signing keys, and (in scope rules) database names | "key" alone is the placement key. In the daemon, scope rules and requirements say "database": `auth::Target::Database`, `Requirement::Database`, `Requirement::DatabaseOrFleet`, and the `Scope:` lines say "admin on the database". Other keys always carry their qualifier: "private key", "signing key". | Matches the crate (ADR-0011). `--key` and `--key-id` keep their names, being universal conventions; their help says "private key" and "signing key". `token revoke --key-id` prints "revoked signing key …". |
| `cluster` for the fleet | `fleet` | FR-059 and ADR-0032. |
| Departure and leave | "leave" in docs | FR-038's title stays, a ticket title being history. |
| Live and up | "live" and "down" in docs | Matches `Liveness::Live` and the CLI's output. |
| `Node` and `Member` in the protocol | No change | Already consistent: a member is the record of a node. |

## Breaking changes in 0.3.0

This list, with FR-059's, is the release notes' breaking-changes section. Nothing is kept as an alias.

- **CLI:**
  - `cert init --cluster <name>` is `cert init --fleet <name>`.
  - `cert node --hosts` is `cert node --addresses`.
  - `watch-leases --node <host:port>` is `lease watch --address <host:port>`, or `lease watch --node <node-id>` with `--control` or `--config`.
  - `status --node <host:port>` is `status --address <host:port>`; `status --node <node-id>` now takes a node ID and asks the control plane for its address.
  - `db exec|query --node <host:port>` is `--address <host:port>`; `--node` now takes a node ID.
- **Scopes:** the scope `cluster` is `fleet`, in `[[principal]] scopes`, `token issue --scope`, and a token's `fleetring.scopes` claim. A configuration that still says `cluster` fails to load, and a token minted by 0.2 that carries `cluster` is refused for fleet-scoped calls; issue a new one.
- **Messages and logs:** refusals say "needs the fleet scope" and "needs read on some database, or the fleet scope"; logs say "joined the fleet" and "left the fleet"; `token revoke --key-id` prints "revoked signing key …"; `cert node` refuses "invalid address '…' for a certificate"; a lease watcher that falls behind is told to open the stream "again for the leases held now". The audit lines' `key` field is `database`.
- **Protocol** (`fleetring.v1`; field numbers, enum values, and RPC paths unchanged, so the binary encoding is the same, and no protocol version bump):
  - enum `Role` is `ReplicaRole`, with values `REPLICA_ROLE_UNSPECIFIED`, `REPLICA_ROLE_PRIMARY`, `REPLICA_ROLE_FOLLOWER`;
  - message `LeaseSnapshot` is `HeldLeases`, and `LeaseUpdate`'s oneof field `snapshot` is `held`;
  - the `Scope:` comments say "fleet", "admin on the database", and "read per database listed, or fleet".

  Nothing serializes the protocol as JSON or looks a type up by name (no `pbjson`, no reflection, no `Any`), so only code that names these types needs regenerating.
- **Rust API** (`fleetringd`, `fleetring-control`, `fleetring-proto`; none is published): `ClusterName` → `FleetName`; `cluster()` → `fleet()` on `SubjectUri`, `Identity`, and `Authority`; `Scope::cluster()` → `Scope::fleet()`; `auth::Target::{Key, Cluster}` → `{Database, Fleet}`; `Requirement::{Cluster, Key, KeyOrCluster}` → `{Fleet, Database, DatabaseOrFleet}`; `Needed::Cluster` → `Needed::Fleet`; `sim::Cluster`, `ClusterBuilder` → `sim::Fleet`, `FleetBuilder`; `fleetringd::Role` → `NodeRole`; `fleetring_control::Role` → `ReplicaRole`; `ReplicationSink` → `BatchPublisher`; `RecordingSink` → `RecordingPublisher`; `replication::ReplicatedSink` → `ReplicatingPublisher`; module `sink` → `publisher`; `engine::Snapshot` → `engine::DatabaseSnapshot`; `LeaseUpdate::Snapshot` → `LeaseUpdate::Held`; `auth::require_key` → `require_database`; `Denied::key()` → `Denied::database()`; `tls::AuthorityError::Host { host }` → `AuthorityError::Address { address }`.
- **Config keys:** none renamed.
- **Data directories:** none incompatible. Nothing persisted changes: the control log and control snapshots store JSON whose field names and enum values are unchanged (Rust type names never reach it), the control log holds no scope strings or fleet names, and the per-database files (`epochs`, chain checkpoints) are untouched. A 0.2 data directory opens in 0.3.
- **Certificates:** unchanged; the fleet's name was always just the URI's host part, so certificates from 0.2 still connect (tested with fixture certificates issued by 0.2.0).
- **Mixed versions:** a 0.2 node and a 0.3 node speak the same bytes, but a token issued by 0.3 with the `fleet` scope is refused by 0.2, and the reverse, and a `[[principal]]` list must say `fleet` on 0.3 and `cluster` on 0.2. Upgrade every node, then reissue fleet-scoped tokens.
- **The placement crate** (`fleetring`): unchanged, so no `cargo semver-checks` change. Its docs use "cluster" in the general sense in three places (`kv_sharding`'s "Redis-style cluster", "small clusters" in diagnostics, and an epoch's "within a cluster"); they change, if at all, in its own release.

## Scope

- The renames in the decisions table, in code, tests, the protocol, help text, messages, both READMEs, `deploy/fleetringd.toml`, `SECURITY.md`, and `docs/` outside accepted ADRs and finished tickets.
- The [lexicon](../lexicon.md): resolve every row of "Open conflicts" into its terms, and remove the table.
- The [CLI style guide](../cli.md)'s "where `fleetringd` stands" table: `--node` and `--address`, `lease watch`.
- FR-056's scope: note that `lease watch` and `--address` landed here.
- **Not in scope:** the `fleetring` placement crate, which already says "key" and has no conflicting names; the placement spec and its golden vectors (ADR-0008); renumbering protocol fields.

## Acceptance criteria

- [x] No Rust item, flag, scope, protocol name, or doc (outside accepted ADRs, finished tickets, and in-progress tickets' history) uses a word from the decisions table in its old sense.
- [x] `status --address`, `status --node <node-id>`, `lease watch --address`, `lease watch --node <node-id>`, `db query --address`, and `db query --node <node-id>` each work against a running fleet, with a test (`tests/database.rs`, `binary`), and `db exec --node <address>` fails with a hint naming `--address`.
- [x] `--cluster`, `--hosts`, `watch-leases`, and the `cluster` scope are refused like any unknown flag, command, or scope, with a test for each.
- [x] `cert node --addresses` issues a certificate naming every address (`tests/tls.rs`).
- [x] The protocol compiles with the new names, field numbers are unchanged, and the RPC table test holds the proto's `Scope:` lines to the new wording.
- [x] The control log's replay test and the simulation pass unchanged in behaviour.
- [x] The lexicon has no "Open conflicts" table, and every resolved name has its entry.

## Progress

- 2026-11-03: Implemented with FR-059 on the `naming` branch, as a clean break (the owner's call: nobody runs `fleetringd` yet).
  - **Code:** `NodeRole` and `ReplicaRole`; `--node <node-id>` and `--address` on `status`, `lease watch`, `db exec`, and `db query`, resolving an ID through the control plane's membership; `lease watch` for `watch-leases`; `cert node --addresses`; `BatchPublisher` for `ReplicationSink`; `DatabaseSnapshot` and `HeldLeases`; `Target::Database`, `Requirement::Database`, and "on the database" in the auth module, its messages, and its audit field.
  - **Protocol:** `ReplicaRole` and `HeldLeases`, with every field number and enum value unchanged; checked by diffing `fleetring.proto` against its base (the only changed non-comment lines are the two type names, the enum value names, and the oneof field name; the sequence of every `= N` number is identical; no `rpc` line changed; `auth::RPCS` paths are unchanged), and by the existing protocol and compatibility tests.
  - **Persisted data:** unchanged. Before the first rename, a scratch test dumped the control state (serde JSON) of three seeded simulation runs; after the last, the same dump has the same set of JSON paths and enum strings, and `reports_without_a_log_epoch_load_from_old_logs` still round-trips a 0.2 heartbeat report with `"role":"follower"` byte for byte. The control log's format version stays 1.
  - **Docs:** the lexicon's open conflicts became entries (new: address, batch publisher, node role, replica role) and a "renamed in 0.3.0" table; the CLI guide's alias and deprecation rules became a clean-break rule; open tickets (FR-041, FR-043, FR-045, FR-049, FR-052, FR-053, FR-056, FR-057) follow the new names; milestone M9 is "Archives and verification".
  - **Not renamed:** `Replication.Snapshot`, `SnapshotRequest`, and `SnapshotChunk` (they only carry database snapshots, inside the replication service); `engine::PinnedSnapshot`, `SnapshotFile`, and `LogError::CorruptSnapshot` (each lives in a module that already says which snapshot; the error's message says "control snapshot"); the `--key` and `--key-id` flags, whose help now says private key and signing key; the `role` config key and `--role`; ticket titles (FR-038's "departure", FR-049's "durability sink"); accepted ADRs; the in-progress tickets FR-047, FR-050, and FR-051, whose text records work already done.
