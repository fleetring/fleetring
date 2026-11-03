# ADR-0029: Durability sinks: verifiable archives of snapshots and log segments in object storage, with point-in-time restore

- **Status:** Proposed (2026-11-02)
- **Date:** 2026-11-02
- **Related:** ADR-0016 (engine adapters, point 3: durability sinks), ADR-0018 (quorums), ADR-0020 (simulation), ADR-0022 (log epochs), ADR-0023 (durability modes), ADR-0025 and ADR-0026 (scopes), ADR-0028 (replicated sink positions); tickets FR-032, FR-039, FR-041, FR-044, FR-049

## Context

Replicas give availability and failover; they don't give backups. A `DROP TABLE` replicates to every copy within a round trip, and a lost cluster loses every copy. ADR-0016 point 3 set durability sinks apart from replicas: S3 snapshots and log archiving for point-in-time restore, behind a trait of their own, so that "replicas plus S3", "S3 only", or an engine's own S3 mode plug in without changing replication. The roadmap's "Recoverable" criterion and MVP step 5 ask for it.

Much of the machinery exists:

- **Change streams** (FR-039) deliver each database's acknowledged batches, in order and at least once, as opaque *physical* payloads with their start and end positions, beside replication and never in a write's acknowledgement path. A lagging sink holds checkpoints back until the lag limit, as a lagging follower does.
- **Replicated sink positions** (ADR-0028) let a new primary continue a stream where the old one stopped.
- **Pinned snapshots** (FR-032) give a consistent copy of a database's files while writes go on, and `replication::archive` packs them into one checksummed stream.
- **The hash chain** (FR-044) gives one BLAKE3 hash per transaction position that commits every byte before it, and every replica can report it (`db verify`).

The open decisions are the archive's layout in object storage, how archived log segments stay engine-neutral, whether the chain goes into the archive, how a restore picks what to apply and how a wall-clock target maps onto a log without clocks deciding anything that matters, the trait shapes, credentials and configuration, the S3 client, how all of it is tested without S3, and the scopes restore needs.

## Decision

### Shape

1. **An archive is a physical change stream plus base snapshots, written to an object store.** An `[[archive]]` entry configures one archive: a name, the databases it covers (the same matchers as sinks), a store, and its pacing and retention. For each database the node serves as primary, the archive runs:
   - an **archive sink**, a `ChangeSink` of physical format that the existing change-stream pumps feed, which turns each delivered batch into one immutable *segment* object; and
   - a **snapshotter**, which takes a *base snapshot* through the engine when the archive has none, when the last one is older than the snapshot interval, or when the segments since it pass a size threshold.

   Because the sink is a change sink, the archive inherits everything streams already guarantee: only acknowledged batches, in order, at least once, never in the write path, holding checkpoints back while it lags, and continuing after a failover from the mark in the new primary's copy (ADR-0028). An archive is asynchronous: its recovery point is its lag, which `status --node` shows as each sink's backlog. An acknowledgement mode that waits for the archive, the "S3 only" deployment of ADR-0016, is a later decision on top of this one.

2. **Two traits.** `ObjectStore` is the storage seam: `put`, `get`, `list`, and `delete` of whole objects by key, async, no other operation, so that any store can implement it. `S3Store` is the first production implementation, `DirectoryStore` (a local directory, for tests and for a mounted volume) and `MemoryStore` (for unit tests and simulation) the others. The archive logic above it (segments, snapshots, retention, restore) is written once against the trait and knows nothing about S3. An engine whose own durability writes to object storage, such as Turso's, would not need this archive at all; it reports its own recovery point through its adapter, which is out of scope here.

3. **Frame-source engines only, for now.** Segments hold the opaque batches a frame-source engine ships, and a restore applies them with the engine's own follower operations (`FrameFollower::apply`, `truncate_to`). A self-replicating engine ships no batches; configuring an archive for a database on one is refused at start with a message naming the engine.

### Layout

4. **Every object is immutable, and object names are the manifest.** Object stores offer no atomic update of several objects, and after a failover an old primary and a new one may both be uploading for a moment. So nothing is ever rewritten: each segment and each snapshot is written once under a name that says what it holds, a snapshot's descriptor is written last and marks it complete, and `list` over a database's prefix is its manifest. No object ever needs a lock or a conditional write. Keys, with `<db>` the database's directory name from `layout::database_dir` (escaped, so every key is safe on every store) and positions as lowercase hex, or `begin` for the log's beginning:

   ```text
   <prefix>/<db>/snapshots/<id>/snapshot.json         descriptor, written last
   <prefix>/<db>/snapshots/<id>/<nnnnnnnn>.chunk       the packed snapshot stream, 8 MiB per chunk
   <prefix>/<db>/segments/<start>_<end>_<epoch>_<at>.seg   one delivered batch
   ```

   Epochs and `<at>`, the segment's `archived_at_ms` (point 11), are 16 hex digits. A snapshot's `<id>` is `<position>_<epoch>`. Two primaries never write the same key, because their epochs differ, and a restore can plan from the names alone, downloading only what it applies.

5. **A snapshot is the existing packed stream, cut into chunks, with a JSON descriptor.** The chunks are the bytes `replication::archive::Packer` produces for a pinned snapshot, the stream followers already verify, split every 8 MiB so that no object needs a multipart upload. `snapshot.json` holds: `format` (1), `database`, `engine` (`sqlite`), `epoch`, `position` (hex, or null for the log's beginning), `chain` (the 64-hex-digit chain hash at `position`), `taken_at_ms`, `chunks`, `bytes`, and `checksum` (the stream's XXH3-128, as the snapshot stream's last chunk carries it).

6. **A segment is one delivered batch with its chain, in a small binary frame.** Bytes, all integers big-endian:

   ```text
   magic        "FRSEG\x01"                     6 bytes
   header_len   u32, then the header as JSON:
                {"format":1,"database":…,"engine":"sqlite","epoch":N,
                 "start":hex|null,"end":hex,"chain_start":hex,"chain_end":hex,
                 "archived_at_ms":N,"payload_len":N}
   payload      the batch's opaque payload, payload_len bytes
   checksum     XXH3-128 over everything before it, 16 bytes
   ```

   The positions and payload are exactly what the engine handed out: the archive never parses them, which is what keeps segments engine-neutral (ADR-0016 point 2). One delivery is one segment; the archive sink asks its pump to pace deliveries (`ChangeSink::pace`, the segment interval, 10 s by default), so a segment holds everything acknowledged in an interval, and an idle database writes none.

7. **The hash chain is carried into the archive.** Each segment records the chain hash at its start and at its end, and each snapshot the hash at its position, taken from the primary's chain (`PrimaryFeed::chain_at`). A restore recomputes the chain over every batch it applies (`ChainHash::chain`, as followers do) and refuses one whose result differs from the recorded `chain_end`, so a restored copy is proven to hold the log the cluster held, transaction by transaction. `fleetringd db archive verify` restores into a scratch directory and compares the final hash with the live replicas' `db verify` at the same position when they still hold it. A segment uploaded while the primary's chain isn't built yet is refused by the sink and retried, so every segment carries a chain.

### Writing

8. **Base snapshots are taken at the archive's own position.** An archive's segments must continue from a snapshot or from an earlier segment. Before uploading a segment, the sink checks that the archive holds a path to the segment's start: a segment it uploaded ending there, a snapshot at it, or, at its first upload for the database since starting, either of those in a `list` of the store. If not, as for a new archive, after a stream reset, or when the store was emptied, it first takes a base snapshot at exactly that position. The snapshotter, when a periodic snapshot is due, takes it at the position the archive's stream continues from, holding the same per-database lock as the sink. Both pin through a new engine operation, `FrameSource::pin_snapshot_at(at)`: a consistent copy that holds the log up to `at`, a commit boundary at or after the log's last checkpoint, or the main file alone for the log's beginning. A pinned snapshot at the current end would include writes no quorum has acknowledged; pinning at the archive's position, which a pump only reaches through acknowledged batches, never does.

9. **Retention keeps whole restore windows.** After each new snapshot completes, the archive keeps the newest `keep_snapshots` (7 by default) complete snapshots, deletes older ones descriptor first (so a half-deleted snapshot reads as incomplete and is ignored), deletes every segment that ends at or before the oldest kept snapshot's position, and deletes incomplete snapshots older than the newest complete one. With the default daily snapshots, that is a week of point-in-time restore. Retention only deletes what no kept snapshot's path can use, so an old and a new primary running it at once delete nothing the other needs.

### Restoring

10. **A restore picks a snapshot, then walks segments by position, preferring the newest epoch.** For a target, which is the latest archived point, a position, or a time:
    1. List the database's snapshots and segments. Candidate snapshots are the complete ones at or before the target (by position, or by `taken_at_ms` for a time), newest first.
    2. From a candidate's position, repeatedly take the segment that starts at the current position, preferring the highest epoch, since a later primary's log is the one that won (ADR-0022 point 2). Stop at the target: before a segment whose `archived_at_ms` is past a time target, or once the position reaches or passes a position target. Planning reads names only.
    3. If the walk can't reach a position target, try the next candidate; if none reaches it, fail and name the furthest position the archive reaches.
    4. Download and verify the snapshot (chunk count, checksum), unpack it into a staging directory, and open it as a follower copy (`FrameSourceEngine::open_follower`). Apply each segment's batch in order, verifying its checksum, that its `chain_start` is the hash the copy is at, and that recomputing the chain over its batch gives its `chain_end`; any mismatch fails the restore as data loss, naming the segment. For a position target inside the last segment, cut the copy back to the target with `FrameFollower::truncate_to`; a target that isn't a transaction boundary, or lies before a log restart inside that segment, is refused with the nearest boundaries named.
    5. Report the restored position, its chain hash, the snapshot, and the number of segments.

    The result is a follower-shaped copy, byte-identical to what a follower holding that prefix would have. Online, it becomes a **new** database (point 12); offline (`fleetringd archive restore --to <dir>`), it is a directory with a plain SQLite database a disaster recovery can open directly, no cluster needed.

11. **A time is mapped to a log position, and clocks never decide consistency.** Each segment records `archived_at_ms`: the archiving node's clock when the sink received the batch, kept non-decreasing per archive and database. The batch was acknowledged before it was delivered, so every transaction in a segment was acknowledged at or before that time. A time target restores every segment whose `archived_at_ms` is at or before it: a prefix of the log that contains no write acknowledged later than the target by that clock, and may stop up to one segment interval short of it. Whatever the clocks say, a restore yields a prefix the cluster acknowledged, verified by the chain; a wrong clock can only make it a different prefix. That is the same split as ADR-0018 point 6: clocks decide when, never whether.

12. **Online restore creates a new database and never overwrites one.** `Database.Restore` names the new database, the archive, the archived database (the *source*), and the target. Like `db create`, the client first creates the new database in control state, then calls the first live member of its replica set, which builds the copy, installs it as its replica with the restored position and chain as its epoch record and chain anchor, takes the first lease, and serves; followers seed from it by snapshot as usual. Restoring to the old name is `db drop` followed by a restore under that name. A restore that fails leaves an unleased database the caller can retry or drop.

### Operating

13. **Configuration.** An `[[archive]]` entry has `name` (unique among sinks and archives, since marks are kept by name), `databases`, `store = "s3" | "directory"`, and for S3: `bucket`, `prefix`, `endpoint` (any S3-compatible service: AWS, MinIO, Cloudflare R2, Garage), `region`, `path_style` (default false), `credentials = "env" | "file"` with `credentials_file`, and optionally `ca` for a private endpoint's authority; `allow_http = true` permits an `http://` endpoint for a local MinIO and nothing else. For a directory: `path`. Pacing and retention: `segment_interval_ms` (10,000), `snapshot_interval_ms` (86,400,000), `snapshot_after_bytes` (1 GiB), `keep_snapshots` (7). The same entries must be on every node, as sinks must (ADR-0028 point 7).

14. **Credentials are static keys, from the environment or a private file, never logged.** `credentials = "env"` reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN` at start. `credentials = "file"` reads a TOML file with `access_key_id`, `secret_access_key`, and optionally `session_token`; the daemon refuses a file that others can read on Unix, and re-reads it when it changes, checked at most once a second when a request is about to be signed, as TLS files are. Secrets are held in `zeroize`d memory and `Debug` shows only the key ID. Instance-metadata and web-identity credentials are a follow-up; an operator who needs them today runs a sidecar that writes the file.

15. **The S3 client is `rusty-s3` over the daemon's own HTTP stack.** `rusty-s3` 0.11 signs requests (SigV4) and parses S3's XML replies without doing any I/O itself; the daemon sends them with the `hyper` client it already uses for webhooks, over TLS with `tokio-rustls` and `ring`, which it already uses for mutual TLS, trusting the platform's roots through `rustls-native-certs` (plus `ca` if set). Its MSRV is 1.89, the daemon's, and its dependencies are small and pure Rust (`hmac`, `sha2`, `jiff`, `url`, `percent-encoding`, `instant-xml`, `base64`, `md-5`, `serde_json`, `zeroize`), all under licences `deny.toml` already allows; the crate itself is BSD-2-Clause, also allowed. Because it does no I/O and draws no randomness, it fits the simulation rules (ADR-0020) and adds no second HTTP or TLS stack.

16. **Tests run with no S3.** One conformance suite runs against `MemoryStore`, `DirectoryStore`, and `S3Store` pointed at an in-process fake S3 (a `hyper` server implementing the five calls the store makes, keeping objects in memory), so the S3 code path, signing included, runs in every CI job. A test against a real endpoint runs only when `FLEETRING_S3_TEST_ENDPOINT` and credentials are set, and is ignored otherwise. Under simulation, every data host's archive writes to one shared `MemoryStore` outside the simulated network, standing for a store that survives the cluster, optionally wrapped in a store that fails a fixed schedule of calls; a scenario with failovers and crashes then restores to several targets and checks the result against the history.

17. **Scopes.** In the requirement table of ADR-0026:

    | RPC | Requirement |
    |---|---|
    | `Database.Restore` | admin on the key |
    | `Database.DescribeArchive` | read on the key, or cluster |
    | `Database.VerifyArchive` | read on the key, or cluster |

    For `Restore` the key is the new database, which the call creates, so `admin` as for `Create`; the handler also requires `admin` on the source, because a restore copies the source's data, including history its current scopes may no longer show, into a database its caller then controls. `DescribeArchive` and `VerifyArchive` name the archived database and only read, like `Verify`.

## Alternatives considered

- **A separate `DurabilitySink` trait fed by its own pump.** ADR-0016 imagined one trait for sinks. Feeding the archive through a change stream reuses acknowledged-only delivery, ordering, checkpoint hold-back, retries, and replicated positions instead of writing them twice; the trait that varies by backend is the object store, which is where the plug-in point now is.
- **A mutable manifest object, or a log of manifest entries.** It would make listing cheaper but needs conditional writes or a single writer, which two primaries during a failover can't promise. Names that say what an object holds need neither, and a `list` per restore is cheap.
- **Segments of a fixed size, rolled up from many batches.** Fewer objects, but a segment can only be uploaded when it is complete, and a sink may report a batch delivered only once it is durable in the store; buffering batches would either lose them on a crash after their marks were written, or need a local spool that a failover loses. Pacing the pump gives the same effect: a slower stream of larger batches.
- **Archiving logical changes instead of batches.** Engine-neutral in a deeper sense, and the input to engine-changing migrations, but a restore would replay SQL rather than reproduce the copy byte for byte, schema changes aren't captured (ADR-0024), and nothing could be verified against the chain.
- **Snapshots at the current log end.** Simpler pinning, but the copy would contain writes no quorum had acknowledged, which a failover may discard; a restore could bring back writes the cluster never confirmed.
- **The `object_store` crate.** Mature and maintained by the Apache Arrow project, with in-memory and local backends built in, and an MSRV of 1.85. Its `aws` feature brings `reqwest` and `aws-lc-rs`, a second HTTP stack and a C library built with CMake, and its cloud-base feature brings `chrono` and `rand`'s thread-local generator, which the simulation rules keep out of daemon code paths. Running it without `reqwest` means implementing its HTTP connector anyway.
- **The AWS SDK (`aws-sdk-s3`).** Complete and official, but it pulls in the Smithy runtime and, by default, `aws-lc-rs`, roughly a hundred crates for five calls, and its MSRV moves with the SDK.
- **`rust-s3`.** A complete client with its own runtime choices and a larger dependency tree, maintained by one person like `rusty-s3`, but without the sans-I/O split that lets the daemon keep one HTTP and TLS stack.
- **Synchronous archiving in the acknowledgement path.** Gives a zero recovery point, at the cost of an object-store round trip on every write. It belongs to a separate acknowledgement-mode decision, if anyone asks for "S3 only".

## Consequences

- `fleetringd` gains a `durability` module (store trait and three stores, formats, the archive sink, the snapshotter, retention, restore), `FrameSource::pin_snapshot_at` in the engine adapter, a chain field and a pacing hook in the change-stream types, three `Database` RPCs, and `db restore`, `db archive describe|verify`, and `archive restore` commands. New dependencies: `rusty-s3`, `rustls-native-certs`, and `zeroize` (already in the tree through `rustls`).
- An archive's recovery point is its lag, at least the segment interval. Its cost is one object per interval per database being written, plus a base snapshot per interval, each pinning checkpoints while it uploads, as follower snapshots do.
- A restore is only as available as the store. A store that loses or reorders objects can make a restore fail, never return a wrong copy, because every byte applied is verified against the chain.
- The archive carries the chain from FR-044, so the "audit trail" the vision describes falls out: an operator can prove that what the archive holds at a position is what every replica held there.
- Self-replicating engines get no archive until their adapters provide one; logical archives and an acknowledgement mode that waits for the archive are follow-ups.
