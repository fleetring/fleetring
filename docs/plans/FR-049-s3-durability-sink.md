---
id: FR-049
title: An S3 durability sink with verifiable point-in-time restore
status: in-progress
priority: P1
size: L
milestone: M9 Archives and verification
depends_on: [FR-039, FR-041, FR-044, FR-047]
adrs: [ADR-0016, ADR-0028, ADR-0029]
---

# FR-049: An S3 durability sink with verifiable point-in-time restore

## Context

Replicas don't protect against a bad `DROP TABLE`, which replicates in a round trip, or against losing the fleet. ADR-0016 point 3 keeps durability sinks, which FleetRing builds as archives, apart from replicas, and the roadmap's "Recoverable" criterion asks for backups with point-in-time restore. ADR-0029 decides the design: an archive is a physical change stream (FR-039) that writes one immutable, chain-carrying segment per delivery to an object store, plus base snapshots pinned at the archive's own position; a restore picks a snapshot, walks segments by position preferring the newest epoch, verifies every batch against the FR-044 hash chain, and builds a new database, online, or a plain SQLite file, offline. Replicated sink positions (FR-041, ADR-0028) let a new primary continue the archive. The S3 client is `rusty-s3`, sans-I/O, over the daemon's own `hyper` and `rustls`.

## Scope

- **`fleetringd::durability`:** the `ObjectStore` trait with `MemoryStore`, `DirectoryStore`, and `S3Store`; the segment and snapshot formats and key names; the archive sink and snapshotter; retention; restore planning and execution.
- **Engine:** `FrameSource::pin_snapshot_at`, a pinned snapshot that holds the log up to a chosen commit boundary.
- **Change streams:** `Change` carries the chain hash at both ends, and a sink can ask its pump to pace deliveries.
- **Configuration:** `[[archive]]` entries; S3 credentials from the environment or a private file.
- **RPCs and commands:** `Database.Restore`, `Database.DescribeArchive`, `Database.VerifyArchive`; `fleetringd db restore`, `db archive describe`, `db archive verify`, and the offline `fleetringd archive restore`.
- **Not in scope:** an acknowledgement mode that waits for the archive; archives for self-replicating engines; logical archives; instance-metadata or web-identity credentials; restoring over an existing database.

## Acceptance criteria

- [x] ADR-0029 is accepted and linked from `docs/adr/README.md`.
- [x] A database archived to a `DirectoryStore` restores to its latest archived point with the same rows, `PRAGMA integrity_check` passing, and the same chain hash the primary reports at that position.
- [x] Restores to a position and to a time hold exactly the transactions at or before the position, and no segment archived after the time.
- [x] After a failover the new primary continues the archive from the replicated mark without a new base snapshot, and a restore across the failover holds every write acknowledged before the last archived point.
- [x] A segment or snapshot chunk altered in the store makes a restore fail with `DATA_LOSS` naming the object; nothing is installed.
- [x] `S3Store` passes the store conformance suite against the in-process fake S3 in every CI job; the real-endpoint test is documented and ignored without credentials.
- [x] The simulation scenario `tests/sim/archive.rs` passes on the heavy fixed seeds within the simulation job's budget.
- [x] `Restore`, `DescribeArchive`, and `VerifyArchive` are in `auth::RPCS` with the requirements ADR-0029 point 17 gives, and the restore handler also requires `admin` on the source.
- [x] `fleetringd archive restore --to <dir>` writes a directory whose `data.db` opens with plain SQLite and holds the restored rows.
- [x] `deploy/fleetringd.toml`, the daemon README, and `docs/roadmap.md` describe archives, restores, and credentials.

## Implementation plan

Each step is about one commit (`feat(fleetringd): …`, `Refs: FR-049`). Every path is under `crates/fleetringd/` unless it says otherwise. New public types derive `Debug` and are added to the module's `assert_debug_send_sync` list; errors are `#[non_exhaustive]` with lowercase messages.

### Step 1: pin a snapshot at a chosen position

- `src/engine/adapter.rs`, trait `FrameSource`, add:

  ```rust
  /// Pins a consistent copy that holds the log up to `at`: a commit boundary of the current log
  /// at or after its start, or `None` for the main file alone, as of the last checkpoint.
  ///
  /// An archive pins at the position its stream continues from, which a pump reaches only
  /// through acknowledged batches, so the copy never holds a write no quorum acknowledged.
  ///
  /// # Errors
  ///
  /// [`EngineError::InvalidPosition`] if `at` isn't a commit boundary between the log's start
  /// and its end, and [`EngineError::Storage`] if the files can't be read.
  fn pin_snapshot_at(&mut self, at: Option<&Position>) -> Result<PinnedSnapshot, EngineError>;
  ```

- `src/engine/sqlite.rs`, `impl FrameSource for SqliteDatabase`: implement it next to `pin_snapshot`. Read the committed batch with `self.read_wal(None)?` and pin as `pin_snapshot` does. Then:
  - `None`: files are the checkpoint marker (if present) and `data.db`; no WAL file; position `None`.
  - `Some(at)` equal to the checkpoint marker's position (`read_marker`): the same files, position `Some(at)`.
  - `Some(at)` inside the current generation: check it is a commit boundary with `cut_at(&batch, at)` (an error there is `InvalidPosition`); files as `pin_snapshot` lists them, with the WAL's length `decode_position(at)?.offset()`; position `Some(at)`.
  - Anything else: `InvalidPosition`.
- `src/actor.rs`: a `Request::PinSnapshotAt { at: Option<Position>, reply }` variant and `DatabaseHandle::pin_snapshot_at(&self, at: Option<Position>) -> Result<PinnedSnapshot, DatabaseError>`, modelled on `pin_snapshot`.
- `src/replication/archive.rs`: make `Packer` and `Unpacker` (and their `new`/`create`, `next_chunk`, `write`, `finish`, `offset`, `position`) `pub(crate)`, and re-export them from `replication` as `pub(crate) use`.
- Tests in `tests/actor/sqlite.rs`:
  - `a_snapshot_pinned_at_an_earlier_commit_holds_the_log_up_to_it`: three single-row transactions ending at `p1 < p2 < p3`; pin at `p2`; copy its files into a directory (as `tests/actor/pinned.rs` does); `open_follower` reports position `p2`; applying `committed_between(Some(p2), p3)` brings it to `p3`, and a query over the copy (opened after promotion-style `-shm` removal) returns the three rows.
  - `a_snapshot_pinned_at_the_log_beginning_is_the_main_file_alone`: after a checkpoint and one more write, `pin_snapshot_at(None)` lists no WAL file; the copy opened as a follower has position `None`, and applying `committed_since(None)` reproduces the primary.
  - `pinning_off_a_commit_boundary_or_before_the_checkpoint_is_refused`: a position inside a transaction and the previous generation's start both give `EngineError::InvalidPosition`.

### Step 2: chains on changes, and paced sinks

- `src/changes/mod.rs`:
  - `Change` gains `pub chain_start: Option<ChainHash>` (the hash at `start`) and `pub chain_end: Option<ChainHash>` (the hash at `position`), documented as FR-044's chain; `None` only while the primary's chain isn't built. Update every construction (`src/changes/pump.rs`, `src/changes/memory.rs`, tests).
  - `ChangeSink` gains `fn pace(&self) -> Duration { Duration::ZERO }`: the least time between the end of one delivery and the next read of the log.
- `src/replication/mod.rs`: `PrimaryFeed::chain_at(&self, position: Option<&Position>) -> Option<ChainHash>`, returning `self.primary.hash_at(position)`.
- `src/changes/pump.rs`: `deliver` takes the feed and fills both chain fields with `feed.chain_at(after.as_ref())` and `feed.chain_at(Some(&until))`. After `Delivery::Delivered`, if `ctx.sink.pace()` is non-zero, sleep for it inside a `tokio::select!` with `feed.changed()` ignored, then check `feed.is_current()` as the loop already does.
- Tests in `tests/changes/file.rs`:
  - `a_physical_change_carries_the_chain_hash_at_both_ends`: a `MemorySink` in physical format; after three writes, each change's `chain_end` equals the next change's `chain_start`, and the last `chain_end` equals `Replication::verify` at its position.
  - `a_paced_sink_receives_fewer_larger_batches`: a `MemorySink` whose `pace` is 200 ms; 50 writes spread over one second arrive in at most 8 changes, in order, gapless.

### Step 3: object stores, key names, and formats

- `src/durability/mod.rs`: module docs summarizing ADR-0029; `pub mod` items below; re-exports; the `assert_debug_send_sync` list. `src/lib.rs`: `pub mod durability;`.
- `src/durability/store.rs`:

  ```rust
  pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + Send + 'a>>;

  /// Whole immutable objects by key: the only operations an archive needs (ADR-0029).
  pub trait ObjectStore: fmt::Debug + Send + Sync + 'static {
      /// Stores `bytes` under `key`, replacing nothing else.
      fn put(&self, key: &str, bytes: Vec<u8>) -> StoreFuture<'_, ()>;
      /// The object under `key`, or `None` if there is none.
      fn get(&self, key: &str) -> StoreFuture<'_, Option<Vec<u8>>>;
      /// Every object whose key starts with `prefix`, in key order.
      fn list(&self, prefix: &str) -> StoreFuture<'_, Vec<ObjectEntry>>;
      /// Removes the object under `key`; removing a missing key succeeds.
      fn delete(&self, key: &str) -> StoreFuture<'_, ()>;
  }

  pub struct ObjectEntry { pub key: String, pub size: u64 }

  pub enum StoreError {          // #[non_exhaustive], thiserror
      Unavailable { reason: String },   // "object store unavailable: {reason}", retried by callers
      Rejected { reason: String },      // "object store rejected the request: {reason}"
      InvalidKey { key: String },       // "invalid object key '{key}'"
  }
  ```

  - `MemoryStore`: `Mutex<BTreeMap<String, Vec<u8>>>`, `MemoryStore::new() -> Self`, `Clone` sharing the map (an `Arc` inside).
  - `DirectoryStore::new(root: PathBuf) -> Self`: a key maps to `root` joined with its `/`-separated components; a key with an empty, `.`, or `..` component is `InvalidKey`. `put` writes `<path>.tmp`, `sync_all`, renames, and syncs the directory (as `changes::position` does); `list` walks the tree under the prefix's directory and filters by full key; filesystem calls run on `tokio::task::spawn_blocking`.
  - `FaultyStore::new(inner: Arc<dyn ObjectStore>, fail_every: NonZeroU32) -> Self`: every `fail_every`-th call (counted with an `AtomicU32`) returns `Unavailable` without touching `inner`; deterministic, for simulation and tests.
- `src/durability/keys.rs`:

  ```rust
  pub struct ArchiveKeys { root: String }  // "<prefix>/<db>" with <db> = layout::database_dir(name), '/'-joined
  impl ArchiveKeys {
      pub fn new(prefix: &str, database: &DatabaseName) -> Self;
      pub fn snapshots(&self) -> String;                          // "<root>/snapshots/"
      pub fn segments(&self) -> String;                           // "<root>/segments/"
      pub fn snapshot_id(position: Option<&Position>, epoch: LeaseEpoch) -> String; // "<pos|begin>_<epoch:016x>"
      pub fn descriptor(&self, id: &str) -> String;               // ".../snapshots/<id>/snapshot.json"
      pub fn chunk(&self, id: &str, index: u32) -> String;        // ".../snapshots/<id>/<index:08>.chunk"
      pub fn segment(&self, name: &SegmentName) -> String;        // ".../segments/<start|begin>_<end>_<epoch:016x>_<at:016x>.seg"
  }
  pub struct SegmentName { pub start: Option<Position>, pub end: Position, pub epoch: LeaseEpoch, pub archived_at_ms: u64 }
  impl SegmentName { pub fn parse(file_name: &str) -> Option<Self>; }
  ```

  Positions are lowercase hex of their bytes (`changes::hex`, `changes::unhex`); `begin` stands for `None`.
- `src/durability/format.rs`:

  ```rust
  pub const SEGMENT_MAGIC: &[u8; 6] = b"FRSEG\x01";
  #[derive(Serialize, Deserialize)] #[serde(deny_unknown_fields)]
  pub struct SegmentHeader {
      pub format: u32,            // 1
      pub database: String,
      pub engine: String,         // EngineKind's display name
      pub epoch: u64,
      pub start: Option<String>,  // hex
      pub end: String,
      pub chain_start: String,    // 64 hex digits
      pub chain_end: String,
      pub archived_at_ms: u64,
      pub payload_len: u64,
  }
  pub fn encode_segment(header: &SegmentHeader, payload: &[u8]) -> Vec<u8>;
  pub fn decode_segment(bytes: &[u8]) -> Result<(SegmentHeader, &[u8]), FormatError>;
  #[derive(Serialize, Deserialize)] #[serde(deny_unknown_fields)]
  pub struct SnapshotDescriptor {
      pub format: u32, pub database: String, pub engine: String, pub epoch: u64,
      pub position: Option<String>, pub chain: String, pub taken_at_ms: u64,
      pub chunks: u32, pub bytes: u64, pub checksum: String,   // XXH3-128, 32 hex digits
  }
  pub enum FormatError { BadMagic, Truncated, Checksum, Header(String), Version(u32) }
  ```

  The segment layout is ADR-0029 point 6: magic, `u32` big-endian header length, header JSON, payload, XXH3-128 (`twox_hash::XxHash3_128`) over everything before it, big-endian.
- Tests:
  - Unit tests in `keys.rs` (`names_round_trip_and_sort_by_start_within_a_database`, `begin_stands_for_the_log_beginning`) and `format.rs` (`a_segment_round_trips`, `a_flipped_byte_fails_the_checksum`, `an_unknown_header_field_is_refused`, `a_newer_format_is_refused`).
  - New test binary `tests/durability/` (`main.rs` declaring `mod common; mod store; mod archive; mod restore; mod s3;`). `tests/durability/store.rs` holds one conformance function `conformance(store: Arc<dyn ObjectStore>)` and runs it as `memory_store_conforms`, `directory_store_conforms`, and (step 4) `s3_store_conforms_against_the_fake`. The function asserts: put then get returns the bytes; get of a missing key is `None`; list returns exactly the keys under a prefix, in order, with sizes; a put under one key leaves others unchanged; delete removes, and deleting a missing key succeeds. Plus `directory_store_refuses_dot_components` and `faulty_store_fails_exactly_every_nth_call`.

### Step 4: the S3 store

- `Cargo.toml` (`crates/fleetringd`): `rusty-s3 = { version = "0.11", default-features = false, features = ["rustcrypto", "full"] }`, `rustls-native-certs = "0.8"`, `zeroize = "1"`. Run `cargo deny check`; every licence must already be in `deny.toml`.
- `src/durability/s3.rs` (with submodules `s3/http.rs` and `s3/credentials.rs`):

  ```rust
  pub enum PathStyle { VirtualHost, Path }
  pub enum HttpPolicy { HttpsOnly, AllowHttp }
  pub enum CredentialSource { Environment, File(PathBuf) }
  pub struct S3Settings {
      pub bucket: String, pub endpoint: String, pub region: String,
      pub path_style: PathStyle, pub credentials: CredentialSource,
      pub ca: Option<PathBuf>, pub http: HttpPolicy,
  }
  pub struct S3Store { /* rusty_s3::Bucket, hyper client, credentials, clock */ }
  impl S3Store {
      /// # Errors
      /// [`S3ConfigError`] for an invalid endpoint, an `http://` endpoint without `AllowHttp`,
      /// missing or unreadable credentials, or a credentials file others can read.
      pub fn new(settings: S3Settings, clock: Arc<dyn Clock>) -> Result<Self, S3ConfigError>;
  }
  impl ObjectStore for S3Store { … }
  ```

  - Requests are rusty-s3 actions (`PutObject`, `GetObject`, `DeleteObject`, `ListObjectsV2` with continuation tokens) signed for 60 s with the time from the injected `Clock`, using rusty-s3's signing call that takes an explicit time, and sent with `hyper_util::client::legacy::Client` over the connector in `s3/http.rs`: `HttpConnector` wrapped in a `tokio_rustls::TlsConnector` whose root store is `rustls_native_certs::load_native_certs()` plus the PEM certificates in `ca`, ALPN `http/1.1`, `ring` provider. `http://` URLs bypass TLS only under `AllowHttp`.
  - Status mapping: 2xx success; 404 on `get` is `Ok(None)`, on `delete` success; 5xx, timeouts (30 s per request), and connection errors retried up to 3 times with 100 ms doubling backoff, then `Unavailable`; other 4xx `Rejected` with the status and S3's error code from the XML body.
  - `s3/credentials.rs`: `Credentials` holding `access_key_id: String`, `secret_access_key: Zeroizing<String>`, `session_token: Option<Zeroizing<String>>`; `Debug` prints only `access_key_id`. `Environment` reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` once in `new`. `File` parses TOML with exactly those three keys (`access_key_id`, `secret_access_key`, `session_token`), refuses on Unix a file whose mode grants group or other any permission, and re-reads it when its modification time or length changes, checked at most once per second before signing.
- `tests/durability/fake_s3.rs`: `FakeS3::start() -> (SocketAddr, FakeS3)`, a `hyper` HTTP/1.1 server on `127.0.0.1:0` serving path-style `PUT|GET|DELETE /<bucket>/<key>` and `GET /<bucket>?list-type=2&prefix=…&continuation-token=…` with at most 2 keys per page (to exercise pagination), returning ListObjectsV2 XML. It refuses a request without `X-Amz-Signature` and `X-Amz-Credential` query parameters with 403, and can be told to fail the next N requests with 503.
- Tests in `tests/durability/s3.rs`: `s3_store_conforms_against_the_fake` (the step 3 suite, `AllowHttp`, `PathStyle::Path`), `a_503_is_retried_and_a_403_is_rejected`, `an_http_endpoint_is_refused_without_allow_http`, `a_credentials_file_others_can_read_is_refused` (`#[cfg(unix)]`), `credentials_never_appear_in_debug_output`, and `#[ignore] a_real_endpoint_conforms` that runs the suite when `FLEETRING_S3_TEST_ENDPOINT`, `FLEETRING_S3_TEST_BUCKET`, `FLEETRING_S3_TEST_REGION`, and the AWS credential variables are set, under a fresh random-free prefix `fleetring-test/<test start in ms>`.

### Step 5: the archive sink, snapshots, retention, and configuration

- `src/durability/config.rs`:

  ```rust
  #[derive(Deserialize)] #[serde(deny_unknown_fields)]
  pub struct ArchiveConfig {
      pub name: String,                          // 1..=64 of [a-z0-9_-], unique among [[changes.sink]] and [[archive]]
      pub databases: Vec<DatabaseMatcher>,
      pub store: StoreKind,                      // "s3" | "directory"
      pub prefix: String,                        // default "fleetring"; no leading or trailing '/'
      pub path: Option<PathBuf>,                 // directory
      pub bucket: Option<String>, pub endpoint: Option<String>, pub region: Option<String>,
      pub path_style: bool,                      // default false; mapped to PathStyle
      pub credentials: Option<CredentialKind>,   // "env" | "file"
      pub credentials_file: Option<PathBuf>, pub ca: Option<PathBuf>,
      pub allow_http: bool,                      // default false; mapped to HttpPolicy
      pub segment_interval_ms: NonZeroU64,       // 10_000
      pub snapshot_interval_ms: NonZeroU64,      // 86_400_000
      pub snapshot_after_bytes: NonZeroU64,      // 1_073_741_824
      pub keep_snapshots: NonZeroU32,            // 7
  }
  impl ArchiveConfig { pub fn validate(&self) -> Result<(), ArchiveConfigError>; }
  ```

  (`bool` fields are configuration data, never function parameters.) `src/config.rs`: `Config` gains `#[serde(default)] pub archive: Vec<ArchiveConfig>`, validated with the rest, including name uniqueness against `changes.sink`.
- `src/durability/archive.rs`:

  ```rust
  pub struct Archive { /* name, Arc<dyn ObjectStore>, prefix, settings, Registry, Replication, Arc<dyn Clock>, Execution, per-database state */ }
  impl Archive {
      pub fn new(config: &ArchiveConfig, store: Arc<dyn ObjectStore>, parts: ArchiveParts) -> Self;
      /// The change sink that writes this archive's segments.
      pub fn sink(&self) -> ConfiguredSink;      // Format::Physical, config's matchers
  }
  struct ArchiveSink { archive: Arc<ArchiveInner> }   // impl ChangeSink: name() is the archive's name
  ```

  Per database, `ArchiveInner` keeps `tokio::sync::Mutex<DbArchive>` with `continuity: Continuity` (`Unknown` or `At { position: Option<Position>, chain: ChainHash }`), `bytes_since_snapshot: u64`, `last_snapshot_ms: Option<u64>`, and `last_archived_ms: u64`. `publish(change)`:
  1. Refuse anything but `ChangeKind::Physical`, and a change without both chain hashes, with `SinkError::Failed` (the pump retries).
  2. Lock the database's state. If `continuity` is `Unknown`, list the store once: a segment ending at `change.start`, or a complete snapshot at it, sets `At`.
  3. If `continuity` isn't `At` with `change.start` and `change.chain_start`, take a base snapshot at `change.start` (below).
  4. `archived_at_ms = max(clock.now().as_millis(), last_archived_ms)`; encode the segment; `put` it; set `continuity`, add its length to `bytes_since_snapshot`, set `last_archived_ms`.
  5. A store error is `SinkError::Failed`; the pump retries and nothing is recorded.

  `pace()` returns `segment_interval`. Taking a snapshot (`ArchiveInner::snapshot(database, at, chain, epoch)`): `registry.open(database)?.pin_snapshot_at(at)`; pack it with `Packer` on a blocking thread, putting each 8 MiB of output as `chunk(id, n)`; call the pin's `confirm()`; put `snapshot.json`; set `continuity = At { at, chain }`, reset `bytes_since_snapshot`, set `last_snapshot_ms`; run retention.
- `src/durability/snapshotter.rs`: `Snapshotters::start(archives: Vec<Arc<ArchiveInner>>, replication: Replication) -> Snapshotters`, supervised from `Replication::served` as `ChangeStreams` is. For each archive and served database it names, a task checks at start and every 60 s whether a snapshot is due: none complete in the store, `now - last_snapshot_ms >= snapshot_interval_ms`, or `bytes_since_snapshot >= snapshot_after_bytes`. When due it locks the database's state and snapshots at the archive's position: `continuity`'s position if `At`, else the database's replicated mark for the archive's name (`DatabaseHandle::sink_marks`, FR-041), else `None`; the chain from `PrimaryFeed::chain_at`.
- `src/durability/retention.rs`: a pure `fn retention(listing: &Listing, keep: NonZeroU32) -> Vec<String>` (keys to delete, descriptors before chunks) per ADR-0029 point 9, and `async fn apply_retention(store, keys)`. `Listing` (also used by step 6) is built by `async fn list(store, keys: &ArchiveKeys) -> Result<Listing, StoreError>`: complete snapshots (with descriptor, fetched and parsed), incomplete snapshot ids, and every parsed `SegmentName` with its size.
- `src/daemon.rs`: build each `[[archive]]` (store from its kind; `S3Store::new` with the daemon's clock), refuse to start when the engine's capability isn't `FrameSource`, add each archive's `sink()` to the `ConfiguredSink`s given to `ChangeStreams::start`, and start `Snapshotters`. `src/environment.rs`: `Environment` gains `object_stores: BTreeMap<String, Arc<dyn ObjectStore>>`, which overrides the store of the archive with that name (the simulation's shared `MemoryStore`).
- Tests in `tests/durability/archive.rs` (a one-node `Harness` like `tests/changes/common.rs`, SQLite, `DirectoryStore` in a temp directory):
  - `a_new_archive_takes_a_base_snapshot_then_uploads_one_segment_per_delivery`: five writes paced at 50 ms give one complete snapshot and segments whose names chain start to end.
  - `segments_chain_from_the_snapshot_without_gaps`: each segment's `chain_start` equals the previous `chain_end`, the first equals the snapshot's `chain`.
  - `a_due_snapshot_is_taken_at_the_archives_position_and_retention_keeps_the_newest`: `snapshot_after_bytes = 1`, `keep_snapshots = 2`; after enough writes, exactly two complete snapshots remain and no segment ends at or before the older one.
  - `a_failing_store_delays_the_archive_but_never_a_write`: a `FaultyStore` failing every second call; writes are acknowledged at once; the archive catches up.
  - `an_archive_on_a_self_replicating_engine_is_refused_at_start`: a daemon configured with the memory engine and an `[[archive]]` fails to start with a message naming the engine.
  - `archive_config_is_validated`: duplicate names with a sink, a missing bucket for `s3`, `http://` without `allow_http`, `keep_snapshots = 0`.

### Step 6: restore

- `src/durability/plan.rs` (pure):

  ```rust
  pub enum RestoreTarget { Latest, Position(Position), Time(u64 /* ms */) }
  pub struct RestorePlan { pub snapshot: SnapshotEntry, pub segments: Vec<SegmentName>, pub cut: Option<Position> }
  pub enum PlanError { NoSnapshot, Unreachable { furthest: Option<Position> }, BeforeEarliest }
  pub fn plan(listing: &Listing, target: &RestoreTarget) -> Result<RestorePlan, PlanError>;
  ```

  The algorithm is ADR-0029 point 10: candidate snapshots at or before the target, newest first by `taken_at_ms` then position; from each, take the segment starting at the current position with the highest epoch; stop per target; `cut` is the position target when it falls inside the last segment.
- `src/durability/restore.rs`:

  ```rust
  pub struct Restored { pub position: Option<Position>, pub chain: ChainHash, pub snapshot: String, pub segments: u32, pub archived_at_ms: Option<u64> }
  pub enum RestoreError { Plan(PlanError), Store(StoreError), DataLoss { object: String, reason: String }, Engine(EngineError), Io(io::Error) }
  /// Builds the copy `target` names in `staging`, a directory that must not exist.
  pub async fn restore_into(store: &dyn ObjectStore, keys: &ArchiveKeys, engine: &dyn FrameSourceEngine, target: &RestoreTarget, staging: &Path, execution: Execution) -> Result<Restored, RestoreError>;
  ```

  It downloads the chunks in order into an `Unpacker` (a missing chunk, a wrong count, or a checksum mismatch is `DataLoss` naming the object), opens the copy with `engine.open_follower(staging)`, and for each segment: `decode_segment` (failure is `DataLoss`), checks `chain_start` against the running hash, recomputes with `ChainHash::chain(engine, prev, start, &batch)` and compares with `chain_end` (mismatch is `DataLoss`), then `follower.apply(&batch)`. A `cut` calls `follower.truncate_to(cut)`; `InvalidPosition` there becomes `Plan(Unreachable)` naming the segment's start and end. On any error the staging directory is removed.
- `src/replication/mod.rs`: `Replication::install_restored(&self, database: &DatabaseName, staging: &Path, restored: &Restored) -> Result<(), Status>`: moves the copy into place with `snapshot::install`, records log epoch 1 with `failover::set_log_epoch`, and records a chain anchor at `restored.position` with `restored.chain` (`record_anchor`), as a seeded follower's is.
- `src/data/node.rs`: `DataNode::restore(&self, name: &DatabaseName, archive: &Archive, source: &DatabaseName, target: RestoreTarget) -> Result<(Placement, Restored), Status>`, modelled on `create`: locate the database, require it unleased and this node its first live replica (same errors as `create`), `restore_into` a staging directory under the replication state directory, `install_restored`, then take the first lease and `start_serving` as `create` does.
- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`, service `Database` (field numbers are new messages' own):

  ```proto
  // Builds a new database from an archive (FR-049): the database must exist in the control plane,
  // unleased, and this node must be the first live member of its replica set.
  // Scope: admin on the database.
  rpc Restore(RestoreRequest) returns (RestoreResponse);
  // What an archive holds for a database: its snapshots and the range its segments cover.
  // Scope: read on the database, or fleet.
  rpc DescribeArchive(DescribeArchiveRequest) returns (DescribeArchiveResponse);
  // Restores into a scratch directory, verifying every batch against the chain, and discards it.
  // Scope: read on the database, or fleet.
  rpc VerifyArchive(VerifyArchiveRequest) returns (VerifyArchiveResponse);

  message RestoreLatest {}
  message RestorePoint {
    oneof point {
      RestoreLatest latest = 1;
      bytes position = 2;
      // Milliseconds since the Unix epoch, by the archiving nodes' clocks.
      uint64 time_ms = 3;
    }
  }
  message RestoreRequest {
    // The new database.
    string database = 1;
    string archive = 2;
    // The archived database to restore from.
    string source = 3;
    RestorePoint point = 4;
  }
  message RestoreResponse {
    Placement placement = 1;
    bytes position = 2;
    bytes chain_hash = 3;
    string snapshot = 4;
    uint32 segments = 5;
    // Unset if the restore applied no segment.
    optional uint64 archived_at_ms = 6;
  }
  message DescribeArchiveRequest {
    string database = 1;
    string archive = 2;
  }
  message ArchivedSnapshot {
    string id = 1;
    uint64 epoch = 2;
    bytes position = 3;
    uint64 taken_at_ms = 4;
    uint64 bytes = 5;
  }
  message DescribeArchiveResponse {
    string database = 1;
    repeated ArchivedSnapshot snapshots = 2;
    // The furthest position a restore reaches, and the time of its last segment.
    bytes latest = 3;
    optional uint64 latest_ms = 4;
    uint32 segments = 5;
    uint64 segment_bytes = 6;
  }
  message VerifyArchiveRequest {
    string database = 1;
    string archive = 2;
    RestorePoint point = 3;
  }
  message VerifyArchiveResponse {
    bytes position = 1;
    bytes chain_hash = 2;
    string snapshot = 3;
    uint32 segments = 4;
  }
  ```

- `src/data/service.rs`: the three handlers. `restore` calls `require_database` on `database` and again on `source` (both `admin`, the RPC's action), finds the archive by name among the node's archives (`NOT_FOUND` naming it otherwise), maps `RestoreError::DataLoss` to `DATA_LOSS`, `Plan` to `FAILED_PRECONDITION` with the furthest position, `Store(Unavailable)` to `UNAVAILABLE`. `describe_archive` builds a `Listing`; `verify_archive` runs `restore_into` into a scratch directory under the replication state directory and removes it.
- `src/auth/policy.rs`: three rows in `RPCS` (`Requirement::Database(Action::Admin)` for `Restore`, `Requirement::DatabaseOrFleet(Action::Read)` for the others); the `// Scope:` comments above; the daemon README's scope table gains the three rows (the policy tests enforce all three places).
- `src/data/client.rs`: `pub enum RestorePoint { Latest, Position(Position), Time(u64) }`; `Client::restore_database(&self, target: &DatabaseName, spec: DatabaseSpec, archive: &str, source: &DatabaseName, point: RestorePoint) -> Result<Restoration, Status>` (control `CreateDatabase`, then `Database.Restore` on the first live replica, as `create_database_with` does); `Client::describe_archive(&self, archive: &str, database: &DatabaseName) -> Result<ArchiveDescription, Status>`; `Client::verify_archive(&self, archive: &str, database: &DatabaseName, point: RestorePoint) -> Result<Verification, Status>`.
- `src/main.rs`: `db restore <TARGET> --archive <NAME> --from <SOURCE> [--at-position <HEX> | --at-time <RFC3339>] [--replicas N] [--acks MODE]`; `db archive describe <SOURCE> --archive <NAME>`; `db archive verify <SOURCE> --archive <NAME> [--at-…]`, which also asks the live replicas for `Verify` at the restored position and prints `archive agrees with every replica` or the replicas that differ; `archive restore --config <PATH> --archive <NAME> --from <SOURCE> --to <DIR> [--at-…]`, offline, which builds the store from the configuration, runs `restore_into` on `<DIR>`, then opens it once as a primary would (removing `-shm`) so the directory holds a plain SQLite database. Enable `time`'s `parsing` and `formatting` features for RFC 3339.
- Tests:
  - Unit tests in `plan.rs`: `latest_walks_every_segment_from_the_newest_snapshot`, `a_time_target_stops_before_the_first_segment_archived_after_it`, `a_position_target_inside_a_segment_sets_the_cut`, `the_highest_epoch_wins_where_two_primaries_archived_from_one_position`, `a_gap_names_the_furthest_reachable_position`, `an_older_snapshot_is_used_when_the_newest_is_past_the_target`, `incomplete_snapshots_are_ignored`.
  - `tests/durability/restore.rs`: `restoring_latest_reproduces_the_primary` (same rows, `integrity_check` is `ok`, chain equals `Replication::verify` at the position), `restoring_to_a_position_holds_exactly_the_writes_before_it`, `restoring_to_a_time_holds_no_segment_archived_after_it`, `a_tampered_segment_is_refused_as_data_loss`, `a_missing_snapshot_chunk_is_refused_as_data_loss`, `offline_restore_writes_a_plain_sqlite_database` (open `data.db` with `rusqlite` and read the rows).
  - `tests/database.rs`: `db_restore_creates_a_new_database_from_the_archive_and_serves_it` (three daemons sharing a `DirectoryStore` archive in a temp directory; write, restore to a new name, query it, write to it, its followers converge), `restore_refuses_a_database_that_already_has_a_lease`, `db_archive_verify_agrees_with_the_live_replicas`.
  - `tests/scopes.rs` (FR-047's binary): `restore_needs_admin_on_the_target_and_the_source` (a principal with `admin:restored/*` and `read:tenant:*` is refused restoring `tenant:acme` into `restored/x`; with `admin:tenant:*` too it succeeds), `describing_an_archive_needs_read_on_the_key`.

### Step 7: simulation

- `src/sim/fleet.rs`: `FleetBuilder::archive(self, name: &str, store: Arc<dyn ObjectStore>, configure: impl Fn(&mut ArchiveConfig) + 'static) -> Self`, which adds a `directory`-kind `[[archive]]` entry (path unused) to every data host and puts `store` in each host's `Environment::object_stores` under `name`, so every host writes to one store outside the simulated network that survives crashes. `Fleet::object_store(&self, name) -> Option<Arc<dyn ObjectStore>>`.
- `src/sim/check.rs`: `Invariant::ArchiveRestoresAcknowledgedPrefix` ("archive restores an acknowledged prefix") and

  ```rust
  /// Checks a restored copy's writes-table values against the history and the logical streams:
  /// the values are exactly those the file sinks recorded at or before `position`, include every
  /// write acknowledged before `complete_before` (when given), and none that was refused.
  pub fn archive_restores_prefix(history: &History, database: &DatabaseName, restored: &BTreeSet<i64>, position: Option<&Position>, streams: &[(String, Vec<ChangeRecord>)], complete_before: Option<Duration>) -> Result<(), Violation>;
  ```

- `tests/sim/archive.rs` (add `mod archive;` to `tests/sim/main.rs`): `archives_restore_an_acknowledged_prefix_after_faults`, over `seeds::FIXED_HEAVY`. A control host and three data hosts, each with a logical file sink (as `tests/sim/changes.rs` configures it) and an archive to one shared `MemoryStore` wrapped in `FaultyStore` failing every 7th call, with `segment_interval_ms = 200` and `snapshot_after_bytes = 64 KiB`. The writer runs `CreateAndWrite::new(1, 3, 400)`; the scenario crashes the primary, isolates the next one, flaps a replica (the faults of `tests/sim/changes.rs`), waits for workloads, convergence, and `ChangeStreams::await_caught_up`, then restores offline with `restore_into` (on the test thread, `Execution::Inline`) to: `Latest` (with `complete_before` the time the last write was acknowledged), the end position of the segment in the middle of the listing, and a time halfway through the run. Each restored copy is read with `rusqlite` and checked with `check::archive_restores_prefix`; the test also asserts that at least one segment was archived at each of two epochs and that the listing holds at least two snapshots. `Fleet::finish` must still pass.

### Step 8: documentation

- `crates/fleetringd/README.md`: a "Backups and restore" section (the archive model, recovery point, restore commands, the offline command for disaster recovery, credentials); the scope table rows.
- `deploy/fleetringd.toml`: a commented `[[archive]]` example for S3 and one for a directory, with every field and its default.
- `docs/roadmap.md`: E2's backup line links FR-049. `docs/tickets/README.md`: remove "Durability sinks" from the backlog. `docs/adr/0016-wal-frame-replication.md` point 3: an amendment note pointing to ADR-0029. `SECURITY.md`: one paragraph on archive credentials and that archives hold every row.
- This ticket's Progress section, and ADR-0029's status once accepted.

## Open risks

- **Snapshot pins hold checkpoints back while a large snapshot uploads.** A 10 GiB database over a slow uplink pins for minutes and the WAL grows meanwhile, as with follower snapshots (FR-032). A later change can upload from a local copy taken under the pin, at the cost of local disk.
- **Object count.** One segment per interval per busy database: 8,640 objects a day at the 10 s default. Compaction of old segments into larger ones is a follow-up if listing or request costs matter.
- **Store consistency.** The design assumes read-after-write and list-after-write consistency, which AWS S3, MinIO, and R2 give. A store without it can make a restore fail (never succeed wrongly); the docs say so.
- **`rusty-s3` has one maintainer.** The surface used is five calls and SigV4 signing; replacing it means re-implementing that over the same `hyper` stack, contained in `durability/s3.rs`.
- **Clock skew between primaries** shifts which prefix a time target restores, by up to the skew. It never makes the restore inconsistent (ADR-0029 point 11).

## Progress

- 2026-11-03: ADR-0029 accepted; implemented on the `archive` branch, in review. The plan predates the fleet rename (#48) and FR-041's merge (#73); it is followed against the code as it is now.
  - **Code:** `FrameSource::pin_snapshot_at` and `DatabaseHandle::pin_snapshot_at`; `Change::chain_start` and `chain_end`, `ChangeSink::pace`, and `PrimaryFeed::chain_at`; the `durability` module (`store`, `keys`, `format`, `s3` with `s3/http` and `s3/credentials`, `config`, `archive`, `listing`, `retention`, `plan`, `restore`); `[[archive]]` in `Config`, `Environment::with_object_store`, and the daemon building each archive; `Replication::install_restored` and `restore_staging`; `DataNode::restore`; `Database.Restore`, `DescribeArchive`, and `VerifyArchive`, in `auth::RPCS`, the proto's `// Scope:` lines, and the README's scope table; `Client::restore_database`, `describe_archive`, and `verify_archive`; `db restore`, `db archive show`, `db archive verify`, and `archive restore`; `FleetBuilder::archive`, `Fleet::object_store`, `Fleet::committed_values`, `sim::written_values`, and `check::archive_restores_prefix`.
  - **Tests:** the three named tests in `tests/actor/pinned.rs`; the two in `tests/changes/file.rs`; the unit tests in `keys.rs`, `format.rs`, `store.rs`, `plan.rs` (all seven named), and `retention.rs`; the new `tests/durability` binary (`store`: the conformance suite on every store, `directory_store_refuses_dot_components`, `faulty_store_fails_exactly_every_nth_call`; `s3`: the five named tests and the ignored real-endpoint test; `archive`: the six named tests, and `a_new_primary_continues_the_archive_from_the_mark_without_a_snapshot`, for the failover criterion, where a new archive at the next epoch finds the segment that ends at the replicated mark; `restore`: the six named tests); `mod archives` in `tests/database.rs` (the three named tests, the last also running the offline `archive restore` binary); `mod archives` in `tests/scopes.rs` (both named tests); `tests/sim/archive.rs` and a unit test of its check.
  - **Deviations from the plan:**
    - **A fix to FR-044 first** (`fix(fleetringd): a chain derived from where the log begins hashes what verify reads`). A replica whose chain has no record that fits its log derives one from where the log's generation begins, and hashed the log as read from its beginning, which carries the generation's header, while `verify` reads back from the anchor's position, which doesn't: the two disagreed at every position. A new database's primary derives its chain this way whenever the chain is built after the first write, so `db verify` reported hashes the chain didn't hold, and the archive, which carries the primary's chain, would have disagreed with `db verify`. Recovery now reads back from the batch's own start too; `a_chain_derived_from_where_the_log_begins_agrees_with_verify` fails without it.
    - **Step 1's tests are in `tests/actor/pinned.rs`**, beside FR-032's pinned-snapshot tests and their `copy` helper, not in `sqlite.rs`. Pinning at the position the current log continues from (the last checkpoint's end) is accepted as well as `None`: the copy is the main file alone, which `open_follower` opens at no position with that `log_start`.
    - **`PrimaryFeed::chain_at` is async.** A database with no follower has no stream to extend the primary's chain, so it brings the chain up to the position through `primary::chain_to` first. Only physical changes carry the chain. A change from the log's beginning carries `chain_start` only on a log that began empty and whose chain was built before its first write: a chain derived from the generation's start, or anchored at a checkpoint's end, holds no hash at `None`. The pace test asserts what pacing guarantees, at least `pace` between deliveries and so a bound on their number that grows with how long the writes took, instead of "at most 8 changes in a second", which a loaded machine breaks; its sink refuses a change without the chain, as an archive does, so that the chain test doesn't depend on when the chain was built.
    - **Keys refuse components that begin with `.`**, not only `.` and `..`, in every store alike: `DirectoryStore` writes each object to a hidden temporary file before renaming it, and a listing skips them. `StoreError::is_transient` tells callers what to retry.
    - **`jiff` is a direct dependency**: rusty-s3's signing call takes a `jiff::Timestamp`, built from the daemon's clock. The fake S3 can also deny the next requests with 403, for `a_503_is_retried_and_a_403_is_rejected`. A credentials file is checked for a change at most once a second by the daemon's clock.
    - **A base snapshot at the delivery's end, not its start** (ADR-0029 point 8, amended). Without a path to a delivery's start, the sink snapshots at the delivery's end and uploads no segment for it. The delivery is acknowledged, so the copy holds nothing unacknowledged, and the sink needs no chain hash at the start, which the primary doesn't hold for the log's beginning after a checkpoint (see above).
    - **No `Snapshotters` task** (point 8, amended). After each segment the sink takes the snapshot that is due, by `snapshot_interval_ms` or `snapshot_after_bytes`, at the segment's end, under the per-database lock it already holds. An idle database gets no new snapshot, which would hold nothing new; nothing needs supervising from `Replication::served`.
    - **The sink verifies the chain**: it recomputes each delivery's chain over its batch from the hash the archive holds at the start, and refuses a delivery whose result isn't the primary's `chain_end`; a mismatch at the start takes a new base snapshot instead, and so does a result that differs when the primary's chain didn't hold the start, so the archive's hash there went unchecked (after review: such a delivery had been refused forever, until the stream reset; `a_chain_the_primary_cant_check_that_differs_takes_a_new_base_snapshot`).
    - **Per-call retries.** Each store call (`retrying`, in `store.rs`) is retried three times with a doubling backoff before an operation fails, so that `FaultyStore` failing every second call, as `a_failing_store_delays_the_archive_but_never_a_write` has it, never fails an operation of several calls, such as a snapshot upload or a listing, at the same call every time.
    - **Each chunk has a checksum** in the descriptor, `chunk_checksums` (point 5, amended), so a restore names the chunk that changed. **Retention** deletes incomplete snapshots older than the oldest kept complete one, not the newest (point 9, amended), so one another primary is uploading during a failover is never deleted under it. It keeps by reachability, not by position (point 9, amended, after review): a snapshot counts towards `keep_snapshots` only if a walk over the segments leads from it to the archive's furthest point, and every segment a kept snapshot's walk reaches is kept. Otherwise a snapshot an old primary took past the mark a new primary resumed from, which leads nowhere, counted as a window, and the older snapshot and the new primary's segments from it were deleted (`a_snapshot_past_the_new_primarys_mark_leaves_later_points_restorable`, `a_snapshot_past_a_new_primarys_mark_doesnt_orphan_its_segments`).
    - **`Listing` has its own module** (`durability::listing`), and objects that don't read back as what their names say are listed as `damaged`, never used and never fatal: a damaged descriptor doesn't stop an archive or a restore from another snapshot. `ArchiveSettings` (with the chunk size) and `Archive::with_settings` let tests run small chunks and fast segments. `[[archive]]`'s `endpoint` defaults to AWS's for the `region`, and `credentials` to `env`.
    - **The self-replicating refusal is tested on `Archive::new`**, since a daemon's configuration can't choose an engine (it always runs SQLite); the daemon builds archives through it, so the refusal is the one a start would hit.
    - **Restore.** `restore_into` takes the source's name, to check headers and descriptors against it, and an `Arc<dyn DatabaseEngine>`, since its blocking steps run on other threads. The latest point is the furthest a walk reaches, preferring the newest epoch, as for every target; a segment past it that the snapshot's segments, of any epoch, don't lead to is a gap, refused as `Unreachable`, never skipped, and one they do lead to is a superseded epoch's (after review: the latest point had been the furthest end of any segment, so it was unreachable while an old and a new primary's segments from one position overlapped). A time target refuses a dead end only if something archived by then that the walk can't lead to lies past it. A position that isn't a transaction boundary is `RestoreError::NotBoundary`, naming the boundaries around it. `finish_offline` opens the copy as a primary and checkpoints it through the engine, so the offline command stays engine-neutral and leaves `data.db` complete on its own.
    - **CLI.** `db archive show`, not `describe`: `show` is the CLI guide's verb for one item in detail, and `restore` is added to its verbs. `show` and `verify` take `--address`, and otherwise ask the first live member, so they work for a source that was dropped. The client reuses `durability::RestoreTarget` instead of a second `RestorePoint` enum. `DescribeArchiveResponse` also reports incomplete snapshots and damaged objects. A restore into a database that was never leased replaces a copy a failed restore left, so a retry finishes it; `DataNode::restore` refuses a database with any lease or primary.
    - **Simulation.** The check compares each restored copy with the primary's final copy in commit order (the writes table's row order) rather than with file sinks' positions: `check::archive_restores_prefix(history, database, restored, committed, complete_before)`. The scenario runs one fixed seed (`FIXED_HEAVY[0]`), with 200 writes rather than 400, keeps 100 snapshots so that every epoch's segments stay and restores cross the failovers, and restores at a time halfway between the oldest snapshot and the last segment; it passed seeds 1, 5, 7, 11, 24, 29, 42, 99, 314, 1234, and 2026 locally.
    - **`VerifyArchive` needs admin** (ADR-0029 point 17, amended after review), not read or fleet: it builds a whole scratch copy on the answering node's disk. A node runs one verification or restore at a time and answers another with `UNAVAILABLE`. `describing_an_archive_needs_read_on_the_key_and_verifying_it_admin` replaces `describing_an_archive_needs_read_on_the_key`.
    - **Creates and restores of one name are serialized on a node** (after review): otherwise a restore's removal of a never-leased copy could delete what a concurrent restore or create of the same name had just installed.
  - **Documentation:** a "Backups and restore" section in the daemon README, commented `[[archive]]` entries in `deploy/fleetringd.toml`, an Archives section and asset in `SECURITY.md`, the roadmap's E2 line, ADR-0016 point 3's amendment note, ADR-0029's amendment notes, and the lexicon's Archive, Segment, Snapshot, and new Restore entries.
  - **Real endpoint.** `FLEETRING_S3_TEST_ENDPOINT`, `FLEETRING_S3_TEST_BUCKET`, `FLEETRING_S3_TEST_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` (plus `FLEETRING_S3_TEST_PATH_STYLE=1` for MinIO, and `FLEETRING_S3_TEST_CA` for a private authority), then `cargo test -p fleetringd --test durability -- --ignored a_real_endpoint_conforms`. Not run here.
