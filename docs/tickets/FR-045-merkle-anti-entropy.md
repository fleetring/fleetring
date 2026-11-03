---
id: FR-045
title: Merkle anti-entropy over pages, so a returning follower fetches only what differs
status: todo
priority: P2
size: L
milestone: M9 Durability sinks and verification
depends_on: [FR-044, FR-032, FR-047]
adrs: [ADR-0010, ADR-0016, ADR-0030]
---

# FR-045: Merkle anti-entropy over pages, so a returning follower fetches only what differs

## Context

A follower that falls behind the log's retention is re-seeded from a full snapshot (FR-032), however little of the database actually changed while it was away. ADR-0010 deferred the standard answer: a hash tree over the database's pages, as Cassandra and Riak build over key ranges for anti-entropy repair, so two copies compare roots, descend only into subtrees that differ, and exchange the leaves that do. For FleetRing the leaves are database pages, which makes the comparison exact (a page is either identical or not) and the repair a page copy. The same tree gives `db verify` (FR-044) a proof that two copies are byte-identical, not only that their logs agree, which also catches a corruption that never went through the log.

ADR-0030 settles the design and supersedes ADR-0010: the tree covers the main file as of the last checkpoint, which a snapshot pin holds still; the engine adapter folds each checkpoint's pages into it and keeps it crash-safe beside the file, with leaf hashes stored and interior levels recomputed; a returning follower settles and rescans its own copy, descends the primary's tree over a `Replication.Repair` stream, fetches the blocks that differ, and takes the snapshot's other files whole, so the result is byte-identical to a snapshot install; and page verification always rescans.

## Scope

- **A page hash tree per database**, BLAKE3 over page content, with a fan-out that keeps the tree for a 10 GiB database at 4 KiB pages under a few megabytes. The primary maintains it incrementally from the frames it ships (each batch names the pages it changed), and persists it beside the database so a restart doesn't rebuild it. Followers maintain theirs from the batches they apply.
  - *Refined by ADR-0030:* the tree describes the main file at the last checkpoint and is folded at each checkpoint from the frames the checkpoint copies, on the primary and on followers alike. The interior levels, all that a repair exchanges, are about 1.3 MiB at 10 GiB with a fan-out of 64; leaf hashes are stored beside the file (0.8 % of its size) and never sent whole.
- **Repair instead of re-seed:** a follower whose position no longer fits the log compares its tree with the primary's, descending only where roots differ, then fetches the differing pages and the log from the primary's current position. Both copies must be at a checkpoint boundary (no pending WAL frames) for the comparison, which the existing snapshot pin provides.
- **`db verify --pages`:** compares every replica's tree root with the primary's and reports byte-identical or the differing page ranges.
- **Not in scope:** repair between two followers without the primary, and trees over anything but whole pages.

## Definition of done

- [ ] A follower that was away while 1% of a database's pages changed catches up by fetching about 1% of the pages, measured in the replication tests, and ends byte-identical to the primary (`db verify --pages` agrees and SQLite's `integrity_check` passes).
- [ ] The tree stays consistent with the database across checkpoints, restarts, and snapshot seeding; a rebuilt tree equals the maintained one.
- [ ] A page corrupted on one replica outside the log is found by `db verify --pages` and repaired by the same path.
- [ ] Tree maintenance cost per batch is measured in `docs/perf.md`. *(Per ADR-0030 the work happens per checkpoint; `docs/perf.md` reports it per checkpoint and amortized per batch at the default threshold.)*
- [ ] ADR-0030 is accepted, ADR-0010's status reads "Superseded by ADR-0030", and both index rows say so.
- [ ] A repair interrupted at any point leaves a copy that is either the old one, the repaired one, or treated as missing and re-seeded in full; never a mix that a stream applies to.
- [ ] `Replication.Repair` and `Database.RepairReplica` are in `auth::RPCS` (node, and admin on the key), with matching proto comments and README rows.

## Implementation plan

Start once ADR-0030 is accepted. Each step is about one commit (`feat(fleetringd): …`, `Refs: FR-045`). Paths are under `crates/fleetringd/` unless they say otherwise. New public types derive `Debug` and the applicable traits and join their module's `assert_debug_send_sync` list; errors are `#[non_exhaustive]` with lowercase messages.

### Step 1: the tree and its file (`engine::tree`, engine-neutral)

- New `src/engine/tree.rs`, declared and re-exported in `src/engine/mod.rs`:

  ```rust
  /// A hash in a block tree: 32 bytes of BLAKE3.
  #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
  pub struct TreeHash([u8; 32]);   // Display and Debug as 64 lowercase hex digits; as_bytes, from_bytes

  /// A hash tree over a file's fixed-size blocks (ADR-0030 point 2): leaves are
  /// `BLAKE3(index u64 LE || block)`, interior nodes hash up to 64 children, and the root binds
  /// the block size and count.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct BlockTree { block_size: NonZeroU32, leaves: Vec<TreeHash>, levels: Vec<Vec<TreeHash>>, dirty: BTreeSet<u64> }

  impl BlockTree {
      pub const FAN_OUT: usize = 64;
      /// An empty tree: no blocks.
      pub fn new(block_size: NonZeroU32) -> Self;
      /// Reads `file` block by block and builds its tree. A trailing partial block is an error.
      pub fn scan(file: &Path, block_size: NonZeroU32) -> io::Result<Self>;
      pub fn block_size(&self) -> NonZeroU32;
      pub fn blocks(&self) -> u64;
      /// Records block `index`'s new content, growing the tree if `index` is past its end.
      pub fn set_block(&mut self, index: u64, block: &[u8]);
      /// Drops every block from `blocks` on.
      pub fn truncate(&mut self, blocks: u64);
      /// The number of levels, leaves included; 1 for a tree of at most one block.
      pub fn levels(&mut self) -> u32;
      /// The root (recomputing dirty nodes first).
      pub fn root(&mut self) -> TreeHash;
      /// The hashes of the children of node `index` at `level` (level 0 is the leaves), in order.
      pub fn children(&mut self, level: u32, index: u64) -> Vec<TreeHash>;
      /// The hash of node `index` at `level`, if there is one.
      pub fn node(&mut self, level: u32, index: u64) -> Option<TreeHash>;
      /// The leaves of `other` that differ from this tree's, by index, plus every index past the
      /// shorter tree's end; a test helper and the follower's final check.
      pub fn diff(&mut self, other: &mut BlockTree) -> Vec<u64>;
  }
  ```

  The root is `blake3::Hasher::new_derive_key("fleetring page tree v1")` over `block_size (u32 LE) || blocks (u64 LE) || top node` (the top node of an empty tree is 32 zero bytes).
- `TreeFile`, the persisted form, in the same module:

  ```rust
  /// A tree's leaves on disk, updated in place (ADR-0030 point 4). Layout: magic `FRTREE\x01\x00`,
  /// state byte (0 clean, 1 updating), 7 reserved zero bytes, block size u32 LE, blocks u64 LE,
  /// position length u16 LE, position bytes (at most 64; the checkpoint the tree describes),
  /// padding to 128 bytes, then 32 bytes per leaf.
  pub struct TreeFile { path: PathBuf }
  impl TreeFile {
      pub fn new(path: PathBuf) -> Self;
      /// The tree, if the file is clean and describes `position`; `None` otherwise (absent, torn,
      /// updating, or another position), and the caller rebuilds.
      pub fn load(&self, position: Option<&Position>, block_size: NonZeroU32) -> io::Result<Option<BlockTree>>;
      /// Writes the whole tree, as after a rebuild: to `<path>.tmp`, synced, renamed, directory synced.
      pub fn write_all(&self, tree: &BlockTree, position: Option<&Position>) -> io::Result<()>;
      /// Marks the file updating and syncs, writes the leaves `changed` at their offsets, truncates
      /// or extends to the tree's count, then writes the header clean with `position` and syncs.
      pub fn update(&self, tree: &BlockTree, changed: &BTreeSet<u64>, position: Option<&Position>) -> io::Result<()>;
  }
  ```

- Unit tests in `tree.rs`: `a_scanned_tree_equals_one_built_block_by_block`, `changing_one_block_changes_the_root_and_one_path`, `a_moved_block_changes_the_root`, `files_of_different_sizes_never_share_a_root`, `children_concatenate_to_their_parent`, `diff_names_exactly_the_changed_and_extra_blocks`, `a_tree_file_round_trips_and_refuses_another_position`, `an_updating_file_loads_as_none`, `an_in_place_update_equals_a_full_write`. A property test (proptest, in the same module): for random block sets and random updates, an updated tree's root equals a fresh scan's.

### Step 2: the SQLite adapter keeps the tree

- `src/engine/sqlite.rs`:
  - `const TREE_FILE: &str = "data.db-tree";` and `fn tree_path(database: &Path) -> PathBuf`.
  - A private `fn fold_frames(tree: &mut BlockTree, batch: &fleetring_wal::Batch, page_size: u32) -> BTreeSet<u64>`: for each frame in order, `tree.set_block(page_number - 1, page_bytes)`, collecting the indices; then `tree.truncate(commit_size)` from the batch's last commit frame. Pages repeat within a batch; the last image wins because frames are applied in order.
  - `SqliteDatabase` gains `tree: Option<BlockTree>` (loaded in `open` with `TreeFile::load` against the checkpoint marker's position; rebuilt with `BlockTree::scan` and `TreeFile::write_all` when `None`, after any WAL recovery the open performs, which never touches `data.db` with automatic checkpoints off).
  - `FrameSource::checkpoint`: after the pin check and before `write_marker`, read `self.read_wal(None)?` (the generation being checkpointed), `fold_frames` into the tree, then checkpoint as now, then `TreeFile::update(&tree, &changed, Some(&position))`. A tree update failure is logged at `warn` and the tree file deleted, so the next open rebuilds; the checkpoint itself has succeeded.
  - `FrameSource::pin_snapshot`: when the tree file is clean for the pinned marker, open it and add it as a `SnapshotFile` named `data.db-tree` before the WAL; set the pinned tree with `PinnedSnapshot::with_tree("data.db", tree.clone())` (step 3).
  - `src/engine/sqlite/follower.rs`, `SqliteFollower`: a `tree: Option<BlockTree>` loaded in `open` like the primary's (against `checkpointed`). In `FrameFollower::checkpoint` and in `apply`'s new-generation branch, before `checkpoint_database`, read the mirrored generation (`read_wal_file(self.mirror.path(), None, self.checkpointed)`), fold it, and update the tree file after the checkpoint with the marker's position.
- `FrameSource` gains (in `src/engine/adapter.rs`):

  ```rust
  /// The maintained tree's summary: the block-structured file, its block count, root, and the
  /// checkpoint position it describes; `None` for an engine without one. Default `None`.
  fn tree_summary(&mut self) -> Option<TreeSummary> { None }
  ```

  with `pub struct TreeSummary { pub file: String, pub block_size: NonZeroU32, pub blocks: u64, pub root: TreeHash, pub checkpoint: Option<Position> }` in `engine/tree.rs`.
- Tests in `tests/actor/sqlite.rs`:
  - `the_maintained_tree_equals_a_scan_after_every_checkpoint`: 30 rounds of writes to pages chosen from a fixed splitmix64 sequence (as `fleetring-control`'s `fixed_ops` generates) and checkpoints; after each, `tree_summary().root` equals `BlockTree::scan("data.db")`'s root.
  - `a_restart_loads_the_tree_without_rebuilding_it`: close and reopen; the tree file's modification time is unchanged and the root is equal.
  - `a_torn_tree_file_is_rebuilt_on_open`: flip the state byte to updating; reopen; root equals a scan.
  - `a_follower_keeps_the_same_tree_as_its_primary`: with the replication harness, after checkpoints on the primary and markers on the follower, both trees' roots and positions are equal; also across a new-generation `apply`.
  - `a_seeded_follower_starts_with_the_primarys_tree`: after FR-032's snapshot install, the follower's `data.db-tree` loads clean.

### Step 3: engine seams for repair

- `src/engine/pinned.rs`, `PinnedSnapshot`: `pub fn with_tree(self, file: impl Into<String>, tree: BlockTree) -> Self` and `pub fn tree(&self) -> Option<(&str, &BlockTree)>`.
- `src/engine/adapter.rs`, `FrameFollower`:

  ```rust
  /// Folds the copy's whole log into its main file so that the file holds everything the copy
  /// has and the log is empty, and names that file with its block size; `None` for an engine
  /// without a block-structured main file, whose copies are always re-seeded in full
  /// (ADR-0030 point 5). Default `None`.
  ///
  /// # Errors
  ///
  /// [`EngineError::Storage`] if the checkpoint fails.
  fn settle(&mut self) -> Result<Option<Settled>, EngineError> { Ok(None) }
  ```

  with `pub struct Settled { pub file: String, pub block_size: NonZeroU32, pub position: Option<Position> }`. The SQLite follower implements it with a TRUNCATE checkpoint of everything mirrored (folding the tree as in step 2) and writes the marker at its position.
- Tests: `settling_a_follower_empties_its_log_and_keeps_its_position` in `tests/actor/sqlite.rs`.

### Step 4: the `Repair` protocol

- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`, service `Replication` (new messages only):

  ```proto
  // A follower whose copy can't continue the log repairs it from the primary's pinned copy
  // (FR-045): it compares page hash trees, fetches the blocks that differ, then the copy's other
  // files, ending as a snapshot install would.
  // Scope: node.
  rpc Repair(stream RepairRequest) returns (stream RepairResponse);

  message RepairRequest {
    oneof request {
      RepairOpen open = 1;
      RepairDescend descend = 2;
      RepairFetch fetch = 3;
      RepairFinish finish = 4;
    }
  }
  message RepairOpen {
    string database = 1;
    uint64 epoch = 2;
    uint32 block_size = 3;
    uint64 blocks = 4;
    bytes root = 5;
  }
  // Asks for the children of `nodes` at `level` (level 0 is the leaves, so `level` is at least 1).
  message RepairDescend {
    uint32 level = 1;
    repeated uint64 nodes = 2;
  }
  message RepairFetch {
    repeated uint64 blocks = 1;
  }
  message RepairFinish {}
  message RepairResponse {
    oneof response {
      RepairStart start = 1;
      RepairNodes nodes = 2;
      RepairBlock block = 3;
      // The snapshot's other files, in the packed format of `Snapshot`; the last chunk carries
      // the position, checksum, and chain hash.
      SnapshotChunk file = 4;
    }
  }
  message RepairStart {
    string file = 1;
    uint32 block_size = 2;
    uint64 blocks = 3;
    uint32 levels = 4;
    bytes root = 5;
  }
  // The children of the requested nodes, in request order, at `level`.
  message RepairNodes {
    uint32 level = 1;
    repeated bytes hashes = 2;
  }
  message RepairBlock {
    uint64 index = 1;
    bytes data = 2;
  }
  ```

- New `src/replication/repair.rs`:
  - Primary side, `pub(super) async fn serve(inner, requests) -> Result<RepairStream, Status>`: require a node peer (`Peer::require_node("Repair")`); on `open`, check the database is served here at `epoch` (as `Snapshot` does), pin with `DatabaseHandle::pin_snapshot`, take `pinned.tree()` (`FAILED_PRECONDITION` "no page tree" otherwise, and the follower falls back), refuse a different block size, answer `RepairStart`; answer each `descend` with `children()` of each node, each `fetch` with one `RepairBlock` per index read from the pinned main file at `index × block_size` (past the end is `OUT_OF_RANGE`), and `finish` with the pinned snapshot's other files through `Packer`, skipping the main file, ending like `snapshot::serve`.
  - Follower side, `pub(super) async fn repair(inner, database, epoch, from: &NodeId, location: &Path, follower: Box<dyn FrameFollower>) -> Result<Seeded, RepairError>`:
    1. `follower.settle()`; `None` is `RepairError::Unsupported`. Drop the follower.
    2. Write the marker `<state>/<database dir>/repairing` (synced).
    3. `BlockTree::scan(location/settled.file)` on a blocking thread.
    4. Open the stream, send `open`, read `start`; if roots are equal skip to 6.
    5. Descend from the top: at each level ask for the children of the nodes that differ from the local tree's nodes at that level (all of them under a missing local node), down to the leaves; collect differing leaf indices and every index `>= local blocks`. Send `fetch` in batches of 256 indices; write each block in place with `FileExt::write_at` (`std::os::unix`, or `seek_write` on Windows) into the main file; then `set_len(blocks × block_size)` and `sync_all`.
    6. Send `finish`; unpack the other files into a staging directory with `Unpacker`, verify the checksum, and move each file over its counterpart in `location` (rename, then sync the directory).
    7. Rescan the main file: its root must equal `start.root`, else `RepairError::Mismatch`.
    8. Remove the marker. Return `Seeded { position, chain }` from the last chunk.

    ```rust
    #[non_exhaustive]
    pub(super) enum RepairError {
        #[error("the engine keeps no page tree")] Unsupported,
        #[error("the repaired copy's root differs from the primary's")] Mismatch,
        #[error("repair failed: {0}")] Status(#[from] Status),
        #[error("repair failed: {0}")] Io(#[from] io::Error),
    }
    ```
- `src/replication/follower.rs`, `reseed`: when the copy exists and `inner.settings.repair == RepairMode::Enabled`, try `repair::repair` first (with `open_timeout`); on any error, log at `info` with the reason and fall back to `snapshot::fetch` as today. After success, do exactly what a snapshot install does next (`failover::set_log_epoch`, `seeded_chain`, reopen with `open_follower`).
- `src/replication/mod.rs`: `ReplicationSettings` gains `pub repair: RepairMode` (`#[non_exhaustive] pub enum RepairMode { Enabled, Disabled }`, default `Enabled`) and the follower counts `repairs`, `repaired_blocks`, and `full_reseeds` in `FollowerStatus` (additive). `Replication::start` (and replication open of a copy) deletes any copy whose `repairing` marker exists, so it is re-seeded in full.
- `src/auth/policy.rs`: `rpc("/fleetring.v1.Replication/Repair", Requirement::Node)` (the `replication_is_for_nodes_only` test then covers it); README row `| \`Replication.Repair\` | node |`.
- Tests in a new `tests/replication/repair.rs` (`mod repair;` in `tests/replication/main.rs`), a database of 4,096 pages of 4 KiB filled with one row per page:
  - `a_follower_away_while_one_percent_changed_fetches_about_one_percent`: stop the follower; update 41 pages chosen by a fixed seed; force checkpoints past `lag_limit` so the log restarts past it; restart the follower. It repairs (not re-seeds), `repaired_blocks` is between 41 and 82 (header pages and index pages may change too), the copy's data `PRAGMA integrity_check` is `ok`, and its main file equals the primary's byte for byte.
  - `a_follower_that_diverged_after_a_failover_repairs_to_the_new_primary`: the ADR-0022 case of a replica whose tail diverged below the new epoch's start: it repairs and converges.
  - `a_repair_interrupted_after_some_blocks_is_redone_in_full`: kill the follower after the first `RepairBlock` (a test hook, `ReplicationSettings::repair_fail_after_blocks: Option<u64>`, `#[doc(hidden)]`), restart: the marker sends it to a full snapshot, and it converges.
  - `a_repair_whose_root_differs_falls_back_to_a_snapshot`: a primary whose tree file is replaced with a stale one (test hook) makes the follower's final check fail; it re-seeds in full and converges.
  - `repair_disabled_always_takes_a_full_snapshot`.

### Step 5: page verification

- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`:
  - `VerifyRequest` gains `// Rescan the main file's page tree too (FR-045).` `bool pages = 3;`
  - `VerifyResponse` gains `PageTree pages = 5;` with

    ```proto
    // A replica's page tree, rescanned from its main file, and the maintained one on a primary.
    message PageTree {
      string file = 1;
      uint32 block_size = 2;
      uint64 blocks = 3;
      bytes root = 4;
      // The checkpoint position the main file is at; empty for none.
      bytes checkpoint = 5;
      // The primary's maintained root; empty on a follower.
      bytes maintained_root = 6;
    }
    ```

  - service `Database`: `// Sends the follower a Reseed on its stream, which repairs or re-seeds its copy (FR-045). Answered by the primary.` `// Scope: admin on the key.` `rpc RepairReplica(RepairReplicaRequest) returns (RepairReplicaResponse);` with `message RepairReplicaRequest { string database = 1; string node_id = 2; }` and `message RepairReplicaResponse {}`.
- `src/replication/mod.rs`, `Replication::verify`: with `pages`, scan the copy's main file on a blocking thread (a follower settles nothing; it scans the file as checkpointed, with `log_start()` as `checkpoint`); on the primary also report `tree_summary().root` as `maintained_root`, and if the scan differs, log at `error`, replace the maintained tree with the scan (`TreeFile::write_all`), and still report both.
- `src/replication/primary.rs`: `Primary::request_reseed(&self, node: &NodeId) -> bool`, which sends `Reseed` on that follower's stream as `needs_snapshot` handling does; `Replication::repair_replica(database, node) -> Result<(), Status>` (`NOT_FOUND` if not a follower of this primary).
- `src/data/service.rs`: `repair_replica` handler (route to the primary as other data calls do); `src/auth/policy.rs`: `rpc("/fleetring.v1.Database/RepairReplica", Requirement::Key(Action::Admin))`; README row.
- `src/data/client.rs`: `Client::verify_pages(&self, database: &DatabaseName) -> Result<PageVerification, Status>` (each replica's `PageTree`, grouped by checkpoint), `Client::repair_replica(&self, database: &DatabaseName, node: &NodeId) -> Result<(), Status>`.
- `src/main.rs`, `db verify <DATABASE> --pages [--repair]`: prints one line per replica (`<node> checkpoint <pos> root <hash>`), then `all replicas at checkpoint <pos> are byte-identical`, or the replicas whose root differs, or `<node> is at another checkpoint (<pos>); not comparable`; and `the primary's main file changed outside the log` when its rescan differs from its maintained root. With `--repair`, calls `repair_replica` for each differing follower and prints `repairing <node>`.
- Tests in `tests/database.rs`:
  - `db_verify_pages_reports_byte_identical_replicas`: three daemons, writes, a checkpoint; one line of agreement.
  - `a_page_corrupted_outside_the_log_is_found_and_repaired`: overwrite 4 KiB in the middle of one follower's `data.db` (after a checkpoint, with the follower's database closed by stopping its daemon, then restarting it); `db verify --pages` names it; `--repair` repairs it; the next `db verify --pages` agrees and `integrity_check` passes on every copy.
  - `a_follower_at_another_checkpoint_is_reported_not_comparable`.
- `tests/scopes.rs`: `repairing_a_replica_needs_admin_on_the_key`.

### Step 6: measurement and documentation

- `benches/tree.rs` (divan, `[[bench]] name = "tree" harness = false` in `Cargo.toml`): folding a checkpoint of 4 MiB of WAL into the tree of a 64 MiB and a 1 GiB database, `TreeFile::update` for the same, and `BlockTree::scan` of 1 GiB. `docs/perf.md` gains a "Page trees" section with the per-checkpoint cost, the amortized per-batch cost at the default threshold and the `tests/replication/perf.rs` batch rate, and repair transfer for 1 % changed versus a full snapshot.
- `crates/fleetringd/README.md`: `db verify --pages [--repair]`, what repair does, and the `Replication.Repair` and `Database.RepairReplica` scope rows.
- `docs/adr/0010-replica-anti-entropy-hash-trees.md` status "Superseded by ADR-0030"; ADR-0030 Accepted with the date; both rows in `docs/adr/README.md`. `docs/review.md`: anti-entropy moves from a known gap to how it works. This ticket's Progress.

## Open risks

- **Local scan cost on the follower.** A repair scans the follower's whole main file (about 7 s per 10 GiB on one core at BLAKE3's speed, plus the read). It runs off the database's thread and the follower serves nothing for that database meanwhile, as during a re-seed; still far cheaper than moving the file. Hashing in parallel (`blake3`'s `rayon` feature) is a later option.
- **Pin duration.** The primary holds its pin while blocks are fetched; checkpoints wait as they do for a snapshot.
- **Engines without a block-structured file** never repair; they keep full re-seeds.
- **The tree file's two extra syncs per checkpoint** are measured in step 6; if they matter, the update can be batched every N checkpoints at the cost of more rebuilds after a crash.

## Progress

- 2026-11-02: ADR-0030 proposed; implementation plan written. Waiting for the ADR's acceptance.
