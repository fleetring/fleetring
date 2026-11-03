# ADR-0030: Page hash trees over the checkpointed main file, kept by the engine adapter, for repair and page verification

- **Status:** Proposed (2026-11-02)
- **Date:** 2026-11-02
- **Supersedes:** ADR-0010 (proposed, deferred)
- **Related:** ADR-0016 (engine adapters, points 7, 8, and 12), ADR-0019 (protocol), ADR-0022 (re-seeding), ADR-0025 (node-only replication); tickets FR-032, FR-044, FR-045

## Context

A follower that can't continue the primary's log is re-seeded with a full snapshot (FR-032, ADR-0022 point 3), however little changed while it was away. ADR-0010 proposed the standard answer, a hash tree over pages, and deferred it until replication existed. Replication now exists, with a hash chain over the log (FR-044), and FR-045 asks for the tree. Building it raises questions ADR-0010 left open or answered for a design that changed since:

- **Over what?** The pages of a database live in two places between checkpoints: the main file holds the state as of the last checkpoint, the WAL the transactions since. ADR-0010 asked how a checksum of "the logical database at a txid" interacts with checkpoints.
- **Maintained, or built on demand?** ADR-0010 chose on demand, with an XOR checksum updated per write. FR-045 asks for a tree maintained incrementally and persisted, so that a restart doesn't rebuild it, and measured per batch.
- **Where does page knowledge live?** Pages are SQLite's concept; nothing above the adapter may know them (ADR-0016 point 2).
- **What is exchanged, and how does a repaired copy end up identical to a snapshot?**
- **How is a page corrupted outside the log found,** given that a maintained tree records what was written, not what is on disk now?

Facts that shape the answer: in WAL mode with automatic checkpoints off, the main file changes only when `fleetringd` checkpoints (ADR-0016 point 7), and a pinned snapshot holds checkpoints off (FR-032), so the main file under a pin is stable. A snapshot is the main file, the WAL prefix up to its position, and the checkpoint marker. Peers on the `Replication` service are authenticated nodes (ADR-0025, ADR-0026).

## Decision

1. **The tree covers the main file as of the last checkpoint, not the live database.** The tree is updated exactly when the main file is, at checkpoints, so a pinned snapshot's main file and its tree always agree without copying either. A repair transfers the main file's differing blocks and the snapshot's other files whole (the WAL prefix and the marker), so the repaired copy is byte-identical to a snapshot installed at the same position, and every rule about installed snapshots (log epoch, chain anchor, truncation limits) applies to it unchanged.

2. **Shape.** The main file is a sequence of fixed-size blocks, the engine's pages. A leaf is `BLAKE3(index as u64 little-endian || block bytes)`; binding the index makes a moved page a difference. Interior nodes hash the concatenation of up to 64 children, level by level, until one node remains. The root is `BLAKE3` keyed by the derived key of `"fleetring page tree v1"` over `block_size (u32 LE) || block_count (u64 LE) || top node`, so files of different sizes never share a root. BLAKE3, as ADR-0010 point 5 chose; the 64-bit placement hash is never used for integrity. For a 10 GiB database of 4 KiB pages, the interior levels are about 1.3 MiB, which is all a repair exchanges in the worst case; leaf hashes, 32 bytes per page (0.8 % of the database, 80 MiB at 10 GiB), are stored but never sent whole.

3. **The engine adapter maintains it, at every checkpoint, from the log it checkpoints.** Before a checkpoint copies the WAL into the main file, the adapter reads the frames it is about to copy, hashes the last image of each page, and folds those leaves into the tree; after it, it truncates the tree to the main file's new length. The primary does this in `FrameSource::checkpoint`, a follower in `FrameFollower::checkpoint` and in the checkpoint `apply` runs when a new log generation starts, with the same function. The cost is one BLAKE3 pass over the checkpointed log (4 MiB at the default threshold), amortized over the batches since the previous checkpoint; it never reads the main file. The tree's data structure (`BlockTree`) and file format are engine-neutral, in `fleetringd::engine::tree`; only which file is block-structured, its block size, and which blocks a checkpoint writes are the adapter's knowledge.

4. **Persisted beside the main file, updated in place, crash-safe.** The SQLite adapter keeps `data.db-tree`: a header (magic, format 1, block size, block count, the checkpoint position the tree describes, and a state byte) followed by the leaf hashes. A checkpoint sets the state to *updating* and syncs, writes the changed leaves at their offsets, then writes the new count and position with the state *clean* and syncs. On open, a clean file whose position equals the checkpoint marker is loaded and its interior levels recomputed in memory; anything else is rebuilt by scanning the main file once. Pinned snapshots include the tree file, which the pin keeps stable, so a seeded follower starts with a valid tree, and a promoted follower keeps its own.

5. **A repair replaces the full snapshot when the follower has a copy.** When a follower is told to re-seed (`Reseed`, or `needs_snapshot` on its side) and holds a copy whose engine has a block-structured main file, it first tries `Replication.Repair`, a bidirectional stream:
   1. The follower *settles* its copy (`FrameFollower::settle`: for SQLite, a TRUNCATE checkpoint, so the main file holds everything the copy has and the WAL is empty), then **rebuilds its tree by scanning its main file**; it doesn't trust a maintained tree on a copy that may be damaged. It opens the stream with the database, the epoch, its block size, block count, and root.
   2. The primary pins a snapshot and answers with its main file's name, block size, count, levels, and root, from the maintained tree.
   3. Where roots differ, the follower asks for the children of the top node, compares them with its own, and asks for the children of each that differ, level by level, down to leaf hashes: at most `levels` round trips.
   4. The follower fetches the blocks whose leaves differ, and every block past its own count; the primary reads them from the pinned main file.
   5. The primary streams the snapshot's other files (WAL prefix, marker, tree file) in the existing packed format, ending with the position, checksum, and chain hash a snapshot's last chunk carries.
   6. The follower writes the blocks into its main file in place, sets its length, replaces the other files, syncs everything, and rescans: the root must equal the primary's, or the follower falls back to a full snapshot. It then installs the result exactly as a snapshot (log epoch, chain anchor) and reopens.

   A marker file in the replication state directory (`<state>/<database>/repairing`) is written before the first block and removed after the root matches; a copy found with the marker after a crash is treated as missing and re-seeded in full. Any repair failure falls back to the full snapshot, so a repair can make catching up cheaper, never less safe.

6. **Page verification rescans, everywhere.** `db verify --pages` asks every replica to rescan its main file and report the root, the block count, and the checkpoint position it describes. Replicas at the primary's checkpoint position must report the primary's root; one at another position is reported as not comparable, with its position. The primary also reports its maintained root; if its rescan differs, the primary's main file changed outside the log, the CLI says so, and the primary rebuilds its maintained tree from the scan. `db verify --pages --repair` sends `Reseed` to each follower whose root differs, which takes the repair path. Repairing a damaged primary from followers is out of scope.

7. **Engine-neutral seams.** The adapter exposes the tree through: `PinnedSnapshot::tree()` (the main file's name and its tree, for the primary's side of a repair); `FrameFollower::settle()` (fold the log into the main file and name it with its block size, or `None` for an engine with no block-structured file, which then always takes full snapshots); `FrameSource::tree_summary()` (the maintained root, count, and checkpoint position, for verification); and `BlockTree::scan(path, block_size)` for rescans. The replication layer moves blocks and hashes by index and never learns what a page is.

8. **ADR-0010's other proposals are not adopted.** The per-write XOR checksum is unnecessary: FR-044's chain already proves two logs agree, and a linear XOR sum is weak as a proof of identity. Building the tree only on demand is kept for verification (point 6) and dropped for repair, where the primary would otherwise scan the whole database for every returning follower.

## Alternatives considered

- **A tree over the live, logical database,** updated per batch from the frames shipped. Always current, but a repair needs pages "as of" one position while writes continue, which means reading a page through the WAL index at the pinned position: SQLite knowledge above the adapter, or a copy-on-write tree.
- **No stored leaves,** recomputing a changed page's group from the main file at each checkpoint. It keeps the stored tree near 1.3 MiB at 10 GiB, but a checkpoint touching 1,000 scattered pages would read 1,000 groups of 64 pages, about 256 MiB, on every checkpoint.
- **On-demand trees only (ADR-0010 point 4).** No write-path cost at all, but every repair scans the primary's whole main file, and FR-045 asks for a maintained, persisted tree.
- **Sending whole groups of 64 pages instead of descending to leaves.** One round trip fewer, but with 1 % of pages changed at random almost half of all groups differ, so a repair would move about half the database.
- **Repair between followers.** Useful when the primary is far away, but it needs a rule for which follower is right; out of scope, as FR-045 says.

## Consequences

- The SQLite adapter keeps `data.db-tree` and does one hashing pass per checkpoint, plus two small syncs. Snapshots carry 0.8 % more bytes.
- A follower that returns after a short absence moves roughly the pages that changed, plus the WAL prefix, instead of the whole database, at the price of scanning its own copy locally.
- `db verify --pages` proves byte-identity of the checkpointed main files across replicas, and with FR-044's chain over the log since, the whole copy.
- The `Replication` service gains `Repair` (node only, ADR-0026), and `Verify` gains a pages mode; both are additive.
- ADR-0010 becomes "Superseded by ADR-0030".
