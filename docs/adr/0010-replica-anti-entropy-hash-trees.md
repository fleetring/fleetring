# ADR-0010: Replica anti-entropy with page-level hash trees

- **Status:** Proposed, deferred until replication work starts (MVP steps 2–3)
- **Date:** 2026-10-27
- **Scope:** the replication and migration layer (`fleetringd`, or a `fleetring-repl` crate). It does **not** apply to the `fleetring` placement crate, which never moves data (ADR-0001).
- **Related:** ADR-0001, ADR-0006

## Context

Cassandra, DynamoDB, and Riak use Merkle trees for anti-entropy:
1. Replicas compare root hashes.
2. They walk down only the branches that differ.
3. They sync just those keys.

Those systems are leaderless. Any replica accepts writes, so replicas drift apart in arbitrary key ranges, and the tree is the main repair mechanism.

FleetRing is different in two ways.

**It has a single leader per database.** `docs/mvp.md`: "A database's first ring node is its primary; the next N−1 are followers pulling its WAL." The primary's WAL is authoritative and ordered. In steady state a follower converges by replaying frames from a known position, so there is nothing for a tree to find. Replicas drift only in three cases:
1. A follower stayed offline longer than the primary keeps WAL. This is the "sleeping laptop".
2. A failover left the old primary with frames that were never replicated.
3. A bug, disk corruption, or a bad migration or restore.

**The replication unit is a SQLite page, not a row.** FleetRing doesn't own the storage engine ("Not building: a custom storage engine"). Trees over rows or key ranges would mean hooking into SQL execution, for example through SQLite's session extension, which is the wrong layer.

## Decision (proposed)

1. **WAL shipping stays the sync path.**
   - Followers track a position `(epoch, txid)` and pull frames after it.
   - After a failover, a divergent tail is truncated back to the last common position, as Raft's log matching does. That step needs no hashing.
2. **Every database keeps a cheap whole-database checksum, updated on every write.**
   - The checksum is the XOR of per-page hashes. XOR doesn't depend on order, and a changed page updates it in O(1).
   - LiteFS takes a similar approach: its LTX format carries a database-wide checksum built from per-page checksums.
   - Primary and followers compare checksums after applying each transaction batch, and after any migration or restore.
   - Equal checksums show the follower matches the primary. A mismatch means drift.
3. **On a mismatch, or when a follower is older than the WAL the primary keeps, a page-level hash tree finds the difference.**
   - Leaves are page hashes. Internal nodes cover fixed fan-out page ranges (for example, 64 pages each).
   - Peers compare roots, descend only into subtrees that differ, and transfer only the pages that differ.
   - For *d* differing pages out of *P*, this exchanges about *d · log₆₄(P)* hashes instead of a full snapshot. That makes "close a lid, open it, it catches up" cheap even for large databases on slow links.
4. **The tree is built on demand, not maintained on writes.** It comes from page hashes when it's needed, or is cached per checkpoint. The write path only pays the XOR update.
5. **Page and tree hashes use BLAKE3.**
   - BLAKE3 is fast (SIMD, gigabytes per second per core) and cryptographic, so the same digests can prove tamper evidence for S3 snapshots and encrypted cloud backups, the first paid product.
   - XXH3-128 is the fallback if profiling shows BLAKE3's cost.
   - The 64-bit placement hash (ADR-0002) is never used for integrity.

## Alternatives considered

- **A Merkle tree as the main sync mechanism (Dynamo style).** It's built for leaderless stores, it duplicates the work an ordered WAL already does, and keeping it current costs work on every write.
- **A page version index** (each page's last txid, so a follower can ask for pages changed since txid *T*). It needs one round trip and is exact. But it costs per-page metadata on every write, and it needs history the primary may have discarded. It complements the tree: use the index while history exists, and fall back to the tree when history is gone or untrusted.
- **A full snapshot on any drift.** Simplest, but expensive for large databases and slow links.
- **Row-level Merkle trees.** The wrong layer for a system that replicates SQLite files.

## Consequences

- Nothing changes in the `fleetring` placement crate.
- The replication layer gains three pieces:
  - a checksum update when WAL frames are applied;
  - a tree-diff protocol (request subtree hashes by page range);
  - page transfer.
- Followers can prove they match the primary, which gives a strong integrity story after migrations and restores.
- Questions to answer when replication work starts:
  - The checksum should cover the logical database at a txid, not the file. How does it interact with WAL checkpoints?
  - How should per-database page sizes be handled?
  - What tree fan-out works best?
  - Do libSQL's frame hooks already expose what's needed?
