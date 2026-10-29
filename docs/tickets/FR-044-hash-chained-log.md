---
id: FR-044
title: A hash-chained replication log, so replicas can prove they share a prefix
status: todo
priority: P2
size: L
milestone: M9 Durability sinks and verification
depends_on: [FR-030, FR-032]
adrs: [ADR-0016, ADR-0018]
---

# FR-044: A hash-chained replication log, so replicas can prove they share a prefix

## Context

Each shipped batch is checked frame by frame with SQLite's cumulative checksum and XXH3 (FR-025), which catches corruption in transit and on disk. It can't answer the question an operator asks after an incident: do all three replicas hold the same log, and has anything changed since position P? Raft-style logs, including rqlite's, answer it by identity of (index, term) only; an audit needs the content covered too. A chain of cryptographic hashes does that: each batch's hash covers its payload and the previous batch's hash, so one hash at a position commits every byte before it, and two replicas that agree on the hash at P agree on the whole prefix. BLAKE3 is fast enough to run inline on every batch. The chain also gives an S3 durability sink (ADR-0016, point 3) something to carry, so an archive can be verified against the cluster without re-reading it.

## Scope

- **A chain hash per batch:** `hash_n = BLAKE3(hash_{n-1} || epoch || start || end || payload)`, with the generation's header folded into the first batch of each generation. The primary computes it when it captures a batch; followers recompute it when they apply one and refuse a batch whose hash doesn't match. The hash is stored with the batch's end position in the engine-neutral replication state (`epochs.rs` records, or a sibling file), never inside the SQLite WAL, so the engine's log stays byte-identical to SQLite's.
- **XXH3 stays** for the per-frame check; the chain is per batch and covers it.
- **`db verify <database>`:** asks every replica for its chain hash at a position (the latest common position by default) and reports agreement or the first divergence, plus "nothing changed since P" when the caller presents a hash it recorded earlier. Through the Node service, additive proto.
- **Durability sink:** the S3 sink, when it exists, stores each batch with its chain hash, so a restore can be verified end to end.
- **Not in scope:** signing (a hash chain proves consistency, not provenance), and verifying page content inside the database file (FR-045).

## Definition of done

- [ ] Every replica of a database reports the same chain hash at every common commit position in the replication tests, and a deliberately altered batch is refused by a follower and reported by `db verify`.
- [ ] `db verify` on a healthy three-replica database prints one hash and "all replicas agree"; on a database where one replica's log was tampered with, it names the replica and the first position that differs.
- [ ] Checkpoints and snapshot catch-up carry the chain across generations: a follower seeded from a snapshot continues the chain from the snapshot's position and agrees with the primary afterwards.
- [ ] The hash cost per batch is measured in `docs/perf.md` and stays under 5% of the primary's commit time at 4 KiB pages.
