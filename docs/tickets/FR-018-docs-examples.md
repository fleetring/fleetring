---
id: FR-018
title: Crate documentation, examples, and README accuracy
status: done
priority: P1
size: M
milestone: M4 Hardening and release
depends_on: [FR-011, FR-012, FR-013]
adrs: [ADR-0001, ADR-0004, ADR-0007, ADR-0008]
---

# FR-018: Crate documentation, examples, and README accuracy

## Goal

A new user can place a database within five minutes and knows exactly what is guaranteed.

## Scope

- **Crate-level rustdoc**
  - What FleetRing places.
  - Home placement versus actual placement.
  - Preference lists and replica sets.
  - Membership versus liveness.
  - Epochs and fingerprints.
  - Determinism guarantees, with a link to the spec.
  - How to choose a strategy.
  - A table of feature flags.
- **Item docs:** every public item has docs and an example that uses `?` (no `unwrap` in docs). Each has `# Errors` and `# Panics` sections where relevant.
- **Positioning (ADR-0011):** present the crate as general-purpose placement. Docs say "key", not "database", and the examples span domains: a database fleet, key-value sharding, and chat-room failover.
- **`examples/`**
  - `database_fleet.rs`: tenant databases across zones (the FleetRing scenario)
  - `kv_sharding.rs`: cache keys over weighted nodes
  - `chat_failover.rs`: chat rooms with a liveness predicate
  - `rebalance.rs`: queue partitions; add a node, then diff and summarize
  - `bounded_loads.rs`: LLM conversations on GPU nodes by KV-cache bytes
  - `spec_roundtrip.rs`: needs the `serde` feature
- **Crate README:** `crates/fleetring/README.md` gets a quickstart aimed at crate users.
- **Root README:**
  - Planned features match ADR-0004 and ADR-0007.
  - New determinism section.
  - Links to `docs/adr/`, `docs/spec/`, and `docs/tickets/`.
- **Changelog:** start `CHANGELOG.md` in Keep a Changelog format.

## Acceptance criteria

- [x] `cargo doc --no-deps --all-features` with `-D warnings` is clean, and so is `missing_docs`.
- [x] Every example runs in CI.
- [x] README claims match the accepted ADRs: no jump hash, and bounded loads described as an assignment-time policy.

## Progress

- 2026-10-28: Done. A subagent built it in an isolated copy, and I merged and re-verified it.
  - **Crate docs:** now cover concepts (keys, nodes, replica sets, preference lists, home vs. actual placement, membership vs. liveness, epochs and fingerprints), choosing a strategy (a table built from `docs/perf.md`), determinism, and feature flags.
  - **Examples:** six domain-flavored programs, run in CI by looping over `examples/*.rs`.
  - **`CHANGELOG.md`:** started in Keep a Changelog format.
  - **Root README:** gains a "Design docs" section.
- Fixes found while documenting:
  - **Allocation claim:** the docs now say replica lookups don't allocate *for up to 8 replicas*, not "never".
  - **Bounded-load docs:** they now say `r = min(R, N)`, matching the code.
  - **Wording:** single-leader wording replaces mentions of `fleetringd` in item docs.
- Note: the docs mention golden vectors, which land with FR-014 before the release.
