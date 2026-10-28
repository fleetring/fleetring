---
id: FR-018
title: Crate documentation, examples, and README accuracy
status: todo
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
  - `locate.rs`
  - `failover.rs` (liveness predicate)
  - `rebalance.rs` (add a node, diff, summarize)
  - `bounded_loads.rs`
  - `spec_roundtrip.rs` (needs the `serde` feature)
- **Crate README:** `crates/fleetring/README.md` gets a quickstart aimed at crate users.
- **Root README:**
  - Planned features match ADR-0004 and ADR-0007.
  - New determinism section.
  - Links to `docs/adr/`, `docs/spec/`, and `docs/tickets/`.
- **Changelog:** start `CHANGELOG.md` in Keep a Changelog format.

## Acceptance criteria

- [ ] `cargo doc --no-deps --all-features` with `-D warnings` is clean, and so is `missing_docs`.
- [ ] Every example runs in CI.
- [ ] README claims match the accepted ADRs: no jump hash, and bounded loads described as an assignment-time policy.
