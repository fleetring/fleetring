---
id: FR-001
title: "Workspace hygiene: edition 2024, MSRV, lints, licenses"
status: done
priority: P0
size: S
milestone: M0 Foundations
depends_on: []
adrs: [ADR-0002, ADR-0003, ADR-0009]
---

# FR-001: Workspace hygiene: edition 2024, MSRV, lints, licenses

## Goal

Set up the workspace so it enforces ADR-0009 before any real code lands.

## Scope

- **Root `Cargo.toml`**
  - Add `resolver = "3"`.
  - In `[workspace.package]`, set `edition = "2024"`, `rust-version = "1.88"`, and `authors`. Keep the existing version, license, repository, and homepage.
  - Add `[workspace.dependencies]` entries for `twox-hash` (`default-features = false, features = ["xxhash3_64"]`), `thiserror` 2, `serde` 1, `smallvec` 1, `proptest` 1, `serde_json` 1, and `divan`.
  - Add `[workspace.lints.rust]` and `[workspace.lints.clippy]` exactly as listed in ADR-0009.
- **`crates/fleetring/Cargo.toml`**
  - Inherit the workspace fields (`version.workspace = true`, `edition.workspace = true`, and so on) and set `[lints] workspace = true`.
  - Add the features `default = ["std"]`, `std = []`, `serde = ["dep:serde"]`, and `test-util = []`.
  - Add `[package.metadata.docs.rs] all-features = true`.
  - Fix `readme = "README.md"`, which points at a file that doesn't exist. Either add a crate README or point it at `../../README.md`.
- **`crates/fleetring/clippy.toml`**
  - Set `disallowed-types` for `std::hash::DefaultHasher`, `std::collections::hash_map::DefaultHasher`, `std::hash::RandomState`, `std::collections::hash_map::RandomState`, `std::collections::HashMap`, and `std::collections::HashSet`. Give each a `reason` that cites ADR-0002.
  - Set `allow-unwrap-in-tests = true` and `allow-expect-in-tests = true`.
- **`crates/fleetring/src/lib.rs`**
  - Replace the placeholder `add()` with a crate-level `//!` summary.
  - Add `#![deny(clippy::float_arithmetic)]` with a comment that cites ADR-0003.
- **Licenses**
  - Add `LICENSE-MIT` and `LICENSE-APACHE` at the repo root. The README links to both, but neither exists yet.
  - Include both files in the crate package.

## Acceptance criteria

- [x] These all pass on stable:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `cargo test --workspace`
  - `cargo doc --no-deps`
- [x] `cargo +1.88 check --workspace --all-features` passes.
- [x] `cargo package -p fleetring --list` includes both license files and a README.
- [x] `cargo publish -p fleetring --dry-run` succeeds.
- [x] Temporarily adding `std::collections::HashMap` to the crate makes clippy fail. Check this once and don't commit it.
- [x] No placeholder code remains.

## Notes

- Declare a dependency only in `[workspace.dependencies]` until a ticket actually uses it, so unused dependencies never ship.
- Don't add a `rust-toolchain.toml` (see ADR-0009).

## Progress

- 2026-10-27: Done. fmt, clippy, test, doc, and MSRV 1.88 all pass. `cargo package` includes both licenses (symlinked from the root) and a crate README, and `cargo publish --dry-run` succeeds. A temporary `HashMap` makes clippy fail as intended.
- Deviation: the `serde` feature isn't declared yet. Cargo rejects a feature that names an undeclared optional dependency, so the feature lands with FR-013.
- Added: `[profile.dev.package."*"] opt-level = 2`, so hashing and proptest run fast in debug test builds.
- The MIT copyright line reads "2026 Sam Ruberti". Change it if the holder should be an organization.
