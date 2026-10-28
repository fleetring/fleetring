---
id: FR-001
title: "Workspace hygiene: edition 2024, MSRV, lints, licenses"
status: todo
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

- [ ] These all pass on stable:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `cargo test --workspace`
  - `cargo doc --no-deps`
- [ ] `cargo +1.88 check --workspace --all-features` passes.
- [ ] `cargo package -p fleetring --list` includes both license files and a README.
- [ ] `cargo publish -p fleetring --dry-run` succeeds.
- [ ] Temporarily adding `std::collections::HashMap` to the crate makes clippy fail. Check this once and don't commit it.
- [ ] No placeholder code remains.

## Notes

- Declare a dependency only in `[workspace.dependencies]` until a ticket actually uses it, so unused dependencies never ship.
- Don't add a `rust-toolchain.toml` (see ADR-0009).
