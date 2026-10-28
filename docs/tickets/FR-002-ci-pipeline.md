---
id: FR-002
title: CI pipeline across the platforms where determinism breaks
status: in-progress
priority: P0
size: M
milestone: M0 Foundations
depends_on: [FR-001]
adrs: [ADR-0008, ADR-0009]
---

# FR-002: CI pipeline across the platforms where determinism breaks

## Goal

Make CI prove the determinism promise (ADR-0008) on every platform class where it usually breaks: big-endian, 32-bit, wasm, and the three major operating systems.

## Scope

- **`.github/workflows/ci.yml`** runs the jobs from the ADR-0009 table:
  - fmt
  - clippy
  - the test matrix: Linux x86-64, macOS aarch64, and Windows x86-64; each with default features, `--all-features`, and `--no-default-features`; each in debug and `--release`
  - MSRV 1.88
  - docs, with `RUSTDOCFLAGS=-D warnings`
  - i686 tests
  - s390x tests via `cross`
  - `wasm32-wasip1` tests under wasmtime, plus a `wasm32-unknown-unknown` build
  - `cargo deny`
- **`deny.toml`**
  - License allowlist from ADR-0009.
  - Advisories set to deny.
  - Sources limited to crates.io.
  - Duplicate versions warned on, not denied.
- **Hygiene**
  - Set `permissions: contents: read`.
  - Add a `concurrency` group that cancels superseded runs.
  - Cache with `Swatinem/rust-cache`, install toolchains with `dtolnay/rust-toolchain`, and install `cross`, `wasmtime`, and `cargo-deny` with `taiki-e/install-action`.
  - Pin every action to a tag or SHA.
- **Statistical tests:** add a dedicated job that runs the heavy statistical suite (FR-006) in release mode, so the emulated targets stay fast.
- **Documentation:** list the required status checks in `docs/tickets/README.md`.

## Acceptance criteria

- [x] All jobs pass on a PR that contains the FR-001 skeleton. *(They pass on the push of `a4edb49` to main, run 40640646430.)*
- [ ] The big-endian job really runs tests. Prove it on a throwaway branch: a test asserting `1u64.to_ne_bytes() == 1u64.to_le_bytes()` fails only on s390x. Remove the test afterwards.
- [x] The i686 job runs tests, not just a build.
- [x] The `wasm32-wasip1` job runs the test binary under wasmtime.
- [ ] A full run takes less than about 15 minutes of wall time.

## Notes

- Run tests with `--no-fail-fast`.
- On wasm, property tests can be cfg'd out: proptest's fork and failure persistence features don't work there. Golden vectors and unit tests must still run (see FR-014).
- The existing Conventional Commits workflow stays as it is.

## Progress

- 2026-10-27: `.github/workflows/ci.yml` (10 jobs) and `deny.toml` are written. The YAML and TOML parse, and the `deny.toml` keys follow the current cargo-deny template (no `version` fields).
- Still open: none of the acceptance criteria can be checked until the workflow runs on GitHub. `cross`, `wasmtime`, and `cargo-deny` aren't installed locally.
- 2026-10-28: First runs on GitHub.
  - Run 40639603679: the i686 and wasm32-wasip1 jobs failed on one unit test that converted a 64-bit stride to `usize`. The failure shows both jobs really run tests; on wasm, the panic aborted the test binary under wasmtime. Every placement test, including the vectors and corpus checksums, passed on both targets. Fixed in `23fd170`.
  - Run 40640646430 (`a4edb49`): all 13 jobs pass, including s390x, i686, and wasm32-wasip1. The s390x integration tests ran under emulation (147 passed in 205 s). The throwaway-branch proof is still open: it costs a full CI run, so it waits until the repository is public and Actions minutes are free.
  - Wall time was 17 minutes, over the 15-minute target. The Windows test job is the long pole at 17 minutes, because it runs the suite four times. Ubuntu takes 13 minutes and macOS 3.
  - `actions/checkout` moved to v5, for Node 24, in `3931504`.
