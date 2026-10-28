# ADR-0009: Toolchain, lint, dependency, and CI policy

- **Status:** Accepted (2026-10-27)
- **Date:** 2026-10-27
- **Related:** ADR-0002, ADR-0003, ADR-0008; tickets FR-001, FR-002, FR-016, FR-017, FR-019

## Context

The workspace is a fresh skeleton: edition 2021, a placeholder `add()`, no lints, and CI that only checks commit messages. The crate's main promise is identical placement everywhere. The only way to verify that is to run the same tests on the platforms where determinism usually breaks. The project follows Rust best practices from the Google Rust style guide, the Rust API Guidelines, *Effective Rust*, and Microsoft's Pragmatic Rust guidelines.

## Decision

1. **Edition and MSRV.**
   - Edition 2024 across the workspace, with `resolver = "3"` (dependency resolution that respects the MSRV).
   - MSRV is **1.88**, which brings let chains, `#[expect]`, and `core::error::Error`.
   - The MSRV rises only in a minor release, never to a toolchain less than six months old, and every change is noted in the CHANGELOG. CI checks the MSRV.

2. **Lints live in `[workspace.lints]`.**
   - **Rust:** `unsafe_code = "forbid"`, `missing_docs`, `missing_debug_implementations`, `unreachable_pub`, `unsafe_op_in_unsafe_fn`.
   - **Clippy:** `all`, `allow_attributes_without_reason`, `unwrap_used`, and the cast lints `cast_possible_truncation`, `cast_sign_loss`, `cast_possible_wrap`, and `cast_lossless`. Silent casts are a classic source of determinism bugs.
   - **Crate-specific invariants** are crate or module attributes, each with a comment that cites its ADR: `clippy::float_arithmetic` at the `fleetring` root (ADR-0003), and `clippy::arithmetic_side_effects` in the math module.
   - **`crates/fleetring/clippy.toml`** sets `disallowed-types` (ADR-0002) and allows `unwrap`/`expect` in tests.
   - **Suppressions** use `#[expect(lint, reason = "...")]`.

3. **Warnings fail CI, not the source.** There is no `#![deny(warnings)]` in the code. Instead, CI runs clippy with `-D warnings` and rustdoc with `RUSTDOCFLAGS=-D warnings`.

4. **Dependencies.**
   - **Runtime:**
     - `twox-hash` for XXH3.
     - `thiserror` 2, used for one error type per situation, each `#[non_exhaustive]`.
     - `serde`, optional, behind the `serde` feature.
     - `smallvec`, for replica sets that don't allocate. It never appears in the public API.
   - **Dev:** `proptest`, `serde_json`, `divan`.
   - **No `rand`** anywhere in placement or test corpora, because its algorithms aren't stable across versions. Corpora use an in-tree splitmix64 instead.
   - **`cargo deny`** checks advisories, bans, sources, and licenses against an allowlist: MIT, Apache-2.0, BSD-2/3-Clause, ISC, Unicode-3.0, and Zlib.

5. **Features** are additive and have bare names:
   - `std` is on by default. It's reserved now so `no_std + alloc` support can come later without breaking users who set `default-features = false`.
   - `serde` adds serialization.
   - `test-util` provides the test hasher double.

6. **CI matrix.**

   | Job | What it runs |
   |---|---|
   | fmt | `cargo fmt --all --check` |
   | clippy | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
   | test | Linux x86-64, macOS aarch64, and Windows x86-64; each with default features, `--all-features`, and `--no-default-features`; in debug and `--release` |
   | msrv | `cargo +1.88 check --workspace --all-features` |
   | docs | `cargo doc --no-deps --all-features` with `-D warnings` |
   | 32-bit | `cargo test --target i686-unknown-linux-gnu` |
   | big-endian | `cross test --target s390x-unknown-linux-gnu` |
   | wasm | `cargo test --target wasm32-wasip1` under wasmtime, plus `cargo build --target wasm32-unknown-unknown` |
   | deny | `cargo deny check` |
   | commits | the existing Conventional Commits check |
   | fuzz | nightly toolchain, run on a schedule (FR-017) |
   | semver | `cargo semver-checks`, starting with 0.1.0 |

7. **Commits** keep the existing Conventional Commits rules, using the crate area as the scope (`feat(ring): …`, `test(spec): …`) and a `Refs: FR-NNN` trailer.

## Alternatives considered

- **Edition 2021, or an MSRV at the 2024 edition floor (1.85):** gives up let chains for a tiny compatibility gain on a crate that has no users yet.
- **Pinning the toolchain in `rust-toolchain.toml`:** makes clippy runs reproducible, but hides new lints and MSRV drift. CI pins versions explicitly instead.
- **Setting `#![forbid(unsafe_code)]` in each crate:** equivalent, but the workspace setting keeps one source of truth. A future FFI crate can opt out explicitly.

## Consequences

- CI takes longer, since the cross-target jobs run tests under emulation. Vectors and property tests are sized so each job stays well under ten minutes. Heavy statistical runs happen only in a dedicated release-mode job.
- A new Rust release may add clippy warnings that fail CI until they're fixed. That's accepted and intended.
