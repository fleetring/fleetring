---
id: FR-003
title: StableHasher trait and the default Xxh3 hasher
status: todo
priority: P0
size: S
milestone: M1 Hashing and model
depends_on: [FR-001]
adrs: [ADR-0002]
---

# FR-003: `StableHasher` trait and the default `Xxh3` hasher

## Goal

Implement ADR-0002: a placement hash that never changes, behind a trait so users can plug in their own.

## Scope

- **`hasher` module:** the `StableHasher` trait, with `fn algorithm(&self) -> &'static str` and `fn hash(&self, bytes: &[u8], seed: u64) -> u64`. The rustdoc covers:
  - the stability contract;
  - what the seed means;
  - why `std::hash` is deliberately unused;
  - how to implement a custom hasher, with an example that pins its own vectors.
- **`Xxh3`:**
  - a unit struct deriving `Debug, Clone, Copy, Default, PartialEq, Eq, Hash`;
  - `algorithm()` returns `"xxh3-64"`;
  - `hash()` calls `twox_hash::XxHash3_64::oneshot_with_seed`.
- **Forwarding impls:** `impl<H: StableHasher + ?Sized> StableHasher for &H` and `for Box<H>`, so `dyn StableHasher` works.
- **`test-util` feature:** a deterministic test double whose outputs tests control (for example, a table from input bytes to hash, falling back to XXH3). FR-010 unit tests need it to place keys and tokens at exact positions.
- **Reference vectors**
  - Cover XXH3-64 at input lengths 0, 1, 3, 4, 8, 9, 16, 17, 128, 129, 240, 241, and 1024 bytes, which together hit every XXH3 code path.
  - Use seeds `0`, `1`, and `0x9E37_79B9_7F4A_7C15`.
  - Inputs come from a documented byte pattern.
  - Generate the expected values with the upstream C library, for example through the Python `xxhash` package, and record the tool version with the vectors.

## Acceptance criteria

- [ ] The vectors pass on every CI target, including s390x, i686, and wasm.
- [ ] A compile test or doc test shows `Ring<Box<dyn StableHasher>>` compiles. Until FR-007 lands, use a stand-in generic function.
- [ ] No `std::hash` trait or type is used for placement (clippy `disallowed-types` enforces this).
- [ ] Every public item is documented, and the examples use `?`.

## Out of scope

- A keyed SipHash hasher (backlog).
