# ADR-0002: Stable hashing behind a `StableHasher` trait, with XXH3-64 as the default

- **Status:** Accepted (2026-10-27)
- **Date:** 2026-10-27
- **Related:** ADR-0003, ADR-0008; tickets FR-001, FR-003

## Context

Every placement decision starts with a hash. If two processes hash the same key differently, they disagree on where a database lives. That's split-brain routing, and it's nearly impossible to debug. The hash must give identical results across:

- processes and restarts (so no per-process random seeds);
- machines and architectures: x86-64, aarch64, 32-bit, big-endian, and wasm32;
- Rust releases and `fleetring` releases;
- languages: the v0.2 TypeScript and Python SDKs, plus the WASM playground.

The standard library doesn't meet these requirements, and its documentation says so:

- `DefaultHasher`: "The internal algorithm is not specified, and so it and its hashes should not be relied upon over releases." Today it's SipHash-1-3.
- `RandomState` seeds each instance randomly, so two processes hash differently by design.
- The `Hash` trait isn't portable either, even when it feeds a stable hasher: "data fed by `Hash` to a `Hasher` should not be considered portable across platforms… the data passed by most standard library types should not be considered stable between compiler versions." Integers are fed in native byte order, `usize` width varies by platform, and slices and strings add length prefixes or terminators.

Placement keys are short, structured strings such as `agent:sam-researcher` and `tenant:000123`. The hash has to mix these well and run fast on short inputs. Its specification must be frozen, with implementations available in many languages.

## Decision

1. **Trait.** Placement hashing goes through a one-shot trait that takes bytes:

   ```rust
   /// A 64-bit hash function whose output never changes.
   pub trait StableHasher {
       /// Stable algorithm name, recorded in ring specs (for example `"xxh3-64"`).
       fn algorithm(&self) -> &'static str;

       /// Hashes `bytes` under `seed`. The result depends only on the hasher's
       /// configuration, `bytes`, and `seed`.
       fn hash(&self, bytes: &[u8], seed: u64) -> u64;
   }
   ```

   - **Dispatch.** The trait is object-safe: no generic methods and no associated consts. Bindings can use `dyn StableHasher`, while `Ring<H: StableHasher = Xxh3>` gets static dispatch on the hot path.
   - **Not `std::hash::Hasher`.** This is deliberate. A streaming `write`/`finish` interface invites feeding data through `Hash` impls, which aren't portable.
   - **Seed.** The seed belongs to the ring (`RingBuilder::seed`, default 0) and is recorded in the ring spec, so the built-in hashers are stateless unit structs.
   - **Secret keys.** A hasher that needs a secret key, such as a keyed SipHash for adversarial key spaces, holds the key itself and never writes it into the spec. The ring fingerprint (ADR-0008) catches mismatched keys.
   - **Open to users.** The trait is not sealed. Users can plug in their own hasher, for example to match a legacy system's placement. The docs state the contract: deterministic, portable, and pinned by the implementer's own test vectors.

2. **Default: XXH3-64.** The `Xxh3` hasher implements xxHash's `XXH3_64bits_withSeed`.
   - Its output format has been frozen since xxHash v0.8.0 (July 2020).
   - It passes SMHasher.
   - It has a fast path for inputs of 0–16 bytes. The per-node draws in ADR-0004 hash exactly 16 bytes.
   - It has maintained implementations in C, Rust, Go, Java, JavaScript/WASM, and Python.

3. **Implementation crate.** We use `twox-hash` 2.x (MIT, `no_std`) with `default-features = false, features = ["xxhash3_64"]`, which keeps `rand` out of the build. Our own reference vectors, cross-checked against the upstream C implementation, pin the output, so the crate can be swapped without changing placement.

4. **Explicit encodings.** Composite hash inputs are built from explicit bytes. Integers are fixed-width little-endian (`to_le_bytes`), never `usize`. Keys are hashed exactly as given: no Unicode normalization and no case folding. Canonicalizing names is the router's or daemon's job.

5. **Banned in placement code:**
   - `std::hash::{Hash, Hasher, BuildHasher}` for placement inputs;
   - `DefaultHasher` and `RandomState`;
   - anything with unspecified iteration order (`HashMap`, `HashSet`). Use `BTreeMap` or a sorted `Vec` instead.

   `disallowed-types` in `crates/fleetring/clippy.toml` enforces this.

6. **64-bit output** is plenty for placement. If two keys collide, the two databases share replicas, which is harmless.

## Alternatives considered

| Option | Verdict |
|---|---|
| `DefaultHasher` / `RandomState` | Rejected: the algorithm is unspecified and seeds are random. |
| XXH64 | Stable since 2014 and available everywhere, but slower than XXH3 on short inputs. Fallback if XXH3 ports ever disappoint. |
| wyhash / rapidhash | Very fast, but both have shipped several revisions with incompatible output, a poor fit for a format meant to stay stable for decades. |
| foldhash, ahash, FxHash | Built for in-memory tables and promise no stability across versions or platforms. ahash output depends on CPU features and FxHash on pointer width. Fine for internal maps, never for placement. |
| SipHash-1-3/2-4 with a fixed key | Stable and keyed, but several times slower on short inputs. A keyed variant may come later for adversarial key spaces. |
| MurmurHash3 | Stable and widespread, but slower than XXH3, with known seed-independent collisions. |
| BLAKE3 / SHA-256 | Overkill: far more short-input latency than placement needs. |
| FNV-1a | Simple, but avalanches poorly on keys that differ only in a trailing digit. |

## Consequences

- One small runtime dependency (`twox-hash`).
- **Placement is not a security boundary.** An attacker who can choose database names could steer many of them onto one node. The mitigations live in the daemon (per-tenant quotas, bounded loads), and a keyed hasher can be plugged in through the trait.
- Changing the default hasher, or how hash inputs are composed, changes placement and therefore requires a new placement spec version (ADR-0008).
