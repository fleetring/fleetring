---
id: FR-013
title: RingSpec, canonical encoding, fingerprint, and serde support
status: todo
priority: P0
size: M
milestone: M3 Change and interop
depends_on: [FR-009]
adrs: [ADR-0008]
---

# FR-013: `RingSpec`, canonical encoding, fingerprint, and serde support

## Goal

Make rings portable. `fleetringd` stores them in the control db and ships them to routers and clients, and peers prove they run the same placement function by comparing fingerprints (ADR-0008).

## Scope

- **Spec types:** `RingSpec`, `NodeSpec`, and `StrategySpec`, all `#[non_exhaustive]`. `RingSpec` holds `FORMAT_VERSION = 1`, `epoch`, `hasher` (the algorithm name), `seed`, `strategy`, `replication_factor`, `nodes`, and `fingerprint`.
- **Conversions:** `Ring::to_spec()` and `Ring::from_spec(&RingSpec, hasher: H) -> Result<Ring<H>, FromSpecError>`.
- **`FromSpecError`** is `#[non_exhaustive]` and has these variants:
  - `UnsupportedFormat { found }`
  - `HasherMismatch { spec, hasher }`
  - `FingerprintMismatch { stored, computed }`
  - `InvalidRing(BuildRingError)`, which exposes the build error through `source()`
- **`Fingerprint(u64)`:** `Display`/`FromStr` use 16 lowercase hex digits. `Ring::fingerprint()` computes it once at build time and caches it.
- **Canonical binary encoding** (crate-private; specified in FR-014): versioned, length-prefixed, little-endian, with nodes in canonical order and the epoch left out.
- **Probe set:** 64 fixed keys defined in the spec. Their replica sets, written as indices in canonical node order, are appended to the fingerprint input.
- **`serde` feature**
  - Derive serde on the spec types.
  - Labels and weights deserialize through `TryFrom`, so serde can't build an invalid value.
  - Unknown fields are ignored; an unknown `format` is rejected.

## Acceptance criteria

- [ ] **Round trip:** `Ring::from_spec(&ring.to_spec(), h)` gives the same placements over a corpus and the same fingerprint, for every strategy that exists.
- [ ] Changing any field that affects placement changes the fingerprint: seed, a weight, a zone, a host, R, strategy parameters, or the hasher. Changing only the epoch does not (proptest).
- [ ] A spec whose stored fingerprint doesn't match is rejected with `FingerprintMismatch`. So is a correct spec loaded with a different seed.
- [ ] `tests/fixtures/ring-spec-v1.json` deserializes, and re-serializing the same ring reproduces it byte for byte. This locks field names and field order.
- [ ] Invalid labels or a zero weight in JSON produce a clear error, never a panic.

## Notes

- This ticket is P0 because `fleetringd` needs specs from day one. If FR-010 hasn't landed yet, it adds the vnode parameters later.
