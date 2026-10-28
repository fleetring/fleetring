# ADR-0008: Versioned placement spec, golden vectors, and ring fingerprints

- **Status:** Accepted (2026-10-27)
- **Date:** 2026-10-27
- **Related:** ADR-0002, ADR-0003, ADR-0004, ADR-0006, ADR-0007; tickets FR-002, FR-006, FR-013, FR-014

## Context

Placement output is effectively a wire format. Suppose `fleetring` 0.3 on a router places `agent:42` differently from `fleetring` 0.2 on a node, or the TypeScript SDK disagrees with the Rust client. Writes then go to the wrong primary. Crate semver can't express this: a patch release that "improves" tie-breaking breaks every running cluster.

Rings also travel. The daemon stores them in the control db and ships them to routers and clients. Peers need a cheap way to confirm they're running the same placement function.

## Decision

1. **Placement spec.** `docs/spec/placement-v1.md` is the normative definition of placement v1. It covers:
   - byte encodings and how hash inputs are composed;
   - the fixed-point logarithm, comparisons, and tie-breaks;
   - vnode tokens and candidate orders;
   - the diversity passes and the bounded-load capacity formula;
   - the canonical spec encoding and the fingerprint.

   Any change that alters which nodes a key maps to, for the same ring spec, needs a new spec version, whatever the crate version.

2. **Golden vectors.** `crates/fleetring/tests/vectors/placement-v1.json` holds test cases. Each case is a ring spec, a list of keys with their expected replica IDs in order, and the ring's fingerprint. The file also holds hasher and fixed-point-log vectors for port authors.
   - A tool generates the file once. After that it is **append-only**: new cases can be added, but existing cases never change.
   - CI runs the vectors on Linux, macOS, and Windows; on x86-64, aarch64, 32-bit (i686), and big-endian (s390x); and on wasm32-wasip1 (ADR-0009).
   - Every future port must pass the same file.

3. **`RingSpec`.** This is the serializable description of a ring. It records the format version, `Epoch`, hasher algorithm name, seed, strategy and its parameters, replication factor, and the nodes (id, weight, zone, host).
   - serde support sits behind the `serde` feature.
   - `Ring::to_spec()` produces a spec, and `Ring::from_spec(&spec, hasher)` builds a ring from one.
   - `from_spec` validates everything and rejects unknown format versions and hasher mismatches.
   - `from_spec` ignores unknown fields. That's safe because any field that affects placement also changes the fingerprint (point 4), so it can't diverge silently.

4. **Fingerprint.** The fingerprint is a 64-bit XXH3-64 digest (seed 0) computed over two things: the canonical binary encoding of the spec, minus the epoch, and then the placements of a fixed set of probe keys defined in the spec.
   - It identifies the *placement function*. A wrong hasher, seed, or strategy, or a buggy port, all show up as a mismatch.
   - It's stored in `RingSpec` and verified by `from_spec`.
   - Peers exchange `(epoch, fingerprint)`. Two peers with the same epoch but different fingerprints indicate a bug or misconfiguration, and the mismatch must fail loudly.

5. **Epoch.** An opaque `u64` assigned by the control plane and monotonic within a cluster. It lets peers spot stale rings and never affects placement.

6. **Freeze.** Spec v1 freezes at `fleetring` 0.1.0. Until then, in 0.0.x, it can change freely.

7. **Three test layers:**
   - **Golden vectors** check exact outputs.
   - **Property tests** (proptest) check invariants: determinism, independence from insertion order, distinctness, prefix consistency, and minimal churn.
   - **Statistical tests** check balance within a tolerance. They run on fixed key corpora, so they're deterministic and never flaky.

## Alternatives considered

- **"The Rust crate is the spec":** doesn't work for TypeScript and Python SDKs, and gives no way to check a port.
- **Crate semver alone:** mixes API compatibility up with placement compatibility.
- **Fingerprinting a serde encoding:** JSON and similar formats aren't canonical (field order, number formatting vary), so we use a small binary encoding specified by hand.
- **Rejecting unknown fields:** would stop the daemon from adding metadata that doesn't affect placement unless it bumped the version. The fingerprint already protects placement.

## Consequences

- Placement changes ship as spec v2 alongside v1. That covers a new algorithm, a tie-break fix, or a new default. Clusters opt in by rebuilding their ring, which is a full migration.
- Port authors get a self-contained conformance kit.
- The vector file and the spec need review discipline. CI fails if any existing case changes.
