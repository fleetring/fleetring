---
id: FR-010
title: Weighted virtual-node ring strategy
status: done
priority: P1
size: M
milestone: M2 Placement core
depends_on: [FR-009]
adrs: [ADR-0004]
---

# FR-010: Weighted virtual-node ring strategy

## Goal

Ship the optional O(log T) strategy for very large fleets (ADR-0004). It must meet the same candidate-order contract as rendezvous, so FR-009 works on it unchanged.

## Scope

- **`Strategy::Vnodes(VnodeParams)`:** `VnodeParams` is `#[non_exhaustive]`, holds `tokens_per_weight: NonZeroU32` (default 256), and provides a constructor and accessor.
- **Tokens**
  - For `j` in `0..weight × tokens_per_weight`, `token = H(le64(node_hash) ‖ le32(j), seed)`.
  - Sort by `(token, NodeId)`.
  - Store as struct-of-arrays: `tokens: Vec<u64>` and `owners: Vec<u32>`.
- **Build limits**
  - Total tokens may not exceed 2²². Exceeding it returns `BuildRingError::TooManyTokens`.
  - Compute `weight × tokens_per_weight` with overflow checks.
- **Candidate order:** binary-search for the first token ≥ `key_hash` (wrapping to index 0), then walk clockwise, yielding each node at its first occurrence.
- **Spec integration:** if FR-013 has already landed, add the vnode parameters to `StrategySpec` and the canonical encoding.

## Acceptance criteria

- [x] The insertion, removal, and reweight properties hold, using the same proptest suite as FR-008, parameterized by strategy.
- [x] Every FR-009 property passes with `Strategy::Vnodes`.
- [x] **Balance**
  - Exact arc ownership (computed from tokens) matches empirical primary shares within sampling error.
  - With equal weights and 256 tokens per weight, the maximum relative deviation on the named fixtures stays under 5/√256.
- [x] Unit tests cover wrap-around and token ties, using the `test-util` hasher.
- [x] Building 1000 nodes × 256 tokens takes under 100 ms in release. Record the number in FR-016.

## Notes

- The docs must say that vnode weights should be small relative units, because the token count scales with weight. Rendezvous has no such limit.

## Progress

- 2026-10-27: Strategy implemented and wired in:
  - `Strategy::Vnodes(VnodeParams)`, with a default of 256 tokens per unit of weight;
  - a `TooManyTokens` check at exactly 2²², which saturates instead of overflowing;
  - a crate-private `StrategyIndex` that holds the token table, so lookups dispatch without an `expect`;
  - `VnodeWalk`, which implements the shared `CandidateOrder`.
- Verification:
  - Every shared ADR-0006 property passes at 20k cases, and the public-API proptests run for both strategies.
  - A unit test shows primaries match each node's exact arc ownership within 5σ.
  - The worst node's deviation with 12 equal nodes at 256 tokens is under 5/√256.
  - A 1000 × 256 build takes about 7 ms in release, against a 100 ms budget.
- Still open: adding the vnode parameters to `StrategySpec` and the canonical encoding. That happens when FR-013 merges.
- 2026-10-28: Spec integration is done in the FR-013 merge: `StrategySpec::Vnodes { tokens_per_weight }`, with canonical tag 1 followed by a u32. The spec, bounded-load, and placement tests all run with both strategies.
