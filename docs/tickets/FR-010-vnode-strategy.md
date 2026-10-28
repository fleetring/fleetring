---
id: FR-010
title: Weighted virtual-node ring strategy
status: todo
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

- [ ] The insertion, removal, and reweight properties hold, using the same proptest suite as FR-008, parameterized by strategy.
- [ ] Every FR-009 property passes with `Strategy::Vnodes`.
- [ ] **Balance**
  - Exact arc ownership (computed from tokens) matches empirical primary shares within sampling error.
  - With equal weights and 256 tokens per weight, the maximum relative deviation on the named fixtures stays under 5/√256.
- [ ] Unit tests cover wrap-around and token ties, using the `test-util` hasher.
- [ ] Building 1000 nodes × 256 tokens takes under 100 ms in release. Record the number in FR-016.

## Notes

- The docs must say that vnode weights should be small relative units, because the token count scales with weight. Rendezvous has no such limit.
