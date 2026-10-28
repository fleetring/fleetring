---
id: FR-017
title: Fuzzing for spec decoding, builder input, and lookup invariants
status: todo
priority: P2
size: S
milestone: M4 Hardening and release
depends_on: [FR-013]
adrs: [ADR-0008, ADR-0009]
---

# FR-017: Fuzzing for spec decoding, builder input, and lookup invariants

## Goal

No input that arrives from the network or the control db can cause a panic.

## Scope

- **Setup:** a `fuzz/` directory set up with cargo-fuzz, using `arbitrary`. It has its own workspace, outside the main one.
- **Targets**
  - `spec_json`: arbitrary bytes → `serde_json` → `Ring::from_spec`.
  - `builder`: arbitrary nodes, labels, weights, R, and strategy → `build()`. If the build succeeds, check the FR-009 invariants on arbitrary keys.
  - `lookup`: an arbitrary ring, key, liveness bitmap, and bounded-load view → check the preference, replica, and bounded-placement invariants.
- **Seed corpus:** the golden vectors (FR-014) and the JSON fixtures (FR-013).
- **CI:** a scheduled nightly GitHub Action runs each target for 5 minutes and uploads any crash as an artifact.

## Acceptance criteria

- [ ] Each target runs at least 10M executions locally with no findings, or each finding is fixed and has a regression test.
- [ ] The nightly workflow passes.
- [ ] `fuzz/README.md` explains how to run and triage.
