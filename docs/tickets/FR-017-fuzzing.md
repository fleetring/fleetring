---
id: FR-017
title: Fuzzing for spec decoding, builder input, and lookup invariants
status: in-progress
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
- **CI:** a GitHub Action runs each target for 5 minutes and uploads any crash as an artifact. It runs nightly once the repository is public, and on demand until then.

## Acceptance criteria

- [x] Each target runs at least 10M executions locally with no findings, or each finding is fixed and has a regression test.
- [ ] The `Fuzz` workflow passes on GitHub (a manual run while the repository is private).
- [x] `fuzz/README.md` explains how to run and triage.

## Progress

- 2026-10-28: Three libFuzzer targets live in `fuzz/` (a workspace of its own, run with nightly and cargo-fuzz):
  - `spec_json`: untrusted JSON specs, checked by round-tripping through `from_spec` and comparing fingerprints;
  - `builder`: arbitrary memberships, checked against the replica-set invariants;
  - `lookup`: liveness filters and bounded-load capacity.
- Local runs (Apple M-series, coverage instrumented) found nothing:
  - `spec_json`: 30.7M executions;
  - `builder`: 11.3M;
  - `lookup`: 13.4M.
- The `Fuzz` workflow runs each target nightly for 5 minutes and on demand, uploading crash artifacts. It is waiting on its first GitHub run, which is the last open criterion.
- 2026-10-28: Turned the nightly schedule off while the repository is private. Nightly runs would use about 750 of the org's 2,000 free Actions minutes a month, and public repositories don't pay for minutes. The workflow runs on demand, and the schedule is commented out in `fuzz.yml` for when the repository goes public. When it does, consider caching `fuzz/corpus/` between runs, so each run continues from the last one instead of restarting from the seeds.
