# ADR-0014: Run the full feature and profile matrix on Linux only

- **Status:** Accepted (2026-10-28)
- **Date:** 2026-10-28
- **Amends:** ADR-0009, decision 6 (the `test` row)
- **Related:** ADR-0008; ticket FR-002

## Context

ADR-0009 runs the test suite on Linux, macOS, and Windows, each with default features, `--all-features`, and `--no-default-features`, in debug and `--release`. On GitHub's runners the Windows job takes more than 16 minutes, over the 15-minute budget in FR-002. Most of that time goes to building the same tests four times, every doctest included, and Windows links slowly.

Feature flags and the release profile change which code is compiled, not how it behaves on a given operating system. Cross-platform determinism is covered by the golden vectors, which every test job runs, and by the i686, s390x, and wasm32-wasip1 jobs.

## Decision

- Linux runs the full matrix: default features, `--all-features`, `--no-default-features`, and `--all-features --release`, plus every example.
- macOS and Windows run the suite once, with `--all-features` in debug.

## Alternatives considered

- **Raising the time budget.** Every pull request would wait on the slowest job, for coverage that only repeats Linux.
- **Skipping doctests on Windows.** It saves less, and doctests are the documentation users copy, so they should run on every OS.

## Consequences

- A full CI run fits in about 15 minutes again.
- A bug that appears for one feature combination on macOS or Windows but not on Linux would go unnoticed. The crate has no OS-specific code, so this is unlikely.
