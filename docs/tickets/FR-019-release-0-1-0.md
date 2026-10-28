---
id: FR-019
title: Release fleetring 0.1.0 and freeze placement spec v1
status: done
priority: P1
size: S
milestone: M4 Hardening and release
depends_on: [FR-014, FR-016, FR-018]
adrs: [ADR-0008, ADR-0009]
---

# FR-019: Release `fleetring` 0.1.0 and freeze placement spec v1

## Goal

Ship 0.1.0. From this release on, placement output for spec v1 never changes.

## Scope

- **Freeze spec v1:** set its status to `Final`. The FR-014 vector guard becomes strict: v1 cases are immutable from now on.
- **ADR review:** move each ADR from `Proposed` to `Accepted`, or supersede it.
- **Version and changelog:** bump the workspace version to 0.1.0 and add a CHANGELOG entry.
- **Package checks:** review `cargo package --list` and run `cargo publish --dry-run`.
- **Semver:** enable a `cargo semver-checks` CI job that compares future PRs against 0.1.0.
- **Tag and release:** tag `fleetring-v0.1.0` and publish GitHub release notes that link to the spec and the ADRs.

## Acceptance criteria

- [x] Every P0 and P1 ticket is done.
- [x] The maintainer runs `cargo publish -p fleetring`, and it succeeds.
- [x] The docs.rs build succeeds with all features.
- [x] The semver-checks job runs on PRs. *(It runs on every push and pull request, and passed on `5846aeb`.)*

## Progress

- 2026-10-28: `RELEASING.md` holds the full checklist: pre-flight, spec freeze, version and changelog, package verification, tag and publish, and turning on `cargo semver-checks` once 0.1.0 is the baseline.
- Remaining:
  - FR-020 (cells ship in 0.1.0);
  - green CI on GitHub (FR-002 needs a push);
  - the version bump, changelog entry, and spec freeze;
  - the publish, which the maintainer does.
- 2026-10-28: Release prepared.
  - Spec v1 is marked Final, the workspace version is 0.1.0, and `CHANGELOG.md` has the 0.1.0 entry.
  - CI has a `semver-checks` job that compares each change with the latest release on crates.io.
  - The cells benchmarks were re-measured on an idle machine: 2.19 µs (rendezvous) and 0.30 µs (vnodes) per lookup, against 87 µs for one flat 10,000-node ring.
  - `cargo package --list` is reviewed and `cargo publish --dry-run` passes.
  - ADR-0010 and ADR-0013 stay Proposed. They describe the replication layer and the daemon, not shipped crate behavior.
  - FR-021 (`CellsSpec`) ships after 0.1.0. Its JSON form is documented in rustdoc, like `RingSpec`'s, so it doesn't touch the frozen spec.
- Remaining: the maintainer tags, publishes, and creates the GitHub release; then check docs.rs and the first semver-checks run.
- 2026-10-28: Released. `fleetring` 0.1.0 is on crates.io, tagged `fleetring-v0.1.0` at `5846aeb`, with a [GitHub release](https://github.com/fleetring/fleetring/releases/tag/fleetring-v0.1.0). docs.rs built it with all features. Placement spec v1 is now frozen.
