---
id: FR-019
title: Release fleetring 0.1.0 and freeze placement spec v1
status: in-progress
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

- [ ] Every P0 and P1 ticket is done.
- [ ] The maintainer runs `cargo publish -p fleetring`, and it succeeds.
- [ ] The docs.rs build succeeds with all features.
- [ ] The semver-checks job runs on PRs.

## Progress

- 2026-10-28: `RELEASING.md` holds the full checklist: pre-flight, spec freeze, version and changelog, package verification, tag and publish, and turning on `cargo semver-checks` once 0.1.0 is the baseline.
- Remaining:
  - FR-020 (cells ship in 0.1.0);
  - green CI on GitHub (FR-002 needs a push);
  - the version bump, changelog entry, and spec freeze;
  - the publish, which the maintainer does.
