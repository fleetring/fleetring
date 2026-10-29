---
id: FR-048
title: A threat model in SECURITY.md and verifiable release artifacts
status: todo
priority: P1
size: M
milestone: M10 Security
depends_on: [FR-034, FR-046, FR-047]
adrs: [ADR-0025]
---

# FR-048: A threat model in SECURITY.md and verifiable release artifacts

## Context

`SECURITY.md` says how to report a vulnerability and that a panic on external input is a bug. It doesn't say what the system defends against. Once FR-046 and FR-047 land, the answer is precise enough to write down: what a compromised client, a compromised node, and an attacker on the network can and cannot do, and which properties rest on the authority's key. Release artifacts are checksummed (FR-034) but not signed, so an operator can't tell a binary built by this repository's workflow from one that merely matches a checksum file published next to it. Keyless Sigstore signing from GitHub Actions and GitHub's build attestations are the current standard for that, and they need no long-lived signing key in the repository.

## Scope

- **Threat model** in `SECURITY.md`: assets (database contents, the replication log, the control log, the authority key, tokens), actors (network attacker, compromised client with scopes, compromised node, lost authority key, malicious operator), and for each actor what the design prevents, what it detects (hash chain, `db verify`, audit lines), and what it doesn't cover (a compromised node can read every database it replicates; encryption at rest is out of scope and left to the disk). One table, kept current by every security ticket.
- **Dependency audit** already runs `cargo deny` advisories; add `cargo audit`'s database refresh on the nightly job and fail the release workflow on an unresolved advisory.
- **Signed releases:** the release workflow signs `SHA256SUMS` with `cosign sign-blob` keyless (the workflow's OIDC identity) and publishes the signature and certificate next to it; GitHub artifact attestations for every binary and the container image. `install.sh` verifies the signature when `cosign` is present and says how to install it otherwise. The README's install section shows `cosign verify-blob` and `gh attestation verify` with the expected identity.
- **Pinned actions:** every third-party action in the workflows pinned to a commit SHA.
- **Not in scope:** an external protocol review (tracked on the roadmap under E3), encryption at rest, a bug bounty.

## Definition of done

- [ ] `SECURITY.md` has the threat model table and a maintained "what a compromised X can do" section, reviewed against FR-046 and FR-047's tests.
- [ ] A release's `SHA256SUMS` has a verifiable keyless signature, every binary and the image have attestations, and the documented verify commands succeed against the published artifacts and fail against a modified one.
- [ ] All workflow actions are pinned by commit SHA, and the release workflow fails on an unresolved advisory.
