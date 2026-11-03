---
id: FR-048
title: A threat model in SECURITY.md and verifiable release artifacts
status: done
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
- **Signed releases:** the release workflow signs `SHA256SUMS` with `cosign sign-blob` keyless (the workflow's OIDC identity) and publishes a single `SHA256SUMS.sigstore.json` bundle (signature, certificate, and transparency-log proof) next to it; GitHub artifact attestations for every binary and the container image. `install.sh` verifies the signature when `cosign` is present and says how to install it otherwise. The README's install section shows `cosign verify-blob` and `gh attestation verify` with the expected identity.
- **Pinned actions:** every third-party action in the workflows pinned to a commit SHA.
- **Not in scope:** an external protocol review (tracked on the roadmap under E3), encryption at rest, a bug bounty.

## Definition of done

- [x] `SECURITY.md` has the threat model table and a maintained "what a compromised X can do" section, reviewed against FR-046 and FR-047's tests.
- [x] A release's `SHA256SUMS` has a verifiable keyless signature, every binary and the image have attestations, and the documented verify commands succeed against the published artifacts and fail against a modified one.
- [x] All workflow actions are pinned by commit SHA, and the release workflow fails on an unresolved advisory.

## Progress

- 2026-11-02: The supply-chain items are done; the threat model is pending FR-047.
  - **Pinned actions:** every `uses:` in `.github/workflows/` names a 40-character commit with the release tag in a comment, and Dependabot (`.github/dependabot.yml`, weekly) bumps them. `dtolnay/rust-toolchain` is pinned to `master`, and each use passes `toolchain:` explicitly, since the branch name used to select it.
  - **Audit:** `.github/workflows/audit.yml` runs `cargo audit` daily against a fresh advisory database, and the release job runs `cargo audit --deny warnings` before it publishes. `.cargo/audit.toml` mirrors `deny.toml`'s ignore list, which is empty.
  - **Signing:** the release job signs `SHA256SUMS` with keyless `cosign sign-blob` (cosign v3, which writes only the `SHA256SUMS.sigstore.json` bundle; it holds the signature, certificate, and transparency-log proof) and attests the archives and `install.sh`; the container workflow attests each per-architecture image and the multi-arch manifest. `scripts/install.sh` verifies the signature when `cosign` is on `PATH`. The README's "Verify a release" section and the release notes show `cosign verify-blob` and `gh attestation verify`. The certificate identity matches `fleetringd-v*` tags, the tags that trigger the workflow.
  - **Not yet verified:** signing and attestation first run on the next `fleetringd-v*` tag. The Definition-of-done check that the verify commands succeed against published artifacts and fail against a modified one waits for that release.
  - **Pending:** the `SECURITY.md` threat model and "what a compromised X can do" section, which wait for FR-047's scopes and tokens.
- 2026-11-03: The threat model is in `SECURITY.md`.
  - **Contents:** the assets; one table with, for each actor (network attacker, compromised client, compromised data node, compromised control node, lost or leaked authority key, malicious operator, compromised CI or release pipeline), what the design prevents, what it detects, and what it doesn't cover; which properties rest on the authority key and how to rotate it and revoke its tokens; links to the scope table and the release verification steps.
  - **Reviewed against the code and tests:** `auth::policy` (`RPCS`, `ForNodes`), `auth::boundary` (token resolution, forwarding, stale denylists, stream guards, audit lines), `tls` (TLS 1.3 only, cluster name check, reload), the SQLite adapter's confinement, `replication::follower` (node calls check only that the caller is a node), and `tests/tls.rs`, `tests/scopes.rs`, and `tests/control.rs::a_node_certificate_acts_on_its_own_node_only`. Node-to-node calls not bound to placement are marked planned: FR-050.
  - **Supply chain:** checked the third criterion: every `uses:` in `.github/workflows/` names a 40-character commit, and `release.yml` runs `cargo audit --deny warnings`.
  - **Left:** the second criterion's check that the verify commands succeed against published artifacts and fail against a modified one, which waits for the first `fleetringd-v*` release.
- 2026-11-03: Verified against the published `fleetringd` 0.2.0 release; the ticket is done.
  - **Succeeded:**
    - `cosign verify-blob` on `SHA256SUMS` with its `.sigstore.json` bundle, with the README's certificate identity and issuer, printed `Verified OK`.
    - The `aarch64-apple-darwin` archive matched `SHA256SUMS`.
    - `gh attestation verify` succeeded for that archive and for `oci://ghcr.io/fleetring/fleetringd:0.2.0`.
  - **Failed, as they should:** a copy of `SHA256SUMS` with one byte added failed `cosign verify-blob`, and an archive with one byte appended failed `gh attestation verify`.
  - **`RELEASING.md`** now lists these commands as the post-release checklist, so every release repeats the check.
