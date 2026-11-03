---
id: FR-054
title: "`fleetringd upgrade` and a tested rolling upgrade"
status: todo
priority: P1
size: L
milestone: M7 Durability and operations
depends_on: [FR-038, FR-048]
adrs: []
---

# FR-054: `fleetringd upgrade` and a tested rolling upgrade

## Goal

Upgrading a node today means re-running `install.sh` and restarting by hand, with no check that the new version can run alongside the rest of the cluster. The roadmap's "safe to use" bar asks for a tested rolling upgrade from the previous release. This ticket adds a verified self-upgrade and proves that a cluster can move from release N to N+1 one node at a time without losing an acknowledged write.

## Scope

- **An ADR first:** the version compatibility policy.
  - Which versions may run in one cluster at the same time. The proposal is the current and previous minor release.
  - How a node learns the others' versions. The proposal is that `Join` and `Heartbeat` carry the daemon version and the highest protocol and control-log format it speaks.
  - How a new control-log command or proto field is gated until every member supports it, as feature flags in control state.
  - What `fleetringd` refuses, and with which error.
- **`fleetringd upgrade [--version X.Y.Z] [--check] [--no-restart]`:**
  - Finds the latest release (or the given version) for its own target and downloads the archive, `SHA256SUMS` and `SHA256SUMS.sigstore.json`.
  - Verifies the signature against the release workflow's identity, then the checksum. It refuses on any failure.
    - Signature verification uses `cosign` when it's on `PATH`, at the same minimum version as `install.sh`.
    - Otherwise it verifies in-process with the `sigstore` crate, if its dependency cost and MSRV are acceptable. Record the choice in the ADR. Never install unverified.
  - Replaces the binary atomically and keeps the previous one as `fleetringd.previous`, for `fleetringd upgrade --rollback`.
  - On a running node, unless `--no-restart`: checks compatibility against the cluster's members (refuses a jump the policy forbids), drains the node's primaries (FR-038), restarts, and waits until it has rejoined and caught up. The restart works under systemd, the container image, and a plain process; document each.
  - `--check` prints the available version and whether the cluster allows it, and changes nothing.
- **A tested rolling upgrade:**
  - A test harness that starts a three-node cluster on the previous release's published binaries.
  - A client writes and reads throughout, while the harness upgrades each node in turn to the current build.
  - A history check over the whole run.
  - It runs in the release workflow's pre-flight, or nightly. It downloads the previous release's verified archive, not a local build.
- **Docs:** an "Upgrading" section in the daemon README (the policy, the command, rollback, and manual steps for operators who don't use the command), and the upgrade test added to `RELEASING.md`'s pre-flight.
- **Not in scope:** upgrading the control plane under Raft (E5), downgrades across a control-log format change, and automatic upgrades without an operator's command.

## Acceptance criteria

- [ ] `fleetringd upgrade --check` reports the latest release and the cluster's compatibility, and changes nothing.
- [ ] `fleetringd upgrade` refuses a release whose signature or checksum doesn't verify, and one the compatibility policy forbids, and says why.
- [ ] Upgrading a node drains it, restarts it, and rejoins it with no failed client write beyond one retry, as FR-038's drain allows.
- [ ] `fleetringd upgrade --rollback` restores the previous binary.
- [ ] The rolling-upgrade test moves a three-node cluster from the previous release to the current build under writes, and the history check finds no lost acknowledged write.
- [ ] A mixed-version cluster refuses a control command that a member doesn't support, instead of applying it on some nodes only.

## Progress

- Not started.
