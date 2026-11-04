---
id: FR-054
title: "`fleetringd upgrade` and a tested rolling upgrade"
status: in review
priority: P1
size: L
milestone: M7 Durability and operations
depends_on: [FR-038, FR-048]
adrs: [ADR-0017, ADR-0018, ADR-0020, ADR-0035, ADR-0038]
---

# FR-054: `fleetringd upgrade` and a tested rolling upgrade

## Context

Upgrading a node meant re-running `install.sh` and restarting by hand, with nothing checking that the new release can run beside the rest of the fleet. The roadmap's "Recoverable" criterion asks for a tested rolling upgrade from the previous release. ADR-0038 decides the compatibility policy, how nodes learn each other's releases, how new control commands are gated, and how `upgrade` verifies, swaps, and restarts.

## Plan

1. **ADR-0038** (accepted): two adjacent release lines per fleet; `Software` on `Join`, `Heartbeat`, and membership updates, with reportless members counted as 0.3.0; fleet features enabled on first use once every member supports them; cosign-only verification; atomic swap with `fleetringd.previous`; `SIGHUP` restarts in place; the rolling test nightly and in the release pre-flight.
2. **Control state** (`fleetring-control`): `Release`, `Software`, `Feature`; `Member::software`; `Action::Heartbeat.software`; `Action::Undrain`, the first feature; `State::features` and `State::check_features`; the `IncompatibleVersion`, `MissingFeature`, and `FeatureUnsupported` rejections. The command generator covers them, so the replay and property tests do too.
3. **The daemon**: protocol 1.1 (`Software`, `Control.Undrain`); nodes report their software; the control node records it, corrects it from heartbeats, and refuses gated commands before logging them; `node undrain`; `status` prints versions.
4. **`fleetringd upgrade`** (`fleetringd::upgrade`): release lookup and download, cosign and checksum verification, extraction, the swap and rollback, the process file, and the drain, restart, rejoin, and catch-up; `SIGHUP` and `Daemon::run_until_stop`.
5. **Tests**: fast ones on every pull request against a local release server and a stand-in cosign; the rolling upgrade `#[ignore]`d, run by `.github/workflows/rolling-upgrade.yml`.
6. **Docs**: the daemon README's "Upgrading", `RELEASING.md`'s pre-flight, `docs/cli.md`, the lexicon, and the roadmap.

## Progress

- 2026-11-04: ADR-0038 accepted; implemented on the `upgrade` branch, in review.
  - **Code:** `crates/fleetring-control/src/software.rs`; the gate in `apply` and `State::check_features`; `Member::software` and `Member::running`; `crates/fleetringd/src/software.rs`; the writer's screening in `control/node.rs` (`SubmitError::Unsupported`); `ControlService::join`, `heartbeat`, `undrain`, and `membership_update`; `auth::RPCS`'s `Control.Undrain`; `crates/fleetringd/src/upgrade/` (`release`, `verify`, `install`, `process`); `Stop` and `Daemon::run_until_stop`; `MembershipTasks::stop_without_leaving`; the CLI's `upgrade`, `node undrain`, `StopSignals`, and the restart in place.
  - **Acceptance criteria:**
    - `upgrade --check` reports and changes nothing: `the_fleet_window_is_checked_before_anything_is_downloaded` (`tests/upgrade/main.rs`).
    - Refusals: `a_release_that_does_not_verify_is_refused_and_nothing_changes` (bad signature, wrong identity, bad checksum), `without_a_new_enough_cosign_nothing_is_downloaded_or_installed`, `a_binary_that_is_another_release_is_refused`, and the policy refusal in `the_fleet_window_is_checked_before_anything_is_downloaded`.
    - Drain, restart, rejoin with no write failing beyond one retry: `upgrading_a_running_node_drains_restarts_and_rejoins_it`.
    - `--rollback`: `an_upgrade_verifies_swaps_atomically_and_rolls_back`, and `replacing_keeps_the_previous_binary_and_rollback_swaps_them` (`upgrade/install.rs`).
    - The rolling upgrade under writes with a history check: `rolling::a_fleet_rolls_from_the_previous_release_to_this_build_under_writes` (ignored; nightly).
    - A mixed fleet refuses an unsupported command: `a_mixed_fleet_refuses_a_command_a_member_does_not_support` (`tests/it/software.rs` and `tests/control.rs`), and the mixed phase of the rolling test.
  - **Deviations:**
    - **cosign only.** The `sigstore` crate's verification pulls in 355 crates, `aws-lc-sys` among them, and calls itself experimental, so `upgrade` refuses without cosign 2.4.2 or newer (ADR-0038, point 6).
    - **A first fleet feature, `undrain`,** was added so the gate has a real command to gate; it also lets an operator cancel a drain, such as one an abandoned upgrade left.
    - **0.3.0 nodes can't restart in place**: they have no process file and no `SIGHUP` handler, so the first rolling upgrade from 0.3.0 uses the README's manual steps, which the rolling test follows for a baseline without `upgrade`; it then reinstalls one node with this build's `upgrade` to cover the restart in place.
    - **Windows:** `upgrade --check` works; installing refuses, as `install.sh` does.
    - **`SIGHUP` drains but stays a member.** ADR-0038's first draft had it stop exactly as `SIGTERM` does; leaving and rejoining changes the ring twice for a node that comes straight back, so a restart in place stays (`Stop::Restart`, `MembershipTasks::stop_without_leaving`).
    - **Clean refusals during a leave and rejoin.** The manual path from 0.3.0 stops each node with `SIGTERM`, which leaves the fleet, and starts it again, which joins it. While the ring changes, the primary sometimes refuses a write with `too few replicas are reachable: 1 of the 2 a write needs; nothing committed` (2 of 20 rolling runs under load before the writer resent these). Such a refusal commits nothing and is documented as safe to retry, so the tests' writer resends it without counting it against the write's one retry, and counts it separately; an ambiguous or hand-over failure still gets one retry. The restart in place, which doesn't leave, didn't produce one in 100 runs. Follow-up: whether a membership change should cost a primary its followers' streams at all. The resends are bounded: a write may be refused at most `MAX_REFUSALS_IN_A_ROW` (30, three seconds or more) times in a row; twelve rolling runs under load needed at most one.
- 2026-11-04, after an independent review (ADR-0038's dated amendment):
  - **Features gate upgrades too.** Membership updates carry the enabled fleet features (`MembershipUpdate.features`, `MembershipView::features`); `upgrade --software` prints what a binary supports; `--check`, `--version`, and `--rollback` refuse a release whose binary lacks an enabled feature before any swap, on a data node and on a control-only node, which now restarts in place without a drain. Test: `a_release_without_an_enabled_feature_is_refused_before_it_is_installed`.
  - **Software no longer grows the membership epoch**, so a control node rolled back to 0.3.0, which decodes the log without it, replays to the same epoch; the control node sends an update when only software changes, and data nodes keep their ring then. Tests: `a_log_replayed_without_software_reaches_the_same_membership`, `a_new_release_is_recorded_without_changing_the_membership_epoch`.
  - **`fleetringd node remove`** takes out a member that is gone for good, which otherwise holds the window and the gate back. Test: `removing_a_member_that_is_gone_unblocks_the_fleet`.
  - **The restart signal is sent in process** (rustix), and the process file is read again just before it.
  - **The README** says a closing terminal's `SIGHUP` restarts the daemon.
  - **The proptest regression entry** in `properties.txt` is this branch's: a member whose heartbeat recorded software a join would refuse could never rejoin; a rejoin on the recorded software is now never refused, and the case passes.
