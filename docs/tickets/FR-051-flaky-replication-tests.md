---
id: FR-051
title: Find and fix the replication tests that fail intermittently in CI
status: in-progress
priority: P1
size: M
milestone: M7 Durability and operations
depends_on: []
adrs: [ADR-0020, ADR-0022, ADR-0026]
---

# FR-051: Find and fix the replication tests that fail intermittently in CI

## Goal

Tests that fail once in CI and pass on a rerun have each been a hidden cause, and `main`'s CI was red about half the time. A rerun hides the cause, and a test that fails at random on one platform is either a race in the test or a real timing bug in the daemon. Each one is either fixed or shown to be a test-only race, so CI is green on the first run.

## Known failures

The ticket was opened for the first three entries; later CI runs added the other three.

- **`tests/replication/protocol.rs::followers_refuse_older_epochs_and_batches_that_skip_ahead`** (ubuntu-latest, PR #33): the test expected `FailedPrecondition` and got `Aborted: "a newer stream replaced this one"`. Two streams race to the follower, and the test assumes an order.
- **Two Windows replication tests** (run 41575487780 on `main`, PR #31, and run 41610667386 on PR #43): a follower missed a 10-second acknowledgement.
- **`tests/database.rs::draining_a_node_moves_its_primaries_while_a_client_keeps_writing`** (macos-latest, run 41562990938 on PR #29, and ubuntu-latest in run 41610667386): "1 of the 1 followers a write needs did not acknowledge it within 20s" during a drain.
- **`tests/scopes.rs::scopes_hold_through_certificates_and_tokens`** (windows-latest, run 41610667386, twice): the revoked token's `WatchLeases` stream ended `Unavailable` "this node's token denylist is stale; retry" instead of `Unauthenticated` "the token is revoked".
- **`tests/scopes.rs::the_command_line_acts_as_its_certificate_or_its_token`** (ubuntu-latest, run 41609384019 on `main`): `token revoke: ... error: unexpected argument '-y' found`.
- **`tests/control.rs::the_control_node_starts_a_promotion_for_an_expired_lease_and_a_node_leaves`** (windows-latest, run 41599273075 on `main`): `epoch 2 of database 'tenant:acme' is stale; the current epoch is 3`.

## Scope

- For each test, reproduce the failure locally under load: run it in a loop, with `--test-threads` raised, and on the platform it failed on where possible. Record the failure rate before and after the fix.
- Decide whether each one is a test race or a daemon bug. A daemon bug gets a fix and a simulation scenario that covers it (ADR-0020). A test race gets a fix that waits on the condition the test means, not a longer timeout.
- A longer timeout alone is not a fix, unless the ticket records why the old bound was wrong.

## Acceptance criteria

- [x] Each is either fixed with its cause stated in the commit, or shown to be a test-only race and fixed in the test.
- [x] Each test passes 200 runs in a row locally (macOS; the Windows runs can't be reproduced here).
- [ ] None of them needs a rerun across ten consecutive CI runs on `main`.

## Progress

Measured on macOS (10 cores, shared with other jobs), running the debug test binary directly: some number of runs, a number at once, and a number of CPU hogs (`yes > /dev/null`). The 200-run "after" figures are all 8 at once with 2 hogs. "Before" is `origin/main`'s binary; "after" is this branch. Most of the Windows failures don't reproduce on macOS at the CI's rate, so for those the table also gives an induced run that widens the window the race lives in, and says so.

| Test | Cause | Fix | Before | After |
|---|---|---|---|---|
| `scopes_hold_through_certificates_and_tokens` | Daemon bound too tight (see below) | `fix(fleetringd): bound a stale token denylist by the heartbeat timeout` | 40/40 under load (6 hogs); 0/20 when the process is frozen 0.5 s every 0.8 s (the stale message above) | 200/200 (8 at once, 2 hogs); 20/20 under the same freezes |
| `the_command_line_acts_as_its_certificate_or_its_token` | Daemon (CLI) bug: about 1 token ID in 64 starts with `-`, which clap read as a flag | `fix(fleetringd): accept token and key IDs that start with a hyphen` | `token revoke --token-id -yabc` fails every time; 1 run in 64 by chance | 200/200, plus a parse test on a `-` ID |
| `the_control_node_starts_a_promotion_for_an_expired_lease_and_a_node_leaves` | Test race: the control node supersedes a promotion that stalls for a lease duration | `test(fleetringd): drive a promotion through supersession in the control test` | 60/60 at the 2 s lease; with the lease cut to 250 ms to make stalls likely, 2/40, 27 of the failures the CI message | 200/200; 40/40 at the 250 ms lease |
| `followers_refuse_older_epochs_and_batches_that_skip_ahead` | Test race: the primary's stream reopens and replaces the test's | `test(fleetringd): let the follower judge the stream in the protocol test` | 100/100 at the 50 ms retry; with the retry cut to 1 ms, 58/60, the failures the CI message | 200/200; 60/60 at the 1 ms retry |
| `mirror::caught_up_followers_are_byte_identical_...`, `snapshot::a_follower_added_during_writes_...` | Test bound: a 10 s acknowledgement timeout no test meant to assert; the snapshot test also hit the drain bug below when it re-served its primary | `test(fleetringd): don't assert a write latency in the replication tests` | mirror 24/24 (12 at once), snapshot 24/24 (8 at once, 6 hogs): not reproduced | mirror 200/200, snapshot 200/200 |
| `draining_a_node_moves_its_primaries_while_a_client_keeps_writing` | Daemon bug: a write waiting for its quorum was never woken when its primary stepped down | `fix(fleetringd): fail writes waiting on a quorum when the primary steps down` | 53/60 (8 at once, 6 hogs): 7 failures, all the CI message | 59/60 under the same load (the one failure was `node 'data-2' is down`, see below); 200/200 at 8 at once, 2 hogs |

### Causes

- **Stale denylist (daemon bound, changed).** A data node refused tokens when its denylist was three heartbeat intervals old. The tests use a 100 ms interval, so 300 ms, which a heartbeat reply delayed by a loaded runner exceeds; a node that stalls for 3 s does the same in production, where it should not refuse every token. The bound is now the control plane's heartbeat timeout (5 s by default), or three intervals if that is longer; ADR-0026 has an amendment with the reasoning and the cost (a partitioned node may honor a revoked token for 5 s, not 3 s). It still fails closed. The scopes test sets a 1.5 s timeout so it waits for the stale state, and checks that a revocation arrives before it.
- **Token ID with a leading hyphen.** `token issue` prints 16 random bytes as base64url, so one ID in 64 begins with `-`, which `token revoke --token-id` refused as an unknown flag. Not a test race: that token could not be revoked from the command line.
- **Promotion supersession.** The control node replaces a promotion that has stalled for `lease_duration + failover_margin` with one at the next epoch. The test's daemon shares its thread, and each call is fsynced, so on a slow runner the steps outrun 2 s and the next call is correctly refused as stale. The steps now run as one attempt that restarts at the newer epoch.
- **Replaced stream.** A follower hands its slot to the newest stream, so the test's stream ends the primary's, which reopens 50 ms later and takes the slot back. If that beats the follower reading the test's second message, the test's stream ends `Aborted`. The test resends until the follower judges a stream.
- **Writes waiting on a primary that is gone (daemon bug).** A write waits for W-1 followers on a task that holds an `Arc` to its `Primary`, so dropping the primary (a drain's step down, a newer epoch, `serve_primary` replacing it) never woke it, and the aborted streams could not acknowledge. The write sat until `ack_timeout` and then failed. In a drain, a write accepted just before the node stops taking writes publishes after the tail check and hits this. A `Primary` now has a retired flag, set when it is dropped, and waiting writes fail at once with `ABORTED` (clients already retry it). Covered by `tests/replication/quorum.rs::a_write_waiting_for_its_quorum_fails_when_its_primary_steps_down` and by the simulation scenario `tests/sim/drain.rs::a_drain_releases_writes_waiting_for_acknowledgements` (the busiest node is cut off from both followers while the writer runs, then drained; no write may still be waiting once its databases move). Both fail without the fix.
- **Windows acknowledgement timeouts.** The replication binary took 96 s on that runner, and the tests' `settings()` set `ack_timeout` to 10 s while its own comment says nothing there relies on a timeout, so a slow fsync became a failure, and the snapshot test (which retries refused writes) spent 10 s on each. The bound was a latency assertion nobody meant to make; it is now the hang guard, like `stall_timeout`. I could not reproduce these on macOS, so this one rests on the logs: both failed within 3 s of each other in a run where that binary took 96 s, and neither failed in 24 runs of the mirror test at 12 at once. The snapshot test also re-serves its primary mid-write, which the drain fix above now ends at once.

### Clients and `UNAVAILABLE`

ADR-0026 says clients must treat `UNAVAILABLE` from a data node as retryable. The control client does (`transport::retry` covers `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` for its idempotent calls). The data-plane `Client` and the command line do not retry database calls at all, by design (a lost reply leaves a write's outcome unknown), so a stale node's `UNAVAILABLE` reaches the caller of `db exec` and `db query`. A refusal by the token check happens before any statement runs, so it is safe to retry; a follow-up could retry reads, or refusals the node marks as "nothing ran". Not done here.

### Not reproduced

- Windows runs of the mirror and snapshot tests (above).
- Under the heaviest load tried (8 at once, 6 hogs, load average above 30) one drain run failed for another reason: `node 'data-2' is down` from `drain_node`, the node having missed its 5 s heartbeat timeout. That is the load, not the test; it did not recur at the lighter load of the 200-run check.

### Still to do

- Ten consecutive green CI runs on `main` after the merge (the last acceptance criterion).
