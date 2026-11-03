---
id: FR-051
title: Find and fix the replication tests that fail intermittently in CI
status: todo
priority: P1
size: M
milestone: M7 Durability and operations
depends_on: []
adrs: [ADR-0020, ADR-0022]
---

# FR-051: Find and fix the replication tests that fail intermittently in CI

## Goal

Three replication tests have each failed once in CI and passed on a rerun. A rerun hides the cause, and a test that fails at random on one platform is either a race in the test or a real timing bug in the daemon. Each one is either fixed or shown to be a test-only race, so CI is green on the first run.

## Known failures

- **`tests/replication/protocol.rs::followers_refuse_older_epochs_and_batches_that_skip_ahead`** (ubuntu-latest, PR #33): the test expected `FailedPrecondition` and got `Aborted: "a newer stream replaced this one"`. Two streams race to the follower, and the test assumes an order.
- **Two Windows replication tests** (run 41575487780 on `main`, and again on PR #31): a follower missed a 10-second acknowledgement.
- **`tests/database.rs::draining_a_node_moves_its_primaries_while_a_client_keeps_writing`** (macos-latest, run 41562990938 on PR #29): "1 of the 1 followers a write needs did not acknowledge it within 20s" during a drain.

## Scope

- For each test, reproduce the failure locally under load: run it in a loop, with `--test-threads` raised, and on the platform it failed on where possible. Record the failure rate before and after the fix.
- Decide whether each one is a test race or a daemon bug. A daemon bug gets a fix and a simulation scenario that covers it (ADR-0020). A test race gets a fix that waits on the condition the test means, not a longer timeout.
- A longer timeout alone is not a fix, unless the ticket records why the old bound was wrong.

## Acceptance criteria

- [ ] Each of the three is either fixed with its cause stated in the commit, or shown to be a test-only race and fixed in the test.
- [ ] Each test passes 200 runs in a row locally, on the platform it failed on where possible.
- [ ] None of them needs a rerun across ten consecutive CI runs on `main`.

## Progress

- Not started.
