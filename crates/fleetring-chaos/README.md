# fleetring-chaos

Fault injection on real `fleetringd` processes, judged by the history checker ([ADR-0036](../../docs/adr/0036-fault-injection-on-real-daemons.md), FR-065).

The simulation (ADR-0020) runs the real daemon code, but over turmoil's network and clock, so it can't find bugs in real TCP and HTTP/2 timeouts, thread scheduling, `fsync`, paused processes, or wall clocks that jump. This harness runs the shipped binary instead: five daemons in Docker containers, `n1` the control node and `n2` to `n5` data nodes, with every connection mutual TLS. While a seeded workload writes and reads, a seeded fault schedule partitions nodes with `iptables`, kills and pauses them with signals, and skews the control node's clock with `libfaketime`; afterwards FR-036's checker judges the history against the rows on every node's disk.

## Requirements

- Linux, with Docker 24 or later. `run` refuses any other host. `schedule` runs anywhere.
- About 4 GiB of free disk for the image build.
- Docker without user-namespace remapping: the containers run as the run directory's owner, so everything they write stays readable by the harness.
- A kernel with the `nf_tables` modules, which the image's `iptables` uses; GitHub's Ubuntu runners have them.

## Running it

```sh
# One scenario: partitions, kills, pauses, clock, or mixed. Builds deploy/chaos/Dockerfile first.
cargo run -p fleetring-chaos --features harness --release -- run --scenario mixed --time-limit 600s

# The self-test: must find a lost acknowledged write, or it fails.
cargo run -p fleetring-chaos --features harness --release -- run --scenario partitions --self-test

# The faults a seed plans, without Docker.
cargo run -p fleetring-chaos --features harness -- schedule --scenario mixed --seed 7

# Remove containers and the network an interrupted run left behind.
cargo run -p fleetring-chaos --features harness -- clean
```

`--seed` fixes the run's choices (random and printed first if unset), `--image` and `--skip-build` use an image you built, `--out` moves the run directories (default `target/chaos`), and `--keep` leaves the containers running for inspection. The harness's clients run on the host and reach the nodes at `10.77.0.11` to `10.77.0.15` on the `fleetring-chaos` bridge network. Docker Desktop on macOS doesn't route the host to bridge addresses, so run the harness on a Linux host or VM. On Docker Desktop, a Linux build of the harness also runs in a container on the VM's host network, with the Docker socket, a `docker` CLI, and the workspace mounted at the same path (Docker resolves the run directory's bind mounts on the host):

```sh
docker run --rm --network host -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$PWD:$PWD" -w "$PWD" -v /path/to/linux/docker:/usr/local/bin/docker:ro \
  rust:1.99-slim-bookworm target/linux/release/fleetring-chaos run --scenario mixed --skip-build
```

The nightly `Chaos` workflow (`.github/workflows/chaos.yml`) runs the five scenarios and the self-test for 600 s each, and on manual dispatch with a time limit and seed of your choice.

## What a run does

1. Builds the image, creates the network, and issues the fleet's certificates with the image's own `fleetring cert` commands.
2. Starts the five containers, waits until all five members are live, and creates eight databases with replication factor 3: `chaos/db-0` to `chaos/db-5` with `acks = quorum`, `chaos/db-6` and `chaos/db-7` with `acks = all` (all `local` in the self-test, checked as `quorum`).
3. Waits until 55 s after the certificates were issued, so no clock skew puts one outside its validity, then runs ten clients (70 % writes of unique values, 30 % reads) and the fault schedule together for the time limit.
4. Heals every fault, runs quiet for 30 s, reads every database again, and waits up to 60 s per database for its replicas' hash chains to agree.
5. Saves each node's log, stops the containers, reads every node's copy of every database from its data directory, and checks the history, each database's final replica set, and the control log's replay.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Every check passed (self-test: the checker found a lost acknowledged write). |
| 1 | A check found a violation (self-test: the run looked valid, so the harness checks nothing). |
| 2 | The harness itself failed: no Docker, a fleet that never came up, a fault that couldn't be applied, or fewer than 100 acknowledged writes. |
| 130 | Interrupted with Ctrl-C; every fault was undone and the fleet removed. |

A failing run prints its seed, its run directory, and the command that reruns it.

## A run directory

`target/chaos/<scenario>-<seed>/` holds:

- `history.txt`: every operation's invocation and completion, in time order. A completion is `ok` (acknowledged: its effect must survive), `fail` (refused: it had no effect), or `info` (no answer in 5 s: it may or may not have taken effect).
- `schedule.txt`: each fault as it ran, with its start and end in milliseconds from the fault phase's start, its kind, and its targets (`n1,n4 | n2,n3,n5` for a partition, `n1,n4 | n2 | n3,n5` for a bridge with `n2` in the middle, `chaos/db-3 primary n4` for an isolated primary, `n1 -42s` or `n1 ±42s` for the clock). The first line says where the fault phase starts in `history.txt`'s time. A planned fault the deadline cut off is marked `(not run)`.
- `verdict.json`: `valid`, `invalid`, or `unknown`, the broken invariant, the detail, and how many operations ended each way.
- `nodes/nX.log`: each daemon's output; `nodes/nX/data/`: its data directory; `tls/`: the fleet's certificates.

## Reproducing a finding

A seed replays the fault schedule and the workload's choices, not the processes' timing, so a violation may not recur on a rerun. Start from the run directory: find the operation the verdict names in `history.txt`, the faults around it in `schedule.txt`, and what the nodes logged then. Then reproduce it in the simulation with `sim::Fleet`'s faults (partitions, crashes, stops, restarts), and add that test's seed to the fixed list, so the fix has a deterministic regression test.
