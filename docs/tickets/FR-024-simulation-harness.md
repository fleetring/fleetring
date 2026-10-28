---
id: FR-024
title: Simulation harness with turmoil
status: done
priority: P0
size: M
milestone: M5 Daemon foundations
depends_on: [FR-022, FR-023]
adrs: [ADR-0020]
---

# FR-024: Simulation harness with turmoil

## Goal

Run whole clusters in one test process, on a simulated network and clock, with a seed that replays any failure (ADR-0020). It comes early so that every later ticket adds its invariants here.

## Scope

- **Hosts:** one turmoil host per node, running the real `fleetringd` code in the data role, the control role, or both.
- **Determinism:** one seed drives turmoil and the workload. Daemon code takes time from `tokio::time` and randomness from an injected generator. A test or lint keeps `SystemTime::now` and thread-local RNGs out of daemon code.
- **Engine simulation mode:** SQLite calls run inline on the simulation thread. FR-026 provides the switch.
- **Faults:** partition and heal, delay and drop messages, crash and restart a host (its data directory survives), and stop the control node.
- **Workload:** clients that write sequenced values and record which writes were acknowledged.
- **Checker:** a module of invariants that later tickets extend (ADR-0020, point 4).
- **CI:** a fixed set of seeds on every build, and a scheduled sweep over random seeds that prints any failing seed.

## Acceptance criteria

- [x] A simulated three-node cluster starts, exchanges heartbeats, and shuts down deterministically: the same seed produces the same event trace twice.
- [x] A test partitions and heals the network, and crashes and restarts a host.
- [x] A failing seed can be rerun alone with one command, which is printed on failure.
- [x] The fixed CI seeds finish in under two minutes.

## Notes from FR-029

- **Exact replay** needs `RUSTFLAGS="--cfg tokio_unstable"` in the simulation CI job, so turmoil can seed Tokio's scheduler RNG. Without it, event order replays but simulated times drift by a few milliseconds.
- **`Daemon` can't run under turmoil yet:** it binds a real TCP listener, uses `SystemClock`, and dials with `ControlClient::new`. `membership_sim.rs` assembles `ControlNode`, `ControlService`, and `Membership` by hand. Let `Daemon::start` take an injected incoming stream, a connector, and a clock, so the harness runs the real daemon.
- **Partitions in turmoil leave connections stalled**, not failed. Every client loop needs per-call timeouts and a reconnect, as `Membership` has.

## Progress

- 2026-10-28: Done.
  - **Injection:** `Daemon::start_with(config, Environment)` takes the network, the clock, and the storage execution; `Daemon::start(config)` passes `Environment::production()` (real TCP with default timeouts, `SystemClock`, storage on dedicated threads), so the binary behaves as before.
    - `Network` is `Tcp(TransportConfig)` or, behind the `simulation` feature, `Simulated`. It binds the listener (`tokio` or turmoil `TcpListener`, served through `serve_with_incoming_shutdown`) and builds the control client and peer channels (`Network::control_client`, `Network::channel`).
    - `Execution::Inline` runs the control log inline (`StorageIo::Inline`) and forces `[engine] execution = "inline"`; `Execution::Threads` leaves the engine to its configuration.
    - `Environment::simulated(clock)` is turmoil's network, the given clock, and inline storage. Production code never names turmoil outside `cfg(feature = "simulation")`.
  - **Harness (`fleetringd::sim`, behind `simulation`):** in the library rather than a test-support module, so every test binary and later ticket shares one linted, documented copy.
    - `Cluster::builder(seed)` adds hosts by role (`control`, `data`, `both`, or `host(name, Role)`), `Timings` (heartbeat interval and timeout, lease duration, failover margin, snapshot interval), a per-host `configure` hook, the maximum message latency (default 20 ms), and the client operation timeout. Each host runs the real `Daemon` on its own turmoil host and real temporary data directory.
    - Time: `step`, `run_for`, `run_until(limit, what, condition)`, `await_workloads`, `await_ring`, `await_ring_on(nodes)`.
    - Faults (`Fault`, recorded in the trace): `partition`/`repair`, `set_latency`, `set_loss` (`LossRate` in thousandths per tick), `crash` (power loss: memory gone, disk kept, peers' connections stall), `stop` (graceful), and `restart`.
    - Workloads: a `Workload` trait (`next(&mut SimRng) -> Step`, `observe(operation, outcome)`) runs on a client host; every `Operation` (join, heartbeat, leave, create, locate, acquire, renew) lands in a `History` as acknowledged, rejected, or indeterminate. `CreateAndAcquire` creates databases, takes their first leases on random nodes (with `Locate` after an uncertain acquire), and renews them; `Script` replays a list until each step is answered; `Operator` takes operations the test queues during a run.
    - Checker (`sim::check`): `ring_converged` (every live data node publishes the control plane's epoch, fingerprint, and liveness), `control_log_replays` (recovery equals a replay from genesis), `acknowledged_control_survives` (no acknowledged join, create, grant, or renewal lost), and `one_primary_per_epoch` (from granted leases). Crashing or stopping the control host runs the log checks; `Cluster::check` runs the others at any time; `Cluster::finish` stops every host and also checks that the recovered state equals the state the control node last published. The module docs mark the extension points for "no acknowledged write lost" (FR-030) and data-node primaries per epoch (FR-031).
    - Trace: `Event { at, source, kind }` for starts, stops, publications, operations, faults, and notes. `FLEETRING_SIM_TRACE=1` prints each cluster's trace when it is dropped.
  - **Determinism:** one seed drives turmoil (`rng_seed`) and, through `SimRng` (SplitMix64, kept in-tree so a seed survives dependency upgrades), the workloads and the test's own fault schedule.
    - With `RUSTFLAGS="--cfg tokio_unstable"`, two runs of a seed give identical traces, simulated times included (`Replay::Exact`), in one process and across processes. Without it, only the test's own events (faults and notes) are compared, in order and without times (`Replay::Order`): node publications can coalesce differently and operations can fail with different statuses when events shift by a millisecond. `Replay::supported()` picks the mode from the build.
    - Crash and restart cut the host off from every peer while turmoil tears its runtime down. Tokio drops a runtime's tasks in an order set by process-wide task IDs, and each connection closed would otherwise send a message that draws a delay from turmoil's generator, so runs diverged after the first crash.
  - **Seeds:** `sim::seeds::run(binary, fixed, scenario)` runs the fixed seeds (`FIXED = [1, 24, 29, 2026]`), or only `FLEETRING_SIM_SEED`. A failing or panicking seed prints `FLEETRING_SIM_SEED=<seed> cargo test -p fleetringd --features simulation --test sim -- --exact <test>`, prefixed with `RUSTFLAGS="--cfg tokio_unstable"` in such builds.
  - **Tests (`tests/sim/`, one binary, `required-features = ["simulation"]`):**
    - Three data nodes and a control node start, converge, exchange heartbeats for 2 s, and shut down; the same seed gives the same trace twice.
    - A partition marks a node down and its repair brings it back; a crashed data node is marked down and rejoins after a restart.
    - A create-and-acquire workload runs while the control node crashes twice and stops once at seeded times, a data node crashes, and the client's link drops messages; every database ends up leased and no acknowledgement is lost.
    - `control_sim.rs` (FR-028) and `membership_sim.rs` (FR-029) moved onto the harness as `control` and `membership`, keeping their checks. The control test now crashes the control node every 40 to 120 ms of uptime and requires at least five crashes (the old fixed schedule assumed crashes reset connections).
    - The fixed seeds take about 0.6 s in a debug build. Sweeps of 300 seeds passed with and without `tokio_unstable`.
  - **Daemon fix found by the harness:** shutdown waited for every HTTP/2 connection to close, and HTTP/2's graceful shutdown waits for the peer, so a peer that vanished held shutdown until TCP gave up. `run_until` now gives connections `SHUTDOWN_GRACE` (5 s) and then drops them.
  - **CI:** a `simulation` job runs `tests/sim` with `RUSTFLAGS="--cfg tokio_unstable"`; the existing all-features test jobs run it without the flag on all three OSes. `simulation-sweep.yml` runs nightly and on demand over random seeds (500 by default), listing each failing seed's rerun command in the job summary.

