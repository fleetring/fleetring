---
id: FR-024
title: Simulation harness with turmoil
status: todo
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

- [ ] A simulated three-node cluster starts, exchanges heartbeats, and shuts down deterministically: the same seed produces the same event trace twice.
- [ ] A test partitions and heals the network, and crashes and restarts a host.
- [ ] A failing seed can be rerun alone with one command, which is printed on failure.
- [ ] The fixed CI seeds finish in under two minutes.
