# FleetRing

**Consistent-hashing placement and routing for fleets of SQLite, libSQL, and Turso databases.**

One database per agent, tenant, or device, spread across nodes you own. FleetRing decides where each database lives, routes traffic to it, and moves it when nodes join or leave.

> **Status:** early development. The placement crate is released as 0.1.0; the router, live migration, and CLI are planned. APIs will change. Not production-ready.

## Why

SQLite is now a server-side building block, and a database per agent or tenant is cheap. The hard part is **where each one lives, how clients find it, and how it moves** without losing writes. FleetRing handles that, vendor-neutral and self-hostable.

## Features

**Available now**, in the `fleetring` crate:

- Consistent hashing: weighted rendezvous (default) or a virtual-node ring, with identical placement on every platform, verified in CI on big-endian, 32-bit, and wasm targets
- Placement API: database name → nodes, with zone- and host-aware replicas and deterministic failover
- Bounded loads, applied when a database is created or moved: it skips nodes that would end up more than a set headroom above their share of bytes or traffic
- Cells: a directory or ring picks a region or tenant pool, and a ring inside it picks the nodes
- Ring diffs for migration planning, fingerprints that catch diverging peers, and topology diagnostics

**Planned:**

- Router that works with existing libSQL and Turso clients
- Live migration when nodes join or leave, with zero lost writes
- CLI: `fleetring up`, `node add/remove`, `db create/locate/move`, `status`

## Install

The placement crate is on crates.io: `cargo add fleetring`.

The `fleetringd` daemon ships as one binary. Each `fleetringd` release has three ways to install it:

- **Binaries** for Linux (glibc 2.36 or newer) and macOS on x86_64 and ARM64, and for Windows on x86_64. They're on the [releases page](https://github.com/fleetring/fleetring/releases) as `fleetringd-<version>-<target>.tar.gz` (`.zip` on Windows), with a `SHA256SUMS` file.
- **The install script** picks the archive for your OS and CPU, verifies its checksum, and installs `fleetringd` to `/usr/local/bin` as root, or to `~/.local/bin` otherwise:

  ```sh
  curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh | sh
  # A given version, somewhere else:
  curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh \
    | sh -s -- --version 0.1.0 --prefix /opt/fleetring
  ```

- **Docker**, for `linux/amd64` and `linux/arm64`. The image runs a one-node cluster from [`deploy/fleetringd.toml`](deploy/fleetringd.toml) as a non-root user, with its databases in the `/data` volume:

  ```sh
  docker run -d --name fleetringd -p 7400:7400 -v fleetring-data:/data ghcr.io/fleetring/fleetringd
  docker exec fleetringd fleetringd status --config /etc/fleetring/fleetringd.toml
  ```

  Mount your own configuration over `/etc/fleetring/fleetringd.toml` to change it.

To build from source instead: `cargo install --locked --git https://github.com/fleetring/fleetring fleetringd`.

## The placement crate

The [`fleetring`](crates/fleetring) crate at the heart of FleetRing is general-purpose: *deterministic, cross-language placement with zone-aware replicas*. FleetRing uses it to place SQLite databases, but it works for anything keyed: key-value shards, chat rooms, actors, queue partitions, and inference sessions.

## Use cases

- **AI agent memory:** a database per agent, placed next to its compute
- **Multi-tenant SaaS:** a database per customer, for isolation and easy deletes
- **Edge and IoT:** local databases per site or device that sync to regional nodes

## Roadmap

- [x] `fleetring` ring and placement crate (0.1.0)
- [ ] Router sidecar (libSQL/Hrana protocol)
- [ ] Live migration
- [ ] Demo: 10k agents with live rebalancing
- [ ] Backups and point-in-time restore

## Design docs

- [`docs/adr/`](docs/adr/): architecture decision records, one per decision, explaining why the placement crate works the way it does.
- [`docs/spec/`](docs/spec/): the placement spec. [Version 1](docs/spec/placement-v1.md) defines placement precisely enough that every implementation, in any language, puts each key on the same nodes. It is frozen as of `fleetring` 0.1.0.
- [`docs/tickets/`](docs/tickets/): the work plan for the placement crate, with each ticket's scope, status, and acceptance criteria.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.

---

[fleetring.dev](https://fleetring.dev) · [GitHub](https://github.com/fleetring/fleetring)
