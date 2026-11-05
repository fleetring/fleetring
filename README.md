# FleetRing

**Consistent-hashing placement and routing for fleets of SQLite, libSQL, and Turso databases.**

One database per agent, tenant, or device, spread across nodes you own. FleetRing decides where each database lives, routes traffic to it, and moves it when nodes join or leave.

> **Status:** early development. The [`fleetring`](https://crates.io/crates/fleetring) placement crate is released as 0.1.0, and `fleetringd`, the daemon that replicates, fails over, archives, and restores fleets of SQLite databases, has [a release](https://github.com/fleetring/fleetring/releases/latest) with binaries and a container image. `fleetringd` is **not production-ready**: [`docs/roadmap.md`](docs/roadmap.md) lists what has to be true before it is safe to depend on, and its configuration, protocol, and on-disk formats can still change between releases.

## Why

SQLite is now a server-side building block, and a database per agent or tenant is cheap. The hard part is **where each one lives, how clients find it, and how it moves** without losing writes. FleetRing handles that, vendor-neutral and self-hostable.

## Features

**Available now**, in the `fleetring` crate:

- Consistent hashing: weighted rendezvous (default) or a virtual-node ring, with identical placement on every platform, verified in CI on big-endian, 32-bit, and wasm targets
- Placement API: database name → nodes, with zone- and host-aware replicas and deterministic failover
- Bounded loads, applied when a database is created or moved: it skips nodes that would end up more than a set headroom above their share of bytes or traffic
- Cells: a directory or ring picks a region or tenant pool, and a ring inside it picks the nodes
- Ring diffs for migration planning, fingerprints that catch diverging peers, and topology diagnostics

**Available now**, in the `fleetringd` daemon, which runs fleets of SQLite databases on machines you own:

- Replication and failover: the primary streams WAL frames to its followers and acknowledges a write once a quorum holds it, leases and epochs keep one writer per database, and a new or lagging follower catches up from a snapshot
- Durability, set per database: when a write is acknowledged and how many replicas must be reachable; `node drain` hands a node's primaries over before a planned stop
- Archives and point-in-time restore: each database's log and base snapshots go to an S3-compatible bucket or a directory, and a restore checks every batch against the log's hash chain
- Change streams to file or webhook sinks (over HTTPS, with optional client certificates or bearer tokens) that resume where they stopped after a failover, and `lease watch`, which follows the leases a node gains and loses
- Security: mutual TLS with a built-in certificate authority, principals with prefix scopes and signed, revocable tokens, statements confined to their database, node calls bound to placement, and signed, attested releases
- Operations: `db create/exec/query/locate/list/drop/verify` and `status`, with a hash-chained log that shows whether every replica holds the same history; Prometheus metrics, health and readiness probes, request IDs, and JSON logs; `fleetringd upgrade`, verified with cosign, with rollback and a tested rolling upgrade
- `fleetringd dev`: one command for a local node on a laptop, over the same TLS and scopes as a fleet

**Planned**, each tracked as a [GitHub issue](https://github.com/fleetring/fleetring/issues):

- Router that works with existing libSQL and Turso clients
- Live migration when nodes join or leave, with zero lost writes, and rebalancing
- A Raft control plane, so the control plane has no single point of failure
- Fault injection on real daemons, judged by the history checker
- Keyed leases, Merkle anti-entropy, and cells in the daemon
- CLI contexts and JSON output, a terminal UI, and a desktop app
- Archive follow-ups: acknowledging a write once it is archived, workload-identity credentials, and Vercel Blob
- An agent kit with an MCP server, vector search, a Turso adapter, a Rust client, framework adapters, and a Ruby gem
- apt, yum, AUR, and Homebrew packages

## Install

The placement crate is on crates.io: `cargo add fleetring`.

The `fleetringd` daemon ships as one binary. Each `fleetringd` release has three ways to install it:

- **Binaries** for Linux (glibc 2.36 or newer) and macOS on x86_64 and ARM64, and for Windows on x86_64. They're on the [releases page](https://github.com/fleetring/fleetring/releases) as `fleetringd-<version>-<target>.tar.gz` (`.zip` on Windows), with a `SHA256SUMS` file.
- **The install script** picks the archive for your OS and CPU, verifies its checksum, and installs `fleetringd` to `/usr/local/bin` as root, or to `~/.local/bin` otherwise:

  ```sh
  curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh | sh
  # A given version, somewhere else:
  curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh \
    | sh -s -- --version 0.5.0 --prefix /opt/fleetring
  ```

- **Docker**, for `linux/amd64` and `linux/arm64`. The image runs a one-node fleet from [`deploy/fleetringd.toml`](deploy/fleetringd.toml) as a non-root user, with its databases in the `/data` volume:

  ```sh
  docker run -d --name fleetringd -p 7400:7400 -v fleetring-data:/data ghcr.io/fleetring/fleetringd
  docker exec fleetringd fleetringd status --config /etc/fleetring/fleetringd.toml
  ```

  Mount your own configuration over `/etc/fleetring/fleetringd.toml` to change it.

To build from source instead: `cargo install --locked --git https://github.com/fleetring/fleetring fleetringd`.

### Verify a release

The release workflow signs `SHA256SUMS` keylessly with [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) and attaches build provenance to every archive, `install.sh`, and the container image. The install script runs the first check below when `cosign` is on your `PATH`, and says so when it isn't. To verify by hand, download `SHA256SUMS` and `SHA256SUMS.sigstore.json` from the release, then:

```sh
# The checksum file was signed by this repository's release workflow, from a fleetringd tag.
cosign verify-blob SHA256SUMS \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/fleetring/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum --check --ignore-missing SHA256SUMS   # shasum -a 256 -c on macOS

# A downloaded file was built by that workflow.
gh attestation verify fleetringd-<version>-<target>.tar.gz --repo fleetring/fleetring

# The container image was too.
gh attestation verify oci://ghcr.io/fleetring/fleetringd:<version> --repo fleetring/fleetring
```

## Quickstart

Five minutes on a laptop, with no certificates or configuration to write. Install `fleetringd` (above), then start a dev mode node: one node on loopback, with its own authority and an admin certificate, kept in `~/.fleetring` and reused on every start ([ADR-0039](docs/adr/0039-dev-mode.md)):

```sh
fleetringd dev
```

It prints where it listens and the line that points the commands at it. In another terminal:

```sh
. ~/.fleetring/env                       # FLEETRING_CONTROL, FLEETRING_TLS_DIR, FLEETRING_AS
fleetringd status                        # starts with "dev mode: one node on loopback, ..."
fleetringd db create notes               # one replica, a dev node's default
fleetringd db exec notes "CREATE TABLE note (body TEXT)"
fleetringd db exec notes "INSERT INTO note VALUES (?)" "hello"
fleetringd db query notes "SELECT body FROM note"
tail -f ~/.fleetring/changes/notes.ndjson   # the change stream: one JSON line per transaction
```

Run another `db exec` and watch its row arrive in the `tail`. Ctrl-C stops the node, and the next `fleetringd dev` picks up the same databases. A dev node holds one copy of each database: its configuration makes 1 the default replication factor, so `db create` needs no `--replication-factor`, and the same commands and the same client code run against a fleet, given its address and certificates, where the default is 3. Dev mode is for development only: it listens on loopback alone and keeps its authority's key in `~/.fleetring` ([SECURITY.md](SECURITY.md#dev-mode)). [`deploy/dev/`](deploy/dev/) has a Homebrew service block and a systemd user unit that run it at login.

## The placement crate

The [`fleetring`](crates/fleetring) crate at the heart of FleetRing is general-purpose: *deterministic, cross-language placement with zone-aware replicas*. FleetRing uses it to place SQLite databases, but it works for anything keyed: key-value shards, chat rooms, actors, queue partitions, and inference sessions.

## Use cases

- **AI agent memory:** a database per agent, placed next to its compute
- **Multi-tenant SaaS:** a database per customer, for isolation and easy deletes
- **Edge and IoT:** local databases per site or device that sync to regional nodes

## Roadmap

- [x] `fleetring` ring and placement crate (0.1.0)
- [x] `fleetringd`: replication, quorum acknowledgements, and failover (0.1.0)
- [x] Security: mutual TLS, scopes and tokens, signed releases (0.2.0)
- [x] Change streams and lease watch (0.2.0); streams resume after failover (0.3.0)
- [x] Backups and point-in-time restore (0.3.0)
- [x] Metrics, health checks, and a tested rolling upgrade (0.4.0)
- [x] `fleetringd dev`, a one-command local node, and webhook sinks over HTTPS (0.5.0)
- [ ] Router sidecar (libSQL/Hrana protocol)
- [ ] Live migration
- [ ] Raft control plane
- [ ] Fault injection on real daemons
- [ ] Demo: 10k agents with live rebalancing
- [ ] Safe to depend on, including an outside review of the protocol and the auth design

[`docs/roadmap.md`](docs/roadmap.md) explains each step and what "safe to use" means. [GitHub Issues](https://github.com/fleetring/fleetring/issues) track the work.

## Design docs

- [`docs/adr/`](docs/adr/): architecture decision records, one per decision, explaining why the placement crate works the way it does.
- [`docs/spec/`](docs/spec/): the placement spec. [Version 1](docs/spec/placement-v1.md) defines placement precisely enough that every implementation, in any language, puts each key on the same nodes. It is frozen as of `fleetring` 0.1.0.
- [GitHub Issues](https://github.com/fleetring/fleetring/issues): the work plan, with each issue's scope and acceptance criteria. [`docs/tickets/`](docs/tickets/) archives the finished tickets, and [`docs/plans/`](docs/plans/) holds implementation plans.
- [`docs/roadmap.md`](docs/roadmap.md): the epics between today and a release that is safe to depend on, and [`docs/review.md`](docs/review.md), the known bottlenecks and the standards behind the design.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.

---

[fleetring.dev](https://fleetring.dev) · [GitHub](https://github.com/fleetring/fleetring)
