# fleetringd

The FleetRing daemon. It places, replicates, and fails over SQLite databases across machines you own. One binary runs a data node, the control node, or both (a one-machine cluster), and its subcommands talk to a running cluster.

> **Status:** early development. Not production-ready.

## Run it

Write a configuration file. This one runs a one-node cluster, with its databases in `/var/lib/fleetring`:

```toml
data_dir = "/var/lib/fleetring"
listen = "127.0.0.1:7400"
role = "both"

[node]
id = "node-1"
```

Then start the daemon, and talk to it from another terminal:

```sh
fleetringd --config fleetringd.toml

fleetringd status --config fleetringd.toml
fleetringd db create notes --replication-factor 1 --config fleetringd.toml
fleetringd db exec notes "CREATE TABLE note (body TEXT)" --config fleetringd.toml
fleetringd db exec notes "INSERT INTO note VALUES (?)" "hello" --config fleetringd.toml
fleetringd db query notes "SELECT body FROM note" --config fleetringd.toml
fleetringd db locate notes --config fleetringd.toml
fleetringd db list --config fleetringd.toml
fleetringd status --node 127.0.0.1:7400
fleetringd db drop notes --yes --config fleetringd.toml
```

`db list` shows every database with its engine, epoch, replication factor, primary, and replicas, and ends with how many databases each node is primary for; `--node <id>` keeps the databases one node is primary for. `status --node <host:port>` asks one data node what it believes on its own, so its ring fingerprint can be compared with `status`. `db drop` removes a database for good: every replica deletes its copy within a few seconds. It asks first on a terminal; `--yes` skips the question.

Each database chooses its durability when it is created (ADR-0023). `--acks quorum` (the default) acknowledges a write once a majority of its replicas hold it; `--acks all` waits for every replica, so one replica down blocks writes; `--acks local` acknowledges after the primary's own fsync, which is fastest but may lose acknowledged writes when the primary fails over. `--min-replicas <n>` sets the floor: a write that arrives while fewer than `n` replicas, the primary included, are reachable is refused with `UNAVAILABLE` before anything commits, so it can be retried cleanly; it must lie between the write quorum and the replication factor, and defaults to the write quorum. `db locate` and `db list` show the settings, `db set-durability` changes them from the database's next epoch on, and `[replication] acks` and `min_replicas` in the control node's configuration set the defaults for databases created without them.

The subcommands take `--control <host:port>`, the address of the node that runs the control role, or `--config <file>` to read it from a daemon's configuration. A control call sent to a data-only node answers with the control node's address.

`fleetringd --help` lists every flag and subcommand. [`deploy/fleetringd.toml`](https://github.com/fleetring/fleetring/blob/main/deploy/fleetringd.toml) documents the main settings, including a multi-node cluster's.

## Install

- **Release archives** for Linux (glibc 2.36 or newer), macOS, and Windows, on x86_64 and ARM64 (Windows on x86_64 only), are on the [releases page](https://github.com/fleetring/fleetring/releases), with a `SHA256SUMS` file.
- **The install script** picks the archive for the machine, verifies its checksum, and installs the binary:

  ```sh
  curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh | sh
  ```

- **The container image**, for `linux/amd64` and `linux/arm64`: `ghcr.io/fleetring/fleetringd`.

## License

Dual-licensed under MIT (`LICENSE-MIT`) or Apache-2.0 (`LICENSE-APACHE`), at your option.

[fleetring.dev](https://fleetring.dev) · [GitHub](https://github.com/fleetring/fleetring)
