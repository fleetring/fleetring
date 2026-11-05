# ADR-0039: Dev mode is the normal daemon on loopback, with a local authority and an admin principal it creates once

- **Status:** Accepted (2026-11-04)
- **Date:** 2026-11-04
- **Related:** ADR-0025 (principals and scopes), ADR-0034 (admin listener), ADR-0038 (upgrades); tickets FR-068, FR-056, FR-060

## Context

A first run of `fleetringd` needs an authority, a node certificate, a client certificate, a configuration file, and a `[[principal]]` entry before a single statement runs. That is the right ceremony for a fleet and the wrong one for a laptop: the first contact should take minutes, as `brew services start postgresql` does, and the code written against it should run unchanged against a fleet.

Shortcuts that would get there fast all weaken something: plaintext (`--insecure-plaintext`) teaches code that never presents a certificate; a second, simpler server is a second implementation to keep in step; a node that listens beyond loopback with an authority key beside it is an open door. So dev mode is a security posture as much as a convenience, and it is written down here.

## Decision

1. **`fleetringd dev [--data-dir <dir>] [--port <n>]` runs the normal daemon with a generated configuration.** It prepares a directory, `~/.fleetring` by default (`$HOME` on Unix, `%USERPROFILE%` on Windows), then loads `<dir>/fleetringd.toml` with `Config::load` and runs it exactly as `fleetringd --config` would: the same `Daemon::start`, the same mutual TLS, the same scope checks, the same SIGHUP restart. Nothing in the daemon branches on dev mode except validation and one status field (point 4). `fleetringd --config ~/.fleetring/fleetringd.toml` runs the same node.

2. **What it creates, on first run, with the library calls `cert init`, `cert node`, and `cert client` make:**
   - `tls/ca.crt` and `tls/ca.key`: an authority for the fleet `dev`;
   - `tls/node.dev.crt` and `.key`: the node `dev`, for `127.0.0.1`, `::1`, and `localhost`;
   - `tls/client.admin.crt` and `.key`: the principal `admin`, which the configuration's `[[principal]]` entry gives every scope (`admin:*`, `stream:*`, `fleet`);
   - `fleetringd.toml`: `dev = true`, `role = "both"`, `listen = "127.0.0.1:<port>"` (7400 by default), the admin listener on the next port (7401), `[membership] on_shutdown = "stay"` so a stop keeps the databases placed for the next start, and a logical file sink, `dev`, writing every database's changes to `changes/`;
   - `data/`, the daemon's data directory, and `changes/`.

   Leaf certificates last a year, not the 7 and 30 days `cert client` and `cert node` default to, since nobody renews certificates on a laptop. **No token** is stored: the CLI authenticates with the admin certificate, which already carries every scope, and a token lasts at most 30 days, so a stored one would go stale; `fleetringd token issue --dir ~/.fleetring/tls` issues one when an application needs it.

3. **Loopback only, enforced by the configuration, not the command.** With `dev = true`, `Config::validate` refuses a `listen`, `node.advertise`, `control.address`, or `[admin] listen` that isn't a loopback address, a role other than `both`, and `insecure_plaintext`. Because the check lives in validation, it holds however the node is started: `fleetringd dev`, `fleetringd --config` with the generated file, `--listen` on top of either, or a hand-edited file. The reason is the directory: `ca.key` and the admin key sit unencrypted beside the node, so anyone who could reach the node could as well have been handed the authority. `fleetringd dev` also refuses a directory that isn't dev mode's, before it creates or writes anything in it, so `--data-dir` pointed at a real node's directory neither mints a certificate from that fleet's authority nor leaves a new key there. A directory is dev mode's if its `fleetringd.toml` says `dev = true` (and agrees with `--port`), or, without that file, if it is missing, empty, or a *partial dev state*: nothing but `tls/`, `data/`, `changes/`, and `env`. In either case `tls/` may hold only the six files dev mode writes, and an authority there must be for the fleet `dev`.

4. **It says so.** The banner names the address, the data directory, and the one line that points the CLI at it. The control node sets the new `MembershipUpdate.dev_mode` field (field 4; additive, unset by older nodes), and `fleetringd status` prints `dev mode: one node on loopback, for development only` above the members. The daemon also logs a warning at every start, as plaintext does.

5. **State is reused, never replaced.** Every file is written with `create_new`, so an existing one is never overwritten: a second run reuses the authority, the certificates, and the configuration byte for byte. A partial state completes what's missing from what exists: with `ca.crt` and `ca.key` present, missing leaf certificates are issued from that authority. A state that can't be completed safely fails with the file and the fix: `ca.crt` without `ca.key` (or the reverse), a certificate without its key, leaf certificates without their authority, an expired leaf certificate, or a `--port` that disagrees with the existing configuration. The authority is never created over an existing file, nor beside certificates another one issued. When the fix is to start over with a new authority (removing `tls/`), the message says that every certificate and token the old one issued then stops working; the databases are kept.

6. **The CLI is pointed at it with an environment file.** `<dir>/env` exports `FLEETRING_CONTROL`, `FLEETRING_TLS_DIR`, and `FLEETRING_AS=admin`, and the banner prints `. <dir>/env`. It is derived from `fleetringd.toml` and rewritten at every start, so it follows an edited port. `FLEETRING_CONTROL` is new: `--control`'s variable, read after `--control` and `--config`, as `docs/cli.md`'s precedence says. This replaces the ticket's CLI context until FR-056 builds contexts; then `fleetringd dev` writes a `dev` context instead, and the file goes.

7. **Permissions.** On Unix the directory and its subdirectories are created `0700` and private keys `0600`, as `cert init` writes them. Windows gets the user profile's default ACLs.

## Alternatives considered

- **A separate in-process "dev server".** Faster to start, but a second implementation whose behavior drifts from the daemon's, and code tested against it could fail against a fleet. Rejected for point 1.
- **Plaintext dev mode.** No certificates to create, but clients would never exercise TLS or scopes, and a dev node reachable over the network would accept anything. Rejected.
- **Client commands falling back to `~/.fleetring` when no TLS flags are given.** No file to load, but it is an implicit identity that changes what a command does depending on what lies in the home directory, and it would have to be undone when contexts arrive. The environment file is explicit and needs no code in the commands beyond `FLEETRING_CONTROL`. Rejected.
- **Checking loopback in `fleetringd dev` only.** Weaker: `fleetringd --config ~/.fleetring/fleetringd.toml --listen 0.0.0.0:7400` would bypass it. Rejected for point 3.
- **A dev-mode default replication factor of 1 on the control node.** Would let `db create notes` work without `--replication-factor 1`, but changes what a creation request without a factor means on the wire. Left for a follow-up; the quickstart passes the flag, and the same command works against a fleet.

## Consequences

- A laptop runs a real node in one command, with the same TLS and scopes as a fleet; code that works against it runs against a fleet, given the fleet's address and certificates.
- `ca.key` and the admin key sit unencrypted in the data directory. That is acceptable only because the node is loopback-only and single-user; `SECURITY.md` says dev mode is for local development only.
- A single node holds one copy of each database: a disk failure loses it, and `--replication-factor` above 1 leaves writes unable to reach their quorum.
- FR-056 replaces the environment file with a context, and FR-060 packages `deploy/dev/`'s service definitions (a Homebrew `service do` block and a systemd user unit).
