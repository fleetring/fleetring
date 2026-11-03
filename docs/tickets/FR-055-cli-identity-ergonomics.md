---
id: FR-055
title: Pick a client identity without repeating `--cert` and `--key`
status: todo
priority: P2
size: S
milestone: M10 Security
depends_on: [FR-047]
adrs: [ADR-0025]
---

# FR-055: Pick a client identity without repeating `--cert` and `--key`

## Goal

Acting as any principal other than the default `client.crt` means passing `--cert tls/client.<name>.crt --key tls/client.<name>.key` on every command, as the README's application step and a first local test both showed. That is long, easy to get wrong, and invites symlinking keys around. The CLI should take the identity once.

## Scope

- **`--as <principal>` and `FLEETRING_AS`:** resolve to `client.<principal>.crt` and `client.<principal>.key` under `FLEETRING_TLS_DIR`. An unknown principal is an error that names the files it looked for.
- **`FLEETRING_CA`, `FLEETRING_CERT`, `FLEETRING_KEY`:** environment equivalents of `--ca`, `--cert` and `--key`.
- **Precedence:** explicit flag, then environment variable, then `--as`/`FLEETRING_AS`, then the `FLEETRING_TLS_DIR` defaults. `--help` documents the order.
- **The audit line for a refused token names it:** `credential="token:<id>"` whenever the token parsed far enough to have an ID, as the other refusal lines do. Today a revoked token's refusal logs only `credential="token"`.
- **Drain on a node with a database that has no other replica:** say so ("no other replica; the database is unavailable until this node returns"), instead of "it will fail over once this node stops".
- **README:** the application step and the walkthrough use `--as notes-app`.

## Acceptance criteria

- [ ] `fleetringd db query notes "SELECT 1" --as notes-app` acts as `notes-app`, and so does `FLEETRING_AS=notes-app`.
- [ ] The precedence is tested: a flag overrides the environment, and the environment overrides `--as`.
- [ ] A refused revoked token's audit line carries its token ID.
- [ ] Draining a single-replica database logs that it has no other replica.

## Progress

- Not started.
