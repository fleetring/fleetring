---
id: FR-056
title: CLI contexts, `--json` on every read command, shell completions, and man pages
status: todo
priority: P1
size: L
milestone: M11 Operator experience
depends_on: [FR-055, FR-059]
adrs: [ADR-0033]
---

# FR-056: CLI contexts, `--json` on every read command, shell completions, and man pages

## Goal

Every command today needs `--control` or `--config`, and the TLS flags or `FLEETRING_TLS_DIR`, and prints only human text. Scripts parse that text, and people repeat the same five flags. This ticket brings the CLI to the [CLI style guide](../cli.md): contexts so a command needs no connection flags, stable JSON on every read command, documented exit codes, completions, and man pages. It also creates the `fleetring-client` crate (ADR-0033), since contexts and the JSON types belong to every surface, not to the CLI.

## Scope

- **The `fleetring-client` crate** (ADR-0033, decision 1):
  - Move `data::Client`, the control client's redirect handling, and identity loading out of `fleetringd` into it, and switch the CLI to it.
  - Typed results for every read command, deriving `serde::Serialize`, with snake_case lexicon field names.
  - Decide in this ticket whether the daemon's own control client moves too, and record it in ADR-0033's consequences.
- **Contexts** (`fleetringd context add|use|list|show|remove`):
  - A context holds a fleet name, one or more control addresses, a TLS directory or explicit `ca`, `cert`, and `key` paths, a default principal (`as`), and optionally a `token_command`. Never a token or key contents.
  - Stored in `$XDG_CONFIG_HOME/fleetring/contexts.toml` (`~/.config/fleetring/` by default, on macOS too; `%APPDATA%\fleetring\` on Windows), written `0600`.
  - Selected by `--context <name>`, then `FLEETRING_CONTEXT`, then the current context.
  - On use, the context's fleet name must match the fleet its certificates name; a mismatch names both.
- **Precedence**, as the style guide's section 3 sets it: flag, then environment variable, then context, then default. FR-055's `--as` and `FLEETRING_AS` fit in as one level of the identity: an explicit `--ca`, `--cert`, or `--key` (or its environment variable) wins over `--as` (or `FLEETRING_AS`, or the context's `as`), which wins over the TLS directory's `client.crt` and `client.key`. `--help` states it.
- **`-o, --output table|json|jsonl` and `--json`** on every read command: `status`, `status --node`, `db list`, `db locate`, `db verify`, `db query`, `context list`, and the lease watch (`jsonl`). The fields follow the style guide's rules; a test pins each command's JSON shape, so a rename fails CI.
- **`lease watch` and `--address`** landed in FR-062: `lease watch` replaced `watch-leases`, `--node` takes a node ID everywhere, and a command that dials a node directly takes `--address <host:port>`, with no aliases (ADR-0032, as revised). This ticket adds JSON output to them like the rest.
- **Exit codes** 0, 1, 2, 3, 4, and 130 as the style guide's table defines them, mapped from gRPC status codes in one place, and listed in `--help`.
- **Errors with hints:** a `hint:` line for the common failures (no control address, no identity, unknown database, refused scope, unavailable control plane).
- **Confirmation:** `token revoke --key-id` asks on a terminal, `node drain` asks when the node holds a database's only replica, and both refuse without a terminal unless `--yes`.
- **Secrets:** `--token-file <path>` and `--token-file -`, and a warning when `--token` is given on the command line.
- **Quiet and colour:** `-q, --quiet`; `--color auto|always|never`; log lines use colour only when stderr is a terminal and `NO_COLOR` is unset.
- **Completions and man pages:** `fleetringd completions bash|zsh|fish|powershell` with `clap_complete`, context names completed dynamically; man pages with `clap_mangen`, generated at build time, included in the release archives, and listed in `RELEASING.md`'s archive contents.
- **Docs:** both READMEs' walkthroughs use a context instead of repeated flags, and the style guide's "where `fleetringd` stands" table is updated.
- **Not in scope:** the TUI (FR-057) and the desktop app (FR-058); a pager; YAML output.

## Acceptance criteria

- [ ] `fleetring-client` exists, the CLI uses it, and no CLI command builds a connection outside it.
- [ ] After `fleetringd context add home … && fleetringd context use home`, `fleetringd db list` works with no other flags, and `--context`, `FLEETRING_CONTEXT`, a flag, and an environment variable each override as the precedence says, with a test for each step.
- [ ] `--as`, `--cert`/`--key`, and their environment variables resolve in the documented order when a context also names a principal.
- [ ] Every read command's `--json` output parses, and a snapshot test pins its fields.
- [ ] `lease watch -o jsonl` prints one event per line, flushed per event, and exits 0 when stdout closes.
- [ ] A refused request exits 3, an unavailable control plane exits 4, a usage error exits 2, and each is tested.
- [ ] `fleetringd completions zsh` produces a script that completes subcommands and context names, and the release archives contain the man pages.

## Progress

- Not started.
