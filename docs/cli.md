# CLI style guide

How `fleetringd`'s commands look and behave, for anyone adding or changing one. The rules follow [Command Line Interface Guidelines](https://clig.dev/) and Heroku's [12-Factor CLI Apps](https://medium.com/@jdxcode/12-factor-cli-apps-dd3c227a0e46), and the conventions of `gh`, `kubectl`, `cargo`, and `fly` where those agree. Names come from the [lexicon](lexicon.md); a command, flag, column, or JSON field that needs a new word adds it there first.

The CLI is one of three surfaces over one client crate (ADR-0033). The TUI and the desktop app follow the same names, the same confirmation rules, and the same output fields.

## 1. Structure: `fleetringd <noun> <verb>`

```text
fleetringd db create notes --replication-factor 3
fleetringd node drain node-2
fleetringd token revoke --token-id 3q2-…
fleetringd context use home
```

- **Nouns** are lexicon terms, singular: `db`, `node`, `cert`, `token`, `lease`, `context`, `sink`, `archive`. `db` is the one abbreviation, and stays because it is short and universal.
- **Verbs** come from a small set, reused across nouns:

  | Verb | Means | Destructive |
  |---|---|---|
  | `list` | every item the caller may see, as a table | no |
  | `show` | one item in detail (`db locate` predates this rule and stays) | no |
  | `create`, `add`, `issue`, `init` | make something new | no |
  | `set-<field>` | change one setting (`db set-durability`) | no |
  | `watch` | follow a stream until interrupted | no |
  | `verify` | check, and report only | no |
  | `drain` | move work off, keeping data | asks |
  | `drop`, `remove`, `revoke` | delete for good | asks |

- **Top-level commands** are few: `status`, `ui`, `completions`, `upgrade`, and the daemon itself (`fleetringd --config <file>`). A new one needs a reason in its pull request.
- **Arguments** are for the thing the command acts on (`db drop <database>`); everything else is a flag. At most one positional kind per command, plus SQL and its parameters for `db exec` and `db query`.
- **No aliases.** A command or flag has one spelling; a rename replaces it outright ([renames](#14-renames)).

## 2. Flags

- **Long flags, kebab-case:** `--replication-factor`, `--min-replicas`. A flag's name is a lexicon term or a field of one.
- **One flag, one kind of value, everywhere.** `--node` takes a node ID in every command; an address is `--address <host:port>`. A command that dials one node takes either: `--node` looks the address up in the control plane's membership, and `--address` needs nothing else.
- **Short flags** only for the universal ones: `-h`, `-V`, `-o`, `-q`, `-v`. No other single letters, so they never collide.
- **Booleans** are presence flags (`--yes`, `--insecure-plaintext`). A `--no-<x>` form exists only when the default is on.
- **Durations** are `<n>d`, `<n>h`, `<n>m`, or `<n>s`, as `--ttl` already takes them. Sizes are bytes with an optional `KiB`, `MiB`, or `GiB`.
- **Lists** repeat the flag (`--scope read:a --scope write:b`); a comma-separated list is accepted only where values never hold commas.

## 3. Configuration and precedence

A setting is resolved in this order, first match wins:

1. **The flag** on the command line.
2. **The environment variable**, named `FLEETRING_<FLAG>` in upper snake case: `--token` is `FLEETRING_TOKEN`, `--tls-dir` is `FLEETRING_TLS_DIR`.
3. **The context**: the one `--context` or `FLEETRING_CONTEXT` names, else the current one (FR-056).
4. **The built-in default**, if there is a safe one. If there isn't, the command fails and says which flag, variable, or context field would supply it.

Identity has one extra step, from FR-055: within the steps above, an explicit `--ca`, `--cert`, or `--key` (or `FLEETRING_CA`, `FLEETRING_CERT`, `FLEETRING_KEY`) wins over `--as <principal>` (or `FLEETRING_AS`, or the context's `as` field), which picks `client.<principal>.crt` and `client.<principal>.key` from the TLS directory, which wins over the TLS directory's `client.crt` and `client.key`. `--help` states the order for every flag it applies to.

`--config <file>` reads a *daemon's* configuration file, and the commands take only the control address from it. It is a convenience on a node, not a client configuration; contexts replace it for everything else.

## 4. Contexts

A context names everything a command needs to reach one fleet as one principal, so the flags aren't repeated (FR-056). The model is `kubectl config` and `docker context`.

```sh
fleetringd context add home --fleet home --control 10.0.0.2:7400 --tls-dir ~/.config/fleetring/tls/home --as admin
fleetringd context use home
fleetringd context list
fleetringd db list                 # uses the current context
fleetringd db list --context lab   # or another one, for one command
```

- **Stored** in `$XDG_CONFIG_HOME/fleetring/contexts.toml` (`~/.config/fleetring/` when unset, on macOS too, as `gh` does; `%APPDATA%\fleetring\` on Windows), written with mode `0600`.
- **Holds paths and names, never secrets:** the fleet name, control addresses, the TLS directory or the `ca`, `cert`, and `key` paths, and the default principal (`as`). Never a token or a private key's contents. A context may name a command that prints a token (`token_command`), as `kubectl`'s exec credentials do.
- **Checked on use:** a context whose fleet name doesn't match the certificates it points at fails with both names.
- **Shown:** `context list` marks the current one; every command's `--verbose` output names the context it used.

## 5. Output

### Human output by default

- **Tables** for lists, with upper-case headers from the lexicon (`NAME`, `ENGINE`, `EPOCH`, `PRIMARY`), aligned columns, and no borders, as `kubectl get` and `gh pr list` print them. One row per item, so `grep` and `wc -l` work.
- **Key and value lines** for one item, as `db locate` prints them.
- **A summary line** after a table only where it adds something (`3 databases; primaries per node: …`), and never in machine output.
- **Results go to stdout; everything else to stderr:** progress, warnings, prompts, and logs. A command's stdout is its answer and nothing else.

### Machine output: `-o json` and `--json`

- **Every read command** takes `-o, --output table|json|jsonl`, and `--json` is short for `-o json`. A `watch` command's machine format is `jsonl`, one event per line, flushed as each arrives.
- **Fields are stable.** Names are snake_case lexicon terms (`replication_factor`, `lease_epoch`, `membership_epoch`, `primary`, `followers`). Within a minor release, fields are only added, never renamed, retyped, or removed; a removal is a breaking change, listed in the release notes. The same structs serialize for the CLI, the TUI's debug view, and the desktop app (ADR-0033).
- **Values are explicit:** enums are lower-case lexicon words (`live`, `down`, `primary`, `quorum`); times are RFC 3339 in UTC; durations are integer milliseconds in fields ending `_ms`; positions and hashes are the strings the human output prints; an absent value is `null`, never `""` or `"none"`.
- **Lists are objects**, `{"databases": […]}`, so a field can be added beside the list later.
- **Errors in machine mode** still go to stderr as text, and the exit code says what happened; stdout stays empty or valid JSON.

### Quiet when piped

When stdout isn't a terminal: no colour, no progress or spinners, no pager, no prompts, and no hints that only make sense to a person. `-q, --quiet` drops everything but the result and errors; a command with no result then prints nothing.

### Colour

- **Off** when `NO_COLOR` is set to anything non-empty, when `TERM=dumb`, when the stream isn't a terminal, or with `--color never`; `--color always` forces it.
- **The 16 ANSI colours only**, never 256-colour or RGB values, so output inherits the terminal's theme, as the TUI does (ADR-0033). Colour adds emphasis and is never the only signal: `down` is red *and* says `down`.
- Log lines on stderr follow the same rules.

## 6. Errors

An error states what went wrong, with the values involved, and the next step:

```text
fleetringd: database 'notes' doesn't exist on fleet 'home'
  hint: list databases with `fleetringd db list`, or create it with `fleetringd db create notes`
```

- **Format:** `fleetringd: <problem>` on the first line, lower-case, no trailing period; `caused by:` lines for the chain; then at most one `hint:` line with a command to run.
- **Name the input:** the flag, value, file, or principal at fault, and the file paths looked at (FR-055's unknown principal names the files it looked for).
- **Refusals** name the action and the database the request needed, never the caller's scopes (ADR-0025).
- **No stack traces or debug formatting** unless `RUST_LOG` asks for them; no panics on bad input.
- **Retryable errors say so** (`the control plane is unavailable; retry, or check fleetringd status`), and exit with the retryable code.

## 7. Confirmation and `--yes`

- **Destructive commands ask first** on a terminal: dropping a database, removing a member, revoking every token of a signing key, re-initializing an authority. The prompt names what will be lost: `drop database 'notes' and every replica's copy? type the name to confirm:`. Irreversible actions ask for the name typed, not `y`.
- **`--yes` skips the question.** Without a terminal and without `--yes`, the command refuses with exit code 2 and says to pass `--yes`.
- **`--dry-run`** shows what would change, where the command can know: a drain lists the databases it would hand over.
- **Drain asks too** when the node holds a database's only replica, since that database is then unavailable until the node returns. FR-055 adds the warning, and FR-056 the question.

## 8. Exit codes

Documented in `--help` and the man page, and the same for every command:

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | The command failed: a missing database, a failed precondition, or a failed check (replicas that disagree in `db verify`). |
| 2 | Usage: a bad flag or argument, or a confirmation needed without a terminal. |
| 3 | Authentication or authorization: `UNAUTHENTICATED` or `PERMISSION_DENIED`. |
| 4 | Unavailable, and a retry may succeed: `UNAVAILABLE`, a timeout, no leader yet. |
| 130 | Interrupted (Ctrl-C). A `watch` ends this way normally. |

## 9. Help, completions, and man pages

- **`--help` is the reference:** every flag with its environment variable, default, and precedence; one example per command; the exit codes. `-h` prints the short form.
- **Completions** from `fleetringd completions bash|zsh|fish|powershell`, generated with `clap_complete`, including dynamic values where cheap (context names; not database names, which need a network call).
- **Man pages** generated with `clap_mangen` at build time and shipped in the release archives, one per subcommand (`fleetringd-db-create(1)`).
- **The README and the daemon README** show commands only in forms `--help` documents.

## 10. Stdin and stdout

- **`-` means stdin** for any argument that takes a file or a value that may be long: `fleetringd db exec notes -` reads one statement from stdin.
- **Read stdin only when told to**, never because it isn't a terminal, so a command in a pipeline doesn't hang or swallow input.
- **Streams are line-oriented:** one event per line, flushed per event, and a closed stdout (`| head`) ends the command quietly with code 0.
- **Output that holds a secret** (`token issue`) prints the secret alone on stdout, so `$(…)` captures exactly it, and nothing about it on stderr.

## 11. Secrets

- **Never require a secret on the command line,** where `ps`, shell history, and CI logs see it. Every flag that takes a secret has an environment variable, a file form, and stdin: `--token`, `FLEETRING_TOKEN`, `--token-file <path>`, `--token-file -`.
- **Prefer paths to contents** for keys: `--key <path>`, never key material in a flag.
- **Warn** when a secret arrives on argv: `warning: --token on the command line is visible to other processes; use FLEETRING_TOKEN or --token-file`.
- **Never print or log a secret,** in errors, `--verbose`, debug logs, or audit lines; print an ID instead (`token:<id>`).
- **Files with secrets** are written `0600`, and a command that reads one warns when it is group- or world-readable.

## 12. Long-running commands

- **Progress** goes to stderr, and only on a terminal.
- **Ctrl-C** stops cleanly: a drain stops after the database it is handing over, and says what it moved and what it left.
- **Timeouts** have a flag (`--timeout 30s`) and a stated default.

## 13. Logging

Commands log only warnings by default, to stderr; `RUST_LOG` raises it, and `-v, --verbose` is short for `RUST_LOG=fleetringd=debug`. The daemon logs at info. Log fields use lexicon names.

## 14. Renames

Before 1.0, a renamed flag, value, or command is a clean break: the old spelling stops working in the release that renames it, with no hidden alias and no deprecation period (ADR-0032). The old spelling fails like any unknown flag, command, or value, and the error may carry one `hint:` line naming the new spelling. The release notes list every rename under breaking changes (FR-062 keeps 0.3.0's list).

How renames work after 1.0 is decided with 1.0's compatibility promise.

## Where `fleetringd` stands

As of the 0.3.0 renames (FR-059, FR-062). "Ticket" is where the gap closes.

| Rule | Today | Ticket |
|---|---|---|
| `noun verb` structure | Yes: `db create`, `node drain`, `cert init`, `token issue`, `lease watch`. | Done (FR-062) |
| Names from the lexicon | Yes: `--fleet`, the `fleet` scope, and "fleet" in help and logs. | Done (FR-059, FR-062) |
| One flag, one kind of value | Yes: `--node` takes a node ID in `db list`, `status`, `lease watch`, `db exec`, and `db query`; `--address` takes `host:port`; `cert node --addresses` takes the addresses a node is dialed at. | Done (FR-062) |
| Precedence: flag, env, context, default | Flag, then environment variable, then `--as` (or `FLEETRING_AS`), then the `FLEETRING_TLS_DIR` defaults, for `--ca`, `--cert`, and `--key` (`FLEETRING_CA`, `FLEETRING_CERT`, `FLEETRING_KEY`); `--token` has `FLEETRING_TOKEN`. No context step. | FR-056 |
| Contexts | None; every command takes `--control` or `--config`, and TLS flags. | FR-056 |
| `--json` / `-o` with stable fields | None; human output only. | FR-056 |
| Human tables by default | `db list` prints an aligned table with upper-case headers; `status` and `db locate` print key and value lines. | Done |
| Quiet when piped | Commands print no progress or colour; logs are warnings only. No `--quiet`. | FR-056 |
| `NO_COLOR` | Results have no colour. Log lines on stderr use ANSI colour, honour `NO_COLOR`, but don't check for a terminal. | FR-056 |
| Errors with the next step | `fleetringd: <error>` with `caused by:` lines; some say what to pass (`pass --control or --config`). One `hint:` line so far, when `--node` is given an address. | FR-056 |
| Confirmation and `--yes` | `db drop` asks on a terminal and refuses elsewhere without `--yes`. `node drain` and `token revoke --key-id` don't ask; the draining node logs `no other replica; the database is unavailable until this node returns` for a database that has none (FR-055). | FR-056 |
| Documented exit codes | 0, 1, and clap's 2 for usage; not documented. | FR-056 |
| Completions and man pages | None. | FR-056 |
| Stdin and stdout | Results on stdout, logs on stderr; `token issue` prints the token alone. No `-` for stdin. | FR-056 |
| No secrets on argv | `--token` is accepted on argv, with `FLEETRING_TOKEN` as the alternative; no `--token-file`, no warning. Keys are always paths. | FR-056 |
| Renames | Clean breaks, listed in FR-062's breaking changes for 0.3.0. | Done |
