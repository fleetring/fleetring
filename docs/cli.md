# CLI style guide

This guide covers how `fleetring`'s commands look and behave, for anyone adding or changing one. The rules follow:

- [Command Line Interface Guidelines](https://clig.dev/);
- Heroku's [12-Factor CLI Apps](https://medium.com/@jdxcode/12-factor-cli-apps-dd3c227a0e46);
- the conventions of `gh`, `kubectl`, `cargo`, `docker` and `fly`, where those agree.

Names come from the [lexicon](lexicon.md). A command, flag, column or JSON field that needs a new word adds it there first.

The CLI is one of three surfaces over one client crate (ADR-0033). The TUI and the desktop app follow the same names, the same confirmation rules and the same output fields.

This guide describes the 0.7.0 CLI (ADR-0043). It is updated before the code, as #103 asks. [Where `fleetring` stands](#where-fleetring-stands) lists what isn't built yet.

## 1. Structure: `fleetring <noun> <verb>`

```text
fleetring db create notes --replication-factor 3
fleetring node drain node-2
fleetring token revoke --token-id 3q2-…
fleetring switch home
```

### The binary and its compatibility name

There is one binary, `fleetring`. `fleetringd` is the same file under a second name: a symbolic link, or a copy on Windows. It is kept until 1.0 so that service units and scripts keep working (ADR-0043, point 1). Two things depend on the name it runs as:

- **The name it prints.** It prints that name in `--version`, usage lines and errors.
- **0.6's form of running a node.** Under either name, `fleetringd --config <file> [daemon flags]`, with no verb, is read as `serve`. The daemon then logs a warning that the form goes in 1.0.

Docs, examples, the site and help text use `fleetring` only. The crate and package stay `fleetringd`, so `cargo install --git … fleetringd` installs `fleetring`.

### The command tree

Groups are the headings `fleetring -h` prints them under (§9).

| Group | Command | What it does |
|---|---|---|
| Getting started | `dev [-d] [--data-dir <dir>] [--port <n>]` | Runs a dev node on loopback (ADR-0039); `-d` runs it in the background |
| | `dev stop \| status \| logs [-f]` | Stops, describes, or shows the log of the background dev node |
| | `serve --config <file> [daemon flags]` | Runs a node |
| | `stop (--data-dir <dir> \| --config <file>)` | Stops a node started in a terminal or script: drains, leaves, waits |
| | `switch [<name> \| -]` | Makes a context current; with no name, a picker; `-` goes back |
| | `context list \| show \| current \| add \| use \| remove` | Manages contexts (§4) |
| | `completions bash\|zsh\|fish\|powershell` | Prints a shell completion script |
| Databases | `db create \| list \| show \| edit \| locate \| exec \| query \| verify \| restore \| drop` | Databases. `db restore` is the online restore into the fleet |
| Fleet | `status [--node <id> \| --address <host:port>]` | Gives the fleet overview, or one data node's view |
| | `node list \| drain \| undrain \| remove` | Members, with liveness, release, and drain state |
| | `orphan list \| remove` | Copies a data node keeps that the control plane doesn't know (ADR-0042) |
| | `upgrade [--check \| --rollback \| --version <v>]` | Installs another release in place (ADR-0038) |
| | `lease watch` | Follows the leases a data node holds |
| Security | `cert init \| node \| client` | Creates the authority and issues certificates |
| | `token issue \| revoke` | Issues and revokes bearer tokens |
| Agents | `mcp [--agent <id>] [--read-only]` | Serves the Model Context Protocol on stdio as one principal (ADR-0041) |
| | `agent add \| list \| show \| renew \| remove` | Manages agent identities: a certificate and a scoped token per agent (ADR-0043, point 8) |
| Recovery | `archive show \| verify \| restore` | Archives. `archive restore` is the offline restore into a directory, with no fleet |

### Nouns, verbs and top-level commands

- **Nouns** are lexicon terms, singular: `db`, `node`, `cert`, `token`, `lease`, `context`, `agent`, `archive`, `orphan`. `db` is the one abbreviation, and stays because it is short and universal.
- **Verbs** come from a small set, reused across nouns:

  | Verb | Means | Destructive |
  |---|---|---|
  | `list` | every item the caller may see, as a table | no |
  | `show` | one item in detail: `db show`, placement included (`db locate` predates this rule and stays, for placement alone) | no |
  | `create`, `add`, `issue`, `init` | make something new | no |
  | `edit` | change an item's settings, any number at once (`db edit`: description, labels, expiry, durability) | no |
  | `use` | make an item the current one (`context use`) | no |
  | `renew` | reissue an item's credentials before they expire (`agent renew`) | no |
  | `watch` | follow a stream until interrupted | no |
  | `verify` | check, and report only | no |
  | `restore` | build a new item from an archive, never over an existing one | no |
  | `drain` | move work off, keeping data | asks |
  | `undrain` | clear a drain mark, so the node takes work again | no |
  | `stop` | end a running process cleanly (`stop`, `dev stop`) | no |
  | `drop`, `remove`, `revoke` | delete for good | asks; `context remove` doesn't, as it deletes only paths |

- **Top-level commands are few.** A new one needs a reason in its pull request. Today's, besides the nouns:
  - **`status`**, the fleet overview.
  - **`dev`, `serve` and `stop`, which run or stop a node.** Each acts on a process, not on a fleet noun.
  - **`switch`.** It is the one documented shortcut: `switch <name>` is `context use <name>`, plus a picker and `-`. No other command has a second spelling.
  - **`upgrade`**, which replaces this binary.
  - **`mcp`, a long-running process on stdio** (ADR-0041, point 8). It acts on many nouns, so it belongs under none.
  - **`completions`** and **`ui`** (*planned*, FR-057).
- **Arguments** are for the thing the command acts on (`db drop <database>`, `archive restore <database>`, `agent add <id>`). Everything else is a flag. A command takes at most one positional kind, plus SQL and its parameters for `db exec` and `db query`.
- **No aliases.** A command or flag has one spelling; a rename replaces it outright ([renames](#14-renames)). The exceptions are the compatibility names above, which are documented in `--help` and go in 1.0, and `switch`.

## 2. Flags

- **Long flags, kebab-case:** `--replication-factor`, `--min-replicas`. A flag's name is a lexicon term or a field of one.
- **One flag, one kind of value, everywhere.** `--node` takes a node ID in every command; an address is `--address <host:port>`. A command that dials one node takes either: `--node` looks the address up in the control plane's membership, and `--address` needs nothing else.
- **Short flags** only for the universal ones: `-h`, `-V`, `-o`, `-q` and `-v`. There are two more, with the meaning `docker` gives them: `-d` (`--detach`, on `dev`) and `-f` (`--follow`, on `dev logs`). No other single letters, so they never collide.
- **Booleans** are presence flags (`--yes`, `--insecure-plaintext`). A `--no-<x>` form exists only when the default is on (`agent remove --no-revoke`).
- **Durations** are `<n>d`, `<n>h`, `<n>m`, or `<n>s`, as `--ttl`, `--timeout` and `--expires-in` take them. Times are RFC 3339, as `--expires-at` and `--at-time` take them. Sizes are bytes with an optional `KiB`, `MiB`, or `GiB`.
- **Lists** repeat the flag (`--scope read:a --scope write:b`, `context add --control a:7400 --control b:7400`). A comma-separated list is accepted only where values never hold commas.

## 3. Configuration and precedence

A client command resolves each setting on its own, and the first match wins:

1. **The flag** on the command line.
2. **The environment variable**, named `FLEETRING_<FLAG>` in upper snake case: `--token` is `FLEETRING_TOKEN`, `--tls-dir` is `FLEETRING_TLS_DIR`, `--agent` is `FLEETRING_AGENT`.
3. **The context `--context <name>` names**, or else the one `FLEETRING_CONTEXT` names.
4. **The current context.**
5. **The built-in default**, if there is a safe one. If there isn't, the command fails, and its hint names a flag, a variable and a context field that would supply it, or `fleetring dev -d` when nothing is set up at all.

Steps 3 and 4 select one context. Fields are never merged from two contexts.

| Setting | Flag | Variable | Context field |
|---|---|---|---|
| Control address | `--control <host:port>`, or `--config <file>` (a daemon's file, for its control address) | `FLEETRING_CONTROL` | `control` (a list, tried in order) |
| TLS directory | `--tls-dir <dir>` | `FLEETRING_TLS_DIR` | `tls_dir` |
| Authority certificate | `--ca <file>` | `FLEETRING_CA` | `ca` |
| Client certificate and key | `--cert <file>`, `--key <file>` | `FLEETRING_CERT`, `FLEETRING_KEY` | `cert`, `key` |
| Principal | `--as <principal>` | `FLEETRING_AS` | `as` |
| Agent identity | `--agent <id>` | `FLEETRING_AGENT` | none; found under the context's agents directory |
| Token | `--token <t>`, `--token-file <file \| ->` | `FLEETRING_TOKEN`, `FLEETRING_TOKEN_FILE` | `token_file` |
| Context | `--context <name>` | `FLEETRING_CONTEXT` | the file's `current` |

**Identity, per file.** The certificate, its key and the authority each resolve on their own. At each step, an explicit path (`--cert`, `FLEETRING_CERT`, the context's `cert`) wins over a principal at the same step (`--as`, `FLEETRING_AS`, the context's `as`). A principal picks `client.<principal>.crt` and `.key` under the TLS directory. At the last step, the TLS directory's `client.crt` and `client.key` are used. `--agent <id>` is shorthand, at the flag step, for `--tls-dir <agents dir>/<id> --as agent-<id> --token-file <agents dir>/<id>/token`, and it conflicts with the flags it stands for. `--help` states the order for every flag it applies to.

**Overrides are visible.** When a context is in use and an environment variable overrides one of its fields, the command prints one warning per variable on stderr, as `warning: FLEETRING_CONTROL overrides context 'home' (control 10.0.0.2:7400)`. It does so only when stderr is a terminal and `-q` isn't given. A shell that sourced dev mode's `env` file is the usual cause. `--verbose` names the context used and the source of each setting.

**Daemon configuration isn't client configuration.** `--config <file>` reads a *daemon's* configuration file, and client commands take only the control address from it. It is a convenience on a node. Contexts replace it everywhere else. `serve`, `dev` and `stop` never read contexts, though `dev` writes one.

## 4. Contexts

A context names everything a command needs to reach one fleet as one principal, so the flags aren't repeated. The models are `kubectl config`, `docker context` and kubectx.

```sh
fleetring dev -d                    # writes the dev context, and makes it current if none is
fleetring db list                   # uses the current context
fleetring context add home --control 10.0.0.2:7400 --tls-dir ~/.config/fleetring/tls/home --as ops
fleetring switch home               # or: fleetring context use home
fleetring switch -                  # back to dev
fleetring switch                    # a picker, on a terminal
fleetring db list --context home    # another context, for one command
PS1='$(fleetring context current) $ '
```

- **Stored** in `contexts.toml` under `$XDG_CONFIG_HOME/fleetring/`. That is `~/.config/fleetring/` when the variable is unset, on macOS too, as `gh` does, and `%APPDATA%\fleetring\` on Windows. The directory is `0700` and the file `0600`. Writes take a lock and replace the file atomically.
- **Holds paths and names, never secrets:** the fleet name, the control addresses, the TLS directory or the `ca`, `cert` and `key` paths, the default principal (`as`), and a `token_file`. It never holds a token, a private key's contents, or a command to run.
- **The format** is TOML with `version = 1`, `current`, `previous`, and one `[contexts.<name>]` table per context (ADR-0043, point 4.3). Unknown keys are refused. Names are lowercase letters, digits, `-`, `_` and `.`.
- **Commands:**
  - `context list` marks the current context in a `CURRENT` column.
  - `context show [<name>]` shows one, the current one by default.
  - `context current` prints just the name, for prompts. It does no network I/O, and exits 1 with no output when none is current.
  - `context add` infers `--fleet` from the certificate. The first context added becomes current.
  - `context use <name>` and `switch <name>` record the context they replace as `previous`, for `switch -`.
  - `context remove` doesn't ask.
- **The picker.** `switch` with no name opens a picker when stdin and stderr are terminals: arrows, type to filter, the current context marked and preselected, Esc to cancel. Without a terminal it prints `context list` on stderr and exits 2, with a hint to pass a name.
- **The dev context.** `fleetring dev` writes a context named `dev` on every start (control `127.0.0.1:<port>`, the dev directory's `tls/`, `as = "admin"`), and makes it current unless another context is current. It rewrites only a `dev` context it wrote itself, and its banner says `context: dev (current)` or how to switch to it.
- **Checked on use.** A context whose fleet name doesn't match the fleet its certificate names fails with both names.

## 5. Output

### Human output by default

- **Tables** for lists, with upper-case headers from the lexicon (`NAME`, `ENGINE`, `EPOCH`, `PRIMARY`), aligned columns and no borders, as `kubectl get` and `gh pr list` print them. One row per item, so `grep` and `wc -l` work.
- **Key and value lines** for one item, as `db locate` prints them.
- **A summary line** after a table only where it adds something (`3 databases; primaries per node: …`), and never in machine output.
- **Results go to stdout; everything else to stderr:** progress, spinners, check marks, warnings, prompts and logs. A command's stdout is its answer and nothing else, so `$(…)` captures the same bytes whether or not a terminal is attached.

### Machine output: `-o json` and `--json`

- **Every read command** takes `-o, --output table|json|jsonl`, and `--json` is short for `-o json`. A `watch` command's machine format is `jsonl`, one event per line, flushed as each arrives.
- **Fields are stable.** Names are snake_case lexicon terms (`replication_factor`, `lease_epoch`, `membership_epoch`, `primary`, `followers`). Within a minor release, fields are only added, never renamed, retyped or removed. A removal is a breaking change, listed in the release notes. The same structs serialize for the CLI, the TUI's debug view and the desktop app (ADR-0033).
- **Values are explicit:**
  - enums are lower-case lexicon words (`live`, `down`, `primary`, `quorum`);
  - times are RFC 3339 in UTC;
  - durations are integer milliseconds, in fields ending `_ms`;
  - positions and hashes are the strings the human output prints;
  - an absent value is `null`, never `""` or `"none"`.
- **Lists are objects**, `{"databases": […]}`, so a field can be added beside the list later.
- **Errors in machine mode** still go to stderr as text, and the exit code says what happened. stdout stays empty or valid JSON.

### Terminals, pipes and CI

Each of these is decided once, from a snapshot of the process's surroundings, by one tested function:

| Feature | Shown only when |
|---|---|
| Colour | the stream is a terminal, `NO_COLOR` is unset or empty, `TERM` isn't `dumb`, and `--color` isn't `never`. `--color always` forces it |
| Spinners and check marks | stdout and stderr are both terminals; the output is human (not `-o json`, `jsonl`, or `--json`); there's no `-q`; `NO_COLOR` is unset or empty; `TERM` isn't `dumb`; and `CI` is unset or empty |
| Prompts and the `switch` picker | stdin and stderr are terminals. Otherwise a command that must ask refuses with exit 2 (§7), and `switch` with no name prints the list and exits 2 |
| Override warnings and `hint:` lines meant for people | stderr is a terminal, and there's no `-q` |

`-q, --quiet` drops everything but the result and errors. A command with no result then prints nothing.

### Colour

- **The 16 ANSI colours only**, never 256-colour or RGB values, so output inherits the terminal's theme, as the TUI does (ADR-0033). Colour adds emphasis and is never the only signal: `down` is red *and* says `down`.
- Log lines on stderr follow the same rules. A detached dev node's log file never holds colour.

## 6. Errors

An error states what went wrong, with the values involved, and the next step:

```text
fleetring: database 'notes' doesn't exist on fleet 'home'
  hint: list databases with `fleetring db list`, or create it with `fleetring db create notes`
```

- **Format:**
  - `fleetring: <problem>` on the first line, lower-case, no trailing period, with the name the binary was run as;
  - `caused by:` lines for the chain;
  - then at most one `hint:` line with a command to run.
- **Name the input:** the flag, value, file, context or principal at fault, and the file paths looked at (an unknown principal names the files it looked for, and an agent with no identity names its directory).
- **Refusals** name the action and the database the request needed, never the caller's scopes (ADR-0025).
- **No stack traces or debug formatting** unless `RUST_LOG` asks for them; no panics on bad input.
- **Retryable errors say so** (`the control plane is unavailable; retry, or check fleetring status`), and exit with the retryable code.

## 7. Confirmation and `--yes`

- **Destructive commands ask first** on a terminal. That covers dropping a database, removing a member, removing an orphan copy, removing an agent, revoking every token of a signing key, and re-initializing an authority. The prompt names what will be lost: `drop database 'notes' and every replica's copy? type the name to confirm:`. Irreversible actions ask for the name typed, not `y`.
- **`--yes` skips the question.** Without a terminal and without `--yes`, the command refuses with exit code 2 and says to pass `--yes`.
- **`--dry-run`** shows what would change, where the command can know: a drain lists the databases it would hand over.
- **Drain asks too** when the node holds a database's only replica, since that database is then unavailable until the node returns.

## 8. Exit codes

Documented in `--help` and the man page, and the same for every command:

| Code | Meaning |
|---|---|
| 0 | Success. `dev -d` when the dev node already runs, `dev status` either way. |
| 1 | The command failed: a missing database, a failed precondition, a failed check (replicas that disagree in `db verify`), no node to `stop`, or an agent with no identity outside dev mode. |
| 2 | Usage: a bad flag or argument, a confirmation needed without a terminal, or `switch` with no name and no terminal. |
| 3 | Authentication or authorization: `UNAUTHENTICATED` or `PERMISSION_DENIED`. |
| 4 | Unavailable, and a retry may succeed: `UNAVAILABLE`, a timeout, no leader yet. |
| 130 | Interrupted (Ctrl-C). A `watch`, `dev logs -f`, and a cancelled picker end this way normally. |

## 9. Help, completions and man pages

- **`-h` is one screen, `--help` is the reference.**
  - `-h` gives one-line summaries.
  - `--help` gives the full text: every flag with its environment variable, default and precedence, the examples, and the exit codes.
  - Both wrap to the terminal's width, at most 100 columns (clap's `wrap_help`).
- **One-line summaries.** A command's or flag's doc comment starts with one summary line, at most 80 characters, no trailing period. Then comes a blank line, then the details, which only `--help` shows. A test walks the command tree and fails on any summary that breaks the rule.
- **The top level groups commands under headings** in this order: Getting started, Databases, Fleet, Security, Agents, Recovery. §1's table gives the commands in each. clap has no headings for subcommands, so the root's `help_template` omits `{subcommands}`, and the grouped list is rendered from the command tree into `before_help` (ADR-0043, point 7). A test fails if a command is in no group or in two. A new command adds itself to a group.
- **Examples come first:**

  ```text
  Examples:
    fleetring dev -d                   start a dev node in the background
    fleetring switch dev               use it (dev does this for you)
    fleetring db create notes          create a database
    fleetring switch prod              talk to another fleet
  ```

- **Budget:** `fleetring -h` at 100 columns is at most 50 lines, none wider than 100 columns. A snapshot test pins it.
- **The compatibility line** closes the top-level `--help`: `fleetringd is the same program, kept until 1.0; fleetringd --config <file> runs serve.`
- **Completions** come from `fleetring completions bash|zsh|fish|powershell`, generated with `clap_complete`. They include dynamic values where cheap: context names and agent IDs, but not database names, which need a network call.
- **Man pages** are generated with `clap_mangen` at build time and shipped in the release archives, one per subcommand (`fleetring-db-create(1)`).
- **The READMEs, the quickstarts and the site** show commands only in forms `--help` documents, and only as `fleetring`.

## 10. Stdin and stdout

- **`-` means stdin** for any argument that takes a file or a value that may be long: `fleetring db exec notes -` reads one statement from stdin, and `--token-file -` reads the token.
- **Read stdin only when told to**, never because it isn't a terminal, so a command in a pipeline doesn't hang or swallow input. `mcp` is the exception by nature: stdin and stdout carry the protocol.
- **Streams are line-oriented:** one event per line, flushed per event, and a closed stdout (`| head`) ends the command quietly with code 0.
- **Output that holds a secret** (`token issue`) prints the secret alone on stdout, so `$(…)` captures exactly it, and nothing about it on stderr. `agent add --print-config` prints no secret at all: the configuration runs `fleetring mcp --agent <id>`, which finds its own credentials.

## 11. Secrets

- **Never require a secret on the command line,** where `ps`, shell history and CI logs see it. Every flag that takes a secret has an environment variable, a file form and stdin: `--token`, `FLEETRING_TOKEN`, `--token-file <path>`, `--token-file -`.
- **Prefer paths to contents** for keys: `--key <path>`, never key material in a flag. A context holds paths only.
- **Warn** when a secret arrives on argv: `warning: --token on the command line is visible to other processes; use FLEETRING_TOKEN or --token-file`.
- **Never print or log a secret,** in errors, `--verbose`, debug logs or audit lines. Print an ID instead (`token:<id>`).
- **Files with secrets** are written `0600` in directories made `0700`: an agent's key and token, `contexts.toml`, a detached dev node's log. A command that reads one warns when it is group- or world-readable.

## 12. Long-running commands and background processes

- **Progress** is a spinner on stderr, shown after 150 ms and only where §5's table allows. Long-running commands are dev startup, `dev -d`, `dev stop`, `stop`, `db create`, `db restore`, `db verify`, `archive verify`, `archive restore`, `upgrade`, `node drain`, `orphan remove` and `agent add`. On success the spinner becomes `✓ <what happened>`; on failure it is cleared, and the error prints. A command whose steps differ (`upgrade`) changes the spinner's message per step. Log lines printed meanwhile don't tear the spinner's line.
- **Ctrl-C** stops cleanly. A drain stops after the database it is handing over, and says what it moved and what it left.
- **Timeouts** have a flag (`--timeout 30s`) and a stated default: 60s for `dev -d` to be ready, 2m for `stop` to drain, 5m for each `upgrade` step.
- **A background process is a `docker run -d`:**
  - `dev -d` returns once the node is ready, prints its address, data directory and log path on stdout, and is idempotent. The log rotates only when the node starts, so it grows during one long run.
  - The process file records a node `dev -d` started, and `dev stop` always stops it.
  - It is stopped by `dev stop` or `stop`, which send `SIGTERM` through the process file and wait for the drain. They never send `SIGKILL`.
  - A node that systemd or `brew services` runs is stopped by that supervisor. `stop` refuses it, naming the supervisor's command. A node counts as supervised only when its parent is the supervisor, never because of variables a shell inherited.
  - On Windows, `dev -d` and `stop` aren't available yet. Run `fleetring dev` in its own terminal.

## 13. Logging

Commands log only warnings by default, to stderr. `RUST_LOG` raises the level, and `-v, --verbose` is short for `RUST_LOG=fleetringd=debug` (the crate's target, which keeps its name). A node logs at info. Log fields use lexicon names.

`--log-format text|json` (or `FLEETRING_LOG_FORMAT`, or the daemon's `[log] format`) chooses the format, in that order. Text is the default. JSON is one object per line, with the current span and its parents. The flag goes before or after a command's name, as a global flag; it is the one flag nodes and commands share.

## 14. Renames

Before 1.0, a renamed flag, value or command is a clean break: the old spelling stops working in the release that renames it, with no hidden alias and no deprecation period (ADR-0032). The old spelling fails like any unknown flag, command or value, and the error may carry one `hint:` line naming the new spelling. The release notes list every rename under breaking changes (FR-062 keeps 0.3.0's list).

0.6.0 removes `db set-description`, `db set-labels`, `db set-expiry` and `db set-durability` in favour of `db edit` (FR-076). Each fails like an unknown command, with a `hint:` line naming `db edit` and the flags that replace it (`--remove` is now `--remove-label`, and `--never` is `--clear-expiry`).

0.7.0 renames (ADR-0043, point 2):

| 0.6 | 0.7.0 |
|---|---|
| `fleetringd …` | `fleetring …`. `fleetringd` stays as a compatibility name until 1.0 |
| `fleetringd --config <file> [daemon flags]` | `fleetring serve --config <file> [daemon flags]`. The old form stays until 1.0, with a warning |
| `db archive show <db>` | `archive show <db>` |
| `db archive verify <db>` | `archive verify <db>` |
| `archive restore --from <db> --to <dir>` | `archive restore <db> --to <dir>` |
| `--version` prints `fleetringd X.Y.Z` | `fleetring X.Y.Z`, or `fleetringd X.Y.Z` when run under that name |
| errors start `fleetringd:` | `fleetring:`, or `fleetringd:` when run under that name |
| the dev banner's `. ~/.fleetring/env` | the `dev` context. The file is still written, for scripts |
| `ghcr.io/fleetring/fleetringd`, `CMD --config …` | `ghcr.io/fleetring/fleetring`, `CMD serve --config …`. The old image name is pushed too until 1.0 |
| an MCP client runs `fleetringd mcp` with four variables and a token | `fleetring mcp --agent <id>` |

These names don't change in 0.7.0, because a 0.6 binary or an existing directory reads them:

- the `fleetringd-v` release tags and the `fleetringd-<version>-<target>` archives;
- `fleetringd.process`, `fleetringd.lock` and `FLEETRINGD_RESTARTS`;
- `fleetringd.toml`;
- the crate's log targets.

A host upgraded from 0.6 keeps its real file named `fleetringd` and its previous binary as `fleetringd.previous`; only a fresh 0.7 install has `fleetring.previous`.

After switching a unit to `fleetring serve`, rolling back to 0.6 requires reverting the unit to `fleetringd --config` first. A 0.6 binary doesn't know `serve`, and `fleetringd.previous upgrade --rollback`, run as 0.6, can't check for it (ADR-0043, point 3.5).

How renames work after 1.0 is decided with 1.0's compatibility promise, and 1.0 removes the compatibility names.

## Where `fleetring` stands

As of 0.6.0, against this guide. "Plan" points into [`docs/plans/0.7.0-cli.md`](plans/0.7.0-cli.md); FR-056 is #59, FR-075 is #103, and FR-079 is #114.

| Rule | 0.6.0 | Plan |
|---|---|---|
| One binary, `fleetring`, with `fleetringd` as its compatibility name | The binary is `fleetringd`; a node runs from bare `--config`. | Phase A (FR-075) |
| `noun verb` structure | Yes: `db create`, `node drain`, `cert init`, `token issue`, `lease watch`. `db archive show/verify` and the top-level `archive restore --from` split one noun in two. | Done (FR-062); `archive` group in phase E |
| `serve`, `stop`, `dev -d` | None; `dev` runs in the foreground only. | Phases A and C (FR-075) |
| `node list` | `status` lists members; there is no `node list`. | Phase E (FR-075) |
| Names from the lexicon | Yes: `--fleet`, the `fleet` scope, and "fleet" in help and logs. | Done (FR-059, FR-062) |
| One flag, one kind of value | Yes: `--node` takes a node ID in `db list`, `status`, `lease watch`, `db exec` and `db query`; `--address` takes `host:port`; `cert node --addresses` takes the addresses a node is dialed at. | Done (FR-062) |
| Precedence: flag, env, context, default | Flag, then environment variable, then `--as` (or `FLEETRING_AS`), then the `FLEETRING_TLS_DIR` defaults, for `--ca`, `--cert`, and `--key`; `--token` has `FLEETRING_TOKEN`; `--control`, then `--config`, then `FLEETRING_CONTROL`. No context step and no `--tls-dir` flag. | Phase B (FR-056) |
| Contexts and `switch` | None. `fleetringd dev` writes `<dir>/env`, which sets the variables for its node (ADR-0039). | Phase B (FR-056) |
| Agents | Six manual steps (the agent quickstart, step 2); MCP configurations carry a token. | Phase F (FR-079) |
| `--json` / `-o` with stable fields | `db show` and `orphan list` only; every other command prints for people only. | Phase G (FR-056) |
| Human tables by default | `db list` prints an aligned table with upper-case headers; `status` and `db locate` print key and value lines. | Done |
| Terminals, pipes and CI | Commands print no progress or colour; logs are warnings only. No `--quiet`. | Phases D and G |
| Spinners | None. | Phase D (FR-075) |
| `NO_COLOR` | Results have no colour. Log lines on stderr use ANSI colour and honour `NO_COLOR`, but don't check for a terminal. | Phase C (detached logs), phase G |
| Errors with the next step | `fleetringd: <error>` with `caused by:` lines; `hint:` lines in a few places. | Phase G (FR-056) |
| Confirmation and `--yes` | `db drop`, `node remove` and `orphan remove` ask on a terminal and refuse elsewhere without `--yes`. `node drain` and `token revoke --key-id` don't ask. | Phase G (FR-056) |
| Documented exit codes | 0, 1, and clap's 2 for usage; not documented. | Phase G (FR-056) |
| Help fits one screen, grouped | One long paragraph per summary; sixteen commands, ungrouped. | Phase D (FR-075) |
| Completions and man pages | None. | Phase G (FR-056) |
| Stdin and stdout | Results on stdout, logs on stderr; `token issue` prints the token alone. `--description -` reads a description from stdin. `mcp` speaks the protocol on stdin and stdout. | Phase G (FR-056) |
| No secrets on argv | `--token` is accepted on argv, with `FLEETRING_TOKEN` as the alternative; no `--token-file`, no warning. Keys are always paths. | Phase B (`--token-file`), phase G (the warning) |
| Renames | Clean breaks, listed in FR-062's breaking changes for 0.3.0 and FR-076's for 0.6.0. | 0.7.0's list: phase A, in the release notes |
| `upgrade` | Installs a release verified with cosign and by checksum, keeps `fleetringd.previous` for `--rollback`, and restarts the node `--config` names in place (ADR-0038). | Phase A: across the rename |
| `db watch` | None; change streams reach only configured sinks. | Deferred: its own ticket and ADR (ADR-0043, point 9) |
