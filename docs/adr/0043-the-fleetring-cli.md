# ADR-0043: One binary named `fleetring`, with `fleetringd` as its compatibility name until 1.0, contexts, a detached dev node, agents that set themselves up, and help that fits a screen

- **Status:** Accepted (2026-11-06)
- **Date:** 2026-11-05
- **Related:** ADR-0025 and ADR-0026 (principals, scopes, tokens; the scope grammar's reserved words, point 8.11), ADR-0032 (renames are clean breaks; amended here by points 2 and 6), ADR-0033 (surfaces and the client crate; point 2 amended by point 1), ADR-0034 (admin listener, `/readyz`), ADR-0038 (upgrades; points 6 and 7 amended by point 3), ADR-0039 (dev mode; points 1, 3 and 6 amended by points 5 and 7), ADR-0041 (`mcp`); tickets FR-075 (#103), FR-056 (#59), FR-079 (#114), FR-054 (#57), FR-060 (#62); [CLI style guide](../cli.md)

## Context

The CLI grew one feature at a time inside the daemon's binary, `fleetringd`. 0.7.0 is the release that makes it pleasant to use. The owner decided its shape on 2026-11-05 (#103, #59, #114); this ADR records those decisions and the ones they left to design.

- **The name.** Every client command is spelled `fleetringd db …`, which reads as "talk to the daemon". The owner chose one binary named `fleetring`, with `fleetringd` kept as a name for it until 1.0 so that service units and scripts keep working. `fleet` was rejected: it collides with JetBrains Fleet, Rancher Fleet and FleetDM's `fleetctl`. The placement crate, `fleetring`, isn't renamed; crate and binary names are independent.
- **The upgrade path crosses the rename.** `fleetringd upgrade` (ADR-0038) is how nodes move between releases, and a 0.6 binary will be the one that installs 0.7. What 0.6 does is fixed: it reads `fleetringd-vX.Y.Z` tags from GitHub's releases list; verifies `SHA256SUMS` with cosign against `refs/tags/fleetringd-v`; downloads `fleetringd-<version>-<target>.tar.gz`; extracts exactly `fleetringd-<version>-<target>/fleetringd`, which must be a regular file; runs it as `<scratch>/fleetringd --version` and requires `fleetringd <version>`; runs `<it> upgrade --software` for its fleet features; and swaps it in at the canonical path of the running binary. To restart a running node in place, the 0.6 daemon executes its own binary path again with the same arguments, passing `FLEETRINGD_RESTARTS`, and the upgrader waits until `<data_dir>/fleetringd.process` counts the restart. Any of those that 0.7 breaks breaks the first upgrade across the rename.
- **Every command needs connection flags.** Each takes `--control` or `--config`, and the TLS flags or `FLEETRING_TLS_DIR`. Dev mode writes `<dir>/env` to source (ADR-0039, point 6) until contexts exist. FR-056 planned contexts in the style of `kubectl config` and `docker context`, and the owner added a `switch` shortcut in the style of kubectx and k9s's `:ctx`.
- **A dev node needs a terminal of its own.** `fleetringd dev` runs in the foreground. The owner wants `docker run -d` ergonomics: start it in the background, get the prompt back once it is ready, and stop it by name.
- **Agents set up by hand.** Giving an agent its own identity takes six manual steps (the agent quickstart, step 2): issue a client certificate into the dev authority's directory, move it out because dev mode refuses a `tls/` holding files it didn't write, copy `ca.crt`, issue a scoped token, `chmod` it, and hand-write an MCP client configuration with four variables and a secret in it. The owner tried it and it failed partway, with no recovery hint for a missing `ca.crt`. The goal of #114 is an agent that boots and acquires a node and an identity for itself.
- **Help is hard to read.** Every subcommand's summary is one long doc-comment paragraph, so `-h` crams paragraphs into one-line slots, and the top level lists sixteen commands in declaration order.
- **ADR-0032 forbids hidden aliases.** A renamed spelling stops working in the release that renames it. The owner's decision keeps `fleetringd` working until 1.0, so this ADR has to say exactly what is kept, where it is documented, and when it goes.

## Decision

### 1. One binary, `fleetring`, also installed as `fleetringd` until 1.0

1. **The binary is `fleetring`.** `crates/fleetringd/Cargo.toml` gains `[[bin]] name = "fleetring", path = "src/main.rs"`, and clap's command name becomes `fleetring`. It runs a node with `fleetring serve [--config <file>] [daemon flags]`, a dev node with `fleetring dev`, and every client command as `fleetring <group> …`.
2. **`fleetringd` is the same file under a second name, never a second build.** `install.sh` installs `fleetring` and makes `fleetringd` a symbolic link to it; the container image does the same; packages (FR-060) will too. The Windows `.zip` holds two copies, `fleetring.exe` and `fleetringd.exe`, because Windows symbolic links need privileges.
   - **Why not a second `[[bin]]`:** it builds and links the CLI twice, and leaves two files on disk that `upgrade` would have to keep in step. `upgrade` replaces the canonical path of the binary it runs (ADR-0038, point 7), so with one file behind two names, both names are upgraded and rolled back together.
   - **Why not behaviour by `argv[0]`:** a restart in place executes `current_exe()`, which Linux resolves through the link, so a daemon started as `fleetringd --config …` comes back as `fleetring --config …`. The command line must therefore mean the same under both names.
3. **The name only changes what the binary calls itself.** If `argv[0]`'s file name starts with `fleetringd` (`fleetringd`, `fleetringd.exe`, `fleetringd.previous`), the binary sets both clap's `display_name` and its `bin_name` to `fleetringd`; otherwise to `fleetring`. Setting both matters: `display_name` alone changes `--version`, but clap derives usage lines from `bin_name`, which it takes from `argv[0]` verbatim, so `fleetringd.previous` or an upgrader's `.fleetringd.new` would leak into them. That name prefixes `--version` (`fleetringd 0.7.0`), usage lines, and errors (`fleetringd: …`); nothing else depends on it. This is what keeps a 0.6 upgrader working: it runs the new binary as `<scratch>/fleetringd --version` and expects `fleetringd 0.7.0` (point 3).
4. **`fleetringd` with 0.6's daemon flags still runs a node.** If the first argument after the global `--log-format <v>` is one of 0.6's top-level daemon flags (`--config`, `--node-id`, `--data-dir`, `--listen`, `--advertise`, `--control`, `--role` or `--insecure-plaintext`, alone or as `--flag=value`), the arguments are read as `serve` followed by them. This holds under either name, for the reason in point 2. It is what existing service units run, what the container image's old `CMD` runs, and what a 0.6 daemon's restart in place executes after a 0.6 upgrader has swapped in 0.7. The daemon then logs one warning at start: ``starting without `serve` is kept until 1.0; run `fleetring serve --config <file>` ``. The rewrite happens before clap parses, so the top-level help lists no daemon flags, and `fleetring` with no arguments prints help and exits 2.
5. **These are compatibility names, not hidden aliases.** The `fleetringd` name and the bare daemon flags are documented in `fleetring --help` (one line under the command list), in `fleetring serve --help`, in the style guide's renames section, and in the 0.7.0 release notes, and each is removed in 1.0. ADR-0032's "no hidden alias" holds: nothing is accepted that the help doesn't mention. Point 6 amends ADR-0032 to allow exactly these two.
6. **The package stays `fleetringd`.** Renaming the package would rename the library crate every test imports (`use fleetringd::…`), every `-p fleetringd` in CI and the docs, the `fleetringd-v` release tags that `RELEASING.md` ties to the package's version, and the tags a 0.6 upgrader looks for (point 3), all for a package that isn't published. `cargo install --git https://github.com/fleetring/fleetring fleetringd` installs the `fleetring` binary; the README says so, and says to add the link (`ln -s fleetring ~/.cargo/bin/fleetringd`) only if something still calls `fleetringd`. If the CLI is ever published to crates.io, it is as `fleetring-cli`, a decision for 1.0.
7. **What keeps its name.** These are on-disk or on-the-wire names that a 0.6 binary or an existing directory depends on, so they stay through 0.x and are decided at 1.0 (open question 4):
   - the release tag prefix `fleetringd-v` and the archive names `fleetringd-<version>-<target>.tar.gz` and `.zip`, which 0.6's `upgrade` and `install.sh` look for and its cosign identity names;
   - `<data_dir>/fleetringd.process`, `<data_dir>/fleetringd.lock` and `FLEETRINGD_RESTARTS`, which a 0.6 upgrader waits on and a 0.6 daemon passes to the binary it executes;
   - `fleetringd.toml`, which dev mode's directory check reads (ADR-0039, point 3), and `deploy/fleetringd.toml`;
   - the crate's tracing targets (`fleetringd::audit`, `RUST_LOG=fleetringd=debug`) and `FLEETRINGD_BASELINE_VERSION` and `FLEETRINGD_BASELINE_BIN` in the rolling-upgrade test.

   A test pins the process-file, lock and variable names, so nobody renames them by accident.
8. **The container image is published as `ghcr.io/fleetring/fleetring`,** and the same manifest is also pushed as `ghcr.io/fleetring/fleetringd` until 1.0. The image holds `/usr/local/bin/fleetring` and a `fleetringd` link, with `ENTRYPOINT ["/usr/local/bin/fleetring"]` and `CMD ["serve", "--config", "/etc/fleetring/fleetringd.toml"]`. A user-supplied `CMD` of `--config …` still runs a node, by point 4.

### 2. Renames in 0.7.0

These are clean breaks under ADR-0032, apart from the two compatibility names. Each old spelling fails like any unknown command or flag, with one `hint:` line naming the new one. The release notes list them under breaking changes.

| 0.6 | 0.7.0 |
|---|---|
| `fleetringd …` (the binary) | `fleetring …`; `fleetringd` stays as a compatibility name until 1.0 (point 1.5) |
| `fleetringd --config <file> [daemon flags]` | `fleetring serve --config <file> [daemon flags]`; the old form stays until 1.0, with a warning (point 1.4) |
| `fleetringd db archive show <db> --archive <a>` | `fleetring archive show <db> --archive <a>` |
| `fleetringd db archive verify <db> --archive <a>` | `fleetring archive verify <db> --archive <a>` |
| `fleetringd archive restore --from <db> --to <dir> …` | `fleetring archive restore <db> --to <dir> …` (the database is the argument) |
| `--version` prints `fleetringd X.Y.Z` | prints `fleetring X.Y.Z`, or `fleetringd X.Y.Z` when run as `fleetringd` |
| errors start `fleetringd:` | start `fleetring:`, or `fleetringd:` when run as `fleetringd` |
| the dev banner says `. ~/.fleetring/env` | it names the `dev` context; `<dir>/env` is still written, for scripts |
| `ghcr.io/fleetring/fleetringd`, `ENTRYPOINT fleetringd`, `CMD --config …` | `ghcr.io/fleetring/fleetring` (the old name is pushed too until 1.0), `ENTRYPOINT fleetring`, `CMD serve --config …` |
| `deploy/dev/fleetringd-dev.service`, `homebrew.mxcl.fleetringd`, `opt_bin/"fleetringd"` | `fleetring-dev.service`, `homebrew.mxcl.fleetring`, `opt_bin/"fleetring"` (no package was published yet, FR-060) |
| an MCP client runs `fleetringd mcp` with four variables and a token | it runs `fleetring mcp --agent <id>` (point 8) |
| the previous binary is kept as `fleetringd.previous` | it is kept as `<name>.previous` beside the real file: still `fleetringd.previous` on a host upgraded from 0.6, where the real file stays `fleetringd`; `fleetring.previous` only on a fresh 0.7 install |

`db restore` keeps its name: it is the online restore into the fleet, while `archive restore` is the offline one. New in 0.7.0, so not renames: `serve`, `stop`, `dev -d`, `dev stop|status|logs`, `context …`, `switch`, `--context`, `FLEETRING_CONTEXT`, `--tls-dir`, `--token-file`, `node list`, `agent …`, `mcp --agent`, `FLEETRING_AGENT` and `cert client --out`.

### 3. The upgrade path across the rename

1. **A 0.6 binary installs 0.7.0 unchanged.** For the 0.7 line, each archive's `fleetringd-<version>-<target>/` directory holds `fleetring` and a second regular copy named `fleetringd`, because a 0.6 upgrader extracts only a regular file at that path. It names its scratch copy `fleetringd`, so the new binary prints `fleetringd 0.7.0` (point 1.3), and `upgrade --software` answers as before. The swap writes 0.7 over the canonical path the node runs, and a running daemon's restart in place executes that path with its 0.6 arguments, which 0.7 reads as `serve` (point 1.4). The 0.7 daemon reads `FLEETRINGD_RESTARTS` and writes `fleetringd.process` as 0.6 did, so the upgrader sees the restart counted.
2. **The release workflow checks this.** A script, `scripts/check-archive.sh`, fails the release unless both entries are regular files, identical byte for byte (`cmp`), that print `fleetring <version>` and `fleetringd <version>` under their own names. The nightly rolling-upgrade test, with `FLEETRINGD_BASELINE_VERSION=0.6.0`, runs the 0.6 release's own `upgrade` against an archive laid out as the workflow lays it out (ADR-0038, point 9). It must pass before 0.7.0 is tagged.
3. **A 0.7 upgrader looks for the new layout and accepts the old.** It extracts `<dir>/fleetring` if the archive has it, and otherwise `<dir>/fleetringd` (a release before 0.7, for a downgrade or a check). It accepts `fleetring <version>` or `fleetringd <version>` from `--version`, and names its scratch copy `fleetring`. From 0.8.0 on, the archive's `fleetringd` entry is a symbolic link to `fleetring`, since every release that may upgrade to 0.8 (the 0.7 line, by the window in ADR-0038 point 1) looks for `fleetring`. `install.sh` follows links and installs either layout.
4. **A 0.7 upgrade completes the pair.** After a 0.6 upgrader has installed 0.7 over `fleetringd`, there is no `fleetring` on the machine. The release notes give the one command that adds it (`ln -s fleetringd "$(dirname "$(command -v fleetringd)")/fleetring"`) or say to rerun `install.sh`. Every 0.7 `upgrade` and `--rollback` also adds it: if the binary it replaced is named `fleetringd` and no `fleetring` sits beside it, it creates the relative link `fleetring -> fleetringd` and says so in its report.
5. **Rolling back past the rename is refused while it would break the node.** A 0.6 binary doesn't know `serve`. A 0.7 daemon records its arguments in the process file (a new optional field, `args`, which a 0.6 reader ignores). `upgrade --rollback`, and `upgrade --version` with an older release, refuse to install a release before 0.7.0 over a node whose arguments start with `serve`, before they swap anything. The refusal names the release and says to change the node's start command to `fleetringd --config <file>`, restart it, and roll back again.
   - **It applies to every rollback or downgrade the 0.7 binary performs,** with or without `--no-restart`, whether the 0.7 binary is the installed one or the one kept as `.previous`.
   - **Without `--config` naming a running node,** `upgrade` can't see the node's arguments. It then asks, on a terminal, whether any node on this machine starts with `serve`, and without a terminal refuses unless `--yes` confirms that none does. It never only warns.
   - **The escape hatch can't check.** ADR-0038 point 7 lets the kept binary roll back on its own: `fleetringd.previous upgrade --rollback`. When that kept binary is 0.6, it knows nothing of `serve`, so it can't refuse. On a unit already switched to `fleetring serve`, its `SIGHUP` restart then executes `serve --config …` under 0.6, which exits 2, and systemd restarts it in a loop. The same happens to any supervisor start after a rollback by hand.
   - **So the release notes say, in these words:** "After switching a unit to `fleetring serve`, rolling back to 0.6 requires reverting the unit to `fleetringd --config` first." They also recommend keeping service units on `fleetringd --config …` until no node needs to roll back to 0.6.
6. **A restart in place keeps the name it was started with.** The 0.7 daemon executes its binary with `argv[0]` set to the one it received (`CommandExt::arg0`), so a node started as `fleetringd` still says `fleetringd` after a restart.

### 4. Contexts (#59)

1. **What a context is.** A context is a name for everything a client command needs to reach one fleet as one principal: the fleet name, one or more control addresses, a TLS directory or explicit `ca`, `cert` and `key` paths, a default principal (`as`), and optionally a token file. It holds paths and names, never a secret: no token, no key contents. A token is referenced by file (`token_file`). FR-056's `token_command` is not adopted. A context file someone hands you shouldn't run programs, and the owner asked for files.
2. **Where contexts live.** They are stored in `contexts.toml` in the configuration directory:
   - `$XDG_CONFIG_HOME/fleetring/`, or `~/.config/fleetring/` when that is unset or not absolute, on Linux and on macOS alike, as `gh` does, so dotfile managers and docs share one path;
   - `%APPDATA%\fleetring\` on Windows.

   The directory is created `0700` and the file written `0600`. A write takes an exclusive lock on `contexts.toml.lock` (`File::lock`), writes a temporary file beside the store, and renames it over the store, so concurrent writers (a `fleetring dev` start and a `switch`) never lose each other's change.
3. **The format.**

   ```toml
   # Written by fleetring. Edit it with `fleetring context`.
   version = 1
   current = "dev"
   previous = "home"

   [contexts.dev]
   fleet = "dev"
   control = ["127.0.0.1:7400"]
   tls_dir = "/home/me/.fleetring/tls"
   as = "admin"
   dev_dir = "/home/me/.fleetring"          # written by dev mode; see point 5

   [contexts.home]
   fleet = "home"
   control = ["10.0.0.2:7400", "10.0.0.3:7400"]
   ca = "/home/me/.config/fleetring/tls/home/ca.crt"
   cert = "/home/me/.config/fleetring/tls/home/client.ops.crt"
   key = "/home/me/.config/fleetring/tls/home/client.ops.key"
   token_file = "/home/me/.config/fleetring/tokens/home"
   ```

   Context names are 1 to 63 characters of lowercase letters, digits, `-`, `_` and `.`, start with a letter or digit, and aren't `-`. A `version` other than 1 is refused, with the file's path. Control addresses are tried in order until one connects; a control node that isn't the leader redirects as today. Unknown keys are refused, so a typo doesn't silently drop a setting.
4. **Precedence.** Each setting is resolved on its own, and the first match wins:
   1. the setting's own flag on the command line (`--control`, `--tls-dir`, `--as` and so on);
   2. the context the `--context` flag names;
   3. the setting's environment variable (`FLEETRING_CONTROL`, `FLEETRING_TLS_DIR`, `FLEETRING_CA`, `FLEETRING_CERT`, `FLEETRING_KEY`, `FLEETRING_AS`, `FLEETRING_TOKEN`, `FLEETRING_TOKEN_FILE`, `FLEETRING_AGENT`);
   4. the context `FLEETRING_CONTEXT` names, or else the current context;
   5. the built-in default, where a safe one exists.

   **This amends #59's stated order** (flags, then variables, then `--context`), after review. A `--context` flag typed on this command line is the most explicit choice there is, while a field variable is ambient state, often left behind by a sourced `env` file. So the flag outranks the variables. Without the flag, the variables still override `FLEETRING_CONTEXT`'s context and the current one, as #59 wanted, so scripts that set `FLEETRING_CONTROL` keep working.

   Steps 2 and 4 pick one context; they never merge two. Within that order, the identity resolves per file (the certificate, its key and the authority). An explicit path at a step (`--cert`, `FLEETRING_CERT`, the context's `cert`) wins over a principal at the same step (`--as`, `FLEETRING_AS`, the context's `as`, which picks `client.<as>.crt` under the TLS directory). A principal wins over the TLS directory's `client.crt`. The TLS directory itself is resolved by the same order (`--tls-dir`, `FLEETRING_TLS_DIR`, the context's `tls_dir`). `--config <file>` counts as a flag for the control address, as today.
5. **Overrides are visible.** The commonest surprise is a shell that sourced dev mode's `env` file and then switched contexts. Every override prints one warning per variable on stderr unless `-q` is given, terminal or not, so a script's log shows it too:
   - a field variable that overrides the current context's (or `FLEETRING_CONTEXT`'s) field: `warning: FLEETRING_CONTROL overrides context 'home' (control 10.0.0.2:7400)`;
   - a field variable that a `--context` flag outranks, and that therefore goes unused: `warning: FLEETRING_CONTROL is ignored: --context home sets control`.

   `--verbose` names the context used and where each setting came from.
6. **Destructive confirmations name the fleet and the context.** The prompts of `db drop`, `orphan remove`, `node remove`, `token revoke` and `agent remove` say where they act, as `drop database 'notes' on fleet 'home' (context 'home') and every replica's copy? type the name to confirm:`. Without a context, they name the fleet and the control address. Typing a name into the wrong fleet is the mistake contexts make easy.
7. **Checked on use.** If the context names a fleet and the client certificate names another, the command fails and names both. The certificate's fleet comes from its URI SAN (`fleetring://<fleet>/principal/<name>`).
8. **Commands.**
   - `fleetring context list [-o table|json]`: `CURRENT` (`*`), `NAME`, `FLEET`, `CONTROL` and `AS`.
   - `context show [<name>] [-o table|json]`: the current context by default.
   - `context current`: prints only the name, for shell prompts. It reads no network and starts no runtime, prints nothing and exits 1 when no context is current.
   - `context add <name> --control <addr>… [--fleet <f>] [--tls-dir <dir> | --ca <f> --cert <f> --key <f>] [--as <p>] [--token-file <f>]`: `--fleet` defaults to the fleet the certificate names. A name that exists is refused, with a hint to remove it first. The first context added becomes current.
   - `context use <name>`: makes it current and records the one it replaces as `previous`.
   - `context remove <name>`: removes it. It doesn't ask, because it deletes only paths, which `context add` restores. Removing the current context leaves none current, and the command says so.
9. **`fleetring switch` is the shortcut, the only one.**
   - `switch <name>` is `context use <name>`.
   - `switch -` goes back to `previous`, like `cd -`.
   - `switch` with no name opens a picker when stdin and stderr are terminals: arrow keys, type to filter, the current context marked and preselected, Esc to cancel (exit 130). Without a terminal it prints `context list`'s table on stderr and exits 2, with the hint `` pass a name: `fleetring switch <name>` ``. A script that runs `switch` without a name has a bug, and exit 0 would hide it.

   `context use` stays the scriptable form. It takes only a name, never the picker or `-`. The style guide lists `switch` as the one documented shortcut, so it is neither an alias nor hidden. The picker is `dialoguer`'s `FuzzySelect` (point 12).
10. **Contexts belong to the client crate.** Following ADR-0033 (decision 1), the context code (the file, its format, its lock, and selecting a context) lives in a new crate, `fleetring-client` (`publish = false`, MSRV 1.89 for `File::lock`), which FR-056 then grows with the clients and the typed results. The precedence itself is resolved in the CLI, where the flags are. It is a pure function of the flags, an environment snapshot and the selected context, so it is tested without touching the process environment.
11. **The `env` file stays.** Dev mode still writes `<dir>/env` for scripts and CI (ADR-0039, point 6). It is no longer how people are told to point the CLI at a dev node.
12. **Dependencies.** `dialoguer` 0.12 (MIT, MSRV 1.66) with only its `fuzzy-select` feature, for the picker. It shares `console` with `indicatif` (point 6.3), so the two add one terminal layer, not two. Raw-mode input on Unix and Windows is the part not worth hand-rolling.

### 5. A detached dev node, `stop`, and the dev context

1. **`fleetring dev` writes the `dev` context on every start,** foreground or detached: `fleet = "dev"`, `control = ["127.0.0.1:<port>"]`, `tls_dir = "<dir>/tls"`, `as = "admin"` and `dev_dir = "<dir>"`, taken from the configuration it runs, so it follows an edited port. It makes `dev` current unless another context is current.
   - **The banner says which:** `context    dev (current)`, or `context    dev (not current: 'home' is; run fleetring switch dev)`.
   - **It overwrites a `dev` context only if dev mode wrote it,** which the `dev_dir` key marks. Otherwise it leaves the user's own `dev` context alone, says so in the banner, and still starts.
   - **The node starts even if the context can't be written,** for example when the configuration directory is read-only. The banner then says why, and points to `<dir>/env`.

   A first `fleetring dev -d` followed by `fleetring db create notes` therefore works with no other step.
2. **`fleetring dev -d` / `--detach` runs the dev node in the background** and returns once it is ready:
   1. **It prepares the directory in the foreground,** as `dev` does, so every refusal (not dev mode's directory, a bad port, an expired certificate) is immediate. Then it writes the context.
   2. **If a dev node already runs from that directory, it says so and exits 0.** A running node is one whose process file names a process that still holds the data directory's lock. This keeps setup scripts idempotent.
   3. **It starts the node:** it spawns its own executable as `dev --data-dir <dir> [--port <n>]`, with `argv[0]` preserved and stdin from `/dev/null`. stdout and stderr are appended to `<dir>/fleetring.log`, opened `0600`; at each start, a log over 16 MiB is first renamed to `fleetring.log.1`. Rotation happens only at start: the child holds the file open, so during one long run the log grows without bound. `dev logs` and the README say so, and `fleetring dev stop && fleetring dev -d` rotates it. The environment variable `FLEETRING_DEV_DETACHED=1` tells the child it was detached. This variable is internal, not a CLI setting.
   4. **The child detaches itself.** Its first act is `setsid()` (`rustix::process::setsid`, a safe call), which takes it out of the terminal's session. Closing the terminal therefore sends it no `SIGHUP`, which would otherwise restart it in place (ADR-0038, point 8). Daemonizing with `fork` in the parent was rejected, because forking a process that may already run threads is unsafe, and the workspace forbids `unsafe` (`pre_exec`). The detached child logs without ANSI colour, since its stderr isn't a terminal.
   5. **It waits until the node is ready,** for `--timeout`, 60s by default. Ready means two things: the process file names the child's PID, and the admin listener's `/readyz` answers 200 (ADR-0034). If the configuration has no admin listener, it waits for a TCP connect to `listen`. If the child exits first, the command fails with exit 1, printing the log's last 20 lines and its path.
   6. **It prints the result on stdout:** the address, the data directory, the log's path, the context line and `Stop it with fleetring dev stop`. A spinner shows on stderr meanwhile, and a check mark when it is done (point 6).
3. **`fleetring dev stop | status | logs [-f]`** each take `--data-dir <dir>`, `~/.fleetring` by default.
   - **`dev stop`** is `fleetring stop` for that directory.
   - **`dev status`** prints `running` with the PID, address, release, log path and the context line, or `not running`. It takes `-o json`, and exits 0 either way (it is a read command).
   - **`dev logs`** prints the log. `-f` follows it, polling every 200 ms, starting over if the file shrinks, and ending on Ctrl-C (exit 130). A foreground dev node writes no log file, and `dev logs` says so.
4. **`fleetring stop (--data-dir <dir> | --config <file>)`** stops any node started in a terminal or a script:
   - **`--data-dir`** takes the node's data directory, or a directory holding a `fleetringd.toml`, whose `data_dir` is then used. That way a dev directory and its `data/` both work. `--config` reads `data_dir` from the file.
   - **With neither, it is a usage error** whose hint is `fleetring dev stop` for the dev node. Defaulting to some directory would stop the wrong node.
   - **It reads `fleetringd.process`.** If the file is missing, or the process no longer holds the data directory's lock, it fails (exit 1), naming the file or the stale PID.
   - **Otherwise it sends `SIGTERM`, which drains the node and leaves the fleet** as `[membership] on_shutdown` says. It then waits until the lock is released, for `--timeout`, 2m by default, with a spinner. It never escalates to `SIGKILL`. On timeout it says the node is still draining, and exits 1.
5. **Supervisors stop their own nodes, and `dev stop` always stops its own.**
   - **A detached node says so.** The process file gains an optional `started_by` field, which `dev -d`'s child writes as `"detach"` (it knows from `FLEETRING_DEV_DETACHED`). `dev stop` and `stop` always stop a node started that way, whatever else the file says.
   - **Detection doesn't trust the environment alone.** Shells inherit supervisor variables: a terminal under `gnome-terminal-server.service`, VS Code's terminal, or any program a unit started passes `INVOCATION_ID` on. So the daemon records `supervisor = "systemd"` only when `INVOCATION_ID` is set *and* its parent process is the supervisor itself: PID 1, or a process whose `/proc/<ppid>/comm` is `systemd` (the user manager). It records `"launchd"` only when `XPC_SERVICE_NAME` starts with `homebrew.mxcl.` *and* its parent is PID 1. Detection runs at start, while a `dev -d` parent is still alive, so a detached child is never mistaken for one reparented to a subreaper.
   - **`dev -d` removes the supervisor variables from its child's environment** (`INVOCATION_ID`, `JOURNAL_STREAM`, `NOTIFY_SOCKET`, `LISTEN_PID`, `LISTEN_FDS`, `LISTEN_FDNAMES`, `WATCHDOG_PID`, `WATCHDOG_USEC` and `XPC_SERVICE_NAME`), so the node neither looks supervised nor talks to a supervisor that didn't start it.
   - **`stop` and `dev stop` refuse a supervised node,** with the supervisor's own command as the hint (`systemctl stop …`, `brew services stop fleetring`), because a supervisor that keeps the node alive would restart it.
6. **Windows.** `dev -d`, `dev stop` and `stop` refuse on Windows in 0.7 and say to run `fleetring dev` in its own terminal and press Ctrl-C. Windows has no `SIGTERM` to drain a detached process with, and `upgrade` already restarts in place only on Unix. `dev status` and `dev logs` work everywhere.
7. **The dev directory allows the new entries.** Dev mode's directory check (ADR-0039, point 3) adds `fleetring.log`, `fleetring.log.1`, `fleetring.start.lock` and `agents/` to the entries a partial dev state may hold.

### 6. Progress output

1. **Spinners, never bars,** on stderr, for the commands that can take noticeably long:
   - `dev` startup, foreground or `-d`, `dev stop` and `stop`;
   - `db create`, `db restore` and `db verify`;
   - `archive verify` and `archive restore`;
   - `upgrade`, with one message per step: download, verify, drain, restart, rejoin, catch up;
   - `node drain`, `orphan remove` and `agent add`.

   A spinner appears only after 150 ms, so a fast command never flickers. On success it is replaced by `✓ <what happened>` on stderr. On failure it is cleared, and the error prints as usual. The result still goes to stdout, so `$(…)` captures the same bytes with or without a spinner.
2. **A spinner shows only if all of these hold:**
   - stdout and stderr are both terminals;
   - the output is human, not `-o json`, `jsonl` or `--json`;
   - `-q` isn't given;
   - `NO_COLOR` is unset or empty;
   - `TERM` isn't `dumb`;
   - `CI` is unset or empty.

   The check is one pure function over a snapshot of those facts, tested on its own. Log lines written while a spinner shows go through it (`ProgressBar::suspend`), so they don't tear the line.
3. **`indicatif` 0.18** (MIT, MSRV 1.85), built with `default-features = false` and only `unicode-width`. It adds `console` and `unit-prefix`; `portable-atomic` and `unicode-width` are already in the tree. A hand-rolled spinner would be short, but the suspend-while-logging and terminal-width handling are where it goes wrong, and the picker needs `console` anyway (point 4.12).

### 7. Help that fits one screen

1. **One-line summaries.** Every command's and flag's doc comment starts with a single summary line, at most 80 characters with no trailing period, then a blank line, then the details. clap's derive takes the first paragraph as `about` (for `-h`) and the whole comment as `long_about` (for `--help`). The fault today is that each comment is one long paragraph. A test walks the whole command tree and fails on any summary that wraps or exceeds 80 characters.
2. **Wrapping.** clap's `wrap_help` feature wraps to the terminal's width, with `max_term_width(100)`, so lines stay readable on wide terminals.
3. **Groups under headings.** clap has headings for arguments (`next_help_heading`), but subcommands only get one heading for all of them (`subcommand_help_heading`). The root command therefore uses a `help_template` without `{subcommands}`:

   ```text
   {about-with-newline}
   {usage-heading} {usage}

   {before-help}{options}{after-help}
   ```

   `before_help` (for `-h`) and `before_long_help` (for `--help`) are rendered at start-up from the command tree itself: the examples, then each group's heading and its commands' summaries, styled with clap's own header style so that `NO_COLOR` and `--color` apply. No subcommand is hidden, so completions and man pages still list every one. The groups and their order are:

   | Heading | Commands |
   |---|---|
   | Getting started | `dev`, `serve`, `stop`, `switch`, `context`, `completions` |
   | Databases | `db` |
   | Fleet | `status`, `node`, `orphan`, `upgrade`, `lease` |
   | Security | `cert`, `token` |
   | Agents | `mcp`, `agent` |
   | Recovery | `archive` |

   A test fails if a subcommand is in no group or in two.
4. **Quick-start examples come first:**

   ```text
   Examples:
     fleetring dev -d                   start a dev node in the background
     fleetring switch dev               use it (dev does this for you)
     fleetring db create notes          create a database
     fleetring switch prod              talk to another fleet
   ```

   `--help` adds a longer set, the exit codes, and the compatibility line: `fleetringd is the same program, kept until 1.0; fleetringd --config <file> runs serve.`
5. **Budget.** `fleetring -h` at 100 columns is at most 50 lines, none wider than 100 columns. Each group's `-h` (for example `fleetring db -h`) lists one line per command. A snapshot test pins `fleetring -h` at width 100.

### 8. Agents set themselves up (#114)

1. **`fleetring mcp --agent <id>` is the whole setup** in dev mode. An example is `claude mcp add fleetring -- fleetring mcp --agent 42 --context dev`, with the context pinned as point 8.8 pins it. When the selected context was written by dev mode, or when no context is selected and no connection flag or variable is set, it does three things:
   1. **It acquires a node.** If no dev node runs from the dev directory (the context's `dev_dir`, or `~/.fleetring`), it starts one exactly as `dev -d` does (point 5.2) and waits until it is ready. That also writes the `dev` context and makes it current if none is. The node outlives the MCP server, and `fleetring dev stop` stops it.
   2. **It acquires an identity.** If `<dev_dir>/agents/<id>/` holds no valid identity, it mints one from the dev authority (`tls/ca.crt` and `tls/ca.key`): a client certificate for the principal `agent-<id>`, valid for a year like dev mode's others, and a token for that principal scoped `admin:agent-<id>-*`, valid 30 days, the most a token may last. It then serves with that identity.
   3. **It serves MCP** as `agent-<id>`, as `fleetring mcp` does today (ADR-0041, point 9).

   **Minting is authorized by the directory, never by the context file.** A context's `dev_dir` is a path in a file the user can edit, so it only says where to look. Immediately before any mint or renewal from a dev authority (here, `agent add` and `agent renew` in dev mode), and whether or not a node already holds the directory's lock (in which case `prepare` was skipped), the full dev-directory check of ADR-0039 point 3 runs read-only on `dev_dir`: its `fleetringd.toml` says `dev = true` and passes `Config::validate`, so every address is loopback; `tls/` holds only dev mode's files and the agents' directory; and the authority is for the fleet `dev`. If any check fails, nothing is minted, and the error names the check. Editing a context to point at a real node's directory therefore can't reach minting.

   Everything that start-up says goes to stderr, because stdout carries JSON-RPC alone. That includes one line naming the agent, its context, and when its certificate and token expire, so the renew-at-start rule below is predictable. Two servers starting at once serialize on `<dev_dir>/fleetring.start.lock` (the node) and `<dev_dir>/agents/<id>/.lock` (the identity), so a second start waits for the first instead of failing.
2. **Re-runs reuse everything and regenerate nothing.** In dev mode only, credentials are renewed at start when they are close to expiring:
   - the certificate when fewer than 30 days remain, or when it wasn't signed by the current `ca.crt` (a dev directory that started over);
   - the token when fewer than 7 days remain, so a session has at least a week.

   A renewal revokes the old token if the node is reachable, and otherwise logs that it couldn't. A session that outlives its token gets the node's refusal as a tool error that says to restart the MCP server. Renewing inside a running server is not in 0.7 (open question 7).
3. **An agent identity is a directory,** `<agents_dir>/<id>/`, holding:
   - `ca.crt`, a copy, so the directory is a complete TLS directory;
   - `client.agent-<id>.crt` and `client.agent-<id>.key`, the key `0600`;
   - `token`, `0600`;
   - `identity.toml`, which holds no secret: the principal, the scopes, `read_only`, the token's ID and expiry, the certificate's expiry, the fleet, `issued_by` (`dev`, `authority`, and later `enrollment`), and two fields for delegation (point 8.11):
     - `owner`, the principal that created the identity: the principal the command ran as (`admin` in dev mode), or `authority` when `agent add --authority` ran without one;
     - `parent`, the agent ID of the agent that created it, or absent for none. It is always absent in 0.7, since only people and their contexts create agents.

   The directory is created `0700`. `--agent <id>` therefore means `--tls-dir <agents_dir>/<id> --as agent-<id> --token-file <agents_dir>/<id>/token`, at the flag's step of the precedence (point 4.4). It conflicts with `--as`, `--cert`, `--key`, `--tls-dir`, `--token` and `--token-file`. Its variable is `FLEETRING_AGENT`. It works on every client command (open question 2), always with an identity that already exists. Only `mcp --agent`, `agent add` and `agent renew` mint.
4. **Where agent identities live.** `<agents_dir>` is the selected context's `agents_dir` key if it sets one. For a context dev mode wrote, it is `<dev_dir>/agents`, so the agents' credentials sit beside the authority that issued them and go with it. For any other context it is `$XDG_DATA_HOME/fleetring/agents/<context>/` (`~/.local/share/…`; `%LOCALAPPDATA%\fleetring\agents\<context>\` on Windows). That is data, not configuration, and a context file therefore never points at keys it didn't choose. A context doesn't list its agents, and `switch` never shows them.
5. **Agent IDs are 1 to 32 characters of lowercase letters, digits and `_`.** The `-` that follows the ID in `agent-<id>-*` then always ends it, so `admin:agent-4-*` can't cover agent `4-2`'s databases (`agent-4-2-…`). The same reasoning made the quickstart keep the trailing `-`.
6. **Auto-minting is dev-mode-only.** It needs the local dev authority, which is on loopback by construction (ADR-0039, point 3). Against any other context, `--agent <id>` uses an existing identity. If there is none, it fails with exit 1 and mints nothing. The error names the directory it looked in, and gives the commands that create an identity where the fleet's authority is:

   ```text
   fleetring agent add 42 --context prod --authority <dir with ca.key>
   ```

   It also says to copy the resulting directory to `<agents_dir>/42/` on the agent's machine. Enrollment (point 8.9) removes that copy.
7. **`fleetring agent add | list | show | remove | renew`** manage identities explicitly. Each works on the selected context's `<agents_dir>`.
   - **`agent add <id> [--scope <s>]… [--read-only] [--authority <dir>] [--ttl <d>] [--print-config claude|cursor|json] [--install claude]`** mints as point 1 does. `--scope` replaces the default `admin:agent-<id>-*`, and is repeatable. `--read-only` makes the default scope `read:agent-<id>-*` and records `read_only`, so that `mcp --agent <id>` serves only the reading tools. `--authority` names the directory with `ca.crt` and `ca.key` for a fleet that isn't dev mode's. In dev mode it defaults to `<dev_dir>/tls`, and elsewhere it is required. An existing identity is refused, with a hint to use `renew`.
   - **`agent list [-o table|json]`** shows `ID`, `PRINCIPAL`, `SCOPES`, `READ-ONLY`, `CERT EXPIRES` and `TOKEN EXPIRES`.
   - **`agent show <id> [-o table|json]`** shows the same, plus the paths, the MCP command, and when the token must be renewed by (its expiry, and that the next `mcp --agent` start renews it once fewer than 7 days remain).
   - **`agent renew <id> [--authority <dir>]`** reissues the certificate and the token now, and revokes the old token.
   - **`agent remove <id> [--yes] [--no-revoke]`** revokes the token (`Control.RevokeToken`, which needs the fleet scope, as dev mode's admin has) and deletes the directory. It asks for the ID to be typed, as irreversible commands do. If the fleet is unreachable, it refuses unless `--no-revoke` is given, in which case it warns that the token stays valid until it expires. The certificate needs no revocation: `agent-<id>` has no `[[principal]]` entry, so the certificate alone authorizes nothing (ADR-0025).
8. **Printed and installed configuration holds no secret.** It is just the command, because `mcp --agent` finds its own credentials:
   - `--print-config json` and `cursor` print `{"mcpServers": {"fleetring": {"command": "<abs path to fleetring>", "args": ["mcp", "--agent", "42", "--context", "dev"]}}}`;
   - `--print-config claude` prints the `claude mcp add fleetring -- …` line;
   - `--install claude` runs that line and fails with the `--print-config claude` hint if `claude` isn't on `PATH`.

   The path is absolute because GUI MCP clients often lack the shell's `PATH`, and the context is pinned so that a later `switch` doesn't move the agent to another fleet.
9. **Future work: enrollment for remote fleets.** An operator runs `fleetring agent invite <id> --scope … --ttl …` to get a one-time code. The agent's `fleetring mcp --agent <id> --enroll <code>` exchanges the code through a control-plane issuance RPC for a certificate (its key generated on the agent's machine, so no key file is ever copied) and a short-lived token. That needs its own ADR, covering the RPC, approval, single use, rate limits, revocation and audit. 0.7 stays compatible with it:
   - the identity directory and `identity.toml` are what enrollment writes too (`issued_by = "enrollment"`);
   - `--agent` resolves an identity the same way however it was issued;
   - no `--enroll` flag or `invite` command exists until that ADR, so nothing hidden is reserved.
10. **Small fixes found on the way.**
    - **`cert client --out <dir>`** writes the certificate, its key and a copy of `ca.crt` to `<dir>` (created `0700`) instead of the authority's directory. The output is then a complete TLS directory.
    - **A missing `ca.crt` beside `ca.key` gets a recovery hint** in `cert client`, `cert node`, `token issue` and `agent add`. The hint says to copy `ca.crt` back from a node's `[tls] ca` file or a backup, or, in dev mode, to remove `<dir>/tls` to start a new authority, which makes every certificate and token the old one issued stop working. Dev mode's own start-up message already says this (ADR-0039, point 5).
    - **The agent quickstart is rewritten around `fleetring mcp --agent`.**
11. **Forward-compatible with delegation.** An ADR on identity and delegation is being written in parallel. Delegated child agents, an agent minting a narrower identity for a sub-agent it starts, are future work for that ADR, not 0.7. Two things keep 0.7 compatible with it:
    - **Identities record their lineage.** `identity.toml` records `owner` and `parent` (point 8.3), so identities minted now can be read by a delegation-aware release without guessing.
    - **The scope grammar reserves `fork` and `grant`.** Both words are reserved as a bare scope and as an action (`fork:<pattern>`, `grant:<pattern>`). Until delegation is implemented, every parser refuses them with `'fork' is reserved for delegation, which this release doesn't support`: `token issue --scope`, `[[principal]]` scopes, and `agent add --scope`. A node refuses a token that carries one. They can therefore never come to mean something else. This amends ADR-0025's grammar by refusal only; every scope valid today stays valid.

### 9. Not in 0.7.0: `db watch`

`fleetring db watch <db>` (#103, item 7) needs a new `Database` RPC that streams a database's logical changes (ADR-0024) to a client. That RPC has open design questions of its own:

- which replica serves it, and what happens on failover;
- resuming from a position;
- slow consumers and back-pressure;
- the `stream` scope;
- how it relates to sinks' positions (ADR-0028).

It is deferred to its own ticket and ADR, so that 0.7.0 changes no protocol and a 0.6/0.7 fleet mixes with nothing to gate.

## Alternatives considered

- **A second `[[bin]]` named `fleetringd`.** `cargo install` would install both names, but every build links twice, and `upgrade` would replace one file and leave the other on the old release (point 1.2).
- **Multi-call by `argv[0]`,** with client commands only under `fleetring` and the daemon only under `fleetringd`, as #103 first proposed. A restart in place loses the name it was started with (Linux resolves `current_exe()` through links), so a node could come back as the wrong program. It also doubles the help and the tests for no user benefit.
- **Renaming the package to `fleetring-cli` now.** That would churn every test, CI job and release tag, and break a 0.6 upgrader's tag lookup, for a package that isn't published (point 1.6).
- **New release tags (`fleetring-cli-v…`) and archive names in 0.7.0.** A 0.6 upgrader would find no newer release and couldn't verify the signature against its pinned identity. The question is left for 1.0.
- **Requiring `serve` from 0.7.0 on.** Every service unit, the image's old `CMD`, and above all the 0.6 → 0.7 restart in place (which re-executes the 0.6 arguments) would fail to start a node.
- **One `contexts.toml` plus a separate state file for `current`,** as kubectx keeps the previous context apart from kubeconfig. Two files means two locks; one file with one lock is simpler, and `current` is configuration in `kubectl` too.
- **`~/Library/Application Support/fleetring` on macOS.** It is the platform's convention, but it breaks the one-path rule that `gh` and most CLIs follow, and makes docs say two paths.
- **`token_command` in a context** (FR-056). It runs a program named in a file, and the owner asked for file references. A later ADR can add it with a trust rule.
- **Contexts that merge** (a field missing from `--context` falls through to the current one). It is surprising and hard to explain; one context is selected, or none.
- **Daemonizing with `fork`** in the parent, or a `setsid` helper program. Forking a threaded process is unsafe, the workspace forbids `unsafe`, and macOS ships no `setsid(1)`. The child calling `setsid()` itself is safe and portable across Unix.
- **`stop` defaulting to the dev directory.** It is ambiguous on a machine that also runs a real node, and `dev stop` already exists.
- **Hiding subcommands and listing them by hand** to get headings. Hidden subcommands vanish from completions. The template keeps them visible.
- **Agent identities as contexts** (`agent-42@dev`). That reuses the precedence, but fills `switch` with identities that aren't fleets, and puts key paths a program chose into the user's configuration. A directory per agent, found by `--agent`, keeps contexts about fleets.
- **Auto-minting against any fleet whose `ca.key` is at hand.** A process that an MCP client starts silently signing credentials with a production authority is too much power for a convenience. Outside dev mode, minting is an explicit `agent add --authority`.
- **Putting the token in the printed MCP configuration,** as the quickstart does today. That leaks a secret into a file that clients sync and users commit. The configuration names only the command.
- **Hand-rolling the spinner and the picker,** or using `inquire` (on `crossterm`). Hand-rolled raw-mode input is the risky part. `dialoguer` and `indicatif` share `console`, and the future TUI's `crossterm` (ADR-0033) is a separate crate.
- **`db watch` in 0.7.0.** See point 9.

## Consequences

- **Every doc, test and deploy file changes once.** That includes both READMEs, `docs/agents/`, `llms.txt`, the site, `SECURITY.md`, `RELEASING.md`, `deploy/`, the `Dockerfile`, `install.sh`, both release workflows and every `CARGO_BIN_EXE_fleetringd`. The rename lands first and alone, so the other changes rebase onto it (plan: `docs/plans/0.7.0-cli.md`).
- **0.6 → 0.7 is an ordinary `upgrade`.** The nightly rolling test from the 0.6.0 baseline proves it, and `RELEASING.md`'s pre-flight requires that run for 0.7.0. The 0.7 archives are about twice their size, because of the second regular copy. From 0.8 the `fleetringd` entry is a link.
- **A new crate, `fleetring-client`,** joins the 1.89 MSRV check beside `fleetringd`; the 1.88 check excludes it.
- **New dependencies: `indicatif`, `dialoguer` and `console`,** plus clap's `wrap_help`. `cargo deny` must pass.
- **The process file grows three optional fields: `args`, `supervisor` and `started_by`.** Its name, the lock's name and `FLEETRINGD_RESTARTS` are pinned by a test until 1.0.
- **The dev directory may hold `fleetring.log`, `fleetring.start.lock` and `agents/`,** and dev mode's directory check allows them.
- **The agent quickstart becomes one command,** and printed MCP configurations carry no secret.
- **At 1.0, a follow-up ADR removes the compatibility names** (`fleetringd` and the bare daemon flags), and decides the release tag and archive names and whether to publish `fleetring-cli`.
- **Follow-ups:** `db watch` (its own ticket and ADR), agent enrollment for remote fleets (its own ADR), delegated child agents with `fork` and `grant` (the identity-and-delegation ADR), renewing a token inside a running MCP server, and Windows support for `dev -d` and `stop` (its own ticket).
- **On acceptance,** these accepted records are amended by this one, each in its own status line:
  - ADR-0025 (the scope grammar reserves `fork` and `grant`, point 8.11);
  - ADR-0032 (point 5: the two compatibility names and the `switch` shortcut are allowed);
  - ADR-0033 (point 2: the CLI is `fleetring`, not `fleetringd`'s subcommands);
  - ADR-0038 (points 6 and 7: what `upgrade` extracts and accepts);
  - ADR-0039 (points 1 and 6: the `dev` context replaces the `env` line in the banner; point 3: the new entries).

## Open questions for the owner

Decided on 2026-11-06: the owner took every recommendation, and the reviewer's precedence fix. The body states each answer where it applies.

1. **`db watch` in 0.7.0?** Deferred to its own ticket and ADR, so 0.7.0 changes no protocol (point 9).
2. **Should `--agent` work on every client command?** Yes, using an identity that exists, so a person can see what the agent sees (`fleetring db list --agent 42`). Only `mcp --agent`, `agent add` and `agent renew` mint (point 8.3).
3. **Where do agent identities live?** Beside the dev authority (`<dev_dir>/agents/<id>/`) for a context dev mode wrote, and `$XDG_DATA_HOME/fleetring/agents/<context>/` for others. Never as contexts (point 8.4).
4. **The release tag and archive names** (`fleetringd-v…`, `fleetringd-<v>-<target>`). Kept through 0.x, since a 0.6 upgrader needs them, and decided at 1.0 with `fleetring-cli` (point 1.7).
5. **Windows `dev -d` and `stop`?** Refused in 0.7 with a clear message, and ticketed (point 5.6).
6. **`fleetring stop` with no flag?** A usage error with the `dev stop` hint, rather than a default directory (point 5.4).
7. **Renewing an agent's token inside a running MCP server?** Not in 0.7: renewal happens at start when fewer than 7 days remain, and an expired token's refusal says to restart the server (point 8.2).
8. **`agent add --authority` for fleets other than dev?** Yes, explicit only, until enrollment's ADR (points 8.6 and 8.7).
9. **The reviewer's precedence finding.** An explicit `--context` flag wins over the field variables, which still override the current context and `FLEETRING_CONTEXT`'s context. Every override warns (points 4.4 and 4.5).

## Revisions

- **2026-11-05, after an independent review, before acceptance.**
  - Supervisor detection checks the parent process, not only inherited variables, and `dev -d` records itself and clears the variables, so `dev stop` always stops its own node (point 5.5).
  - Rolling back past the rename names the `fleetringd.previous` escape hatch that can't check, the unit-first rule for the release notes, and a refusal that never degrades to a warning (point 3.5).
  - Minting runs the full dev-directory check first, whether or not a node is running, so a context file can't authorize it (point 8.1).
  - Both `display_name` and `bin_name` are set; `check-archive.sh` compares the two entries byte for byte; the rename table says which hosts keep `fleetringd.previous`.
  - Log rotation is at start only, `switch` without a name or a terminal exits 2, and the token's expiry is printed by `agent show` and at MCP start-up.
- **2026-11-06, on acceptance.**
  - The owner's answers to the open questions are folded into the body.
  - An explicit `--context` flag outranks the field variables, which still override the current context. Every override warns unless `-q`, and destructive confirmations name the fleet and the context (points 4.4 to 4.6).
  - Agent identities record `owner` and `parent` in `identity.toml`, and the scope grammar reserves `fork` and `grant` for delegation (points 8.3 and 8.11).
