# ADR-0033: The CLI, a TUI, and a desktop app are separate surfaces over one client crate

- **Status:** Proposed
- **Date:** 2026-11-03
- **Related:** ADR-0015 (crate layout), ADR-0019 (gRPC), ADR-0025 and ADR-0026 (principals and scopes), ADR-0032 (fleet); tickets FR-042, FR-055, FR-056, FR-057, FR-058; [lexicon](../lexicon.md), [CLI style guide](../cli.md)

## Context

People reach a fleet in three ways, and want a fourth:

- **Scripts and CI** need stable commands, stable machine output, and exit codes.
- **An operator at a terminal** runs the same commands by hand, and increasingly wants to *watch*: leases moving during a drain, a node going down, a sink falling behind. Today that means `watch-leases` against one node at a time and re-running `status`.
- **Keyboard-first desktops** (tiling window managers, Omarchy-style setups) live in the terminal. Tools like k9s and lazygit show that a full-screen terminal UI over an API's watch streams is the fastest way to operate a system, as long as it looks like the rest of the terminal.
- **Some people want a window**: a viewer for a fleet's databases and nodes that doesn't need a terminal.

All of the client code today lives inside the `fleetringd` crate: `data::Client` for database calls and redirects, `control::ControlClient` for control calls and leader redirects, `tls::Identity` for loading certificates, and the formatting in `main.rs`. A TUI or a desktop app built against that would either depend on the whole daemon or copy pieces of it. Copies drift: each surface ends up with its own retry rules, its own idea of which files hold an identity, and its own names for things, until the same database is a "db" in one, a "tenant" in another, and a "shard" in a third.

The protocol already has what live views need: `Database.WatchLeases` streams one node's lease changes (FR-042), and `Control.WatchMembership` streams the membership with liveness and draining. Lists and per-node status (`ListDatabases`, `NodeStatus`) are request and response.

## Decision

1. **One client crate, `fleetring-client`.** It holds everything a caller needs and nothing about how a surface draws it:
   - identity: TLS files, tokens, and `--as` resolution (FR-055), with the CLI's precedence;
   - contexts (FR-056): reading, writing, and selecting them;
   - the control client with leader redirects, and data-node calls with primary redirects;
   - watch subscriptions, including a fan-out that opens `WatchLeases` on every live node from `WatchMembership` and merges them into one stream of fleet-wide lease events;
   - typed results that derive `serde::Serialize`. These are the CLI's JSON fields, the TUI's rows, and what the desktop app's commands return, so the three can't disagree about a field's name or meaning.

   It depends on `fleetring-proto`, `fleetring` (for fingerprints and node types), `tonic`, and `rustls`; never on `fleetringd`. The daemon's own node-to-node calls stay in `fleetringd`. FR-056 creates the crate and moves the CLI onto it; whether the daemon's control client moves too is decided there.

2. **The CLI is for scripts and operators.** It stays `fleetringd`'s subcommands, follows the [CLI style guide](../cli.md), and is the only surface with a stability promise for its output (`-o json`). Anything a surface can do, the CLI can do.

3. **A TUI in the style of k9s,** in its own crate, `fleetring-tui`, on `ratatui` and `crossterm` (FR-057, which decides whether it starts as `fleetringd ui` or as a binary of its own).
   - **Keyboard first, vim bindings:** `j`/`k` and arrows to move, `g`/`G` for top and bottom, `Enter` to open, `Esc` to go back, `/` to filter the current view, `:` for a command line that switches views (`:db`, `:nodes`, `:leases`, `:sinks`) and runs actions, `?` for help that lists every binding, `q` to quit. Every action has a key; nothing needs a mouse.
   - **The terminal's 16 ANSI colours only**, never 256-colour or RGB values, so it inherits the user's terminal theme: a light theme stays light, and an Omarchy theme switch recolours it with everything else. Emphasis uses bold and reverse video as well as colour, and `NO_COLOR` turns colour off.
   - **Live views come from the streaming RPCs:** membership and liveness from `WatchMembership`, leases from `WatchLeases` through the client crate's fan-out. Views without a stream (databases, sinks) refresh by polling `ListDatabases` and `NodeStatus` at a stated interval. A new view that needs a new RPC gets that RPC through an ADR, for every surface, not a private endpoint.
   - **Read-only first.** Actions come after, each with the same confirmation rules as its CLI command (a typed name for anything irreversible).

4. **A desktop app on Tauri with React** (FR-058).
   - The Rust side is a thin Tauri layer over `fleetring-client`: it holds the identity and the connections, and exposes commands that return the client crate's types.
   - The web view never sees a private key or a token and never speaks gRPC; it calls Tauri commands and renders their JSON.
   - It starts as a viewer, with the TUI's views. Actions come later, under the same rules as the TUI.
   - It lives in `apps/desktop`, outside the Cargo workspace, so its toolchain, its JavaScript dependencies, and its platform webview libraries don't touch the crates' MSRV or the cross-target CI matrix (ADR-0009). It may embed the `fleetring` crate for offline "what if" placement, but fleet state always comes from the RPCs.

5. **One vocabulary.** All three surfaces use the [lexicon](../lexicon.md)'s words for commands, view titles, column headers, field names, and messages. A surface that needs a word the lexicon lacks adds it to the lexicon first. A column called `PRIMARY` in `db list` is called Primary in the desktop app, never Leader or Owner.

6. **The same RPCs and the same authorization.** No surface has an API of its own or a way around scopes. Each acts as a principal, sees what its scopes allow (listings are filtered, ADR-0026), and shows a refusal as one: the action and the database it needed.

## Alternatives considered

- **Everything in the `fleetringd` crate.** One crate is simpler today, but the TUI's and the desktop app's dependencies would land in the daemon's build, and nothing would stop a surface from reaching into daemon internals.
- **A web UI served by every node.** One less app to install, but every node would grow an HTTP server, static assets, a session model, and CSRF defences: a new attack surface on the machines that hold the data. A desktop app talks to the same mutual-TLS gRPC port the CLI does.
- **The web view speaking gRPC-web.** It would need a proxy, and the client's private key and tokens in JavaScript.
- **Electron.** Ships a browser and a Node runtime with every copy, and the backend wouldn't be Rust, so it couldn't use the client crate.
- **A native Rust GUI (egui, iced, Slint).** One language end to end, but less mature accessibility, theming, and table components than the web ecosystem, and fewer people who can contribute views.
- **A TUI with its own theme.** Fixed RGB colours look right in one terminal theme and wrong in the rest; inheriting the 16 colours is what makes a TUI feel native on a themed desktop.

## Consequences

- New crates: `fleetring-client` (FR-056) and `fleetring-tui` (FR-057), each held to ADR-0009's lint and MSRV policy. `apps/desktop` gets its own CI workflow, run when its files change.
- `fleetringd`'s CLI moves onto the client crate, so its JSON output and the other surfaces' data come from the same types.
- Gaps the views will hit:
  - **Audit lines** are logs on each node; no RPC streams them. The TUI's audit view needs one, such as a fleet-scope `WatchAudit` per node, decided in an ADR before FR-057's audit view is built.
  - **No watch for databases.** The database list polls `ListDatabases` until a `WatchDatabases` stream is worth adding.
  - **`WatchLeases` is per node**, so the fleet-wide leases view opens one stream per live node; at E4's scale (thousands of nodes) that needs a fleet-wide stream from the control plane.
  - **`WatchMembership` needs the fleet scope**, so a principal without it sees no nodes view, only its databases.
- The lexicon becomes a review checklist for UI changes as well as code.
