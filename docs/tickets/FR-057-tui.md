---
id: FR-057
title: A k9s-style TUI for watching and operating a fleet
status: todo
priority: P2
size: L
milestone: M11 Operator experience
depends_on: [FR-056, FR-042]
adrs: [ADR-0033]
---

# FR-057: A k9s-style TUI for watching and operating a fleet

## Goal

Operating a fleet from a terminal today means re-running `status` and `db list`, and following leases one node at a time with `watch-leases`. A full-screen terminal UI over the existing watch streams shows the fleet as it changes: leases moving during a drain, a node going down, a sink falling behind. It follows ADR-0033: its own crate over `fleetring-client`, vim bindings, and the terminal's 16 colours, so it looks like the rest of a themed, keyboard-first desktop.

## Decision: `fleetringd ui`, from a crate of its own

The TUI lives in a library crate, `fleetring-tui`, that depends on `fleetring-client` and never on `fleetringd`. The `fleetringd` binary links it behind a `ui` Cargo feature and starts it with `fleetringd ui`.

- **One binary to install,** as ADR-0015 promises: whoever has the CLI has the TUI, with the same contexts and identity.
- **The crate boundary keeps it honest:** the TUI can only do what the client crate, and so the RPCs, allow.
- **The release archives build with `ui`**; the container image builds without it, since nobody runs a TUI inside the daemon's container, which keeps `ratatui` and `crossterm` out of the image.
- A separate `fleetring-tui` binary was considered: a smaller daemon build, but a second thing to install, version, sign, and keep in step with the CLI's contexts. If the feature's size or MSRV cost becomes a problem, the crate can ship a binary of its own without changing anything else.

## Scope

- **Views**, switched with `:` (`:fleet`, `:nodes`, `:db`, `:leases`, `:sinks`, `:audit`) or a number key:
  - **Fleet:** the fleet name, context, principal, membership epoch, ring fingerprint, and counts of live, down, and draining nodes and of databases. From `WatchMembership` and `ListDatabases`.
  - **Nodes:** each member with liveness, draining, zone, host, weight, address, and how many databases it is primary for. Live, from `WatchMembership`.
  - **Databases:** name, engine, lease epoch, replication factor, durability, primary, and replicas; `Enter` opens one with its replicas' positions and liveness (`Locate`). Polled from `ListDatabases`.
  - **Leases:** every lease the fleet holds, with `gained` and `lost` events flashing as they arrive. Live, from the client crate's `WatchLeases` fan-out over every live node.
  - **Sinks:** each node's change sinks with position and backlog. Polled from `NodeStatus`.
  - **Audit:** refused and allowed requests. No RPC streams audit lines today, so this view waits for an ADR that adds one (ADR-0033's consequences); until then it shows how to read them from a node's logs.
- **Keys:** `j`/`k` and arrows, `g`/`G`, `Enter`, `Esc`, `/` to filter the current view (substring, and `!` to invert), `:` for views and commands, `?` for help listing every binding, `q` to quit, `Ctrl-C` to quit from anywhere. The bindings live in one table that the help view renders, so they can't drift.
- **Look:** the 16 ANSI colours only, plus bold and reverse video; no RGB or 256-colour values anywhere, enforced by a test over the theme module. `NO_COLOR` gives a monochrome UI that still marks state with text (`down`, `draining`).
- **Read-only first.** The first release has no actions.
- **Actions second,** each through the same RPC and with the same confirmation as its CLI command: drain a node (`:drain`), transfer a lease, drop a database (typed name), revoke a token. Each shows the CLI command it is equivalent to.
- **Names** from the [lexicon](../lexicon.md): view titles, column headers, and messages use its words.
- **Resilience:** a dropped stream reconnects with backoff and shows that the view is stale; a refused view (no fleet scope for `:nodes`) says which scope it needs.
- **Tests:** views rendered against `ratatui`'s test backend from recorded client events, and key handling as a pure function from state and key to state.
- **Not in scope:** mouse support beyond scrolling, editing configuration, the desktop app (FR-058).

## Acceptance criteria

- [ ] `fleetringd ui` opens on the current context and shows the fleet, nodes, databases, leases, and sinks views; `fleetringd ui --context lab` opens another.
- [ ] Draining a node in another terminal shows its leases moving to followers in the leases view within a second of each transfer, without a refresh.
- [ ] Every binding works as listed, and `?` lists exactly the bindings the key table holds.
- [ ] The theme test fails on any colour outside the 16 ANSI colours.
- [ ] With `NO_COLOR` set, the UI uses no colour, and state is still readable.
- [ ] A principal without the fleet scope sees its databases and a message on the nodes view naming the scope it lacks.
- [ ] The container image doesn't contain the TUI.
- [ ] (Second stage) each action asks as its CLI command does and refuses without confirmation.

## Progress

- Not started.
