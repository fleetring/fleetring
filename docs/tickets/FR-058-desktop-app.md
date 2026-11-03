---
id: FR-058
title: A Tauri and React desktop app, starting as a viewer
status: todo
priority: P2
size: L
milestone: M11 Operator experience
depends_on: [FR-056]
adrs: [ADR-0033]
---

# FR-058: A Tauri and React desktop app, starting as a viewer

## Goal

Some people who run a fleet, or depend on one, want a window rather than a terminal: to see which databases exist, where each lives, and whether every node is live. ADR-0033 settles the shape: a Tauri app whose Rust side uses `fleetring-client`, with a React front end that never holds a key or speaks gRPC. This ticket builds it as a read-only viewer.

## Scope

- **Layout:** `apps/desktop/`, outside the Cargo workspace.
  - `src-tauri/` is a Rust crate with its own lockfile that depends on `fleetring-client` by path.
  - `src/` is React with TypeScript, built with Vite.
  - The crates' MSRV, lints, and cross-target matrix don't apply to it. It follows ADR-0009's spirit: `clippy -D warnings`, `cargo deny`, and a lockfile for npm.
- **The Rust side:**
  - It loads contexts through the client crate and holds identities and connections.
  - It exposes Tauri commands that return the client crate's serializable types: `contexts`, `fleet_status`, `databases`, `locate`, `nodes`, and event streams for membership and leases.
  - Tauri's capabilities allow only those commands: no shell, no arbitrary file system, and no network from the web view.
- **The React side:**
  - Views matching the TUI's: fleet, nodes, databases with a detail pane, and leases (live).
  - A context picker.
  - Light and dark themes that follow the operating system.
  - Column headers and labels from the [lexicon](../lexicon.md). The TypeScript types are generated from the Rust types (with `ts-rs` or `specta`), so field names can't drift.
- **Security:**
  - Private keys and tokens stay in the Rust process.
  - The content security policy allows no remote origins.
  - The web view loads only bundled assets.
- **CI:** a workflow that runs when `apps/desktop/**` or `crates/fleetring-client/**` changes. It type-checks, lints, and tests the front end, builds the app on Linux, macOS, and Windows, and runs the Rust side's tests. Dependabot covers its npm and Cargo dependencies.
- **Packaging:** unsigned development builds as workflow artifacts. Signed, notarized releases are a later ticket, since they need signing identities this repository doesn't have yet.
- **Not in scope:** actions (a later ticket, with the TUI's confirmation rules), the WASM ring playground, auto-update.

## Acceptance criteria

- [ ] The app opens on the current context and shows the fleet, nodes, databases, and leases views. The leases view updates live during a drain.
- [ ] Switching context in the app switches the fleet it shows, with the identity that context names.
- [ ] Searching the web view's bundle and its IPC finds no key material or token, and the capability file allows only the listed commands.
- [ ] The TypeScript types are generated from the client crate. A renamed Rust field fails the front end's type check.
- [ ] CI builds the app on three operating systems when its files change, and doesn't run otherwise.

## Progress

- Not started.
