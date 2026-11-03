---
id: FR-059
title: Rename cluster to fleet, keeping the old spellings as deprecated aliases
status: todo
priority: P1
size: M
milestone: M11 Operator experience
depends_on: [FR-047]
adrs: [ADR-0032]
---

# FR-059: Rename cluster to fleet, keeping the old spellings as deprecated aliases

## Goal

ADR-0032 makes "fleet" the word for the nodes one control plane and one certificate authority govern. This ticket carries that decision through the code, the CLI, configuration, and the docs. Every old spelling keeps working through 0.x with a warning, so no configuration, script, certificate, or token breaks.

## Scope

- **Rust renames, with no aliases** (`fleetringd` is unpublished; ADR-0032, decision 6):
  - `ClusterName` becomes `FleetName`.
  - The `cluster()` accessors on `SubjectUri`, `Authority`, and the loaded identity become `fleet()`.
  - `Scope::cluster()` becomes `Scope::fleet()`, and the `Requirement::Cluster` and `Needed::Cluster` variants become `Fleet`.
  - `sim::Cluster` and `ClusterBuilder` become `sim::Fleet` and `FleetBuilder`. `tests/sim/cluster.rs` becomes `fleet.rs`, and the seed rerun commands it prints change with it.
- **The scope:**
  - `fleet` and `cluster` parse to the same scope in `[[principal]]` entries, in `token issue --scope`, and in a verified token's `fleetring.scopes` claim.
  - A `[[principal]]` entry that says `cluster` logs a warning at start that names the principal.
  - `token issue` writes `cluster` into tokens in the release that ships this ticket. The release after it writes `fleet`, as ADR-0032, decision 4, says. Add a test that fails when the version moves past that release, so the switch isn't forgotten.
- **The CLI:**
  - `cert init --fleet <name>`, with `--cluster` as a hidden alias. The alias prints `warning: --cluster is deprecated and will be removed in 1.0; use --fleet` once, to stderr.
  - Help text and errors say fleet: "the fleet scope", and "fleet '…' is not this fleet '…'".
- **Logs:** "joined the fleet", "left the fleet", and the other membership messages.
- **The protocol:** only comments change, from `Scope: cluster.` to `Scope: fleet.` The `fleetringd::auth::RPCS` table and the test that keeps the two equal follow.
- **Docs:**
  - Both READMEs, `deploy/fleetringd.toml`, `SECURITY.md`, `docs/mvp.md`, `docs/roadmap.md`, and `docs/review.md`.
  - The [lexicon](../lexicon.md): drop the "rename pending" marks and the conflict row.
  - The [CLI style guide](../cli.md)'s "where `fleetringd` stands" table.
  - Accepted ADRs and finished tickets keep their text (ADR-0032, decision 8).
- **Not in scope:**
  - Changing the certificate URI or the token's `iss`; neither contains the word.
  - Removing the aliases, which waits for 1.0.

## Acceptance criteria

- [ ] `fleetringd cert init --fleet home` and `--cluster home` produce equivalent authorities. Only the second warns, and only on stderr.
- [ ] Certificates issued before the rename still connect, with a test that uses a fixture certificate issued by 0.2.0.
- [ ] A token whose claim says `cluster` and one that says `fleet` both pass a fleet-scope check, with a test for each.
- [ ] A `[[principal]]` entry with `cluster` works and logs one warning naming the principal. One with `fleet` works without a warning.
- [ ] Tokens from `token issue --scope fleet` carry `cluster` in this release, and a test records when that changes.
- [ ] Refusal messages say "the fleet scope", and the RPC table test passes against the updated protocol comments.
- [ ] Outside accepted ADRs, finished tickets, and the places ADR-0032 keeps, `grep -ri cluster` finds no use of the word for a fleet in `crates/`, `deploy/`, the READMEs, or `docs/`.

## Progress

- Not started.
