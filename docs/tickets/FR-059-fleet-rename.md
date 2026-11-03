---
id: FR-059
title: Rename cluster to fleet, as a clean break in 0.3.0
status: done
priority: P1
size: M
milestone: M11 Operator experience
depends_on: [FR-047]
adrs: [ADR-0032]
---

# FR-059: Rename cluster to fleet, as a clean break in 0.3.0

## Goal

ADR-0032 makes "fleet" the word for the nodes one control plane and one certificate authority govern. This ticket carries that decision through the code, the CLI, configuration, the protocol's comments, and the docs. Nobody runs `fleetringd` in production yet, so 0.3.0 makes the change outright: the old spellings stop working, and no alias or deprecation path is kept (ADR-0032, decision 3). FR-062 makes the lexicon's other renames in the same release and keeps the breaking-changes list.

## Scope

- **Rust renames** (`fleetringd` is unpublished; ADR-0032, decision 6):
  - `ClusterName` becomes `FleetName`.
  - The `cluster()` accessors on `SubjectUri`, `Authority`, and the loaded identity become `fleet()`.
  - `Scope::cluster()` becomes `Scope::fleet()`, and the `Requirement::Cluster`, `Needed::Cluster`, and `Target::Cluster` variants become `Fleet`.
  - `sim::Cluster` and `ClusterBuilder` become `sim::Fleet` and `FleetBuilder`. `src/sim/cluster.rs` and `tests/sim/cluster.rs` become `fleet.rs`, and the seed rerun commands they print change with them.
- **The scope:** `fleet` is the only spelling, in `[[principal]]` entries, in `token issue --scope`, and in a verified token's `fleetring.scopes` claim. `cluster` fails to parse, and the error names the valid forms. `token issue` writes `fleet`.
- **The CLI:**
  - `cert init --fleet <name>`; `--cluster` is an unknown flag.
  - Help text and errors say fleet: "the fleet scope", and "fleet '…' is not this fleet '…'".
- **Logs:** "joined the fleet", "left the fleet", and the other membership messages.
- **The protocol:** only comments change, from `Scope: cluster.` to `Scope: fleet.` The `fleetringd::auth::RPCS` table and the test that keeps the two equal follow.
- **Docs:**
  - Both READMEs, `deploy/fleetringd.toml`, `SECURITY.md`, `docs/mvp.md`, `docs/roadmap.md`, and `docs/review.md`.
  - The [lexicon](../lexicon.md): drop the "rename pending" marks and the conflict row.
  - The [CLI style guide](../cli.md)'s "where `fleetringd` stands" table.
  - Accepted ADRs and finished tickets keep their text (ADR-0032, decision 7).
- **Not in scope:** changing the certificate URI or the token's `iss`, neither of which contains the word; the `fleetring` placement crate, which uses "cluster" only in its general sense.

## Acceptance criteria

- [x] `fleetringd cert init --fleet home` creates an authority for `home`, and `--cluster home` is refused as an unknown flag.
- [x] Certificates issued before the rename still connect, with a test that uses fixture certificates issued by 0.2.0.
- [x] A token whose claim says `fleet` passes a fleet-scope check, and one that says `cluster` is refused, with a test for each.
- [x] A `[[principal]]` entry with `fleet` works, and one with `cluster` fails to load with an error naming the scope.
- [x] Tokens from `token issue --scope fleet` carry `fleet`.
- [x] Refusal messages say "the fleet scope", and the RPC table test passes against the updated protocol comments.
- [x] Outside accepted ADRs, finished tickets, and the places ADR-0032 keeps, `grep -ri cluster` finds no use of the word for a fleet in `crates/`, `deploy/`, the READMEs, or `docs/`.

## Progress

- 2026-11-03: Implemented on the `naming` branch with FR-062, after ADR-0032 was revised to a clean break.
  - **Code:** `FleetName`, `fleet()` accessors, `Scope::fleet()`, `Target::Fleet`, `Requirement::Fleet`, `Needed::Fleet`, `sim::Fleet` and `FleetBuilder` (`src/sim/fleet.rs`, `tests/sim/fleet.rs`, and the rerun commands they print), `cert init --fleet`, and "fleet" in help, logs, refusals, the protocol's comments, the daemon README, and `deploy/fleetringd.toml`.
  - **Tests:** `cert_init_names_the_fleet_and_refuses_the_old_flag` and `certificates_from_0_2_still_connect` (`tests/tls.rs`, with certificates the 0.2.0 binary issued in `tests/fixtures/tls-0.2.0/`, the authority's private key removed); `the_fleet_scope_is_written_and_read_as_fleet_and_cluster_is_refused` (`auth::token`); the `cluster` scope in `scopes_parse_and_print_back` and in `principals_are_parsed_and_validated_at_start`.
  - **Kept:** "cluster" where it names something else (other systems in `docs/review.md`), in accepted ADRs and finished tickets, and in the `fleetring` placement crate's docs, which use it in the general sense and change only in that crate's own release.
- 2026-11-03: Merged in PR #48; status is done.
