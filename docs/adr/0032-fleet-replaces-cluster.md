# ADR-0032: "Fleet" replaces "cluster" for the nodes one control plane and one authority govern

- **Status:** Accepted (2026-11-03)
- **Date:** 2026-11-03
- **Related:** ADR-0006 (topology), ADR-0012 (cells), ADR-0017 (control plane), ADR-0025 and ADR-0026 (principals and scopes), ADR-0033 (surfaces); tickets FR-054, FR-056, FR-059; [lexicon](../lexicon.md)

## Context

The code and docs call the set of nodes that one control plane and one certificate authority govern a *cluster*:

- `fleetringd cert init --cluster <name>` names the authority, and the `ClusterName` type carries the name.
- Every certificate names it in its URI subject alternative name, `fleetring://<cluster>/node/<id>` or `fleetring://<cluster>/principal/<name>`, and every token carries it in its `iss` claim. A node refuses a certificate or token for another cluster.
- The scope for operations that name no database is the word `cluster`, in `[[principal]]` entries, in tokens' `fleetring.scopes` claim, in `token issue --scope`, in the protocol's `Scope:` comments, and in refusal messages ("Drain needs the cluster scope").
- Logs say "joined the cluster", the simulation harness has `sim::Cluster` and `ClusterBuilder`, and the READMEs, `deploy/fleetringd.toml`, and `SECURITY.md` use the word throughout.

Meanwhile other parts already say *fleet*. The product is FleetRing. The placement crate's test fixtures call a set of nodes a fleet (`fixtures::fleet(12, 3)`, `fleet_1000()`), ADR-0012's example builds `let fleet = Cells::builder()…`, and `docs/mvp.md` connects with `fleet.db("…")`.

"Cluster" also fits badly:

- **It collides with the engines FleetRing will run.** In Postgres, a "database cluster" is the set of databases one server instance manages, and a Postgres adapter is planned (ADR-0016). "Cluster" would mean one Postgres server in the adapter and a whole FleetRing deployment above it.
- **It collides with where FleetRing runs.** "A three-node fleetringd cluster on a Kubernetes cluster" is ambiguous; "a fleet on a Kubernetes cluster" isn't.
- **It suggests one co-located, homogeneous group.** A FleetRing deployment spans home machines and cloud VMs, cells and zones (ADR-0012), and the word should hold all of that.

The rename has to happen before 1.0, while breaking changes are cheap. Nobody runs `fleetringd` in production yet, so 0.3.0 is the cheapest moment there will be.

## Decision

1. **A fleet is the set of nodes that one control plane and one certificate authority govern, under one fleet name.** A fleet contains cells, cells contain zones, zones contain hosts, and hosts run nodes, with the meaning ADR-0006 and ADR-0012 give them: cells partition keys, zones and hosts are labels that spread a key's replicas within its cell. A fleet without cells is one cell. The [lexicon](../lexicon.md) is the reference.

2. **New names.** Every user-facing spelling changes:

   | Was | Becomes |
   |---|---|
   | `cert init --cluster <name>` | `cert init --fleet <name>` |
   | the `cluster` scope | the `fleet` scope |
   | `ClusterName` | `FleetName` |
   | `SubjectUri::cluster()`, `Identity::cluster()`, `Authority::cluster()` | `…::fleet()` |
   | `sim::Cluster`, `ClusterBuilder` | `sim::Fleet`, `FleetBuilder` |
   | "joined the cluster", "the cluster scope", "cluster '…' is not this cluster '…'" | "joined the fleet", "the fleet scope", "fleet '…' is not this fleet '…'" |
   | "cluster" in help text, the READMEs, `deploy/fleetringd.toml`, `SECURITY.md`, and the protocol's comments | "fleet" |

   The old spellings stop working; see decision 3.

3. **A clean break in 0.3.0, with no aliases.** Nobody runs `fleetringd` in production yet, so the rename is done once, completely, rather than carried through 0.x:
   - `--cluster` and the `cluster` scope stop working in 0.3.0. A configuration, script, or token that still says `cluster` is refused like any unknown flag or scope; the scope's parse error names the valid spellings, `<action>:<pattern>` or `fleet`.
   - `token issue` writes `fleet` from 0.3.0.
   - The release notes list every rename under breaking changes (FR-062 keeps the list).

4. **Wire names change where they conflict with the [lexicon](../lexicon.md); wire numbers don't.**
   - **The protocol:** no message or field is named `cluster`, so this rename changes only comments. Other conflicting names in the protocol (FR-062) are renamed in the same release: message, enum, and field names may change, but field numbers and RPC paths stay, so the binary encoding is unchanged and no protocol version bump is needed. Nothing serializes the protocol as JSON or looks types up by name.
   - **Certificates:** the URI is `fleetring://<name>/node/<id>`, and the word never appears in it, so every issued certificate stays valid. Docs write the placeholder as `<fleet>`.
   - **Tokens:** `iss` holds the fleet name, as it held the cluster name. The scope claim changes with decision 3, so a token minted by 0.2 with `cluster` is refused by 0.3 for fleet-scoped calls, and a 0.3 token is refused by 0.2: upgrade every node, then reissue fleet-scoped tokens.
   - **The control log and data directories:** they hold no scope strings or cluster names, and the renames change no persisted field. A 0.2 data directory opens in 0.3. A later rename that does change what is persisted makes the daemon refuse an old data directory with a clear error, rather than migrate or misread it.
   - **The placement spec and its golden vectors** are a separate wire format (ADR-0008) and don't change.

5. **The CLI has no hidden aliases.** `cert init --fleet` replaces `--cluster` outright, and the same holds for every other rename FR-062 makes. Contexts (FR-056) are new and only ever say `fleet`.

6. **Rust items are renamed outright.** `fleetringd`, `fleetring-control`, and `fleetring-proto` aren't published to crates.io (`publish = false`), so `ClusterName`, `sim::Cluster`, and the `cluster()` methods have no users outside this repository, and no deprecated aliases are kept. The `fleetring` crate has no "cluster" in its API and doesn't change.

7. **Accepted ADRs and finished tickets aren't rewritten.** ADR-0025 and ADR-0026 say `cluster` for the scope; this ADR amends their scope's name, and their decisions are otherwise unchanged.

8. **What keeps the word.** Accepted ADRs and finished tickets keep their text; this ADR maps the old word to the new. "Cluster" stays where it names something else: a Kubernetes cluster, a Postgres database cluster, the systems ADR-0025 compares, and the placement crate, whose docs use the word in its general sense (a Redis-style cluster in the `kv_sharding` example, "small clusters" in diagnostics): the crate knows no fleet (ADR-0011) and is published, so it changes only in its own release.

9. **"Fleet" as prose.** The README's "fleets of SQLite, libSQL, and Turso databases" uses the everyday sense, many databases, and may stay. In names, flags, types, and messages, "fleet" means only decision 1.

## Alternatives considered

- **Keep "cluster".** No work, but it keeps the Postgres and Kubernetes collisions, and the placement crate and the product name already say fleet.
- **"Deployment."** Collides with Kubernetes Deployments, which is exactly where some fleets will run.
- **"Ring."** A ring is the placement structure, and a fleet with cells holds one ring per cell plus a selector ring.
- **"Trust domain" or "realm."** Accurate for the authority's reach (a SPIFFE trust domain is the same idea), but security jargon for what is also the unit of membership and control.
- **Aliases through 0.x.** An earlier draft of this ADR kept `--cluster` and the `cluster` scope as deprecated aliases until 1.0, with `token issue` writing `cluster` for one more minor release so tokens survived a rolling upgrade. It protects configurations and tokens that nobody has yet, at the price of two code paths, a version-pinned test, and a lexicon full of exceptions until 1.0.
- **Keeping protocol names that conflict with the lexicon.** Safe, since only numbers reach the wire, but the generated types are what every client reads, and a second rename later would break more people.

## Consequences

- FR-059 implements it: the renames, the `fleet` scope, and tests that the old spellings are refused. FR-062 makes the other lexicon renames in the same release, and keeps the breaking-changes list for the release notes.
- Docs change in one pass: both READMEs, `deploy/fleetringd.toml`, `SECURITY.md`, `docs/mvp.md`, `docs/roadmap.md`, `docs/review.md`, and the protocol's comments.
- On acceptance, mark ADR-0025 and ADR-0026 as amended by this ADR for the scope's name; their decisions are otherwise unchanged.
- Upgrading from 0.2 is all nodes, then new tokens; there is no mixed-version window for fleet-scoped tokens.
- Contexts, the TUI, and the desktop app (ADR-0033) start with "fleet" and never carry the old word.
