# ADR-0032: "Fleet" replaces "cluster" for the nodes one control plane and one authority govern

- **Status:** Proposed
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

The rename has to happen before 1.0, while breaking changes are cheap, and it has to keep working configurations, scripts, certificates, and issued tokens working.

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

3. **Wire and on-disk formats don't change.**
   - **Certificates:** the URI is `fleetring://<name>/node/<id>`, and the word never appears in it, so every issued certificate stays valid. Docs write the placeholder as `<fleet>`.
   - **Tokens:** `iss` holds the fleet name, as it held the cluster name. Unchanged.
   - **The protocol:** no message or field is named `cluster`; only comments change. No protocol version bump.
   - **The control log:** holds no scope strings or cluster names. Unchanged.

4. **The scope string has two spellings during 0.x.**
   - Parsing accepts `fleet` and `cluster` as the same scope, everywhere: `[[principal]] scopes`, `token issue --scope`, and the `fleetring.scopes` claim of a token a node verifies.
   - **Writing keeps `cluster` for one minor release.** In the release that first accepts `fleet` (N), `token issue` still writes `cluster` into the claim, whichever spelling it was given, so that a token minted during a rolling upgrade works on nodes still running N−1, which know only `cluster`. From N+1, which the compatibility policy of FR-054 allows to run only beside N, it writes `fleet`.
   - A `[[principal]]` entry that says `cluster` logs a deprecation warning at start, naming the principal.

5. **The CLI keeps `--cluster` as a hidden alias** for `--fleet`, through every 0.x release. Using it prints `warning: --cluster is deprecated and will be removed in 1.0; use --fleet` once, to stderr. Contexts (FR-056) are new and accept only `fleet`.

6. **Rust items are renamed outright.** `fleetringd` isn't published to crates.io (`publish = false`), so `ClusterName`, `sim::Cluster`, and the `cluster()` methods have no users outside this repository, and no deprecated aliases are kept. The `fleetring` crate has no "cluster" in its API.

7. **Removal.** The `cluster` scope spelling and the `--cluster` flag are removed no earlier than 1.0, listed under breaking changes in that release's notes. Tokens last 30 days at most, so by then no token that still says `cluster` is valid.

8. **What keeps the word.** Accepted ADRs and finished tickets keep their text; this ADR maps the old word to the new. "Cluster" stays where it names something else: a Kubernetes cluster, a Postgres database cluster, the systems ADR-0025 compares, and the placement crate's `kv_sharding` example's "Redis-style cluster".

9. **"Fleet" as prose.** The README's "fleets of SQLite, libSQL, and Turso databases" uses the everyday sense, many databases, and may stay. In names, flags, types, and messages, "fleet" means only decision 1.

## Alternatives considered

- **Keep "cluster".** No work, but it keeps the Postgres and Kubernetes collisions, and the placement crate and the product name already say fleet.
- **"Deployment."** Collides with Kubernetes Deployments, which is exactly where some fleets will run.
- **"Ring."** A ring is the placement structure, and a fleet with cells holds one ring per cell plus a selector ring.
- **"Trust domain" or "realm."** Accurate for the authority's reach (a SPIFFE trust domain is the same idea), but security jargon for what is also the unit of membership and control.
- **A hard rename without aliases.** Every `[[principal]]` entry with `cluster`, every script with `--cluster`, and every token issued before the upgrade would break at once, and a rolling upgrade would refuse tokens on the nodes upgraded last.
- **Writing `fleet` into tokens at once.** Simpler, but a token minted by an upgraded `fleetringd token issue` would be refused by older nodes during a rolling upgrade, which is the case FR-054 exists to make safe.

## Consequences

- FR-059 implements it: the renames, both scope spellings, the `--cluster` alias and its warning, the token-writing rule, and tests that every old spelling still works and warns.
- Docs change in one pass: both READMEs, `deploy/fleetringd.toml`, `SECURITY.md`, `docs/mvp.md`, `docs/roadmap.md`, `docs/review.md`, and the protocol's comments.
- On acceptance, mark ADR-0025 and ADR-0026 as amended by this ADR for the scope's name; their decisions are otherwise unchanged.
- The deprecation path adds two branches (a scope alias and a flag alias) that stay until 1.0.
- Contexts, the TUI, and the desktop app (ADR-0033) start with "fleet" and never carry the old word.
