# Releasing `fleetring`

This is the checklist for publishing the `fleetring` crate (FR-019). A release freezes placement behavior as well as the API, so work through it in order. The `fleetringd` daemon has its own process, in [Releasing `fleetringd`](#releasing-fleetringd).

## 1. Pre-flight

- [ ] Every issue labelled `P0` or `P1` in the release's [milestone](https://github.com/fleetring/fleetring/milestones) is closed.
- [ ] CI is green on `main`, including the cross-target jobs (i686, s390x, wasm32-wasip1) and `spec-reference`.
- [ ] The nightly `Fuzz` workflow has passed recently.
- [ ] `docs/perf.md` reflects current numbers (`cargo bench -p fleetring --bench lookup`).

## 2. Freeze the placement spec (first release of a spec version only)

- [ ] Change `docs/spec/placement-v1.md` from `Status: Draft` to `Status: Final`.
- [ ] Confirm `crates/fleetring/tests/vectors/placement-v1.lock` covers every case. From this release on, frozen cases never change. Any change to which nodes a key maps to needs spec v2 (ADR-0008).
- [ ] Review ADR statuses. Move each `Proposed` ADR that now describes shipped behavior to `Accepted`.

## 3. Version and changelog

- [ ] Set `version` in `[workspace.package]` (root `Cargo.toml`).
- [ ] In `CHANGELOG.md`, rename `[Unreleased]` to `[0.1.0] - YYYY-MM-DD`, start a new empty `[Unreleased]`, and update the compare links.
- [ ] Note any MSRV change. Policy: raise it only in a minor release, never to a toolchain younger than six months (ADR-0009).

## 4. Verify the package

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test -p fleetring --all-features --release -- --ignored
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +1.88 check --workspace --exclude fleetringd --exclude fleetring-client --exclude fleetring-wal-sqlite-tests --all-features --all-targets
cargo +1.89 check -p fleetringd -p fleetring-client -p fleetring-wal-sqlite-tests --all-features --all-targets
cargo package -p fleetring --list      # review: no stray or oversized files
cargo publish -p fleetring --dry-run
```

## 5. Tag and publish

1. Commit: `chore(release): fleetring 0.1.0`.
2. Tag: `git tag -a fleetring-v0.1.0 -m "fleetring 0.1.0"`.
3. Push the commit and the tag.
4. Publish (the maintainer does this): `cargo publish -p fleetring`.
5. Create a GitHub release from the tag. Use the changelog entry as the body, with links to the placement spec and the ADR index.
6. Check that docs.rs built the crate with all features.

## 6. Semver checks

The `semver-checks` job in `.github/workflows/ci.yml` compares every change with the latest release on crates.io. It runs this job:

```yaml
  semver:
    name: semver-checks
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
      - uses: taiki-e/install-action@v2
        with:
          tool: cargo-semver-checks
      - run: cargo semver-checks --package fleetring
```

Placement compatibility is separate from API compatibility. `cargo semver-checks` guards the API. The frozen vectors and their lock guard placement, and a placement change needs a new spec version whatever the crate version says.

# Releasing `fleetringd`

The daemon is released separately from the crate (FR-034). Its package is `fleetringd`, and it builds the `fleetring` binary; the package, its `fleetringd-vX.Y.Z` tags and its archive names keep the old name until 1.0, because a 0.6 `upgrade` looks for them (ADR-0043, point 1.7). It isn't published to crates.io (`publish = false`); a release is a set of prebuilt binaries and a container image, made by CI from a `fleetringd-vX.Y.Z` tag. Two workflows run on the tag:

- `.github/workflows/release.yml` builds `fleetring` for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin`, `aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`. It packages each as `fleetringd-<version>-<target>.tar.gz` (`.zip` on Windows), whose directory holds `fleetring` and a regular copy named `fleetringd`, with the licenses, `crates/fleetringd/README.md`, and the man pages, one per command, under `man/` (`fleetring.1`, `fleetring-db-create.1`, …), copied from `crates/fleetringd/man/`; `scripts/check-archive.sh` fails the build unless both copies are identical, each prints its own name and the version, and `man/fleetring.1` is there. It writes `SHA256SUMS` and a `.sha256` per file, signs `SHA256SUMS` keylessly with cosign (one bundle, `SHA256SUMS.sigstore.json`, holding the signature, the certificate, and the transparency-log proof), attests the archives and `scripts/install.sh`, and attaches everything to the tag's GitHub release, creating it if needed. It runs `cargo audit` first and fails on an unresolved advisory.
- `.github/workflows/container.yml` builds the `Dockerfile` natively for `linux/amd64` and `linux/arm64` and pushes one manifest to `ghcr.io/fleetring/fleetring`, and the same manifest to `ghcr.io/fleetring/fleetringd` until 1.0, tagged `X.Y.Z`. A final release also moves `X.Y` and `latest`; a prerelease (`X.Y.Z-rc.1`) gets its own tag only.

## 1. Pre-flight

- [ ] CI is green on `main`, including `test (ubuntu-24.04-arm)` and `simulation (fixed seeds)`.
- [ ] **The rolling upgrade passes from the previous release** (FR-054, ADR-0038): `.github/workflows/rolling-upgrade.yml` starts three nodes on the previous release's binaries, downloaded and verified with cosign, and moves them one at a time to `main` under writes. It runs nightly; if it hasn't run since the last merge, run it by hand (Actions → Rolling upgrade → Run workflow, optionally naming the release to upgrade from). Locally, with cosign installed:

  ```sh
  FLEETRINGD_BASELINE_VERSION=<previous> cargo test -p fleetringd --test upgrade -- --ignored --nocapture rolling
  ```

  **0.7.0 only:** run it with `FLEETRINGD_BASELINE_VERSION=0.6.0`, so that 0.6's own `upgrade` installs the 0.7 archive across the rename (ADR-0043, point 3.2). It must pass before 0.7.0 is tagged.

  If the release changes the protocol, the control log, or a command that older members can't read, its notes say which releases it runs beside, and a new control command is a fleet feature (ADR-0038, point 4).
- [ ] `RUST_VERSION` agrees in `.github/workflows/release.yml` and the `Dockerfile`.
- [ ] **A manual dry run, only if the release or container workflows changed since the last release.** Check with:

  ```sh
  git diff --stat fleetringd-v<previous>..main -- \
    .github/workflows/release.yml .github/workflows/container.yml Dockerfile scripts/install.sh
  ```

  If that prints anything, run both workflows by hand on `main` (Actions → Release → Run workflow, and Actions → Container → Run workflow). A manual run builds and smoke-tests every archive and both images, and publishes nothing; download the `fleetringd-<version>-dist` artifact and try an archive.

  If it prints nothing, skip the dry run. The workflows are the ones that last succeeded, so the tag run is the test, and a run that fails is re-run on the same tag (see [After the workflows finish](#4-after-the-workflows-finish)).

## 2. Version

- [ ] Set `version` in `crates/fleetringd/Cargo.toml` to exactly the version the tag will name; `fleetring --version` prints it. Both workflows fail unless the tag is `fleetringd-v` followed by that version, character for character.
  - **A release candidate** sets it to `X.Y.Z-rc.N` first, and is tagged `fleetringd-vX.Y.Z-rc.N`. The release is marked a prerelease, and the image gets only its own tag.
  - Tagging `-rc.N` while `Cargo.toml` still says `X.Y.Z` fails the `version` job. That is why 0.2.0's `rc.1` tag failed.
  - The final release sets `X.Y.Z` in a commit of its own.
  - **0.7.0's version bump** also removes `knows_serve`'s own-line clause, `|| release.line() >= this_release().line()`, from `crates/fleetringd/src/upgrade/mod.rs`. It only lets a build that still says 0.6.0 reinstall itself over a `serve` node, and does nothing once the version is 0.7.0.
- [ ] Commit: `chore(release): fleetringd X.Y.Z` (or `X.Y.Z-rc.N`).

`CHANGELOG.md` documents the `fleetring` crate only; the daemon's notes go in its GitHub release.

## 3. Tag

```sh
git tag -a fleetringd-vX.Y.Z -m "fleetringd X.Y.Z"
git push origin main fleetringd-vX.Y.Z
```

## 4. After the workflows finish

A failed workflow is re-run on the same tag, once its cause is fixed if it was more than a flake: the release workflow replaces the release's files, and the container workflow pushes the manifest again. Never move a published tag; release a new patch version instead.

### Verify what was published

Run these in an empty directory, against the published release, not a local build:

```sh
V=X.Y.Z
gh release download fleetringd-v$V --repo fleetring/fleetring

# 1. Checksums: the archives and install.sh match SHA256SUMS.
sha256sum --check SHA256SUMS   # shasum -a 256 -c SHA256SUMS on macOS

# 2. The signature: SHA256SUMS was signed by this repository's release workflow, from a fleetringd tag.
cosign verify-blob SHA256SUMS \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/fleetring/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

# 3. Provenance for every archive and the install script.
for file in fleetringd-$V-*.tar.gz fleetringd-$V-*.zip install.sh; do
  gh attestation verify "$file" --repo fleetring/fleetring
done

# 4. Provenance for the container image.
gh attestation verify oci://ghcr.io/fleetring/fleetring:$V --repo fleetring/fleetring
gh attestation verify oci://ghcr.io/fleetring/fleetringd:$V --repo fleetring/fleetring

# 5. The install script installs this version.
curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh \
  | sh -s -- --version $V --prefix "$PWD/prefix"
"$PWD/prefix/bin/fleetring" --version
"$PWD/prefix/bin/fleetringd" --version   # the link, until 1.0
"$PWD/prefix/bin/fleet" --version        # the short name, if no other fleet is on your PATH
MANPATH="$PWD/prefix/share/man" man -w fleetring   # the man pages, from 0.7.0

# 6. The image runs on both architectures.
docker run --rm --platform linux/amd64 ghcr.io/fleetring/fleetring:$V --version
docker run --rm --platform linux/arm64 ghcr.io/fleetring/fleetring:$V --version
docker run --rm --platform linux/amd64 --entrypoint /usr/local/bin/fleetringd ghcr.io/fleetring/fleetringd:$V --version
docker run --rm --platform linux/amd64 --entrypoint /usr/local/bin/fleet ghcr.io/fleetring/fleetring:$V --version
```

- [ ] The release lists five archives, each holding `fleetring` and `fleetringd` (`.exe` on Windows), `SHA256SUMS` with its `.sigstore.json` bundle, a `.sha256` for each file, and `install.sh`.
- [ ] Every checksum passes.
- [ ] `cosign verify-blob` prints `Verified OK`.
- [ ] `gh attestation verify` succeeds for every archive, `install.sh`, and the image.
- [ ] The installed `fleetring` prints `fleetring X.Y.Z`, `fleetringd` prints `fleetringd X.Y.Z`, and `fleet` prints `fleet X.Y.Z` (from the release that adds it; skipped if another `fleet` is on your `PATH`, which `install.sh` says); `man -w fleetring` finds the installed page.
- [ ] The first two `docker run` commands print `fleetring X.Y.Z`, the third `fleetringd X.Y.Z`, and the fourth `fleet X.Y.Z`. On a machine of one architecture, the other runs under emulation (Docker Desktop has it; on Linux, `docker run --privileged --rm tonistiigi/binfmt --install all` adds it).
- [ ] The first time only: make the `fleetring` and `fleetringd` container packages public in the organization's GitHub package settings, and link them to this repository. 0.7.0 is the first release of `ghcr.io/fleetring/fleetring`.
- [ ] Replace the release's notes with the [template](#5-release-notes).
- [ ] If the release fixes a vulnerability, publish its advisory now ([`SECURITY.md`](SECURITY.md#publishing-an-advisory)).

## 5. Release notes

The workflow creates the release with install and verify instructions. Replace its body with this template. Leave out any section with nothing in it, except Verify:

````markdown
## fleetringd X.Y.Z

<One paragraph: what this release is for, and who should upgrade how soon.>

### Breaking changes

- <What changed, who it affects, and what to do: a renamed flag, a changed default, a configuration key, a protocol or on-disk format. Say whether nodes of the previous release can run beside this one.>

### Security

- <GHSA-xxxx-xxxx-xxxx: the advisory's title. Its severity, the affected versions, and what to do after upgrading, with a link to the advisory.>

### Features

- <What a user can now do, with the command or setting, and its ticket (FR-NNN).>

### Fixes

- <What was wrong, as a user saw it, and that it no longer happens.>

### Verify

```sh
cosign verify-blob SHA256SUMS --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/fleetring/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify fleetringd-X.Y.Z-<target>.tar.gz --repo fleetring/fleetring
gh attestation verify oci://ghcr.io/fleetring/fleetring:X.Y.Z --repo fleetring/fleetring
```
````

### 0.7.0: upgrading across the rename

A draft for 0.7.0's notes, under "Breaking changes" (ADR-0043, points 1 and 3). The binary is `fleetring`; `fleetringd` stays a name for it until 1.0, and `fleetringd --config <file>`, without `serve`, still runs a node, with a warning.

**Renamed.** Each old spelling but the two compatibility names fails like any unknown command or flag, with a `hint:` line naming the new one:

| 0.6 | 0.7.0 |
|---|---|
| `fleetringd …` (the binary) | `fleetring …`; `fleetringd` stays a name for it until 1.0 |
| `fleetringd --config <file> [daemon flags]` | `fleetring serve --config <file> [daemon flags]`; the old form still runs a node until 1.0, with a warning |
| `fleetringd db archive show <db> --archive <a>` | `fleetring archive show <db> --archive <a>` |
| `fleetringd db archive verify <db> --archive <a>` | `fleetring archive verify <db> --archive <a>` |
| `fleetringd archive restore --from <db> --to <dir> …` | `fleetring archive restore <db> --to <dir> …` (the database is the argument) |
| `--version` prints `fleetringd X.Y.Z` | prints `fleetring X.Y.Z`, or `fleetringd X.Y.Z` when run as `fleetringd` |
| errors start `fleetringd:` | start `fleetring:`, or `fleetringd:` when run as `fleetringd` |
| the dev banner says `. ~/.fleetring/env` | it names the `dev` context; `~/.fleetring/env` is still written, for scripts |
| `ghcr.io/fleetring/fleetringd`, `ENTRYPOINT fleetringd`, `CMD --config …` | `ghcr.io/fleetring/fleetring` (the old name is pushed too until 1.0), `ENTRYPOINT fleetring`, `CMD serve --config …` |
| `deploy/dev/fleetringd-dev.service`, `homebrew.mxcl.fleetringd`, `opt_bin/"fleetringd"` | `fleetring-dev.service`, `homebrew.mxcl.fleetring`, `opt_bin/"fleetring"` |
| the previous binary is kept as `fleetringd.previous` | it is kept as `<name>.previous` beside the real file: still `fleetringd.previous` on a host upgraded from 0.6, `fleetring.previous` on a fresh 0.7 install |

**Not renamed**, because a 0.6 binary or an existing directory depends on them; they are decided at 1.0:

- the `fleetringd` package and crate (`cargo install … fleetringd`, `-p fleetringd`), whose log targets stay `fleetringd::audit` and `RUST_LOG=fleetringd=debug`;
- the `fleetringd-vX.Y.Z` release tags and the `fleetringd-<version>-<target>.tar.gz` and `.zip` archives;
- `<data_dir>/fleetringd.process`, `<data_dir>/fleetringd.lock` and `FLEETRINGD_RESTARTS`;
- `fleetringd.toml`, dev mode's configuration file, and `deploy/fleetringd.toml`.

**Upgrading from 0.6:**

- **Upgrade from 0.6 as usual,** with `fleetringd upgrade` or `install.sh`. The archives keep their names and hold the binary under both names, so 0.6's `upgrade` installs 0.7 over `fleetringd` and restarts the node in place on its 0.6 arguments.
- **Add the new name.** A host upgraded by 0.6's `upgrade` has no `fleetring`. Add it with `ln -s fleetringd "$(dirname "$(command -v fleetringd)")/fleetring"`, or rerun `install.sh`. A 0.7 `upgrade` or `upgrade --rollback` adds the link itself, and says so. On such a host the real file stays `fleetringd`, and the previous binary is kept as `fleetringd.previous`.
- **After `install.sh` over a 0.6 host, roll back by hand.** `install.sh` makes `fleetring` the real file and keeps no previous binary, so a `fleetringd.previous` that 0.6's `upgrade` kept belongs to nothing: `upgrade --rollback` looks for `fleetring.previous` and says there is none. That file holds the release before the one `install.sh` replaced. To go back to it, move it into place with `mv fleetringd.previous fleetring` in that directory, and restart the node; `fleetringd` follows, since it is a link. To go back to 0.6 itself, run `install.sh --version 0.6.<n>`.
- **A flag now outranks the variables for the identity.** `--as` beats `FLEETRING_CERT` and `FLEETRING_KEY`, where 0.6 merged each flag with its variable and the variables won. The identity (directory, authority, certificate, key, principal and token) now resolves as a unit from the highest source that sets any TLS material, and a warning names both when a higher one replaces a lower one's (ADR-0043, point 4.4).
- **Dotted `agent-` names are refused everywhere** (ADR-0044, point 1.8; #114). A principal name that starts with `agent-` and contains `.` is reserved for delegated agents. 0.7 issues none (`cert client`, `token issue --principal`), a node fails to start with a `[[principal]]` entry or an `[admin] scrapers` name that is one, and a node refuses a client certificate or a token that names one with `UNAUTHENTICATED`. The authority keeps no list of what it issued, so such identities are found where they are used: configurations at the first start on 0.7, certificates and tokens at their first request. Rename and reissue them; other dotted names, such as `workload.prod`, keep working.
- **`fork` and `grant` are reserved scope words,** alone or as actions, and refused wherever a scope is parsed, a token's included (ADR-0043, point 8.11). No scope valid in 0.6 means either.
- **Exit codes have meanings** (`docs/cli.md`, section 8; FR-056). A refusal (`UNAUTHENTICATED`, `PERMISSION_DENIED`) now exits 3 and an unavailable fleet or a timeout exits 4, where 0.6 exited 1 for both; a confirmation a command can't ask without a terminal exits 2. A script that tests for 1 should test for non-zero.
- **More commands ask first, and scripts pass `--yes`.** `token revoke --key-id` asks, and `node drain` asks when the node holds a database's only replica; without a terminal both refuse with exit 2 unless `--yes`. `db drop`, `node remove` and `orphan remove` now ask for the name typed rather than `y`, and every prompt names the fleet and the context.
- **`-q` drops every note and warning,** not only the override warnings: a command run with it prints its result and its errors alone.
- **New database names may not start with `_fleetring`,** which is FleetRing's own namespace. A database that already has such a name stays.
- **Keep service units on `fleetringd --config <file>`** until no node needs to roll back to 0.6. 0.6 doesn't know `serve`. A 0.7 `upgrade --rollback`, or `upgrade --version` with a release before 0.7, refuses to install it over a node started with `serve`, and without a running node to check, it asks, or needs `--yes`. But the 0.6 binary kept as `fleetringd.previous` can't check: `fleetringd.previous upgrade --rollback` installs 0.6 anyway, and a unit that runs `fleetring serve` then fails to start, and its supervisor restarts it in a loop. After switching a unit to `fleetring serve`, rolling back to 0.6 requires reverting the unit to `fleetringd --config` first.

### 0.8.0: `db watch`, `fleet`, and the router

A draft for 0.8.0's notes (ADR-0045; ADR-0037; ADR-0043's amendment of 2026-11-07; ADR-0016's and ADR-0038's amendments of 2026-11-08; FR-066, #69; FR-075, #103; FR-078, #109; FR-081, #126; FR-088, #138; #95; #152; #153; #154).

**Security:**

- **The bundled SQLite moves from 3.50.2 to 3.54.1, for CVE-2026-119375** (RUSTSEC-2026-0459, medium; #152). SQLite before 3.54.0 that opens a database beside a crafted `-wal` file whose page size is smaller than the database's can return uninitialized memory from the process in ordinary query results. SQL can't reach it: the statements a node runs can't attach, create, or choose a database file or set the page size. A node opens such a pair only when it restores from an archive (`fleetring archive restore`, `db restore`) or installs a snapshot or WAL from a peer, so an attacker who can write your archive's object store could read node memory back through a restored database; archive objects carry checksums that catch damage, not forgery. Every release before 0.8.0 is affected. Upgrade, and until every node runs 0.8.0, restore only from archive stores that only your nodes and operators can write. A peer could do the same, but a peer already holds a node's credentials. 3.54 also fixes SQLite's WAL-reset corruption bug, which 3.50.2 carried.
- **Restores and peers' copies are checked before they are opened** (#154). Whatever SQLite is bundled, a node now refuses an archive's snapshot or a peer's snapshot whose WAL's page size differs from its database file's, before anything opens it, and a WAL batch whose page size differs from the follower's database file's; the restore or the seed fails as data loss naming the archive object or the peer, and leaves nothing behind.
- **User statements can no longer run the daemon's own cached statements.** Before 0.8.0, a statement sent through `db exec` or `db query` whose text matched, word for word, one the daemon runs on its reserved tables ran without the confinement's checks: a principal allowed to write a database could rewrite its replicated sink positions, so that a change stream skipped or repeated batches after a failover, and one allowed to read it could read its recorded changes. Statements that mention a reserved name are now always checked. Releases from 0.2.0 on are affected, and from 0.3.0 on for sink positions.
- **The MCP server's `rmcp` moves from 3.8.0 to 3.9.0** (RUSTSEC-2026-0462; #153). The advisory is in `rmcp`'s HTTP clients, which `fleetring mcp` doesn't build or run: it serves over stdio, so no release was affected.

**Features:**

- **`fleetring router --config <file>`** runs the router: a front door that speaks libSQL's Hrana protocol over HTTP, so unchanged libSQL and Turso clients (`@libsql/client`, the `libsql` crate's remote client, Turso's serverless drivers) reach each database's primary through FleetRing (FR-066, #69). Point a client at `https://<router>/db/<database>/`, with the trailing slash, and give it a FleetRing token as its auth token. The router serves Hrana 2 and 3 over HTTP/1.1 and HTTP/2 with JSON: pipelines, cursors, batches with conditions, sequences, `describe`, stored SQL, and transactions across requests, each running with SQLite's own semantics on the primary. It verifies every request's token and checks `read` before it calls any node, then forwards the token, which nodes check again; a token with `read` alone runs `SELECT`s and is refused writes. A drain or failover moves a stream outside a transaction to the new primary with no error; a request whose reply was lost fails with `FLEETRING_OUTCOME_UNKNOWN` and is never sent again. It serves plaintext only on loopback unless told otherwise. `deploy/router.toml` documents its settings; it needs a client certificate for a principal every node lists with `forward_tokens = true` and no scopes. Not yet: Hrana over WebSocket, the protobuf encoding, follower reads, and router metrics.
- **`Database.Session`** is the data-plane stream the router runs each Hrana stream on, with `[sessions] transaction_timeout_ms` (30 seconds) and `max_waiting_writes` (64) bounding a transaction's hold on a database's writer: a write past the bound is refused with `RESOURCE_EXHAUSTED`. It needs `read` on the database; without `write`, the session is read-only (ADR-0037 amends ADR-0026, point 4, for sessions only). The protocol moves to 1.3.

- **`fleetring db watch <db>`** follows a database's row changes from its primary, one committed transaction per line, as the write quorum acknowledges them: from now, or after a position with `--from <position>`. `-o jsonl` (and `--json`) prints one object per transaction: `database`, `start`, `position`, `lease_epoch`, and `changes`, each `table`, `operation`, `key`, `before`, `after` and `updated`. When the primary changes, by a failover or a drain, or the command falls behind, it resumes on the new primary after the last position it printed. It needs the `stream` scope on the database, which `read`, `write` and `admin` don't grant, and a database a logical change sink names; dev mode names every database, and its admin has `stream:*`. A resume after changes the sinks have since pruned is refused, naming the oldest position held, and never skips.
- `fleet` is a permanent short name for `fleetring`: `fleet db list`, `fleet --version`. `install.sh` and the container image link it to `fleetring`; `install.sh` skips it, and says why, if `PREFIX/bin/fleet` is something else or another `fleet` (Rancher Fleet's CLI, JetBrains Fleet's launcher) is on your `PATH`, and `--no-alias` skips it always. The archives are unchanged, and `fleetring upgrade` keeps a `fleet` link working but doesn't add one: rerun `install.sh` for that (#103).
- **`Database.WatchChanges`** is the protocol call behind it, for clients of their own; `fleetringd::data::Client::watch_changes` opens it.
- **Agents follow their own databases' changes** (FR-081, #126). A newly minted agent identity gets `stream:agent-<id>-*` beside `admin:agent-<id>-*`, or beside `read:agent-<id>-*` with `--read-only`; `--scope` still replaces the defaults. `fleetring mcp` gains a `changes` tool, offered with and without `--read-only`: `changes(database, after?, limit?)` returns at most `limit` transactions (20 by default, at most 100) recorded after `after`, or after the database's current position without it, and `next_after`, the position to continue from. A call returns once it has `limit` of them or has caught up, and waits at most 2 seconds; table names, column names and values come back only in string fields, with the usual notice. Nothing holds an agent's place between calls, so one that falls behind the sinks' pruning gets `out_of_range` with `oldest_position`. The agent skill and quickstart gain a "follow changes" pattern.
- **Statements stop at a deadline** (FR-078, #109). A write, query, or `db show` schema read that runs past `[limits] statement_timeout_ms` on its primary (30 seconds unless set), or past the earlier deadline its call carries, is interrupted on the node: a query fails with `DEADLINE_EXCEEDED`, exit 4, with a hint, and a write rolls back, leaving no row, no change record, and nothing for its followers. A cancelled call, such as a client that times out or goes away, stops its statement too, so the database serves its next request at once. `db exec` and `db query` gain `--timeout <n>s`, 30 seconds unless given (before, a call over 10 seconds failed with `CANCELLED`, `Timeout expired`, exit 1); `fleetring mcp` gives each statement 10 seconds; a gRPC client's own `grpc-timeout` is honoured, the node answering just before it with `DEADLINE_EXCEEDED`; `fleetring_writes_total` and `fleetring_queries_total` gain the outcome `cancelled`, for a call dropped before the node answered; `fleetringd::data::Client::with_call_timeout` sets one for code using the client. Before 0.8 a statement ran to its end on the node whatever its client did, holding the database's thread the whole time.
- **Two new scope actions, `fork` and `grant`** (ADR-0044 point 2, FR-088, #138), which 0.7 reserved. `fork:<pattern>` reads the databases it names and is what forking *from* them needs; `read` and `write` don't imply it, and `admin` does. `grant:<pattern>` authorizes no request at all: it bounds what an agent's token may hand a child, and nothing implies it, `admin` included. Both parse in `token issue --scope`, `agent add --scope`, and every node's token verifier; `fork` also in a `[[principal]]` entry, while `grant` there fails validation with "grant has no effect in a certificate's scopes; it is carried by an agent's token". Every existing scope keeps its meaning. `fleetringd::auth::Grant` is renamed `TokenSpec`, and `Verified::grant` is `Verified::spec`; `Scopes::intersect`, `Scopes::grant_step` and `Scopes::grant_step_strict` narrow scopes exactly, for the delegation that follows, and refuse a side of more than 256 scopes (`MAX_SCOPES`). `token issue` and `agent add` refuse to mint a token of more than 256 scopes; nodes still accept one minted before, but its holder can't delegate from it.

- **`fleetring serve --config <file> --check`** says whether a node would start with a configuration and `serve`'s flags, and starts nothing (#95). It runs the checks a start runs before it touches anything, reads none of the files the configuration names, no environment variable and no data directory, and binds nothing. It exits 0 if the node would start, and otherwise prints the first problem, naming the file, the dotted field, and the line and column, and exits 1. `--json` (or `-o json`) prints `config`, `valid`, `node`, `role` and `problems`, each `field`, `line`, `column` and `message`. 0.6's form, `fleetringd --config <file> --check`, works too.
- **`fleetring upgrade` asks the new release whether the node would start before it swaps it in** (#95). With `--config`, it runs the release's own `serve --check` on the node's command line, in the directory the node started in, and a configuration the release refuses stops the upgrade with the release's message and the fix: `--no-restart`, edit the configuration until `serve --check` passes, then restart; until the edit, any restart (a supervisor's, a crash, a reboot) leaves the node down. Only the command line a process file records for `serve`, naming the same `--config`, is run; anything else falls back to `serve --config <file> --check`. `--rollback` asks the previous binary the same way. `upgrade --software` gains `config_check: true`, and the process file records the directory a node started in (`cwd`).

**Fixes:**

- **A database recording row changes no longer stops taking writes after its first recorded transaction fails.** The rollback took the change table and the capture triggers with it, and every later write to the database failed with `no such table: _fleetring_cdc` until the node reopened it.

**Upgrading from 0.7:**

- **Text that SQL makes from a REAL value keeps 17 significant digits,** where 0.7's SQLite kept 15 (SQLite 3.53.0): `CAST(1.0/3 AS TEXT)` is `0.33333333333333332`, not `0.333333333333333`, while `0.1` stays `0.1`. The same goes for `||`, `printf('%s', …)`, `json_array()`, and a REAL stored in a TEXT column. REAL values themselves, and the numbers that queries return, are unchanged. Row changes are recorded as JSON, so `db watch` and the `changes` tool now report a REAL value that a 0.8 primary recorded exactly, where a 0.7 primary rounded it to 15 digits. During a rolling upgrade a 0.7 primary and a 0.8 primary write different text for the same statement; replicas copy the primary's pages, so the copies of a database never disagree, but a database written across the upgrade can hold both forms.
- **SQLite now authorizes the functions in a `CREATE TABLE`'s `DEFAULT` clauses** (SQLite 3.54.0), when the table is created and when a default is used. A default that calls `load_extension` or a function in FleetRing's reserved namespace is refused like the same call anywhere else.
- **`fleetring mcp`'s output schemas list as `required` the fields every result already carries,** `null` when empty, such as `written_by`, `primary`, `notice` and `hint` (`rmcp` 3.9). The results are unchanged; a client that validates them against the schemas sees the same objects.
- **The configuration check starts with the next upgrade.** 0.7's `upgrade` doesn't know `serve --check`, so an upgrade from 0.7 to 0.8 installs 0.8 without asking it about the node's configuration: run the 0.8 binary's `fleetring serve --config <file> --check` first if a setting changed. Upgrades and rollbacks run by 0.8 and later ask every release that has the check, and install a release before 0.8 with a note that its configuration wasn't checked.
- **Upgrade every data node before you point applications at a router.** A 0.7 primary doesn't know `Database.Session`, and the router answers its databases' requests with an error that says to upgrade it. `[sessions]` is new in 0.8.0, and a 0.7 node refuses it as an unknown field, as it does `[limits]`.
- **Nodes of 0.7 and 0.8 run side by side;** nothing is gated. A `db watch` whose primary runs 0.7 fails with `the primary runs an older release of fleetring, which can't stream changes; upgrade it`, and a watch that follows its database onto a 0.7 primary ends there with that error and the position to resume after.
- **The change table gains a floor row** (`_fleetring_cdc`, a commit record whose `table_name` is `_fleetring_floor`), written when recording first meets an empty table and by each prune. 0.7 reads it as a commit record and skips it, so rolling back to 0.7 keeps working. Tooling that reads `_fleetring_cdc` directly should skip commit records (`change_type = 2`).
- **A node serves at most 64 watchers at once;** the next is refused with `UNAVAILABLE`, exit 4.
- **Tokens carrying `fork` or `grant` work only on 0.8 nodes.** A 0.7 node refuses such a token outright with `UNAUTHENTICATED` ("the token carries an invalid scope"), as it was built to, so during a rolling upgrade a mixed fleet only ever refuses more, never allows more; `token issue` and `agent add` print "note: nodes before 0.8 refuse this token, since it carries fork or grant" when they mint one. Nothing replicated changes, so nothing is gated. A `[[principal]]` entry with `fork` fails validation on a 0.7 node, so add one only after every node runs 0.8, or `fleetring upgrade` leaves a restarted 0.7 node (on a rollback) down until it is removed.
- **`[limits] statement_timeout_ms` is new in 0.8.0.** A node without it uses 30 seconds, so no configuration needs changing. A 0.7 node refuses the `[limits]` table as an unknown field, so add it only after the node runs 0.8, or `fleetring upgrade` leaves the restarted node down until the table is removed. A statement that took longer than 30 seconds on 0.7 now fails with `DEADLINE_EXCEEDED`; a node that must allow longer sets a larger value. During a rolling upgrade a 0.7 primary ignores the deadline a call carries and runs the statement to its end, as before.
- **Existing agent identities keep their scopes,** so one minted by 0.7 has no `stream`, and the `changes` tool and `db watch --agent` are refused for it with a hint. `fleetring mcp --agent <id>` never widens an identity; it notes on stderr that the identity predates `stream`. Run `fleetring agent renew <id>` (with `--authority <dir>` outside dev mode) to reissue the credentials with today's defaults: it adds `stream:agent-<id>-*` only when the identity holds exactly 0.7's default, `admin:agent-<id>-*`, or `read:agent-<id>-*` for a read-only one, says what it added, and revokes the old token. Scopes chosen with `--scope` are never widened; to add `stream` to them, remove the agent and add it again with the scopes you want.
- **`identity.toml` gains `scopes_from = "flags"`,** written for an identity whose scopes `--scope` chose, at `version = 2`. A 0.7 `fleetring` refuses such an identity with "this identity was written by a newer fleetring; upgrade"; an identity with the default scopes stays at `version = 1`, which 0.7 still reads.
