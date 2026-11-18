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

### 0.8.0: online delegation, forks, and the hand-over

The final body of the `fleetringd-v0.8.0` release (ADR-0031's amendment 2; ADR-0037; ADR-0045; ADR-0046 and its amendment; ADR-0047; FR-066, #69; FR-075, #103; FR-078, #109; FR-081, #126; FR-085 to FR-088, #135 to #138; FR-090 to FR-093, #140 to #143; FR-102, #157; FR-106; #95; #152; #153; #154). Paste it over the workflow's notes. Known limits FR-110 (#177), FR-111 (#178) and FR-112 (#180) come out as each is fixed.

````markdown
## fleetringd 0.8.0

The agents and front-door release. Agents can fork databases and give the agents they spawn narrower, shorter-lived credentials of their own; a libSQL/Hrana router lets unchanged libSQL and Turso clients reach the fleet; `db watch` follows a database's row changes; and a membership change now hands replica sets over on joint quorums, which closes a way to lose an acknowledged write that the simulation found (#157). Upgrade soon, for the security fixes below, but in order: every data node first, then the control node, with no membership change until every node runs 0.8. Three new fleet features, `fork`, `delegation` and `handover`, each close rollback to 0.7 the first time they are used; read **Breaking changes** for which commands use them. The daemon is still not safe to depend on (`docs/roadmap.md`).

### Breaking changes

**Upgrading from 0.7:**

- **Upgrade every data node first, then the control node** (ADR-0038, point 9). A 0.8 primary under a 0.7 control node never counts placement by a newer ring than the control node last answered for, so the control node's upgrade itself opens no window.
- **Change no membership while the fleet is mixed:** no joins, leaves, drains that leave, `node remove`, or weight changes until every node, live or down, runs 0.8. Until then a membership change moves replica sets at once, as 0.7 does, and a write acknowledged on the old set can still be lost if its primary fails before it learns of the change (#157).
- **Avoid dropping a database and creating its name again while any node runs 0.7.** A 0.7 primary neither names the database's incarnation in its streams nor checks a follower's first chain hash, so while it leads the new database a slow follower can still count its copy of the dropped one, as in 0.7.
- **Three fleet features close rollback the first time they are used.** Each is refused, naming the member that lacks it, until every member, live or down, runs 0.8. After first use a 0.7 node can't join, and `fleetring upgrade --rollback` below 0.8 is refused, naming the feature:
  - **`fork`:** the first `db fork` or MCP `db_fork`.
  - **`handover`:** the control node enables it on its own, with a logged command and an audit line, just before the first join or leave that would move a leased database's replica set, once every member runs 0.8. So the first membership change after the upgrade closes rollback.
  - **`delegation`:** the first `agent delegate`, the first `token revoke --principal`, or the first revocation under the ancestor rule. **So does the first `agent remove <root-id>`** on a fleet whose every node runs 0.8: the agent revokes itself, as a principal and by its token ID, under the ancestor rule. To remove a root agent and keep rollback open, read its token ID (the `token id` line of `fleetring agent show <id>`), then run `fleetring agent remove <id> --no-revoke` and `fleetring token revoke --token-id <id>` with `fleet`. `agent renew <root-id>` revokes the token it replaces with `fleet` (dev mode's admin, or the context's identity), which keeps rollback open; only when no identity with `fleet` is at hand does it revoke under the ancestor rule, and it warns on stderr first.
- **Give each control node a delegation issuer once every node runs 0.8:** `fleetring cert issuer --name <node-id> --dir <authority dir>`, then `[control] issuer_cert` and `issuer_key` in its configuration; the node reloads them without a restart. A 0.7 binary refuses both keys, so **remove those two lines before you roll a control node back to 0.7**, or `fleetring upgrade --rollback` leaves it down until they're gone. Dev mode makes its own pair.
- **Root agents delegate after `agent renew`.** While any node runs 0.7, `agent add` and `agent renew` mint tokens that can't delegate, and say so. Once every node runs 0.8, `fleetring agent renew <id>` (with `--authority <dir>` outside dev mode) reissues an identity that holds exactly an older release's default scopes with the defaults that delegate; scopes chosen with `--scope` are never widened.
- **Existing agent identities keep their scopes,** so one minted by 0.7 has no `stream`, and `db watch --agent` and the MCP `changes` tool are refused for it with a hint. `fleetring agent renew <id>` adds `stream:agent-<id>-*` to an identity that holds exactly 0.7's default, says what it added, and revokes the old token.
- **`identity.toml` moves to version 2** for a delegated agent and for an identity whose scopes `--scope` chose (`scopes_from = "flags"`). A 0.7 `fleetring` refuses it with "this identity was written by a newer fleetring; upgrade"; an identity with the default scopes stays at version 1.
- **New configuration tables are refused by 0.7:** `[fork]`, `[sessions]` and `[limits]`, and a `[[principal]]` entry with the `fork` action. Add them only once the node runs 0.8, or a rollback leaves it down until they're removed. A node without them uses the defaults, so none needs adding.
- **The configuration check starts with the next upgrade.** 0.7's `upgrade` doesn't know `serve --check`, so run the 0.8 binary's `fleetring serve --config <file> --check` before upgrading if a setting changed.
- **During a rolling upgrade, a 0.7 follower counts only once it has acknowledged a write.** A 0.8 primary counts a follower's first acknowledgement on a stream only once its chain hash matches the primary's log, and a 0.7 follower sends none. Writes are never refused or delayed for it; see **Known limits** for what waits on an idle database.
- **Upgrade every data node before you point applications at a router.** A 0.7 primary doesn't know `Database.Session`, and the router answers its databases' requests with an error that says to upgrade it.

**Changes scripts and clients will see:**

- **Statements stop at a deadline:** 30 seconds on a primary unless `[limits] statement_timeout_ms` says otherwise, and 10 seconds per statement in `fleetring mcp`. A statement that took longer on 0.7 now fails with `DEADLINE_EXCEEDED`, exit 4. `db exec` and `db query` take `--timeout <n>s`, 30 seconds by default; before, a call over 10 seconds failed with `CANCELLED`, exit 1.
- **A token on a connection with no client certificate is refused** with `UNAUTHENTICATED`, whatever signed it. The gRPC listener always required a certificate, so only a listener that skipped one is affected.
- **Nodes refuse a token whose `iat` is more than 30 seconds ahead of their clock.** 0.7's tokens, whose `iat` equals their `nbf`, keep working; a token another JWT library minted with a future `iat` doesn't.
- **A data node refuses every token while its denylist holds an entry it can't read,** with `UNAVAILABLE`, where 0.7 skipped the entry. `/readyz` and `fleetring_denylist_unusable` say so; certificates keep working.
- **Tokens carrying `fork` or `grant` work only on 0.8 nodes.** A 0.7 node refuses them with `UNAUTHENTICATED` ("the token carries an invalid scope"), so a mixed fleet only ever refuses more. `token issue` and `agent add` print a note when they mint one.
- **Text that SQL makes from a REAL value keeps 17 significant digits,** where 0.7's SQLite kept 15 (SQLite 3.53): `CAST(1.0/3 AS TEXT)` is `0.33333333333333332`. The same goes for `||`, `printf('%s', …)`, `json_array()`, and a REAL stored in a TEXT column, and for the row changes `db watch` and `changes` report. REAL values themselves are unchanged, and replicas copy the primary's pages, so copies never disagree.
- **SQLite now authorizes the functions in a `CREATE TABLE`'s `DEFAULT` clauses** (SQLite 3.54), so a default that calls `load_extension` or a reserved function is refused like the same call anywhere else.
- **`fleetring mcp`'s output schemas list as `required` the fields every result already carries,** `null` when empty (`rmcp` 3.9). The results are unchanged.
- **The change table gains a floor row** (a commit record whose `table_name` is `_fleetring_floor` in `_fleetring_cdc`). 0.7 skips it, so rolling back works; tooling that reads `_fleetring_cdc` directly should skip commit records (`change_type = 2`).

**The `fleetringd` library:**

- **`auth::Grant` is `TokenSpec`,** and `Verified::grant` is `Verified::spec`. `TokenSpec` is `#[non_exhaustive]`: build it with `TokenSpec::new(principal, scopes, id, not_before, expires_at)`, then `.signed_at(at)` and `.agent(owner)`. It gains `issued_at`, `kind`, `owner`, and `lineage`.
- **`auth::Denylist::load` returns `Result<Arc<Revocations>, Unusable>`,** and `receive` replaces `replace`. The scope grammar and lattice moved into `fleetring_control::scope`, re-exported from `fleetringd::auth` unchanged; `Scopes::intersect`, `grant_step` and `grant_step_strict` narrow exactly, and refuse a side of more than 256 scopes.
- **New public types:** `DelegationSigner`, `Lineage`, `IssuerCertificate`, `IssuerPair`, `ControlFreshness`, `UnreadableEntry` and `Unusable` in `fleetringd`; `ControlClient::delegate_token` and `list_delegations`; `AgentName`, `TokenClaims`, `DelegationId`, `ChainLink`, `Revoked::Principal` and `Added` in `fleetring-control`; and `DelegationReport` in `fleetring_client::output`. The protocol moves to 1.3.

### Security

- **The bundled SQLite moves from 3.50.2 to 3.54.1, for CVE-2026-119375** (RUSTSEC-2026-0459, medium; #152). SQLite before 3.54.0 that opens a database beside a crafted `-wal` file whose page size is smaller than the database's can return uninitialized process memory in ordinary query results. SQL can't reach it: a node opens such a pair only when it restores from an archive (`archive restore`, `db restore`) or installs a snapshot or WAL from a peer. So an attacker who can write your archive's object store could read node memory back through a restored database. Every release before 0.8.0 is affected. Upgrade, and until every node runs 0.8.0, restore only from archive stores that only your nodes and operators can write. 3.54 also fixes SQLite's WAL-reset corruption bug, which 3.50.2 carried.
- **Restores and peers' copies are checked before they are opened** (#154). Whatever SQLite is bundled, a node refuses an archive's or a peer's snapshot whose WAL's page size differs from its database file's, and a WAL batch whose page size differs from the follower's; the restore or the seed fails naming the archive object or the peer, and leaves nothing behind.
- **User statements can no longer run the daemon's own cached statements.** Before 0.8.0, a statement sent through `db exec` or `db query` whose text matched, word for word, one the daemon runs on its reserved tables ran without the confinement's checks: a principal allowed to write a database could rewrite its replicated sink positions, so that a change stream skipped or repeated batches after a failover, and one allowed to read it could read its recorded changes. Statements that mention a reserved name are now always checked. Releases from 0.2.0 on are affected, and from 0.3.0 on for sink positions.
- **An open stream ends when the authority its token rests on leaves `[tls] ca`,** at the next recheck after the node reloads the bundle, as a new request with that token already was refused. Before, it ran until the token expired.
- **The MCP server's `rmcp` moves from 3.8.0 to 3.9.0** (RUSTSEC-2026-0462; #153). The advisory is in `rmcp`'s HTTP clients, which `fleetring mcp` doesn't build: it serves over stdio, so no release was affected.

### Features

- **`fleetring router --config <file>`** (FR-066, #69): a front door that speaks libSQL's Hrana protocol over HTTP, so unchanged libSQL and Turso clients (`@libsql/client`, the `libsql` crate's remote client, Turso's serverless drivers) reach each database's primary. Point a client at `https://<router>/db/<database>/`, with the trailing slash, and give it a FleetRing token as its auth token. It serves Hrana 2 and 3 over HTTP/1.1 and HTTP/2 with JSON: pipelines, cursors, batches with conditions, sequences, `describe`, stored SQL, and transactions across requests, with SQLite's own semantics. It checks every request's token before it calls any node, then forwards it, and keeps a new stream only once a node has opened a session for its token. A drain or failover moves a stream outside a transaction to the new primary with no error; a request whose reply was lost fails with `FLEETRING_OUTCOME_UNKNOWN` and is never sent again. `deploy/router.toml` documents its settings; it needs a client certificate for a principal every node lists with `forward_tokens = true` and no scopes. Not yet: Hrana over WebSocket, the protobuf encoding, follower reads, and router metrics.
- **`fleetring db watch <db>`** (FR-075, #103) follows a database's row changes from its primary, one committed transaction per line, as the write quorum acknowledges them: from now, or after a position with `--from <position>`. `-o jsonl` prints `database`, `start`, `position`, `lease_epoch`, and `changes`. It resumes on the new primary after a failover or a drain, needs the `stream` scope, which no other scope grants, and a database a logical change sink names; dev mode names every one. A node serves at most 64 watchers at once. `Database.WatchChanges` is the protocol call behind it.
- **Database forks** (ADR-0046; FR-085 to FR-087, #135 to #137). `fleetring db fork <source> <target>` makes `<target>`, a new database, from the source's contents at a position its write quorum acknowledged, and waits until the fork is **ready**; it is independent from then on, with settings and metadata of its own. The command prints an operation ID before it asks, and resends under it when a reply is lost, so a retry never makes a second fork. `--operation-id` waits for the same fork again, from the same principal, for 7 days after it ends; `--detach` returns once it is accepted; Ctrl-C stops the wait and the fork carries on; `db drop <target>` cancels a fork in progress. `db show`, `db locate` and `db list` show a fork's state (`seeding`, `opening`, `ready`, or `failed`), where it came from, and the database's incarnation. The MCP server gains `db_fork`, offered with read-write access. Forking needs `fork` on the source, which `admin` implies, and `admin` on the target. A fork holds a full copy of its source on each of its replicas, staged until it is ready, which `orphan list` shows and `orphan remove` deletes if a node keeps one. `[fork]` bounds what seeding costs a source's primary and a node, with defaults from FR-087's measurements (`crates/fleetringd/README.md`). New metrics: `fleetring_forks_total`, `fleetring_fork_ready_seconds`, `fleetring_fork_attempts_total`, `fleetring_fork_seed_bytes`, `fleetring_fork_pin_seconds`, `fleetring_fork_pinned_wal_bytes` and `fleetring_fork_reserved_disk_bytes`.
- **Delegated agents** (ADR-0047; FR-090 to FR-093, #140 to #143). An agent's harness runs `fleetring agent delegate 42.c7` to give a child, `agent-42.c7`, a narrower, shorter-lived token without the authority's key. The control plane checks the parent's live token, narrows exactly, records the delegation and its lineage, and signs the child's token with the control node's own delegation issuer key, which the authority certifies, so every node verifies it through the authority it already trusts. A child holds only scopes inside its root agent's databases, never `fleet`, for 2 hours by default and 24 at most, never past its parent's token, and is accepted only on a connection whose certificate names it or an ancestor, so it runs where its root's certificate is. A root lineage holds at most 256 live delegations, and a caller makes at most 30 a minute.
  - `--agent 42.c7` and `mcp --agent 42.c7` act as the child; `agent list` and `agent show` show lineage; `agent renew` and `agent remove` take a child's ID; `agent list --fleet` lists the fleet's live delegation records, never tokens, with `fleet`. `agent delegate` and `agent renew <child>` are safe to run again after a crash or an exit 4: they resend under the same operation ID and never record a second delegation.
  - Revoking an ancestor's token ID, its signing key, or its principal ends the whole subtree below it, on requests and open streams. `token revoke --principal <name>` revokes a principal and every name below it; without `fleet`, an agent may revoke under the *ancestor rule*: itself, the token it presented, or what was delegated below it.
  - `fleetring cert issuer --name <control node> --dir <authority dir>` makes a control node's issuer pair, which `[control] issuer_cert` and `issuer_key` name and the node reloads on change, refusing a key its group or other users can read. Dev mode makes and renews its own.
  - A root agent minted once every node reports `delegation` holds `admin` (or `read`), `stream`, and `grant` on `agent-<id>-*` and `agent-<id>.*`, so it can delegate. No MCP tool delegates, renews, revokes, or returns a token, and a child's streams and sessions count toward its root's per-principal quotas. Agents on one machine can still read each other's credentials, so this remains **trusted local development**. `SECURITY.md` describes what each compromise reaches and what each revocation ends.
- **Two new scope actions, `fork` and `grant`** (ADR-0044 point 2; FR-088, #138), which 0.7 reserved. `fork:<pattern>` reads the databases it names and is what forking *from* them needs; `read` and `write` don't imply it, and `admin` does. `grant:<pattern>` authorizes no request: it bounds what an agent's token may hand a child, and nothing implies it. A `[[principal]]` entry may carry `fork`; one with `grant` fails validation.
- **Agents follow their own databases' changes** (FR-081, #126). A newly minted agent identity gets `stream:agent-<id>-*`, and `fleetring mcp` gains a `changes` tool, offered with and without `--read-only`, which returns at most `limit` transactions (20 by default, at most 100) recorded after a position, and `next_after`, waiting at most 2 seconds. The agent skill gains patterns for following changes, delegated agents, and forks.
- **Statements stop at a deadline** (FR-078, #109). A write, query, or `db show` schema read that runs past `[limits] statement_timeout_ms` (30 seconds), or past its call's own deadline, is interrupted on the node: a query fails with `DEADLINE_EXCEEDED`, exit 4, and a write rolls back, leaving nothing for its followers. A cancelled call stops its statement too, so the database serves its next request at once; before, a statement ran to its end whatever its client did.
- **Membership changes hand replica sets over on joint quorums** (ADR-0031, amendment 2; FR-102, #157). A join or a leave that moves a leased database keeps the old replica set as the one promotions fence until the primary reports that the new placement's write quorum holds its log, a few heartbeats later, and the primary counts both write quorums meanwhile. `db locate` prints `placed on:` (`placed_replicas` in JSON) beside the kept replicas, and `fleetring_control_kept_replica_sets` counts them.
- **`Database.Session`** is the data-plane stream the router runs each Hrana stream on, with `[sessions] transaction_timeout_ms` (30 seconds) and `max_waiting_writes` (64) bounding a transaction's hold on a database's writer. It needs `read`; without `write`, the session is read-only.
- **`fleetring serve --config <file> --check`** (#95) says whether a node would start with a configuration and `serve`'s flags, and starts nothing: it exits 0, or prints the first problem with the file, the dotted field, the line and the column, and exits 1. `--json` prints `config`, `valid`, `node`, `role` and `problems`.
- **`fleetring upgrade` asks the new release whether the node would start before it swaps it in** (#95), by running the release's own `serve --check` on the node's command line; a configuration it refuses stops the upgrade with the release's message and the fix. `--rollback` asks the previous binary the same way, and `upgrade --software` gains `config_check: true`.
- **`fleet`** is a permanent short name for `fleetring` (#103): `fleet db list`. `install.sh` and the container image link it; `install.sh` skips it, and says why, when another `fleet` is on your `PATH`, and `--no-alias` skips it always.

### Fixes

- **A write acknowledged just before a membership change is no longer lost by the next promotion** (#157). A join or a leave moved a database's replica set in control state at once, while its primary went on counting the old set's write quorum: with R = 3, {P, A, B} became {P, A, X}, P acknowledged a write that P and B held, P failed, and the promotion fenced A and X, neither of which held it. The hand-over above closes it once `handover` is enabled.
- **A follower's copy of a dropped database no longer counts toward a database created again under its name** (FR-106). A slow follower could keep the old copy, acknowledge its position on the new database's stream, and be counted toward writes it didn't hold, so a promotion could serve the dropped rows as the new database's. A primary now counts a follower's first acknowledgement on a stream only once the follower's chain hash there matches its own log's, and re-seeds a copy whose hash differs or whose position is past its log's end. In a fleet that has forked, a stream's open and a promotion's fence also name the database's incarnation, and a copy of another incarnation refuses both and is kept as an orphan; a drop notice deletes such a copy at any epoch. **Known limits** says what remains.
- **A database recording row changes no longer stops taking writes after its first recorded transaction fails.** The rollback took the change table and its triggers with it, and every later write failed with `no such table: _fleetring_cdc` until the node reopened the database.
- **A follower whose truncated tail held its chain record no longer reports a divergence the data didn't have.** A stream's open now carries the primary's hash at the epoch's start.
- **A node stopped and started again in one process frees and rebinds its port,** rather than finding it taken for up to a minute while the previous run's connections sit in `TIME_WAIT`.
- **A freshly issued certificate works at once on a node whose clock is a little behind.** Certificates from `agent add`, `agent renew`, `cert init`, `cert node`, `cert client` and dev mode now start 30 seconds before they are issued, the skew a token's `nbf` already allows; before, a node even a second behind the issuing host refused the handshake until its clock caught up.

### Known limits

- **In a fleet that has never forked, don't drop a database and create the same name again in quick succession** (FR-110, #177). Until `fork` is enabled, every database has incarnation 0, so the new database shares it with the dropped one. A follower whose drop notice arrives after the new database is leased can keep the dropped copy, and if the new primary fails before it has streamed to that follower, a promotion can serve the dropped rows under the name. 0.7 has the same limit. Once a fleet has forked, every new database gets a random incarnation, which its streams and its promotions' fences name.
- **An idle database's followers can wait for a write to count** (FR-111, #178). When a follower's first acknowledgement on a stream proves nothing, as from a 0.7 follower or where its hash chain or the primary's is derived or lost, its position counts only after it acknowledges a write. Until then, on a database that takes none, `node drain` hands its primary over with the control plane choosing the new one, rather than naming the most advanced follower and shipping it the tail first; an on-demand checkpoint (`Replication::checkpoint` in the library) refuses as if a follower were behind; the hand-over waits; and the database reads as under-replicated. Writes are never refused or delayed for it, and the first write counts the followers again.
- **A stale copy can stall a promotion** (FR-112, #180). Control state ranks promotion candidates by their reports' log epoch and position, without the incarnation, so a data node's stale copy of a dropped database can rank first. Its own fence refuses it, so no data is lost, but the promotion can stall, round after round, until that data node checks the copy and keeps it as an orphan, or longer for a copy those checks miss.
- **A kept replica set that lost its quorum waits for a member to come back.** With {P, A, B}, if B fails and is removed and P fails before its database's hand-over finishes, the promotion needs two of {P, A, B}. Restart P; else restart B on its old disk; and only if neither disk survives, start a node under B's ID on an empty disk, which loses the writes only P and B held. No command yet releases a kept set without its primary's report; the daemon README's runbook has the steps.
- **A fork takes a few seconds however small** (about 4 s in FR-094's trial), and no fork advances until `lease_duration_ms + failover_margin_ms` (12 seconds by default) after a control node starts.
- **Agents are trusted local development.** 0.8 ships no agent launcher, so a delegated agent runs as the same OS user as its root, with its root's certificate key on the same disk; delegation narrows what a node accepts, not what a process can read.

### Verify

```sh
cosign verify-blob SHA256SUMS --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/fleetring/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify fleetringd-0.8.0-<target>.tar.gz --repo fleetring/fleetring
gh attestation verify oci://ghcr.io/fleetring/fleetring:0.8.0 --repo fleetring/fleetring
```
````

### 0.8.1: a random incarnation for every database

A draft for 0.8.1's notes (ADR-0046's FR-110 amendment; FR-110, #177). The 0.8.0 body above is what was published, its known limits included; this release's notes say which of them it removes. Add each later fix of the line here as it lands, and paste the body over the workflow's notes when 0.8.1 is tagged.

````markdown
## fleetringd 0.8.1

A fix release for 0.8. Once every node runs 0.8.1, every new database gets a random incarnation, whether or not the fleet has forked, which removes 0.8.0's first known limit: a name dropped and created again in quick succession could have a promotion serve the dropped rows under it. Upgrade every data node first, then the control node. **The first `db create` after every node runs 0.8.1 enables a new fleet feature, `incarnations`, and from then on rolling back below 0.8.1 is refused**; read **Breaking changes** before you upgrade the last node.

### Breaking changes

**Upgrading from 0.8.0 or 0.7:**

- **A new fleet feature, `incarnations`, closes rollback below 0.8.1 the first time it is used.** It is used by the first database created once every member, live or down, runs 0.8.1, and creating databases is so common that, in practice, upgrading the last node is the last point to roll back from. After that, a 0.8.0 or 0.7 node can't join, and `fleetring upgrade --rollback` below 0.8.1 is refused, naming the feature. The control node logs the feature's enabling on its audit target.
- **The fix starts once every node runs 0.8.1.** Until then creations store incarnation 0, as on 0.8.0, and 0.8.1 stays rollback-safe. **In a fleet that has never forked, don't drop a database and create its name again until every node runs 0.8.1 and a database has been created since**, and, as for 0.8.0, while any node runs 0.7.
- **Existing databases keep incarnation 0.** Only databases created once the feature is enabled get a random one, which the `incarnation` field of `db show --json` and `db list --json` shows.
- **A copy of a dropped database can now be kept as an orphan in a fleet that has never forked.** A data node that missed the drop of a database created before the feature was enabled keeps that copy as an orphan if the name was created again: the new database's streams name its random incarnation, which the copy doesn't record. A forked fleet already worked this way. `fleetring orphan list` shows the copy, and `fleetring orphan remove` deletes it, after which the node follows the new database. Nothing is lost.

### Fixes

- **A database dropped and created again under the same name no longer risks serving the dropped rows** (FR-110, #177). In a fleet that had never forked, both lives of the name had incarnation 0. A follower whose drop notice arrived after the new database was leased could keep the dropped copy, and if the new primary failed before streaming to it, a promotion could serve the dropped rows as the new database's and lose writes it had acknowledged. Every database created now has a random incarnation, which its streams and its promotions' fences name, and a copy of another incarnation refuses both.
- **A `db drop` right after the control node restarts no longer leaves orphans** (FR-121, #191). A data node that couldn't yet check the drop notice against the restarted control node refused it, and kept its copy as an orphan, so the drop needed `fleetring orphan remove` and `db create` of the name was refused until then. The node now answers that it can't check the notice yet, and the control node tells it again a few times, a second apart. A notice that control state contradicts still deletes nothing.

### Known limits

- **An idle database's followers can wait for a write to count** (FR-111, #178), as in 0.8.0.
- **A stale copy can stall a promotion** (FR-112, #180), as in 0.8.0.
- **A database created before `incarnations` was enabled, dropped, and created again before then** has the limit 0.8.0 published until it is dropped again.

### Verify

```sh
cosign verify-blob SHA256SUMS --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/fleetring/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify fleetringd-0.8.1-<target>.tar.gz --repo fleetring/fleetring
gh attestation verify oci://ghcr.io/fleetring/fleetring:0.8.1 --repo fleetring/fleetring
```
````
