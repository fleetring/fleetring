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
cargo +1.88 check --workspace --exclude fleetringd --exclude fleetring-client --all-features --all-targets
cargo +1.89 check -p fleetringd -p fleetring-client --all-features --all-targets
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

The daemon is released separately from the crate (FR-034). It isn't published to crates.io (`publish = false`); a release is a set of prebuilt binaries and a container image, made by CI from a `fleetringd-vX.Y.Z` tag. Two workflows run on the tag:

- `.github/workflows/release.yml` builds `fleetringd` for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin`, `aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`. It packages each with the licenses and `crates/fleetringd/README.md`, writes `SHA256SUMS` and a `.sha256` per file, signs `SHA256SUMS` keylessly with cosign (one bundle, `SHA256SUMS.sigstore.json`, holding the signature, the certificate, and the transparency-log proof), attests the archives and `scripts/install.sh`, and attaches everything to the tag's GitHub release, creating it if needed. It runs `cargo audit` first and fails on an unresolved advisory.
- `.github/workflows/container.yml` builds the `Dockerfile` natively for `linux/amd64` and `linux/arm64` and pushes one manifest to `ghcr.io/fleetring/fleetringd`, tagged `X.Y.Z`. A final release also moves `X.Y` and `latest`; a prerelease (`X.Y.Z-rc.1`) gets its own tag only.

## 1. Pre-flight

- [ ] CI is green on `main`, including `test (ubuntu-24.04-arm)` and `simulation (fixed seeds)`.
- [ ] **The rolling upgrade passes from the previous release** (FR-054, ADR-0038): `.github/workflows/rolling-upgrade.yml` starts three nodes on the previous release's binaries, downloaded and verified with cosign, and moves them one at a time to `main` under writes. It runs nightly; if it hasn't run since the last merge, run it by hand (Actions → Rolling upgrade → Run workflow, optionally naming the release to upgrade from). Locally, with cosign installed:

  ```sh
  FLEETRINGD_BASELINE_VERSION=<previous> cargo test -p fleetringd --test upgrade -- --ignored --nocapture rolling
  ```

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

- [ ] Set `version` in `crates/fleetringd/Cargo.toml` to exactly the version the tag will name; `fleetringd --version` prints it. Both workflows fail unless the tag is `fleetringd-v` followed by that version, character for character.
  - **A release candidate** sets it to `X.Y.Z-rc.N` first, and is tagged `fleetringd-vX.Y.Z-rc.N`. The release is marked a prerelease, and the image gets only its own tag.
  - Tagging `-rc.N` while `Cargo.toml` still says `X.Y.Z` fails the `version` job. That is why 0.2.0's `rc.1` tag failed.
  - The final release sets `X.Y.Z` in a commit of its own.
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
gh attestation verify oci://ghcr.io/fleetring/fleetringd:$V --repo fleetring/fleetring

# 5. The install script installs this version.
curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh \
  | sh -s -- --version $V --prefix "$PWD/prefix"
"$PWD/prefix/bin/fleetringd" --version

# 6. The image runs on both architectures.
docker run --rm --platform linux/amd64 ghcr.io/fleetring/fleetringd:$V --version
docker run --rm --platform linux/arm64 ghcr.io/fleetring/fleetringd:$V --version
```

- [ ] The release lists five archives, `SHA256SUMS` with its `.sigstore.json` bundle, a `.sha256` for each file, and `install.sh`.
- [ ] Every checksum passes.
- [ ] `cosign verify-blob` prints `Verified OK`.
- [ ] `gh attestation verify` succeeds for every archive, `install.sh`, and the image.
- [ ] The installed binary prints `fleetringd X.Y.Z`.
- [ ] Both `docker run` commands print `fleetringd X.Y.Z`. On a machine of one architecture, the other runs under emulation (Docker Desktop has it; on Linux, `docker run --privileged --rm tonistiigi/binfmt --install all` adds it).
- [ ] The first time only: make the `fleetringd` package public in the organization's GitHub package settings, and link it to this repository.
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
gh attestation verify oci://ghcr.io/fleetring/fleetringd:X.Y.Z --repo fleetring/fleetring
```
````

### 0.7.0: upgrading across the rename

A draft for 0.7.0's notes, under "Breaking changes" (ADR-0043, points 1 and 3). The binary is `fleetring`; `fleetringd` stays a name for it until 1.0, and `fleetringd --config <file>`, without `serve`, still runs a node, with a warning.

- **Upgrade from 0.6 as usual,** with `fleetringd upgrade` or `install.sh`. The archives keep their names and hold the binary under both names, so 0.6's `upgrade` installs 0.7 over `fleetringd` and restarts the node in place on its 0.6 arguments.
- **Add the new name.** A host upgraded by 0.6's `upgrade` has no `fleetring`. Add it with `ln -s fleetringd "$(dirname "$(command -v fleetringd)")/fleetring"`, or rerun `install.sh`. A 0.7 `upgrade` or `upgrade --rollback` adds the link itself, and says so. On such a host the real file stays `fleetringd`, and the previous binary is kept as `fleetringd.previous`.
- **After `install.sh` over a 0.6 host, roll back by hand.** `install.sh` makes `fleetring` the real file and keeps no previous binary, so a `fleetringd.previous` that 0.6's `upgrade` kept belongs to nothing: `upgrade --rollback` looks for `fleetring.previous` and says there is none. That file holds the release before the one `install.sh` replaced. To go back to it, move it into place with `mv fleetringd.previous fleetring` in that directory, and restart the node; `fleetringd` follows, since it is a link. To go back to 0.6 itself, run `install.sh --version 0.6.<n>`.
- **Keep service units on `fleetringd --config <file>`** until no node needs to roll back to 0.6. 0.6 doesn't know `serve`. A 0.7 `upgrade --rollback`, or `upgrade --version` with a release before 0.7, refuses to install it over a node started with `serve`, and without a running node to check, it asks, or needs `--yes`. But the 0.6 binary kept as `fleetringd.previous` can't check: `fleetringd.previous upgrade --rollback` installs 0.6 anyway, and a unit that runs `fleetring serve` then fails to start, and its supervisor restarts it in a loop. After switching a unit to `fleetring serve`, rolling back to 0.6 requires reverting the unit to `fleetringd --config` first.
