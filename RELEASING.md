# Releasing `fleetring`

This is the checklist for publishing the `fleetring` crate (FR-019). A release freezes placement behavior as well as the API, so work through it in order. The `fleetringd` daemon has its own process, in [Releasing `fleetringd`](#releasing-fleetringd).

## 1. Pre-flight

- [ ] Every P0 and P1 ticket in `docs/tickets/README.md` is `done`.
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
cargo +1.88 check --workspace --exclude fleetringd --all-features --all-targets
cargo +1.89 check -p fleetringd --all-features --all-targets
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

- `.github/workflows/release.yml` builds `fleetringd` for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin`, `aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`. It packages each with the licenses and `crates/fleetringd/README.md`, writes `SHA256SUMS` and a `.sha256` per file, and attaches the archives, the checksums, and `scripts/install.sh` to the tag's GitHub release, creating it if needed.
- `.github/workflows/container.yml` builds the `Dockerfile` natively for `linux/amd64` and `linux/arm64` and pushes one manifest to `ghcr.io/fleetring/fleetringd`, tagged `X.Y.Z`. A final release also moves `X.Y` and `latest`; a prerelease (`X.Y.Z-rc.1`) gets its own tag only.

## 1. Pre-flight

- [ ] CI is green on `main`, including `test (ubuntu-24.04-arm)` and `simulation (fixed seeds)`.
- [ ] A manual run of both workflows (Actions → Release, and Actions → Container → Run workflow) passes. It builds and smoke-tests every archive and both images, and publishes nothing. Download the `fleetringd-<version>-dist` artifact and try an archive.
- [ ] `RUST_VERSION` agrees in `.github/workflows/release.yml` and the `Dockerfile`.

## 2. Version

- [ ] Set `version` in `crates/fleetringd/Cargo.toml`; `fleetringd --version` prints it. Both workflows fail if the tag doesn't match it.
- [ ] Commit: `chore(release): fleetringd X.Y.Z`.

`CHANGELOG.md` documents the `fleetring` crate only; the daemon's notes go in its GitHub release.

## 3. Tag

```sh
git tag -a fleetringd-vX.Y.Z -m "fleetringd X.Y.Z"
git push origin main fleetringd-vX.Y.Z
```

## 4. After the workflows finish

- [ ] The release lists five archives, `SHA256SUMS`, a `.sha256` for each file, and `install.sh`. Edit its notes to summarize the changes.
- [ ] `curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh | sh -s -- --version X.Y.Z` installs a binary that prints `fleetringd X.Y.Z`.
- [ ] `docker run --rm ghcr.io/fleetring/fleetringd:X.Y.Z --version` prints the same, on both architectures.
- [ ] The first time only: make the `fleetringd` package public in the organization's GitHub package settings, and link it to this repository.

A failed workflow can be rerun on the same tag: the release workflow replaces the release's files, and the container workflow pushes the manifest again. Never move a published tag; release a new patch version instead.
