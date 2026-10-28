# Releasing `fleetring`

This is the checklist for publishing the `fleetring` crate (FR-019). A release freezes placement behavior as well as the API, so work through it in order.

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

- [ ] Set `version` in `[workspace.package]` (root `Cargo.toml`). `0.0.1` is a placeholder already on crates.io, so the first real release is `0.1.0`.
- [ ] In `CHANGELOG.md`, rename `[Unreleased]` to `[0.1.0] - YYYY-MM-DD`, start a new empty `[Unreleased]`, and update the compare links.
- [ ] Note any MSRV change. Policy: raise it only in a minor release, never to a toolchain younger than six months (ADR-0009).

## 4. Verify the package

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test -p fleetring --all-features --release -- --ignored
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +1.88 check --workspace --all-features --all-targets
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

## 6. After the first release

Turn on semver checking against the published baseline by adding this job to `.github/workflows/ci.yml`:

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
