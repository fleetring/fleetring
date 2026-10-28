---
id: FR-034
title: Release builds, ARM CI, and container images for fleetringd
status: in-progress
priority: P1
size: M
milestone: M6 Replication and failover
depends_on: [FR-022]
adrs: [ADR-0009, ADR-0015]
---

# FR-034: Release builds, ARM CI, and container images for `fleetringd`

## Goal

Make `fleetringd` installable without a Rust toolchain: prebuilt binaries for the common platforms, a checksum-verifying install script, and a multi-arch container image, all produced from a tag. Test the workspace natively on Linux ARM64, where most small self-hosted machines run.

## Scope

- **Linux ARM64 CI.** A `test (ubuntu-24.04-arm)` job in `ci.yml` runs the workspace tests with all features, then the simulation's fixed seeds under `--cfg tokio_unstable`, natively on GitHub's free ARM64 Linux runner. macOS ARM64 is already covered: `macos-latest` is an Apple Silicon runner.
- **Release workflow** (`.github/workflows/release.yml`). On a `fleetringd-v*` tag, or by hand as a dry run:
  - builds `fleetringd` in release for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin`, `aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`;
  - packages each as `fleetringd-<version>-<target>.tar.gz` (`.zip` on Windows), holding the binary, both license files, and a README;
  - writes a `.sha256` file per archive and one `SHA256SUMS` for all of them;
  - attaches the archives, the checksums, and `install.sh` to the GitHub release for the tag.
- **Install script** (`scripts/install.sh`). POSIX `sh`: detects the OS and architecture, downloads the archive for a version (the newest `fleetringd` release by default), verifies it against `SHA256SUMS`, and installs into a prefix.
- **Container image.** A multi-stage `Dockerfile`, a `.dockerignore`, an example `deploy/fleetringd.toml`, and `.github/workflows/container.yml`, which pushes `linux/amd64` and `linux/arm64` to `ghcr.io/fleetring/fleetringd` under one manifest.
- **Docs.** An Install section in the root README, a "Releasing fleetringd" section in `RELEASING.md`, and a README for the archives (`crates/fleetringd/README.md`).

## Design

- **A plain matrix, not `cargo-dist`.** `cargo-dist` generates its workflow and then owns it: the generated file must be regenerated rather than edited, and it brings its own installer and conventions for tags and archive names. This repository releases two things from one workspace under two tag families (`fleetring-v*` for the crate, `fleetringd-v*` for the daemon), and one binary is all the daemon ships (ADR-0015). A hand-written matrix of five jobs is short, uses only the actions CI already uses, and reads top to bottom.
- **Native builds.** Each target builds on a runner of its own architecture: `ubuntu-24.04` and `ubuntu-24.04-arm` for Linux, `macos-latest` (Apple Silicon) for `aarch64-apple-darwin`, and `windows-latest` for Windows. The one exception is `x86_64-apple-darwin`: GitHub is retiring its Intel macOS runners, and Apple's toolchain cross-compiles between its two architectures without extra tools, so it builds on `macos-latest` too. No target needs QEMU or `cross`.
- **Linux binaries build in `rust:<version>-slim-bookworm`**, run with `docker run` on the native runner, so they need glibc 2.36 or newer (Debian 12, Ubuntu 22.10, and later). Built directly on Ubuntu 24.04 they would need glibc 2.39 and refuse to start on Debian 12. The same base also builds the container image, whose runtime (`gcr.io/distroless/cc-debian12`) carries that glibc.
- **One pinned toolchain.** `RUST_VERSION` in the workflow, and the `Dockerfile`'s `RUST_VERSION` argument, pin the compiler for every artifact, so a release doesn't change compiler because `stable` moved.
- **Windows links the C runtime statically** (`+crt-static`), so the binary runs without the Visual C++ redistributable.
- **No local paths in artifacts.** Builds pass `--remap-path-prefix` for the checkout and the Cargo home, so panic messages and debug info name `/fleetring/...` and `/cargo/...` rather than the build machine's directories.
- **The tag must match the crate.** A `fleetringd-vX.Y.Z` tag fails the release unless `crates/fleetringd/Cargo.toml` says `version = "X.Y.Z"`, which is also what `fleetringd --version` prints.
- **Container image.** Distroless `cc` rather than `debian-slim`: SQLite is compiled into the binary (`rusqlite`'s `bundled` feature), so the runtime needs only glibc and `libgcc_s`, and distroless has no shell or package manager to patch. The image runs as UID 65532, keeps its databases in a `/data` volume owned by that user, and starts with `--config /etc/fleetring/fleetringd.toml`. Each architecture builds on its own runner and pushes by digest; a final job joins the two digests into one tagged manifest (`X.Y.Z`, `X.Y`, and `latest` for a final release).

## Acceptance criteria

- [x] CI runs the workspace tests and the simulation's fixed seeds natively on `ubuntu-24.04-arm`.
- [ ] Pushing a `fleetringd-v*` tag publishes a GitHub release with five archives, their checksums, and `install.sh`.
- [ ] A manual run of the release workflow builds and uploads every archive as a workflow artifact without publishing anything.
- [x] `scripts/install.sh` installs a verified binary on Linux and macOS, on x86_64 and ARM64, and refuses an archive whose checksum doesn't match. *(Checked locally against a mirror directory; the GitHub download path needs a real release.)*
- [ ] `docker run ghcr.io/fleetring/fleetringd:<version> --version` works on `linux/amd64` and `linux/arm64`, as a non-root user.
- [x] The README, `RELEASING.md`, and the archive README describe installation and the release process.
- [x] Every workflow passes `actionlint`, and `scripts/install.sh` passes `shellcheck`.

## Notes

- GitHub's ARM64 Linux runners are free for public repositories. While the repository is private, the ARM job and the ARM release builds need a paid runner, or must be skipped.
- `CHANGELOG.md` documents the `fleetring` crate only, so it gets no entry for the daemon's packaging.
- Not in scope: signing (Sigstore, Apple notarization, Authenticode), SBOMs and build provenance, Homebrew and other package managers, `musl` builds for Alpine and older glibc, and a Windows installer.

## Progress

- 2026-10-28: Implemented. The tag-only criteria wait for the first `fleetringd-v*` tag.
  - CI: the `test (ubuntu-24.04-arm)` job.
  - Release: `.github/workflows/release.yml`, `scripts/install.sh`, and `crates/fleetringd/README.md` for the archives.
  - Container: `Dockerfile`, `.dockerignore`, `deploy/fleetringd.toml`, and `.github/workflows/container.yml`. `crates/fleetringd/tests/deploy.rs` parses the example configuration, so it can't drift from the daemon's schema.
  - Checked locally:
    - `actionlint` (with `shellcheck`) passes on every workflow, `shellcheck` and `sh -n` on the install script, and `hadolint` on the `Dockerfile`.
    - A release build on macOS ARM64 prints `fleetringd 0.0.0` for `--version`. Built with the workflow's remaps, it embeds no path from the build machine.
    - The install script, run under `sh` and `dash` against a local mirror, installs a good archive and refuses one whose checksum doesn't match.
    - The archive README's walkthrough (`status`, `db create`, `db exec`, `db query`, `db locate`) runs against a local daemon, and the daemon starts from `deploy/fleetringd.toml` and stops cleanly on SIGTERM.
  - Not yet checked: the Docker daemon wasn't available, so the image hasn't been built. The workflows themselves run only on GitHub.
