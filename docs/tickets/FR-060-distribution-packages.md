---
id: FR-060
title: Distribution packages for apt, yum, the AUR, and Homebrew
status: todo
priority: P2
size: L
milestone: M12 Distribution
depends_on: [FR-034, FR-048]
adrs: []
---

# FR-060: Distribution packages for apt, yum, the AUR, and Homebrew

## Goal

Today `fleetringd` installs from a release archive, `scripts/install.sh`, or the container image. People on Debian, Ubuntu, Fedora, Arch (and Omarchy), and macOS expect their package manager to install and upgrade it. This ticket adds those channels without weakening what FR-048 established: every file a user installs traces back to a signed, attested release build, and no channel rebuilds the binary somewhere we can't verify, except a source package that says so.

## Scope

Channels, in the order they ship:

- **`.deb` and `.rpm` in the release workflow.**
  - `release.yml` builds them for `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` with `cargo-deb` and `cargo-generate-rpm`, from the same binaries the archives hold; no second compile.
  - Package metadata lives in `crates/fleetringd/Cargo.toml` (`[package.metadata.deb]`, `[package.metadata.generate-rpm]`): the binary in `/usr/bin`, the licenses, the README, `deploy/fleetringd.toml` as an example under `/usr/share/doc/fleetringd/`, a `fleetringd` system user, and a systemd unit that is installed but not enabled.
  - The packages join `SHA256SUMS`, its cosign signature, and the build-provenance attestations exactly as the archives do, and are attached to the GitHub release.
- **AUR `fleetringd-bin`.**
  - A `PKGBUILD` that downloads the signed release archive for the machine's architecture, checks it against `SHA256SUMS`, and installs the binary, licenses, README, and the systemd unit. For Arch and Omarchy users through `yay` or `paru`.
  - The `PKGBUILD` and `.SRCINFO` live in this repository under `packaging/aur/fleetringd-bin/`, and the release workflow (or a documented manual step, until an AUR SSH key is set up as a repository secret) pushes them to the AUR.
- **AUR `fleetringd`** (source).
  - A `PKGBUILD` that builds from the tagged source with `cargo build --release --locked` and the pinned toolchain's MSRV or later. It says in its description that it is built locally, so it carries no release attestation.
- **Homebrew tap `fleetring/homebrew-tap`.**
  - A `fleetringd` formula that installs the release archive for macOS (Intel and Apple silicon) and Linux, with `sha256` per archive and a `test do` block that runs `fleetringd --version`.
  - The release workflow opens or pushes the formula update to the tap after the GitHub release is published, using a token scoped to the tap repository only.
- **Later, not in this ticket's acceptance:** Scoop and winget manifests for Windows; a Nix flake or nixpkgs package; a hosted, signed apt and yum repository (so `apt upgrade` sees new versions without a manual download). Each gets its own ticket when someone asks for it.
- **Docs:** the README's install section lists every channel; `RELEASING.md` lists the new files a release has and the steps that publish to the AUR and the tap; `SECURITY.md` says which channels carry attestations and which build from source.
- **Not in scope:** packaging the `fleetring` crate (crates.io is its channel); changing what the binary does when installed by a package (it still needs a configuration file and certificates).

## Acceptance criteria

- [ ] **`.deb`:** a tag's release lists `fleetringd_<version>_amd64.deb` and `_arm64.deb`. Each is in `SHA256SUMS` and passes `gh attestation verify`. `apt install ./fleetringd_<version>_amd64.deb` on Debian 12 and Ubuntu 24.04 installs `/usr/bin/fleetringd`, which prints the release version, and the systemd unit, which isn't enabled. A dry run of `release.yml` builds and installs both in a container.
- [ ] **`.rpm`:** the same for `fleetringd-<version>-1.x86_64.rpm` and `.aarch64.rpm` on Fedora and Rocky Linux, installed with `dnf install`.
- [ ] **AUR `fleetringd-bin`:** `paru -S fleetringd-bin` (or `yay -S`) on Arch installs the release binary for x86_64 and aarch64, and `namcap` reports no errors. The `PKGBUILD`'s checksums are the release's `SHA256SUMS` entries, and the AUR package is updated within a day of a release.
- [ ] **AUR `fleetringd`:** `makepkg -si` builds the tagged source with `--locked` and installs the same files.
- [ ] **Homebrew:** `brew install fleetring/tap/fleetringd` installs the release binary on macOS (arm64 and x86_64) and Linux, `brew test fleetringd` passes, and a tag's release updates the formula without a manual edit.
- [ ] **Docs:** the README, `RELEASING.md`, and `SECURITY.md` cover every channel above, and `RELEASING.md`'s verification checklist covers the new files.

## Progress

- Not started.
