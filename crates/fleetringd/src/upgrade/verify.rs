//! Verifying a download: the cosign signature on `SHA256SUMS`, then the archive's checksum
//! (ADR-0038, point 6).

use std::ffi::OsStr;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::upgrade::UpgradeError;

/// The identity that signs releases: the release workflow, run from a `fleetringd-v` tag, as
/// `RELEASING.md` and `install.sh` name it.
pub(crate) const IDENTITY_REGEXP: &str =
    "^https://github.com/fleetring/fleetring/.github/workflows/release.yml@refs/tags/fleetringd-v";

/// The OIDC issuer of that identity: GitHub Actions.
pub(crate) const ISSUER: &str = "https://token.actions.githubusercontent.com";

/// The oldest cosign that can check a release's bundle, as `install.sh` requires: older ones fail
/// with "bundle does not contain cert".
pub(crate) const COSIGN_MIN: (u64, u64, u64) = (2, 4, 2);

/// Where to get cosign, for the refusal that says it's missing.
const COSIGN_INSTALL: &str = "https://docs.sigstore.dev/cosign/system_config/installation/";

/// The `cosign` program on `PATH`, at [`COSIGN_MIN`] or newer.
#[derive(Debug, Clone)]
pub(crate) struct Cosign {
    program: PathBuf,
}

impl Cosign {
    /// Finds cosign on `PATH` and checks its version.
    ///
    /// # Errors
    ///
    /// [`UpgradeError::CosignMissing`] if there is none, and [`UpgradeError::CosignTooOld`] if
    /// it is older than [`COSIGN_MIN`] or doesn't say its version.
    pub(crate) fn find() -> Result<Self, UpgradeError> {
        let program = on_path("cosign").ok_or(UpgradeError::CosignMissing {
            install: COSIGN_INSTALL,
        })?;
        let output = Command::new(&program)
            .arg("version")
            .output()
            .map_err(|source| UpgradeError::Io {
                path: program.clone(),
                source,
            })?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let found = git_version(&text);
        match found {
            Some(version) if version >= COSIGN_MIN => Ok(Self { program }),
            _ => Err(UpgradeError::CosignTooOld {
                found: found.map_or_else(
                    || "an unknown version".to_owned(),
                    |(major, minor, patch)| format!("{major}.{minor}.{patch}"),
                ),
                install: COSIGN_INSTALL,
            }),
        }
    }

    /// Verifies that `bundle` holds a signature on `sums` by the release workflow, run from a
    /// `fleetringd-v` tag.
    ///
    /// # Errors
    ///
    /// [`UpgradeError::Signature`] with cosign's reason if it doesn't.
    pub(crate) fn verify(&self, sums: &Path, bundle: &Path) -> Result<(), UpgradeError> {
        let output = Command::new(&self.program)
            .arg("verify-blob")
            .arg(sums)
            .arg("--bundle")
            .arg(bundle)
            .args(["--certificate-identity-regexp", IDENTITY_REGEXP])
            .args(["--certificate-oidc-issuer", ISSUER])
            .output()
            .map_err(|source| UpgradeError::Io {
                path: self.program.clone(),
                source,
            })?;
        if output.status.success() {
            return Ok(());
        }
        // cosign's last line says why; the earlier ones are its progress.
        let stderr = String::from_utf8_lossy(&output.stderr);
        let reason = stderr
            .lines()
            .map(str::trim)
            .rfind(|line| !line.is_empty())
            .unwrap_or("cosign exited with an error")
            .to_owned();
        Err(UpgradeError::Signature { reason })
    }
}

/// The first `name` on `PATH` that is a file.
fn on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    let names = [
        name.to_owned(),
        format!("{name}{}", std::env::consts::EXE_SUFFIX),
    ];
    std::env::split_paths(&path)
        .flat_map(|dir| names.iter().map(move |name| dir.join(name)))
        .find(|candidate| candidate.is_file())
}

/// The version on cosign's `GitVersion:` line, such as `v2.4.3`.
fn git_version(text: &str) -> Option<(u64, u64, u64)> {
    let line = text
        .lines()
        .find_map(|line| line.trim().strip_prefix("GitVersion:"))?;
    let version = line.trim().trim_start_matches('v');
    let mut numbers = version.split(['.', '-', '+']).map(str::parse::<u64>);
    Some((
        numbers.next()?.ok()?,
        numbers.next()?.ok()?,
        numbers.next()?.ok()?,
    ))
}

/// Checks that `archive` hashes to the checksum `sums` lists for `name`.
///
/// # Errors
///
/// [`UpgradeError::NotListed`] if `sums` has no line for `name`, and [`UpgradeError::Checksum`]
/// if the hash differs.
pub(crate) fn check_sum(sums: &Path, name: &str, archive: &Path) -> Result<(), UpgradeError> {
    let read = |path: &Path| {
        std::fs::read(path).map_err(|source| UpgradeError::Io {
            path: path.to_owned(),
            source,
        })
    };
    let listing = String::from_utf8_lossy(&read(sums)?).into_owned();
    let expected = listing
        .lines()
        .find_map(|line| {
            let (hash, file) = line.split_once(char::is_whitespace)?;
            let file = file.trim_start();
            let file = file.strip_prefix('*').unwrap_or(file);
            (OsStr::new(file) == OsStr::new(name)).then(|| hash.to_ascii_lowercase())
        })
        .ok_or_else(|| UpgradeError::NotListed {
            file: name.to_owned(),
        })?;
    let digest = ring::digest::digest(&ring::digest::SHA256, &read(archive)?);
    let actual = digest
        .as_ref()
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            // Writing to a `String` can't fail.
            let _ = write!(hex, "{byte:02x}");
            hex
        });
    if actual == expected {
        Ok(())
    } else {
        Err(UpgradeError::Checksum {
            file: name.to_owned(),
            expected,
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosign_versions_are_read_from_the_git_version_line() {
        let text = "  ______\nGitVersion:    v2.4.3\nGitCommit:     abc\n";
        assert_eq!(git_version(text), Some((2, 4, 3)));
        assert_eq!(git_version("GitVersion: v3.1.4-rc.1"), Some((3, 1, 4)));
        assert_eq!(git_version("cosign 2.4.3"), None);
        assert!(git_version("GitVersion: v2.4.1").is_some_and(|v| v < COSIGN_MIN));
    }

    #[test]
    fn a_checksum_is_matched_by_file_name() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let archive = dir.path().join("a.tar.gz");
        std::fs::write(&archive, b"abc")?;
        let sums = dir.path().join("SHA256SUMS");
        let abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        std::fs::write(&sums, format!("{abc}  a.tar.gz\n{abc} *b.zip\n"))?;
        check_sum(&sums, "a.tar.gz", &archive)?;
        std::fs::write(&archive, b"abd")?;
        assert!(matches!(
            check_sum(&sums, "a.tar.gz", &archive),
            Err(UpgradeError::Checksum { .. })
        ));
        assert!(matches!(
            check_sum(&sums, "c.tar.gz", &archive),
            Err(UpgradeError::NotListed { .. })
        ));
        Ok(())
    }
}
