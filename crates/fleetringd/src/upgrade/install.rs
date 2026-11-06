//! Extracting the new binary and swapping it in atomically, keeping the previous one (ADR-0038,
//! point 7).

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::process::Command;

use fleetring_control::Release;

use crate::upgrade::UpgradeError;

/// The largest binary an archive may hold.
const MAX_BINARY_BYTES: u64 = 512 * 1024 * 1024;

fn io_error(path: &Path) -> impl FnOnce(io::Error) -> UpgradeError + '_ {
    move |source| UpgradeError::Io {
        path: path.to_owned(),
        source,
    }
}

/// The binary's name in a release archive from 0.7 on, which holds it under its compatibility
/// name too (ADR-0043, point 3).
const BINARY: &str = "fleetring";

/// The binary's name in a release archive before 0.7, and its compatibility name since.
const COMPAT_BINARY: &str = "fleetringd";

/// Extracts the binary from the `.tar.gz` at `archive` into `into`, and nothing else: the regular
/// file `fleetringd-<release>-<target>/fleetring` if the archive has it, else
/// `fleetringd-<release>-<target>/fleetringd`, as a release before 0.7 holds it (ADR-0043,
/// point 3.3). The archive is read in one pass.
///
/// # Errors
///
/// [`UpgradeError::Archive`] if the archive can't be read, holds neither file, or holds the one it
/// would use as anything but a regular file.
pub(crate) fn extract(
    archive: &Path,
    release: &Release,
    target: &str,
    into: &Path,
) -> Result<(), UpgradeError> {
    let dir = format!("fleetringd-{release}-{target}");
    let wanted = format!("{dir}/{BINARY}");
    let compat = format!("{dir}/{COMPAT_BINARY}");
    let bad = |reason: String| UpgradeError::Archive { reason };
    let file = File::open(archive).map_err(io_error(archive))?;
    let mut entries = tar::Archive::new(flate2::read::GzDecoder::new(file));
    // The compatibility entry, if it came first: used only if the archive has no `fleetring`.
    let mut fallback: Option<Result<(), UpgradeError>> = None;
    for entry in entries.entries().map_err(|error| bad(error.to_string()))? {
        let entry = entry.map_err(|error| bad(error.to_string()))?;
        let path = entry.path().map_err(|error| bad(error.to_string()))?;
        let Some(path) = path.to_str().map(str::to_owned) else {
            continue;
        };
        if path == wanted {
            return copy_binary(entry, &wanted, into);
        }
        if path == compat && fallback.is_none() {
            // Copied now, since a tar stream can't be read twice; `fleetring`, if it follows,
            // overwrites it.
            fallback = Some(copy_binary(entry, &compat, into));
        }
    }
    fallback.unwrap_or_else(|| Err(bad(format!("it holds neither {wanted} nor {compat}"))))
}

/// Copies the archive's `entry`, named `name`, into `into`, if it is a regular file no larger than
/// [`MAX_BINARY_BYTES`].
fn copy_binary<R: io::Read>(
    entry: tar::Entry<'_, R>,
    name: &str,
    into: &Path,
) -> Result<(), UpgradeError> {
    let bad = |reason: String| UpgradeError::Archive { reason };
    if !entry.header().entry_type().is_file() {
        return Err(bad(format!("{name} isn't a regular file")));
    }
    let mut out = File::create(into).map_err(io_error(into))?;
    let copied = io::copy(&mut entry.take(MAX_BINARY_BYTES + 1), &mut out)
        .map_err(|error| bad(error.to_string()))?;
    if copied > MAX_BINARY_BYTES {
        return Err(bad(format!(
            "{name} is larger than {MAX_BINARY_BYTES} bytes"
        )));
    }
    out.sync_all().map_err(io_error(into))?;
    make_executable(into)
}

/// Checks that the binary at `path` runs and prints `fleetring <release>`, or `fleetringd
/// <release>`.
///
/// # Errors
///
/// [`UpgradeError::Binary`] otherwise.
pub(crate) fn check_binary(path: &Path, release: &Release) -> Result<(), UpgradeError> {
    let printed = version_of(path)?;
    if printed == *release {
        Ok(())
    } else {
        Err(UpgradeError::Binary {
            reason: format!("it says it is fleetring {printed}, not {release}"),
        })
    }
}

/// The release the binary at `path` says it is, from `--version`: `fleetring <version>`, or
/// `fleetringd <version>` from a release before 0.7 or a binary run under that name.
///
/// # Errors
///
/// [`UpgradeError::Binary`] if it doesn't run or prints something else.
pub(crate) fn version_of(path: &Path) -> Result<Release, UpgradeError> {
    let output = Command::new(path)
        .arg("--version")
        .output()
        .map_err(|error| UpgradeError::Binary {
            reason: format!("{} doesn't run: {error}", path.display()),
        })?;
    let text = String::from_utf8_lossy(&output.stdout);
    let printed = text.trim();
    printed
        .strip_prefix("fleetring ")
        .or_else(|| printed.strip_prefix("fleetringd "))
        .and_then(|version| version.parse().ok())
        .filter(|_| output.status.success())
        .ok_or_else(|| UpgradeError::Binary {
            reason: format!(
                "{} --version printed '{printed}', not 'fleetring <version>'",
                path.display(),
            ),
        })
}

/// What a binary says it runs: its release and the fleet features it supports, by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reported {
    pub(crate) release: Release,
    pub(crate) features: BTreeSet<String>,
}

impl Reported {
    /// What this binary runs.
    pub(crate) fn this_binary() -> Self {
        let software = crate::software::running();
        Self {
            release: software.release().clone(),
            features: software
                .features()
                .map(|feature| feature.as_str().to_owned())
                .collect(),
        }
    }
}

/// What the binary at `path` runs, from `--version` and `upgrade --software`. A binary without
/// `upgrade --software`, such as 0.3.0, supports no fleet features.
///
/// # Errors
///
/// [`UpgradeError::Binary`] if it doesn't run or doesn't say its version.
pub(crate) fn software_of(path: &Path) -> Result<Reported, UpgradeError> {
    /// `upgrade --software`'s output; the other fields are ignored.
    #[derive(serde::Deserialize)]
    struct Printed {
        version: String,
        #[serde(default)]
        features: Vec<String>,
    }
    let release = version_of(path)?;
    let features = Command::new(path)
        .args(["upgrade", "--software"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| serde_json::from_slice::<Printed>(&output.stdout).ok())
        .filter(|printed| printed.version == release.to_string())
        .map(|printed| printed.features.into_iter().collect())
        .unwrap_or_default();
    Ok(Reported { release, features })
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<(), UpgradeError> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).map_err(io_error(path))
}

#[cfg(not(unix))]
fn make_executable(_: &Path) -> Result<(), UpgradeError> {
    Ok(())
}

/// The installed binary, with its previous version kept beside it as `<name>.previous`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Installed {
    path: PathBuf,
}

impl Installed {
    /// The binary at `path`, with symbolic links resolved, so the file replaced is the file that
    /// runs.
    pub(crate) fn at(path: &Path) -> Result<Self, UpgradeError> {
        let path = fs::canonicalize(path).map_err(io_error(path))?;
        Ok(Self { path })
    }

    /// If this binary is another's [`Installed::previous`], returns that other one.
    pub(crate) fn kept_for(&self) -> Option<Self> {
        let name = self.path.file_name()?.to_str()?;
        let installed = self.path.with_file_name(name.strip_suffix(".previous")?);
        installed.is_file().then_some(Self { path: installed })
    }

    /// Returns the binary's path.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Returns where the previous binary is kept.
    pub(crate) fn previous(&self) -> PathBuf {
        self.sibling("", ".previous")
    }

    /// A path in the binary's directory: its file name with `prefix` and `suffix`.
    fn sibling(&self, prefix: &str, suffix: &str) -> PathBuf {
        let name = self
            .path
            .file_name()
            .map_or_else(|| BINARY.into(), |name| name.to_string_lossy());
        self.path.with_file_name(format!("{prefix}{name}{suffix}"))
    }

    /// Puts the `fleetring` name beside a binary installed as `fleetringd`, as a 0.6 upgrader
    /// leaves it (ADR-0043, point 3.4): if the installed file is named `fleetringd` and nothing
    /// is named `fleetring` beside it, creates the relative symbolic link `fleetring ->
    /// fleetringd`. Returns the link's path if it made one. On Windows, which installs two
    /// copies, it does nothing.
    ///
    /// # Errors
    ///
    /// [`UpgradeError::Io`] if the directory can't be read or the link can't be made.
    pub(crate) fn complete_names(&self) -> Result<Option<PathBuf>, UpgradeError> {
        #[cfg(unix)]
        {
            if self.path.file_name() != Some(std::ffi::OsStr::new(COMPAT_BINARY)) {
                return Ok(None);
            }
            let link = self.path.with_file_name(BINARY);
            match fs::symlink_metadata(&link) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error(&link)(error)),
                Ok(_) => return Ok(None),
            }
            std::os::unix::fs::symlink(COMPAT_BINARY, &link).map_err(io_error(&link))?;
            self.sync_directory()?;
            Ok(Some(link))
        }
        #[cfg(not(unix))]
        Ok(None)
    }

    /// Checks that a new binary can be put beside the installed one, before anything is
    /// downloaded.
    ///
    /// # Errors
    ///
    /// [`UpgradeError::NotWritable`] if the directory can't be written, as in the container
    /// image.
    pub(crate) fn check_writable(&self) -> Result<(), UpgradeError> {
        let probe = self.sibling(".", ".probe");
        let created = File::create(&probe);
        let _gone = fs::remove_file(&probe);
        created
            .map(drop)
            .map_err(|source| UpgradeError::NotWritable {
                path: self.path.clone(),
                source,
            })
    }

    /// Replaces the installed binary with the one at `new`, keeping the installed one as
    /// [`Installed::previous`]. Each step writes a temporary file beside its destination and
    /// renames it over it, so both paths always hold a whole binary.
    ///
    /// # Errors
    ///
    /// [`UpgradeError::Io`] if a step fails; the installed binary is then unchanged, or already
    /// the new one if only the directory sync failed.
    pub(crate) fn replace_with(&self, new: &Path) -> Result<(), UpgradeError> {
        self.keep_as(&self.previous())?;
        let staged = self.sibling(".", ".new");
        fs::copy(new, &staged).map_err(io_error(&staged))?;
        make_executable(&staged)?;
        // Windows flushes a file only through a handle that may write it.
        fs::OpenOptions::new()
            .write(true)
            .open(&staged)
            .and_then(|file| file.sync_all())
            .map_err(io_error(&staged))?;
        fs::rename(&staged, &self.path).map_err(io_error(&self.path))?;
        self.sync_directory()
    }

    /// Swaps the installed binary with [`Installed::previous`], so that a second rollback rolls
    /// forward again.
    ///
    /// # Errors
    ///
    /// [`UpgradeError::NoPrevious`] if there is no previous binary, or [`UpgradeError::Io`].
    pub(crate) fn roll_back(&self) -> Result<(), UpgradeError> {
        let previous = self.previous();
        if !previous.is_file() {
            return Err(UpgradeError::NoPrevious { path: previous });
        }
        let current = self.sibling(".", ".rollback");
        self.keep_as(&current)?;
        fs::rename(&previous, &self.path).map_err(io_error(&self.path))?;
        fs::rename(&current, &previous).map_err(io_error(&previous))?;
        self.sync_directory()
    }

    /// Puts a copy of the installed binary at `destination`, atomically: a hard link if the file
    /// system allows one, else a copy, written under a temporary name and renamed.
    fn keep_as(&self, destination: &Path) -> Result<(), UpgradeError> {
        let staged = self.sibling(".", ".keep");
        match fs::remove_file(&staged) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => {
                return Err(io_error(&staged)(error));
            }
            _ => {}
        }
        if fs::hard_link(&self.path, &staged).is_err() {
            fs::copy(&self.path, &staged).map_err(io_error(&staged))?;
        }
        fs::rename(&staged, destination).map_err(io_error(destination))
    }

    /// Makes the renames durable.
    fn sync_directory(&self) -> Result<(), UpgradeError> {
        #[cfg(unix)]
        if let Some(dir) = self.path.parent() {
            File::open(dir)
                .and_then(|dir| dir.sync_all())
                .map_err(io_error(dir))?;
        }
        Ok(())
    }

    /// Returns whether the installed binary's bytes equal the file at `other`.
    #[cfg(test)]
    fn same_bytes(&self, other: &Path) -> bool {
        let read = |path: &Path| {
            let mut bytes = Vec::new();
            File::open(path).and_then(|mut file| file.read_to_end(&mut bytes))?;
            Ok::<_, io::Error>(bytes)
        };
        matches!((read(&self.path), read(other)), (Ok(a), Ok(b)) if a == b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One entry of a test archive.
    enum Entry<'a> {
        File(&'a str, &'a [u8]),
        Symlink(&'a str, &'a str),
    }

    /// Writes a `.tar.gz` holding `entries` to `path`.
    fn write_archive(path: &Path, entries: &[Entry<'_>]) -> io::Result<()> {
        let mut archive = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        for entry in entries {
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o755);
            match entry {
                Entry::File(name, bytes) => {
                    header.set_size(u64::try_from(bytes.len()).map_err(io::Error::other)?);
                    header.set_cksum();
                    archive.append_data(&mut header, name, *bytes)?;
                }
                Entry::Symlink(name, target) => {
                    header.set_entry_type(tar::EntryType::Symlink);
                    header.set_size(0);
                    archive.append_link(&mut header, name, target)?;
                }
            }
        }
        fs::write(path, archive.into_inner()?.finish()?)
    }

    #[test]
    fn extract_prefers_fleetring_and_falls_back_to_fleetringd() -> Result<(), UpgradeError> {
        let dir = tempfile::tempdir().map_err(io_error(Path::new(".")))?;
        let release: Release = "0.7.0".parse().map_err(|_| UpgradeError::Unsupported {
            reason: "a release",
        })?;
        let target = "x86_64-unknown-linux-gnu";
        let prefix = format!("fleetringd-{release}-{target}");
        let at = |name: &str| format!("{prefix}/{name}");
        let into = dir.path().join("out");
        let extracted = |entries: &[Entry<'_>]| {
            let archive = dir.path().join("release.tar.gz");
            write_archive(&archive, entries).map_err(io_error(&archive))?;
            let _gone = fs::remove_file(&into);
            extract(&archive, &release, target, &into)?;
            fs::read(&into).map_err(io_error(&into))
        };
        let (fleetring, fleetringd) = (at("fleetring"), at("fleetringd"));
        let readme = at("README.md");

        // Both names, in either order: the `fleetring` entry.
        for entries in [
            &[
                Entry::File(&readme, b"read me"),
                Entry::File(&fleetring, b"new"),
                Entry::File(&fleetringd, b"compat"),
            ][..],
            &[
                Entry::File(&fleetringd, b"compat"),
                Entry::File(&fleetring, b"new"),
            ],
        ] {
            assert_eq!(extracted(entries)?, b"new");
        }
        // A release before 0.7: only `fleetringd`.
        assert_eq!(extracted(&[Entry::File(&fleetringd, b"old")])?, b"old");
        // Only a link named `fleetringd`: refused, as 0.6's extractor refuses it.
        let link = extracted(&[Entry::Symlink(&fleetringd, "fleetring")]);
        assert!(
            matches!(&link, Err(UpgradeError::Archive { reason }) if reason.contains("isn't a regular file")),
            "{link:?}"
        );
        // Neither name: the error names both.
        let neither = extracted(&[Entry::File(&readme, b"read me")]);
        assert!(
            matches!(&neither, Err(UpgradeError::Archive { reason })
                if reason.contains(&fleetring) && reason.contains(&fleetringd)),
            "{neither:?}"
        );
        Ok(())
    }

    #[cfg(unix)]
    fn script(path: &Path, printed: &str) -> Result<(), UpgradeError> {
        fs::write(path, format!("#!/bin/sh\necho '{printed}'\n")).map_err(io_error(path))?;
        make_executable(path)
    }

    #[cfg(unix)]
    #[test]
    fn version_of_accepts_both_names() -> Result<(), UpgradeError> {
        let dir = tempfile::tempdir().map_err(io_error(Path::new(".")))?;
        for (name, printed) in [("new", "fleetring 0.7.0"), ("old", "fleetringd 0.6.0")] {
            let path = dir.path().join(name);
            script(&path, printed)?;
            assert_eq!(
                version_of(&path)?.to_string(),
                printed.split(' ').nth(1).unwrap_or("")
            );
        }
        let other = dir.path().join("other");
        script(&other, "fleet 0.7.0")?;
        let refused = version_of(&other);
        assert!(
            matches!(&refused, Err(UpgradeError::Binary { reason }) if reason.contains("not 'fleetring <version>'")),
            "{refused:?}"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn complete_names_links_fleetring_beside_a_fleetringd_file() -> Result<(), UpgradeError> {
        let dir = tempfile::tempdir().map_err(io_error(Path::new(".")))?;
        let path = dir.path().join("fleetringd");
        fs::write(&path, b"binary").map_err(io_error(&path))?;
        let installed = Installed::at(&path)?;
        let Some(link) = installed.complete_names()? else {
            panic!("no link beside {}", installed.path().display());
        };
        assert_eq!(link, installed.path().with_file_name("fleetring"));
        assert_eq!(
            fs::read_link(&link).map_err(io_error(&link))?,
            PathBuf::from("fleetringd")
        );
        assert_eq!(fs::read(&link).ok(), Some(b"binary".to_vec()));
        // Done once: a second call leaves it.
        assert_eq!(installed.complete_names()?, None);
        // Through the link, the installed file is still `fleetringd`.
        assert_eq!(Installed::at(&link)?, installed);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn complete_names_leaves_an_existing_fleetring_alone() -> Result<(), UpgradeError> {
        let dir = tempfile::tempdir().map_err(io_error(Path::new(".")))?;
        let fleetringd = dir.path().join("fleetringd");
        let fleetring = dir.path().join("fleetring");
        fs::write(&fleetringd, b"binary").map_err(io_error(&fleetringd))?;
        fs::write(&fleetring, b"another").map_err(io_error(&fleetring))?;
        assert_eq!(Installed::at(&fleetringd)?.complete_names()?, None);
        assert_eq!(fs::read(&fleetring).ok(), Some(b"another".to_vec()));
        // A dangling link counts as there, too.
        fs::remove_file(&fleetring).map_err(io_error(&fleetring))?;
        std::os::unix::fs::symlink("nowhere", &fleetring).map_err(io_error(&fleetring))?;
        assert_eq!(Installed::at(&fleetringd)?.complete_names()?, None);
        // A fresh 0.7 install: `fleetring` is the file, and needs nothing.
        let fresh = tempfile::tempdir().map_err(io_error(Path::new(".")))?;
        let real = fresh.path().join("fleetring");
        fs::write(&real, b"binary").map_err(io_error(&real))?;
        assert_eq!(Installed::at(&real)?.complete_names()?, None);
        assert!(!fresh.path().join("fleetringd").exists());
        Ok(())
    }

    #[test]
    fn replacing_keeps_the_previous_binary_and_rollback_swaps_them() -> Result<(), UpgradeError> {
        let dir = tempfile::tempdir().map_err(io_error(Path::new(".")))?;
        let path = dir.path().join("fleetringd");
        let new = dir.path().join("new");
        fs::write(&path, b"old").map_err(io_error(&path))?;
        fs::write(&new, b"new").map_err(io_error(&new))?;
        let installed = Installed::at(&path)?;
        assert!(matches!(
            installed.roll_back(),
            Err(UpgradeError::NoPrevious { .. })
        ));
        installed.replace_with(&new)?;
        assert!(installed.same_bytes(&new));
        assert_eq!(fs::read(installed.previous()).ok(), Some(b"old".to_vec()));
        installed.roll_back()?;
        assert_eq!(fs::read(&path).ok(), Some(b"old".to_vec()));
        assert_eq!(fs::read(installed.previous()).ok(), Some(b"new".to_vec()));
        installed.roll_back()?;
        assert!(installed.same_bytes(&new));
        // Nothing temporary is left behind.
        let mut names: Vec<String> = fs::read_dir(dir.path())
            .map_err(io_error(dir.path()))?
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["fleetringd", "fleetringd.previous", "new"]);
        Ok(())
    }
}
