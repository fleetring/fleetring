//! A snapshot directory as one byte stream, for the minimal initial copy (FR-032 replaces it).
//!
//! The engine decides what a snapshot directory holds; this module only moves its files. The
//! archive is each regular file in name order, as a big-endian `u16` name length, the UTF-8 name,
//! a big-endian `u64` content length, and the content.

use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::Path;

use super::epochs::sync_dir;

/// Packs every file directly inside `dir`.
///
/// # Errors
///
/// An I/O error if a file can't be read, or `dir` holds anything but regular files.
pub(super) fn pack(dir: &Path) -> io::Result<Vec<u8>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(invalid(format!(
                "snapshot entry {} isn't a regular file",
                entry.path().display()
            )));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|name| invalid(format!("snapshot file name {name:?} isn't UTF-8")))?;
        names.push(name);
    }
    names.sort();
    let mut archive = Vec::new();
    for name in names {
        let content = fs::read(dir.join(&name))?;
        let name_len = u16::try_from(name.len())
            .map_err(|_| invalid(format!("snapshot file name {name} is too long")))?;
        archive.extend_from_slice(&name_len.to_be_bytes());
        archive.extend_from_slice(name.as_bytes());
        archive.extend_from_slice(
            &u64::try_from(content.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        archive.extend_from_slice(&content);
    }
    Ok(archive)
}

/// Unpacks `archive` into `dir`, which must not exist yet, and syncs every file and the
/// directory.
///
/// # Errors
///
/// An I/O error if the archive is malformed, names a file outside `dir`, or can't be written.
pub(super) fn unpack(mut archive: &[u8], dir: &Path) -> io::Result<()> {
    fs::create_dir(dir)?;
    while !archive.is_empty() {
        let name_len = usize::from(u16::from_be_bytes(take(&mut archive)?));
        let name = std::str::from_utf8(split(&mut archive, name_len)?)
            .map_err(|_| invalid("snapshot file name isn't UTF-8".to_owned()))?;
        if !is_plain_name(name) {
            return Err(invalid(format!(
                "snapshot file name {name:?} isn't a plain name"
            )));
        }
        let len = usize::try_from(u64::from_be_bytes(take(&mut archive)?))
            .map_err(|_| invalid("snapshot file too large".to_owned()))?;
        let content = split(&mut archive, len)?;
        let mut file = File::create(dir.join(name))?;
        file.write_all(content)?;
        file.sync_all()?;
    }
    sync_dir(dir)
}

/// Whether `name` names a file directly inside a directory, and nothing else.
fn is_plain_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', '\0', ':'])
}

fn take<const N: usize>(archive: &mut &[u8]) -> io::Result<[u8; N]> {
    let bytes = split(archive, N)?;
    let mut out = [0; N];
    out.copy_from_slice(bytes);
    Ok(out)
}

fn split<'a>(archive: &mut &'a [u8], len: usize) -> io::Result<&'a [u8]> {
    if archive.len() < len {
        return Err(invalid("snapshot archive ends early".to_owned()));
    }
    let (head, tail) = archive.split_at(len);
    *archive = tail;
    Ok(head)
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_directory_round_trips() {
        let dir = tempfile::tempdir().expect("temp dir");
        let source = dir.path().join("source");
        fs::create_dir(&source).expect("source");
        fs::write(source.join("data.db"), b"pages").expect("file");
        fs::write(source.join("data.db-wal"), b"").expect("file");
        let archive = pack(&source).expect("pack");
        let target = dir.path().join("target");
        unpack(&archive, &target).expect("unpack");
        assert_eq!(fs::read(target.join("data.db")).expect("read"), b"pages");
        assert_eq!(fs::read(target.join("data.db-wal")).expect("read"), b"");
    }

    #[test]
    fn names_that_escape_the_directory_are_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut archive = Vec::new();
        archive.extend_from_slice(&5_u16.to_be_bytes());
        archive.extend_from_slice(b"../x0");
        archive.extend_from_slice(&0_u64.to_be_bytes());
        assert!(unpack(&archive, &dir.path().join("t")).is_err());
        assert!(unpack(&[0, 9, b'a'], &dir.path().join("u")).is_err());
    }
}
