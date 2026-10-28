//! Where each database lives under the data directory.

use std::fmt::Write as _;
use std::path::PathBuf;

use fleetring_control::DatabaseName;

/// The longest path component a database's directory uses, well under every file system's limit.
const COMPONENT_LEN: usize = 128;

/// The directory of `name`, relative to the databases root.
///
/// The name is encoded so that every database gets its own directory on every file system,
/// including case-insensitive ones: lowercase ASCII letters, digits, `-`, and `_` stay as they
/// are, and every other byte becomes `%` and two lowercase hex digits. The encoding is split into
/// components of at most 128 bytes. Components never contain `.`, so they can't collide with the
/// files an engine keeps inside a database's directory.
pub(crate) fn database_dir(name: &DatabaseName) -> PathBuf {
    let mut encoded = String::with_capacity(name.as_str().len());
    for byte in name.as_str().bytes() {
        if byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_' {
            encoded.push(char::from(byte));
        } else {
            // Writing to a `String` can't fail.
            let _ = write!(encoded, "%{byte:02x}");
        }
    }
    let mut dir = PathBuf::new();
    let mut component = String::with_capacity(COMPONENT_LEN);
    for c in encoded.chars() {
        if component.len() == COMPONENT_LEN {
            dir.push(&component);
            component.clear();
        }
        component.push(c);
    }
    dir.push(component);
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        database_dir(&name.parse().expect("valid name"))
    }

    #[test]
    fn safe_names_are_kept() {
        assert_eq!(dir("tenant-42_a"), PathBuf::from("tenant-42_a"));
    }

    #[test]
    fn other_bytes_are_escaped_so_case_never_collides() {
        assert_eq!(dir("Acme"), PathBuf::from("%41cme"));
        assert_ne!(dir("Acme"), dir("acme"));
        assert_eq!(dir("a/b.c"), PathBuf::from("a%2fb%2ec"));
        assert_eq!(dir(".."), PathBuf::from("%2e%2e"));
    }

    #[test]
    fn long_names_split_into_short_components() {
        let name = "x".repeat(255);
        let path = dir(&name);
        let lens: Vec<usize> = path
            .components()
            .map(|component| component.as_os_str().len())
            .collect();
        assert_eq!(lens, vec![128, 127]);
        let escaped = dir(&"é".repeat(127));
        assert!(
            escaped
                .components()
                .all(|component| component.as_os_str().len() <= COMPONENT_LEN)
        );
    }
}
