//! The page-size check on copies the adapter didn't write itself (#154).
//!
//! SQLite takes a database's page size from its file's header and a WAL's from the WAL's header.
//! Before 3.54.0, a database opened beside a WAL whose page size was smaller could return
//! uninitialized page-cache memory through ordinary queries (RUSTSEC-2026-0459). User statements
//! can't set the page size, but an archive's snapshot and a peer's snapshot or batches are bytes
//! from elsewhere: whoever can write the bucket, or a faulty peer, chooses them. So before a copy
//! is opened, and before a batch is mirrored, their page sizes are compared, and a copy or batch
//! that disagrees is refused without anything being opened.
//!
//! A database file with no header yet (an empty file) and a WAL that is absent, shorter than its
//! header, or whose header SQLite would ignore as invalid, carry no page size, and agree with
//! anything; a database file too short for a header, or whose header isn't SQLite's, is refused.

use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use fleetring_wal::{DATABASE_HEADER_LEN, WAL_HEADER_LEN, WalHeader, database_page_size};

use super::{storage, wal_path};
use crate::engine::EngineError;

/// Where a page size was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Source {
    /// The database file's header.
    Database,
    /// The WAL's header.
    Wal,
    /// A replicated batch of WAL frames.
    Batch,
    /// `[engine] page_size`.
    Configured,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Database => "the database file's page size",
            Self::Wal => "the WAL's page size",
            Self::Batch => "the WAL batch's page size",
            Self::Configured => "the configured [engine] page_size",
        })
    }
}

/// Two page sizes that must agree, and don't.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{found} is {found_size} bytes, but {expected} is {expected_size}")]
pub(super) struct Mismatch {
    pub(super) found: Source,
    pub(super) found_size: u32,
    pub(super) expected: Source,
    pub(super) expected_size: u32,
}

/// Checks that the database file at `database`, its WAL, and `configured` if given agree on the
/// page size.
///
/// # Errors
///
/// [`EngineError::UnsafeCopy`] if any two disagree, or the database file's header isn't a SQLite
/// database's, and [`EngineError::Storage`] if the files can't be read.
pub(super) fn check(database: &Path, configured: Option<u32>) -> Result<(), EngineError> {
    let configured = configured.map(|size| (Source::Configured, size));
    let mut sizes = [
        database_file(database)?,
        wal(&wal_path(database))?,
        configured,
    ]
    .into_iter()
    .flatten();
    let Some(first) = sizes.next() else {
        return Ok(());
    };
    sizes.try_for_each(|size| agree(size, first).map_err(unsafe_copy))
}

/// The database file's page size, or `None` if the file is empty or absent.
pub(super) fn database_file(database: &Path) -> Result<Option<(Source, u32)>, EngineError> {
    let header = read_prefix(database, DATABASE_HEADER_LEN).map_err(storage)?;
    if header.is_empty() {
        return Ok(None);
    }
    database_page_size(&header)
        .map(|size| Some((Source::Database, size)))
        .map_err(|error| EngineError::UnsafeCopy {
            reason: format!("the database file's header is unusable: {error}"),
        })
}

/// The WAL's page size, or `None` if it has no header SQLite would use.
fn wal(wal: &Path) -> Result<Option<(Source, u32)>, EngineError> {
    let header = read_prefix(wal, WAL_HEADER_LEN).map_err(storage)?;
    Ok(WalHeader::parse(&header)
        .ok()
        .map(|header| (Source::Wal, header.page_size())))
}

/// Fails unless `found` agrees with `expected`.
pub(super) fn agree(found: (Source, u32), expected: (Source, u32)) -> Result<(), Mismatch> {
    if found.1 == expected.1 {
        return Ok(());
    }
    Err(Mismatch {
        found: found.0,
        found_size: found.1,
        expected: expected.0,
        expected_size: expected.1,
    })
}

fn unsafe_copy(mismatch: Mismatch) -> EngineError {
    EngineError::UnsafeCopy {
        reason: mismatch.to_string(),
    }
}

/// Up to the first `len` bytes of the file at `path`; none if it doesn't exist.
fn read_prefix(path: &Path, len: usize) -> io::Result<Vec<u8>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::with_capacity(len);
    file.take(u64::try_from(len).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}
