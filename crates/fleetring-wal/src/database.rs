//! The page size in a SQLite database file's 100-byte header.
//!
//! SQLite takes a database's page size from its file's header, and a WAL's from the WAL header.
//! A copy whose two disagree must never be opened: before 3.54.0, SQLite could then return
//! uninitialized page-cache memory through ordinary queries (RUSTSEC-2026-0459). Comparing the
//! two needs both, so the database header's page size is read here, next to the WAL header's
//! ([`WalHeader::page_size`](crate::WalHeader::page_size)).

use crate::header::is_valid_page_size;

/// The length of a SQLite database file's header in bytes.
pub const DATABASE_HEADER_LEN: usize = 100;

/// The 16 bytes every SQLite database file starts with.
const MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// The offset of the 2-byte big-endian page size in the database header.
const PAGE_SIZE_OFFSET: usize = 16;

/// The page size in the database header at the start of `bytes`: a power of two from 512 to
/// 65536.
///
/// The header stores 65536 as 1, since it doesn't fit in its two bytes. Bytes after the first
/// [`DATABASE_HEADER_LEN`] are ignored, so a whole database file, or its first page, may be
/// passed.
///
/// # Errors
///
/// [`DatabaseHeaderError`] if `bytes` is shorter than a header, doesn't start with SQLite's magic
/// string, or holds a page size SQLite doesn't support.
///
/// # Examples
///
/// ```
/// use fleetring_wal::database_page_size;
///
/// let mut header = [0_u8; 100];
/// header[..16].copy_from_slice(b"SQLite format 3\0");
/// header[16..18].copy_from_slice(&4096_u16.to_be_bytes());
/// assert_eq!(database_page_size(&header), Ok(4096));
/// ```
pub fn database_page_size(bytes: &[u8]) -> Result<u32, DatabaseHeaderError> {
    let Some(header) = bytes.first_chunk::<DATABASE_HEADER_LEN>() else {
        return Err(DatabaseHeaderError::TooShort { len: bytes.len() });
    };
    if !header.starts_with(MAGIC) {
        return Err(DatabaseHeaderError::BadMagic);
    }
    let stored = u16::from_be_bytes([header[PAGE_SIZE_OFFSET], header[PAGE_SIZE_OFFSET + 1]]);
    let page_size = match stored {
        1 => 65_536,
        other => u32::from(other),
    };
    if !is_valid_page_size(page_size) {
        return Err(DatabaseHeaderError::InvalidPageSize { stored });
    }
    Ok(page_size)
}

/// An error from reading a database file's header.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DatabaseHeaderError {
    /// The input is shorter than a database header.
    #[error("database header needs {DATABASE_HEADER_LEN} bytes, got {len}")]
    TooShort {
        /// The input's length in bytes.
        len: usize,
    },

    /// The input doesn't start with `SQLite format 3\0`.
    #[error("not a sqlite database header")]
    BadMagic,

    /// The stored page size isn't a power of two from 512 to 65536.
    #[error("invalid database page size field {stored}")]
    InvalidPageSize {
        /// The two-byte field as stored.
        stored: u16,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(stored: u16) -> [u8; DATABASE_HEADER_LEN] {
        let mut bytes = [0; DATABASE_HEADER_LEN];
        bytes[..MAGIC.len()].copy_from_slice(MAGIC);
        bytes[PAGE_SIZE_OFFSET..PAGE_SIZE_OFFSET + 2].copy_from_slice(&stored.to_be_bytes());
        bytes
    }

    #[test]
    fn every_supported_page_size_reads_back() {
        for shift in 9..=15 {
            let page_size: u16 = 1 << shift;
            assert_eq!(
                database_page_size(&header(page_size)),
                Ok(u32::from(page_size))
            );
        }
        assert_eq!(database_page_size(&header(1)), Ok(65_536), "1 means 65536");
    }

    #[test]
    fn unsupported_page_sizes_are_refused() {
        for stored in [0, 2, 256, 1000, 4095] {
            assert_eq!(
                database_page_size(&header(stored)),
                Err(DatabaseHeaderError::InvalidPageSize { stored })
            );
        }
    }

    #[test]
    fn short_or_foreign_headers_are_refused() {
        assert_eq!(
            database_page_size(&header(4096)[..99]),
            Err(DatabaseHeaderError::TooShort { len: 99 })
        );
        assert_eq!(
            database_page_size(&[]),
            Err(DatabaseHeaderError::TooShort { len: 0 })
        );
        let mut foreign = header(4096);
        foreign[0] = b'X';
        assert_eq!(
            database_page_size(&foreign),
            Err(DatabaseHeaderError::BadMagic)
        );
    }

    #[test]
    fn bytes_past_the_header_are_ignored() {
        let mut page = vec![0xAB; 4096];
        page[..DATABASE_HEADER_LEN].copy_from_slice(&header(8192));
        assert_eq!(database_page_size(&page), Ok(8192));
    }
}
