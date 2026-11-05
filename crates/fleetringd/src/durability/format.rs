//! The bytes of an archive's objects (ADR-0029, points 5 and 6): segments in a small binary
//! frame, and snapshot descriptors as JSON.
//!
//! A segment is, with every integer big-endian:
//!
//! ```text
//! magic        "FRSEG\x01"                6 bytes
//! header_len   u32, then the header as JSON (SegmentHeader)
//! payload      the batch's opaque payload, payload_len bytes
//! checksum     XXH3-128 over everything before it, 16 bytes
//! ```
//!
//! A database's metadata object (ADR-0041, point 10) is JSON as well, and its name carries its
//! XXH3-128 checksum.
//!
//! Every format carries a `format` number. A reader refuses a number newer than its own before
//! it looks at any other field, and refuses a field it doesn't know, so a newer writer's object
//! is never half-understood.

use std::collections::BTreeMap;

use fleetring_control::{MAX_DESCRIPTION_BYTES, MAX_LABELS};
use serde::{Deserialize, Serialize};
use twox_hash::XxHash3_128;

/// The magic bytes every segment starts with.
pub const SEGMENT_MAGIC: &[u8; 6] = b"FRSEG\x01";

/// The format number this version writes and the newest it reads.
pub const FORMAT: u32 = 1;

/// The format number of a metadata object: its own, apart from [`FORMAT`], since it can change
/// without the segments and descriptors changing.
pub const METADATA_FORMAT: u32 = 1;

/// The largest metadata object a restore reads and an archive writes, in bytes. A description
/// holds at most [`MAX_DESCRIPTION_BYTES`] and a database at most [`MAX_LABELS`] labels of at
/// most 380 bytes each, so a genuine object is far smaller: the bound only keeps a damaged or
/// hostile object from being read whole.
pub const MAX_METADATA_BYTES: usize = 64 * 1024;

/// The length of a segment's checksum.
const CHECKSUM_LEN: usize = 16;

/// A segment's header: what the segment holds, and the hash chain at both its ends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentHeader {
    /// The format number, [`FORMAT`].
    pub format: u32,
    /// The database.
    pub database: String,
    /// The engine whose batch the payload is (ADR-0021).
    pub engine: String,
    /// The epoch of the primary that archived it.
    pub epoch: u64,
    /// The position the batch continues from, as hex, or `None` for the log's beginning.
    pub start: Option<String>,
    /// The position just past the batch, as hex.
    pub end: String,
    /// The hash chain at `start`, as 64 hex digits.
    pub chain_start: String,
    /// The hash chain at `end`, as 64 hex digits.
    pub chain_end: String,
    /// The archiving node's clock when the archive received the batch, in milliseconds since
    /// the Unix epoch.
    pub archived_at_ms: u64,
    /// The payload's length in bytes.
    pub payload_len: u64,
}

/// A snapshot's descriptor, written last: its presence marks the snapshot complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDescriptor {
    /// The format number, [`FORMAT`].
    pub format: u32,
    /// The database.
    pub database: String,
    /// The engine that took it.
    pub engine: String,
    /// The epoch of the primary that took it.
    pub epoch: u64,
    /// The position the snapshot holds the log up to, as hex, or `None` for the log's beginning.
    pub position: Option<String>,
    /// The hash chain at `position`, as 64 hex digits.
    pub chain: String,
    /// The archiving node's clock when it pinned the snapshot, in milliseconds since the Unix
    /// epoch.
    pub taken_at_ms: u64,
    /// How many chunks the packed stream was cut into.
    pub chunks: u32,
    /// The packed stream's length in bytes.
    pub bytes: u64,
    /// The packed stream's XXH3-128 checksum, as 32 hex digits.
    pub checksum: String,
    /// Each chunk's XXH3-128 checksum, as 32 hex digits, so that a restore names the chunk
    /// that changed.
    pub chunk_checksums: Vec<String>,
}

/// A database's metadata as an archive keeps it beside each base snapshot (ADR-0041, point 10).
///
/// Nothing in it is authenticated: an archive isn't signed, so a restore treats the description
/// and its author as the archive's claims and never as a principal's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedMetadata {
    /// The format number, [`METADATA_FORMAT`].
    pub format: u32,
    /// The database.
    pub database: String,
    /// The description, if it had one.
    pub description: Option<String>,
    /// The principal that last set the description, if the control plane could name one.
    pub description_by: Option<String>,
    /// The labels.
    pub labels: BTreeMap<String, String>,
    /// The principal that created the database, if the creation recorded one.
    pub created_by: Option<String>,
    /// When the database was created, in milliseconds since the Unix epoch.
    pub created_at_ms: Option<u64>,
    /// When the database was to expire. A restore never uses it.
    pub expires_at_ms: Option<u64>,
    /// The metadata's version.
    pub version: u64,
}

/// The error returned when an object's bytes aren't a valid object of its kind.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FormatError {
    /// The object doesn't start with the segment magic.
    #[error("not a segment: the magic bytes are wrong")]
    BadMagic,
    /// The object ends before its declared lengths.
    #[error("the object is truncated")]
    Truncated,
    /// The checksum doesn't match the bytes.
    #[error("the checksum doesn't match the object's bytes")]
    Checksum,
    /// The header or descriptor isn't valid JSON of its shape.
    #[error("invalid header: {0}")]
    Header(String),
    /// The object was written by a newer format than this version reads.
    #[error("format {0} is newer than this version reads")]
    Version(u32),
    /// A metadata object holds more than a database's metadata can.
    #[error("the metadata is too large: {0}")]
    TooLarge(String),
}

/// Just the format number, read before anything else.
#[derive(Deserialize)]
struct Versioned {
    format: u32,
}

/// Parses `text` as `T`, after checking that its format number is `supported`.
fn parse_versioned<'a, T: Deserialize<'a>>(
    text: &'a [u8],
    supported: u32,
) -> Result<T, FormatError> {
    let versioned: Versioned =
        serde_json::from_slice(text).map_err(|error| FormatError::Header(error.to_string()))?;
    if versioned.format != supported {
        return Err(FormatError::Version(versioned.format));
    }
    serde_json::from_slice(text).map_err(|error| FormatError::Header(error.to_string()))
}

/// Encodes a segment of `header` and `payload`.
///
/// # Panics
///
/// If the header's JSON is longer than `u32::MAX` bytes, which its fields can't make it.
#[must_use]
pub fn encode_segment(header: &SegmentHeader, payload: &[u8]) -> Vec<u8> {
    let json = serde_json::to_vec(header).expect("a segment header always serializes");
    let json_len = u32::try_from(json.len()).expect("a segment header is small");
    let mut bytes =
        Vec::with_capacity(SEGMENT_MAGIC.len() + 4 + json.len() + payload.len() + CHECKSUM_LEN);
    bytes.extend_from_slice(SEGMENT_MAGIC);
    bytes.extend_from_slice(&json_len.to_be_bytes());
    bytes.extend_from_slice(&json);
    bytes.extend_from_slice(payload);
    let checksum = XxHash3_128::oneshot(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

/// Decodes a segment: its header and its payload.
///
/// # Errors
///
/// [`FormatError`] if the bytes aren't a whole segment of a format this version reads, its
/// checksum doesn't match, or its payload's length isn't the header's.
pub fn decode_segment(bytes: &[u8]) -> Result<(SegmentHeader, &[u8]), FormatError> {
    let magic = bytes
        .get(..SEGMENT_MAGIC.len())
        .ok_or(FormatError::Truncated)?;
    if magic != SEGMENT_MAGIC {
        return Err(FormatError::BadMagic);
    }
    let body_len = bytes
        .len()
        .checked_sub(CHECKSUM_LEN)
        .ok_or(FormatError::Truncated)?;
    let (body, checksum) = bytes.split_at(body_len);
    if XxHash3_128::oneshot(body).to_be_bytes() != checksum {
        return Err(FormatError::Checksum);
    }
    let rest = &body[SEGMENT_MAGIC.len()..];
    let (len, rest) = rest.split_at_checked(4).ok_or(FormatError::Truncated)?;
    let len = u32::from_be_bytes(len.try_into().map_err(|_| FormatError::Truncated)?);
    let (json, payload) = rest
        .split_at_checked(usize::try_from(len).map_err(|_| FormatError::Truncated)?)
        .ok_or(FormatError::Truncated)?;
    let header: SegmentHeader = parse_versioned(json, FORMAT)?;
    if u64::try_from(payload.len()).ok() != Some(header.payload_len) {
        return Err(FormatError::Truncated);
    }
    Ok((header, payload))
}

/// Encodes a snapshot descriptor.
///
/// # Panics
///
/// Never: a descriptor always serializes.
#[must_use]
pub fn encode_descriptor(descriptor: &SnapshotDescriptor) -> Vec<u8> {
    serde_json::to_vec_pretty(descriptor).expect("a snapshot descriptor always serializes")
}

/// The XXH3-128 checksum of `bytes`, as 32 hex digits, as descriptors record them.
#[must_use]
pub fn checksum_hex(bytes: &[u8]) -> String {
    format!("{:032x}", XxHash3_128::oneshot(bytes))
}

/// Decodes a snapshot descriptor.
///
/// # Errors
///
/// [`FormatError`] if the bytes aren't a descriptor of a format this version reads.
pub fn decode_descriptor(bytes: &[u8]) -> Result<SnapshotDescriptor, FormatError> {
    parse_versioned(bytes, FORMAT)
}

/// Encodes a metadata object as pretty JSON, so an operator can read it with `cat`.
///
/// # Panics
///
/// Never: metadata always serializes.
#[must_use]
pub fn encode_metadata(metadata: &ArchivedMetadata) -> Vec<u8> {
    serde_json::to_vec_pretty(metadata).expect("archived metadata always serializes")
}

/// Decodes a metadata object, checking its format number first, as [`decode_descriptor`] does.
///
/// # Errors
///
/// [`FormatError`] if the bytes aren't metadata of a format this version reads, or hold more
/// than a database's metadata can: over [`MAX_METADATA_BYTES`], a description over
/// [`MAX_DESCRIPTION_BYTES`], or more than [`MAX_LABELS`] labels.
pub fn decode_metadata(bytes: &[u8]) -> Result<ArchivedMetadata, FormatError> {
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(FormatError::TooLarge(format!(
            "{} bytes, at most {MAX_METADATA_BYTES}",
            bytes.len()
        )));
    }
    let metadata: ArchivedMetadata = parse_versioned(bytes, METADATA_FORMAT)?;
    if let Some(description) = &metadata.description
        && description.len() > MAX_DESCRIPTION_BYTES
    {
        return Err(FormatError::TooLarge(format!(
            "a description of {} bytes, at most {MAX_DESCRIPTION_BYTES}",
            description.len()
        )));
    }
    if metadata.labels.len() > MAX_LABELS {
        return Err(FormatError::TooLarge(format!(
            "{} labels, at most {MAX_LABELS}",
            metadata.labels.len()
        )));
    }
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(payload: &[u8]) -> SegmentHeader {
        SegmentHeader {
            format: FORMAT,
            database: "tenant/a".to_owned(),
            engine: "sqlite".to_owned(),
            epoch: 3,
            start: None,
            end: "0102".to_owned(),
            chain_start: "00".repeat(32),
            chain_end: "ab".repeat(32),
            archived_at_ms: 1_700_000_000_000,
            payload_len: u64::try_from(payload.len()).expect("small"),
        }
    }

    #[test]
    fn a_segment_round_trips() {
        let payload = b"opaque batch bytes";
        let bytes = encode_segment(&header(payload), payload);
        assert!(bytes.starts_with(SEGMENT_MAGIC));
        let (decoded, body) = decode_segment(&bytes).expect("decodes");
        assert_eq!(decoded, header(payload));
        assert_eq!(body, payload);
        let empty = encode_segment(&header(b""), b"");
        assert_eq!(decode_segment(&empty).expect("decodes").1, b"");
    }

    #[test]
    fn a_flipped_byte_fails_the_checksum() {
        let payload = b"opaque batch bytes";
        let bytes = encode_segment(&header(payload), payload);
        for at in [
            SEGMENT_MAGIC.len() + 1,
            20,
            bytes.len() - 20,
            bytes.len() - 1,
        ] {
            let mut flipped = bytes.clone();
            flipped[at] ^= 0x01;
            assert_eq!(
                decode_segment(&flipped),
                Err(FormatError::Checksum),
                "byte {at}"
            );
        }
        let mut magic = bytes.clone();
        magic[0] = b'X';
        assert_eq!(decode_segment(&magic), Err(FormatError::BadMagic));
        assert_eq!(decode_segment(&bytes[..30]), Err(FormatError::Checksum));
        assert_eq!(decode_segment(&bytes[..10]), Err(FormatError::Truncated));
        assert_eq!(decode_segment(&bytes[..3]), Err(FormatError::Truncated));
    }

    /// A segment whose header is `json`, checksummed as a writer would.
    fn with_header(json: &str) -> Vec<u8> {
        let mut bytes = SEGMENT_MAGIC.to_vec();
        bytes.extend_from_slice(&u32::try_from(json.len()).expect("small").to_be_bytes());
        bytes.extend_from_slice(json.as_bytes());
        let checksum = XxHash3_128::oneshot(&bytes);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes
    }

    #[test]
    fn an_unknown_header_field_is_refused() {
        let mut value = serde_json::to_value(header(b"")).expect("json");
        value["surprise"] = serde_json::json!(true);
        let bytes = with_header(&value.to_string());
        assert!(matches!(
            decode_segment(&bytes),
            Err(FormatError::Header(_))
        ));
        let mut descriptor = serde_json::json!({
            "format": 1, "database": "a", "engine": "sqlite", "epoch": 1, "position": null,
            "chain": "00", "taken_at_ms": 0, "chunks": 1, "bytes": 1, "checksum": "00",
            "chunk_checksums": ["00"],
        });
        assert!(decode_descriptor(descriptor.to_string().as_bytes()).is_ok());
        descriptor["extra"] = serde_json::json!(1);
        assert!(matches!(
            decode_descriptor(descriptor.to_string().as_bytes()),
            Err(FormatError::Header(_))
        ));
    }

    #[test]
    fn a_newer_format_is_refused() {
        let mut value = serde_json::to_value(header(b"")).expect("json");
        value["format"] = serde_json::json!(2);
        value["field_of_format_2"] = serde_json::json!("x");
        let bytes = with_header(&value.to_string());
        assert_eq!(decode_segment(&bytes), Err(FormatError::Version(2)));
        assert_eq!(
            decode_descriptor(br#"{"format": 7, "anything": 1}"#),
            Err(FormatError::Version(7))
        );
    }

    fn metadata() -> ArchivedMetadata {
        ArchivedMetadata {
            format: METADATA_FORMAT,
            database: "tenant/a".to_owned(),
            description: Some("Notes\nfor agent 7.".to_owned()),
            description_by: Some("agent-7".to_owned()),
            labels: BTreeMap::from([
                ("purpose".to_owned(), "memory".to_owned()),
                ("agent".to_owned(), "7".to_owned()),
            ]),
            created_by: Some("ops".to_owned()),
            created_at_ms: Some(1_700_000_000_000),
            expires_at_ms: Some(1_800_000_000_000),
            version: 4,
        }
    }

    #[test]
    fn metadata_round_trips_and_refuses_unknown_fields_and_newer_formats() {
        let bytes = encode_metadata(&metadata());
        assert_eq!(decode_metadata(&bytes), Ok(metadata()));
        let text = String::from_utf8(bytes).expect("UTF-8");
        assert!(text.contains("\n  "), "pretty JSON: {text}");
        // Labels are written in key order, so the same metadata always has the same bytes.
        assert!(text.find("agent").expect("agent") < text.find("purpose").expect("purpose"));

        let bare = ArchivedMetadata {
            description: None,
            description_by: None,
            labels: BTreeMap::new(),
            created_by: None,
            created_at_ms: None,
            expires_at_ms: None,
            ..metadata()
        };
        assert_eq!(decode_metadata(&encode_metadata(&bare)), Ok(bare));

        let mut value = serde_json::to_value(metadata()).expect("json");
        value["surprise"] = serde_json::json!(true);
        assert!(matches!(
            decode_metadata(value.to_string().as_bytes()),
            Err(FormatError::Header(_))
        ));
        let mut value = serde_json::to_value(metadata()).expect("json");
        value["format"] = serde_json::json!(2);
        value["field_of_format_2"] = serde_json::json!("x");
        assert_eq!(
            decode_metadata(value.to_string().as_bytes()),
            Err(FormatError::Version(2))
        );
        assert!(matches!(
            decode_metadata(b"not json"),
            Err(FormatError::Header(_))
        ));
    }

    #[test]
    fn metadata_over_its_limits_is_refused() {
        let long = ArchivedMetadata {
            description: Some("x".repeat(MAX_DESCRIPTION_BYTES + 1)),
            ..metadata()
        };
        assert!(matches!(
            decode_metadata(&encode_metadata(&long)),
            Err(FormatError::TooLarge(_))
        ));
        let many = ArchivedMetadata {
            labels: (0..=MAX_LABELS)
                .map(|n| (format!("k{n}"), "v".to_owned()))
                .collect(),
            ..metadata()
        };
        assert!(matches!(
            decode_metadata(&encode_metadata(&many)),
            Err(FormatError::TooLarge(_))
        ));
        assert!(matches!(
            decode_metadata(&vec![b' '; MAX_METADATA_BYTES + 1]),
            Err(FormatError::TooLarge(_))
        ));
        let at_limit = ArchivedMetadata {
            description: Some("x".repeat(MAX_DESCRIPTION_BYTES)),
            ..metadata()
        };
        assert_eq!(decode_metadata(&encode_metadata(&at_limit)), Ok(at_limit));
    }
}
