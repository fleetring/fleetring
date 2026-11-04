//! An archive's object names (ADR-0029, point 4): the names are the manifest.
//!
//! ```text
//! <prefix>/<db>/snapshots/<id>/snapshot.json              descriptor, written last
//! <prefix>/<db>/snapshots/<id>/<nnnnnnnn>.chunk            the packed snapshot stream
//! <prefix>/<db>/segments/<start>_<end>_<epoch>_<at>.seg    one delivered batch
//! ```
//!
//! `<db>` is the database's directory name, escaped so that every key is safe on every store;
//! positions are lowercase hex, or `begin` for the log's beginning; epochs and `<at>`, the
//! segment's `archived_at_ms`, are 16 hex digits; and a snapshot's `<id>` is
//! `<position>_<epoch>`.

use std::path::Component;

use fleetring_control::{DatabaseName, LeaseEpoch};

use crate::changes::{hex, unhex};
use crate::engine::Position;
use crate::layout;

/// What stands for the log's beginning where a position goes in a name.
const BEGIN: &str = "begin";

/// The descriptor's name inside a snapshot's directory.
const DESCRIPTOR: &str = "snapshot.json";

/// The suffix of a snapshot chunk's name.
const CHUNK_SUFFIX: &str = ".chunk";

/// The suffix of a segment's name.
const SEGMENT_SUFFIX: &str = ".seg";

/// The keys of one database's objects in one archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveKeys {
    root: String,
}

impl ArchiveKeys {
    /// The keys of `database`'s objects under `prefix`, a key without a trailing `/`, or empty
    /// for the store's top level.
    #[must_use]
    pub fn new(prefix: &str, database: &DatabaseName) -> Self {
        let db: Vec<String> = layout::database_dir(database)
            .components()
            .filter_map(|component| match component {
                Component::Normal(part) => part.to_str().map(str::to_owned),
                _ => None,
            })
            .collect();
        let db = db.join("/");
        let root = if prefix.is_empty() {
            db
        } else {
            format!("{prefix}/{db}")
        };
        Self { root }
    }

    /// The prefix every one of the database's objects is under, ending in `/`.
    #[must_use]
    pub fn database(&self) -> String {
        format!("{}/", self.root)
    }

    /// The prefix every snapshot's objects are under.
    #[must_use]
    pub fn snapshots(&self) -> String {
        format!("{}/snapshots/", self.root)
    }

    /// The prefix every segment is under.
    #[must_use]
    pub fn segments(&self) -> String {
        format!("{}/segments/", self.root)
    }

    /// A snapshot's ID: its position and the epoch of the primary that took it.
    #[must_use]
    pub fn snapshot_id(position: Option<&Position>, epoch: LeaseEpoch) -> String {
        format!("{}_{:016x}", position_name(position), epoch.get())
    }

    /// The position and epoch a snapshot's ID names, if it is one.
    #[must_use]
    pub fn parse_snapshot_id(id: &str) -> Option<(Option<Position>, LeaseEpoch)> {
        let (position, epoch) = id.split_once('_')?;
        Some((parse_position(position)?, parse_epoch(epoch)?))
    }

    /// The key of snapshot `id`'s descriptor.
    #[must_use]
    pub fn descriptor(&self, id: &str) -> String {
        format!("{}{id}/{DESCRIPTOR}", self.snapshots())
    }

    /// The key of snapshot `id`'s chunk number `index`, from 0.
    #[must_use]
    pub fn chunk(&self, id: &str, index: u32) -> String {
        format!("{}{id}/{index:08}{CHUNK_SUFFIX}", self.snapshots())
    }

    /// The key of the segment `name` names.
    #[must_use]
    pub fn segment(&self, name: &SegmentName) -> String {
        format!("{}{}", self.segments(), name.file_name())
    }

    /// Which snapshot object `key` is, if it is one of this database's.
    #[must_use]
    pub fn parse_snapshot_key(&self, key: &str) -> Option<(String, SnapshotObject)> {
        let rest = key.strip_prefix(&self.snapshots())?;
        let (id, object) = rest.split_once('/')?;
        Self::parse_snapshot_id(id)?;
        let object = if object == DESCRIPTOR {
            SnapshotObject::Descriptor
        } else {
            let index = object.strip_suffix(CHUNK_SUFFIX)?;
            if index.len() != 8 || !index.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            SnapshotObject::Chunk(index.parse().ok()?)
        };
        Some((id.to_owned(), object))
    }

    /// The segment `key` names, if it is one of this database's.
    #[must_use]
    pub fn parse_segment_key(&self, key: &str) -> Option<SegmentName> {
        SegmentName::parse(key.strip_prefix(&self.segments())?)
    }
}

/// One of the objects of a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SnapshotObject {
    /// The descriptor, written last.
    Descriptor,
    /// A chunk of the packed stream, by its index from 0.
    Chunk(u32),
}

/// What a segment's name says it holds: everything the archive needs to plan a restore.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentName {
    /// The position the segment continues from, or `None` for the log's beginning.
    pub start: Option<Position>,
    /// The position just past it.
    pub end: Position,
    /// The epoch of the primary that archived it.
    pub epoch: LeaseEpoch,
    /// The archiving node's clock when the archive received it, in milliseconds since the Unix
    /// epoch (ADR-0029, point 11).
    pub archived_at_ms: u64,
}

impl SegmentName {
    /// The segment's file name: `<start>_<end>_<epoch>_<at>.seg`.
    #[must_use]
    pub fn file_name(&self) -> String {
        format!(
            "{}_{}_{:016x}_{:016x}{SEGMENT_SUFFIX}",
            position_name(self.start.as_ref()),
            position_name(Some(&self.end)),
            self.epoch.get(),
            self.archived_at_ms
        )
    }

    /// The segment a file name names, if it is a segment's.
    #[must_use]
    pub fn parse(file_name: &str) -> Option<Self> {
        let stem = file_name.strip_suffix(SEGMENT_SUFFIX)?;
        let mut parts = stem.split('_');
        let (Some(start), Some(end), Some(epoch), Some(at), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return None;
        };
        Some(Self {
            start: parse_position(start)?,
            end: parse_position(end)??,
            epoch: parse_epoch(epoch)?,
            archived_at_ms: parse_hex_u64(at)?,
        })
    }
}

fn position_name(position: Option<&Position>) -> String {
    position.map_or_else(|| BEGIN.to_owned(), |position| hex(position.as_bytes()))
}

/// The position a name spells: `Some(None)` for the log's beginning, `None` if it isn't one.
fn parse_position(text: &str) -> Option<Option<Position>> {
    if text == BEGIN {
        return Some(None);
    }
    if text.is_empty() || text.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return None;
    }
    unhex(text).ok().map(|bytes| Some(Position::from(bytes)))
}

fn parse_epoch(text: &str) -> Option<LeaseEpoch> {
    parse_hex_u64(text).map(LeaseEpoch::new)
}

fn parse_hex_u64(text: &str) -> Option<u64> {
    let lower = text
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    (text.len() == 16 && lower)
        .then(|| u64::from_str_radix(text, 16).ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(text: &str) -> DatabaseName {
        text.parse().expect("a database name")
    }

    fn segment(start: Option<u8>, end: u8, epoch: u64, at: u64) -> SegmentName {
        SegmentName {
            start: start.map(|byte| Position::new([0, byte])),
            end: Position::new([0, end]),
            epoch: LeaseEpoch::new(epoch),
            archived_at_ms: at,
        }
    }

    #[test]
    fn names_round_trip_and_sort_by_start_within_a_database() {
        let keys = ArchiveKeys::new("fleetring", &name("tenant/A"));
        assert_eq!(keys.database(), "fleetring/tenant%2f%41/");
        let names = [
            segment(Some(1), 2, 1, 10),
            segment(Some(2), 9, 1, 20),
            segment(Some(2), 5, 2, 15),
            segment(Some(10), 11, 2, 30),
        ];
        let mut keyed: Vec<String> = names.iter().map(|name| keys.segment(name)).collect();
        for (key, name) in keyed.iter().zip(&names) {
            assert!(key.starts_with(&keys.segments()), "{key}");
            assert_eq!(keys.parse_segment_key(key).as_ref(), Some(name), "{key}");
        }
        keyed.sort();
        let starts: Vec<Option<Position>> = keyed
            .iter()
            .filter_map(|key| keys.parse_segment_key(key))
            .map(|name| name.start)
            .collect();
        assert!(starts.is_sorted(), "{starts:?}");

        let id = ArchiveKeys::snapshot_id(Some(&Position::new([0, 2])), LeaseEpoch::new(3));
        assert_eq!(id, "0002_0000000000000003");
        assert_eq!(
            ArchiveKeys::parse_snapshot_id(&id),
            Some((Some(Position::new([0, 2])), LeaseEpoch::new(3)))
        );
        assert_eq!(
            keys.parse_snapshot_key(&keys.descriptor(&id)),
            Some((id.clone(), SnapshotObject::Descriptor))
        );
        assert_eq!(
            keys.parse_snapshot_key(&keys.chunk(&id, 12)),
            Some((id.clone(), SnapshotObject::Chunk(12)))
        );
        assert_eq!(
            keys.chunk(&id, 12),
            format!("{}{id}/00000012.chunk", keys.snapshots())
        );
        assert_eq!(
            keys.parse_snapshot_key(&format!("{}{id}/x", keys.snapshots())),
            None
        );
        assert_eq!(keys.parse_segment_key("elsewhere/a_b_c_d.seg"), None);
    }

    #[test]
    fn begin_stands_for_the_log_beginning() {
        let first = segment(None, 4, 1, 0);
        assert!(
            first.file_name().starts_with("begin_0004_"),
            "{}",
            first.file_name()
        );
        assert_eq!(SegmentName::parse(&first.file_name()), Some(first));
        assert_eq!(
            ArchiveKeys::parse_snapshot_id(&ArchiveKeys::snapshot_id(None, LeaseEpoch::new(1))),
            Some((None, LeaseEpoch::new(1)))
        );
        // An end is never the beginning, and malformed names are no segment.
        for bad in [
            "0001_begin_0000000000000001_0000000000000000.seg",
            "0001_0002_1_0000000000000000.seg",
            "0001_0002_0000000000000001_0000000000000000.chunk",
            "0001_0002_0000000000000001_0000000000000000_x.seg",
            "0A01_0002_0000000000000001_0000000000000000.seg",
        ] {
            assert_eq!(SegmentName::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn an_empty_prefix_puts_databases_at_the_top() {
        let keys = ArchiveKeys::new("", &name("ledger"));
        assert_eq!(keys.segments(), "ledger/segments/");
    }
}
