//! Retention (ADR-0029, point 9): whole restore windows are kept, and only what no kept
//! snapshot's path can use is deleted.

use std::num::NonZeroU32;

use super::listing::Listing;
use super::store::{ObjectStore, StoreError, retrying};

/// The keys retention deletes from `listing`, in the order to delete them: snapshots past the
/// newest `keep` complete ones, descriptor first, so that a half-deleted snapshot reads as
/// incomplete; every segment that ends at or before the oldest kept snapshot's position; and
/// incomplete snapshots older than the oldest kept one, which no restore can use.
///
/// Nothing a kept snapshot's path can use is deleted, so an old and a new primary pruning one
/// archive at once never delete what the other needs.
#[must_use]
pub fn retention(listing: &Listing, keep: NonZeroU32) -> Vec<String> {
    let mut snapshots: Vec<_> = listing.snapshots.iter().collect();
    snapshots.sort_by(|a, b| {
        (&b.position, b.epoch, b.descriptor.taken_at_ms).cmp(&(
            &a.position,
            a.epoch,
            a.descriptor.taken_at_ms,
        ))
    });
    let keep = usize::try_from(keep.get()).unwrap_or(usize::MAX);
    if snapshots.len() <= keep {
        return Vec::new();
    }
    let (kept, dropped) = snapshots.split_at(keep);
    let Some(oldest) = kept.last().map(|snapshot| snapshot.position.clone()) else {
        return Vec::new();
    };
    let mut keys = Vec::new();
    for snapshot in dropped {
        keys.push(snapshot.key.clone());
        keys.extend(snapshot.chunks.iter().cloned());
    }
    keys.extend(
        listing
            .segments
            .iter()
            .filter(|segment| Some(&segment.name.end) <= oldest.as_ref())
            .map(|segment| segment.key.clone()),
    );
    keys.extend(
        listing
            .incomplete
            .iter()
            .filter(|snapshot| snapshot.position < oldest)
            .flat_map(|snapshot| snapshot.chunks.iter().cloned()),
    );
    keys
}

/// Deletes `keys` from `store`, in order, stopping at the first failure.
///
/// # Errors
///
/// [`StoreError`] if a delete fails; what follows it is left for the next run.
pub async fn apply_retention(store: &dyn ObjectStore, keys: &[String]) -> Result<(), StoreError> {
    for key in keys {
        retrying(|| store.delete(key)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use fleetring_control::LeaseEpoch;

    use super::*;
    use crate::durability::format::{FORMAT, SnapshotDescriptor};
    use crate::durability::keys::SegmentName;
    use crate::durability::listing::{IncompleteSnapshot, SegmentEntry, SnapshotEntry};
    use crate::engine::Position;
    use crate::replication::ChainHash;

    fn at(n: u8) -> Position {
        Position::new([0, n])
    }

    fn snapshot(position: u8) -> SnapshotEntry {
        SnapshotEntry {
            id: format!("s{position}"),
            key: format!("s{position}/snapshot.json"),
            position: Some(at(position)),
            epoch: LeaseEpoch::new(1),
            chain: ChainHash::GENESIS,
            descriptor: SnapshotDescriptor {
                format: FORMAT,
                database: "db".to_owned(),
                engine: "sqlite".to_owned(),
                epoch: 1,
                position: None,
                chain: String::new(),
                taken_at_ms: u64::from(position),
                chunks: 1,
                bytes: 1,
                checksum: String::new(),
                chunk_checksums: vec![String::new()],
            },
            chunks: vec![format!("s{position}/00000000.chunk")],
        }
    }

    fn segment(start: u8, end: u8) -> SegmentEntry {
        SegmentEntry {
            key: format!("seg{start}-{end}"),
            name: SegmentName {
                start: Some(at(start)),
                end: at(end),
                epoch: LeaseEpoch::new(1),
                archived_at_ms: 0,
            },
            size: 1,
        }
    }

    fn incomplete(position: u8) -> IncompleteSnapshot {
        IncompleteSnapshot {
            id: format!("i{position}"),
            position: Some(at(position)),
            chunks: vec![format!("i{position}/00000000.chunk")],
        }
    }

    #[test]
    fn whole_windows_are_kept_and_older_ones_deleted_descriptor_first() {
        let listing = Listing {
            snapshots: vec![snapshot(1), snapshot(5), snapshot(9)],
            incomplete: vec![incomplete(3), incomplete(7)],
            segments: vec![segment(1, 3), segment(3, 5), segment(5, 7), segment(7, 9)],
            ..Listing::default()
        };
        let keep = NonZeroU32::new(2).expect("nonzero");
        assert_eq!(
            retention(&listing, keep),
            [
                "s1/snapshot.json",
                "s1/00000000.chunk",
                "seg1-3",
                "seg3-5",
                "i3/00000000.chunk"
            ]
        );
        let keep_all = NonZeroU32::new(3).expect("nonzero");
        assert!(retention(&listing, keep_all).is_empty());
    }
}
