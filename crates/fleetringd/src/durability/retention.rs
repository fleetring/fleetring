//! Retention (ADR-0029, point 9): whole restore windows are kept, and only what no kept
//! snapshot's path can use is deleted.

use std::num::NonZeroU32;

use super::listing::Listing;
use super::store::{ObjectStore, StoreError, retrying};

/// The keys retention deletes from `listing`, in the order to delete them.
///
/// Retention keeps the newest snapshots until `keep` of them are *live*: a walk over the
/// segments, of any epoch, leads from them to the furthest point the archive holds. A snapshot
/// that leads nowhere, as one an old primary took past the mark a new primary resumed from,
/// is kept too but doesn't count, so the older snapshot the new primary's segments continue is
/// kept with them. Every segment a walk from a kept snapshot reaches is kept.
///
/// It deletes the snapshots past the kept ones, descriptor first, so that a half-deleted
/// snapshot reads as incomplete, then its chunks, then its metadata object, so that a crash
/// leaves at worst an orphan and never a complete snapshot that lost its metadata; every
/// segment no kept snapshot reaches; and incomplete snapshots and orphaned metadata objects
/// older than the oldest kept snapshot, which no restore can use. While fewer than `keep`
/// snapshots are live, it deletes nothing.
///
/// Nothing a kept snapshot's walk can use is deleted, so an old and a new primary pruning one
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
    let (latest, _) = listing.latest();
    let mut live = 0;
    let mut kept = 0;
    for snapshot in &snapshots {
        if live == keep {
            break;
        }
        kept += 1;
        if listing
            .reachable_from([snapshot.position.clone()])
            .contains(&latest)
        {
            live += 1;
        }
    }
    if kept == snapshots.len() {
        return Vec::new();
    }
    let (kept, dropped) = snapshots.split_at(kept);
    let Some(oldest) = kept.iter().map(|snapshot| snapshot.position.clone()).min() else {
        return Vec::new();
    };
    let reached = listing.reachable_from(kept.iter().map(|snapshot| snapshot.position.clone()));
    let mut keys = Vec::new();
    for snapshot in dropped {
        keys.push(snapshot.key.clone());
        keys.extend(snapshot.chunks.iter().cloned());
        keys.extend(
            snapshot
                .metadata
                .iter()
                .map(|metadata| metadata.key.clone()),
        );
    }
    keys.extend(
        listing
            .segments
            .iter()
            .filter(|segment| !reached.contains(&segment.name.start))
            .map(|segment| segment.key.clone()),
    );
    keys.extend(
        listing
            .incomplete
            .iter()
            .filter(|snapshot| snapshot.position < oldest)
            .flat_map(|snapshot| snapshot.chunks.iter().cloned()),
    );
    keys.extend(
        listing
            .orphaned_metadata
            .iter()
            .filter(|orphan| orphan.position < oldest)
            .map(|orphan| orphan.key.clone()),
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
    use crate::durability::listing::{
        IncompleteSnapshot, MetadataEntry, OrphanedMetadata, SegmentEntry, SnapshotEntry,
    };
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
            metadata: None,
        }
    }

    /// `snapshot` with a metadata object, as the archive writes one.
    fn with_metadata(mut snapshot: SnapshotEntry) -> SnapshotEntry {
        snapshot.metadata = Some(MetadataEntry {
            key: format!("{}.json", snapshot.id),
            checksum: "0".repeat(32),
            size: 1,
        });
        snapshot
    }

    fn orphan(position: u8) -> OrphanedMetadata {
        OrphanedMetadata {
            key: format!("o{position}.json"),
            id: format!("o{position}"),
            position: Some(at(position)),
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

    #[test]
    fn a_deleted_snapshots_metadata_goes_last() {
        let listing = Listing {
            snapshots: vec![
                with_metadata(snapshot(1)),
                with_metadata(snapshot(5)),
                with_metadata(snapshot(9)),
            ],
            segments: vec![segment(1, 5), segment(5, 9)],
            ..Listing::default()
        };
        let keep = NonZeroU32::new(2).expect("nonzero");
        assert_eq!(
            retention(&listing, keep),
            ["s1/snapshot.json", "s1/00000000.chunk", "s1.json", "seg1-5"]
        );
        // What a kept snapshot has is never deleted.
        assert!(retention(&listing, NonZeroU32::new(3).expect("nonzero")).is_empty());
    }

    #[test]
    fn old_orphaned_metadata_is_deleted_and_recent_orphans_are_kept() {
        let listing = Listing {
            snapshots: vec![snapshot(1), snapshot(5), snapshot(9)],
            segments: vec![segment(1, 5), segment(5, 9)],
            // 3 is older than the oldest kept snapshot (5); 7 and 9 are an upload in progress
            // or a failover's, and 5 is the oldest kept one's own.
            orphaned_metadata: vec![orphan(3), orphan(5), orphan(7), orphan(9)],
            ..Listing::default()
        };
        let keep = NonZeroU32::new(2).expect("nonzero");
        assert_eq!(
            retention(&listing, keep),
            ["s1/snapshot.json", "s1/00000000.chunk", "seg1-5", "o3.json"]
        );
    }

    /// A segment of epoch 2, a new primary's.
    fn resumed(start: u8, end: u8) -> SegmentEntry {
        let mut segment = segment(start, end);
        segment.key = format!("e2-{start}-{end}");
        segment.name.epoch = LeaseEpoch::new(2);
        segment
    }

    /// Whether a restore of what `listing` keeps after retention reaches `target`.
    fn restorable(listing: &Listing, keep: NonZeroU32, target: u8) -> bool {
        let doomed = retention(listing, keep);
        let kept = Listing {
            snapshots: listing
                .snapshots
                .iter()
                .filter(|snapshot| !doomed.contains(&snapshot.key))
                .cloned()
                .collect(),
            segments: listing
                .segments
                .iter()
                .filter(|segment| !doomed.contains(&segment.key))
                .cloned()
                .collect(),
            ..Listing::default()
        };
        crate::durability::plan(
            &kept,
            &crate::durability::RestoreTarget::Position(at(target)),
        )
        .is_ok()
    }

    #[test]
    fn a_snapshot_past_a_new_primarys_mark_doesnt_orphan_its_segments() {
        // The old primary archived up to 10 and snapshotted there, then failed before the mark
        // past 6 replicated; the new primary resumed at 6 and archived past 10 without passing
        // through it, then took a snapshot at 12.
        let listing = Listing {
            snapshots: vec![snapshot(1), snapshot(10), snapshot(12)],
            segments: vec![segment(1, 6), segment(6, 10), resumed(6, 8), resumed(8, 12)],
            ..Listing::default()
        };
        let keep = NonZeroU32::new(2).expect("nonzero");
        // The snapshot at 10 leads nowhere, so the one at 1 is kept, with every segment.
        assert_eq!(retention(&listing, keep), Vec::<String>::new());
        assert!(restorable(&listing, keep, 11), "a point after the dead end");
        assert!(restorable(&listing, keep, 7));

        // Once two later snapshots lead to the end, the older window goes.
        let mut later = listing.clone();
        later.snapshots.push(snapshot(14));
        later.segments.push(resumed(12, 14));
        assert_eq!(
            retention(&later, keep),
            [
                "s10/snapshot.json",
                "s10/00000000.chunk",
                "s1/snapshot.json",
                "s1/00000000.chunk",
                "seg1-6",
                "seg6-10",
                "e2-6-8",
                "e2-8-12"
            ]
        );
        assert!(restorable(&later, keep, 13));
    }
}
