//! A database's manifest in an archive: one `list` of its prefix, with each snapshot's
//! descriptor read.

use std::collections::{BTreeMap, BTreeSet};

use fleetring_control::{DatabaseName, LeaseEpoch};

use super::format::{SnapshotDescriptor, decode_descriptor};
use super::keys::{ArchiveKeys, SegmentName, SnapshotObject};
use super::store::{ObjectStore, StoreError, retrying};
use crate::changes::unhex;
use crate::engine::Position;
use crate::replication::ChainHash;

/// What an archive holds for one database.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Listing {
    /// The complete snapshots: those whose descriptor was written and reads back whole.
    pub snapshots: Vec<SnapshotEntry>,
    /// The snapshots whose descriptor isn't written, as after a crash during an upload, or
    /// while one is being taken.
    pub incomplete: Vec<IncompleteSnapshot>,
    /// Descriptors that don't read back as what their names say, and objects whose names are
    /// no snapshot's or segment's. A restore never uses them.
    pub damaged: Vec<DamagedObject>,
    /// Every segment, in key order.
    pub segments: Vec<SegmentEntry>,
}

/// A complete snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// Its ID, `<position>_<epoch>`.
    pub id: String,
    /// Its descriptor's key.
    pub key: String,
    /// The position it holds the log up to, or `None` for the log's beginning.
    pub position: Option<Position>,
    /// The epoch of the primary that took it.
    pub epoch: LeaseEpoch,
    /// The hash chain at its position.
    pub chain: ChainHash,
    /// Its descriptor.
    pub descriptor: SnapshotDescriptor,
    /// The keys of the chunks the listing found, in order.
    pub chunks: Vec<String>,
}

/// A snapshot whose descriptor isn't written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompleteSnapshot {
    /// Its ID.
    pub id: String,
    /// The position it was taken at.
    pub position: Option<Position>,
    /// The keys of the chunks written so far.
    pub chunks: Vec<String>,
}

/// An object a restore won't use, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DamagedObject {
    /// The object's key.
    pub key: String,
    /// Why it can't be used.
    pub reason: String,
}

/// A segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentEntry {
    /// Its key.
    pub key: String,
    /// What its name says it holds.
    pub name: SegmentName,
    /// Its length in bytes.
    pub size: u64,
}

impl Listing {
    /// Lists `database`'s objects under `keys`, and reads each snapshot's descriptor, retrying
    /// each call that fails for a reason that may pass.
    ///
    /// # Errors
    ///
    /// [`StoreError`] if the store can't be listed or a descriptor can't be read.
    pub async fn read(
        store: &dyn ObjectStore,
        keys: &ArchiveKeys,
        database: &DatabaseName,
    ) -> Result<Self, StoreError> {
        let mut listing = Self::default();
        let prefix = keys.database();
        let objects = retrying(|| store.list(&prefix)).await?;
        let mut snapshots: BTreeMap<String, (bool, Vec<String>)> = BTreeMap::new();
        for object in objects {
            if let Some(name) = keys.parse_segment_key(&object.key) {
                listing.segments.push(SegmentEntry {
                    key: object.key,
                    name,
                    size: object.size,
                });
            } else if let Some((id, part)) = keys.parse_snapshot_key(&object.key) {
                let entry = snapshots.entry(id).or_default();
                match part {
                    SnapshotObject::Descriptor => entry.0 = true,
                    SnapshotObject::Chunk(_) => entry.1.push(object.key),
                }
            } else {
                listing.damaged.push(DamagedObject {
                    key: object.key,
                    reason: "its name is no snapshot's or segment's".to_owned(),
                });
            }
        }
        for (id, (described, mut chunks)) in snapshots {
            chunks.sort();
            let Some((position, epoch)) = ArchiveKeys::parse_snapshot_id(&id) else {
                continue;
            };
            if !described {
                listing.incomplete.push(IncompleteSnapshot {
                    id,
                    position,
                    chunks,
                });
                continue;
            }
            let key = keys.descriptor(&id);
            let Some(bytes) = retrying(|| store.get(&key)).await? else {
                // Deleted since the listing: retention removes descriptors first.
                listing.incomplete.push(IncompleteSnapshot {
                    id,
                    position,
                    chunks,
                });
                continue;
            };
            match checked(&bytes, database, position.as_ref(), epoch) {
                Ok((descriptor, chain)) => listing.snapshots.push(SnapshotEntry {
                    id,
                    key,
                    position,
                    epoch,
                    chain,
                    descriptor,
                    chunks,
                }),
                Err(reason) => listing.damaged.push(DamagedObject { key, reason }),
            }
        }
        Ok(listing)
    }

    /// The furthest position any snapshot or segment reaches, and the latest time a segment
    /// was archived.
    #[must_use]
    pub fn latest(&self) -> (Option<Position>, Option<u64>) {
        let furthest = self
            .segments
            .iter()
            .map(|segment| Some(segment.name.end.clone()))
            .chain(
                self.snapshots
                    .iter()
                    .map(|snapshot| snapshot.position.clone()),
            )
            .max()
            .flatten();
        let at = self
            .segments
            .iter()
            .map(|segment| segment.name.archived_at_ms)
            .max();
        (furthest, at)
    }

    /// Every position a walk over the segments, of any epoch, reaches from any of `from`, the
    /// starting positions included: where the archive's history continues from them.
    #[must_use]
    pub fn reachable_from(
        &self,
        from: impl IntoIterator<Item = Option<Position>>,
    ) -> BTreeSet<Option<Position>> {
        let mut next: BTreeMap<&Option<Position>, Vec<&Position>> = BTreeMap::new();
        for segment in &self.segments {
            if Some(&segment.name.end) > segment.name.start.as_ref() {
                next.entry(&segment.name.start)
                    .or_default()
                    .push(&segment.name.end);
            }
        }
        let mut seen = BTreeSet::new();
        let mut frontier: Vec<Option<Position>> = Vec::new();
        for start in from {
            if seen.insert(start.clone()) {
                frontier.push(start);
            }
        }
        while let Some(at) = frontier.pop() {
            for end in next.get(&at).into_iter().flatten() {
                let end = Some((*end).clone());
                if seen.insert(end.clone()) {
                    frontier.push(end);
                }
            }
        }
        seen
    }
}

/// The descriptor in `bytes` and its chain, if it reads as the descriptor of `database`'s
/// snapshot at `position` and `epoch`.
fn checked(
    bytes: &[u8],
    database: &DatabaseName,
    position: Option<&Position>,
    epoch: LeaseEpoch,
) -> Result<(SnapshotDescriptor, ChainHash), String> {
    let descriptor = decode_descriptor(bytes).map_err(|error| error.to_string())?;
    if descriptor.database != database.as_str() {
        return Err(format!(
            "it describes database `{}`, not `{database}`",
            descriptor.database
        ));
    }
    let described = descriptor
        .position
        .as_deref()
        .map(|text| unhex(text).map(Position::from))
        .transpose()
        .map_err(|_| "its position isn't hex".to_owned())?;
    if described.as_ref() != position || descriptor.epoch != epoch.get() {
        return Err("its position or epoch differs from its name's".to_owned());
    }
    let chain = descriptor
        .chain
        .parse::<ChainHash>()
        .map_err(|error| format!("its chain: {error}"))?;
    if descriptor.chunk_checksums.len() != usize::try_from(descriptor.chunks).unwrap_or(0) {
        return Err("it has a checksum for each of a different number of chunks".to_owned());
    }
    Ok((descriptor, chain))
}
