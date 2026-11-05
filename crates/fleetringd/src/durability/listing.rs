//! A database's manifest in an archive: one `list` of its prefix, and one of its metadata
//! objects' prefix, with each snapshot's descriptor read.

use std::collections::{BTreeMap, BTreeSet};

use fleetring_control::{DatabaseName, LeaseEpoch};

use super::format::{SnapshotDescriptor, decode_descriptor};
use super::keys::{ArchiveKeys, SegmentName, SnapshotObject};
use super::store::{ObjectStore, StoreError, retrying};
use crate::changes::unhex;
use crate::engine::{EngineKind, Position};
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
    /// Metadata objects that belong to no complete snapshot: the snapshot is being uploaded, was
    /// deleted, or has a damaged descriptor; or another object of the same snapshot already
    /// attached.
    pub orphaned_metadata: Vec<OrphanedMetadata>,
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
    /// Its metadata object, if the archive holds one.
    pub metadata: Option<MetadataEntry>,
}

/// A snapshot's metadata object (ADR-0041, point 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEntry {
    /// Its key.
    pub key: String,
    /// The XXH3-128 checksum its name records, as 32 lowercase hex digits.
    pub checksum: String,
    /// Its length in bytes.
    pub size: u64,
}

/// A metadata object of no complete snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanedMetadata {
    /// Its key.
    pub key: String,
    /// The ID of the snapshot it was written for.
    pub id: String,
    /// The position that snapshot holds the log up to, or `None` for the log's beginning.
    pub position: Option<Position>,
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
    /// Lists `database`'s objects under `keys`, and its metadata objects beside them, and reads
    /// each snapshot's descriptor, retrying each call that fails for a reason that may pass. A
    /// descriptor of another database, or of a snapshot another engine than `engine` took, is
    /// damaged. A metadata object is attached by its name alone; it is read at restore.
    ///
    /// # Errors
    ///
    /// [`StoreError`] if the store can't be listed or a descriptor can't be read.
    pub async fn read(
        store: &dyn ObjectStore,
        keys: &ArchiveKeys,
        database: &DatabaseName,
        engine: EngineKind,
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
        let mut metadata: BTreeMap<String, Vec<MetadataEntry>> = BTreeMap::new();
        let beside = keys.metadata_objects();
        for object in retrying(|| store.list(&beside)).await? {
            if let Some((id, checksum)) = keys.parse_metadata_key(&object.key) {
                metadata.entry(id).or_default().push(MetadataEntry {
                    key: object.key,
                    checksum,
                    size: object.size,
                });
            } else {
                listing.damaged.push(DamagedObject {
                    key: object.key,
                    reason: "its name is no snapshot's metadata object".to_owned(),
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
            match checked(&bytes, database, engine, position.as_ref(), epoch) {
                Ok((descriptor, chain)) => {
                    // Two objects for one snapshot, as when a retried snapshot saw an edit in
                    // between: the last by key attaches, and the rest are left to retention.
                    let mut objects = metadata.remove(&id).unwrap_or_default();
                    let attached = objects.pop();
                    listing
                        .orphaned_metadata
                        .extend(objects.into_iter().map(|object| OrphanedMetadata {
                            key: object.key,
                            id: id.clone(),
                            position: position.clone(),
                        }));
                    listing.snapshots.push(SnapshotEntry {
                        id,
                        key,
                        position,
                        epoch,
                        chain,
                        descriptor,
                        chunks,
                        metadata: attached,
                    });
                }
                Err(reason) => listing.damaged.push(DamagedObject { key, reason }),
            }
        }
        for (id, objects) in metadata {
            // `parse_metadata_key` only returns IDs that parse.
            let Some((position, _)) = ArchiveKeys::parse_snapshot_id(&id) else {
                continue;
            };
            listing
                .orphaned_metadata
                .extend(objects.into_iter().map(|object| OrphanedMetadata {
                    key: object.key,
                    id: id.clone(),
                    position: position.clone(),
                }));
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
/// snapshot at `position` and `epoch`, taken by `engine`.
fn checked(
    bytes: &[u8],
    database: &DatabaseName,
    engine: EngineKind,
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
    if descriptor.engine != engine.to_string() {
        return Err(format!(
            "a {} engine took it, not {engine}",
            descriptor.engine
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::format::{FORMAT, checksum_hex, encode_descriptor};
    use crate::durability::store::MemoryStore;
    use crate::replication::ChainHash;

    fn name() -> DatabaseName {
        "ledger".parse().expect("a database name")
    }

    /// A complete snapshot at position `[0, byte]` and epoch 1: a chunk and a descriptor.
    async fn put_snapshot(store: &MemoryStore, keys: &ArchiveKeys, byte: u8) -> String {
        let at = Position::new([0, byte]);
        let id = ArchiveKeys::snapshot_id(Some(&at), LeaseEpoch::new(1));
        let chunk = vec![byte];
        store
            .put(&keys.chunk(&id, 0), chunk.clone())
            .await
            .expect("put");
        let descriptor = SnapshotDescriptor {
            format: FORMAT,
            database: "ledger".to_owned(),
            engine: "sqlite".to_owned(),
            epoch: 1,
            position: Some(crate::changes::hex(at.as_bytes())),
            chain: ChainHash::GENESIS.to_string(),
            taken_at_ms: u64::from(byte),
            chunks: 1,
            bytes: 1,
            checksum: String::new(),
            chunk_checksums: vec![checksum_hex(&chunk)],
        };
        store
            .put(&keys.descriptor(&id), encode_descriptor(&descriptor))
            .await
            .expect("put");
        id
    }

    #[tokio::test]
    async fn metadata_objects_attach_to_their_snapshots_and_orphans_are_listed() {
        let store = MemoryStore::new();
        let keys = ArchiveKeys::new("fleetring", &name());
        let with = put_snapshot(&store, &keys, 1).await;
        let without = put_snapshot(&store, &keys, 2).await;
        let sum = "0123456789abcdef0123456789abcdef";
        let other = "fedcba9876543210fedcba9876543210";

        // One snapshot's object; an object of a snapshot that isn't there, and one of a
        // snapshot still being uploaded (chunks, no descriptor); a second object for `with`;
        // and a key under the prefix that is none of them.
        store
            .put(&keys.metadata(&with, sum), b"one".to_vec())
            .await
            .expect("put");
        store
            .put(&keys.metadata(&with, other), b"two".to_vec())
            .await
            .expect("put");
        let gone = ArchiveKeys::snapshot_id(Some(&Position::new([0, 9])), LeaseEpoch::new(1));
        store
            .put(&keys.metadata(&gone, sum), b"x".to_vec())
            .await
            .expect("put");
        let uploading = ArchiveKeys::snapshot_id(Some(&Position::new([0, 7])), LeaseEpoch::new(1));
        store
            .put(&keys.chunk(&uploading, 0), b"c".to_vec())
            .await
            .expect("put");
        store
            .put(&keys.metadata(&uploading, sum), b"y".to_vec())
            .await
            .expect("put");
        let junk = format!("{}notes.txt", keys.metadata_objects());
        store.put(&junk, b"?".to_vec()).await.expect("put");
        // Another database's objects are not this one's.
        let tenant = ArchiveKeys::new("fleetring", &"tenant".parse().expect("a name"));
        store
            .put(&tenant.metadata(&with, sum), b"z".to_vec())
            .await
            .expect("put");

        let listing = Listing::read(&store, &keys, &name(), EngineKind::Sqlite)
            .await
            .expect("lists");

        let attached: Vec<(&str, Option<&str>)> = listing
            .snapshots
            .iter()
            .map(|snapshot| {
                (
                    snapshot.id.as_str(),
                    snapshot.metadata.as_ref().map(|entry| entry.key.as_str()),
                )
            })
            .collect();
        // The last of a snapshot's objects by key attaches.
        let attached_key = keys.metadata(&with, sum).max(keys.metadata(&with, other));
        let mut expected = vec![
            (with.as_str(), Some(attached_key.as_str())),
            (without.as_str(), None),
        ];
        expected.sort();
        let mut attached = attached;
        attached.sort();
        assert_eq!(attached, expected);
        let entry = listing
            .snapshots
            .iter()
            .find_map(|snapshot| snapshot.metadata.as_ref())
            .expect("an entry");
        assert_eq!(entry.size, 3);
        assert_eq!(
            keys.parse_metadata_key(&entry.key).expect("a key").1,
            entry.checksum
        );

        let mut orphans: Vec<&str> = listing
            .orphaned_metadata
            .iter()
            .map(|orphan| orphan.key.as_str())
            .collect();
        orphans.sort();
        let mut want = [
            keys.metadata(&with, sum).min(keys.metadata(&with, other)),
            keys.metadata(&gone, sum),
            keys.metadata(&uploading, sum),
        ];
        want.sort();
        assert_eq!(orphans, want.iter().map(String::as_str).collect::<Vec<_>>());
        let nine = listing
            .orphaned_metadata
            .iter()
            .find(|orphan| orphan.id == gone)
            .expect("the gone snapshot's");
        assert_eq!(nine.position, Some(Position::new([0, 9])));

        // The unparsable key is damaged, as under the database's own prefix; nothing else is.
        assert_eq!(listing.damaged.len(), 1, "{:#?}", listing.damaged);
        assert_eq!(listing.damaged[0].key, junk);
        assert_eq!(listing.incomplete.len(), 1);
        assert_eq!(listing.segments.len(), 0);
    }
}
