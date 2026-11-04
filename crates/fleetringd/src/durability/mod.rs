//! Archives: verifiable copies of databases in object storage, with point-in-time restore
//! (ADR-0029, FR-049).
//!
//! Replicas give availability; archives give backups. A `DROP TABLE` replicates to every copy
//! within a round trip, and a lost fleet loses every copy, but an archive keeps every
//! acknowledged batch of the log in a store outside the fleet, from which a restore rebuilds a
//! database as it was at any point the archive holds.
//!
//! # Shape
//!
//! An archive is a physical change stream (FR-039) plus base snapshots, written to an
//! [`ObjectStore`]. Its sink turns each delivered batch into one immutable **segment**; its
//! pump is paced, so a segment holds everything acknowledged in an interval. A **base
//! snapshot** is a pinned copy of the database at the archive's own position, which a pump
//! reaches only through acknowledged batches, packed and cut into chunks. Because the sink is a
//! change sink, the archive inherits what streams guarantee: only acknowledged batches, in order,
//! at least once, never in a write's path, and continuing after a failover from the mark in the
//! new primary's copy (ADR-0028). Its recovery point is its lag.
//!
//! # Layout
//!
//! Every object is written once under a name that says what it holds ([`ArchiveKeys`]), so a
//! `list` of a database's prefix is its manifest and no object ever needs a lock or a
//! conditional write. A snapshot's descriptor is written last and marks it complete.
//!
//! # The chain
//!
//! Each segment records the hash chain over the log (FR-044) at its start and its end, and each
//! snapshot the hash at its position. A restore recomputes the chain over every batch it applies
//! and refuses a batch whose result differs, so a restored copy is proven to hold the log the
//! fleet held, transaction by transaction.
//!
//! # Stores
//!
//! [`MemoryStore`] is for tests and simulation, [`DirectoryStore`] for tests and mounted
//! volumes, and [`FaultyStore`] fails a fixed schedule of calls. The archive logic above them is
//! written once against the trait and knows nothing about any one store.

mod format;
mod keys;
mod store;

use std::fmt;

pub use self::format::{
    FORMAT, FormatError, SEGMENT_MAGIC, SegmentHeader, SnapshotDescriptor, decode_descriptor,
    decode_segment, encode_descriptor, encode_segment,
};
pub use self::keys::{ArchiveKeys, SegmentName, SnapshotObject};
pub use self::store::{
    DirectoryStore, FaultyStore, MemoryStore, ObjectEntry, ObjectStore, StoreError, StoreFuture,
};

const _: () = {
    const fn assert_debug_send_sync<T: fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<ArchiveKeys>();
    assert_debug_send_sync::<DirectoryStore>();
    assert_debug_send_sync::<FaultyStore>();
    assert_debug_send_sync::<FormatError>();
    assert_debug_send_sync::<MemoryStore>();
    assert_debug_send_sync::<ObjectEntry>();
    assert_debug_send_sync::<SegmentHeader>();
    assert_debug_send_sync::<SegmentName>();
    assert_debug_send_sync::<SnapshotDescriptor>();
    assert_debug_send_sync::<SnapshotObject>();
    assert_debug_send_sync::<StoreError>();
};
