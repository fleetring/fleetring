//! The SQLite WAL format, for FleetRing's SQLite engine adapter (ADR-0016).
//!
//! The crate reads committed transactions from a WAL file, validates them with SQLite's own
//! checksums, and mirrors them into a follower's WAL. It works on bytes and files and doesn't
//! link SQLite. Its types stay inside the SQLite adapter: everything above the adapter sees only
//! engine-defined positions and payloads.
//!
//! - [`WalHeader`] and [`FrameHeader`] parse the 32-byte WAL header and the 24-byte frame
//!   headers. Magic numbers `0x377f0682` and `0x377f0683` select little- or big-endian checksum
//!   words ([`ChecksumOrder`]).
//! - [`read_committed`] returns the complete transactions after a [`Position`] as one [`Batch`].
//! - [`Mirror`] appends batches to a follower's WAL, starts new generations, and truncates back
//!   to a commit boundary. [`LogEnd`] holds the same rules without I/O.
//! - [`Position::to_bytes`] and [`Batch::to_bytes`] are the opaque position and payload that
//!   travel above the adapter. Encoded positions compare bytewise in log order.
//!
//! # Examples
//!
//! A primary reads what is new in its WAL; a follower decodes the shipped payload and appends it.
//!
//! ```no_run
//! use fleetring_wal::{Batch, Mirror, read_committed_file};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut follower = Mirror::open("follower.db-wal")?;
//! if let Some(batch) = read_committed_file("primary.db-wal", follower.position())? {
//!     let payload = batch.to_bytes();
//!
//!     let batch = Batch::from_bytes(&payload)?;
//!     if let Some(header) = batch.header() {
//!         // Checkpoint the follower's database before discarding the old generation.
//!         follower.start_generation(header)?;
//!     }
//!     follower.append(&batch)?;
//! }
//! # Ok(())
//! # }
//! ```
#![deny(clippy::float_arithmetic)]

mod batch;
mod checksum;
mod frame;
mod header;
mod log_end;
mod mirror;
mod position;
mod reader;

pub use batch::{Batch, DecodeBatchError};
pub use checksum::ChecksumOrder;
pub use frame::{FRAME_HEADER_LEN, FrameError, FrameHeader};
pub use header::{HeaderError, Salts, WAL_HEADER_LEN, WalHeader};
pub use log_end::{AppendError, LogEnd};
pub use mirror::{Mirror, MirrorError};
pub use position::{DecodePositionError, Generation, Position};
pub use reader::{ReadError, ReadFileError, read_committed, read_committed_file};

const _: () = {
    const fn assert_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_send_sync::<AppendError>();
    assert_send_sync::<Batch>();
    assert_send_sync::<ChecksumOrder>();
    assert_send_sync::<DecodeBatchError>();
    assert_send_sync::<DecodePositionError>();
    assert_send_sync::<FrameError>();
    assert_send_sync::<FrameHeader>();
    assert_send_sync::<Generation>();
    assert_send_sync::<HeaderError>();
    assert_send_sync::<LogEnd>();
    assert_send_sync::<Mirror>();
    assert_send_sync::<MirrorError>();
    assert_send_sync::<Position>();
    assert_send_sync::<ReadError>();
    assert_send_sync::<ReadFileError>();
    assert_send_sync::<Salts>();
    assert_send_sync::<WalHeader>();
};
