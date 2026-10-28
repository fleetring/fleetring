//! Positions in a WAL: a generation and a byte offset within it.

use std::fmt;

use crate::header::Salts;

/// The offset of a generation's first frame, just past the WAL header.
pub(crate) const HEADER_END: u64 = 32;

/// One generation of a WAL: the span between two restarts.
///
/// SQLite restarts the WAL after a checkpoint, writing a new header with the checkpoint sequence
/// number incremented and new salts. The sequence number orders generations; the salts tell
/// apart two logs that happen to share a sequence number.
///
/// The sequence number only grows while the WAL's shared-memory index survives, that is while
/// some connection keeps the database open. The adapter keeps its write connection open for the
/// database's lifetime, so within one log the order holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Generation {
    checkpoint_seq: u32,
    salts: Salts,
}

impl Generation {
    /// Creates a generation from a header's checkpoint sequence number and salts.
    #[must_use]
    pub const fn new(checkpoint_seq: u32, salts: Salts) -> Self {
        Self {
            checkpoint_seq,
            salts,
        }
    }

    /// The checkpoint sequence number from the generation's header.
    #[must_use]
    pub const fn checkpoint_seq(self) -> u32 {
        self.checkpoint_seq
    }

    /// The salts from the generation's header.
    #[must_use]
    pub const fn salts(self) -> Salts {
        self.salts
    }

    /// The position just after the generation's header, before its first frame.
    #[must_use]
    pub const fn start(self) -> Position {
        Position::new(self, HEADER_END)
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{:08x}{:08x}",
            self.checkpoint_seq,
            self.salts.first(),
            self.salts.second()
        )
    }
}

/// A position in a WAL: a generation and a byte offset into that generation's file.
///
/// Positions handed out by this crate always sit on a commit boundary: the start of a
/// generation, or just past a commit frame. Positions are totally ordered by generation, then
/// offset, which is log order within one log.
///
/// [`to_bytes`](Self::to_bytes) gives a fixed-width encoding whose lexicographic byte order is
/// the same as this order, so code above the engine adapter can compare encoded positions
/// without decoding them (ADR-0016).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Position {
    generation: Generation,
    offset: u64,
}

impl Position {
    /// The length of [`to_bytes`](Self::to_bytes)'s output.
    pub const ENCODED_LEN: usize = 20;

    /// Creates a position at byte `offset` of `generation`.
    #[must_use]
    pub const fn new(generation: Generation, offset: u64) -> Self {
        Self { generation, offset }
    }

    /// The generation this position is in.
    #[must_use]
    pub const fn generation(self) -> Generation {
        self.generation
    }

    /// The byte offset into the generation's WAL file. The first frame starts at offset 32.
    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Encodes the position as 20 bytes whose lexicographic order is the position order.
    ///
    /// The layout is the checkpoint sequence number, the first salt, the second salt, and the
    /// offset, each big-endian: 4 + 4 + 4 + 8 bytes.
    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0; Self::ENCODED_LEN];
        out[0..4].copy_from_slice(&self.generation.checkpoint_seq.to_be_bytes());
        out[4..8].copy_from_slice(&self.generation.salts.first().to_be_bytes());
        out[8..12].copy_from_slice(&self.generation.salts.second().to_be_bytes());
        out[12..20].copy_from_slice(&self.offset.to_be_bytes());
        out
    }

    /// Decodes a position from [`to_bytes`](Self::to_bytes)'s encoding.
    ///
    /// # Errors
    ///
    /// Returns [`DecodePositionError::WrongLength`] unless `bytes` is exactly
    /// [`ENCODED_LEN`](Self::ENCODED_LEN) bytes long.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodePositionError> {
        let Ok(bytes) = <&[u8; Self::ENCODED_LEN]>::try_from(bytes) else {
            return Err(DecodePositionError::WrongLength { len: bytes.len() });
        };
        let [s0, s1, s2, s3, a0, a1, a2, a3, b0, b1, b2, b3, rest @ ..] = *bytes;
        let generation = Generation::new(
            u32::from_be_bytes([s0, s1, s2, s3]),
            Salts::new(
                u32::from_be_bytes([a0, a1, a2, a3]),
                u32::from_be_bytes([b0, b1, b2, b3]),
            ),
        );
        Ok(Self::new(generation, u64::from_be_bytes(rest)))
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.generation, self.offset)
    }
}

/// An error from decoding a [`Position`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodePositionError {
    /// The input isn't exactly [`Position::ENCODED_LEN`] bytes long.
    #[error("position needs {} bytes, got {len}", Position::ENCODED_LEN)]
    WrongLength {
        /// The input's length in bytes.
        len: usize,
    },
}
