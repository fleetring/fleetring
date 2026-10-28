//! SQLite's cumulative WAL checksum.

/// The byte order in which a WAL's checksums read the data as 32-bit words.
///
/// SQLite picks it from the writing machine's native order and records it in the low bit of the
/// header's magic number, so a WAL written on one machine validates on any other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChecksumOrder {
    /// Words are little-endian: magic number `0x377f0682`.
    LittleEndian,
    /// Words are big-endian: magic number `0x377f0683`.
    BigEndian,
}

/// A running checksum: the pair of 32-bit sums that SQLite carries from the header through every
/// frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Checksum(pub(crate) [u32; 2]);

impl Checksum {
    /// Continues the checksum over `data`, whose length must be a multiple of 8.
    ///
    /// SQLite's algorithm reads `data` as pairs of words and, for each pair `(a, b)`, sets
    /// `s0 += a + s1` and then `s1 += b + s0`, wrapping on overflow.
    pub(crate) fn update(self, order: ChecksumOrder, data: &[u8]) -> Self {
        debug_assert!(
            data.len().is_multiple_of(8),
            "checksummed data must be whole word pairs, got {} bytes",
            data.len()
        );
        let read = match order {
            ChecksumOrder::LittleEndian => u32::from_le_bytes,
            ChecksumOrder::BigEndian => u32::from_be_bytes,
        };
        let [mut s0, mut s1] = self.0;
        let (pairs, _) = data.as_chunks::<8>();
        for &[a0, a1, a2, a3, b0, b1, b2, b3] in pairs {
            s0 = s0.wrapping_add(read([a0, a1, a2, a3])).wrapping_add(s1);
            s1 = s1.wrapping_add(read([b0, b1, b2, b3])).wrapping_add(s0);
        }
        Self([s0, s1])
    }
}
