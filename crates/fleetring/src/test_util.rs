//! Test doubles for code that uses `fleetring`.
//!
//! Available with the `test-util` feature. Nothing here belongs in a production ring.

use std::collections::BTreeMap;

use crate::{StableHasher, Xxh3};

/// A [`StableHasher`] whose outputs a test chooses.
///
/// Inputs recorded with [`with`](Self::with) hash to the chosen value regardless of the seed.
/// Every other input falls back to [`Xxh3`]. Use it to put keys, nodes, or tokens at exact
/// positions in a test.
///
/// # Examples
///
/// ```
/// use fleetring::StableHasher;
/// use fleetring::test_util::TableHasher;
///
/// let hasher = TableHasher::new().with("agent:42", 7);
/// assert_eq!(hasher.hash(b"agent:42", 0), 7);
/// assert_eq!(hasher.hash(b"agent:42", 99), 7);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableHasher {
    table: BTreeMap<Box<[u8]>, u64>,
}

impl TableHasher {
    /// Creates a hasher with an empty table, which behaves exactly like [`Xxh3`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes `input` hash to `output` under every seed.
    #[must_use]
    pub fn with(mut self, input: impl AsRef<[u8]>, output: u64) -> Self {
        self.table.insert(input.as_ref().into(), output);
        self
    }
}

impl StableHasher for TableHasher {
    fn algorithm(&self) -> &'static str {
        "table-test"
    }

    fn hash(&self, bytes: &[u8], seed: u64) -> u64 {
        self.table
            .get(bytes)
            .copied()
            .unwrap_or_else(|| Xxh3.hash(bytes, seed))
    }
}
