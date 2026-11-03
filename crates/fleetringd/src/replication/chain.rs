//! The hash chain over a database's replication log (FR-044): one BLAKE3 hash per transaction,
//! each folding in the hash before it, so that the hash at a position commits every byte of the
//! log before it, and two replicas that agree on it agree on the whole prefix.
//!
//! The chain is engine-neutral: it hashes the opaque payload bytes of each transaction, which
//! [`FrameSourceEngine::transactions`] delimits inside a batch, so a batch read back from the log
//! as several transactions hashes the same as the batches that shipped them one at a time. The
//! epoch a batch ships at isn't hashed: the same bytes are re-shipped by a later primary after a
//! promotion, and two replicas holding identical logs must agree.
//!
//! Each replica keeps its chain in memory, from a durable **anchor** on: the hash at the
//! position its log's current generation continues from, recorded at every checkpoint and when a
//! snapshot is installed, in a file next to the database's epoch record. After a restart the
//! replica reads its log back from the anchor and recomputes the rest, so no extra write or fsync
//! lands on the write path. A replica whose record doesn't fit its log any more, or that predates
//! the chain, **derives** an anchor from the position its log begins at; a derived chain proves
//! agreement from there on only, and such a follower takes the primary's hash on first contact
//! rather than refusing it.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::str::FromStr;

use fleetring_control::DatabaseName;
use serde::{Deserialize, Serialize};

use super::epochs::sync_dir;
use crate::engine::{EngineError, FrameBatch, FrameSourceEngine, Position};
use crate::layout;

/// The file that holds a database's chain anchor, inside its directory under the store's root.
const CHAIN_FILE: &str = "chain";

/// How many positions a chain keeps hashes for in memory, beyond its anchor. The log between two
/// checkpoints holds fewer transactions than this at the default threshold, so recovery from the
/// anchor always fits; older entries only serve `db verify --at`.
const MAX_ENTRIES: usize = 8192;

/// The context that separates a derived anchor from a transaction hash.
const DERIVED_CONTEXT: &str = "fleetring replication chain v1: derived anchor";

/// A hash on the chain: BLAKE3's 32 bytes.
///
/// It prints and parses as 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChainHash([u8; Self::LEN]);

impl ChainHash {
    /// The hash's length in bytes.
    pub const LEN: usize = 32;

    /// The hash before the first transaction of a log that began empty.
    pub const GENESIS: Self = Self([0; Self::LEN]);

    /// The hash's bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }

    /// The hash `bytes` holds, if there are exactly [`LEN`](Self::LEN) of them.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        <[u8; Self::LEN]>::try_from(bytes).ok().map(Self)
    }

    /// The hash at the end of `batch`, which continues the log from `from_position`, chained
    /// onto `prev`, the hash there: what a primary ships with the batch and a follower checks
    /// before applying it.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidPayload`] if `engine` can't delimit the batch's transactions, or
    /// they don't end at the batch's end.
    pub fn chain(
        engine: &dyn FrameSourceEngine,
        prev: Self,
        from_position: Option<&Position>,
        batch: &FrameBatch,
    ) -> Result<Self, EngineError> {
        match Links::compute(engine, prev, from_position, batch) {
            Ok(links) => Ok(links.end()),
            Err(ChainError::Payload(error)) => Err(error),
            Err(error) => Err(EngineError::InvalidPayload(error.into())),
        }
    }

    /// The hash of the transaction from `start` to `end` with `bytes`, chained onto `prev`.
    ///
    /// The input is `prev`, then each position as its length (a little-endian `u32`) and its
    /// bytes, with an absent `start` as length 0, then the BLAKE3 hash of the transaction's
    /// bytes. The bytes are hashed on their own so that they start on a chunk boundary, where
    /// BLAKE3 hashes whole chunks in parallel; fed after the positions they would not.
    fn next(prev: &Self, start: Option<&Position>, end: &Position, bytes: &[u8]) -> Self {
        let content = blake3::hash(bytes);
        let mut hasher = blake3::Hasher::new();
        hasher.update(&prev.0);
        feed_position(&mut hasher, start);
        feed_position(&mut hasher, Some(end));
        hasher.update(content.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    /// The anchor a replica derives for a log it has no record for, from the position the log
    /// begins at. It depends on nothing but the position, so replicas on the same generation
    /// derive the same one.
    pub(super) fn derived(position: Option<&Position>) -> Self {
        let mut hasher = blake3::Hasher::new_derive_key(DERIVED_CONTEXT);
        feed_position(&mut hasher, position);
        Self(*hasher.finalize().as_bytes())
    }
}

fn feed_position(hasher: &mut blake3::Hasher, position: Option<&Position>) {
    let bytes = position.map(Position::as_bytes).unwrap_or_default();
    let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    hasher.update(&len.to_le_bytes());
    hasher.update(bytes);
}

impl fmt::Display for ChainHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ChainHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChainHash({self})")
    }
}

/// The error returned when text isn't a chain hash.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a chain hash is {} hex digits", 2 * ChainHash::LEN)]
pub struct ParseChainHashError;

impl FromStr for ChainHash {
    type Err = ParseChainHashError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != 2 * Self::LEN {
            return Err(ParseChainHashError);
        }
        let mut bytes = [0; Self::LEN];
        let (pairs, _) = text.as_bytes().as_chunks::<2>();
        for (byte, pair) in bytes.iter_mut().zip(pairs) {
            let pair = std::str::from_utf8(pair).map_err(|_| ParseChainHashError)?;
            *byte = u8::from_str_radix(pair, 16).map_err(|_| ParseChainHashError)?;
        }
        Ok(Self(bytes))
    }
}

impl Serialize for ChainHash {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ChainHash {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// Where a chain's anchor came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Provenance {
    /// Carried over from the chain the log was replicated under: the log began empty, a
    /// snapshot brought it, or a checkpoint recorded it.
    Recorded,
    /// Derived from the position the log begins at, because no record fit the log. The chain
    /// proves agreement from there on only.
    Derived,
}

/// The durable start of a replica's chain: the hash at one position of its log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Anchor {
    /// The database, so that a scan of the store can name it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    /// The position, as hex, or `None` for an empty log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    position: Option<String>,
    hash: ChainHash,
    provenance: Provenance,
}

impl Anchor {
    /// The anchor at `position` with `hash`, from `provenance`.
    pub(super) fn new(
        position: Option<&Position>,
        hash: ChainHash,
        provenance: Provenance,
    ) -> Self {
        Self {
            database: None,
            position: position.map(ToString::to_string),
            hash,
            provenance,
        }
    }

    /// The anchor of a log that began empty.
    pub(super) fn genesis() -> Self {
        Self::new(None, ChainHash::GENESIS, Provenance::Recorded)
    }

    /// The anchor derived for a log that begins at `position`.
    fn derived(position: Option<&Position>) -> Self {
        Self::new(position, ChainHash::derived(position), Provenance::Derived)
    }

    /// The anchor's position.
    pub(super) fn position(&self) -> Option<Position> {
        self.position.as_deref().and_then(decode_hex)
    }
}

/// The hashes a replica holds over its log: an anchor and the hash at every transaction end
/// after it that is still kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Chain {
    anchor: Anchor,
    anchor_position: Option<Position>,
    /// Hashes at positions after the anchor, and, after a rebase, up to [`MAX_ENTRIES`] before
    /// it.
    entries: BTreeMap<Position, ChainHash>,
}

/// The hashes of a batch's transactions, computed and checked before they join a chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Links {
    /// The hash at the batch's start.
    from: ChainHash,
    /// The hash at each transaction's end, in order.
    hashes: Vec<(Position, ChainHash)>,
}

impl Links {
    /// The hash at the batch's end.
    pub(super) fn end(&self) -> ChainHash {
        self.hashes.last().map_or(self.from, |(_, hash)| *hash)
    }

    /// The hashes of `batch`'s transactions, chained onto `from`, the hash at `from_position`,
    /// the position the batch continues from as the replica's log has it.
    ///
    /// That position, not the batch's own `start`, is what the first transaction is hashed
    /// from: a batch that starts a new generation of the log names the generation's beginning
    /// as its start, while every replica continues it from where its log ended.
    ///
    /// # Errors
    ///
    /// [`ChainError::Payload`] if the engine can't delimit the batch's transactions, and
    /// [`ChainError::Malformed`] if they overlap, run past the payload, or don't end at the
    /// batch's end.
    pub(super) fn compute(
        engine: &dyn FrameSourceEngine,
        from: ChainHash,
        from_position: Option<&Position>,
        batch: &FrameBatch,
    ) -> Result<Self, ChainError> {
        let payload = batch.payload.as_bytes();
        let transactions = engine.transactions(batch)?;
        let mut hashes = Vec::with_capacity(transactions.len());
        let mut prev = from;
        let mut start: Option<&Position> = from_position;
        let mut covered = 0;
        for transaction in &transactions {
            let bytes = payload
                .get(transaction.bytes.clone())
                .filter(|_| transaction.bytes.start >= covered)
                .ok_or_else(|| ChainError::Malformed {
                    position: transaction.end.clone(),
                })?;
            covered = transaction.bytes.end;
            prev = ChainHash::next(&prev, start, &transaction.end, bytes);
            hashes.push((transaction.end.clone(), prev));
            start = Some(&transaction.end);
        }
        let ends_at_batch_end = hashes.last().is_some_and(|(end, _)| *end == batch.end);
        if !ends_at_batch_end {
            return Err(ChainError::Malformed {
                position: batch.end.clone(),
            });
        }
        Ok(Self { from, hashes })
    }
}

impl Chain {
    /// A chain that starts at `anchor`.
    pub(super) fn new(anchor: Anchor) -> Self {
        Self {
            anchor_position: anchor.position(),
            anchor,
            entries: BTreeMap::new(),
        }
    }

    /// The chain of a log that began empty: [`ChainHash::GENESIS`] before the first transaction.
    pub(super) fn genesis() -> Self {
        Self::new(Anchor::genesis())
    }

    /// The anchor the chain starts at.
    pub(super) fn anchor(&self) -> &Anchor {
        &self.anchor
    }

    /// The anchor's position.
    pub(super) fn anchor_position(&self) -> Option<&Position> {
        self.anchor_position.as_ref()
    }

    /// Whether the anchor was derived rather than carried over, so the chain proves agreement
    /// from the anchor on only.
    pub(super) fn is_derived(&self) -> bool {
        self.provenance() == Provenance::Derived
    }

    /// Where the anchor came from.
    pub(super) fn provenance(&self) -> Provenance {
        self.anchor.provenance
    }

    /// The position the chain ends at, and the hash there.
    pub(super) fn tip(&self) -> (Option<&Position>, ChainHash) {
        match self.entries.last_key_value() {
            Some((position, hash)) if Some(position) > self.anchor_position.as_ref() => {
                (Some(position), *hash)
            }
            _ => (self.anchor_position.as_ref(), self.anchor.hash),
        }
    }

    /// The hash at `position`, if the chain holds it.
    pub(super) fn hash_at(&self, position: Option<&Position>) -> Option<ChainHash> {
        if position == self.anchor_position.as_ref() {
            return Some(self.anchor.hash);
        }
        self.entries.get(position?).copied()
    }

    /// Every position the chain holds a hash at, with the hash: the anchor's, unless the log
    /// was empty there, and each kept transaction end.
    pub(super) fn history(&self) -> BTreeMap<Position, ChainHash> {
        let mut history = self.entries.clone();
        if let Some(position) = &self.anchor_position {
            history.insert(position.clone(), self.anchor.hash);
        }
        history
    }

    /// The hashes of `batch`'s transactions, chained from the hash at `from`, the position the
    /// batch continues from, checked against any the chain already holds at the same positions.
    ///
    /// # Errors
    ///
    /// [`ChainError::Unknown`] if the chain doesn't hold `from`, [`ChainError::Diverged`] if it
    /// holds a different hash at one of the transaction ends, and the errors of
    /// [`Links::compute`].
    pub(super) fn prepare(
        &self,
        engine: &dyn FrameSourceEngine,
        from: Option<&Position>,
        batch: &FrameBatch,
    ) -> Result<Links, ChainError> {
        let hash = self.hash_at(from).ok_or_else(|| ChainError::Unknown {
            position: from.cloned(),
        })?;
        let links = Links::compute(engine, hash, from, batch)?;
        for (position, computed) in &links.hashes {
            if let Some(held) = self.hash_at(Some(position))
                && held != *computed
            {
                return Err(ChainError::Diverged {
                    position: position.clone(),
                    held,
                    computed: *computed,
                });
            }
        }
        Ok(links)
    }

    /// Adds `links` to the chain.
    pub(super) fn commit(&mut self, links: Links) {
        for (position, hash) in links.hashes {
            self.entries.insert(position, hash);
        }
        self.prune();
    }

    /// Adds `links`, computed outside the chain's lock, unless the chain holds a different hash
    /// at one of their positions.
    ///
    /// # Errors
    ///
    /// [`ChainError::Diverged`] naming the first such position.
    pub(super) fn absorb(&mut self, links: Links) -> Result<(), ChainError> {
        for (position, computed) in &links.hashes {
            if let Some(held) = self.hash_at(Some(position))
                && held != *computed
            {
                return Err(ChainError::Diverged {
                    position: position.clone(),
                    held,
                    computed: *computed,
                });
            }
        }
        self.commit(links);
        Ok(())
    }

    /// Extends the chain with `batch`, which continues from `from`, and returns the hash at its
    /// end.
    ///
    /// # Errors
    ///
    /// As [`prepare`](Self::prepare).
    pub(super) fn extend(
        &mut self,
        engine: &dyn FrameSourceEngine,
        from: Option<&Position>,
        batch: &FrameBatch,
    ) -> Result<ChainHash, ChainError> {
        let links = self.prepare(engine, from, batch)?;
        let end = links.end();
        self.commit(links);
        Ok(end)
    }

    /// Moves the anchor to `position`, which the chain holds, keeping the entries, and returns
    /// the anchor to record. A checkpoint rebases the chain where it ended, so a restart can
    /// recompute the rest from the log.
    pub(super) fn rebase(&mut self, position: &Position) -> Option<Anchor> {
        let hash = self.hash_at(Some(position))?;
        self.anchor = Anchor::new(Some(position), hash, self.anchor.provenance);
        self.anchor_position = Some(position.clone());
        self.entries.insert(position.clone(), hash);
        self.prune();
        Some(self.anchor.clone())
    }

    /// Takes `hash` as the chain's value at `position` and makes it the recorded anchor,
    /// forgetting every other hash: they were on another chain. A follower whose chain was
    /// derived adopts the primary's hash this way, and a promotion's candidate the fenced
    /// member's.
    pub(super) fn adopt(&mut self, position: Option<&Position>, hash: ChainHash) -> Anchor {
        self.entries.clear();
        self.anchor = Anchor::new(position, hash, Provenance::Recorded);
        self.anchor_position = position.cloned();
        if let Some(position) = position {
            self.entries.insert(position.clone(), hash);
        }
        self.anchor.clone()
    }

    fn prune(&mut self) {
        while self.entries.len() > MAX_ENTRIES {
            let oldest = self.entries.keys().next().cloned();
            match oldest {
                Some(oldest) if Some(&oldest) != self.anchor_position.as_ref() => {
                    self.entries.remove(&oldest);
                }
                _ => break,
            }
        }
    }
}

/// What a replica read from its log to rebuild its chain.
#[derive(Debug)]
pub(super) enum LogRead {
    /// Everything after the recorded anchor; `None` if the log ends there.
    Continues(Option<FrameBatch>),
    /// The whole current log, because the recorded anchor doesn't fit it, or there is none;
    /// `None` for an empty log.
    Restarted(Option<FrameBatch>),
}

/// What [`recover`] built, and whether the anchor it starts from is one the replica should now
/// record: the genesis of a log that began empty here, which no record held yet.
#[derive(Debug)]
pub(super) struct Recovered {
    pub(super) chain: Chain,
    pub(super) record: Option<Anchor>,
}

/// Rebuilds a replica's chain from its recorded anchor, if any, and `read`, what it read from
/// its log, which ends at `position`.
///
/// A record that fits the log anchors the chain, and the read continues it to the log's end.
/// A log that began empty and has no record starts at [`ChainHash::GENESIS`], which the
/// replica records so a restart continues from it. Otherwise the chain is derived from the
/// position the log begins at.
///
/// # Errors
///
/// [`ChainError::Payload`] or [`ChainError::Malformed`] if the log read back can't be hashed.
pub(super) fn recover(
    engine: &dyn FrameSourceEngine,
    recorded: Option<Anchor>,
    position: Option<&Position>,
    read: LogRead,
) -> Result<Recovered, ChainError> {
    let had_record = recorded.is_some();
    let batch = match (recorded, read) {
        (Some(anchor), LogRead::Continues(batch)) => {
            let mut chain = Chain::new(anchor);
            if let Some(batch) = &batch {
                let from = chain.anchor_position.clone();
                chain.extend(engine, from.as_ref(), batch)?;
            }
            if chain.tip().0 == position {
                return Ok(Recovered {
                    chain,
                    record: None,
                });
            }
            batch
        }
        (_, LogRead::Continues(batch) | LogRead::Restarted(batch)) => batch,
    };
    let Some(batch) = batch else {
        return Ok(if position.is_none() {
            let chain = Chain::genesis();
            Recovered {
                record: (!had_record).then(|| chain.anchor().clone()),
                chain,
            }
        } else {
            Recovered {
                chain: Chain::new(Anchor::derived(position)),
                record: None,
            }
        });
    };
    let mut chain = Chain::new(Anchor::derived(Some(&batch.start)));
    chain.extend(engine, Some(&batch.start), &batch)?;
    Ok(Recovered {
        chain,
        record: None,
    })
}

/// The error returned when a batch can't join a chain, or a chain can't answer for a position.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(super) enum ChainError {
    /// The chain holds no hash at the position.
    #[error("the chain holds no hash at {}", position_text(position.as_ref()))]
    Unknown {
        /// The position.
        position: Option<Position>,
    },
    /// The chain holds a hash at the position that differs from the one computed.
    #[error("the chain holds {held} at {position}, but the log hashes to {computed}")]
    Diverged {
        /// The position.
        position: Position,
        /// The hash the chain holds.
        held: ChainHash,
        /// The hash computed over the log.
        computed: ChainHash,
    },
    /// The batch's transactions don't cover its payload up to its end.
    #[error("the batch's transactions do not cover it up to {position}")]
    Malformed {
        /// The batch's end, or the transaction's.
        position: Position,
    },
    /// The engine can't delimit the batch's transactions.
    #[error("can't delimit the batch's transactions")]
    Payload(#[from] EngineError),
}

fn position_text(position: Option<&Position>) -> String {
    position.map_or_else(|| "the empty log".to_owned(), ToString::to_string)
}

/// The hash at `at`, recomputed over `batch`, what the log holds from `anchor` to `at`, or
/// `None` if the log ends at the anchor.
///
/// # Errors
///
/// [`ChainError::Unknown`] if the batch doesn't reach `at`, and the errors of
/// [`Chain::extend`].
pub(super) fn recompute(
    engine: &dyn FrameSourceEngine,
    anchor: &Anchor,
    batch: Option<&FrameBatch>,
    at: &Position,
) -> Result<ChainHash, ChainError> {
    let mut chain = Chain::new(anchor.clone());
    if let Some(batch) = batch {
        let from = anchor.position();
        chain.extend(engine, from.as_ref(), batch)?;
    }
    chain.hash_at(Some(at)).ok_or_else(|| ChainError::Unknown {
        position: Some(at.clone()),
    })
}

/// Every database's recorded anchor, one small file each. Cloning is cheap.
#[derive(Debug, Clone)]
pub(super) struct Chains {
    root: PathBuf,
}

impl Chains {
    pub(super) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn file(&self, database: &DatabaseName) -> PathBuf {
        self.root
            .join(layout::database_dir(database))
            .join(CHAIN_FILE)
    }

    /// The anchor recorded for `database`, if any.
    ///
    /// It blocks on the file system.
    pub(super) fn load(&self, database: &DatabaseName) -> io::Result<Option<Anchor>> {
        match fs::read_to_string(self.file(database)) {
            Ok(text) => serde_json::from_str(text.trim())
                .map(Some)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Records `anchor` for `database` durably: a temporary file, synced, renamed over the old
    /// one, then the directory synced.
    ///
    /// It blocks on the file system.
    pub(super) fn store(&self, database: &DatabaseName, anchor: &Anchor) -> io::Result<()> {
        let path = self.file(database);
        let dir = path
            .parent()
            .ok_or_else(|| io::Error::other("a chain file has a parent directory"))?;
        fs::create_dir_all(dir)?;
        let named = Anchor {
            database: Some(database.to_string()),
            ..anchor.clone()
        };
        let temporary = path.with_extension("tmp");
        let mut file = File::create(&temporary)?;
        let text = serde_json::to_string(&named).map_err(io::Error::other)?;
        writeln!(file, "{text}")?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        sync_dir(dir)
    }
}

/// Decodes a position written as hex.
fn decode_hex(text: &str) -> Option<Position> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(text.get(at..at + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()
        .map(Position::from)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::path::Path;

    use super::*;
    use crate::engine::{BatchTransaction, FrameFollower, Payload};

    /// A frame source whose payloads are transactions of `TX` bytes each.
    #[derive(Debug)]
    struct Fixed;

    const TX: usize = 4;

    impl FrameSourceEngine for Fixed {
        fn open_follower(&self, _: &Path) -> Result<Box<dyn FrameFollower>, EngineError> {
            Err(EngineError::NotFound)
        }

        fn transactions(&self, batch: &FrameBatch) -> Result<Vec<BatchTransaction>, EngineError> {
            let start = batch.start.as_bytes()[0];
            let payload = batch.payload.as_bytes();
            Ok((0..payload.len() / TX)
                .map(|index| {
                    let end = u8::try_from(index + 1).expect("small") + start;
                    let range: Range<usize> = index * TX..(index + 1) * TX;
                    BatchTransaction {
                        end: Position::new([end]),
                        bytes: range,
                    }
                })
                .collect())
        }
    }

    /// A chain anchored where the fixed engine's log begins.
    fn rooted() -> Chain {
        Chain::new(Anchor::new(
            Some(&Position::new([0])),
            ChainHash::GENESIS,
            Provenance::Recorded,
        ))
    }

    fn batch(start: u8, transactions: &[u8]) -> FrameBatch {
        let payload: Vec<u8> = transactions
            .iter()
            .flat_map(|byte| std::iter::repeat_n(*byte, TX))
            .collect();
        FrameBatch {
            start: Position::new([start]),
            end: Position::new([start + u8::try_from(transactions.len()).expect("small")]),
            payload: Payload::new(payload),
        }
    }

    #[test]
    fn hashing_does_not_depend_on_how_transactions_were_batched() {
        let mut one = rooted();
        let whole = one
            .extend(&Fixed, Some(&Position::new([0])), &batch(0, &[1, 2, 3]))
            .expect("one batch");
        let mut several = rooted();
        several
            .extend(&Fixed, Some(&Position::new([0])), &batch(0, &[1]))
            .expect("first");
        several
            .extend(&Fixed, Some(&Position::new([1])), &batch(1, &[2, 3]))
            .expect("rest");
        assert_eq!(several.tip().1, whole);
        assert_eq!(several.history(), one.history());
        assert_eq!(
            several.hash_at(Some(&Position::new([1]))),
            one.hash_at(Some(&Position::new([1])))
        );
        assert_ne!(whole, ChainHash::GENESIS);

        let mut other = rooted();
        let differs = other
            .extend(&Fixed, Some(&Position::new([0])), &batch(0, &[1, 2, 4]))
            .expect("other bytes");
        assert_ne!(differs, whole, "the content is covered");
        assert_eq!(
            other.hash_at(Some(&Position::new([2]))),
            one.hash_at(Some(&Position::new([2]))),
            "the prefix agrees"
        );
    }

    #[test]
    fn a_batch_must_continue_the_chain_and_agree_where_it_overlaps() {
        let mut chain = rooted();
        chain
            .extend(&Fixed, Some(&Position::new([0])), &batch(0, &[1, 2]))
            .expect("first");
        assert!(matches!(
            chain.extend(&Fixed, Some(&Position::new([5])), &batch(5, &[9])),
            Err(ChainError::Unknown { .. })
        ));
        // Re-hashing what the chain holds is fine; different bytes there are not.
        chain
            .extend(&Fixed, Some(&Position::new([1])), &batch(1, &[2]))
            .expect("the same bytes again");
        assert!(matches!(
            chain.extend(&Fixed, Some(&Position::new([1])), &batch(1, &[7])),
            Err(ChainError::Diverged { .. })
        ));
        let malformed = FrameBatch {
            end: Position::new([9]),
            ..batch(2, &[3])
        };
        assert!(matches!(
            chain.extend(&Fixed, Some(&Position::new([2])), &malformed),
            Err(ChainError::Malformed { .. })
        ));
    }

    #[test]
    fn rebasing_and_adopting_keep_the_tip_consistent() {
        let mut chain = rooted();
        chain
            .extend(&Fixed, Some(&Position::new([0])), &batch(0, &[1, 2]))
            .expect("batch");
        let at_two = chain.hash_at(Some(&Position::new([2]))).expect("held");
        assert_eq!(chain.tip(), (Some(&Position::new([2])), at_two));
        assert_eq!(chain.rebase(&Position::new([9])), None);

        let anchor = chain.rebase(&Position::new([2])).expect("held");
        assert_eq!(anchor.position(), Some(Position::new([2])));
        assert_eq!(anchor.hash, at_two);
        assert!(chain.hash_at(Some(&Position::new([1]))).is_some());
        chain
            .extend(&Fixed, Some(&Position::new([2])), &batch(2, &[3]))
            .expect("continues");
        assert_eq!(chain.tip().0, Some(&Position::new([3])));

        let adopted = ChainHash([7; ChainHash::LEN]);
        chain.adopt(Some(&Position::new([2])), adopted);
        assert_eq!(chain.tip(), (Some(&Position::new([2])), adopted));
        assert_eq!(chain.hash_at(Some(&Position::new([3]))), None);
        assert!(!chain.is_derived());
    }

    #[test]
    fn recovery_continues_from_a_record_that_fits_and_derives_otherwise() {
        let mut lived = rooted();
        lived
            .extend(&Fixed, Some(&Position::new([0])), &batch(0, &[1, 2]))
            .expect("batch");
        let anchor = lived.rebase(&Position::new([2])).expect("held");
        lived
            .extend(&Fixed, Some(&Position::new([2])), &batch(2, &[3]))
            .expect("after the checkpoint");

        let tail = batch(2, &[3]);
        let recovered = recover(
            &Fixed,
            Some(anchor.clone()),
            Some(&Position::new([3])),
            LogRead::Continues(Some(tail)),
        )
        .expect("recovered");
        assert_eq!(recovered.chain.tip(), lived.tip());
        assert!(!recovered.chain.is_derived());
        assert_eq!(recovered.record, None);

        let derived = recover(
            &Fixed,
            Some(anchor),
            Some(&Position::new([6])),
            LogRead::Restarted(Some(batch(5, &[6]))),
        )
        .expect("derived")
        .chain;
        assert!(derived.is_derived());
        assert_eq!(derived.tip().0, Some(&Position::new([6])));
        let again = recover(
            &Fixed,
            None,
            Some(&Position::new([6])),
            LogRead::Restarted(Some(batch(5, &[6]))),
        )
        .expect("derived again")
        .chain;
        assert_eq!(again.tip(), derived.tip(), "derivation is deterministic");

        let empty = recover(&Fixed, None, None, LogRead::Restarted(None)).expect("empty");
        assert_eq!(empty.chain, Chain::genesis());
        assert_eq!(
            empty.record,
            Some(Anchor::genesis()),
            "recorded from now on"
        );
    }

    #[test]
    fn anchors_round_trip_through_their_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let chains = Chains::new(dir.path().to_owned());
        let db: DatabaseName = "tenant/a".parse().expect("name");
        assert_eq!(chains.load(&db).expect("none"), None);
        let anchor = Anchor::new(
            Some(&Position::new([1, 2, 3])),
            ChainHash([9; ChainHash::LEN]),
            Provenance::Derived,
        );
        chains.store(&db, &anchor).expect("stored");
        let loaded = chains.load(&db).expect("read").expect("some");
        assert_eq!(loaded.position(), Some(Position::new([1, 2, 3])));
        assert_eq!(loaded.hash, anchor.hash);
        assert_eq!(loaded.provenance, Provenance::Derived);
        assert_eq!(loaded.database.as_deref(), Some("tenant/a"));
        let text = fs::read_to_string(chains.file(&db)).expect("file");
        assert!(text.contains("\"position\":\"010203\""), "{text}");
    }

    #[test]
    fn hashes_print_and_parse_as_hex() {
        let hash = ChainHash::next(&ChainHash::GENESIS, None, &Position::new([1]), b"x");
        let text = hash.to_string();
        assert_eq!(text.len(), 64);
        assert_eq!(text.parse::<ChainHash>(), Ok(hash));
        assert!("abc".parse::<ChainHash>().is_err());
        assert_eq!(ChainHash::from_bytes(hash.as_bytes()), Some(hash));
        assert_eq!(ChainHash::from_bytes(&[1, 2]), None);
    }
}
