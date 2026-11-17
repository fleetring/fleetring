//! Hrana streams and their batons (ADR-0037, points 19 and 23).
//!
//! A baton is `base64url(stream ID ‖ 16 random bytes ‖ sequence)`, with the stream ID a
//! big-endian `u64` and the sequence a big-endian `u32`. The stream ID is random too, so that a
//! baton tells nothing of how many streams came before it. The random bytes are the stream's
//! secret, drawn once from an injected [`Entropy`], and compared in constant time; the sequence
//! moves on with every request, so a baton is accepted once, for its stream's current request,
//! and a replayed or forked one is refused. A stream belongs to the token that opened it:
//! another principal's request is refused with `403`, as is the same principal's with another
//! token, so a client that rotates its token mid-stream opens a new stream. A principal holds at
//! most `max_streams_per_principal` streams, so that one caller can't take every place; a
//! delegated agent's streams count under its lineage's root agent, with the root's own, so that
//! delegating never multiplies the quota. A stream idle past its timeout expires, and dropping it
//! closes its session, which rolls back what it left open on the primary.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fleetring_control::{DatabaseName, TokenId};
use tokio::sync::OwnedMutexGuard;
use tokio::time::Instant;

use crate::engine::Autocommit;
use crate::router::auth::Caller;
use crate::router::config::StreamsConfig;
use crate::router::hrana::{Error, code};
use crate::router::session::NodeSession;
use crate::tls::Principal;

/// Where batons' secrets come from: the operating system in production, a seeded generator under
/// simulation (ADR-0020).
pub trait Entropy: fmt::Debug + Send + Sync {
    /// Fills `bytes` with unpredictable bytes.
    fn fill(&self, bytes: &mut [u8]);
}

/// The operating system's randomness, through `ring`.
#[derive(Debug)]
pub struct SystemEntropy {
    random: ring::rand::SystemRandom,
}

impl SystemEntropy {
    /// The operating system's randomness.
    #[must_use]
    pub fn new() -> Self {
        Self {
            random: ring::rand::SystemRandom::new(),
        }
    }
}

impl Default for SystemEntropy {
    fn default() -> Self {
        Self::new()
    }
}

impl Entropy for SystemEntropy {
    fn fill(&self, bytes: &mut [u8]) {
        use ring::rand::SecureRandom as _;
        // A system without randomness can't make unguessable batons, so it can't serve.
        self.random
            .fill(bytes)
            .expect("the operating system provides randomness");
    }
}

/// How long a baton's secret is.
const SECRET_BYTES: usize = 16;

/// A baton, decoded.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Baton {
    stream: u64,
    secret: [u8; SECRET_BYTES],
    sequence: u32,
}

impl fmt::Debug for Baton {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Baton")
            .field("stream", &self.stream)
            .field("sequence", &self.sequence)
            .finish_non_exhaustive()
    }
}

impl Baton {
    /// The baton as the client sends it back.
    pub(crate) fn encode(&self) -> String {
        let mut bytes = Vec::with_capacity(8 + SECRET_BYTES + 4);
        bytes.extend_from_slice(&self.stream.to_be_bytes());
        bytes.extend_from_slice(&self.secret);
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// The baton `text` encodes, if it is one.
    pub(crate) fn decode(text: &str) -> Option<Self> {
        let bytes = URL_SAFE_NO_PAD.decode(text).ok()?;
        let (stream, rest) = bytes.split_first_chunk::<8>()?;
        let (secret, rest) = rest.split_first_chunk::<SECRET_BYTES>()?;
        let sequence: [u8; 4] = rest.try_into().ok()?;
        Some(Self {
            stream: u64::from_be_bytes(*stream),
            secret: *secret,
            sequence: u32::from_be_bytes(sequence),
        })
    }
}

/// Whether two secrets are equal, comparing every byte whatever the first difference, so that the
/// time it takes says nothing about where they differ.
fn same_secret(left: &[u8; SECRET_BYTES], right: &[u8; SECRET_BYTES]) -> bool {
    let difference = left
        .iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        });
    std::hint::black_box(difference) == 0
}

/// A Hrana stream: a client's requests that share one session.
#[derive(Debug)]
pub(crate) struct HranaStream {
    id: u64,
    secret: [u8; SECRET_BYTES],
    sequence: u32,
    principal: Principal,
    token: TokenId,
    pub(crate) database: DatabaseName,
    /// SQL texts stored with `store_sql`, by ID.
    pub(crate) sql: BTreeMap<i32, String>,
    /// The session on the primary, once a call has needed one.
    pub(crate) session: Option<NodeSession>,
    /// Whether a node has accepted the stream's token, by opening a session for it. The router
    /// holds no denylist, so it keeps a stream past its first request only once one has.
    pub(crate) vouched: Vouched,
    /// Whether the stream is in a transaction, as its last call left it.
    pub(crate) autocommit: Autocommit,
    /// Why the stream can't go on, once something broke it.
    pub(crate) broken: Option<Error>,
    /// Whether a `close` request closed it.
    pub(crate) closed: bool,
    last_used: Instant,
}

impl HranaStream {
    /// The baton for the stream's next request.
    pub(crate) fn baton(&self) -> String {
        Baton {
            stream: self.id,
            secret: self.secret,
            sequence: self.sequence,
        }
        .encode()
    }

    /// Moves the stream on to its next request.
    pub(crate) fn advance(&mut self) {
        self.sequence = self.sequence.wrapping_add(1);
        self.last_used = Instant::now();
    }

    /// The stream's ID.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

/// Whether a node has vouched for a stream's token by opening a session for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Vouched {
    /// No session was opened yet.
    Not,
    /// The stream's first request tried to open one, and couldn't.
    Failed,
    /// A node opened one.
    Yes,
}

/// A stream locked for one request, which has it to itself until the guard drops.
pub(crate) type Locked = OwnedMutexGuard<HranaStream>;

/// Why a baton doesn't continue a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamRefusal {
    /// `400`: the stream expired, closed, or never existed.
    Expired,
    /// `400`: the baton isn't the stream's current one, or names another database, or a request
    /// on the stream is still running.
    Invalid(String),
    /// `403`: the stream belongs to another principal or token.
    Forbidden(String),
    /// `503`: the router holds as many streams as it may.
    Full,
    /// `429`: the caller's principal holds as many streams as one principal may.
    PrincipalFull(usize),
}

impl StreamRefusal {
    /// The Hrana error a refused request gets.
    pub(crate) fn error(&self) -> Error {
        match self {
            Self::Expired => Error::new(
                code::STREAM_EXPIRED,
                "the stream expired or never existed; open a new one without a baton",
            ),
            Self::Invalid(why) => Error::new(code::STREAM_INVALID, why.clone()),
            Self::Forbidden(why) => Error::new(code::PERMISSION_DENIED, why.clone()),
            Self::Full => Error::new(
                code::RESOURCE_EXHAUSTED,
                "the router holds as many streams as it may; retry later",
            ),
            Self::PrincipalFull(max) => Error::new(
                code::RESOURCE_EXHAUSTED,
                format!(
                    "this principal, with the other agents of its lineage if it is one, holds \
                     {max} streams already, as many as [streams] max_streams_per_principal \
                     allows; close one, or let one expire"
                ),
            ),
        }
    }
}

/// Every Hrana stream the router holds.
#[derive(Debug)]
pub(crate) struct Streams {
    limits: StreamsConfig,
    table: Mutex<Table>,
}

#[derive(Debug, Default)]
struct Table {
    streams: BTreeMap<u64, Entry>,
    /// How many streams each quota principal holds ([`Caller::quota`]); one with none has no
    /// entry.
    held: BTreeMap<Principal, usize>,
}

impl Table {
    /// Removes the stream `id`, giving its quota principal's place back.
    fn remove(&mut self, id: u64) -> Option<Entry> {
        let entry = self.streams.remove(&id)?;
        if let Some(count) = self.held.get_mut(&entry.quota) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.held.remove(&entry.quota);
            }
        }
        Some(entry)
    }
}

/// A stream in the table, with its secret kept outside its lock, so that a baton is checked
/// before anything about the stream, even whether a request on it is running, is told.
#[derive(Debug)]
struct Entry {
    secret: [u8; SECRET_BYTES],
    /// The principal whose quota the stream counts against.
    quota: Principal,
    stream: Arc<tokio::sync::Mutex<HranaStream>>,
}

impl Streams {
    /// No streams, held to `limits`.
    pub(crate) fn new(limits: StreamsConfig) -> Self {
        Self {
            limits,
            table: Mutex::default(),
        }
    }

    /// A new stream for `caller` on `database`, locked for its first request.
    ///
    /// # Errors
    ///
    /// [`StreamRefusal::Full`] if the router holds `max_streams` already, and
    /// [`StreamRefusal::PrincipalFull`] if the caller's quota principal holds
    /// `max_streams_per_principal`.
    pub(crate) fn create(
        &self,
        caller: &Caller,
        database: &DatabaseName,
        entropy: &dyn Entropy,
    ) -> Result<Locked, StreamRefusal> {
        let mut secret = [0; SECRET_BYTES];
        entropy.fill(&mut secret);
        let mut table = self.lock();
        if table.streams.len() >= self.limits.max_streams.get() {
            return Err(StreamRefusal::Full);
        }
        let per_principal = self.limits.max_streams_per_principal.get();
        if table.held.get(&caller.quota).copied().unwrap_or(0) >= per_principal {
            return Err(StreamRefusal::PrincipalFull(per_principal));
        }
        let id = loop {
            let mut id = [0; 8];
            entropy.fill(&mut id);
            let id = u64::from_be_bytes(id);
            if !table.streams.contains_key(&id) {
                break id;
            }
        };
        let stream = Arc::new(tokio::sync::Mutex::new(HranaStream {
            id,
            secret,
            sequence: 0,
            principal: caller.principal.clone(),
            token: caller.token_id.clone(),
            database: database.clone(),
            sql: BTreeMap::new(),
            session: None,
            vouched: Vouched::Not,
            autocommit: Autocommit::On,
            broken: None,
            closed: false,
            last_used: Instant::now(),
        }));
        let locked = Arc::clone(&stream)
            .try_lock_owned()
            .map_err(|_| StreamRefusal::Invalid("a new stream is already locked".to_owned()))?;
        *table.held.entry(caller.quota.clone()).or_default() += 1;
        table.streams.insert(
            id,
            Entry {
                secret,
                quota: caller.quota.clone(),
                stream,
            },
        );
        Ok(locked)
    }

    /// The stream `baton` continues, locked for `caller`'s request on `database`.
    ///
    /// # Errors
    ///
    /// [`StreamRefusal`] unless the baton is the stream's current one, from the token that
    /// opened it, for the same database, and no other request on it is running.
    pub(crate) fn resume(
        &self,
        baton: &str,
        caller: &Caller,
        database: &DatabaseName,
    ) -> Result<Locked, StreamRefusal> {
        let baton = Baton::decode(baton).ok_or(StreamRefusal::Expired)?;
        let stream = {
            let table = self.lock();
            let entry = table
                .streams
                .get(&baton.stream)
                .ok_or(StreamRefusal::Expired)?;
            // A forged or guessed baton learns nothing: it reads as an expired stream's.
            if !same_secret(&entry.secret, &baton.secret) {
                return Err(StreamRefusal::Expired);
            }
            Arc::clone(&entry.stream)
        };
        let locked = stream.try_lock_owned().map_err(|_| {
            StreamRefusal::Invalid(
                "a request on this stream is still running; send the next one after its answer"
                    .to_owned(),
            )
        })?;
        if locked.principal != caller.principal {
            return Err(StreamRefusal::Forbidden(
                "this stream belongs to another principal".to_owned(),
            ));
        }
        if locked.token != caller.token_id {
            return Err(StreamRefusal::Forbidden(
                "continue a stream with the token that opened it".to_owned(),
            ));
        }
        if locked.database != *database {
            return Err(StreamRefusal::Invalid(format!(
                "this stream is on database '{}'",
                locked.database
            )));
        }
        if locked.sequence != baton.sequence || locked.closed {
            return Err(StreamRefusal::Invalid(
                "the baton isn't the stream's current one: send the baton of the last answer"
                    .to_owned(),
            ));
        }
        Ok(locked)
    }

    /// Removes the stream `id`, which closes its session.
    pub(crate) fn remove(&self, id: u64) {
        let removed = self.lock().remove(id);
        drop(removed);
    }

    /// Removes every stream idle longer than the timeout, closing its session. A stream a
    /// request is using isn't idle.
    pub(crate) fn expire(&self) {
        let idle = self.limits.idle_timeout();
        let now = Instant::now();
        let expired: Vec<Entry> = {
            let mut table = self.lock();
            let ids: Vec<u64> = table
                .streams
                .iter()
                .filter(|(_, entry)| {
                    entry
                        .stream
                        .try_lock()
                        .is_ok_and(|stream| now.saturating_duration_since(stream.last_used) >= idle)
                })
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter().filter_map(|id| table.remove(id)).collect()
        };
        if !expired.is_empty() {
            tracing::debug!(count = expired.len(), "expired idle Hrana streams");
        }
        // Their sessions close as they drop, outside the lock.
        drop(expired);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Table> {
        self.table.lock().expect("stream table lock poisoned")
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    #[derive(Debug)]
    struct Counting(std::sync::atomic::AtomicU8);

    impl Entropy for Counting {
        fn fill(&self, bytes: &mut [u8]) {
            let next = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            bytes.fill(next);
        }
    }

    /// A root agent and every agent of its lineage share one per-principal quota, so minting
    /// children never multiplies the streams a lineage holds; another root still opens its own.
    #[tokio::test]
    async fn a_lineage_shares_one_stream_quota() {
        let streams = Streams::new(StreamsConfig {
            max_streams_per_principal: NonZeroUsize::new(2).expect("nonzero"),
            ..StreamsConfig::default()
        });
        let entropy = Counting(std::sync::atomic::AtomicU8::new(1));
        let database: DatabaseName = "agent-42-notes".parse().expect("a name");
        let root = Caller::verified_for_tests("agent-42", "agent-42", "t-root");
        let c7 = Caller::verified_for_tests("agent-42.c7", "agent-42", "t-c7");
        let c8 = Caller::verified_for_tests("agent-42.c8", "agent-42", "t-c8");
        let other = Caller::verified_for_tests("agent-43.c1", "agent-43", "t-43");
        let first = streams
            .create(&root, &database, &entropy)
            .expect("the root's");
        let second = streams.create(&c7, &database, &entropy).expect("a child's");
        assert_eq!(
            streams.create(&c8, &database, &entropy).map(|_| ()),
            Err(StreamRefusal::PrincipalFull(2)),
            "a second child counts against the same quota"
        );
        let theirs = streams
            .create(&other, &database, &entropy)
            .expect("another lineage's");
        // A stream that goes gives the lineage's place back.
        let id = second.id();
        drop(second);
        streams.remove(id);
        streams
            .create(&c8, &database, &entropy)
            .expect("a place again");
        drop((first, theirs));
    }

    #[test]
    fn batons_round_trip_and_reject_garbage() {
        let baton = Baton {
            stream: 7,
            secret: [3; SECRET_BYTES],
            sequence: 9,
        };
        let text = baton.encode();
        assert_eq!(text.len(), 38, "28 bytes, unpadded");
        assert_eq!(Baton::decode(&text), Some(baton));
        for garbage in ["", "!!", "AAAA", &text[..30]] {
            assert_eq!(Baton::decode(garbage), None, "{garbage}");
        }
    }
}
