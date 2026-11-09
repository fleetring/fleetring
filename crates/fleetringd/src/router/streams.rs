//! Hrana streams and their batons (ADR-0037, points 19 and 23).
//!
//! A baton is `base64url(stream ID ‖ 16 random bytes ‖ sequence)`, with the stream ID a
//! big-endian `u64` and the sequence a big-endian `u32`. The random bytes are the stream's
//! secret, drawn once from an injected [`Entropy`], and compared in constant time; the sequence
//! moves on with every request, so a baton is accepted once, for its stream's current request,
//! and a replayed or forked one is refused. A stream belongs to the token that opened it:
//! another principal's request is refused with `403`, as is the same principal's with another
//! token. A stream idle past its timeout expires, and dropping it closes its session, which
//! rolls back what it left open on the primary.

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
    next_id: u64,
    streams: BTreeMap<u64, Entry>,
}

/// A stream in the table, with its secret kept outside its lock, so that a baton is checked
/// before anything about the stream, even whether a request on it is running, is told.
#[derive(Debug)]
struct Entry {
    secret: [u8; SECRET_BYTES],
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
    /// [`StreamRefusal::Full`] if the router holds `max_streams` already.
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
        table.next_id += 1;
        let id = table.next_id;
        let stream = Arc::new(tokio::sync::Mutex::new(HranaStream {
            id,
            secret,
            sequence: 0,
            principal: caller.principal.clone(),
            token: caller.token_id.clone(),
            database: database.clone(),
            sql: BTreeMap::new(),
            session: None,
            autocommit: Autocommit::On,
            broken: None,
            closed: false,
            last_used: Instant::now(),
        }));
        let locked = Arc::clone(&stream)
            .try_lock_owned()
            .map_err(|_| StreamRefusal::Invalid("a new stream is already locked".to_owned()))?;
        table.streams.insert(id, Entry { secret, stream });
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
        let removed = self.lock().streams.remove(&id);
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
            ids.into_iter()
                .filter_map(|id| table.streams.remove(&id))
                .collect()
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
    use super::*;

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
