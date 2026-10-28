//! How a data node runs its open databases.

use std::num::NonZeroUsize;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

const DEFAULT_MAX_OPEN: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(n) => n,
    None => panic!("256 is not zero"),
};

const DEFAULT_MAILBOX: NonZeroUsize = match NonZeroUsize::new(64) {
    Some(n) => n,
    None => panic!("64 is not zero"),
};

/// The `[engine]` section of the daemon's configuration.
///
/// ```toml
/// [engine]
/// execution = "threads"
/// max_open = 256
/// idle_timeout_secs = 300
/// mailbox = 64
/// page_size = 4096
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EngineConfig {
    /// Where database operations run.
    pub execution: Execution,
    /// The most databases open at once. Opening one more closes the least recently used.
    pub max_open: NonZeroUsize,
    /// How long an open database waits for a request before it closes.
    #[serde(rename = "idle_timeout_secs", deserialize_with = "seconds")]
    pub idle_timeout: Duration,
    /// How many requests may wait in each open database's mailbox.
    pub mailbox: NonZeroUsize,
    /// The SQLite page size for new databases, fixed at creation: a power of two from 512 to
    /// 65536.
    pub page_size: u32,
}

impl EngineConfig {
    /// The defaults: dedicated threads, 256 open databases, a five-minute idle timeout, 64
    /// queued requests, and 4096-byte pages.
    #[must_use]
    pub fn new() -> Self {
        Self {
            execution: Execution::Threads,
            max_open: DEFAULT_MAX_OPEN,
            idle_timeout: Duration::from_secs(300),
            mailbox: DEFAULT_MAILBOX,
            page_size: 4096,
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Where an open database's blocking engine calls run (ADR-0013, ADR-0020).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Execution {
    /// On one dedicated thread per open database, off the async executor.
    #[default]
    Threads,
    /// Inline on the database's task, so a simulation stays on one deterministic thread.
    Inline,
}

fn seconds<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    u64::deserialize(deserializer).map(Duration::from_secs)
}
