//! The bounds on what one MCP call takes and returns (ADR-0041, point 9).

use std::time::Duration;

/// How many bytes a result's JSON holds unless `--max-result-bytes` says otherwise.
pub const DEFAULT_MAX_RESULT_BYTES: usize = 256 * 1024;

/// The most `--max-result-bytes` may be.
pub const MAX_MAX_RESULT_BYTES: usize = 4 * 1024 * 1024;

/// The least `--max-result-bytes` may be: enough for a database's metadata at its largest (a
/// 1 KiB description and 32 labels, about 13 KiB) with its placement, so that only rows, listed
/// databases, and schema objects ever need dropping.
pub const MIN_MAX_RESULT_BYTES: usize = 64 * 1024;

/// How many rows `query` returns unless its `max_rows` says otherwise.
pub const DEFAULT_MAX_ROWS: u32 = 200;

/// The most rows `query` returns, whatever its `max_rows` says.
pub const MAX_ROWS: u32 = 5_000;

/// The longest text or blob a result carries whole; a longer one becomes
/// `{"elided_bytes": <n>}`.
pub const MAX_VALUE_BYTES: usize = 16 * 1024;

/// The longest SQL `query` and `exec` take, in bytes.
pub const MAX_SQL_BYTES: usize = 100 * 1024;

/// The most parameters `query` and `exec` take.
pub const MAX_PARAMS: usize = 1_000;

/// How many databases `db_list` returns unless its `limit` says otherwise.
pub const DEFAULT_LIST_LIMIT: u32 = 100;

/// The most databases `db_list` returns, whatever its `limit` says.
pub const MAX_LIST_LIMIT: u32 = 1_000;

/// How many rows `db_show` counts in each table unless its `count_limit` says otherwise, as
/// `db show` does.
pub const DEFAULT_COUNT_LIMIT: u64 = 10_000;

/// How long one tool call may take, from the request to the reply, before it fails with
/// `deadline_exceeded`. A write that times out may still have taken effect.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// How many transactions `changes` returns unless its `limit` says otherwise.
pub const DEFAULT_CHANGES_LIMIT: u32 = 20;

/// The most transactions `changes` returns, whatever its `limit` says.
pub const MAX_CHANGES_LIMIT: u32 = 100;

/// The longest `changes` waits for transactions once its stream is open. It returns sooner
/// once it holds `limit` of them, or once [`CHANGES_QUIET`] passes after the last one, so a call
/// that finds nothing new returns after this long with no transactions.
pub const CHANGES_WAIT: Duration = Duration::from_secs(2);

/// How long `changes` waits for another transaction after one arrives before it returns what
/// it holds: a backlog arrives at once, so a pause this long means it has caught up.
pub const CHANGES_QUIET: Duration = Duration::from_millis(250);

/// The longest position `changes` takes as `after`, in bytes: more than any engine's.
pub const MAX_POSITION_BYTES: usize = 64;
