//! `fleetringd mcp`: an MCP server over stdio, acting as one principal (ADR-0041, point 9).
//!
//! The server speaks the Model Context Protocol as newline-delimited JSON-RPC on stdin and
//! stdout, so stdout carries the protocol and nothing else; logs go to stderr. It offers eight
//! tools (`db_create`, `db_list`, `db_show`, `query`, `changes`, `exec`, `db_update`, and
//! `db_drop`), or the four that read with [`Access::ReadOnly`].
//!
//! - **One principal.** Every tool call goes through a [`Client`], as every command's calls
//!   do, so the nodes authorize it against the principal the server's credentials name. A
//!   client or a model can't name another principal or pass a credential: inputs refuse
//!   unknown fields, and nothing in them is a credential.
//! - **Untrusted data.** Text that principals wrote (descriptions, labels, schema names and
//!   definitions, row values, a statement's error message) reaches a result only as the value
//!   of a JSON string field. Tool names, tool descriptions, and the server's instructions are
//!   fixed text. A description carries its author, and is marked untrusted unless the server's
//!   own principal wrote it; a description with no recorded author, such as a restored one, is
//!   always untrusted. A result that holds an untrusted description or any row values carries a
//!   fixed notice saying to read them as data.
//! - **Bounds.** A result's JSON is at most `max_result_bytes`, with rows, listed databases, or
//!   schema objects dropped from the end and `truncated` set; a query stops at `max_rows` on the
//!   primary; `changes` returns at most [`MAX_CHANGES_LIMIT`] transactions and waits at most
//!   [`CHANGES_WAIT`] for them; a long value is elided; a statement runs on the primary for at
//!   most [`STATEMENT_TIMEOUT`], its call's deadline; and a call takes at most [`CALL_TIMEOUT`].
//!
//! `rmcp`, the protocol library, is used in this module only, and pinned exactly, so that
//! upgrading it is a change to this module alone.

mod limits;
mod render;
mod server;
mod tools;

use fleetring_control::PrincipalName;
use rmcp::ServiceExt as _;

pub use limits::{
    CALL_TIMEOUT, CHANGES_QUIET, CHANGES_WAIT, DEFAULT_CHANGES_LIMIT, DEFAULT_COUNT_LIMIT,
    DEFAULT_LIST_LIMIT, DEFAULT_MAX_RESULT_BYTES, DEFAULT_MAX_ROWS, MAX_CHANGES_LIMIT,
    MAX_CHANGES_ROWS, MAX_CONCURRENT_CHANGES, MAX_LIST_LIMIT, MAX_MAX_RESULT_BYTES, MAX_PARAMS,
    MAX_POSITION_BYTES, MAX_ROWS, MAX_SQL_BYTES, MAX_VALUE_BYTES, MIN_MAX_RESULT_BYTES,
    STATEMENT_TIMEOUT,
};

use crate::data::Client;

/// Which tools the server offers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Access {
    /// Every tool.
    #[default]
    ReadWrite,
    /// Only the tools that change nothing: `db_list`, `db_show`, `query`, and `changes`. The
    /// principal's scopes still decide what those may read.
    ReadOnly,
}

/// How the server runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSettings {
    /// Which tools it offers.
    pub access: Access,
    /// The most bytes a result's JSON holds, between [`MIN_MAX_RESULT_BYTES`] and
    /// [`MAX_MAX_RESULT_BYTES`]; a value outside is taken as the nearer bound.
    pub max_result_bytes: usize,
    /// The principal the server's credentials name, read from them without verifying, as the
    /// command line reads it: only a description this principal wrote is trusted. `None` when
    /// it can't be read, and then no description is trusted.
    pub principal: Option<PrincipalName>,
}

impl Default for McpSettings {
    fn default() -> Self {
        Self {
            access: Access::default(),
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            principal: None,
        }
    }
}

/// Why the server stopped other than by its client closing stdin.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum McpError {
    /// The client didn't open the session: it sent something other than `initialize` first,
    /// or closed stdin before it did.
    #[error("the MCP client didn't initialize a session: {0}")]
    Initialize(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The session's task failed.
    #[error("the MCP session failed: {0}")]
    Session(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Serves MCP on stdin and stdout until the client closes stdin, acting through `client`.
///
/// # Errors
///
/// [`McpError::Initialize`] if the client doesn't open a session, and [`McpError::Session`] if
/// the session's task fails.
pub async fn serve(client: Client, settings: McpSettings) -> Result<(), McpError> {
    let settings = McpSettings {
        max_result_bytes: settings
            .max_result_bytes
            .clamp(MIN_MAX_RESULT_BYTES, MAX_MAX_RESULT_BYTES),
        ..settings
    };
    let service = server::FleetringMcp::new(client, settings)
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|error| McpError::Initialize(error.into()))?;
    let reason = service
        .waiting()
        .await
        .map_err(|error| McpError::Session(error.into()))?;
    tracing::debug!(?reason, "the MCP session ended");
    Ok(())
}
