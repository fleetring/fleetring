//! The log subscriber: text or JSON lines (ADR-0034 point 8).

use tracing::Subscriber;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

use crate::LogFormat;

/// Whether text log lines carry ANSI colour escapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogColour {
    /// Coloured, for a terminal.
    Ansi,
    /// Plain, for a file or a pipe: a detached node's log, or a journal.
    Plain,
}

/// The subscriber the binary installs: lines in `format`, coloured as `colour` says, filtered by
/// `filter`, written to `writer`.
///
/// JSON is one object per line, with the event's fields, its current span, and the list of
/// spans it sits in, so every line a request causes carries the request's `rpc` span and its
/// `request_id`.
pub fn log_subscriber<W>(
    format: LogFormat,
    colour: LogColour,
    filter: EnvFilter,
    writer: W,
) -> Box<dyn Subscriber + Send + Sync>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(colour == LogColour::Ansi)
        .with_writer(writer);
    match format {
        LogFormat::Json => Box::new(
            builder
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .finish(),
        ),
        LogFormat::Text => Box::new(builder.finish()),
    }
}
