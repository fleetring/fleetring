//! How current a control node's own applied state is, for the tokens it checks against it
//! (ADR-0047, invariant I5's precondition for control nodes).
//!
//! A data node checks tokens against the denylist its heartbeats bring, and refuses every token
//! once that list is older than the heartbeat timeout (ADR-0026, point 6). A control node checks
//! them against its own applied state instead. With one writer, that state is the fleet's, so it
//! is always current. In a replicated control group, a member cut off from the others would hold
//! stale state indefinitely, so its freshness is counted from the last acknowledgement: a
//! leader's from the last time a quorum acknowledged it, a follower's from the last append or
//! heartbeat it accepted from the current leader.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use tokio::time::Instant;

/// How current the control state a node checks tokens against is.
#[derive(Debug, Clone)]
pub enum ControlFreshness {
    /// The node is the fleet's only control writer, so the state it applied is the fleet's and
    /// always current. Every 0.8 control node is one.
    SoleWriter,
    /// The node is a member of a replicated control group: its state is current while `last`
    /// was acknowledged at most `max_age` ago, and every token is refused with `UNAVAILABLE`
    /// after that, as on a data node whose denylist is stale.
    Acknowledged {
        /// When the group last acknowledged this node.
        last: LastAcknowledged,
        /// How old the last acknowledgement may be: the heartbeat timeout.
        max_age: Duration,
    },
}

/// Whether the control state may be trusted for tokens now, by [`ControlFreshness`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Currency {
    /// Current: check tokens against it.
    Current,
    /// Never acknowledged since the node started.
    Unacknowledged,
    /// Acknowledged longer ago than allowed.
    Stale,
}

impl ControlFreshness {
    /// Whether the state is current enough to check tokens against now.
    pub(crate) fn currency(&self) -> Currency {
        match self {
            Self::SoleWriter => Currency::Current,
            Self::Acknowledged { last, max_age } => match last.age() {
                None => Currency::Unacknowledged,
                Some(age) if age > *max_age => Currency::Stale,
                Some(_) => Currency::Current,
            },
        }
    }
}

/// When a control node was last acknowledged by its group, shared between whatever records the
/// acknowledgements and the request boundary that reads them.
///
/// It reads time from `tokio::time`, as [`Denylist`](crate::auth::Denylist) does, so a simulated
/// fleet controls it. Cloning is cheap; clones share the time.
#[derive(Debug, Clone, Default)]
pub struct LastAcknowledged {
    at: Arc<ArcSwapOption<Instant>>,
}

impl LastAcknowledged {
    /// No acknowledgement yet: every token is refused until the first.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an acknowledgement now.
    pub fn record(&self) {
        self.at.store(Some(Arc::new(Instant::now())));
    }

    /// How long ago the last acknowledgement was recorded, or `None` if none was.
    #[must_use]
    pub fn age(&self) -> Option<Duration> {
        self.at.load().as_deref().map(Instant::elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn an_acknowledged_state_is_current_until_its_last_acknowledgement_ages_out() {
        let last = LastAcknowledged::new();
        let freshness = ControlFreshness::Acknowledged {
            last: last.clone(),
            max_age: Duration::from_secs(5),
        };
        assert_eq!(freshness.currency(), Currency::Unacknowledged);
        last.record();
        assert_eq!(freshness.currency(), Currency::Current);
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(freshness.currency(), Currency::Current, "at the bound");
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(freshness.currency(), Currency::Stale);
        last.record();
        assert_eq!(freshness.currency(), Currency::Current);
    }

    #[tokio::test(start_paused = true)]
    async fn a_sole_writer_is_current_however_long_nothing_happens() {
        let freshness = ControlFreshness::SoleWriter;
        assert_eq!(freshness.currency(), Currency::Current);
        tokio::time::advance(Duration::from_secs(365 * 24 * 3600)).await;
        assert_eq!(freshness.currency(), Currency::Current);
    }
}
