//! Timing parameters fixed when a control state is created.

use serde::{Deserialize, Serialize};

use crate::Millis;

/// How long a lease lasts after it is granted or renewed, by default.
const DEFAULT_LEASE_DURATION: Millis = Millis::new(10_000);

/// How long past a lease's expiry the control plane waits before failing over, by default.
const DEFAULT_FAILOVER_MARGIN: Millis = Millis::new(2_000);

/// How long a member stays live after its last heartbeat, by default.
const DEFAULT_HEARTBEAT_TIMEOUT: Millis = Millis::new(5_000);

/// The timing rules of a control state: lease length, failover margin, and heartbeat timeout.
///
/// The configuration is part of [`State`](crate::State) and travels in its snapshots, so a
/// replayed log always runs under the rules it was recorded with. None of these values affects
/// safety (ADR-0018, point 6); they only decide when failover may start.
///
/// # Examples
///
/// ```
/// use fleetring_control::{ControlConfig, Millis};
///
/// let config = ControlConfig::new()
///     .with_lease_duration(Millis::new(3_000))
///     .with_failover_margin(Millis::new(500));
/// assert_eq!(config.lease_duration(), Millis::new(3_000));
/// assert_eq!(config.heartbeat_timeout(), ControlConfig::default().heartbeat_timeout());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlConfig {
    lease_duration: Millis,
    failover_margin: Millis,
    heartbeat_timeout: Millis,
}

impl ControlConfig {
    /// Returns the default configuration: 10 s leases, a 2 s failover margin, and a 5 s heartbeat
    /// timeout.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            lease_duration: DEFAULT_LEASE_DURATION,
            failover_margin: DEFAULT_FAILOVER_MARGIN,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
        }
    }

    /// Sets how long a lease lasts after it is granted or renewed.
    #[must_use]
    pub const fn with_lease_duration(mut self, lease_duration: Millis) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    /// Sets how long past a lease's expiry a promotion must wait.
    #[must_use]
    pub const fn with_failover_margin(mut self, failover_margin: Millis) -> Self {
        self.failover_margin = failover_margin;
        self
    }

    /// Sets how long a member stays live after its last heartbeat.
    #[must_use]
    pub const fn with_heartbeat_timeout(mut self, heartbeat_timeout: Millis) -> Self {
        self.heartbeat_timeout = heartbeat_timeout;
        self
    }

    /// Returns how long a lease lasts after it is granted or renewed.
    #[must_use]
    pub const fn lease_duration(&self) -> Millis {
        self.lease_duration
    }

    /// Returns how long past a lease's expiry a promotion must wait.
    #[must_use]
    pub const fn failover_margin(&self) -> Millis {
        self.failover_margin
    }

    /// Returns how long a member stays live after its last heartbeat.
    #[must_use]
    pub const fn heartbeat_timeout(&self) -> Millis {
        self.heartbeat_timeout
    }
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self::new()
    }
}
