//! Fault injection on real `fleetringd` processes, judged by the history checker (ADR-0036).
//!
//! The simulation (ADR-0020) runs the real daemon code over turmoil's network and clock. This
//! harness runs the shipped binary instead: five daemons in Docker containers on one bridge
//! network, `n1` the control node and `n2` to `n5` data nodes, every connection mutual TLS with
//! certificates the image under test issues. Clients on the host run a seeded workload of
//! creates, writes, and reads through [`fleetringd::data::Client`] while a seeded fault schedule
//! partitions nodes with `iptables`, kills and pauses them with signals, and skews the control
//! node's wall clock with `libfaketime`. After the faults heal, the harness reads every replica's
//! rows from its data directory and judges the run with FR-036's history checker
//! (`fleetringd::sim::history::check`), the convergence check, and the control log replay.
//!
//! The pure parts (node configurations, Docker command lines, durations) are always built and
//! tested; the parts that run a workload need `fleetringd`'s simulation types, behind the
//! `harness` feature.

pub mod config;
pub mod docker;
pub mod duration;
#[cfg(feature = "harness")]
pub mod inject;
pub mod node;
#[cfg(feature = "harness")]
pub mod schedule;
#[cfg(feature = "harness")]
pub mod workload;

/// Whether a run checks the fleet or checks the harness (ADR-0036, point 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Mode {
    /// Databases are created as the checker holds them: a valid run exits 0.
    #[default]
    Normal,
    /// Databases are created with `acks = local` but checked as `quorum`, and only primaries are
    /// isolated: the run exits 0 only if the checker finds a lost acknowledged write.
    SelfTest,
}

/// Whether a run builds the node image first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Build {
    /// Build the image from the workspace before the run.
    #[default]
    Image,
    /// Use the image as it is.
    Skip,
}

/// What a run leaves behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Teardown {
    /// Remove the containers and the network after the run.
    #[default]
    Remove,
    /// Leave them, for inspection.
    Keep,
}

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<Build>();
    assert_debug_send_sync::<Mode>();
    assert_debug_send_sync::<Teardown>();
    assert_debug_send_sync::<docker::Docker>();
    assert_debug_send_sync::<docker::DockerError>();
    assert_debug_send_sync::<docker::Signal>();
    assert_debug_send_sync::<duration::ParseDurationError>();
    assert_debug_send_sync::<node::Node>();
    #[cfg(feature = "harness")]
    {
        assert_debug_send_sync::<inject::AppliedFault>();
        assert_debug_send_sync::<inject::InjectError>();
        assert_debug_send_sync::<inject::Injector>();
        assert_debug_send_sync::<schedule::Fault>();
        assert_debug_send_sync::<schedule::FaultKind>();
        assert_debug_send_sync::<schedule::PlannedFault>();
        assert_debug_send_sync::<schedule::Scenario>();
        assert_debug_send_sync::<schedule::Schedule>();
        assert_debug_send_sync::<workload::Recorder>();
        assert_debug_send_sync::<workload::WorkloadError>();
    }
};
