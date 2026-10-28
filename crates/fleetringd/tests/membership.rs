//! FR-029: membership configuration, and daemons that converge on one ring over real TCP.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use fleetring::Fingerprint;
use fleetring_control::Liveness;
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::{JoinRequest, Node};
use fleetringd::transport::control_client;
use fleetringd::{Config, ConfigError, Daemon, Membership};
use tonic::Code;
use tonic::transport::Endpoint;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long convergence takes.
const HANG_GUARD: Duration = Duration::from_secs(60);

fn config(dir: &Path, id: &str, role: &str, control: Option<SocketAddr>) -> String {
    let control = control.map_or_else(String::new, |address| {
        format!("\n[control]\naddress = \"{address}\"\n")
    });
    format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         \n[membership]\nheartbeat_interval_ms = 100\n{control}",
        dir.join(id)
    )
}

/// Waits until `membership` publishes `epoch` with `fingerprint` and every member live.
async fn converge(
    membership: &Membership,
    epoch: u64,
    fingerprint: Fingerprint,
) -> Result<(), Box<dyn Error>> {
    let view = tokio::time::timeout(
        HANG_GUARD,
        membership.wait_for(|view| {
            view.epoch().get() == epoch
                && view.fingerprint() == Some(fingerprint)
                && view
                    .members()
                    .all(|(_, liveness)| liveness == Liveness::Live)
        }),
    )
    .await?
    .ok_or("membership stopped")?;
    let ring = membership.ring().ok_or("no ring")?;
    assert_eq!(ring.fingerprint(), fingerprint);
    assert_eq!(ring.epoch().get(), epoch);
    assert_eq!(view.members().len(), ring.node_count());
    Ok(())
}

#[test]
fn the_heartbeat_interval_must_fit_the_heartbeat_timeout_three_times() -> TestResult {
    let base =
        "data_dir = \"/tmp/fleetring-test\"\nlisten = \"127.0.0.1:7400\"\n\n[node]\nid = \"a\"\n";
    let defaults = Config::from_toml(base)?;
    assert_eq!(defaults.membership.heartbeat_interval_ms.get(), 1_000);
    assert_eq!(
        defaults.membership.heartbeat_interval(),
        Duration::from_secs(1)
    );

    let fits = format!(
        "{base}\n[control]\nheartbeat_timeout_ms = 900\n\n[membership]\nheartbeat_interval_ms = 300\n"
    );
    Config::from_toml(&fits)?;

    let too_slow = fits.replace("= 300", "= 301");
    let error = Config::from_toml(&too_slow).expect_err("interval too long");
    assert!(
        matches!(
            error,
            ConfigError::Invalid {
                field: "membership.heartbeat_interval_ms",
                ..
            }
        ),
        "{error}"
    );
    assert!(error.to_string().contains("301 ms"), "{error}");

    // A control-only node sends no heartbeats, so the interval doesn't matter there.
    let control_only = too_slow.replace("listen", "role = \"control\"\nlisten");
    Config::from_toml(&control_only)?;

    let zero = fits.replace("= 300", "= 0");
    let error = Config::from_toml(&zero).expect_err("zero interval");
    assert!(matches!(error, ConfigError::Parse(_)), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_daemon_with_both_roles_joins_its_own_control_node() -> TestResult {
    let dir = tempfile::tempdir()?;
    let daemon = Daemon::start(Config::from_toml(&config(
        dir.path(),
        "solo",
        "both",
        None,
    ))?)
    .await?;
    let membership = daemon.membership().ok_or("no membership")?;
    let control = daemon.control().ok_or("no control")?;
    let mut states = control.watch();
    let state = tokio::time::timeout(
        HANG_GUARD,
        states.wait_for(|state| state.membership_epoch().get() == 1),
    )
    .await??
    .clone();
    converge(&membership, 1, state.ring()?.fingerprint()).await?;
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_daemon_and_two_data_daemons_converge_on_one_ring() -> TestResult {
    let dir = tempfile::tempdir()?;
    let control = Daemon::start(Config::from_toml(&config(
        dir.path(),
        "control-1",
        "control",
        None,
    ))?)
    .await?;
    assert!(control.membership().is_none());
    let address = control.control_address().ok_or("no control address")?;
    let mut data = Vec::new();
    for id in ["data-1", "data-2"] {
        let text = config(dir.path(), id, "data", Some(address));
        let daemon = Daemon::start(Config::from_toml(&text)?).await?;
        assert!(daemon.control().is_none() && daemon.control_address().is_none());
        data.push(daemon);
    }

    let handle = control.control().ok_or("no control handle")?;
    let mut states = handle.watch();
    let state = tokio::time::timeout(
        HANG_GUARD,
        states.wait_for(|state| state.membership_epoch().get() == 2),
    )
    .await??
    .clone();
    let fingerprint = state.ring()?.fingerprint();
    for daemon in &data {
        converge(&daemon.membership().ok_or("no membership")?, 2, fingerprint).await?;
    }

    // A data-only daemon serves on `listen` with no services yet, so a call reaches it and is
    // refused as unimplemented.
    let peer = format!("http://{}", data[0].listen_address());
    let mut client = control_client(Endpoint::from_shared(peer)?.connect().await?);
    let refused = client
        .join(JoinRequest {
            protocol: Some(PROTOCOL_VERSION),
            node: Some(Node {
                id: "x".into(),
                zone: None,
                host: None,
                weight: 1,
                address: None,
            }),
        })
        .await
        .expect_err("a data-only node serves no control service");
    assert_eq!(refused.code(), Code::Unimplemented, "{refused}");

    for daemon in data {
        daemon.run_until(async {}).await;
    }
    control.run_until(async {}).await;
    Ok(())
}
