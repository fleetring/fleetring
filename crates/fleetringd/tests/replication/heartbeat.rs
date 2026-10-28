//! Heartbeats report each hosted database, and a database the control plane doesn't know
//! doesn't make the node rejoin.

use std::time::Duration;

use fleetring_control::{Member, Role};
use fleetring_proto::v1::{self as proto, CreateDatabaseRequest, DatabaseStatus, Engine};
use fleetringd::{Config, Daemon, DatabaseStatuses, Membership, MembershipSettings, Network};

use crate::common::{TestResult, eventually, id, name};

/// Reports one database the control plane knows and one it doesn't.
#[derive(Debug)]
struct Hosted;

impl DatabaseStatuses for Hosted {
    fn statuses(&self) -> Vec<DatabaseStatus> {
        ["ghost", "known"]
            .into_iter()
            .map(|database| DatabaseStatus {
                database: database.to_owned(),
                role: proto::Role::Primary.into(),
                epoch: 1,
                position: vec![0, 7],
            })
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeats_report_databases_and_leave_out_unknown_ones() -> TestResult {
    let dir = tempfile::tempdir()?;
    let control = Daemon::start(Config::from_toml(&format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control\"\n",
        dir.path().join("control")
    ))?)
    .await?;
    let address = control
        .control_address()
        .ok_or("no control address")?
        .to_string();
    let handle = control.control().ok_or("no control handle")?;
    let network = Network::default();
    let settings = MembershipSettings {
        heartbeat_interval_ms: std::num::NonZeroU64::new(50).ok_or("nonzero")?,
    };
    let (_membership, _tasks) = Membership::start_reporting(
        Member::new(id("a")),
        network.control_client(address.clone()),
        &settings,
        Hosted,
    );
    let node = id("a");
    eventually("the node joining", || {
        handle.state().member(&node).is_some()
    })
    .await?;
    network
        .control_client(address)
        .create_database(CreateDatabaseRequest {
            database: "known".to_owned(),
            replication_factor: 1,
            engine: Engine::Sqlite.into(),
        })
        .await?;
    let known = name("known");
    eventually("the report arriving", || {
        handle.state().report(&node, &known).is_some()
    })
    .await?;
    let state = handle.state();
    let report = state.report(&node, &known).ok_or("no report")?;
    assert_eq!(report.role(), Role::Primary);
    assert_eq!(report.position().as_bytes(), [0, 7]);
    assert!(state.report(&node, &name("ghost")).is_none());
    let joined_at = state.membership_epoch();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        handle.state().membership_epoch(),
        joined_at,
        "the node never rejoined"
    );
    Ok(())
}
