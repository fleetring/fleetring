//! The control group's commands (FR-064 PR 5, ADR-0035 point 11, ADR-0040 point 9): the group
//! RPCs, who may make them, and the leader they need.

use std::error::Error;
use std::time::Duration;

use fleetring_proto::v1::{
    AddControlNodeRequest, RemoveControlNodeRequest, SetControlVotersRequest,
};
use fleetringd::control::{ControlOptions, ControlService};
use fleetringd::dev::DevMode;
use tonic::Code;

use crate::raft_network::{address, handle, start, start_plaintext, stop, until};
use crate::support::Tls;
use crate::{TestResult, secure_client, serve_service};

/// The group RPCs need the `fleet` scope: a principal without it is refused, and so is a node
/// certificate, so a stolen node key can't add a node or change the voters; an operator with
/// `fleet` reads the group.
#[tokio::test]
async fn group_calls_need_the_fleet_scope() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let c1 = start(dir.path(), &tls, "c1", "new", "", None).await?;
    let at = address(&c1);
    let app = tls.client_network("app")?.control_client(at.clone());
    let node = tls.node_network("c2")?.control_client(at.clone());
    for (who, client) in [("a principal without fleet", &app), ("a node", &node)] {
        let refused = client.describe_control_group().await.expect_err("refused");
        assert_eq!(refused.code(), Code::PermissionDenied, "{who}: {refused:?}");
        let refused = client
            .add_control_node(AddControlNodeRequest {
                node_id: "c2".into(),
                address: "127.0.0.1:1".into(),
            })
            .await
            .expect_err("refused");
        assert_eq!(refused.code(), Code::PermissionDenied, "{who}: {refused:?}");
        let refused = client
            .set_control_voters(SetControlVotersRequest {
                voters: vec!["c2".into()],
            })
            .await
            .expect_err("refused");
        assert_eq!(refused.code(), Code::PermissionDenied, "{who}: {refused:?}");
    }
    let operator = secure_client(c1.listen_address(), &tls)?;
    let group = operator.describe_control_group().await?;
    assert_eq!(group.leader, "c1");
    assert_eq!(group.nodes.len(), 1, "{group:?}");
    assert!(group.nodes[0].voter && group.nodes[0].leader, "{group:?}");
    assert!(group.term >= 1, "{group:?}");
    assert!(group.committed_index >= 1, "{group:?}");
    // The leader refuses an even count, and more than seven, before it changes anything.
    for voters in [
        vec!["c1", "c2"],
        vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"],
    ] {
        let refused = operator
            .set_control_voters(SetControlVotersRequest {
                voters: voters.iter().map(|voter| (*voter).to_owned()).collect(),
            })
            .await
            .expect_err("refused");
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
    }
    stop([c1]).await;
    Ok(())
}

/// A dev mode control node refuses to add a control node whose address isn't loopback, so its
/// control log never leaves the machine (ADR-0040 point 9).
#[tokio::test]
async fn a_dev_node_refuses_a_non_loopback_control_node() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let node = crate::support::open_control(
        dir.path().join("control.db"),
        ControlOptions::default(),
        fleetringd::clock::SystemClock,
    )
    .await?;
    let service = ControlService::new(node.handle()).with_dev_mode(DevMode::On);
    let control = crate::client(serve_service(service).await?);
    for address in ["10.0.0.9:7400", "c2.fleet.internal:7400"] {
        let refused = control
            .add_control_node(AddControlNodeRequest {
                node_id: "c2".into(),
                address: address.into(),
            })
            .await
            .expect_err("refused");
        assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
        assert!(
            refused.message().contains("loopback"),
            "{}",
            refused.message()
        );
    }
    assert_eq!(node.handle().group().voters().len(), 1);
    node.shutdown().await;
    Ok(())
}

/// Each change answers with the group after it: an add names the new learner, and a removal
/// leaves it out; neither leaves the membership joint.
#[tokio::test]
async fn add_and_remove_answer_with_the_group_after() -> TestResult {
    let dir = tempfile::tempdir()?;
    let c1 = start_plaintext(dir.path(), "c1", "new").await?;
    let c2 = start_plaintext(dir.path(), "c2", "join").await?;
    let control = crate::client(c1.listen_address());
    let added = control
        .add_control_node(AddControlNodeRequest {
            node_id: "c2".into(),
            address: address(&c2),
        })
        .await?;
    let learner = added.nodes.iter().find(|node| node.node_id == "c2");
    assert!(learner.is_some_and(|node| !node.voter), "{added:?}");
    assert_eq!(added.leader, "c1");
    let removed = control
        .remove_control_node(RemoveControlNodeRequest {
            node_id: "c2".into(),
        })
        .await?;
    assert_eq!(removed.nodes.len(), 1, "{removed:?}");
    assert!(!removed.joint, "{removed:?}");
    let described = control.describe_control_group().await?;
    assert_eq!(described.nodes.len(), 1, "{described:?}");
    stop([c1, c2]).await;
    Ok(())
}

/// A change whose caller goes away runs to its end: the leader adds the node although the call
/// was cancelled while the probe crossed a slow link.
#[tokio::test]
async fn a_cancelled_group_change_runs_to_its_end() -> TestResult {
    let dir = tempfile::tempdir()?;
    let c1 = start_plaintext(dir.path(), "c1", "new").await?;
    let c2 = start_plaintext(dir.path(), "c2", "join").await?;
    // Each round trip to `c2` through the proxy takes at least 400 ms.
    let slow = crate::delaying_proxy(c2.listen_address(), Duration::from_millis(200)).await?;
    let mut raw = fleetring_proto::v1::control_client::ControlClient::connect(format!(
        "http://{}",
        c1.listen_address()
    ))
    .await?;
    let cancelled = tokio::time::timeout(
        Duration::from_millis(300),
        raw.add_control_node(AddControlNodeRequest {
            node_id: "c2".into(),
            address: slow.to_string(),
        }),
    )
    .await;
    assert!(cancelled.is_err(), "the call outran its cancellation");
    until("c1 adds c2", || handle(&c1).group().contains("c2")).await?;
    stop([c1, c2]).await;
    Ok(())
}
