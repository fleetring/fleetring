//! The control group's commands (FR-064 PR 5, ADR-0035 point 11, ADR-0040 point 9): the group
//! RPCs, who may make them, and the leader they need.

use std::error::Error;
use std::path::Path;
use std::time::Duration;

use fleetring_client::redirect::leader_hint;
use fleetring_control::fork::OperationId;
use fleetring_control::{Action, Feature, Member};
use fleetring_proto::v1::{
    AddControlNodeRequest, ForkDatabaseRequest, GetForkOperationRequest, LeaveRequest,
    ListDatabasesRequest, ListDelegationsRequest, LocateRequest, RemoveControlNodeRequest,
    SetControlVotersRequest,
};
use fleetringd::control::{ControlClient, ControlOptions, ControlService};
use fleetringd::dev::DevMode;
use fleetringd::transport::TransportConfig;
use fleetringd::{Config, Daemon};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::{Code, Status};

use crate::raft_network::{add, address, handle, leader_of, start, start_plaintext, stop, until};
use crate::support::Tls;
use crate::{TestResult, create_request, node_client, secure_client, serve_service};

/// How long a test waits for a call to reach a leader after the group changes.
const SETTLE: Duration = Duration::from_secs(20);

/// Plaintext control nodes `c1`, which starts the group, and `c2` and `c3`, which join it; all
/// three vote, and `c1` leads.
async fn three_plaintext(root: &Path) -> Result<[Daemon; 3], Box<dyn Error>> {
    let c1 = start_plaintext(root, "c1", "new").await?;
    let c2 = start_plaintext(root, "c2", "join").await?;
    let c3 = start_plaintext(root, "c3", "join").await?;
    add(&c1, &[("c2", &c2), ("c3", &c3)]).await?;
    handle(&c1).set_voters(&["c1", "c2", "c3"]).await?;
    until("every node knows c1 leads", || {
        [&c1, &c2, &c3]
            .iter()
            .all(|daemon| leader_of(daemon).as_deref() == Some("c1"))
    })
    .await?;
    Ok([c1, c2, c3])
}

/// A codec for calls whose request and reply this test doesn't read: it sends an empty message,
/// which every request type decodes as its default, and decodes no reply.
#[derive(Debug, Clone, Copy, Default)]
struct Empty;

impl Codec for Empty {
    type Encode = ();
    type Decode = ();
    type Encoder = Self;
    type Decoder = Self;

    fn encoder(&mut self) -> Self {
        Self
    }

    fn decoder(&mut self) -> Self {
        Self
    }
}

impl Encoder for Empty {
    type Item = ();
    type Error = Status;

    fn encode(&mut self, (): (), _: &mut EncodeBuf<'_>) -> Result<(), Status> {
        Ok(())
    }
}

impl Decoder for Empty {
    type Item = ();
    type Error = Status;

    fn decode(&mut self, _: &mut DecodeBuf<'_>) -> Result<Option<()>, Status> {
        Err(Status::internal("a follower answered a call"))
    }
}

/// Retries `call` while it answers `UNAVAILABLE`, as during an election, for at most
/// [`SETTLE`].
async fn eventually<T, F, Fut>(what: &str, mut call: F) -> Result<T, Box<dyn Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Status>>,
{
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        match call().await {
            Ok(value) => return Ok(value),
            Err(status)
                if status.code() == Code::Unavailable && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => return Err(format!("{what}: {status:?}").into()),
        }
    }
}

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

/// Every `Control` call on a follower is answered with a redirect to the leader, before the
/// follower reads the request: the call's empty request would be refused as invalid otherwise.
/// A membership watch opened on a follower is redirected too.
#[tokio::test]
async fn a_follower_redirects_every_control_call_to_the_leader() -> TestResult {
    let dir = tempfile::tempdir()?;
    let [c1, c2, c3] = three_plaintext(dir.path()).await?;
    let channel =
        tonic::transport::Endpoint::from_shared(format!("http://{}", address(&c2)))?.connect_lazy();
    let mut grpc = tonic::client::Grpc::new(channel);
    let control = fleetringd::auth::RPCS
        .iter()
        .filter(|rpc| rpc.service() == "fleetring.v1.Control");
    let mut checked = 0;
    for rpc in control {
        grpc.ready().await?;
        let path = PathAndQuery::from_static(rpc.path);
        let answered = if rpc.method() == "WatchMembership" {
            grpc.server_streaming(tonic::Request::new(()), path, Empty)
                .await
                .map(drop)
        } else {
            grpc.unary(tonic::Request::new(()), path, Empty)
                .await
                .map(drop)
        };
        let status = answered
            .err()
            .ok_or_else(|| format!("{} answered", rpc.path))?;
        assert_eq!(status.code(), Code::Unavailable, "{}: {status:?}", rpc.path);
        assert_eq!(
            leader_hint(&status),
            Some(address(&c1)),
            "{}: {status:?}",
            rpc.path
        );
        checked += 1;
    }
    assert!(checked > 25, "only {checked} control calls");
    stop([c1, c2, c3]).await;
    Ok(())
}

/// A write acknowledged by one leader is seen by the reads of the next: `GetForkOperation`,
/// `Locate`, `ListDatabases`, and `ListDelegations` are read linearizably on the new leader,
/// which applies what the old one committed before it answers.
#[tokio::test]
async fn a_read_after_a_write_sees_it_after_a_leader_change() -> TestResult {
    let dir = tempfile::tempdir()?;
    let [c1, c2, c3] = three_plaintext(dir.path()).await?;
    let forking = fleetringd::software::running().with_feature(Feature::Fork);
    for id in ["d1", "d2", "d3"] {
        handle(&c1)
            .submit(Action::Join {
                member: Member::new(id.parse()?).with_software(forking.clone()),
            })
            .await?;
    }
    let addresses = [&c1, &c2, &c3].map(address).to_vec();
    let client = ControlClient::with_addresses(addresses, TransportConfig::default());
    client.create_database(create_request("notes", 3)).await?;
    let operation = OperationId::from_bytes([0x42; 16]);
    client
        .fork_database(ForkDatabaseRequest {
            operation_id: operation.to_bytes().to_vec(),
            database: "scratch".to_owned(),
            source: "notes".to_owned(),
            replication_factor: 3,
            ..ForkDatabaseRequest::default()
        })
        .await?;
    stop([c1]).await;
    let forked = eventually("read the fork on the new leader", || {
        client.get_fork_operation(GetForkOperationRequest {
            operation_id: operation.to_bytes().to_vec(),
        })
    })
    .await?;
    assert_eq!(forked.database, "scratch");
    let located = eventually("locate on the new leader", || {
        client.locate(LocateRequest {
            database: "notes".into(),
        })
    })
    .await?;
    assert_eq!(
        located.placement.map(|placement| placement.database),
        Some("notes".to_owned())
    );
    let listed = eventually("list on the new leader", || {
        client.list_databases(ListDatabasesRequest::default())
    })
    .await?;
    assert_eq!(listed.databases.len(), 2, "{listed:?}");
    eventually("list delegations on the new leader", || {
        client.list_delegations(ListDelegationsRequest::default())
    })
    .await?;
    let leader = leader_of(&c2).ok_or("no leader")?;
    assert!(leader == "c2" || leader == "c3", "{leader}");
    stop([c2, c3]).await;
    Ok(())
}

/// A role-`both` control node, once it has joined its own fleet.
async fn both(dir: &Path, tls: &Tls) -> Result<Daemon, Box<dyn Error>> {
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"both\"\n\n[node]\nid = \"c1\"\n\n\
         [membership]\nheartbeat_interval_ms = 100\n{}",
        dir.join("c1"),
        tls.node_table("c1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let membership = daemon.membership().ok_or("no membership")?;
    tokio::time::timeout(SETTLE, membership.wait_for(|view| view.ring().is_some()))
        .await?
        .ok_or("membership stopped")?;
    Ok(daemon)
}

/// `fleetring node remove` of a voter of the control group, a principal's `Leave`, is refused
/// with `FAILED_PRECONDITION` naming the commands that take it out of the group first (ADR-0040
/// point 9, decision D7); a node outside the group is still removed.
#[tokio::test]
async fn node_remove_refuses_a_control_group_member() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let c1 = both(dir.path(), &tls).await?;
    let operator = secure_client(c1.listen_address(), &tls)?;
    let refused = operator
        .leave(LeaveRequest {
            node_id: "c1".into(),
        })
        .await
        .expect_err("a member of the control group");
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
    for command in ["fleetring control set-voters", "fleetring control remove"] {
        assert!(refused.message().contains(command), "{}", refused.message());
    }
    handle(&c1)
        .submit(Action::Join {
            member: Member::new("d1".parse()?).with_software(fleetringd::software::running()),
        })
        .await?;
    operator
        .leave(LeaveRequest {
            node_id: "d1".into(),
        })
        .await?;
    stop([c1]).await;
    Ok(())
}

/// A role-`both` voter's own `Leave`, under its node certificate, as `on_shutdown = "leave"`
/// sends it, isn't refused: it changes the fleet, not the group (decision D7).
#[tokio::test]
async fn a_voters_own_leave_is_not_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let c1 = both(dir.path(), &tls).await?;
    let own = node_client(c1.listen_address(), &tls, "c1")?;
    own.leave(LeaveRequest {
        node_id: "c1".into(),
    })
    .await?;
    assert!(handle(&c1).group().contains("c1"));
    stop([c1]).await;
    Ok(())
}
