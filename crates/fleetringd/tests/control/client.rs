//! The control client reaches the control group at any of its addresses (FR-064 PR 4, ADR-0040
//! point 12), against stub `Control` servers and a real control node.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fleetring_proto::v1::{ControlNode, HeartbeatRequest};
use fleetringd::control::ControlClient;
use fleetringd::transport::{Backoff, TransportConfig, control_server};
use fleetringd::{Config, Daemon};
use tonic::Code;

use crate::support::Tls;
use crate::{Publishing, TestResult, join_request, node_client};

/// Serves `stub` in plaintext on a random local port.
async fn serve_stub(stub: Publishing) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(control_server(stub))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    Ok(address)
}

/// An address nothing listens on.
async fn dead_address() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

fn heartbeat() -> HeartbeatRequest {
    HeartbeatRequest {
        node_id: "data-1".into(),
        ..HeartbeatRequest::default()
    }
}

/// A client that retries once, quickly.
fn quick(addresses: &[SocketAddr]) -> ControlClient {
    ControlClient::with_addresses(
        addresses.iter().map(ToString::to_string).collect(),
        TransportConfig::default(),
    )
    .with_backoff(Backoff {
        initial: Duration::from_millis(10),
        max: Duration::from_millis(10),
        attempts: 1,
    })
}

/// With `[dead, live]`, a heartbeat reaches `live`, and the client stays there. A reply without
/// `control_nodes`, as a 0.9 control node sends, leaves it on its configured addresses.
#[tokio::test]
async fn the_client_moves_to_the_next_address_when_one_is_down() -> TestResult {
    let stub = Publishing::default();
    let live = serve_stub(stub.clone()).await?;
    let dead = dead_address().await?;
    let client = quick(&[dead, live]);
    client.heartbeat(heartbeat()).await?;
    assert_eq!(client.leader(), live.to_string());
    assert_eq!(stub.heartbeats.load(Ordering::SeqCst), 1);
    assert_eq!(
        client.addresses_to_try(),
        [dead.to_string(), live.to_string()]
    );
    Ok(())
}

/// A listener that accepts connections and never answers: a hung control node, or one behind a
/// partition that drops packets.
async fn hung_address() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    Ok(address)
}

/// A call to an address that hangs ends at the channel's request timeout, `CANCELLED` or
/// `DEADLINE_EXCEEDED`, and the client tries the next address, which answers.
#[tokio::test]
async fn the_client_moves_past_an_address_that_hangs() -> TestResult {
    let stub = Publishing::default();
    let live = serve_stub(stub.clone()).await?;
    let hung = hung_address().await?;
    let transport = TransportConfig {
        request_timeout: Duration::from_millis(300),
        ..TransportConfig::default()
    };
    let client = ControlClient::with_addresses(vec![hung.to_string(), live.to_string()], transport)
        .with_backoff(Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(10),
            attempts: 1,
        });
    client.heartbeat(heartbeat()).await?;
    assert_eq!(client.leader(), live.to_string());
    assert_eq!(stub.heartbeats.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A caller whose own, shorter deadline ends a call to an address that hangs moves the client on
/// with `advance`, as membership's heartbeats do on each timeout.
#[tokio::test]
async fn a_caller_deadline_moves_the_client_on_with_advance() -> TestResult {
    let stub = Publishing::default();
    let live = serve_stub(stub.clone()).await?;
    let hung = hung_address().await?;
    let client = quick(&[hung, live]);
    let timed_out =
        tokio::time::timeout(Duration::from_millis(300), client.heartbeat(heartbeat())).await;
    assert!(
        timed_out.is_err(),
        "the hung address answered: {timed_out:?}"
    );
    assert_eq!(client.leader(), hung.to_string());
    client.advance();
    assert_eq!(client.leader(), live.to_string());
    client.heartbeat(heartbeat()).await?;
    assert_eq!(stub.heartbeats.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A reply that names the control group adds its members to the addresses the client tries, the
/// leader's first, without moving the client off the address that answers: an advertised address
/// may not be one this node reaches the member at, behind a proxy or after it moved, as here,
/// where the leader's advertised address is dead.
#[tokio::test]
async fn the_client_learns_the_group_and_keeps_the_address_that_answers() -> TestResult {
    let first = Publishing::default();
    let first_address = serve_stub(first.clone()).await?;
    let moved = dead_address().await?;
    let other = dead_address().await?;
    let node = |id: &str, address: SocketAddr, is_leader: bool| ControlNode {
        node_id: id.into(),
        address: address.to_string(),
        voter: true,
        leader: is_leader,
        ..ControlNode::default()
    };
    first.name_group(vec![node("c3", other, false), node("c1", moved, true)]);
    let client = quick(&[first_address]);
    let reply = client.heartbeat(heartbeat()).await?;
    client.learn(&reply.control_nodes);
    assert_eq!(client.leader(), first_address.to_string());
    assert_eq!(
        client.addresses_to_try(),
        [
            first_address.to_string(),
            moved.to_string(),
            other.to_string()
        ]
    );
    client.heartbeat(heartbeat()).await?;
    assert_eq!(first.heartbeats.load(Ordering::SeqCst), 2);
    Ok(())
}

/// Callers whose deadlines end together, each having noted the address it called, move the
/// client on once: `advance_from` moves only from an address the client still calls, where
/// `advance` would move it once per caller, round three addresses and back to the hung one.
#[test]
fn concurrent_timeouts_move_the_client_on_once() {
    let addresses = ["127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"];
    let client = ControlClient::with_addresses(
        addresses.iter().map(ToString::to_string).collect(),
        TransportConfig::default(),
    );
    let asked = client.leader();
    for _ in 0..3 {
        client.advance_from(&asked);
    }
    assert_eq!(client.leader(), addresses[1]);
    for _ in 0..3 {
        client.advance();
    }
    assert_eq!(client.leader(), addresses[1], "advance goes round the list");
}

/// A call sent once per leader goes on to the next address when the first refuses the
/// connection, since nothing received it: here it reaches the stub, which doesn't serve it.
#[tokio::test]
async fn a_once_only_call_moves_past_a_refused_connection() -> TestResult {
    let live = serve_stub(Publishing::default()).await?;
    let client = quick(&[dead_address().await?, live]);
    let answered = client
        .leave(fleetring_proto::v1::LeaveRequest {
            node_id: "data-1".into(),
        })
        .await
        .expect_err("the stub doesn't serve it");
    assert_eq!(answered.code(), Code::Unimplemented, "{answered:?}");
    assert_eq!(client.leader(), live.to_string());
    Ok(())
}

/// When every address is down, the call fails `UNAVAILABLE` once each has been tried.
#[tokio::test]
async fn the_client_gives_up_after_every_address_has_failed() -> TestResult {
    let client = quick(&[dead_address().await?, dead_address().await?]);
    let refused = client
        .heartbeat(heartbeat())
        .await
        .expect_err("nothing is up");
    assert_eq!(refused.code(), Code::Unavailable, "{refused:?}");
    Ok(())
}

/// A control node's join and heartbeat replies name its group: itself, the only voter, leading.
#[tokio::test]
async fn join_and_heartbeat_replies_name_the_control_group() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let client = node_client(address, &tls, "data-1")?;
    let joined = client.join(join_request("data-1")).await?;
    let check = |nodes: &[ControlNode]| {
        assert_eq!(nodes.len(), 1, "{nodes:?}");
        assert_eq!(nodes[0].node_id, "control-1");
        assert_eq!(nodes[0].address, daemon.listen_address().to_string());
        assert!(nodes[0].voter && nodes[0].leader, "{nodes:?}");
        assert!(nodes[0].software.is_some(), "{nodes:?}");
    };
    check(&joined.control_nodes);
    let beat = client.heartbeat(heartbeat()).await?;
    check(&beat.control_nodes);
    daemon.run_until(async {}).await;
    Ok(())
}
