//! The control group's network (FR-064 PR 4, ADR-0040 points 3 and 4): control nodes in one
//! process, over TCP, with node certificates from the test authority.

use std::error::Error;
use std::path::Path;
use std::time::Duration;

use fleetring_control::{
    Action, Engine, Feature, Incarnation, IncarnationRule, Member, Rejection, Release, Software,
};
use fleetring_proto::v1::RaftPayload;
use fleetringd::auth::refused_binding;
use fleetringd::control::{ControlHandle, GroupError, SubmitError};
use fleetringd::transport::raft_client;
use fleetringd::{Config, Daemon, Environment};
use tonic::Code;

use crate::support::Tls;
use crate::{TestResult, create_request, secure_client};

/// How long a test waits for the group to settle: elections, catch-up, and reports.
const SETTLE: Duration = Duration::from_secs(20);

/// The release this build runs.
fn this_release() -> Release {
    fleetringd::software::release()
}

/// Software of `release` with every feature but `lacking`.
fn software(release: Release, lacking: &[Feature]) -> Software {
    Feature::ALL
        .into_iter()
        .filter(|feature| !lacking.contains(feature))
        .fold(Software::new(release, 2), Software::with_feature)
}

/// A member of the fleet that supports every feature, on this build's release.
fn member(id: &str) -> Member {
    Member::new(id.parse().expect("an id")).with_software(software(this_release(), &[]))
}

/// The configuration of control node `id` under `root`, with `bootstrap` and short Raft timings,
/// and `control` appended to its `[control]` table.
fn config(
    root: &Path,
    tls: &str,
    id: &str,
    bootstrap: &str,
    control: &str,
) -> Result<Config, Box<dyn Error>> {
    Ok(Config::from_toml(&format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = {id:?}\n\n\
         [control]\nbootstrap = {bootstrap:?}\n\
         {control}\n{}",
        root.join(id),
        tls
    ))?)
}

/// Starts control node `id`, reporting `software` to its group.
async fn start(
    root: &Path,
    tls: &Tls,
    id: &str,
    bootstrap: &str,
    control: &str,
    software: Option<Software>,
) -> Result<Daemon, Box<dyn Error>> {
    let environment = match software {
        Some(software) => Environment::production().with_software(software),
        None => Environment::production(),
    };
    // A node started again keeps the certificate it was issued.
    let stem = format!("node.{id}");
    let table = if tls.dir().join(format!("{stem}.crt")).exists() {
        tls.table(&stem)
    } else {
        tls.node_table(id)?
    };
    Ok(Daemon::start_with(config(root, &table, id, bootstrap, control)?, environment).await?)
}

fn handle(daemon: &Daemon) -> ControlHandle {
    daemon.control().expect("a control node")
}

fn address(daemon: &Daemon) -> String {
    daemon.listen_address().to_string()
}

/// Waits until `check` holds, polling, for at most [`SETTLE`].
async fn until(what: &str, mut check: impl FnMut() -> bool) -> TestResult {
    let deadline = tokio::time::Instant::now() + SETTLE;
    while !check() {
        if tokio::time::Instant::now() > deadline {
            return Err(format!("timed out waiting until {what}").into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Adds every node of `joiners` to `leader`'s group as a learner.
async fn add(leader: &Daemon, joiners: &[(&str, &Daemon)]) -> TestResult {
    for (id, daemon) in joiners {
        handle(leader).add_node(id, &address(daemon)).await?;
    }
    Ok(())
}

/// The ID of the leader `daemon` knows, if any.
fn leader_of(daemon: &Daemon) -> Option<String> {
    handle(daemon).leader().map(|peer| peer.node_id)
}

/// Starts `c1` with `bootstrap = "new"` and `c2` and `c3` with `"join"`, `c3` reporting
/// `c3_software`, adds both, and makes all three voters.
async fn three(
    root: &Path,
    tls: &Tls,
    c3_software: Option<Software>,
) -> Result<[Daemon; 3], Box<dyn Error>> {
    let c1 = start(root, tls, "c1", "new", "", None).await?;
    let c2 = start(root, tls, "c2", "join", "", None).await?;
    let c3 = start(root, tls, "c3", "join", "", c3_software).await?;
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

async fn stop(daemons: impl IntoIterator<Item = Daemon>) {
    for daemon in daemons {
        daemon.run_until(async {}).await;
    }
}

/// `c1` starts a group, `c2` and `c3` wait to be added; after two adds and `set_voters`, all
/// three agree on one leader, and a join submitted on the leader shows in every state.
#[tokio::test]
async fn three_control_nodes_elect_one_leader() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let [c1, c2, c3] = three(dir.path(), &tls, None).await?;
    let group = handle(&c2).group();
    assert_eq!(group.voters().len(), 3, "{group:?}");
    assert!(group.learners().is_empty(), "{group:?}");
    let committed = handle(&c1)
        .submit(Action::Join {
            member: member("data-1"),
        })
        .await?;
    assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
    until("every state holds data-1", || {
        [&c1, &c2, &c3].iter().all(|daemon| {
            handle(daemon)
                .state()
                .member(&"data-1".parse().expect("an id"))
                .is_some()
        })
    })
    .await?;
    // A follower redirects to the leader.
    let refused = handle(&c2).submit(Action::Tick).await;
    assert!(
        matches!(&refused, Err(SubmitError::NotLeader { leader: Some(leader) }) if *leader == address(&c1)),
        "{refused:?}"
    );
    // Shrinking to one voter keeps the others as learners; a voter can't be removed, a learner
    // can, and a removed member's Raft calls are refused.
    handle(&c1).set_voters(&["c1"]).await?;
    let group = handle(&c1).group();
    assert_eq!(group.voters().len(), 1, "{group:?}");
    assert_eq!(group.learners().len(), 2, "{group:?}");
    let refused = handle(&c1).remove_node("c1").await;
    assert!(
        matches!(refused, Err(GroupError::Invalid { .. })),
        "{refused:?}"
    );
    handle(&c1).remove_node("c3").await?;
    assert!(!handle(&c1).group().contains("c3"));
    let channel = tls.node_network("c3")?.channel(&address(&c1))?;
    let refused = raft_client(channel)
        .append_entries(RaftPayload {
            json: b"{}".to_vec(),
            sender: None,
            sent_at_ms: 0,
        })
        .await
        .expect_err("c3 was removed");
    assert_eq!(refused_binding(&refused), Some("control peer"));
    stop([c1, c2, c3]).await;
    Ok(())
}

/// A Raft service that answers as 0.9.0's boundary answers a path its table doesn't list:
/// `PERMISSION_DENIED`, without a binding.
#[derive(Debug, Clone, Copy)]
struct Release09;

fn unlisted(method: &str) -> tonic::Status {
    tonic::Status::permission_denied(format!(
        "/fleetring.v1.Raft/{method} has no scope requirement"
    ))
}

#[tonic::async_trait]
impl fleetring_proto::v1::raft_server::Raft for Release09 {
    async fn append_entries(
        &self,
        _: tonic::Request<RaftPayload>,
    ) -> Result<tonic::Response<RaftPayload>, tonic::Status> {
        Err(unlisted("AppendEntries"))
    }

    async fn vote(
        &self,
        _: tonic::Request<RaftPayload>,
    ) -> Result<tonic::Response<RaftPayload>, tonic::Status> {
        Err(unlisted("Vote"))
    }

    async fn install_snapshot(
        &self,
        _: tonic::Request<RaftPayload>,
    ) -> Result<tonic::Response<RaftPayload>, tonic::Status> {
        Err(unlisted("InstallSnapshot"))
    }

    async fn probe(
        &self,
        _: tonic::Request<RaftPayload>,
    ) -> Result<tonic::Response<RaftPayload>, tonic::Status> {
        Err(unlisted("Probe"))
    }
}

/// A plaintext control node, `insecure_plaintext`, for tests whose stubs serve no TLS.
async fn start_plaintext(root: &Path, id: &str, bootstrap: &str) -> Result<Daemon, Box<dyn Error>> {
    let config = Config::from_toml(&format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\ninsecure_plaintext = true\nrole = \"control\"\n\n\
         [node]\nid = {id:?}\n\n[control]\nbootstrap = {bootstrap:?}\n\n[admin]\nlisten = \"127.0.0.1:0\"\n",
        root.join(id),
    ))?;
    Ok(Daemon::start(config).await?)
}

/// A 0.9 node answers `Probe` with `PERMISSION_DENIED` and no binding, as its boundary answers
/// any path it doesn't list: refused as running a release without a control group.
#[tokio::test]
async fn a_node_of_0_9_is_refused_as_without_a_control_group() -> TestResult {
    let dir = tempfile::tempdir()?;
    let c1 = start_plaintext(dir.path(), "c1", "new").await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let old = listener.local_addr()?;
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(fleetringd::transport::raft_server(Release09))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    let refused = handle(&c1).add_node("n9", &old.to_string()).await;
    let Err(GroupError::Probe { reason, .. }) = refused else {
        panic!("expected a probe refusal: {refused:?}");
    };
    assert!(reason.contains("which has no control group"), "{reason}");
    stop([c1]).await;
    Ok(())
}

/// A reassembled snapshot whose state doesn't decode is refused `INVALID_ARGUMENT`, never handed
/// to openraft, so the receiver's group keeps answering.
#[tokio::test]
async fn a_snapshot_that_does_not_decode_is_refused_and_the_group_lives() -> TestResult {
    let dir = tempfile::tempdir()?;
    let joiner = start_plaintext(dir.path(), "c9", "join").await?;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", address(&joiner)))?
        .connect_lazy();
    let mut client = raft_client(channel);
    let chunk = |offset: u64, data: &str, done: bool| {
        RaftPayload {
        json: format!(
            r#"{{"vote":{{"term":3,"node":"d733a26dd4b49b94","committed":true}},
                "meta":{{"last_log_id":{{"term":3,"node":"d733a26dd4b49b94","index":1000}},
                         "last_membership":{{"log_id":{{"term":2,"node":"d733a26dd4b49b94","index":17}},
                            "configs":[["d733a26dd4b49b94"]],
                            "nodes":{{"d733a26dd4b49b94":{{"node_id":"c1","address":"127.0.0.1:1"}}}}}}}},
                "offset":{offset},"data":"{data}","done":{done}}}"#
        )
        .into_bytes(),
        sender: None,
        sent_at_ms: 0,
    }
    };
    // "not json", as one whole chunk.
    let refused = client
        .install_snapshot(chunk(0, "bm90IGpzb24=", true))
        .await
        .expect_err("the state doesn't decode");
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
    // A chunk that doesn't continue anything is refused too.
    let refused = client
        .install_snapshot(chunk(9, "bm90IGpzb24=", false))
        .await
        .expect_err("out of order");
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
    let vote = RaftPayload {
        json:
            br#"{"vote":{"term":4,"node":"0000000000000002","committed":false},"last_log_id":null}"#
                .to_vec(),
        sender: None,
        sent_at_ms: 0,
    };
    client.vote(vote).await?;
    stop([joiner]).await;
    Ok(())
}

/// A node certificate outside the group is refused every Raft call, with `PERMISSION_DENIED`
/// naming the `control peer` binding; a principal is refused too.
#[tokio::test]
async fn a_node_outside_the_group_is_refused_raft_calls() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let c1 = start(dir.path(), &tls, "c1", "new", "", None).await?;
    let payload = || RaftPayload {
        json: b"{}".to_vec(),
        sender: None,
        sent_at_ms: 0,
    };
    let channel = tls.node_network("outsider")?.channel(&address(&c1))?;
    let mut outsider = raft_client(channel);
    let refused = outsider
        .append_entries(payload())
        .await
        .expect_err("not a member");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    assert_eq!(refused_binding(&refused), Some("control peer"));
    let refused = outsider.probe(payload()).await.expect_err("not a member");
    assert_eq!(refused_binding(&refused), Some("control peer"));
    let channel = tls.client_network("tester")?.channel(&address(&c1))?;
    let refused = raft_client(channel)
        .vote(payload())
        .await
        .expect_err("a principal");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    stop([c1]).await;
    Ok(())
}

/// A node started with `bootstrap = "join"` belongs to no group, so it accepts the Raft calls of
/// the first leader that adds it, and learns of it.
#[tokio::test]
async fn a_fresh_joiner_accepts_its_first_leader() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let c1 = start(dir.path(), &tls, "c1", "new", "", None).await?;
    let c2 = start(dir.path(), &tls, "c2", "join", "", None).await?;
    assert_eq!(leader_of(&c2), None);
    add(&c1, &[("c2", &c2)]).await?;
    until("c2 knows c1 leads", || {
        leader_of(&c2).as_deref() == Some("c1")
    })
    .await?;
    let group = handle(&c2).group();
    assert_eq!(group.learners().len(), 1, "{group:?}");
    until("c2 holds c1's state", || {
        handle(&c2).state() == handle(&c1).state()
    })
    .await?;
    stop([c1, c2]).await;
    Ok(())
}

/// The probe's refusals (ADR-0040 point 4): a node with its own group, a node with no Raft
/// service, a node two release lines away, and a node whose control state would start from
/// another configuration.
#[tokio::test]
async fn the_probe_refuses_what_adr_0040_lists() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let root = dir.path();
    let c1 = start(root, &tls, "c1", "new", "", None).await?;
    let probe_reason = |result: Result<(), GroupError>| match result {
        Err(GroupError::Probe { reason, .. }) => reason,
        other => panic!("expected a probe refusal: {other:?}"),
    };

    // A node with Raft state: started with the default `bootstrap = "new"`.
    let c4 = start(root, &tls, "c4", "new", "", None).await?;
    let reason = probe_reason(handle(&c1).add_node("c4", &address(&c4)).await);
    assert!(reason.contains("belongs to a control group"), "{reason}");

    // A node without a Raft service: a data-only node.
    let data = Config::from_toml(&format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"data\"\n\n[node]\nid = \"d1\"\n\n\
         [control]\naddress = {:?}\n{}",
        root.join("d1"),
        address(&c1),
        tls.node_table("d1")?
    ))?;
    let d1 = Daemon::start(data).await?;
    let reason = probe_reason(handle(&c1).add_node("d1", &address(&d1)).await);
    assert!(reason.contains("doesn't run the control role"), "{reason}");

    // A node two release lines away.
    let far = Release::new(this_release().line().0, this_release().line().1 + 2, 0);
    let c5 = start(root, &tls, "c5", "join", "", Some(software(far, &[]))).await?;
    let reason = probe_reason(handle(&c1).add_node("c5", &address(&c5)).await);
    assert!(reason.contains("can't run beside"), "{reason}");

    // A node whose genesis differs: it would replay the log from other timings.
    let c6 = start(
        root,
        &tls,
        "c6",
        "join",
        "lease_duration_ms = 20000\n",
        None,
    )
    .await?;
    let reason = probe_reason(handle(&c1).add_node("c6", &address(&c6)).await);
    assert!(reason.contains("lease_duration_ms is 20000"), "{reason}");

    // A node named wrongly.
    let reason = probe_reason(handle(&c1).add_node("c7", &address(&c6)).await);
    assert!(reason.contains("is 'c6', not 'c7'"), "{reason}");

    assert_eq!(handle(&c1).group().learners().len(), 0);
    stop([c1, c4, d1, c5, c6]).await;
    Ok(())
}

/// The leader hears every member's software within a few Raft heartbeats, and the window counts
/// it: a join two lines from a learner's release is refused, naming the learner.
#[tokio::test]
async fn the_leader_learns_every_members_software_within_a_heartbeat() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let (major, minor) = this_release().line();
    let next = Release::new(major, minor + 1, 0);
    let c1 = start(dir.path(), &tls, "c1", "new", "", None).await?;
    let c2 = start(
        dir.path(),
        &tls,
        "c2",
        "join",
        "",
        Some(software(next.clone(), &[])),
    )
    .await?;
    add(&c1, &[("c2", &c2)]).await?;
    until("c1 heard c2's release", || {
        handle(&c1).group_members().iter().any(|member| {
            member.peer.node_id == "c2"
                && member.software.as_ref().map(Software::release) == Some(&next)
        })
    })
    .await?;
    let old = Release::new(major, minor.saturating_sub(1), 0);
    let refused = handle(&c1)
        .submit(Action::Join {
            member: Member::new("old".parse()?).with_software(software(old, &[])),
        })
        .await;
    assert!(
        matches!(&refused, Err(SubmitError::Refused(Rejection::IncompatibleVersion { member, .. })) if member.as_str() == "c2"),
        "{refused:?}"
    );
    stop([c1, c2]).await;
    Ok(())
}

/// The first use of a feature waits for every control node: refused, naming `c3`, while `c3`
/// lacks it, and accepted once `c3`, restarted, reports it.
#[tokio::test]
async fn the_first_use_of_a_feature_waits_for_every_control_node() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let lacking = software(this_release(), &[Feature::Undrain]);
    let [c1, c2, c3] = three(dir.path(), &tls, Some(lacking)).await?;
    let leader = handle(&c1);
    leader
        .submit(Action::Join {
            member: member("data-1"),
        })
        .await?;
    leader
        .submit(Action::Drain {
            id: "data-1".parse()?,
        })
        .await?;
    let undrain = Action::Undrain {
        id: "data-1".parse()?,
    };
    until("c1 heard c3", || {
        handle(&c1)
            .group_members()
            .iter()
            .any(|member| member.peer.node_id == "c3" && member.software.is_some())
    })
    .await?;
    let refused = leader.submit(undrain.clone()).await;
    assert!(
        matches!(&refused, Err(SubmitError::Refused(Rejection::FeatureUnsupported { feature: Feature::Undrain, id, .. })) if id.as_str() == "c3"),
        "{refused:?}"
    );
    c3.run_until(async {}).await;
    let c3 = start(dir.path(), &tls, "c3", "join", "", None).await?;
    until("c1 heard c3 support undrain", || {
        handle(&c1).group_members().iter().any(|member| {
            member.peer.node_id == "c3"
                && member
                    .software
                    .as_ref()
                    .is_some_and(|software| software.supports(Feature::Undrain))
        })
    })
    .await?;
    let committed = leader.submit(undrain).await?;
    assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
    stop([c1, c2, c3]).await;
    Ok(())
}

/// Each injected enabling command's fallback holds while `c3` lacks its feature (ADR-0040 point
/// 4): a drop records no tombstone, a creation under `always` is logged `once_forked`, and a
/// membership change keeps no replica set.
#[tokio::test]
async fn the_gates_fallbacks_hold_while_a_control_node_lacks_the_features() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let lacking = software(
        this_release(),
        &[
            Feature::Tombstones,
            Feature::Incarnations,
            Feature::Handover,
        ],
    );
    let [c1, c2, c3] = three(dir.path(), &tls, Some(lacking)).await?;
    let leader = handle(&c1);
    until("c1 heard c3", || {
        leader
            .group_members()
            .iter()
            .any(|member| member.peer.node_id == "c3" && member.software.is_some())
    })
    .await?;
    for id in ["a", "b", "c"] {
        leader.submit(Action::Join { member: member(id) }).await?;
    }
    // A creation under `always`, through the service, is logged once forked.
    let control = secure_client(c1.listen_address(), &tls)?;
    control
        .create_database(create_request("tenant:acme/notes", 3))
        .await?;
    let state = leader.state();
    assert!(!state.is_enabled(Feature::Incarnations));
    let notes = state
        .database(&"tenant:acme/notes".parse()?)
        .ok_or("the database was created")?;
    assert_eq!(notes.incarnation(), Incarnation::LEGACY);
    // A membership change that would keep a set keeps none.
    let lease = state.replica_set(&"tenant:acme/notes".parse()?)?[0].clone();
    leader
        .submit(Action::AcquireLease {
            database: "tenant:acme/notes".parse()?,
            node: lease,
        })
        .await?;
    leader
        .submit(Action::Join {
            member: member("d"),
        })
        .await?;
    assert!(!leader.state().is_enabled(Feature::Handover));
    // A drop records no tombstone.
    leader
        .submit(Action::CreateDatabase {
            name: "tenant:acme/scratch".parse()?,
            replication_factor: fleetring::ReplicationFactor::new(3).ok_or("nonzero")?,
            engine: Engine::Sqlite,
            durability: None,
            metadata: None,
            incarnation: Incarnation::LEGACY,
            incarnation_rule: IncarnationRule::OnceForked,
        })
        .await?;
    let dropped = leader
        .submit(Action::DropDatabase {
            name: "tenant:acme/scratch".parse()?,
        })
        .await?;
    assert!(dropped.outcome().is_ok(), "{:?}", dropped.outcome());
    assert!(!leader.state().is_enabled(Feature::Tombstones));
    assert_eq!(leader.state().tombstones().len(), 0);
    stop([c1, c2, c3]).await;
    Ok(())
}

/// Crashing the leader mid-stream: every submitter is answered, and every acknowledged join is
/// in the new leader's state.
#[tokio::test]
async fn a_leader_crash_loses_no_acknowledged_command() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let [c1, c2, c3] = three(dir.path(), &tls, None).await?;
    let leader = handle(&c1);
    let submits: Vec<_> = (0..200)
        .map(|n| {
            let leader = leader.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(n * 5)).await;
                let id = format!("data-{n}");
                let answer = tokio::time::timeout(
                    SETTLE,
                    leader.submit(Action::Join {
                        member: member(&id),
                    }),
                )
                .await;
                (id, answer)
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(400)).await;
    c1.run_until(async {}).await;
    let mut acknowledged = Vec::new();
    for submit in submits {
        let (id, answer) = submit.await?;
        match answer {
            Err(_) => return Err(format!("the join of {id} was never answered").into()),
            Ok(Ok(committed)) if committed.outcome().is_ok() => acknowledged.push(id),
            Ok(_) => {}
        }
    }
    assert!(!acknowledged.is_empty(), "the leader acknowledged nothing");
    until("c2 or c3 leads", || {
        [&c2, &c3]
            .iter()
            .any(|daemon| handle(daemon).leadership().is_ready())
    })
    .await?;
    let new_leader = if handle(&c2).leadership().is_ready() {
        &c2
    } else {
        &c3
    };
    let state = handle(new_leader).state();
    for id in &acknowledged {
        assert!(
            state.member(&id.parse()?).is_some(),
            "acknowledged join of {id} lost"
        );
    }
    stop([c2, c3]).await;
    Ok(())
}

/// A learner added after compaction catches up from a control snapshot larger than one 1 MiB
/// chunk, sent in chunks and reassembled.
#[tokio::test]
async fn a_snapshot_larger_than_a_chunk_installs_on_a_learner() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let compacting = "snapshot_every = 10\nlog_keep = 2\n";
    let c1 = start(dir.path(), &tls, "c1", "new", compacting, None).await?;
    let leader = handle(&c1);
    // Members whose software names many features this build doesn't know, which the state keeps
    // as reported: a large state from few commands.
    let padding = "x".repeat(48);
    for n in 0..30 {
        let software = (0..800).fold(software(this_release(), &[]), |software, feature| {
            software.with_feature_name(&format!("newer-{feature}-{padding}"))
        });
        let id = format!("data-{n}");
        let member = Member::new(id.parse()?).with_software(software);
        leader.submit(Action::Join { member }).await?;
    }
    let size = serde_json::to_vec(&*leader.state())?.len();
    assert!(size > 1 << 20, "the state is only {size} bytes");
    // Let openraft build a snapshot and purge behind it.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let c2 = start(dir.path(), &tls, "c2", "join", compacting, None).await?;
    add(&c1, &[("c2", &c2)]).await?;
    until("c2 caught up", || {
        handle(&c2).state() == handle(&c1).state()
    })
    .await?;
    stop([c1, c2]).await;
    Ok(())
}
