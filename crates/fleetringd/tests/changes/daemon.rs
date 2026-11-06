//! Change sinks from the daemon's configuration: `[[changes.sink]]` entries build sinks, the
//! daemon feeds them, and `status --node` reports their positions.

use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::Liveness;
use fleetringd::changes::{Format, MarkHome, SinkKind, database_file};
use fleetringd::data::Client;
use fleetringd::engine::{CaptureMode, Statement};
use fleetringd::{Config, ConfigError, Daemon};

use crate::common::{TestResult, eventually, guarded, name};
use crate::file::{inserted, records};
use crate::support::Tls;

const MINIMAL: &str = r#"
data_dir = "/tmp/fleetring-test"
listen = "127.0.0.1:7400"

[node]
id = "laptop-1"
"#;

#[test]
fn sink_entries_are_parsed_and_validated() -> TestResult {
    let text = format!(
        "{MINIMAL}
[[changes.sink]]
name = \"archive\"
kind = \"file\"
format = \"physical\"
databases = [\"tenant:*\", \"ledger\"]
path = \"/var/lib/fleetring/changes\"

[[changes.sink]]
name = \"hooks\"
kind = \"webhook\"
databases = [\"*\"]
url = \"http://127.0.0.1:8080/changes\"
allow_http = true
timeout_ms = 1000
retries = 2

[[changes.sink]]
name = \"secure\"
kind = \"webhook\"
databases = [\"*\"]
url = \"https://hooks.example.com/changes\"
ca = \"/etc/fleetring/hooks-ca.crt\"
cert = \"/etc/fleetring/hooks.crt\"
key = \"/etc/fleetring/hooks.key\"
token_env = \"HOOKS_TOKEN\"
"
    );
    let config = Config::from_toml(&text)?;
    let sinks = &config.changes.sink;
    assert_eq!(sinks.len(), 3);
    assert_eq!(
        (sinks[0].kind, sinks[0].format),
        (SinkKind::File, Format::Physical)
    );
    assert_eq!(
        (sinks[1].kind, sinks[1].format),
        (SinkKind::Webhook, Format::Logical)
    );
    assert_eq!((sinks[1].timeout_ms, sinks[1].retries), (1000, 2));
    assert!(sinks[1].allow_http);
    assert!(!sinks[2].allow_http);
    assert_eq!(sinks[2].token_env.as_deref(), Some("HOOKS_TOKEN"));
    let rules = config.changes.capture_rules();
    assert_eq!(
        rules.mode_for(&name("anything")),
        Some(CaptureMode::After),
        "the logical sink records every database, in the default mode"
    );
    assert!(sinks[0].matchers().matches(&name("tenant:acme")));
    assert!(!sinks[0].matchers().matches(&name("agent:acme")));

    let entry = |body: &str| format!("{MINIMAL}\n[[changes.sink]]\n{body}\n");
    let invalid = [
        (
            "name = \"Archive\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = \"/x\"",
            "lowercase",
        ),
        (
            "name = \"a\"\nkind = \"file\"\ndatabases = []\npath = \"/x\"",
            "names no databases",
        ),
        (
            "name = \"a\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = \"/x\"\nurl = \"http://x\"",
            "takes no `url`",
        ),
        (
            "name = \"a\"\nkind = \"webhook\"\ndatabases = [\"*\"]",
            "needs a `url`",
        ),
        (
            "name = \"a\"\nkind = \"webhook\"\ndatabases = [\"*\"]\nurl = \"http://x/y\"",
            "`url`, `http://x/y`, which needs `allow_http = true`",
        ),
        (
            "name = \"a\"\nkind = \"webhook\"\ndatabases = [\"*\"]\nurl = \"ftp://x/y\"",
            "isn't an https:// URL",
        ),
        (
            "name = \"a\"\nkind = \"webhook\"\ndatabases = [\"*\"]\nurl = \"https://x/y\"\ncert = \"/c\"",
            "`cert` but no `key`",
        ),
        (
            "name = \"a\"\nkind = \"webhook\"\ndatabases = [\"*\"]\nurl = \"https://x/y\"\nkey = \"/k\"",
            "`key` but no `cert`",
        ),
        (
            "name = \"a\"\nkind = \"webhook\"\ndatabases = [\"*\"]\nurl = \"https://x/y\"\ntoken_file = \"/t\"\ntoken_env = \"T\"",
            "both `token_file` and `token_env`",
        ),
        (
            "name = \"a\"\nkind = \"webhook\"\ndatabases = [\"*\"]\nurl = \"https://x/y\"\ntoken_env = \"\"",
            "empty `token_env`",
        ),
        (
            "name = \"a\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = \"/x\"\nallow_http = true",
            "takes no `allow_http`",
        ),
        (
            "name = \"a\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = \"/x\"\ntoken_file = \"/t\"",
            "takes no `token_file`",
        ),
        (
            "name = \"a\"\nkind = \"kafka\"\ndatabases = [\"*\"]",
            "kafka",
        ),
        (
            "name = \"a\"\nkind = \"file\"\ndatabases = [\"\"]\npath = \"/x\"",
            "not empty",
        ),
    ];
    for (body, expected) in invalid {
        let error = Config::from_toml(&entry(body)).expect_err(expected);
        assert!(error.to_string().contains(expected), "{body}: {error}");
        assert!(
            matches!(error, ConfigError::Invalid { .. } | ConfigError::Parse(_)),
            "{error}"
        );
    }
    let twice = format!(
        "{MINIMAL}\n[[changes.sink]]\nname = \"a\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = \"/x\"\n\n[[changes.sink]]\nname = \"a\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = \"/y\"\n"
    );
    assert!(
        Config::from_toml(&twice)
            .expect_err("twice")
            .to_string()
            .contains("used twice")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_configured_file_sink_is_fed_and_reported_in_the_node_status() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let archive = dir.path().join("archive");
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"both\"\n\n[node]\nid = \"solo\"\n\n[membership]\nheartbeat_interval_ms = 100\n\n[[changes.sink]]\nname = \"archive\"\nkind = \"file\"\ndatabases = [\"ledger\"]\npath = {:?}\n{}",
        dir.path().join("solo"),
        archive,
        tls.node_table("solo")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.listen_address();
    let client = Client::new(
        address.to_string(),
        tls.client_network_with(
            "tester",
            fleetringd::transport::TransportConfig {
                request_timeout: Duration::from_secs(60),
                ..fleetringd::transport::TransportConfig::default()
            },
        )?,
    );
    let ledger = name("ledger");
    let other = name("other");
    // The node joins a moment after the daemon starts; wait until the control plane has it
    // live, and the node has published the ring.
    let handle = daemon.control().ok_or("a control handle")?;
    let mut states = handle.watch();
    guarded(
        "the node joining",
        states.wait_for(|state| {
            state.members().len() == 1
                && state
                    .members()
                    .all(|member| state.liveness(member.id(), state.now()) == Some(Liveness::Live))
        }),
    )
    .await??;
    let membership = daemon.membership().ok_or("a data node")?;
    guarded(
        "the ring publishing",
        membership.wait_for(|view| view.ring().is_some()),
    )
    .await?
    .ok_or("membership stopped")?;
    for database in [&ledger, &other] {
        client
            .create_database(database, ReplicationFactor::ONE)
            .await?;
    }
    for database in [&ledger, &other] {
        client
            .execute(
                database,
                Statement::new("CREATE TABLE t (value INTEGER NOT NULL)"),
            )
            .await?;
        for value in 1..=3 {
            client
                .execute(
                    database,
                    Statement::with_params("INSERT INTO t (value) VALUES (?1)", [value.into()]),
                )
                .await?;
        }
    }
    let streams = daemon.change_streams().ok_or("a data node")?;
    assert!(streams.await_caught_up(Duration::from_secs(60)).await);
    let path = database_file(&archive, &ledger);
    assert_eq!(inserted(&records(&path)?), vec![1, 2, 3]);
    assert!(
        !database_file(&archive, &other).exists(),
        "the sink names only `ledger`"
    );

    eventually("the status reporting the sink", || {
        let status = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(client.node_status(&address.to_string()))
        });
        status.is_ok_and(|status| {
            status.sinks.len() == 1
                && status.sinks[0].sink == "archive"
                && status.sinks[0].database == ledger
                && status.sinks[0].position.is_some()
                && status.sinks[0].position == status.sinks[0].acknowledged
                && status.sinks[0].home == Some(MarkHome::Replicated)
        })
    })
    .await?;

    // The command line says where the position is kept.
    let tls_dir = tls.dir().to_owned();
    let command_address = address.to_string();
    let output = tokio::task::spawn_blocking(move || {
        crate::support::fleetring()
            .env("FLEETRING_TLS_DIR", tls_dir)
            .args(["status", "--address", &command_address])
            .output()
    })
    .await??;
    let printed = String::from_utf8(output.stdout)?;
    assert!(output.status.success(), "{printed}");
    assert!(
        printed.contains("archive ledger position=") && printed.contains(") acknowledged="),
        "{printed}"
    );
    assert!(printed.contains("(replicated)"), "{printed}");
    daemon.run_until(std::future::ready(())).await;
    Ok(())
}
