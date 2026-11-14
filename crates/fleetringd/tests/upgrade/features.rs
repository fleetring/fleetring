//! FR-096: the rolling upgrade from a release without `fork`, `delegation` and `handover`, the
//! 0.7 line, to this build, which has all three (ADR-0038, points 4 and 9; ADR-0046, point 8;
//! ADR-0047, point 12; ADR-0031, amendment 2, point 6).
//!
//! On the previous release, its own binary issues a token for the test principal, mints agent
//! `42` (a version 1 identity whose token has none of the new claims), and the agent creates and
//! writes `agent-42-memory`. Both keep working while the fleet is mixed and after it is upgraded.
//! While the control node still runs the previous release, it refuses a token with `grant`.
//!
//! Once every node runs this build, nothing is enabled: one node rolls back to the previous
//! release, which the fleet allows, and while it runs there `db fork` is refused naming it,
//! `agent delegate` is refused, and `agent renew` keeps the defaults a node of the previous
//! release accepts and says why. The node then upgrades again. The first `db fork` enables `fork`
//! and ends rollback; a database created after it carries an incarnation, and those before keep
//! 0. `agent renew` then gives the agent this release's defaults, and the first `agent delegate`
//! enables `delegation`. The previous release's token still works, `agent remove` revokes the
//! child as its ancestor, and the fork is independent of its source.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use fleetring::NodeId;
use fleetring_control::{Feature, Incarnation, Release};
use fleetringd::data::Client;
use fleetringd::upgrade::ProcessFile;
use serde_json::Value;

use crate::common::{
    PATIENCE, ProcessFleet, TestResult, current, printed, progress, release_of, wait_for,
    wait_for_members,
};

/// The node that rolls back to the previous release and upgrades again: a data node.
const ROLLED_BACK: usize = 2;

/// The agent the previous release mints, and its database.
const AGENT: &str = "42";
const MEMORY: &str = "agent-42-memory";
const FORK: &str = "agent-42-memory-try";

/// The features the previous release must lack for these checks to apply.
const NEW: [Feature; 3] = [Feature::Fork, Feature::Delegation, Feature::Handover];

/// The checks, with what the previous release issued.
pub(crate) struct Features {
    home: PathBuf,
    tls: PathBuf,
    path: String,
    /// The token the previous release's `token issue` signed for the test principal.
    token: String,
    /// The control node's delegation issuer, which this build's `cert issuer` makes.
    issuer: PathBuf,
}

impl Features {
    /// Sets up the checks if `baseline` is a release from 0.7 on that lacks `fork`; otherwise
    /// `None`, and the rolling upgrade runs without them.
    pub(crate) fn on_previous_release(
        fleet: &ProcessFleet,
        baseline: &Path,
        scratch: &Path,
        path: &str,
    ) -> TestResult<Option<Self>> {
        if release_of(baseline)? < Release::new(0, 7, 0) {
            return Ok(None);
        }
        let mut software = Command::new(baseline);
        crate::support::isolate(&mut software);
        let software = crate::support::output_copied(software.args(["upgrade", "--software"]))?;
        let reported: Value = serde_json::from_slice(&software.stdout)?;
        let features = reported["features"]
            .as_array()
            .ok_or("no features in upgrade --software")?;
        if features.iter().any(|feature| feature == "fork") {
            return Ok(None);
        }
        let home = scratch.join("home");
        fs::create_dir_all(&home)?;
        let mut checks = Self {
            home,
            tls: fleet.tls.dir().to_owned(),
            path: path.to_owned(),
            token: String::new(),
            issuer: scratch.join("issuer"),
        };
        let tls = path_str(&checks.tls)?.to_owned();
        checks.ok_with(
            baseline,
            &[
                "context",
                "add",
                "fleet",
                "--control",
                &fleet.control,
                "--tls-dir",
                &tls,
            ],
        )?;
        checks.token = checks
            .ok_with(
                baseline,
                &[
                    "token",
                    "issue",
                    "--principal",
                    "tester",
                    "--scope",
                    "admin:*",
                    "--scope",
                    "fleet",
                    "--ttl",
                    "1h",
                    "--dir",
                    &tls,
                ],
            )?
            .trim()
            .to_owned();
        checks.ok_with(baseline, &["agent", "add", AGENT, "--authority", &tls])?;
        let record = fs::read_to_string(checks.agent_dir(AGENT).join("identity.toml"))?;
        assert!(record.contains("version = 1\n"), "{record}");
        checks.ok_with(baseline, &["db", "create", MEMORY, "--agent", AGENT])?;
        checks.ok_with(
            baseline,
            &[
                "db",
                "exec",
                MEMORY,
                "CREATE TABLE notes (n INTEGER PRIMARY KEY, body TEXT)",
                "--agent",
                AGENT,
            ],
        )?;
        checks.remember(baseline, "from the previous release")?;
        progress("the previous release issued a token and minted agent 42");
        Ok(Some(checks))
    }

    /// Gives the control node a delegation issuer, which the previous release's configuration
    /// refuses, and restarts it in place to read it.
    async fn configure_issuer(&self, fleet: &ProcessFleet, client: &Client) -> TestResult {
        let tls = path_str(&self.tls)?;
        let out = path_str(&self.issuer)?;
        self.ok(&[
            "cert", "issuer", "--name", "node-1", "--dir", tls, "--out", out,
        ])?;
        let node = &fleet.nodes[0];
        let text = fs::read_to_string(&node.config)?;
        assert!(!text.contains("[control]"), "{text}");
        fs::write(
            &node.config,
            format!(
                "{text}\n[control]\nissuer_cert = {:?}\nissuer_key = {:?}\n",
                self.issuer.join("issuer.node-1.crt"),
                self.issuer.join("issuer.node-1.key"),
            ),
        )?;
        let before = ProcessFile::read(&node.data_dir)?.ok_or("no process file")?;
        let signalled = Command::new("kill")
            .args(["-HUP", &before.pid.to_string()])
            .status()?;
        assert!(signalled.success(), "kill -HUP {}", before.pid);
        wait_for("the control node restarted in place", || {
            ProcessFile::read(&node.data_dir)
                .ok()
                .flatten()
                .filter(|now| now.restarts > before.restarts)
        })?;
        wait_for_members(client, &["node-1", "node-2", "node-3"], None).await?;
        Ok(())
    }

    /// While the control node runs the previous release: the previous release's credentials
    /// work, and a token with `grant`, which this build mints, is refused there.
    pub(crate) fn while_the_control_node_is_previous(&self) -> TestResult {
        self.credentials_work("under a previous control node")?;
        let tls = path_str(&self.tls)?;
        let granting = self
            .ok(&[
                "token",
                "issue",
                "--principal",
                "agent-42",
                "--scope",
                "admin:agent-42-*",
                "--scope",
                "grant:agent-42.*",
                "--ttl",
                "1h",
                "--dir",
                tls,
            ])?
            .trim()
            .to_owned();
        let dir = self.agent_dir(AGENT);
        let refused = self.run(&[
            "db",
            "list",
            "--cert",
            path_str(&dir.join("client.agent-42.crt"))?,
            "--key",
            path_str(&dir.join("client.agent-42.key"))?,
            "--token",
            &granting,
        ])?;
        assert!(!refused.status.success(), "{}", printed(&refused));
        assert!(
            stderr(&refused).contains("UNAUTHENTICATED")
                || stderr(&refused).contains("invalid scope"),
            "{}",
            printed(&refused)
        );
        Ok(())
    }

    /// Once every node runs this build: nothing is enabled, a node rolls back to the previous
    /// release and upgrades again, then each feature is enabled by its first command.
    pub(crate) async fn after_the_upgrade(
        &self,
        fleet: &mut ProcessFleet,
        client: &Client,
        url: &str,
    ) -> TestResult {
        wait_for_reporting(client, None).await?;
        self.configure_issuer(fleet, client).await?;
        self.nothing_enabled(client).await?;
        self.credentials_work("after the upgrade")?;

        self.roll_back_and_return(fleet, client, url).await?;
        self.nothing_enabled(client).await?;
        let databases = client.list_databases().await?;
        assert!(
            databases
                .iter()
                .all(|location| location.incarnation == Incarnation::LEGACY),
            "{databases:?}"
        );

        self.first_fork(fleet, client).await?;
        self.first_delegation(fleet, client).await?;
        self.afterwards(client).await
    }

    /// Rolls [`ROLLED_BACK`] back to the previous release, which the fleet allows since nothing
    /// is enabled, checks the mixed fleet's refusals, and upgrades it again with the previous
    /// release's own `upgrade`.
    async fn roll_back_and_return(
        &self,
        fleet: &mut ProcessFleet,
        client: &Client,
        url: &str,
    ) -> TestResult {
        let node = &fleet.nodes[ROLLED_BACK];
        let id: NodeId = node.id.parse()?;
        let rolled = fleet
            .command(&node.bin, &self.path)
            .args(["upgrade", "--rollback", "--config"])
            .arg(&node.config)
            .output()?;
        assert!(
            rolled.status.success(),
            "{}\n{}",
            printed(&rolled),
            fleet.logs()
        );
        wait_for_reporting(client, Some(&id)).await?;
        progress(&format!("{id} rolled back to the previous release"));

        // `db fork` names the member that lacks the feature.
        let refused = self.run(&["db", "fork", MEMORY, FORK])?;
        assert!(!refused.status.success(), "{}", printed(&refused));
        for part in [id.as_str(), "fork"] {
            assert!(
                stderr(&refused).contains(part),
                "{part}: {}",
                printed(&refused)
            );
        }
        // `agent renew` keeps the defaults the previous release accepts, and says why.
        let tls = path_str(&self.tls)?;
        let renewed = self.run(&["agent", "renew", AGENT, "--authority", tls])?;
        assert!(renewed.status.success(), "{}", printed(&renewed));
        assert!(
            stderr(&renewed).contains(&format!(
                "agent {AGENT} can delegate after `fleetring agent renew {AGENT}` once every \
                 node runs 0.8; node '{id}'"
            )),
            "{}",
            printed(&renewed)
        );
        assert_eq!(self.claims(AGENT)?.get("fleetring.kind"), None);
        // So `agent delegate` is refused: the parent can't delegate yet.
        let refused = self.run(&["agent", "delegate", "42.c7"])?;
        assert!(!refused.status.success(), "{}", printed(&refused));
        assert!(!self.agent_dir("42.c7").join("token").exists());
        self.credentials_work("while a member runs the previous release")?;
        self.nothing_enabled(client).await?;

        // The previous release's `upgrade` installs this build again.
        let node = &fleet.nodes[ROLLED_BACK];
        let upgraded = fleet
            .command(&node.bin, &self.path)
            .args([
                "upgrade",
                "--version",
                &current().to_string(),
                "--base-url",
                url,
                "--config",
            ])
            .arg(&node.config)
            .output()?;
        assert!(
            upgraded.status.success(),
            "{}\n{}",
            printed(&upgraded),
            fleet.logs()
        );
        if !String::from_utf8_lossy(&upgraded.stdout).contains("rejoined the fleet") {
            fleet.stop(ROLLED_BACK)?;
            fleet.spawn(ROLLED_BACK)?;
        }
        wait_for_reporting(client, None).await?;
        progress(&format!("{id} upgraded again"));
        Ok(())
    }

    /// The first `db fork` enables `fork`, and rollback is refused from then on.
    async fn first_fork(&self, fleet: &ProcessFleet, client: &Client) -> TestResult {
        let forked = self.run(&["db", "fork", MEMORY, FORK])?;
        assert!(forked.status.success(), "{}", printed(&forked));
        assert_eq!(enabled(client).await?, [Feature::Fork]);
        self.rollback_refused(fleet, "fork")?;
        // Never written, so the reinstall that ends the rolling upgrade has nothing to catch up
        // on in it, and mustn't wait for it.
        self.ok(&["db", "create", "after-the-fork"])?;
        for location in client.list_databases().await? {
            let name = location.database.as_str();
            if name == "after-the-fork" || name == FORK {
                assert_ne!(location.incarnation, Incarnation::LEGACY, "{name}");
            } else {
                assert_eq!(location.incarnation, Incarnation::LEGACY, "{name}");
            }
        }
        progress("the first fork enabled fork");
        Ok(())
    }

    /// `agent renew` gives the agent this release's defaults, and the first `agent delegate`
    /// enables `delegation`.
    async fn first_delegation(&self, fleet: &ProcessFleet, client: &Client) -> TestResult {
        let tls = path_str(&self.tls)?;
        let renewed = self.run(&["agent", "renew", AGENT, "--authority", tls])?;
        assert!(renewed.status.success(), "{}", printed(&renewed));
        assert!(
            !stderr(&renewed).contains("can delegate after"),
            "{}",
            printed(&renewed)
        );
        assert_eq!(self.claims(AGENT)?["fleetring.kind"], "agent");
        assert_eq!(enabled(client).await?, [Feature::Fork]);
        let delegated = self.run(&["agent", "delegate", "42.c7"])?;
        assert!(delegated.status.success(), "{}", printed(&delegated));
        assert_eq!(enabled(client).await?, [Feature::Fork, Feature::Delegation]);
        self.rollback_refused(fleet, "")?;
        progress("the first delegation enabled delegation");
        Ok(())
    }

    /// The previous release's token still works, `agent remove` revokes the child as its
    /// ancestor, and the fork is independent of its source.
    async fn afterwards(&self, client: &Client) -> TestResult {
        self.credentials_work("once both features are enabled")?;
        let child_token = fs::read_to_string(self.agent_dir("42.c7").join("token"))?;
        let removed = self.run(&["agent", "remove", "42.c7", "--yes"])?;
        assert!(removed.status.success(), "{}", printed(&removed));
        let dir = self.agent_dir(AGENT);
        let ca = path_str(&dir.join("ca.crt"))?.to_owned();
        let cert = path_str(&dir.join("client.agent-42.crt"))?.to_owned();
        let key = path_str(&dir.join("client.agent-42.key"))?.to_owned();
        let token = child_token.trim().to_owned();
        wait_for("the child's token is refused", || {
            let refused = self
                .run(&[
                    "db", "list", "--ca", &ca, "--cert", &cert, "--key", &key, "--token", &token,
                ])
                .ok()?;
            (!refused.status.success()).then_some(())
        })?;

        self.ok(&[
            "db",
            "exec",
            FORK,
            "INSERT INTO notes (body) VALUES ('only in the fork')",
        ])?;
        // Each holds what was written before the fork, and only its own writes since.
        let source = self.ok(&["db", "query", MEMORY, "SELECT body FROM notes"])?;
        let fork = self.ok(&["db", "query", FORK, "SELECT body FROM notes"])?;
        for both in [
            "from the previous release",
            "while a member runs the previous release",
        ] {
            assert!(source.contains(both) && fork.contains(both), "{both}");
        }
        assert!(
            source.contains("once both features are enabled"),
            "{source}"
        );
        assert!(!fork.contains("once both features are enabled"), "{fork}");
        assert!(fork.contains("only in the fork"), "{fork}");
        assert!(!source.contains("only in the fork"), "{source}");
        assert_eq!(enabled(client).await?, [Feature::Fork, Feature::Delegation]);
        Ok(())
    }

    /// Asserts that none of this build's new features is enabled, so rollback is allowed.
    async fn nothing_enabled(&self, client: &Client) -> TestResult {
        let enabled = enabled(client).await?;
        assert!(enabled.is_empty(), "{enabled:?}");
        Ok(())
    }

    /// `upgrade --rollback` on [`ROLLED_BACK`] is refused, naming a feature (`feature`, if not
    /// empty), and leaves its binary as it was.
    fn rollback_refused(&self, fleet: &ProcessFleet, feature: &str) -> TestResult {
        let node = &fleet.nodes[ROLLED_BACK];
        let running = fs::read(&node.bin)?;
        let refused = fleet
            .command(&node.bin, &self.path)
            .args(["upgrade", "--rollback", "--config"])
            .arg(&node.config)
            .output()?;
        assert_eq!(refused.status.code(), Some(1), "{}", printed(&refused));
        assert!(
            stderr(&refused).contains(&format!("lacks the fleet feature '{feature}"))
                || (feature.is_empty() && stderr(&refused).contains("lacks the fleet feature")),
            "{}",
            printed(&refused)
        );
        assert_eq!(
            fs::read(&node.bin)?,
            running,
            "the rollback swapped the binary"
        );
        Ok(())
    }

    /// The previous release's token reads the fleet as the test principal, and agent `42` writes
    /// and reads its database, with this build's command line.
    fn credentials_work(&self, when: &str) -> TestResult {
        let listed = self.run(&["db", "list", "--token", &self.token])?;
        assert!(listed.status.success(), "{when}: {}", printed(&listed));
        assert!(
            stdout(&listed).contains(MEMORY),
            "{when}: {}",
            printed(&listed)
        );
        self.remember(Path::new(env!("CARGO_BIN_EXE_fleetring")), when)?;
        Ok(())
    }

    /// Agent `42` writes `body` to its database and reads it back, with the command line `bin`.
    fn remember(&self, bin: &Path, body: &str) -> TestResult {
        let insert = format!("INSERT INTO notes (body) VALUES ('{body}')");
        self.ok_with(bin, &["db", "exec", MEMORY, &insert, "--agent", AGENT])?;
        let read = self.ok_with(
            bin,
            &[
                "db",
                "query",
                MEMORY,
                "SELECT body FROM notes",
                "--agent",
                AGENT,
            ],
        )?;
        assert!(read.contains(body), "{read}");
        Ok(())
    }

    /// The claims of agent `id`'s token, unverified.
    fn claims(&self, id: &str) -> TestResult<serde_json::Map<String, Value>> {
        use base64::Engine as _;
        let token = fs::read_to_string(self.agent_dir(id).join("token"))?;
        let payload = token.trim().split('.').nth(1).ok_or("no claims")?;
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload)?;
        match serde_json::from_slice(&decoded)? {
            Value::Object(claims) => Ok(claims),
            other => Err(format!("claims aren't an object: {other}").into()),
        }
    }

    /// Where agent `id`'s identity lives: the context `fleet`'s agents directory.
    fn agent_dir(&self, id: &str) -> PathBuf {
        self.home
            .join("data")
            .join("fleetring")
            .join("agents")
            .join("fleet")
            .join(id)
    }

    /// Runs this build with `args`, in the checks' own home.
    fn run(&self, args: &[&str]) -> TestResult<Output> {
        self.run_with(Path::new(env!("CARGO_BIN_EXE_fleetring")), args)
    }

    /// Runs `bin` with `args`, in the checks' own home, where the context and the agents are.
    fn run_with(&self, bin: &Path, args: &[&str]) -> TestResult<Output> {
        let mut command = Command::new(bin);
        crate::support::isolate(&mut command);
        Ok(crate::support::output_copied(
            command
                .args(args)
                .env("HOME", &self.home)
                .env("XDG_CONFIG_HOME", self.home.join("config"))
                .env("XDG_DATA_HOME", self.home.join("data"))
                .env("PATH", &self.path)
                .env_remove("FLEETRING_BASE_URL")
                .env_remove("FLEETRING_RELEASES_URL"),
        )?)
    }

    /// Runs this build with `args`, expecting success; returns its stdout.
    fn ok(&self, args: &[&str]) -> TestResult<String> {
        self.ok_with(Path::new(env!("CARGO_BIN_EXE_fleetring")), args)
    }

    /// Runs `bin` with `args`, expecting success; returns its stdout.
    fn ok_with(&self, bin: &Path, args: &[&str]) -> TestResult<String> {
        let output = self.run_with(bin, args)?;
        if !output.status.success() {
            return Err(format!("{} {args:?} failed: {}", bin.display(), printed(&output)).into());
        }
        Ok(stdout(&output))
    }
}

/// Waits until every member reports this build's features, or, with `old`, until `old` reports
/// none of them and the others all.
async fn wait_for_reporting(client: &Client, old: Option<&NodeId>) -> TestResult {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if let Ok(view) = client.status().await {
            let done = view.members().all(|(member, _)| {
                let software = member.software();
                let new = NEW
                    .into_iter()
                    .all(|feature| software.is_some_and(|s| s.supports(feature)));
                let none = NEW
                    .into_iter()
                    .all(|feature| !software.is_some_and(|s| s.supports(feature)));
                if Some(member.id()) == old { none } else { new }
            });
            if done {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() > deadline {
            return Err(format!("members never reported their features (old: {old:?})").into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// This build's new features the fleet has enabled.
async fn enabled(client: &Client) -> TestResult<Vec<Feature>> {
    let view = client.status().await?;
    Ok(NEW
        .into_iter()
        .filter(|feature| view.features().iter().any(|name| name == feature.as_str()))
        .collect())
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn path_str(path: &Path) -> TestResult<&str> {
    path.to_str()
        .ok_or_else(|| format!("{} isn't UTF-8", path.display()).into())
}
