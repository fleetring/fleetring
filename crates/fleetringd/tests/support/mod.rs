//! A test authority: every daemon, node, and client the integration tests run gets a certificate
//! from it, so they run over TLS as a deployed fleet does (FR-046).

#![allow(
    dead_code,
    reason = "each test binary uses the part of the fixture it needs"
)]

use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring_control::Timestamp;
use fleetringd::Network;
use fleetringd::tls::{Authority, Identity, Issued, Security, Validity};
use fleetringd::transport::TransportConfig;

pub(crate) mod mcp;

/// The fleet every test authority issues for.
pub(crate) const FLEET: &str = "test";

/// The host every test daemon listens on, and so the one its certificate names.
pub(crate) const HOST: &str = "127.0.0.1";

/// The principal the tests' clients act as.
pub(crate) const PRINCIPAL: &str = "tester";

/// The `[[principal]]` entry every test daemon gets: [`PRINCIPAL`] may administer, write, read,
/// and stream every database, and run the fleet operations.
pub(crate) const OPERATOR: &str =
    "\n[[principal]]\nname = \"tester\"\nscopes = [\"admin:*\", \"stream:*\", \"fleet\"]\n";

/// The `[admin]` table every test daemon gets: its admin listener on a port of its own, so that
/// daemons running at once never contend for the default 7401.
pub(crate) const ADMIN: &str = "\n[admin]\nlisten = \"127.0.0.1:0\"\n";

/// Every variable the command line reads to find a fleet, an identity or a context. A test that
/// runs the binary starts from none of them (ADR-0043, point 4.4).
pub(crate) const CLIENT_VARS: [&str; 11] = [
    "FLEETRING_AGENT",
    "FLEETRING_CONTEXT",
    "FLEETRING_CONTROL",
    "FLEETRING_TLS_DIR",
    "FLEETRING_AS",
    "FLEETRING_CA",
    "FLEETRING_CERT",
    "FLEETRING_KEY",
    "FLEETRING_TOKEN",
    "FLEETRING_TOKEN_FILE",
    "FLEETRING_LOG_FORMAT",
];

/// A home that nothing creates: the binary run by [`fleetring`] has its home, its configuration
/// and data directories, its contexts and its agents here, never the developer's own.
pub(crate) fn scratch_home() -> PathBuf {
    Path::new(env!("CARGO_TARGET_TMPDIR")).join("no-home")
}

/// Points `command` at [`scratch_home`] and clears every variable in [`CLIENT_VARS`], so that it
/// never reads the developer's contexts or sends a credential from their shell. Call it before
/// setting the variables a test wants.
pub(crate) fn isolate(command: &mut std::process::Command) -> &mut std::process::Command {
    let home = scratch_home();
    command
        .env("HOME", &home)
        .env("XDG_CONFIG_HOME", home.join("xdg"))
        .env("XDG_DATA_HOME", home.join("xdg-data"))
        .env("APPDATA", home.join("appdata"))
        .env("LOCALAPPDATA", home.join("localappdata"))
        .env("USERPROFILE", &home);
    for variable in CLIENT_VARS {
        command.env_remove(variable);
    }
    command
}

/// The binary, isolated by [`isolate`]. Every integration test runs it through this (or through
/// [`isolate`]); `tests/contexts.rs` fails if one doesn't.
pub(crate) fn fleetring() -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_fleetring"));
    isolate(&mut command);
    command
}

/// [`fleetring`] for a test that waits on the process with tokio.
pub(crate) fn fleetring_tokio() -> tokio::process::Command {
    tokio::process::Command::from(fleetring())
}

/// A certificate authority written under a test's directory.
#[derive(Debug)]
pub(crate) struct Tls {
    dir: PathBuf,
    authority: Authority,
}

impl Tls {
    /// Creates an authority for [`FLEET`] under `root/tls`, with `ca.crt` and `ca.key`, and
    /// `client.crt` and `client.key` for the principal [`PRINCIPAL`], so that the directory
    /// serves as `FLEETRING_TLS_DIR` for the command line.
    pub(crate) fn create(root: &Path) -> Result<Self, Box<dyn Error>> {
        let dir = root.join("tls");
        let authority = Authority::create(FLEET.parse()?, Self::validity())?;
        authority.write(&dir)?;
        authority
            .issue_client(&PRINCIPAL.parse()?, Self::validity())?
            .write(&dir, "client")?;
        Ok(Self { dir, authority })
    }

    /// A validity rustls accepts whatever the wall clock says: from the epoch, for a century.
    pub(crate) fn validity() -> Validity {
        Validity::new(
            Timestamp::from_millis(0),
            Duration::from_secs(100 * 365 * 24 * 60 * 60),
        )
    }

    /// A validity that ended long ago.
    pub(crate) fn expired() -> Validity {
        Validity::new(Timestamp::from_millis(0), Duration::from_secs(24 * 60 * 60))
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn authority(&self) -> &Authority {
        &self.authority
    }

    /// Issues a certificate for the node `id` at [`HOST`].
    pub(crate) fn issue_node(&self, id: &str) -> Result<Issued, Box<dyn Error>> {
        Ok(self
            .authority
            .issue_node(&id.parse()?, &[HOST.to_owned()], Self::validity())?)
    }

    /// Issues `node.<id>.crt` and `node.<id>.key` for [`HOST`], and returns the `[tls]` table
    /// that names them, to append to the node's configuration.
    pub(crate) fn node_table(&self, id: &str) -> Result<String, Box<dyn Error>> {
        Ok(format!("{}{ADMIN}", self.node_tls_table(id)?))
    }

    /// Issues the node's files as [`node_table`](Self::node_table) does, and returns the
    /// `[tls]` table without the [`ADMIN`] table, for a configuration that sets its own.
    pub(crate) fn node_tls_table(&self, id: &str) -> Result<String, Box<dyn Error>> {
        let stem = format!("node.{id}");
        self.issue_node(id)?.write(&self.dir, &stem)?;
        Ok(self.tls_table(&stem))
    }

    /// The `[tls]` table naming `<stem>.crt` and `<stem>.key` in the authority's directory,
    /// followed by the [`OPERATOR`] `[[principal]]` entry and the [`ADMIN`] table.
    pub(crate) fn table(&self, stem: &str) -> String {
        format!("{}{ADMIN}", self.tls_table(stem))
    }

    /// The `[tls]` table naming `<stem>.crt` and `<stem>.key` in the authority's directory,
    /// followed by the [`OPERATOR`] `[[principal]]` entry, without an `[admin]` table.
    pub(crate) fn tls_table(&self, stem: &str) -> String {
        format!(
            "\n[tls]\nca = {:?}\ncert = {:?}\nkey = {:?}\n{OPERATOR}",
            self.dir.join("ca.crt"),
            self.dir.join(format!("{stem}.crt")),
            self.dir.join(format!("{stem}.key")),
        )
    }

    /// An in-memory identity for the node `id`.
    pub(crate) fn node_identity(&self, id: &str) -> Result<Identity, Box<dyn Error>> {
        Ok(Identity::from_issued(
            self.authority.certificate_pem(),
            &self.issue_node(id)?,
        )?)
    }

    /// Real TCP with default timeouts, as the node `id`.
    pub(crate) fn node_network(&self, id: &str) -> Result<Network, Box<dyn Error>> {
        Ok(Network::default().with_security(Security::Tls(self.node_identity(id)?)))
    }

    /// An in-memory identity for a client acting as `principal`.
    pub(crate) fn client_identity(&self, principal: &str) -> Result<Identity, Box<dyn Error>> {
        let issued = self
            .authority
            .issue_client(&principal.parse()?, Self::validity())?;
        Ok(Identity::from_issued(
            self.authority.certificate_pem(),
            &issued,
        )?)
    }

    /// Real TCP with default timeouts, as `principal`.
    pub(crate) fn client_network(&self, principal: &str) -> Result<Network, Box<dyn Error>> {
        self.client_network_with(principal, TransportConfig::default())
    }

    /// Real TCP with `transport`'s timeouts, as `principal`.
    pub(crate) fn client_network_with(
        &self,
        principal: &str,
        transport: TransportConfig,
    ) -> Result<Network, Box<dyn Error>> {
        Ok(Network::tcp(transport).with_security(Security::Tls(self.client_identity(principal)?)))
    }
}
