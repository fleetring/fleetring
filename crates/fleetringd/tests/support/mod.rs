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
        let stem = format!("node.{id}");
        self.issue_node(id)?.write(&self.dir, &stem)?;
        Ok(self.table(&stem))
    }

    /// The `[tls]` table naming `<stem>.crt` and `<stem>.key` in the authority's directory,
    /// followed by the [`OPERATOR`] `[[principal]]` entry.
    pub(crate) fn table(&self, stem: &str) -> String {
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
