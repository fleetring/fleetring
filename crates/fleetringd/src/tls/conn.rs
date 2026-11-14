//! Connections: the raw streams of each network, with TLS on top when the daemon has an
//! identity, and the peer each accepted connection authenticated as.

use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use rustls_pki_types::CertificateDer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use tonic::transport::server::Connected;

use crate::tls::names::{FleetName, SubjectUri};
use crate::tls::{Peer, Security};

/// How long a handshake may take before the connection is dropped. It bounds what a peer that
/// connects and goes silent can hold.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A connection as the network delivers it, before any TLS.
#[derive(Debug)]
pub enum Raw {
    /// A real TCP connection.
    Tcp(tokio::net::TcpStream),
    /// A connection on turmoil's simulated network.
    #[cfg(feature = "simulation")]
    Simulated(turmoil::net::TcpStream),
}

macro_rules! delegate_io {
    ($type:ty, $($variant:path),+) => {
        impl AsyncRead for $type {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                match self.get_mut() {
                    $($variant(inner) => Pin::new(inner).poll_read(cx, buf),)+
                }
            }
        }

        impl AsyncWrite for $type {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                match self.get_mut() {
                    $($variant(inner) => Pin::new(inner).poll_write(cx, buf),)+
                }
            }

            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                match self.get_mut() {
                    $($variant(inner) => Pin::new(inner).poll_flush(cx),)+
                }
            }

            fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                match self.get_mut() {
                    $($variant(inner) => Pin::new(inner).poll_shutdown(cx),)+
                }
            }
        }
    };
}

#[cfg(feature = "simulation")]
delegate_io!(Raw, Raw::Tcp, Raw::Simulated);
#[cfg(not(feature = "simulation"))]
delegate_io!(Raw, Raw::Tcp);

/// A connection ready for gRPC: plaintext, or TLS over the raw stream.
#[derive(Debug)]
pub enum Conn {
    /// A plaintext connection, when the daemon runs with `insecure_plaintext`.
    Plain(Raw),
    /// A TLS connection; both ends presented a certificate.
    Tls(Box<TlsStream<Raw>>),
}

delegate_io!(Conn, Conn::Plain, Conn::Tls);

/// An accepted connection with the peer it authenticated as.
///
/// tonic inserts the [`Peer`] into the extensions of every request on the connection.
#[derive(Debug)]
pub struct Accepted {
    conn: Conn,
    peer: Peer,
}

impl Accepted {
    /// Returns the peer the connection authenticated as.
    #[must_use]
    pub fn peer(&self) -> &Peer {
        &self.peer
    }
}

impl Connected for Accepted {
    type ConnectInfo = Peer;

    fn connect_info(&self) -> Peer {
        self.peer.clone()
    }
}

impl AsyncRead for Accepted {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().conn).poll_read(cx, buf)
    }
}

impl AsyncWrite for Accepted {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().conn).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().conn).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().conn).poll_shutdown(cx)
    }
}

/// The error returned when a handshake fails or the peer isn't acceptable.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HandshakeError {
    /// The TLS handshake failed: no certificate, an untrusted or expired one, or a protocol
    /// error.
    #[error("TLS handshake failed")]
    Tls(#[source] io::Error),
    /// The handshake didn't finish within the handshake bound (10 s).
    #[error("TLS handshake timed out after {HANDSHAKE_TIMEOUT:?}")]
    Timeout,
    /// The peer's certificate chains to a trusted authority but names nothing this fleet
    /// accepts.
    #[error("the peer's certificate names no node or principal of this fleet: {reason}")]
    Subject {
        /// Why the subject is refused, without any certificate contents.
        reason: String,
    },
}

impl Security {
    /// Finishes accepting `raw`: a TLS handshake that requires a certificate chaining to a
    /// trusted authority, or nothing, in plaintext.
    ///
    /// # Errors
    ///
    /// [`HandshakeError`] if the handshake fails or the peer's certificate names nothing this
    /// fleet accepts.
    pub async fn accept(&self, raw: Raw) -> Result<Accepted, HandshakeError> {
        match self {
            Self::Plaintext => Ok(Accepted {
                conn: Conn::Plain(raw),
                peer: Peer::Plaintext,
            }),
            Self::Tls(identity) => {
                let loaded = identity.current();
                let acceptor = TlsAcceptor::from(loaded.server_config());
                let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(raw))
                    .await
                    .map_err(|_| HandshakeError::Timeout)?
                    .map_err(HandshakeError::Tls)?;
                let certificates = stream.get_ref().1.peer_certificates().unwrap_or_default();
                let peer = peer_of(certificates, loaded.fleet())?;
                Ok(Accepted {
                    conn: Conn::Tls(Box::new(TlsStream::Server(stream))),
                    peer,
                })
            }
        }
    }

    /// Finishes connecting `raw` to the server at `address` (`host:port`): a TLS handshake
    /// that verifies the server's certificate against the trusted authorities and its host,
    /// or nothing, in plaintext.
    ///
    /// # Errors
    ///
    /// An I/O error if the host isn't a valid server name or the handshake fails.
    pub async fn connect(&self, address: &str, raw: Raw) -> io::Result<Conn> {
        match self {
            Self::Plaintext => Ok(Conn::Plain(raw)),
            Self::Tls(identity) => {
                let connector = TlsConnector::from(identity.current().client_config());
                let name = server_name(address)?;
                let stream = connector.connect(name, raw).await?;
                Ok(Conn::Tls(Box::new(TlsStream::Client(stream))))
            }
        }
    }
}

/// Why [`peer_of`] refuses an issuer certificate, on every listener (ADR-0047, point 1.5).
pub(crate) const ISSUER_REFUSED: &str = "an issuer certificate can't open a connection";

/// The node or principal a peer's verified certificate chain names, if it is one of `fleet`'s.
///
/// # Errors
///
/// [`HandshakeError::Subject`] if there is no certificate, or its subject isn't a node or a
/// principal of `fleet`: an authority's or an issuer's certificate is refused.
pub(crate) fn peer_of(
    certificates: &[CertificateDer<'_>],
    fleet: &FleetName,
) -> Result<Peer, HandshakeError> {
    let end_entity = certificates
        .first()
        .ok_or_else(|| HandshakeError::Subject {
            reason: "no certificate".to_owned(),
        })?;
    let subject =
        SubjectUri::of_certificate(end_entity).map_err(|error| HandshakeError::Subject {
            reason: error.to_string(),
        })?;
    if subject.fleet() != fleet {
        return Err(HandshakeError::Subject {
            reason: format!("fleet '{}' is not this fleet '{fleet}'", subject.fleet()),
        });
    }
    match subject {
        SubjectUri::Node { id, .. } => Ok(Peer::Node(id)),
        SubjectUri::Principal { name, .. } => Ok(Peer::Principal(name)),
        authority @ SubjectUri::Authority { .. } => Err(HandshakeError::Subject {
            reason: format!("{authority} is an authority"),
        }),
        // An issuer certificate has no extended key usage, which rustls-webpki reads as any
        // use, so the handshake admits it; its subject kind is what refuses it (ADR-0047,
        // point 1.5).
        SubjectUri::Issuer { .. } => Err(HandshakeError::Subject {
            reason: ISSUER_REFUSED.to_owned(),
        }),
    }
}

/// The name a client verifies the server at `address` (`host:port`) under: its IP address, or
/// its DNS name.
fn server_name(address: &str) -> io::Result<rustls_pki_types::ServerName<'static>> {
    let host = match address.strip_prefix('[') {
        Some(rest) => rest.split_once(']').map_or(rest, |(host, _)| host),
        None => address.rsplit_once(':').map_or(address, |(host, _)| host),
    };
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(rustls_pki_types::ServerName::IpAddress(ip.into()));
    }
    rustls_pki_types::ServerName::try_from(host.to_owned()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("'{host}' is not a valid server name"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_names_come_from_the_host_part() {
        let name = server_name("127.0.0.1:7400").expect("ipv4");
        assert!(matches!(name, rustls_pki_types::ServerName::IpAddress(_)));
        let name = server_name("[::1]:7400").expect("ipv6");
        assert!(matches!(name, rustls_pki_types::ServerName::IpAddress(_)));
        let name = server_name("node-1:7400").expect("dns");
        assert!(matches!(name, rustls_pki_types::ServerName::DnsName(_)));
        assert!(server_name("bad host:7400").is_err());
    }

    #[test]
    fn peer_of_judges_every_subject_kind() {
        use fleetring_control::Timestamp;
        use rustls_pki_types::pem::PemObject as _;

        use crate::tls::{Authority, Validity};

        let fleet: FleetName = "lab".parse().expect("fleet name");
        let validity = Validity::new(Timestamp::ZERO, Duration::from_secs(24 * 60 * 60));
        let authority = Authority::create(fleet.clone(), validity).expect("authority");
        let der =
            |pem: &str| CertificateDer::from_pem_slice(pem.as_bytes()).expect("a PEM certificate");
        let id = "n1".parse().expect("node id");

        let node = authority.issue_node(&id, &[], validity).expect("node");
        assert_eq!(
            peer_of(&[der(node.certificate_pem())], &fleet).expect("a node"),
            Peer::Node(id.clone())
        );
        let principal = "alice".parse().expect("principal");
        let client = authority
            .issue_client(&principal, validity)
            .expect("client");
        assert_eq!(
            peer_of(&[der(client.certificate_pem())], &fleet).expect("a principal"),
            Peer::Principal(principal)
        );

        let refusal = |certificate: CertificateDer<'_>| match peer_of(&[certificate], &fleet) {
            Err(HandshakeError::Subject { reason }) => reason,
            other => panic!("expected a subject refusal, got {other:?}"),
        };
        let issuer = authority.issue_issuer(&id, validity).expect("issuer");
        assert_eq!(refusal(der(issuer.certificate_pem())), ISSUER_REFUSED);
        assert_eq!(
            ISSUER_REFUSED,
            "an issuer certificate can't open a connection"
        );
        assert!(
            refusal(der(authority.certificate_pem())).ends_with("is an authority"),
            "the authority's own certificate"
        );
    }
}
