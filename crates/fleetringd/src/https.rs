//! The HTTP client the daemon's outbound HTTP uses, the S3 store and the webhook sink: its own
//! `hyper` over TCP, with TLS from `tokio-rustls` and `ring`, trusting the platform's roots and
//! any extra authority, and presenting a client certificate when given one.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http_body_util::Full;
use hyper::Uri;
use hyper::body::Bytes;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::RootCertStore;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use zeroize::Zeroizing;

/// Whether a plain `http://` endpoint may be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpPolicy {
    /// Only `https://`.
    #[default]
    HttpsOnly,
    /// `http://` too, for a local service and nothing else.
    AllowHttp,
}

/// The error returned when the client's TLS can't be set up.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HttpsError {
    /// The extra authority can't be read.
    #[error("authority {}: {reason}", path.display())]
    Ca {
        /// The file.
        path: PathBuf,
        /// Why.
        reason: String,
    },
    /// The client certificate or its key can't be read.
    #[error("client certificate {}: {reason}", path.display())]
    ClientCertificate {
        /// The file.
        path: PathBuf,
        /// Why.
        reason: String,
    },
    /// TLS can't be set up.
    #[error("can't set up TLS: {reason}")]
    Tls {
        /// Why.
        reason: String,
    },
}

/// The client type callers hold.
pub(crate) type HttpClient = Client<Connector, Full<Bytes>>;

/// Builds the client: TLS for `https://` with the platform's roots plus the PEM certificates in
/// `ca`, presenting the certificate chain and key in `identity` (`(certificate, key)` PEM files)
/// if given, and plain TCP for `http://` only under [`HttpPolicy::AllowHttp`].
pub(crate) fn client(
    ca: Option<&Path>,
    identity: Option<(&Path, &Path)>,
    http: HttpPolicy,
) -> Result<HttpClient, HttpsError> {
    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for error in &native.errors {
        tracing::debug!(%error, "can't load a platform root certificate");
    }
    let (_added, _ignored) = roots.add_parsable_certificates(native.certs);
    if let Some(ca) = ca {
        let invalid = |reason: String| HttpsError::Ca {
            path: ca.to_owned(),
            reason,
        };
        let pem = std::fs::read(ca).map_err(|error| invalid(error.to_string()))?;
        let certificates = CertificateDer::pem_slice_iter(&pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| invalid(format!("not a PEM certificate bundle: {error}")))?;
        if certificates.is_empty() {
            return Err(invalid("holds no certificate".to_owned()));
        }
        for certificate in certificates {
            roots
                .add(certificate)
                .map_err(|error| invalid(error.to_string()))?;
        }
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| HttpsError::Tls {
            reason: error.to_string(),
        })?
        .with_root_certificates(roots);
    let mut config = match identity {
        None => builder.with_no_client_auth(),
        Some((certificate, key)) => {
            let (chain, key) = read_identity(certificate, key)?;
            builder.with_client_auth_cert(chain, key).map_err(|error| {
                HttpsError::ClientCertificate {
                    path: certificate.to_owned(),
                    reason: error.to_string(),
                }
            })?
        }
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = Connector {
        tls: TlsConnector::from(Arc::new(config)),
        http,
    };
    Ok(Client::builder(TokioExecutor::new()).build(connector))
}

/// Reads a PEM certificate chain and its PEM private key. The key's text is zeroed once parsed.
fn read_identity(
    certificate: &Path,
    key: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), HttpsError> {
    let invalid = |path: &Path, reason: String| HttpsError::ClientCertificate {
        path: path.to_owned(),
        reason,
    };
    let pem =
        std::fs::read(certificate).map_err(|error| invalid(certificate, error.to_string()))?;
    let chain = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid(certificate, format!("not a PEM certificate chain: {error}")))?;
    if chain.is_empty() {
        return Err(invalid(certificate, "holds no certificate".to_owned()));
    }
    let pem = Zeroizing::new(std::fs::read(key).map_err(|error| invalid(key, error.to_string()))?);
    let key = PrivateKeyDer::from_pem_slice(&pem)
        .map_err(|error| invalid(key, format!("not a PEM private key: {error}")))?;
    Ok((chain, key))
}

/// Dials a request's host: TLS for `https://`, plain TCP for `http://` when allowed.
#[derive(Clone)]
pub(crate) struct Connector {
    tls: TlsConnector,
    http: HttpPolicy,
}

impl std::fmt::Debug for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("http", &self.http)
            .finish_non_exhaustive()
    }
}

impl tower::Service<Uri> for Connector {
    type Response = Stream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Stream>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let tls = self.tls.clone();
        let http = self.http;
        Box::pin(async move {
            let host = uri
                .host()
                .ok_or_else(|| io::Error::other(format!("{uri} names no host")))?;
            let host = host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .unwrap_or(host)
                .to_owned();
            let secure = match uri.scheme_str() {
                Some("https") => true,
                Some("http") if http == HttpPolicy::AllowHttp => false,
                _ => {
                    return Err(io::Error::other(format!("{uri}: only https:// is allowed")));
                }
            };
            let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });
            let tcp = TcpStream::connect((host.as_str(), port)).await?;
            tcp.set_nodelay(true)?;
            if !secure {
                return Ok(Stream::Plain(TokioIo::new(tcp)));
            }
            let name = ServerName::try_from(host).map_err(io::Error::other)?;
            let stream = tls.connect(name, tcp).await?;
            Ok(Stream::Tls(Box::new(TokioIo::new(stream))))
        })
    }
}

/// A connection to the server: plain TCP or TLS over it.
#[derive(Debug)]
pub(crate) enum Stream {
    Plain(TokioIo<TcpStream>),
    Tls(Box<TokioIo<TlsStream<TcpStream>>>),
}

impl Connection for Stream {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl Read for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl Write for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}
