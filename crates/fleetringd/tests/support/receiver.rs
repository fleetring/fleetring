//! An HTTPS receiver for webhook tests: a loopback TLS server that records what each request
//! carried, optionally requires a client certificate, and can change the certificate it presents
//! without changing its address.

#![allow(
    dead_code,
    reason = "each test binary uses the part of the receiver it needs"
)]

use std::convert::Infallible;
use std::error::Error;
use std::sync::{Arc, Mutex};

use fleetringd::tls::{Issued, SubjectUri};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::RootCertStore;
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use crate::support::HOST;

/// Whether the receiver asks the sink for a certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientAuth {
    /// It doesn't.
    None,
    /// It refuses a connection without a certificate the authority issued.
    Required,
}

/// One request the receiver took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Seen {
    /// The `Authorization` header, if there was one.
    pub(crate) authorization: Option<String>,
    /// The subject of the client certificate the connection presented, if it presented one.
    pub(crate) client: Option<String>,
    /// The request body.
    pub(crate) body: String,
}

struct State {
    authority_pem: String,
    client_auth: ClientAuth,
    acceptor: Mutex<TlsAcceptor>,
    seen: Mutex<Vec<Seen>>,
    refused: Mutex<usize>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("client_auth", &self.client_auth)
            .finish_non_exhaustive()
    }
}

/// A running receiver. It stops when dropped.
#[derive(Debug)]
pub(crate) struct Receiver {
    url: String,
    state: Arc<State>,
    task: JoinHandle<()>,
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Receiver {
    /// Starts a receiver on a loopback port, presenting `identity`, a certificate for
    /// [`HOST`]. With [`ClientAuth::Required`] it verifies client certificates against
    /// `authority_pem`.
    pub(crate) async fn start(
        identity: &Issued,
        authority_pem: &str,
        client_auth: ClientAuth,
    ) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind((HOST, 0)).await?;
        let url = format!("https://{}/changes", listener.local_addr()?);
        let state = Arc::new(State {
            authority_pem: authority_pem.to_owned(),
            client_auth,
            acceptor: Mutex::new(acceptor(identity, authority_pem, client_auth)?),
            seen: Mutex::default(),
            refused: Mutex::default(),
        });
        let task = tokio::spawn(accept(listener, Arc::clone(&state)));
        Ok(Self { url, state, task })
    }

    /// The URL to POST to.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// Presents `identity` to connections from now on.
    pub(crate) fn present(&self, identity: &Issued) -> Result<(), Box<dyn Error>> {
        let acceptor = acceptor(identity, &self.state.authority_pem, self.state.client_auth)?;
        *self.state.acceptor.lock().map_err(|_| "acceptor lock")? = acceptor;
        Ok(())
    }

    /// The requests taken so far.
    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.state.seen.lock().expect("seen lock").clone()
    }

    /// How many connections failed their handshake.
    pub(crate) fn refused(&self) -> usize {
        *self.state.refused.lock().expect("refused lock")
    }
}

fn acceptor(
    identity: &Issued,
    authority_pem: &str,
    client_auth: ClientAuth,
) -> Result<TlsAcceptor, Box<dyn Error>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?;
    let builder = match client_auth {
        ClientAuth::None => builder.with_no_client_auth(),
        ClientAuth::Required => {
            let mut roots = RootCertStore::empty();
            for certificate in CertificateDer::pem_slice_iter(authority_pem.as_bytes()) {
                roots.add(certificate?)?;
            }
            builder.with_client_cert_verifier(
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider).build()?,
            )
        }
    };
    let chain = CertificateDer::pem_slice_iter(identity.certificate_pem().as_bytes())
        .collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_slice(identity.key_pem().as_bytes())?;
    let mut config = builder.with_single_cert(chain, key)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

async fn accept(listener: TcpListener, state: Arc<State>) {
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let acceptor = state.acceptor.lock().expect("acceptor lock").clone();
            let Ok(stream) = acceptor.accept(tcp).await else {
                *state.refused.lock().expect("refused lock") += 1;
                return;
            };
            let client = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(<[_]>::first)
                .and_then(|certificate| SubjectUri::of_certificate(certificate).ok())
                .map(|subject| subject.to_string());
            let service = service_fn(move |request: Request<Incoming>| {
                let state = Arc::clone(&state);
                let client = client.clone();
                async move {
                    let authorization = request
                        .headers()
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let body = request
                        .into_body()
                        .collect()
                        .await
                        .map(|collected| {
                            String::from_utf8_lossy(&collected.to_bytes()).into_owned()
                        })
                        .unwrap_or_default();
                    state.seen.lock().expect("seen lock").push(Seen {
                        authorization,
                        client,
                        body,
                    });
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::new()))
                            .expect("a response"),
                    )
                }
            });
            let _served = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}
