//! A fleet over real TCP with mutual TLS, a router in front of it, and an HTTP client for it.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Millis};
use fleetringd::auth::{BearerToken, Scope, TokenSigner, TokenSpec};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::{Client, DatabaseSpec};
use fleetringd::engine::{Statement, Value as SqlValue};
use fleetringd::router::{Router, RouterConfig};
use fleetringd::{Config, Daemon};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::{HeaderMap, Method, Request, StatusCode};
use hyper_util::client::legacy::Client as HttpClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::support::Tls;

pub(crate) type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only.
pub(crate) const HANG_GUARD: Duration = Duration::from_secs(60);

/// The principals every data node knows, besides `tester`: the router, which forwards its
/// callers' tokens and may do nothing itself, and `app`, which acts through tokens.
const PRINCIPALS: &str = "\n[[principal]]\nname = \"router\"\nscopes = []\nforward_tokens = true\n\
     \n[[principal]]\nname = \"app\"\n\
     \n[[principal]]\nname = \"reader\"\nscopes = [\"read:*\"]\n";

/// How the fleet and its router are set up.
#[derive(Debug, Clone)]
pub(crate) struct Options {
    pub(crate) data_nodes: usize,
    /// `[sessions] transaction_timeout_ms` on every data node.
    pub(crate) transaction_timeout_ms: u64,
    /// More `[sessions]` lines for every data node.
    pub(crate) sessions: String,
    /// More tables for every node, after `[sessions]`.
    pub(crate) node_tables: String,
    /// The router's `[streams]` and `[routing]` tables, and any other it needs.
    pub(crate) router_tables: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            data_nodes: 3,
            transaction_timeout_ms: 30_000,
            sessions: String::new(),
            node_tables: String::new(),
            router_tables: String::new(),
        }
    }
}

/// A control node, data nodes, a router, and an administrator's client.
pub(crate) struct Fleet {
    /// Holds the fleet's files until it stops.
    _dir: TempDir,
    pub(crate) tls: Tls,
    control: Daemon,
    data: Vec<Daemon>,
    router: Router,
    pub(crate) admin: Client,
    pub(crate) control_address: SocketAddr,
    signer: TokenSigner,
    http: HttpClient<HttpConnector, Full<Bytes>>,
}

impl Fleet {
    /// Starts a fleet with `options`, and waits until every data node is a member.
    pub(crate) async fn start(options: Options) -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let control = Daemon::start(Config::from_toml(&node_config(
            dir.path(),
            &tls,
            "control",
            "control",
            None,
            &options,
        )?)?)
        .await?;
        let address = control.control_address().ok_or("no control address")?;
        let mut data = Vec::new();
        for index in 1..=options.data_nodes {
            data.push(
                Daemon::start(Config::from_toml(&node_config(
                    dir.path(),
                    &tls,
                    &format!("data-{index}"),
                    "data",
                    Some(address),
                    &options,
                )?)?)
                .await?,
            );
        }
        let admin = Client::new(address.to_string(), tls.client_network("tester")?);
        tokio::time::timeout(HANG_GUARD, async {
            loop {
                match admin.status().await {
                    Ok(view) if view.members().len() == options.data_nodes => {
                        return Ok::<_, tonic::Status>(());
                    }
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await??;
        let router = start_router(dir.path(), &tls, address, &options.router_tables).await?;
        let signer = tls.authority().token_signer()?;
        Ok(Self {
            _dir: dir,
            tls,
            control,
            data,
            router,
            admin,
            control_address: address,
            signer,
            http: HttpClient::builder(TokioExecutor::new()).build_http(),
        })
    }

    /// The router's address.
    pub(crate) fn router(&self) -> SocketAddr {
        self.router.listen_address()
    }

    /// Creates `database` with `replicas` replicas and a table `t (id INTEGER PRIMARY KEY, v)`.
    pub(crate) async fn create(&self, database: &str, replicas: u8) -> TestResult {
        let name: DatabaseName = database.parse()?;
        let factor = ReplicationFactor::new(replicas).ok_or("no replicas")?;
        self.admin
            .create_database_with(&name, DatabaseSpec::new(factor))
            .await?;
        self.admin
            .execute(
                &name,
                Statement::new("CREATE TABLE t (id INTEGER PRIMARY KEY, v)"),
            )
            .await?;
        Ok(())
    }

    /// The values of column `v` of `database`'s table `t`, read through the administrator.
    pub(crate) async fn values(&self, database: &str) -> Result<Vec<SqlValue>, Box<dyn Error>> {
        let rows = self
            .admin
            .query(
                &database.parse()?,
                Statement::new("SELECT v FROM t ORDER BY id"),
            )
            .await?;
        Ok(rows
            .rows
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .collect())
    }

    /// A token for `principal` with `scopes`, valid for an hour.
    pub(crate) fn token(
        &self,
        principal: &str,
        scopes: &[&str],
        id: &str,
    ) -> Result<String, Box<dyn Error>> {
        issue(&self.signer, principal, scopes, id)
    }

    /// The node ID of `database`'s primary.
    pub(crate) async fn primary(&self, database: &str) -> Result<String, Box<dyn Error>> {
        let location = self.admin.locate(&database.parse()?).await?;
        Ok(location.primary.ok_or("no primary")?.to_string())
    }

    /// Posts `body` to the router at `path`, with `token` if there is one.
    pub(crate) async fn post(
        &self,
        path: &str,
        token: Option<&str>,
        body: &Value,
    ) -> Result<Answer, Box<dyn Error>> {
        send(
            &self.http,
            self.router(),
            Method::POST,
            path,
            token,
            None,
            body,
        )
        .await
    }

    /// Posts `body` to the router at `path` with a `Host` header.
    pub(crate) async fn post_to_host(
        &self,
        host: &str,
        path: &str,
        token: Option<&str>,
        body: &Value,
    ) -> Result<Answer, Box<dyn Error>> {
        send(
            &self.http,
            self.router(),
            Method::POST,
            path,
            token,
            Some(host),
            body,
        )
        .await
    }

    /// Stops everything.
    pub(crate) async fn stop(self) {
        self.router.stop().await;
        for node in self.data {
            node.run_until(async {}).await;
        }
        self.control.run_until(async {}).await;
    }
}

/// A router answer: its status, headers, and body as JSON (or the lines of a cursor's body).
#[derive(Debug)]
pub(crate) struct Answer {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Value,
    pub(crate) text: String,
}

impl Answer {
    /// The pipeline's baton.
    pub(crate) fn baton(&self) -> Option<String> {
        self.body["baton"].as_str().map(str::to_owned)
    }

    /// The `index`th result of a pipeline.
    pub(crate) fn result(&self, index: usize) -> &Value {
        &self.body["results"][index]
    }

    /// The error code of the `index`th result, which must have failed.
    pub(crate) fn error_code(&self, index: usize) -> String {
        let result = self.result(index);
        assert_eq!(result["type"], "error", "{result}");
        result["error"]["code"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }
}

/// Sends a request to a router at `router`.
pub(crate) async fn send(
    http: &HttpClient<HttpConnector, Full<Bytes>>,
    router: SocketAddr,
    method: Method,
    path: &str,
    token: Option<&str>,
    host: Option<&str>,
    body: &Value,
) -> Result<Answer, Box<dyn Error>> {
    let mut request = Request::builder()
        .method(method)
        .uri(format!("http://{router}{path}"))
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    if let Some(host) = host {
        request = request.header("host", host);
    }
    let request = request.body(Full::new(Bytes::from(serde_json::to_vec(body)?)))?;
    let response = tokio::time::timeout(HANG_GUARD, http.request(request)).await??;
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await?.to_bytes();
    let text = String::from_utf8(bytes.to_vec())?;
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Ok(Answer {
        status,
        headers,
        body,
        text,
    })
}

/// A token for `principal` with `scopes`, valid for an hour, signed by `signer`.
pub(crate) fn issue(
    signer: &TokenSigner,
    principal: &str,
    scopes: &[&str],
    id: &str,
) -> Result<String, Box<dyn Error>> {
    let now = SystemClock.now();
    let token: BearerToken = signer.issue(&TokenSpec::new(
        principal.parse()?,
        scopes
            .iter()
            .map(|scope| scope.parse::<Scope>())
            .collect::<Result<_, _>>()?,
        id.parse()?,
        now,
        now.saturating_add(Millis::new(3_600_000)),
    ))?;
    Ok(token.expose().to_owned())
}

/// Starts a router for the fleet whose control node is at `control`, with the client
/// certificate of the principal `router`, and `tables` appended to its configuration.
pub(crate) async fn start_router(
    dir: &Path,
    tls: &Tls,
    control: SocketAddr,
    tables: &str,
) -> Result<Router, Box<dyn Error>> {
    let stem = "client.router";
    if !tls.dir().join(format!("{stem}.crt")).exists() {
        tls.authority()
            .issue_client(&"router".parse()?, Tls::validity())?
            .write(tls.dir(), stem)?;
    }
    let text = format!(
        "listen = \"127.0.0.1:0\"\n\n[tls]\nca = {:?}\ncert = {:?}\nkey = {:?}\n\n[control]\naddress = \"{control}\"\n{tables}\n",
        tls.dir().join("ca.crt"),
        tls.dir().join(format!("{stem}.crt")),
        tls.dir().join(format!("{stem}.key")),
    );
    let _ = dir;
    Ok(Router::start(RouterConfig::from_toml(&text)?).await?)
}

/// A data or control node's configuration.
fn node_config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: Option<SocketAddr>,
    options: &Options,
) -> Result<String, Box<dyn Error>> {
    let address = control.map_or_else(String::new, |address| format!("address = \"{address}\"\n"));
    Ok(format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         \n[membership]\nheartbeat_interval_ms = 100\n\n[control]\n{address}\
         \n[sessions]\ntransaction_timeout_ms = {}\n{}\n{}\n{}{PRINCIPALS}",
        dir.join(id),
        options.transaction_timeout_ms,
        options.sessions,
        options.node_tables,
        tls.node_table(id)?,
    ))
}

/// A pipeline request body.
pub(crate) fn pipeline(baton: Option<&str>, requests: Value) -> Value {
    json!({ "baton": baton, "requests": requests })
}

/// An `execute` request for `sql` with positional `args`.
pub(crate) fn execute(sql: &str, args: Value) -> Value {
    json!({ "type": "execute", "stmt": { "sql": sql, "args": args } })
}

/// A Hrana integer value.
pub(crate) fn integer(value: i64) -> Value {
    json!({ "type": "integer", "value": value.to_string() })
}

/// A Hrana text value.
pub(crate) fn text(value: &str) -> Value {
    json!({ "type": "text", "value": value })
}

/// The `close` request.
pub(crate) fn close() -> Value {
    json!({ "type": "close" })
}
