//! Unchanged libSQL client code through the router, against a three-node fleet: the `libsql`
//! crate's remote client, which speaks Hrana 3 over HTTP (`v3/pipeline` and `v3/cursor`).
//!
//! It also pins what ADR-0037 leaves to clients: that the client keeps the URL's path, accepts
//! results without `rows_read`, `rows_written`, and `query_duration_ms`, and reads blobs the
//! router sends without padding.

#[allow(dead_code, reason = "the router tests' fleet; these tests use part of it")]
#[path = "../../../crates/fleetringd/tests/router/fleet.rs"]
mod fleet;

use libsql::{Builder, Connection, Value, named_params, params};

use crate::fleet::{Fleet, Options, TestResult};

/// The test authority, as the daemon's own tests have it: only what [`fleet`] needs.
mod support {
    use std::error::Error;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use fleetring_control::Timestamp;
    use fleetringd::Network;
    use fleetringd::tls::{Authority, Identity, Security, Validity};

    /// The `[[principal]]` entry every node gets: `tester` administers every database.
    const OPERATOR: &str =
        "\n[[principal]]\nname = \"tester\"\nscopes = [\"admin:*\", \"stream:*\", \"fleet\"]\n";

    /// A certificate authority written under a test's directory.
    #[derive(Debug)]
    pub(crate) struct Tls {
        dir: PathBuf,
        authority: Authority,
    }

    impl Tls {
        pub(crate) fn create(root: &Path) -> Result<Self, Box<dyn Error>> {
            let dir = root.join("tls");
            let authority = Authority::create("test".parse()?, Self::validity())?;
            authority.write(&dir)?;
            Ok(Self { dir, authority })
        }

        pub(crate) fn validity() -> Validity {
            Validity::new(
                Timestamp::from_millis(0),
                Duration::from_secs(100 * 365 * 24 * 60 * 60),
            )
        }

        pub(crate) fn dir(&self) -> &Path {
            &self.dir
        }

        pub(crate) fn authority(&self) -> &Authority {
            &self.authority
        }

        /// Issues the node's certificate for 127.0.0.1, and returns the `[tls]` table naming it.
        pub(crate) fn node_table(&self, id: &str) -> Result<String, Box<dyn Error>> {
            let stem = format!("node.{id}");
            self.authority
                .issue_node(&id.parse()?, &["127.0.0.1".to_owned()], Self::validity())?
                .write(&self.dir, &stem)?;
            Ok(format!(
                "\n[tls]\nca = {:?}\ncert = {:?}\nkey = {:?}\n{OPERATOR}\n[admin]\nlisten = \"127.0.0.1:0\"\n",
                self.dir.join("ca.crt"),
                self.dir.join(format!("{stem}.crt")),
                self.dir.join(format!("{stem}.key")),
            ))
        }

        pub(crate) fn client_network(&self, principal: &str) -> Result<Network, Box<dyn Error>> {
            let issued = self
                .authority
                .issue_client(&principal.parse()?, Self::validity())?;
            let identity = Identity::from_issued(self.authority.certificate_pem(), &issued)?;
            Ok(Network::default().with_security(Security::Tls(identity)))
        }
    }
}

/// A connection to `database` through the router, with `token`, made as any application makes
/// one: a URL with the database's path and a trailing slash, and the token as its `authToken`.
async fn connect(fleet: &Fleet, database: &str, token: String) -> Result<Connection, libsql::Error> {
    let url = format!("http://{}/db/{database}/", fleet.router());
    let database = Builder::new_remote(url, token)
        .connector(hyper_014::client::HttpConnector::new())
        .build()
        .await?;
    database.connect()
}

/// The `v` column of `t`, in order of `id`.
async fn values(connection: &Connection) -> Result<Vec<String>, libsql::Error> {
    let mut rows = connection
        .query("SELECT v FROM t ORDER BY id", ())
        .await?;
    let mut values = Vec::new();
    while let Some(row) = rows.next().await? {
        values.push(row.get::<String>(0)?);
    }
    Ok(values)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_libsql_remote_client_runs_unchanged() -> TestResult {
    let fleet = Fleet::start(Options::default()).await?;
    fleet.create("app", 3).await?;
    let token = fleet.token("app", &["write:app"], "t-libsql")?;
    let connection = connect(&fleet, "app", token).await?;

    // `execute` and a query, with the last insert row ID.
    let inserted = connection
        .execute("INSERT INTO t (v) VALUES (?1)", params!["first"])
        .await?;
    assert_eq!(inserted, 1);
    assert_eq!(connection.last_insert_rowid(), 1);
    assert_eq!(values(&connection).await?, ["first"]);

    // A batch that fails inside its transaction leaves nothing.
    let failed = connection
        .execute_batch(
            "BEGIN; INSERT INTO t (v) VALUES ('second'); INSERT INTO missing VALUES (1); COMMIT;",
        )
        .await;
    // The client chains the steps on each other's success and closes the stream, which rolls
    // back the transaction the failure left open.
    assert!(failed.is_err(), "the batch fails at the missing table");
    assert_eq!(values(&connection).await?, ["first"]);

    // A multi-statement script.
    connection
        .execute_batch(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT); \
             INSERT INTO notes (body) VALUES ('a'); INSERT INTO notes (body) VALUES ('b');",
        )
        .await?;
    let mut counted = connection.query("SELECT count(*) FROM notes", ()).await?;
    let count = counted.next().await?.ok_or("a row")?.get::<i64>(0)?;
    assert_eq!(count, 2);

    // An interactive transaction that commits, and one that rolls back.
    let transaction = connection.transaction().await?;
    transaction
        .execute("INSERT INTO t (v) VALUES ('committed')", ())
        .await?;
    transaction.commit().await?;
    let transaction = connection.transaction().await?;
    transaction
        .execute("INSERT INTO t (v) VALUES ('rolled back')", ())
        .await?;
    transaction.rollback().await?;
    assert_eq!(values(&connection).await?, ["first", "committed"]);
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arguments_and_values_round_trip_through_the_libsql_client() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        ..Options::default()
    })
    .await?;
    fleet.create("values", 1).await?;
    let token = fleet.token("app", &["write:values"], "t-values")?;
    let connection = connect(&fleet, "values", token).await?;
    connection
        .execute(
            "CREATE TABLE kinds (i INTEGER, r REAL, s TEXT, b BLOB, n)",
            (),
        )
        .await?;
    connection
        .execute(
            "INSERT INTO kinds VALUES (:i, :r, :s, :b, :n)",
            named_params! {
                ":i": i64::MIN,
                ":r": 1.5_f64,
                ":s": "héllo",
                ":b": vec![0_u8, 1, 2, 255],
                ":n": Value::Null,
            },
        )
        .await?;
    let mut rows = connection
        .query("SELECT i, r, s, b, n FROM kinds WHERE s = ?1", params!["héllo"])
        .await?;
    let row = rows.next().await?.ok_or("a row")?;
    assert_eq!(row.get::<i64>(0)?, i64::MIN);
    assert!((row.get::<f64>(1)? - 1.5).abs() < f64::EPSILON);
    assert_eq!(row.get::<String>(2)?, "héllo");
    assert_eq!(row.get::<Vec<u8>>(3)?, [0, 1, 2, 255]);
    assert_eq!(row.get_value(4)?, Value::Null);
    fleet.stop().await;
    Ok(())
}
