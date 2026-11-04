//! The S3 store, against the in-process fake in every run and against a real endpoint when one
//! is configured.
//!
//! To run the suite against a real endpoint, set `FLEETRING_S3_TEST_ENDPOINT`,
//! `FLEETRING_S3_TEST_BUCKET`, `FLEETRING_S3_TEST_REGION`, `AWS_ACCESS_KEY_ID`, and
//! `AWS_SECRET_ACCESS_KEY` (and `FLEETRING_S3_TEST_PATH_STYLE=1` for MinIO), then run
//! `cargo test -p fleetringd --test durability -- --ignored a_real_endpoint_conforms`. It works
//! under a fresh `fleetring-test/<milliseconds>` prefix and deletes what it writes.

use std::path::Path;
use std::sync::Arc;

use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::durability::{
    CredentialSource, HttpPolicy, ObjectStore, PathStyle, S3ConfigError, S3Settings, S3Store,
    StoreError,
};

use crate::common::TestResult;
use crate::fake_s3::FakeS3;
use crate::store::conformance;

/// Writes a credentials file only its owner can read.
fn credentials_file(
    dir: &Path,
    secret: &str,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let path = dir.join("s3.toml");
    std::fs::write(
        &path,
        format!(
            "access_key_id = \"AKIAFAKEKEY\"\nsecret_access_key = \"{secret}\"\nsession_token = \"token-{secret}\"\n"
        ),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(path)
}

fn settings(endpoint: String, credentials: &Path) -> S3Settings {
    S3Settings {
        bucket: "archive".to_owned(),
        endpoint,
        region: "us-east-1".to_owned(),
        path_style: PathStyle::Path,
        credentials: CredentialSource::File(credentials.to_owned()),
        ca: None,
        http: HttpPolicy::AllowHttp,
    }
}

/// A store over a fresh fake, and the fake.
async fn faked(dir: &Path) -> Result<(S3Store, FakeS3), Box<dyn std::error::Error>> {
    let (address, fake) = FakeS3::start("archive").await?;
    let credentials = credentials_file(dir, "fake-secret")?;
    let store = S3Store::new(
        settings(format!("http://{address}"), &credentials),
        Arc::new(SystemClock),
    )?;
    Ok((store, fake))
}

#[tokio::test]
async fn s3_store_conforms_against_the_fake() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (store, fake) = faked(dir.path()).await?;
    conformance(Arc::new(store), "suite").await?;
    assert!(fake.requests() > 0);
    Ok(())
}

#[tokio::test]
async fn a_503_is_retried_and_a_403_is_rejected() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (store, fake) = faked(dir.path()).await?;
    fake.fail_next(2);
    store.put("a/key", b"value".to_vec()).await?;
    assert_eq!(store.get("a/key").await?, Some(b"value".to_vec()));

    fake.fail_next(10);
    let unavailable = store.get("a/key").await;
    assert!(
        matches!(&unavailable, Err(StoreError::Unavailable { reason }) if reason.contains("SlowDown")),
        "{unavailable:?}"
    );
    fake.fail_next(0);

    fake.deny_next(1);
    let before = fake.requests();
    let denied = store.put("a/other", b"x".to_vec()).await;
    assert!(
        matches!(&denied, Err(StoreError::Rejected { reason }) if reason.contains("AccessDenied")),
        "{denied:?}"
    );
    assert_eq!(fake.requests(), before + 1, "a 403 is not retried");
    assert_eq!(store.get("a/other").await?, None);
    Ok(())
}

#[tokio::test]
async fn an_http_endpoint_is_refused_without_allow_http() -> TestResult {
    let dir = tempfile::tempdir()?;
    let credentials = credentials_file(dir.path(), "secret")?;
    let refused = S3Store::new(
        S3Settings {
            http: HttpPolicy::HttpsOnly,
            ..settings("http://127.0.0.1:9".to_owned(), &credentials)
        },
        Arc::new(SystemClock),
    );
    assert!(
        matches!(refused, Err(S3ConfigError::HttpNotAllowed { .. })),
        "{refused:?}"
    );
    let invalid = S3Store::new(
        settings("not a url".to_owned(), &credentials),
        Arc::new(SystemClock),
    );
    assert!(
        matches!(invalid, Err(S3ConfigError::InvalidEndpoint { .. })),
        "{invalid:?}"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn a_credentials_file_others_can_read_is_refused() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir()?;
    let credentials = credentials_file(dir.path(), "secret")?;
    std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o644))?;
    let refused = S3Store::new(
        settings("http://127.0.0.1:9".to_owned(), &credentials),
        Arc::new(SystemClock),
    );
    assert!(
        matches!(
            refused,
            Err(S3ConfigError::CredentialsShared { mode: 0o644, .. })
        ),
        "{refused:?}"
    );
    std::fs::write(&credentials, "access_key_id = \"a\"\nregion = \"x\"\n")?;
    std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600))?;
    let unknown_key = S3Store::new(
        settings("http://127.0.0.1:9".to_owned(), &credentials),
        Arc::new(SystemClock),
    );
    assert!(
        matches!(unknown_key, Err(S3ConfigError::Credentials { .. })),
        "{unknown_key:?}"
    );
    Ok(())
}

#[tokio::test]
async fn credentials_never_appear_in_debug_output() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (store, _fake) = faked(dir.path()).await?;
    let printed = format!("{store:?} {store:#?}");
    assert!(printed.contains("AKIAFAKEKEY"), "{printed}");
    assert!(!printed.contains("fake-secret"), "{printed}");
    Ok(())
}

#[tokio::test]
#[ignore = "needs FLEETRING_S3_TEST_ENDPOINT, a bucket, and credentials"]
async fn a_real_endpoint_conforms() -> TestResult {
    let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let (Some(endpoint), Some(bucket), Some(region)) = (
        var("FLEETRING_S3_TEST_ENDPOINT"),
        var("FLEETRING_S3_TEST_BUCKET"),
        var("FLEETRING_S3_TEST_REGION"),
    ) else {
        eprintln!("FLEETRING_S3_TEST_ENDPOINT, _BUCKET, and _REGION are unset; skipping");
        return Ok(());
    };
    let store = S3Store::new(
        S3Settings {
            bucket,
            endpoint,
            region,
            path_style: if var("FLEETRING_S3_TEST_PATH_STYLE").is_some() {
                PathStyle::Path
            } else {
                PathStyle::VirtualHost
            },
            credentials: CredentialSource::Environment,
            ca: var("FLEETRING_S3_TEST_CA").map(Into::into),
            http: HttpPolicy::AllowHttp,
        },
        Arc::new(SystemClock),
    )?;
    let root = format!("fleetring-test/{}", SystemClock.now().as_millis());
    conformance(Arc::new(store), &root).await
}
