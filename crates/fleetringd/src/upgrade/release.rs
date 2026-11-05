//! Finding a release and downloading its files (ADR-0038, point 5).

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring_control::Release;
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Bytes;
use hyper::header::{ACCEPT, LOCATION, USER_AGENT};
use hyper::{Request, Uri};
use serde::Deserialize;

use crate::durability::HttpPolicy;
use crate::https::{HttpClient, client};
use crate::upgrade::UpgradeError;

/// The repository releases come from.
const REPOSITORY: &str = "fleetring/fleetring";

/// The releases list `--releases-url` replaces: GitHub's API, newest first.
pub(crate) const RELEASES_URL: &str =
    "https://api.github.com/repos/fleetring/fleetring/releases?per_page=100";

/// The prefix of the daemon's release tags; the `fleetring` crate's tags don't have it.
const TAG_PREFIX: &str = "fleetringd-v";

/// The most redirects one download follows: GitHub sends a release asset through one or two.
const MAX_REDIRECTS: usize = 5;

/// The largest file a download accepts: a release archive is tens of MiB.
const MAX_BYTES: usize = 512 * 1024 * 1024;

/// The signed checksums file every release carries.
pub(crate) const SUMS: &str = "SHA256SUMS";

/// The Sigstore bundle with the signature on [`SUMS`].
pub(crate) const BUNDLE: &str = "SHA256SUMS.sigstore.json";

/// Where releases come from: the releases list, and either GitHub's release downloads or a
/// mirror's directory for one release.
#[derive(Debug, Clone)]
pub(crate) struct Source {
    releases: String,
    mirror: Option<String>,
    http: HttpClient,
    timeout: Duration,
}

/// One entry of GitHub's releases list; the other fields are ignored.
#[derive(Debug, Deserialize)]
struct Listed {
    tag_name: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
}

impl Source {
    /// Reads the releases list from `releases`, or GitHub's, and the files of one release from
    /// `mirror`, as `install.sh --base-url` does, or GitHub's release downloads. Each request
    /// may take `timeout`.
    pub(crate) fn new(
        releases: Option<String>,
        mirror: Option<String>,
        timeout: Duration,
    ) -> Result<Self, UpgradeError> {
        let http =
            client(None, None, HttpPolicy::AllowHttp).map_err(|error| UpgradeError::Network {
                url: String::new(),
                reason: error.to_string(),
            })?;
        Ok(Self {
            releases: releases.unwrap_or_else(|| RELEASES_URL.to_owned()),
            mirror: mirror.map(|mirror| mirror.trim_end_matches('/').to_owned()),
            http,
            timeout,
        })
    }

    /// Returns whether the files come from a mirror rather than GitHub.
    pub(crate) fn is_mirror(&self) -> bool {
        self.mirror.is_some()
    }

    /// The newest final release in the releases list. Drafts, prereleases, and tags that aren't
    /// `fleetringd-vX.Y.Z` are skipped.
    pub(crate) async fn newest(&self) -> Result<Release, UpgradeError> {
        let body = self.get(&self.releases).await?;
        let listed: Vec<Listed> =
            serde_json::from_slice(&body).map_err(|error| UpgradeError::Network {
                url: self.releases.clone(),
                reason: format!("the releases list isn't GitHub's JSON: {error}"),
            })?;
        listed
            .into_iter()
            .filter(|entry| !entry.draft && !entry.prerelease)
            .filter_map(|entry| entry.tag_name.strip_prefix(TAG_PREFIX)?.parse().ok())
            .filter(|release: &Release| !release.is_prerelease())
            .max()
            .ok_or_else(|| UpgradeError::NoRelease {
                url: self.releases.clone(),
            })
    }

    /// The URL of `file` in `release`.
    fn url(&self, release: &Release, file: &str) -> String {
        match &self.mirror {
            Some(mirror) => format!("{mirror}/{file}"),
            None => format!(
                "https://github.com/{REPOSITORY}/releases/download/{TAG_PREFIX}{release}/{file}"
            ),
        }
    }

    /// Downloads `file` of `release` into `dir`, and returns its path.
    pub(crate) async fn download(
        &self,
        release: &Release,
        file: &str,
        dir: &Path,
    ) -> Result<PathBuf, UpgradeError> {
        let url = self.url(release, file);
        let body = self.get(&url).await?;
        let path = dir.join(file);
        std::fs::write(&path, &body).map_err(|source| UpgradeError::Io {
            path: path.clone(),
            source,
        })?;
        Ok(path)
    }

    /// Fetches `url`, following redirects, within the timeout.
    async fn get(&self, url: &str) -> Result<Bytes, UpgradeError> {
        let failed = |reason: String| UpgradeError::Network {
            url: url.to_owned(),
            reason,
        };
        tokio::time::timeout(self.timeout, self.follow(url))
            .await
            .map_err(|_| failed(format!("no answer within {}s", self.timeout.as_secs())))?
    }

    async fn follow(&self, url: &str) -> Result<Bytes, UpgradeError> {
        let mut current = url.to_owned();
        for _ in 0..=MAX_REDIRECTS {
            let failed = |reason: String| UpgradeError::Network {
                url: current.clone(),
                reason,
            };
            let uri = allowed(&current).map_err(failed)?;
            let request = Request::get(uri.clone())
                .header(
                    USER_AGENT,
                    concat!("fleetringd/", env!("CARGO_PKG_VERSION")),
                )
                .header(ACCEPT, "*/*")
                .body(Full::new(Bytes::new()))
                .map_err(|error| failed(error.to_string()))?;
            let response = self
                .http
                .request(request)
                .await
                .map_err(|error| failed(error.to_string()))?;
            let status = response.status();
            if status.is_redirection() {
                let location = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| failed(format!("{status} without a location")))?;
                current = resolve(&uri, location);
                continue;
            }
            if !status.is_success() {
                return Err(failed(format!("answered {status}")));
            }
            let body = Limited::new(response.into_body(), MAX_BYTES)
                .collect()
                .await
                .map_err(|error| failed(error.to_string()))?;
            return Ok(body.to_bytes());
        }
        Err(UpgradeError::Network {
            url: url.to_owned(),
            reason: format!("more than {MAX_REDIRECTS} redirects"),
        })
    }
}

/// Parses `url` and checks it may be fetched: `https://`, or `http://` to a loopback address.
fn allowed(url: &str) -> Result<Uri, String> {
    let uri: Uri = url.parse().map_err(|error| format!("not a URL: {error}"))?;
    match uri.scheme_str() {
        Some("https") => Ok(uri),
        Some("http") if uri.host().is_some_and(is_loopback) => Ok(uri),
        _ => Err("only https:// is fetched, and http:// only from a loopback address".to_owned()),
    }
}

fn is_loopback(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Resolves a redirect's `location` against the request it answered.
fn resolve(base: &Uri, location: &str) -> String {
    if location.contains("://") {
        return location.to_owned();
    }
    let scheme = base.scheme_str().unwrap_or("https");
    let authority = base.authority().map_or("", |authority| authority.as_str());
    if location.starts_with('/') {
        format!("{scheme}://{authority}{location}")
    } else {
        let path = base.path();
        let dir = path.rsplit_once('/').map_or("", |(dir, _)| dir);
        format!("{scheme}://{authority}{dir}/{location}")
    }
}

/// The archive's file name for `release` on `target`.
pub(crate) fn archive_name(release: &Release, target: &str) -> String {
    format!("fleetringd-{release}-{target}.tar.gz")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_https_or_loopback_http_is_fetched() {
        assert!(allowed("https://github.com/x").is_ok());
        assert!(allowed("http://127.0.0.1:8080/x").is_ok());
        assert!(allowed("http://[::1]:8080/x").is_ok());
        assert!(allowed("http://localhost/x").is_ok());
        assert!(allowed("http://example.com/x").is_err());
        assert!(allowed("file:///etc/passwd").is_err());
        assert!(allowed("ftp://127.0.0.1/x").is_err());
    }

    #[test]
    fn redirects_resolve_against_the_request() {
        let base: Uri = "https://github.com/a/b/c".parse().expect("uri");
        assert_eq!(
            resolve(&base, "https://objects.example/x"),
            "https://objects.example/x"
        );
        assert_eq!(resolve(&base, "/d"), "https://github.com/d");
        assert_eq!(resolve(&base, "e"), "https://github.com/a/b/e");
    }

    #[test]
    fn archives_are_named_as_the_release_workflow_names_them() {
        let release: Release = "0.4.0".parse().expect("release");
        assert_eq!(
            archive_name(&release, "aarch64-apple-darwin"),
            "fleetringd-0.4.0-aarch64-apple-darwin.tar.gz"
        );
    }
}
