//! Static S3 credentials, from the environment or a private file (ADR-0029, point 14).

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use serde::Deserialize;
use zeroize::Zeroizing;

use super::{CredentialSource, S3ConfigError};
use crate::clock::Clock;

/// How often, at most, a credentials file is checked for a change, in milliseconds.
const RECHECK_MS: u64 = 1000;

/// One set of static keys. Secrets are zeroed when dropped, and `Debug` shows only the key ID.
#[derive(Clone, PartialEq, Eq)]
struct Credentials {
    access_key_id: String,
    secret_access_key: Zeroizing<String>,
    session_token: Option<Zeroizing<String>>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

impl Credentials {
    /// The keys in rusty-s3's form, for signing.
    fn signing(&self) -> rusty_s3::Credentials {
        match &self.session_token {
            Some(token) => rusty_s3::Credentials::new_with_token(
                self.access_key_id.clone(),
                self.secret_access_key.as_str(),
                token.as_str(),
            ),
            None => rusty_s3::Credentials::new(
                self.access_key_id.clone(),
                self.secret_access_key.as_str(),
            ),
        }
    }
}

/// A credentials file: exactly these keys.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialsFile {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

/// Where a store's credentials come from, and the keys as last read.
pub(super) struct CredentialStore {
    file: Option<PathBuf>,
    clock: Arc<dyn Clock>,
    state: Mutex<Loaded>,
}

struct Loaded {
    signing: Arc<rusty_s3::Credentials>,
    access_key_id: String,
    /// The file's modification time and length when it was read.
    stamp: Option<(Option<SystemTime>, u64)>,
    /// When the file was last checked for a change, by the store's clock.
    checked_ms: u64,
}

impl fmt::Debug for CredentialStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialStore")
            .field("file", &self.file)
            .field("access_key_id", &self.lock().access_key_id)
            .finish_non_exhaustive()
    }
}

impl CredentialStore {
    /// Reads the credentials `source` names, once, now.
    pub(super) fn new(
        source: &CredentialSource,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, S3ConfigError> {
        let now = clock.now().as_millis();
        let (credentials, file, stamp) = match source {
            CredentialSource::Environment => (from_environment()?, None, None),
            CredentialSource::File(path) => {
                let (credentials, stamp) = read_file(path)?;
                (credentials, Some(path.clone()), Some(stamp))
            }
        };
        Ok(Self {
            file,
            clock,
            state: Mutex::new(Loaded {
                signing: Arc::new(credentials.signing()),
                access_key_id: credentials.access_key_id,
                stamp,
                checked_ms: now,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Loaded> {
        self.state.lock().expect("credentials lock poisoned")
    }

    /// The keys to sign the next request with, re-read first if they come from a file that
    /// changed since it was last checked, at most once a second. A file that can't be read
    /// again leaves the keys as they were.
    pub(super) fn current(&self) -> Arc<rusty_s3::Credentials> {
        let Some(path) = &self.file else {
            return Arc::clone(&self.lock().signing);
        };
        let now = self.clock.now().as_millis();
        let mut state = self.lock();
        if now.saturating_sub(state.checked_ms) < RECHECK_MS {
            return Arc::clone(&state.signing);
        }
        state.checked_ms = now;
        let changed = match stamp(path) {
            Ok(stamp) => state.stamp != Some(stamp),
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "can't check the S3 credentials file; keeping the keys read before");
                false
            }
        };
        if changed {
            match read_file(path) {
                Ok((credentials, stamp)) => {
                    tracing::info!(path = %path.display(), access_key_id = %credentials.access_key_id, "re-read the S3 credentials file");
                    state.signing = Arc::new(credentials.signing());
                    state.access_key_id = credentials.access_key_id;
                    state.stamp = Some(stamp);
                }
                Err(error) => {
                    tracing::warn!(path = %path.display(), %error, "can't re-read the S3 credentials file; keeping the keys read before");
                }
            }
        }
        Arc::clone(&state.signing)
    }
}

/// Reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`.
fn from_environment() -> Result<Credentials, S3ConfigError> {
    let var = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    let missing = |name: &str| S3ConfigError::Credentials {
        reason: format!("{name} is not set"),
    };
    Ok(Credentials {
        access_key_id: var("AWS_ACCESS_KEY_ID").ok_or_else(|| missing("AWS_ACCESS_KEY_ID"))?,
        secret_access_key: Zeroizing::new(
            var("AWS_SECRET_ACCESS_KEY").ok_or_else(|| missing("AWS_SECRET_ACCESS_KEY"))?,
        ),
        session_token: var("AWS_SESSION_TOKEN").map(Zeroizing::new),
    })
}

fn stamp(path: &Path) -> io::Result<(Option<SystemTime>, u64)> {
    let metadata = fs::metadata(path)?;
    Ok((metadata.modified().ok(), metadata.len()))
}

/// Reads and checks a credentials file, and its stamp as of before the read.
fn read_file(path: &Path) -> Result<(Credentials, (Option<SystemTime>, u64)), S3ConfigError> {
    let unreadable = |reason: String| S3ConfigError::Credentials {
        reason: format!("{}: {reason}", path.display()),
    };
    let stamp = stamp(path).map_err(|error| unreadable(error.to_string()))?;
    check_private(path)?;
    let text =
        Zeroizing::new(fs::read_to_string(path).map_err(|error| unreadable(error.to_string()))?);
    let file: CredentialsFile =
        toml::from_str(&text).map_err(|error| unreadable(error.message().to_owned()))?;
    if file.access_key_id.trim().is_empty() || file.secret_access_key.trim().is_empty() {
        return Err(unreadable("a key is empty".to_owned()));
    }
    Ok((
        Credentials {
            access_key_id: file.access_key_id,
            secret_access_key: Zeroizing::new(file.secret_access_key),
            session_token: file.session_token.map(Zeroizing::new),
        },
        stamp,
    ))
}

/// Refuses a credentials file whose mode grants its group or others any permission.
#[cfg(unix)]
fn check_private(path: &Path) -> Result<(), S3ConfigError> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = fs::metadata(path)
        .map_err(|error| S3ConfigError::Credentials {
            reason: format!("{}: {error}", path.display()),
        })?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(S3ConfigError::CredentialsShared {
            path: path.to_owned(),
            mode: mode & 0o777,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private(_: &Path) -> Result<(), S3ConfigError> {
    Ok(())
}
