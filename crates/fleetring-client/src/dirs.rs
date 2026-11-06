//! Where FleetRing's client files live: the configuration directory and the data directory.
//!
//! The lookups take the environment as a closure, so a test supplies a map and never touches the
//! process environment.

use std::ffi::OsString;
use std::path::PathBuf;

/// The directory name under the platform's base directory.
const DIR_NAME: &str = "fleetring";

/// The configuration directory: `$XDG_CONFIG_HOME/fleetring` if that is set and absolute, else
/// `$HOME/.config/fleetring`, on Linux and macOS; `%APPDATA%\fleetring` on Windows.
///
/// `env` is `|name| std::env::var_os(name)` in production.
///
/// # Errors
///
/// [`DirsError::NoHome`] if the variable the platform needs is unset or empty.
pub fn config_dir(env: &dyn Fn(&str) -> Option<OsString>) -> Result<PathBuf, DirsError> {
    if cfg!(windows) {
        return windows_dir(env, "APPDATA");
    }
    unix_dir(env, "XDG_CONFIG_HOME", ".config")
}

/// The data directory for files a program wrote: `$XDG_DATA_HOME/fleetring` if set and
/// absolute, else `$HOME/.local/share/fleetring`; `%LOCALAPPDATA%\fleetring` on Windows.
///
/// # Errors
///
/// [`DirsError::NoHome`] if the variable the platform needs is unset or empty.
pub fn data_dir(env: &dyn Fn(&str) -> Option<OsString>) -> Result<PathBuf, DirsError> {
    if cfg!(windows) {
        return windows_dir(env, "LOCALAPPDATA");
    }
    unix_dir(env, "XDG_DATA_HOME", ".local/share")
}

fn windows_dir(
    env: &dyn Fn(&str) -> Option<OsString>,
    variable: &'static str,
) -> Result<PathBuf, DirsError> {
    non_empty(env, variable)
        .map(|base| PathBuf::from(base).join(DIR_NAME))
        .ok_or(DirsError::NoHome { variable })
}

fn unix_dir(
    env: &dyn Fn(&str) -> Option<OsString>,
    xdg: &str,
    under_home: &str,
) -> Result<PathBuf, DirsError> {
    if let Some(base) = non_empty(env, xdg).map(PathBuf::from)
        && base.is_absolute()
    {
        return Ok(base.join(DIR_NAME));
    }
    non_empty(env, "HOME")
        .map(|home| PathBuf::from(home).join(under_home).join(DIR_NAME))
        .ok_or(DirsError::NoHome { variable: "HOME" })
}

fn non_empty(env: &dyn Fn(&str) -> Option<OsString>, name: &str) -> Option<OsString> {
    env(name).filter(|value| !value.is_empty())
}

/// Why a directory couldn't be found.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DirsError {
    /// The variable that names the base directory is unset or empty.
    #[error("{variable} is unset or empty, so there is no home directory")]
    NoHome {
        /// The variable the platform reads.
        variable: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn lookup<'a>(vars: &'a BTreeMap<&str, &str>) -> impl Fn(&str) -> Option<OsString> + 'a {
        |name| vars.get(name).map(OsString::from)
    }

    #[cfg(not(windows))]
    #[test]
    fn xdg_wins_when_it_is_absolute() {
        let vars = BTreeMap::from([("XDG_CONFIG_HOME", "/x"), ("HOME", "/home/me")]);
        assert_eq!(
            config_dir(&lookup(&vars)).expect("a directory"),
            PathBuf::from("/x/fleetring")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn a_relative_or_empty_xdg_falls_back_to_home() {
        for xdg in ["relative/dir", ""] {
            let vars = BTreeMap::from([("XDG_CONFIG_HOME", xdg), ("HOME", "/home/me")]);
            assert_eq!(
                config_dir(&lookup(&vars)).expect("a directory"),
                PathBuf::from("/home/me/.config/fleetring"),
                "XDG_CONFIG_HOME={xdg:?}"
            );
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn the_data_directory_follows_the_same_rule() {
        let vars = BTreeMap::from([("HOME", "/home/me")]);
        assert_eq!(
            data_dir(&lookup(&vars)).expect("a directory"),
            PathBuf::from("/home/me/.local/share/fleetring")
        );
        let vars = BTreeMap::from([("XDG_DATA_HOME", "/d"), ("HOME", "/home/me")]);
        assert_eq!(
            data_dir(&lookup(&vars)).expect("a directory"),
            PathBuf::from("/d/fleetring")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn no_home_is_an_error_naming_the_variable() {
        let vars = BTreeMap::new();
        let error = config_dir(&lookup(&vars)).expect_err("no home");
        assert_eq!(
            error.to_string(),
            "HOME is unset or empty, so there is no home directory"
        );
        let vars = BTreeMap::from([("HOME", "")]);
        assert!(config_dir(&lookup(&vars)).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn windows_reads_appdata() {
        let vars = BTreeMap::from([("APPDATA", r"C:\Users\me\AppData\Roaming")]);
        assert_eq!(
            config_dir(&lookup(&vars)).expect("a directory"),
            PathBuf::from(r"C:\Users\me\AppData\Roaming").join("fleetring")
        );
    }
}
