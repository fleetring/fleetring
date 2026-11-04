//! The software this build runs, as nodes report it in `Join` and `Heartbeat` (ADR-0038).

use fleetring_control::{Feature, Release, Software};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1 as proto;
use tonic::Status;

use crate::control::FORMAT_VERSION;

/// The release this build is, as `fleetringd --version` prints it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The target triple this build was compiled for, which picks a release's archive.
pub const TARGET: &str = env!("FLEETRINGD_TARGET");

/// Returns this build's release.
///
/// # Panics
///
/// Never: the crate's version is a valid release, which a unit test checks.
#[must_use]
pub fn release() -> Release {
    VERSION
        .parse()
        .expect("the crate's version is a valid release")
}

/// Returns the software this build runs: its release, the control-log format it reads, and every
/// fleet feature it knows.
#[must_use]
pub fn running() -> Software {
    Feature::ALL.into_iter().fold(
        Software::new(release(), FORMAT_VERSION),
        Software::with_feature,
    )
}

/// The protocol form of `software`, which speaks this build's protocol version.
pub(crate) fn software_to_proto(software: &Software) -> proto::Software {
    proto::Software {
        version: software.release().to_string(),
        protocol: Some(PROTOCOL_VERSION),
        control_format: software.control_format(),
        features: software
            .features()
            .map(|feature| feature.as_str().to_owned())
            .collect(),
    }
}

/// The software a node reported. Features this build doesn't know are left out: the node is
/// newer, and no command this build applies needs them.
///
/// # Errors
///
/// `INVALID_ARGUMENT` if the version isn't a release.
pub(crate) fn software_from_proto(software: proto::Software) -> Result<Software, Status> {
    let release: Release = software
        .version
        .parse()
        .map_err(|error| Status::invalid_argument(format!("invalid software version: {error}")))?;
    Ok(software
        .features
        .iter()
        .filter_map(|name| name.parse().ok())
        .fold(
            Software::new(release, software.control_format),
            Software::with_feature,
        ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_build_reports_a_release_with_every_feature() {
        let software = running();
        assert_eq!(software.release().to_string(), VERSION);
        assert!(
            Feature::ALL
                .into_iter()
                .all(|feature| software.supports(feature))
        );
        assert_eq!(
            software_from_proto(software_to_proto(&software)).expect("valid"),
            software
        );
    }

    #[test]
    fn unknown_features_are_ignored_and_bad_versions_refused() {
        let mut reported = software_to_proto(&running());
        reported.features.push("teleport".to_owned());
        assert_eq!(
            software_from_proto(reported.clone()).expect("valid"),
            running()
        );
        reported.version = "next".to_owned();
        assert_eq!(
            software_from_proto(reported).expect_err("invalid").code(),
            tonic::Code::InvalidArgument
        );
    }
}
