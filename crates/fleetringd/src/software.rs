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

/// Fleet features the control crate knows but this build doesn't carry out yet, so it doesn't
/// report them. A release reports a feature only once it acts on every command the feature
/// covers (ADR-0041, point 3).
///
/// `expiry`: `apply` decides `ExpireDatabase`, but nothing submits one until the control node's
/// expiry sweep exists (FR-070 phase 3, which removes this list).
const NOT_YET_REPORTED: [Feature; 1] = [Feature::Expiry];

/// Returns the software this build runs: its release, the control-log format it reads, and every
/// fleet feature it carries out.
#[must_use]
pub fn running() -> Software {
    Feature::ALL
        .into_iter()
        .filter(|feature| !NOT_YET_REPORTED.contains(feature))
        .fold(
            Software::new(release(), FORMAT_VERSION),
            Software::with_feature,
        )
}

/// What this build runs, as `fleetringd upgrade --software` prints it: one JSON object with the
/// release, the protocol, the control-log format, and the fleet features, so that an upgrade can
/// ask a binary it hasn't installed yet what it supports.
#[must_use]
pub fn running_json() -> String {
    let software = running();
    serde_json::json!({
        "version": software.release().to_string(),
        "protocol": format!("{}.{}", PROTOCOL_VERSION.major, PROTOCOL_VERSION.minor),
        "control_format": software.control_format(),
        "features": software.features().map(Feature::as_str).collect::<Vec<_>>(),
    })
    .to_string()
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
            .chain(software.unknown_features().map(str::to_owned))
            .collect(),
    }
}

/// The software a node reported. Feature names this build doesn't know are kept as given, as
/// the control log keeps them, so the software a member reported and the software recorded for
/// it compare equal; they never count as supported, since no command this build applies needs
/// them.
///
/// # Errors
///
/// `INVALID_ARGUMENT` if the version isn't a release.
pub(crate) fn software_from_proto(software: proto::Software) -> Result<Software, Status> {
    let release: Release = software
        .version
        .parse()
        .map_err(|error| Status::invalid_argument(format!("invalid software version: {error}")))?;
    Ok(software.features.iter().fold(
        Software::new(release, software.control_format),
        |software, name| software.with_feature_name(name),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_build_reports_every_feature_it_carries_out() {
        let software = running();
        assert_eq!(software.release().to_string(), VERSION);
        assert!(software.supports(Feature::Undrain));
        assert!(software.supports(Feature::Metadata));
        // Nothing sweeps expired databases until FR-070 phase 3, so `expiry` isn't reported:
        // a fleet could otherwise enable it with no node that ever expires a database.
        assert!(!software.supports(Feature::Expiry));
        assert!(
            Feature::ALL
                .into_iter()
                .all(|feature| software.supports(feature) != NOT_YET_REPORTED.contains(&feature))
        );
        assert_eq!(
            software_from_proto(software_to_proto(&software)).expect("valid"),
            software
        );
        let printed: serde_json::Value =
            serde_json::from_str(&running_json()).expect("upgrade --software prints JSON");
        assert_eq!(
            printed["features"],
            serde_json::json!(["undrain", "metadata"])
        );
    }

    #[test]
    fn unknown_features_are_kept_unsupported_and_bad_versions_refused() {
        let mut reported = software_to_proto(&running());
        reported.features.push("teleport".to_owned());
        let software = software_from_proto(reported.clone()).expect("valid");
        assert_eq!(
            software.unknown_features().collect::<Vec<_>>(),
            ["teleport"]
        );
        assert!(
            Feature::ALL
                .into_iter()
                .all(|feature| software.supports(feature) == running().supports(feature))
        );
        assert_eq!(
            software.features().collect::<Vec<_>>(),
            running().features().collect::<Vec<_>>()
        );
        reported.version = "next".to_owned();
        assert_eq!(
            software_from_proto(reported).expect_err("invalid").code(),
            tonic::Code::InvalidArgument
        );
    }

    /// A newer member's feature names survive the protocol both ways, so the software it reports
    /// equals the software the control log records for it, and a rejoin on the same software
    /// takes the rejoin path.
    #[test]
    fn unknown_feature_names_round_trip_through_the_protocol() {
        let reported = proto::Software {
            version: "0.7.0".to_owned(),
            protocol: Some(PROTOCOL_VERSION),
            control_format: 2,
            features: vec![
                "undrain".to_owned(),
                "future-x".to_owned(),
                "metadata".to_owned(),
            ],
        };
        let software = software_from_proto(reported).expect("valid");
        assert_eq!(
            software.unknown_features().collect::<Vec<_>>(),
            ["future-x"]
        );
        let again = software_to_proto(&software);
        assert_eq!(again.features, ["undrain", "metadata", "future-x"]);
        assert_eq!(software_from_proto(again).expect("valid"), software);

        let stored = serde_json::to_string(&software).expect("serializes");
        let recorded: Software = serde_json::from_str(&stored).expect("decodes");
        assert_eq!(recorded, software);
    }
}
