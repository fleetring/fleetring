//! Durations as the command line writes them: `<n>s`, `<n>m`, `<n>h`, or `<n>d`, as a TTL is
//! written everywhere else in FleetRing (`docs/cli.md`).

use std::time::Duration;

/// Parses `text` as a whole number followed by one unit: `s`, `m`, `h`, or `d`.
///
/// # Errors
///
/// [`ParseDurationError`] if `text` is empty, has no known unit, or doesn't start with a whole
/// number that fits.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use fleetring_chaos::duration::parse_duration;
///
/// assert_eq!(parse_duration("10m")?, Duration::from_secs(600));
/// # Ok::<(), fleetring_chaos::duration::ParseDurationError>(())
/// ```
pub fn parse_duration(text: &str) -> Result<Duration, ParseDurationError> {
    let Some(unit) = text.chars().last() else {
        return Err(ParseDurationError::Empty);
    };
    let seconds_per_unit: u64 = match unit {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        'd' => 24 * 60 * 60,
        _ => {
            return Err(ParseDurationError::Unit {
                text: text.to_owned(),
            });
        }
    };
    let digits = &text[..text.len() - unit.len_utf8()];
    let number = || ParseDurationError::Number {
        text: text.to_owned(),
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(number());
    }
    let count: u64 = digits.parse().map_err(|_| number())?;
    let seconds = count.checked_mul(seconds_per_unit).ok_or_else(number)?;
    Ok(Duration::from_secs(seconds))
}

/// Why a duration didn't parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseDurationError {
    /// The text is empty.
    #[error("a duration can't be empty: expected <n>s, <n>m, <n>h, or <n>d")]
    Empty,
    /// The text doesn't end in a known unit.
    #[error("'{text}' has no unit: expected <n>s, <n>m, <n>h, or <n>d")]
    Unit {
        /// The text given.
        text: String,
    },
    /// The text doesn't start with a whole number that fits.
    #[error("'{text}' isn't a whole number of units: expected <n>s, <n>m, <n>h, or <n>d")]
    Number {
        /// The text given.
        text: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_limits_parse_like_ttls() {
        assert_eq!(parse_duration("600s"), Ok(Duration::from_secs(600)));
        assert_eq!(parse_duration("10m"), Ok(Duration::from_secs(600)));
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3600)));
        assert_eq!(parse_duration("2d"), Ok(Duration::from_secs(172_800)));
        assert_eq!(parse_duration(""), Err(ParseDurationError::Empty));
        for invalid in ["10", "10x", "s"] {
            assert!(
                matches!(
                    parse_duration(invalid),
                    Err(ParseDurationError::Unit { .. } | ParseDurationError::Number { .. })
                ),
                "{invalid}"
            );
        }
        assert_eq!(
            parse_duration("-5s"),
            Err(ParseDurationError::Number {
                text: "-5s".to_owned()
            })
        );
    }
}
