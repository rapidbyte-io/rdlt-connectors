//! The text face: Postgres TEXT output forms → values.
//!
//! Two audiences with two strictness levels, both living HERE so a form is
//! never parsed two ways by accident:
//!
//! - the STRICT parsers (`parse_*`) accept exactly what the server emits —
//!   the CDC change stream and anything else quoting server output;
//! - [`parse_scalar`] is the LENIENT config-literal face (`initial_value`,
//!   `end_value`): it additionally accepts RFC3339 timestamps and `HH:MM`
//!   times, because humans write those.
//!
//! Errors are `String` details; callers add the column/table context.

use super::{Kind, Scalar};

pub(crate) fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "t" => Ok(true),
        "f" => Ok(false),
        other => Err(format!("`{other}` is not a bool")),
    }
}

pub(crate) fn parse_int(value: &str) -> Result<i64, String> {
    value
        .parse::<i64>()
        .map_err(|e| format!("`{value}` is not an integer: {e}"))
}

pub(crate) fn parse_float(value: &str) -> Result<f64, String> {
    // Rust accepts "NaN"/"Infinity"/"-Infinity" case-insensitively — the
    // exact postgres text spellings.
    value
        .parse::<f64>()
        .map_err(|e| format!("`{value}` is not a float: {e}"))
}

/// numeric text → i128 at the declared scale (exact; excess fractional
/// digits are a typed error, and `NaN` has no Arrow representation).
pub(crate) fn parse_decimal_scaled(value: &str, scale: u8) -> Result<i128, String> {
    let (sign, body) = match value.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1, value),
    };
    let (integer_part, fraction_part) = body.split_once('.').unwrap_or((body, ""));
    if integer_part.is_empty() && fraction_part.is_empty()
        || !integer_part.chars().all(|c| c.is_ascii_digit())
        || !fraction_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("`{value}` is not a plain numeric literal"));
    }
    if fraction_part.len() > scale as usize {
        return Err(format!(
            "`{value}` carries more fractional digits than numeric scale {scale}"
        ));
    }
    let mut digits = String::with_capacity(integer_part.len() + scale as usize);
    digits.push_str(integer_part);
    digits.push_str(fraction_part);
    for _ in fraction_part.len()..scale as usize {
        digits.push('0');
    }
    digits
        .parse::<i128>()
        .map(|scaled| sign * scaled)
        .map_err(|e| format!("`{value}` overflows the numeric wire range: {e}"))
}

pub(crate) fn parse_bytea_hex(value: &str) -> Result<Vec<u8>, String> {
    let hex = value
        .strip_prefix("\\x")
        .ok_or_else(|| format!("bytea text `{value}` lacks the \\x prefix"))?;
    if hex.len() % 2 != 0 {
        return Err("bytea hex has odd length".into());
    }
    (0..hex.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&hex[offset..offset + 2], 16)
                .map_err(|_| format!("bytea hex contains a non-hex pair at offset {offset}"))
        })
        .collect()
}

/// Postgres timestamp text (`2026-07-21 10:11:12.123456`, with an optional
/// `+HH[:MM]` offset when `with_zone`) → µs since the Unix epoch. `±infinity`
/// saturates, matching the binary decoder.
pub(crate) fn parse_timestamp(value: &str, with_zone: bool) -> Result<i64, String> {
    match value {
        "infinity" => return Ok(i64::MAX),
        "-infinity" => return Ok(i64::MIN),
        _ => {}
    }
    if with_zone {
        // `%#z` accepts +00, +05:30, and +0530 spellings.
        chrono::DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z")
            .map(|instant| instant.timestamp_micros())
            .map_err(|e| format!("`{value}` is not a timestamptz: {e}"))
    } else {
        chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
            .map(|instant| instant.and_utc().timestamp_micros())
            .map_err(|e| format!("`{value}` is not a timestamp: {e}"))
    }
}

/// Postgres date text → days since the Unix epoch; `±infinity` saturates.
pub(crate) fn parse_date_days(value: &str) -> Result<i32, String> {
    match value {
        "infinity" => return Ok(i32::MAX),
        "-infinity" => return Ok(i32::MIN),
        _ => {}
    }
    let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|e| format!("`{value}` is not a date: {e}"))?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a valid date");
    i32::try_from((date - epoch).num_days()).map_err(|_| format!("date `{value}` out of range"))
}

pub(crate) fn parse_time_microseconds(value: &str) -> Result<i64, String> {
    use chrono::Timelike;
    let time = chrono::NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
        .map_err(|e| format!("`{value}` is not a time: {e}"))?;
    Ok(time.num_seconds_from_midnight() as i64 * 1_000_000 + (time.nanosecond() / 1_000) as i64)
}

/// RFC3339 first ("2026-01-02T03:04:05.123456Z"), then the space and `T`
/// naive forms — the lenient timestamp intake for config literals.
fn parse_timestamp_flexible(value: &str) -> Option<i64> {
    if let Ok(instant) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(instant.timestamp_micros());
    }
    let naive = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()?;
    Some(naive.and_utc().timestamp_micros())
}

/// Parse a config-provided literal for a cursor column of `kind`. Formats
/// are the canonical text forms of the type mapping, plus the human-friendly
/// timestamp/time leniencies documented on the module.
pub(crate) fn parse_scalar(kind: Kind, value: &str) -> Result<Scalar, String> {
    match kind {
        Kind::Int16 | Kind::Int32 | Kind::Int64 => value
            .parse::<i64>()
            .map(Scalar::Int)
            .map_err(|e| format!("cursor literal `{value}` is not an integer: {e}")),
        Kind::Decimal { scale, .. } => parse_decimal_scaled(value, scale)
            .map(|scaled| Scalar::Decimal { scaled, scale })
            .map_err(|_| format!("cursor literal `{value}` is not a numeric(…,{scale})")),
        Kind::Text => Ok(Scalar::Text(value.to_owned())),
        Kind::Uuid => Ok(Scalar::Uuid(value.to_ascii_lowercase())),
        Kind::TimestampTz => parse_timestamp_flexible(value)
            .map(Scalar::TimestampTz)
            .ok_or_else(|| format!("cursor literal `{value}` is not an RFC3339/ISO timestamp")),
        Kind::TimestampNaive => parse_timestamp_flexible(value)
            .map(Scalar::TimestampNaive)
            .ok_or_else(|| format!("cursor literal `{value}` is not an RFC3339/ISO timestamp")),
        // The STRICT date parser saturates the server's `±infinity`
        // spellings; a config literal must not — an unbounded window is
        // spelled by omitting the bound, and letting the sentinel through
        // would defer the mistake to a server-side range error mid-COPY.
        Kind::Date => match value {
            "infinity" | "-infinity" => {
                Err(format!("cursor literal `{value}` is not a YYYY-MM-DD date"))
            }
            _ => parse_date_days(value)
                .map(Scalar::Date)
                .map_err(|_| format!("cursor literal `{value}` is not a YYYY-MM-DD date")),
        },
        Kind::Time => {
            use chrono::Timelike;
            let lenient = chrono::NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveTime::parse_from_str(value, "%H:%M"));
            lenient
                .map(|time| {
                    Scalar::Time(
                        time.num_seconds_from_midnight() as i64 * 1_000_000
                            + (time.nanosecond() / 1_000) as i64,
                    )
                })
                .map_err(|_| format!("cursor literal `{value}` is not a HH:MM[:SS[.ffffff]] time"))
        }
        Kind::Bool | Kind::Float32 | Kind::Float64 | Kind::Jsonb | Kind::Bytea => {
            Err("cursor column type is not cursor-capable".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_forms_parse_the_server_spellings() {
        assert_eq!(parse_bool("t"), Ok(true));
        assert_eq!(parse_bool("f"), Ok(false));
        assert!(parse_bool("true").is_err(), "server emits t/f only");
        assert_eq!(parse_int("42"), Ok(42));
        assert_eq!(parse_float("-Infinity"), Ok(f64::NEG_INFINITY));
        assert_eq!(parse_decimal_scaled("-12345.67", 2), Ok(-1_234_567));
        assert_eq!(parse_bytea_hex("\\x0aff"), Ok(vec![0x0a, 0xff]));
        assert_eq!(parse_date_days("1970-01-02"), Ok(1));
        assert_eq!(
            parse_time_microseconds("10:11:12.123456"),
            Ok(36_672_123_456)
        );
    }

    #[test]
    fn decimal_excess_fraction_and_nan_are_refused() {
        for bad in ["1.234", "NaN"] {
            assert!(parse_decimal_scaled(bad, 2).is_err(), "{bad}");
        }
    }

    #[test]
    fn timestamp_offset_spellings_and_infinities() {
        for (value, expected) in [
            ("2026-01-02 03:04:05+00", Some(1_767_323_045_000_000i64)),
            ("2026-01-02 08:34:05+05:30", Some(1_767_323_045_000_000)),
            ("infinity", Some(i64::MAX)),
            ("-infinity", Some(i64::MIN)),
            ("2026-99-02 03:04:05+00", None),
        ] {
            assert_eq!(parse_timestamp(value, true).ok(), expected, "{value}");
        }
    }

    #[test]
    fn scalar_literals_accept_the_lenient_forms() {
        assert_eq!(
            parse_scalar(Kind::TimestampTz, "2026-01-02T03:04:05Z"),
            Ok(Scalar::TimestampTz(1_767_323_045_000_000))
        );
        assert_eq!(
            parse_scalar(Kind::Time, "10:11"),
            Ok(Scalar::Time(36_660_000_000))
        );
        assert_eq!(
            parse_scalar(Kind::Uuid, "550E8400-E29B-41D4-A716-446655440000"),
            Ok(Scalar::Uuid("550e8400-e29b-41d4-a716-446655440000".into()))
        );
        assert!(parse_scalar(Kind::Bool, "t").is_err(), "not cursor-capable");
        // Server-side infinity spellings are STRICT-parser territory; a
        // config literal refuses them at open instead of failing mid-COPY.
        for sentinel in ["infinity", "-infinity"] {
            assert!(parse_scalar(Kind::Date, sentinel).is_err(), "{sentinel}");
        }
    }
}
