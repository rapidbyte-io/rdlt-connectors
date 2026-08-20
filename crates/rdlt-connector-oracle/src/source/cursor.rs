//! The watermark cursor: a persisted v1 wire format, one watermark
//! per stream, never lowered.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::core::cursor::Cursor;
use rdlt_connector_sdk::spi::error::SourceError;

/// The persisted format version this crate writes.
const CURSOR_FORMAT_VERSION: u32 = 1;

/// The whole persisted cursor.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OracleCursor {
    #[serde(default = "default_version")]
    pub format_version: u32,
    /// Per-stream watermarks: the RENDERED maximum of the cursor
    /// column, compared SQL-side on resume.
    #[serde(default)]
    pub streams: BTreeMap<String, StreamCursor>,
}

fn default_version() -> u32 {
    CURSOR_FORMAT_VERSION
}

// Manual: serde default fns apply only at deserialize (the 030 wire
// lesson — a derived Default would mint version-0 cursors).
impl Default for OracleCursor {
    fn default() -> Self {
        Self {
            format_version: CURSOR_FORMAT_VERSION,
            streams: BTreeMap::new(),
        }
    }
}

/// One stream's progress.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamCursor {
    /// The rendered high-water mark of the cursor column.
    pub watermark: String,
    /// The ROWID of the LAST row read at exactly that watermark.
    ///
    /// Rows sharing a cursor value would otherwise be indivisible: a
    /// page boundary inside them either re-reads them (`>=`) or drops
    /// them (`>`). The tie-break makes the resume predicate exact —
    /// `c > w OR (c = w AND ROWID > tie)`. Additive at v1: cursors
    /// written before it simply carry none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tie: Option<String>,
}

impl OracleCursor {
    /// Decode a persisted cursor; absent means empty; unreadable and
    /// FUTURE formats are typed (state is precious — silently starting
    /// over duplicates; a future shape would decode EMPTY through the
    /// serde defaults).
    pub fn decode(cursor: Option<&Cursor>) -> Result<Self, SourceError> {
        let Some(cursor) = cursor else {
            return Ok(Self::default());
        };
        let decoded: Self = serde_json::from_value(cursor.as_value().clone())
            .map_err(|e| SourceError::fatal(format!("unreadable oracle cursor: {e}")))?;
        if decoded.format_version > CURSOR_FORMAT_VERSION {
            return Err(SourceError::fatal(format!(
                "oracle cursor format v{} is newer than this build supports \
                 (v{CURSOR_FORMAT_VERSION}); upgrade rdlt instead of clearing state",
                decoded.format_version
            )));
        }
        Ok(decoded)
    }

    /// Encode for the checkpoint channel.
    pub fn encode(&self) -> Cursor {
        Cursor::new(serde_json::to_value(self).expect("cursor serialization"))
    }

    /// Record progress at `candidate`, carrying the row's ROWID as
    /// the tie-break.
    ///
    /// The watermark never lowers; at an EQUAL watermark the tie
    /// advances, because rows are read in `(cursor, ROWID)` order and
    /// a later row at the same cursor value is strictly further along.
    pub fn advance(&mut self, stream: &str, candidate: &str, tie: Option<&str>) {
        let tie = tie.map(str::to_owned);
        match self.streams.get_mut(stream) {
            Some(existing) if watermark_less(&existing.watermark, candidate) => {
                existing.watermark = candidate.to_owned();
                existing.tie = tie;
            }
            Some(existing) if existing.watermark == candidate => existing.tie = tie,
            Some(_) => {}
            None => {
                self.streams.insert(
                    stream.to_owned(),
                    StreamCursor {
                        watermark: candidate.to_owned(),
                        tie,
                    },
                );
            }
        }
    }
}

/// Order two rendered watermarks: numerically when both parse, else
/// lexically (ISO timestamps order lexically by construction).
/// Order two rendered watermarks.
///
/// NOT through f64. Oracle's NUMBER carries 38 digits and f64 has ~15
/// of precision, so two consecutive sequence values above 2^53 —
/// `100000000000000001` and `...002`, the ordinary shape of a
/// snowflake id or an epoch-nanosecond key — collapse to the SAME
/// f64. Neither `<` nor `==` then held, `advance` fell through its
/// catch-all, and the checkpoint stopped moving for the rest of the
/// run while the read carried on: the next run re-read everything
/// after it, forever.
///
/// Decimal strings are compared DIGIT-WISE instead, which is exact at
/// any magnitude. Anything that is not a decimal (an ISO timestamp)
/// falls back to lexical order, which is correct for the one
/// canonical shape `watermark_text` renders.
fn watermark_less(a: &str, b: &str) -> bool {
    match (decimal_parts(a), decimal_parts(b)) {
        (Some(x), Some(y)) => decimal_less(x, y),
        _ => a < b,
    }
}

/// `(negative, integer digits, fraction digits)` for a decimal, or
/// `None` if the text is not one.
fn decimal_parts(value: &str) -> Option<(bool, &str, &str)> {
    let value = value.trim();
    let (negative, body) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) {
        return None;
    }
    Some((negative, whole, fraction))
}

/// Digit-wise `<` over two decimals, exact at any magnitude.
fn decimal_less(
    (a_neg, a_int, a_frac): (bool, &str, &str),
    (b_neg, b_int, b_frac): (bool, &str, &str),
) -> bool {
    if a_neg != b_neg {
        return a_neg;
    }
    let a_int_trimmed = a_int.trim_start_matches('0');
    let b_int_trimmed = b_int.trim_start_matches('0');
    let magnitude_less = match a_int_trimmed.len().cmp(&b_int_trimmed.len()) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => match a_int_trimmed.cmp(b_int_trimmed) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            // Same integer part: compare fractions padded to a
            // common length so `.5` and `.45` order correctly.
            std::cmp::Ordering::Equal => {
                let width = a_frac.len().max(b_frac.len());
                let pad = |f: &str| format!("{f:0<width$}");
                pad(a_frac) < pad(b_frac)
            }
        },
    };
    // For negatives the magnitude order reverses, and equal values
    // are never "less".
    if a_neg {
        !magnitude_less && (a_int_trimmed, a_frac) != (b_int_trimmed, b_frac)
    } else {
        magnitude_less
    }
}

/// A watermark travels back into SQL as a LITERAL, so its shape is
/// REFUSED unless it is unambiguously a number or an ISO-like
/// timestamp — never quoted free text (the injection gate; the
/// cursor document is operator-editable).
pub fn checked_watermark_literal(value: &str) -> Result<String, SourceError> {
    // `--` STARTS A COMMENT IN SQL, and a character-class gate that
    // allows `-` allows it. A persisted watermark of `1--1` produced
    // `WHERE "ID" > 1--1 OR (…) ORDER BY …`, where everything from
    // the `--` vanishes: still-valid SQL, but with the ORDER BY GONE.
    // Rows then arrive in server order, the tie records whatever came
    // last, and the next run's `ROWID > tie` skips everything below
    // it — silent loss, from a document this rulebook itself calls
    // operator-editable. A sign may only LEAD.
    let well_signed = |value: &str| {
        let body = value.strip_prefix(['-', '+']).unwrap_or(value);
        !body.is_empty() && !body.contains(['-', '+'])
    };
    let numeric = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+'))
        && well_signed(value);
    let timestampish = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '-' | ':' | 'T' | 'Z' | '.' | '+' | ' '))
        // The timestamp branch is QUOTED, so it cannot end the
        // literal — but a `--` inside it would still comment out the
        // format model and everything after it if the quoting ever
        // changed. Refuse it here rather than depend on that.
        && !value.contains("--");
    if numeric {
        Ok(value.to_owned())
    } else if timestampish {
        // The model matches `watermark_text`'s ONE canonical shape:
        // fraction and offset always present.
        Ok(format!(
            "TO_TIMESTAMP_TZ('{value}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM')"
        ))
    } else {
        Err(SourceError::fatal(format!(
            "cursor watermark `{value}` is neither numeric nor timestamp-shaped — \
             refusing to interpolate it into SQL; clear the pipeline state"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The literal v1 wire shape.
    #[test]
    fn the_wire_shape_is_frozen() {
        let mut cursor = OracleCursor::default();
        cursor.advance("events", "42", None);
        assert_eq!(
            serde_json::to_value(&cursor).expect("json"),
            serde_json::json!({
                "format_version": 1,
                "streams": {"events": {"watermark": "42"}}
            })
        );
    }

    /// Watermarks only rise — numerically for numbers ("9" < "10"),
    /// lexically for timestamps.
    /// `--` opens a SQL comment, and a numeric gate that allows `-`
    /// allows it. The danger is NOT a dropped table — it is that the
    /// statement stays VALID with its `ORDER BY` commented out, so
    /// rows arrive unordered and the next resume skips whatever fell
    /// below the recorded tie. Silent loss, not a crash.
    #[test]
    fn a_watermark_cannot_open_a_sql_comment() {
        for poison in ["1--1", "--1", "1--1", "1--", "2026-01-01T00:00:00--x"] {
            assert!(
                checked_watermark_literal(poison).is_err(),
                "`{poison}` must be refused"
            );
        }
        // And the legitimate shapes still pass.
        for ok in ["150", "-3.5", "+42", "0.0001"] {
            assert!(
                checked_watermark_literal(ok).is_ok(),
                "`{ok}` must be accepted"
            );
        }
        assert!(
            checked_watermark_literal("2026-01-02T03:04:05.678000+00:00").is_ok(),
            "the canonical timestamp shape must be accepted"
        );
    }

    /// Two consecutive keys above 2^53 are DISTINCT.
    ///
    /// Through f64 they were the same number, so the checkpoint
    /// stopped advancing for the rest of the run while the read
    /// carried on — and the next run re-read everything after it.
    /// Bare NUMBER is exactly how Oracle spells a sequence key.
    #[test]
    fn large_integer_keys_stay_distinct() {
        assert!(watermark_less("100000000000000001", "100000000000000002"));
        assert!(!watermark_less("100000000000000002", "100000000000000001"));
        assert!(!watermark_less("100000000000000001", "100000000000000001"));
        // 38 digits, Oracle's own limit.
        let a = format!("{}1", "9".repeat(37));
        let b = format!("{}2", "9".repeat(37));
        assert!(watermark_less(&a, &b));
    }

    /// Magnitude, sign and fraction all order correctly.
    #[test]
    fn decimals_order_by_value_not_by_text() {
        assert!(watermark_less("9", "10"), "lexical order would say 9 > 10");
        assert!(watermark_less("-5", "3"));
        assert!(watermark_less("-10", "-5"), "more negative is less");
        assert!(!watermark_less("1.5", "1.45"));
        assert!(watermark_less("1.45", "1.5"));
        assert!(!watermark_less("0.1", "0.10"), "equal is not less");
    }

    #[test]
    fn watermarks_never_lower() {
        let mut cursor = OracleCursor::default();
        cursor.advance("s", "9", None);
        cursor.advance("s", "10", None);
        assert_eq!(cursor.streams["s"].watermark, "10");
        cursor.advance("s", "2", None);
        assert_eq!(cursor.streams["s"].watermark, "10", "never lowered");
        cursor.advance("t", "2026-01-02T00:00:00.000000Z", None);
        cursor.advance("t", "2026-01-01T00:00:00.000000Z", None);
        assert!(cursor.streams["t"].watermark.starts_with("2026-01-02"));
    }

    /// At an EQUAL watermark the tie-break advances — that is what
    /// lets a page boundary fall inside a run of equal cursor values
    /// without dropping or repeating them.
    #[test]
    fn the_tie_break_advances_at_an_equal_watermark() {
        let mut cursor = OracleCursor::default();
        cursor.advance("s", "5", Some("AAA"));
        cursor.advance("s", "5", Some("BBB"));
        assert_eq!(cursor.streams["s"].tie.as_deref(), Some("BBB"));
        cursor.advance("s", "9", Some("CCC"));
        assert_eq!(cursor.streams["s"].watermark, "9");
        assert_eq!(cursor.streams["s"].tie.as_deref(), Some("CCC"));
        cursor.advance("s", "1", Some("ZZZ"));
        assert_eq!(
            cursor.streams["s"].tie.as_deref(),
            Some("CCC"),
            "a lower watermark changes nothing"
        );
    }

    /// A future cursor format refuses as an upgrade prompt.
    #[test]
    fn a_future_format_refuses_upgrade_not_reset() {
        let value = Cursor::new(serde_json::json!({"format_version": 9}));
        let err = OracleCursor::decode(Some(&value))
            .expect_err("refused")
            .to_string();
        assert!(
            err.contains("v9 is newer than this build supports (v1)"),
            "{err}"
        );
    }

    /// The injection gate on the watermark literal.
    #[test]
    fn watermark_literals_are_shape_checked() {
        assert_eq!(checked_watermark_literal("42.5").expect("num"), "42.5");
        assert!(
            checked_watermark_literal("2026-01-01T00:00:00.000000Z")
                .expect("ts")
                .starts_with("TO_TIMESTAMP_TZ")
        );
        let err = checked_watermark_literal("1; DROP TABLE x --")
            .expect_err("refused")
            .to_string();
        assert!(err.contains("refusing to interpolate"), "{err}");
    }
}
