//! Driver rows into an Arrow batch, column by column.
//!
//! This is what the NDJSON transport used to do the long way round:
//! render typed values to JSON text, hand the text to the engine, and
//! have the engine parse it back into these very builders. Besides
//! the round trip it lost information at every step — decimals became
//! strings, binary was hex-encoded at double the size, NaN and
//! Infinity had no JSON literal and were nulled, and the type had to
//! be re-supplied out of band as a hint.
//!
//! Values are pulled by the DECLARED Arrow type, never guessed from
//! what the driver happens to hand back.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder,
    RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use rdlt_connector_sdk::spi::error::SourceError;

/// One column's in-progress Arrow array.
enum Column {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Decimal(Decimal128Builder, u8, i8),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Timestamp(TimestampMicrosecondBuilder, Option<Arc<str>>),
    Boolean(BooleanBuilder),
}

/// Accumulates rows for one batch.
pub(crate) struct BatchBuilder {
    schema: Arc<Schema>,
    columns: Vec<Column>,
    rows: usize,
    /// Which column, if any, carries the watermark.
    cursor_at: Option<usize>,
}

impl BatchBuilder {
    pub(crate) fn new(schema: Arc<Schema>) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Int64 => Column::Int64(Int64Builder::new()),
                DataType::Float64 => Column::Float64(Float64Builder::new()),
                DataType::Decimal128(p, s) => Column::Decimal(
                    Decimal128Builder::new()
                        .with_precision_and_scale(*p, *s)
                        .unwrap_or_else(|_| Decimal128Builder::new()),
                    *p,
                    *s,
                ),
                DataType::Binary => Column::Binary(BinaryBuilder::new()),
                DataType::Boolean => Column::Boolean(BooleanBuilder::new()),
                DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    Column::Timestamp(TimestampMicrosecondBuilder::new(), tz.clone())
                }
                _ => Column::Utf8(StringBuilder::new()),
            })
            .collect();
        Self {
            schema,
            columns,
            rows: 0,
            cursor_at: None,
        }
    }

    /// Name the column whose value is the watermark, so [`append`]
    /// can hand it back without a second pass over the row.
    pub(crate) fn with_cursor_at(mut self, at: Option<usize>) -> Self {
        self.cursor_at = at;
        self
    }

    pub(crate) fn len(&self) -> usize {
        self.rows
    }

    /// Append one driver row, returning the watermark if this stream
    /// has a cursor column.
    pub(crate) fn append(&mut self, row: &oracle::Row) -> Result<Option<String>, SourceError> {
        for (at, column) in self.columns.iter_mut().enumerate() {
            let field = self.schema.field(at);
            append_one(column, row, at, field.name())?;
        }
        self.rows += 1;
        match self.cursor_at {
            Some(at) => Ok(Some(watermark_text(
                row,
                at,
                self.schema.field(at).data_type(),
            )?)),
            None => Ok(None),
        }
    }

    pub(crate) fn finish(mut self) -> Result<RecordBatch, SourceError> {
        let arrays: Vec<ArrayRef> = self
            .columns
            .iter_mut()
            .map(|column| match column {
                Column::Int64(b) => Arc::new(b.finish()) as ArrayRef,
                Column::Float64(b) => Arc::new(b.finish()) as ArrayRef,
                Column::Decimal(b, p, s) => {
                    let array = b.finish();
                    match array.clone().with_precision_and_scale(*p, *s) {
                        Ok(exact) => Arc::new(exact) as ArrayRef,
                        Err(_) => Arc::new(array) as ArrayRef,
                    }
                }
                Column::Utf8(b) => Arc::new(b.finish()) as ArrayRef,
                Column::Binary(b) => Arc::new(b.finish()) as ArrayRef,
                Column::Timestamp(b, tz) => {
                    let array = b.finish();
                    match tz {
                        Some(zone) => Arc::new(array.with_timezone(zone.clone())) as ArrayRef,
                        None => Arc::new(array) as ArrayRef,
                    }
                }
                Column::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
            })
            .collect();
        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| SourceError::fatal(format!("assembling an Arrow batch: {e}")))
    }
}

/// Pull one value at `at` into its builder, by the DECLARED type.
fn append_one(
    column: &mut Column,
    row: &oracle::Row,
    at: usize,
    name: &str,
) -> Result<(), SourceError> {
    let bad = |e: oracle::Error| SourceError::fatal(format!("reading column `{name}`: {e}"));
    match column {
        Column::Int64(b) => b.append_option(row.get::<_, Option<i64>>(at).map_err(bad)?),
        // NaN and Infinity are legal BINARY_DOUBLE values and Arrow
        // holds them natively — JSON had no literal and nulled them.
        Column::Float64(b) => b.append_option(row.get::<_, Option<f64>>(at).map_err(bad)?),
        Column::Decimal(b, _, scale) => {
            // The driver renders NUMBER as exact text; scaling it
            // here keeps all 38 digits, where f64 would round.
            match row.get::<_, Option<String>>(at).map_err(bad)? {
                Some(text) => match scaled_i128(&text, *scale) {
                    Some(value) => b.append_value(value),
                    None => {
                        return Err(SourceError::fatal(format!(
                            "column `{name}`: `{text}` does not fit the declared \
                             decimal scale {scale}"
                        )));
                    }
                },
                None => b.append_null(),
            }
        }
        Column::Utf8(b) => b.append_option(row.get::<_, Option<String>>(at).map_err(bad)?),
        Column::Binary(b) => match row.get::<_, Option<Vec<u8>>>(at).map_err(bad)? {
            Some(bytes) => b.append_value(bytes),
            None => b.append_null(),
        },
        Column::Timestamp(b, tz) => {
            let micros = match tz {
                Some(_) => row
                    .get::<_, Option<oracle::sql_type::Timestamp>>(at)
                    .map_err(bad)?
                    .map(utc_micros)
                    .transpose()
                    .map_err(|e: String| SourceError::fatal(format!("column `{name}`: {e}")))?,
                None => row
                    .get::<_, Option<chrono::NaiveDateTime>>(at)
                    .map_err(bad)?
                    .map(|t| t.and_utc().timestamp_micros()),
            };
            b.append_option(micros);
        }
        Column::Boolean(b) => b.append_option(row.get::<_, Option<bool>>(at).map_err(bad)?),
    }
    Ok(())
}

/// A zoned Oracle timestamp as microseconds since the UTC epoch.
///
/// THE OFFSET IS APPLIED HERE, deliberately, rather than through the
/// driver's `DateTime<Utc>` conversion — that one keeps the
/// wall-clock fields and relabels them UTC, so
/// `03:04:05.678 +02:00` arrived as 03:04:05.678Z instead of
/// 01:04:05.678Z. Measured, not assumed: every zoned value would have
/// been silently shifted by its own zone.
fn utc_micros(stamp: oracle::sql_type::Timestamp) -> Result<i64, String> {
    let naive = chrono::NaiveDate::from_ymd_opt(stamp.year(), stamp.month(), stamp.day())
        .and_then(|d| {
            d.and_hms_nano_opt(
                stamp.hour(),
                stamp.minute(),
                stamp.second(),
                stamp.nanosecond(),
            )
        })
        .ok_or_else(|| format!("`{stamp}` is not a representable instant"))?;
    let offset_minutes =
        i64::from(stamp.tz_hour_offset()) * 60 + i64::from(stamp.tz_minute_offset());
    Ok(naive.and_utc().timestamp_micros() - offset_minutes * 60 * 1_000_000)
}

/// Oracle's exact decimal text as a scaled i128.
///
/// Returns `None` when the value carries more fractional digits than
/// the declared scale can hold — silently truncating there would
/// change the number.
fn scaled_i128(text: &str, scale: i8) -> Option<i128> {
    let text = text.trim();
    let (sign, digits) = match text.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, text.strip_prefix('+').unwrap_or(text)),
    };
    let (whole, fraction) = match digits.split_once('.') {
        Some((w, f)) => (w, f),
        None => (digits, ""),
    };
    let scale = usize::try_from(scale).ok()?;
    if fraction.len() > scale {
        // Trailing zeros beyond the scale are not a loss.
        if fraction[scale..].bytes().any(|b| b != b'0') {
            return None;
        }
    }
    let mut packed = String::with_capacity(whole.len() + scale);
    packed.push_str(whole);
    for i in 0..scale {
        packed.push(fraction.as_bytes().get(i).copied().unwrap_or(b'0') as char);
    }
    packed.parse::<i128>().ok().map(|v| v * sign)
}

/// The watermark, rendered so it can be compared and written back
/// into SQL on the next run.
fn watermark_text(row: &oracle::Row, at: usize, kind: &DataType) -> Result<String, SourceError> {
    // BY THE DECLARED TYPE, never by asking for a String first: the
    // driver will happily render a DATE in the session's NLS format
    // (`02-JAN-26`), and the resume literal's format string then
    // rejects it with ORA-01861 on the very next run.
    const ISO_MICROS: &str = "%Y-%m-%dT%H:%M:%S%.6f+00:00";
    let text = match kind {
        // Through the SAME offset-applying path as the stored value,
        // so the watermark and the column cannot disagree.
        DataType::Timestamp(_, Some(_)) => row
            .get::<_, Option<oracle::sql_type::Timestamp>>(at)
            .ok()
            .flatten()
            .and_then(|s| utc_micros(s).ok())
            .and_then(chrono::DateTime::from_timestamp_micros)
            .map(|t| t.format(ISO_MICROS).to_string()),
        DataType::Timestamp(_, None) => row
            .get::<_, Option<chrono::NaiveDateTime>>(at)
            .ok()
            .flatten()
            .map(|t| t.format(ISO_MICROS).to_string()),
        DataType::Int64 => row
            .get::<_, Option<i64>>(at)
            .ok()
            .flatten()
            .map(|v| v.to_string()),
        // A non-finite value is a legal BINARY_DOUBLE but NOT a
        // watermark: `inf` renders, `advance` treats it as the
        // highest value there is and persists it, and the NEXT run
        // refuses to parse it — leaving state that only a human can
        // clear. Refusing here keeps the failure inside the run that
        // caused it.
        DataType::Float64 => row
            .get::<_, Option<f64>>(at)
            .ok()
            .flatten()
            .filter(|v| v.is_finite())
            .map(|v| v.to_string()),
        // Exact decimals and bare NUMBER both keep their digits.
        _ => row.get::<_, Option<String>>(at).ok().flatten(),
    };
    text.ok_or_else(|| {
        SourceError::fatal(
            "the cursor column yielded a value no watermark can represent — choose another \
             cursor, or read the stream without one",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_decimals_scale_without_rounding() {
        assert_eq!(scaled_i128("10.25", 2), Some(1025));
        assert_eq!(scaled_i128("-3.5", 2), Some(-350));
        assert_eq!(scaled_i128("7", 2), Some(700));
        assert_eq!(scaled_i128("0.01", 2), Some(1));
    }

    /// 38 digits is Oracle's own limit and i128 holds it — this is
    /// the case f64 would have silently rounded.
    #[test]
    fn the_full_38_digits_survive() {
        let digits = "1".repeat(38);
        assert_eq!(scaled_i128(&digits, 0), digits.parse::<i128>().ok());
    }

    /// The zone offset is APPLIED, not discarded.
    ///
    /// The driver's own `DateTime<Utc>` conversion keeps the
    /// wall-clock fields and relabels them UTC, which silently shifts
    /// every zoned value by its own offset. Measured live before this
    /// was written.
    #[test]
    fn a_zoned_timestamp_becomes_its_utc_instant() {
        let plus_two = oracle::sql_type::Timestamp::new(2026, 1, 2, 3, 4, 5, 678_000_000)
            .expect("valid")
            .and_tz_hm_offset(2, 0)
            .expect("valid offset");
        let utc = chrono::DateTime::from_timestamp_micros(utc_micros(plus_two).expect("micros"))
            .expect("instant");
        assert_eq!(utc.to_string(), "2026-01-02 01:04:05.678 UTC");

        // And the other direction, so a sign error cannot pass.
        let minus_five = oracle::sql_type::Timestamp::new(2026, 1, 2, 3, 4, 5, 0)
            .expect("valid")
            .and_tz_hm_offset(-5, 0)
            .expect("valid offset");
        let utc = chrono::DateTime::from_timestamp_micros(utc_micros(minus_five).expect("micros"))
            .expect("instant");
        assert_eq!(utc.to_string(), "2026-01-02 08:04:05 UTC");
    }

    /// More fraction than the declared scale is REFUSED, not
    /// truncated — truncating would change the number.
    #[test]
    fn excess_precision_is_refused_but_trailing_zeros_are_not() {
        assert_eq!(scaled_i128("1.239", 2), None);
        assert_eq!(scaled_i128("1.2300", 2), Some(123));
    }
}
