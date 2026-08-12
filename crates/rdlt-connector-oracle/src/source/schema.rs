//! Oracle's declared types, mapped to Arrow.
//!
//! THE RULEBOOK, row by row and deliberate:
//!
//! - `NUMBER(p,s)` with `s == 0` and `p <= 18` is `Int64` — it fits,
//!   exactly, and integers are what most keys are.
//! - `NUMBER(p,s)` with `0 < s <= p <= 38` is `Decimal128(p,s)`,
//!   exact. This is the case the NDJSON transport could not carry:
//!   a decimal became a string and landed as TEXT downstream.
//! - Everything else numeric — bare `NUMBER` (floating scale),
//!   `p > 38`, and NEGATIVE scales (Oracle rounds ABOVE the decimal
//!   point at `NUMBER(10,-2)`, and `FLOAT` describes as scale -127) —
//!   travels as `Utf8` holding the exact digits. Decimal128 cannot
//!   express a negative scale, and silently rounding through f64
//!   would lose the value.
//! - `BINARY_FLOAT`/`BINARY_DOUBLE` are `Float64`. These are true
//!   binary floats, so NaN and Infinity are legal values and Arrow
//!   holds them natively — JSON had no literal and had to null them.
//! - `FLOAT` is NOT one of those, despite the name: it is a NUMBER
//!   subtype with a BINARY precision, stored as decimal. It follows
//!   the NUMBER rules above, because f64 would round it.
//! - `DATE` CARRIES A TIME in Oracle, so it is a naive timestamp,
//!   NEVER an Arrow `Date32`.
//! - `TIMESTAMP WITH [LOCAL] TIME ZONE` is UTC-stamped; the session
//!   is pinned to UTC at connect so the LOCAL variant is well
//!   defined.
//! - `RAW`/`BLOB`/`LONG RAW` are `Binary`. Under NDJSON these were
//!   hex-encoded, which doubled every byte on the wire.
//! - `CLOB`/`NCLOB`/`NVARCHAR2`/`NCHAR` are `Utf8` — the national
//!   character set included, which the previous driver silently
//!   destroyed.
//! - Native `JSON` (21c+) is REFUSED: the driver has no fetch path
//!   for it, so claiming a mapping only moved the failure into its
//!   internals.
//!
//! Anything else is a typed refusal naming the column: a type this
//! rulebook has not considered must not be guessed at.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use oracle::sql_type::OracleType;

/// The Arrow type for one Oracle column, or the reason it has none.
pub(crate) fn arrow_type(name: &str, oracle_type: &OracleType) -> Result<DataType, String> {
    Ok(match oracle_type {
        OracleType::Number(precision, scale) => number_type(*precision, *scale),
        // BINARY_FLOAT/BINARY_DOUBLE are true IEEE binary floats.
        OracleType::BinaryFloat | OracleType::BinaryDouble => DataType::Float64,
        // `FLOAT` is NOT one of those. It is a NUMBER subtype with a
        // BINARY precision — `FLOAT(126)` is ~38 decimal digits and
        // `REAL` is FLOAT(63), ~18 — stored as decimal and handed
        // over as exact text. Routing it through f64 silently rounded
        // it: 1234567890123456789 came back 1234567890123456768.
        OracleType::Float(precision) => number_type(*precision, -127),
        OracleType::Int64 => DataType::Int64,
        OracleType::UInt64 => DataType::Decimal128(20, 0),

        OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Long
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Rowid
        | OracleType::Xml => DataType::Utf8,

        OracleType::Raw(_) | OracleType::LongRaw | OracleType::BLOB => DataType::Binary,

        // Oracle's DATE carries a time; it is a naive timestamp.
        OracleType::Date | OracleType::Timestamp(_) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }

        OracleType::Boolean => DataType::Boolean,

        // The driver has no fetch path for a native 21c+ JSON
        // column, so mapping it to Utf8 only moved the failure into
        // its own internal error at describe time. Refused by name
        // instead, with the workaround that does work.
        OracleType::Json => {
            return Err(format!(
                "column `{name}` is native JSON, which this driver cannot fetch — select it \
                 as `JSON_SERIALIZE({name} RETURNING VARCHAR2)` through a view, or exclude \
                 it from the stream"
            ));
        }

        other => {
            return Err(format!(
                "column `{name}` is {other}, which this connector has no mapping for — \
                 exclude it from the stream or open an issue naming the type"
            ));
        }
    })
}

/// NUMBER's three cases, kept in one place because the boundaries
/// are the whole point.
fn number_type(precision: u8, scale: i8) -> DataType {
    // Arrow REFUSES a decimal whose scale exceeds its precision, but
    // `NUMBER(2,5)` is legal Oracle (it holds values like 0.00012).
    // Letting it through produced a builder that silently fell back
    // to (38,10) and then failed batch assembly with a message about
    // Arrow rather than about the column.
    if scale as i16 > precision as i16 {
        return DataType::Utf8;
    }
    if scale < 0 || precision == 0 || precision > 38 {
        // Bare NUMBER, over-precise NUMBER, and negative scales:
        // exact digits as text. Decimal128 cannot say "rounded above
        // the decimal point", and f64 would round silently.
        DataType::Utf8
    } else if scale == 0 && precision <= 18 {
        DataType::Int64
    } else {
        DataType::Decimal128(precision, scale)
    }
}

/// Does this column order well enough to carry a watermark?
///
/// `Utf8` is included ONLY for numerics that landed there for
/// magnitude reasons — see [`is_numeric`]. A genuine string column is
/// not a watermark.
pub(crate) fn cursor_capable(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::Int64 | DataType::Float64 | DataType::Decimal128(..) | DataType::Timestamp(..)
    )
    // NOTE: no `Date32` arm. Oracle's DATE carries a time and maps to
    // a naive timestamp, so `arrow_type` can never yield `Date32`.
}

/// Is this an Oracle NUMBER whose Arrow type is text only because its
/// magnitude has no exact binary home?
///
/// It still ORDERS numerically, which is what a watermark needs — and
/// bare `NUMBER` is how Oracle estates spell a sequence-backed
/// surrogate key, i.e. the most common incremental cursor there is.
pub(crate) fn is_numeric(oracle_type: &OracleType) -> bool {
    matches!(
        oracle_type,
        OracleType::Number(..)
            | OracleType::Float(_)
            | OracleType::BinaryFloat
            | OracleType::BinaryDouble
            | OracleType::Int64
            | OracleType::UInt64
    )
}

/// The Arrow schema for a described result set.
///
/// Column names are lower-cased for the engine, and a collision there
/// is REFUSED rather than resolved: two Oracle columns differing only
/// in case (legal, via quoted identifiers) would otherwise become one
/// field and silently drop a value.
pub(crate) fn schema_of(columns: &[oracle::ColumnInfo]) -> Result<Schema, String> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        let name = column.name().to_lowercase();
        if !seen.insert(name.clone()) {
            return Err(format!(
                "two columns differ only in case (`{name}`) — the emitted rows would keep \
                 only one of them"
            ));
        }
        let kind = arrow_type(column.name(), column.oracle_type())?;
        fields.push(Field::new(name, kind, column.nullable()));
    }
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_splits_into_int_decimal_and_text() {
        assert_eq!(number_type(8, 0), DataType::Int64);
        assert_eq!(number_type(18, 0), DataType::Int64);
        assert_eq!(number_type(12, 2), DataType::Decimal128(12, 2));
        assert_eq!(number_type(38, 10), DataType::Decimal128(38, 10));
    }

    /// Oracle allows a scale ABOVE the precision; Arrow does not.
    #[test]
    fn a_scale_above_the_precision_travels_as_text() {
        // NUMBER(2,5) holds 0.00012 — legal, and not a Decimal128.
        assert_eq!(number_type(2, 5), DataType::Utf8);
    }

    /// FLOAT is a NUMBER subtype, not an IEEE float — f64 would
    /// round its 38 digits.
    #[test]
    fn float_follows_the_number_rules() {
        assert_eq!(
            arrow_type("f", &OracleType::Float(126)).expect("mapped"),
            DataType::Utf8
        );
    }

    /// The cases Decimal128 cannot express, and f64 would corrupt.
    #[test]
    fn magnitudes_without_an_exact_home_travel_as_text() {
        // Bare NUMBER: Oracle accepts any magnitude.
        assert_eq!(number_type(0, 0), DataType::Utf8);
        // NUMBER(10,-2) rounds ABOVE the decimal point.
        assert_eq!(number_type(10, -2), DataType::Utf8);
        // FLOAT describes with scale -127.
        assert_eq!(number_type(126, -127), DataType::Utf8);
        // A 19-digit integer does not fit i64.
        assert_eq!(number_type(19, 0), DataType::Decimal128(19, 0));
    }

    /// DATE carries a time in Oracle — mapping it to Date32 would
    /// silently drop it.
    #[test]
    fn date_is_a_timestamp_not_a_date() {
        assert_eq!(
            arrow_type("d", &OracleType::Date).expect("mapped"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    /// Binary stays binary: under the old NDJSON transport it was
    /// hex-encoded, doubling every byte.
    #[test]
    fn raw_and_blob_are_binary() {
        assert_eq!(
            arrow_type("r", &OracleType::Raw(16)).expect("mapped"),
            DataType::Binary
        );
        assert_eq!(
            arrow_type("b", &OracleType::BLOB).expect("mapped"),
            DataType::Binary
        );
    }

    /// An unmapped type is refused BY NAME, never guessed.
    #[test]
    fn an_unmapped_type_is_refused_by_name() {
        let err = arrow_type("iv", &OracleType::IntervalDS(2, 6)).expect_err("refused");
        assert!(err.contains("`iv`"), "{err}");
        assert!(err.contains("no mapping"), "{err}");
    }
}
