//! The encode face: Arrow columns → binary-COPY wire fields (the
//! destination's hot path, mirroring [`super::binary`] on the read side).
//!
//! [`wire_of`] decides one column's wire type — LOGICAL type first, so
//! Utf8-as-text vs Utf8-as-json/uuid can never confuse — and [`Encoder`] is
//! that column downcast ONCE, ready to encode any row: the per-cell path has
//! no `downcast_ref`, no trait object, and no allocation (the three costs a
//! per-cell `Box<dyn ToSql>` design pays on every one of the ~12M cells a
//! 1M-row load encodes).
//!
//! Errors are `String` details naming nothing but the failure; callers add
//! the column context and the taxonomy.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use bytes::{BufMut, BytesMut};
use rdlt_connector_sdk::spi::core::{schema::ColumnType, types::LogicalType};
use tokio_postgres::types::{ToSql, Type};

/// The wire decision for one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wire {
    Bool,
    Int8,
    Float8,
    Text,
    Bytea,
    TimestampTz,
    TimestampNaive,
    Date,
    Time,
    Numeric { scale: u8 },
    Jsonb,
    Uuid,
}

/// Decide the wire encoding from the schema's logical type + the batch's
/// Arrow type. `logical: None` (a column outside the ensured schema, e.g.
/// engine system columns in edge orders) falls back to the Arrow type.
pub(crate) fn wire_of(logical: Option<&ColumnType>, arrow: &DataType) -> Result<Wire, String> {
    // The logical type is consulted ONLY where it says something the Arrow
    // type cannot: json and uuid are both Utf8 on the representation side,
    // so without it they would be sent as text.
    //
    // Decimal and Time are deliberately NOT consulted. Their scale and unit
    // live in the ARRAY — that is the scale the i128 payload is actually
    // stored at — and reading them from the schema instead would silently
    // multiply or divide every value by a power of ten whenever the two
    // disagree. The representation match below reads the array, and
    // additionally refuses a negative scale rather than wrapping it.
    if let Some(ColumnType::Scalar { scalar }) = logical {
        match (scalar, arrow) {
            (LogicalType::Json, DataType::Utf8) => return Ok(Wire::Jsonb),
            (LogicalType::Uuid, DataType::Utf8) => return Ok(Wire::Uuid),
            _ => {} // representation-driven below
        }
    }
    Ok(match arrow {
        DataType::Boolean => Wire::Bool,
        DataType::Int64 => Wire::Int8,
        DataType::Float64 => Wire::Float8,
        DataType::Utf8 => Wire::Text,
        DataType::Binary => Wire::Bytea,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Wire::TimestampTz,
        DataType::Timestamp(TimeUnit::Microsecond, None) => Wire::TimestampNaive,
        DataType::Date32 => Wire::Date,
        DataType::Time64(TimeUnit::Microsecond) => Wire::Time,
        DataType::Decimal128(_, scale) => Wire::Numeric {
            scale: u8::try_from(*scale)
                .map_err(|_| format!("negative decimal scale {scale} unsupported"))?,
        },
        other => return Err(format!("unsupported arrow type for COPY: {other}")),
    })
}

/// One column of a batch, already downcast, ready to encode any row. The
/// enum arm IS the encode decision.
pub(crate) enum Encoder<'a> {
    Bool(&'a BooleanArray),
    Int8(&'a Int64Array),
    Float8(&'a Float64Array),
    Text(&'a StringArray),
    Bytea(&'a BinaryArray),
    TimestampTz(&'a TimestampMicrosecondArray),
    TimestampNaive(&'a TimestampMicrosecondArray),
    Date(&'a Date32Array),
    Time(&'a Time64MicrosecondArray),
    Numeric {
        array: &'a Decimal128Array,
        scale: u8,
    },
    Jsonb(&'a StringArray),
    Uuid(&'a StringArray),
}

impl<'a> Encoder<'a> {
    /// Downcast once for the whole column — a mismatch is caught HERE, once
    /// per column, instead of once per cell.
    pub(crate) fn new(wire: Wire, array: &'a dyn Array) -> Result<Self, String> {
        macro_rules! cast {
            ($ty:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$ty>()
                    .ok_or_else(|| "array type mismatch".to_string())?
            };
        }
        Ok(match wire {
            Wire::Bool => Self::Bool(cast!(BooleanArray)),
            Wire::Int8 => Self::Int8(cast!(Int64Array)),
            Wire::Float8 => Self::Float8(cast!(Float64Array)),
            Wire::Text => Self::Text(cast!(StringArray)),
            Wire::Bytea => Self::Bytea(cast!(BinaryArray)),
            Wire::TimestampTz => Self::TimestampTz(cast!(TimestampMicrosecondArray)),
            Wire::TimestampNaive => Self::TimestampNaive(cast!(TimestampMicrosecondArray)),
            Wire::Date => Self::Date(cast!(Date32Array)),
            Wire::Time => Self::Time(cast!(Time64MicrosecondArray)),
            Wire::Numeric { scale } => Self::Numeric {
                array: cast!(Decimal128Array),
                scale,
            },
            Wire::Jsonb => Self::Jsonb(cast!(StringArray)),
            Wire::Uuid => Self::Uuid(cast!(StringArray)),
        })
    }

    /// Append one cell as a complete binary-COPY *field*: an `i32` byte
    /// length followed by the value bytes, or `-1` alone for NULL.
    ///
    /// Field shape lives here; TUPLE and STREAM framing (field count,
    /// header, trailer) lives in the caller — the encoder owns how a value
    /// looks, the write path owns how the stream is assembled.
    #[inline]
    pub(crate) fn append_field(&self, row: usize, out: &mut BytesMut) -> Result<(), String> {
        /// A length-prefixed value whose prefix is backfilled once the
        /// value's own length is known. NULL never reaches here — the macro
        /// emits its bare `-1` and returns — so there is exactly one place
        /// that decides what a NULL looks like on the wire. Generic over the
        /// writer, not `dyn`: each arm monomorphizes, so the value encoding
        /// inlines into this frame instead of paying an indirect call per
        /// cell.
        #[inline(always)]
        fn framed<W>(out: &mut BytesMut, write: W) -> Result<(), String>
        where
            W: FnOnce(&mut BytesMut) -> Result<(), String>,
        {
            let start = out.len();
            out.put_i32(0); // length placeholder, backfilled below
            write(out)?;
            let length = i32::try_from(out.len() - start - 4)
                .map_err(|_| "value exceeds 2 GiB".to_string())?;
            out[start..start + 4].copy_from_slice(&length.to_be_bytes());
            Ok(())
        }

        macro_rules! variable_field {
            ($array:expr, $write:expr) => {{
                if $array.is_null(row) {
                    out.put_i32(-1);
                    return Ok(());
                }
                framed(out, $write)
            }};
        }

        /// A value whose encoded width is known before it is written: the
        /// length prefix and the value go out as ONE contiguous store.
        ///
        /// `framed` cannot do this — it must emit a placeholder, let the
        /// writer decide the length, then come back and patch: three touches
        /// of the buffer, two through `put_slice`, which on `BytesMut`
        /// bounds-checks, re-derives the uninitialised chunk, and dispatches
        /// a `memcpy` per call. Half the wire types have a compile-time
        /// width and would pay all of that for nothing.
        ///
        /// The scratch array is sized by the widest such value (a 16-byte
        /// uuid) rather than by `N + 4`, which const generics cannot express
        /// on stable; the debug assertion keeps that honest.
        ///
        /// Measured in the first-generation crate on these four arms: the
        /// encoder microbench fell 5.4% of instructions and a full 1M-row
        /// load 2.0%, reproducible to ±0.05%; wall clock could not see it
        /// (server-dominated). Do not re-litigate with a stopwatch.
        ///
        /// The temporal arms stay on `framed`: they also have fixed widths,
        /// but their values are validated through chrono and written by
        /// `ToSql` — taking them would re-derive Postgres's epoch arithmetic
        /// here and, with it, the range rejections those arms deliberately
        /// perform.
        #[inline(always)]
        fn fixed<const N: usize>(out: &mut BytesMut, value: [u8; N]) {
            const WIDEST: usize = 16;
            debug_assert!(N <= WIDEST, "fixed-width value wider than a uuid");
            let mut framed = [0u8; WIDEST + 4];
            framed[..4].copy_from_slice(&(N as i32).to_be_bytes());
            framed[4..4 + N].copy_from_slice(&value);
            out.put_slice(&framed[..4 + N]);
        }

        macro_rules! fixed_field {
            ($array:expr, $bytes:expr) => {{
                if $array.is_null(row) {
                    out.put_i32(-1);
                    return Ok(());
                }
                fixed(out, $bytes);
                Ok(())
            }};
        }

        /// `ToSql::to_sql` on a CONCRETE type: monomorphic, inlinable, and
        /// the same bytes the driver would have written.
        fn sql<T: ToSql>(value: &T, wire_type: &Type, out: &mut BytesMut) -> Result<(), String> {
            value
                .to_sql(wire_type, out)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        match *self {
            Self::Bool(array) => fixed_field!(array, [u8::from(array.value(row))]),
            Self::Int8(array) => fixed_field!(array, array.value(row).to_be_bytes()),
            Self::Float8(array) => fixed_field!(array, array.value(row).to_be_bytes()),
            // Borrowed: no `String::to_owned` per cell (`&str: ToSql` writes
            // the same UTF-8 bytes an owned String would).
            Self::Text(array) => {
                variable_field!(array, |o: &mut BytesMut| sql(
                    &array.value(row),
                    &Type::TEXT,
                    o
                ))
            }
            Self::Bytea(array) => {
                variable_field!(array, |o: &mut BytesMut| sql(
                    &array.value(row),
                    &Type::BYTEA,
                    o
                ))
            }
            Self::TimestampTz(array) => variable_field!(array, |o: &mut BytesMut| {
                let microseconds = array.value(row);
                let instant = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(microseconds)
                    .ok_or_else(|| "timestamp out of range".to_string())?;
                sql(&instant, &Type::TIMESTAMPTZ, o)
            }),
            Self::TimestampNaive(array) => variable_field!(array, |o: &mut BytesMut| {
                let microseconds = array.value(row);
                let instant = chrono::DateTime::from_timestamp_micros(microseconds)
                    .ok_or_else(|| "timestamp out of range".to_string())?
                    .naive_utc();
                sql(&instant, &Type::TIMESTAMP, o)
            }),
            Self::Date(array) => variable_field!(array, |o: &mut BytesMut| {
                // Checked: the shift overflows i32 near the type's extremes,
                // and an overflow-checks build would panic rather than
                // refuse.
                let days = i64::from(array.value(row)) + 719_163;
                let date = i32::try_from(days)
                    .ok()
                    .and_then(chrono::NaiveDate::from_num_days_from_ce_opt)
                    .ok_or_else(|| "date out of range".to_string())?;
                sql(&date, &Type::DATE, o)
            }),
            Self::Time(array) => variable_field!(array, |o: &mut BytesMut| {
                // Bounds-checked BEFORE the cast. A negative or beyond-24h
                // value wraps through `as u64` into a plausible time, which
                // would send a DIFFERENT time rather than refusing the
                // value.
                const MICROSECONDS_PER_DAY: i64 = 86_400_000_000;
                let raw = array.value(row);
                if !(0..MICROSECONDS_PER_DAY).contains(&raw) {
                    return Err(format!(
                        "time {raw} µs is outside a day (0..{MICROSECONDS_PER_DAY})"
                    ));
                }
                let microseconds = raw as u64;
                let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                    (microseconds / 1_000_000) as u32,
                    ((microseconds % 1_000_000) * 1_000) as u32,
                )
                .ok_or_else(|| "time out of range".to_string())?;
                sql(&time, &Type::TIME, o)
            }),
            Self::Numeric { array, scale } => variable_field!(array, |o: &mut BytesMut| {
                append_numeric(array.value(row), scale, o);
                Ok(())
            }),
            Self::Jsonb(array) => variable_field!(array, |o: &mut BytesMut| {
                // Version byte 1 + the UTF-8 document — the exact byte the
                // wire face's Jsonb decode strips.
                o.put_u8(1);
                o.put_slice(array.value(row).as_bytes());
                Ok(())
            }),
            Self::Uuid(array) => {
                if array.is_null(row) {
                    out.put_i32(-1);
                    return Ok(());
                }
                let text = array.value(row);
                let bytes = parse_uuid_text(text)
                    .ok_or_else(|| format!("`{text}` is not a canonical uuid"))?;
                fixed(out, bytes);
                Ok(())
            }
        }
    }
}

/// Postgres binary `numeric`: i128 at a declared scale → ndigits/weight/
/// sign/dscale + base-10⁴ digit groups (canonical: zero groups stripped),
/// appended in place.
///
/// Hand-written deliberately: `postgres-protocol` exposes no
/// `numeric_to_sql`; `rust_decimal`'s 96-bit mantissa cannot hold
/// `Decimal128`'s 38 digits; and `bigdecimal` allocates per value, which is
/// the cost this exists to remove.
///
/// Grouping is by integer divmod, NOT via a decimal string. The obvious
/// route — multiply by 10^pad to align the fraction to a group boundary,
/// then take `% 10000` — overflows: a 39-digit i128 times 10³ exceeds
/// u128::MAX. Instead the pad is folded into the FIRST group only
/// (`(v % 10^(4−pad)) × 10^pad`, both factors bounded so the product stays
/// under 10⁴), after which dividing by `10^(4−pad)` leaves a value whose
/// remaining groups are plain `% 10000` steps. Same digits, no wide
/// intermediate, no allocation.
pub(crate) fn append_numeric(value: i128, scale: u8, out: &mut BytesMut) {
    let pad = u32::from(scale) % 4;
    let pad = (4 - pad) % 4;
    let low_divisor = 10u128.pow(4 - pad);

    // Base-10⁴ groups, least-significant first. 16 is ample: an i128
    // magnitude is at most 39 digits, plus 3 pad digits, is 11 groups.
    let mut groups = [0u16; 16];
    let mut count = 0usize;
    let mut remaining = value.unsigned_abs();

    let first = (remaining % low_divisor) * 10u128.pow(pad);
    groups[count] = u16::try_from(first).expect("a base-10^4 group is < 10000");
    count += 1;
    remaining /= low_divisor;
    while remaining > 0 {
        groups[count] = u16::try_from(remaining % 10_000).expect("a base-10^4 group is < 10000");
        count += 1;
        remaining /= 10_000;
    }

    let display_scale = i16::from(scale);
    if count == 1 && groups[0] == 0 {
        // Canonical zero: no digits, weight 0.
        out.put_i16(0); // ndigits
        out.put_i16(0); // weight
        out.put_u16(0); // sign (+)
        out.put_i16(display_scale);
        return;
    }

    // weight = index of the most significant group relative to the decimal
    // point (units group = 0).
    let fraction_groups = (i32::from(scale) + pad as i32) / 4;
    let weight = count as i32 - 1 - fraction_groups;

    // Canonical form strips trailing zero groups — the LEAST significant
    // end, which is the FRONT of this array. Leading zero groups cannot
    // occur: the loop stops as soon as `remaining` is exhausted, so the last
    // group written is always non-zero (the all-zero case returned above).
    let mut low = 0usize;
    while low < count && groups[low] == 0 {
        low += 1;
    }
    let digit_count = count - low;

    out.put_i16(i16::try_from(digit_count).expect("at most 11 groups"));
    // `expect`, not a substitute value: a weight beyond i16 cannot be
    // encoded, and silently sending i16::MAX would write a number that is
    // not the one that was asked for. Unreachable — the digit count is
    // bounded above.
    out.put_i16(i16::try_from(weight).expect("weight fits i16: at most 11 base-10000 groups"));
    out.put_u16(if value < 0 { 0x4000 } else { 0x0000 });
    out.put_i16(display_scale);
    for index in (low..count).rev() {
        out.put_u16(groups[index]);
    }
}

/// Postgres binary `uuid`: 16 raw bytes, parsed from the canonical text form
/// the engine ships (LogicalType::Uuid arrives as Utf8).
///
/// The `uuid` crate is deliberately NOT adopted: it appears nowhere in the
/// measured profile, so the "measured-better" test for taking a dependency
/// is unmet, and `Uuid::try_parse` accepts a strictly narrower set of texts
/// than both this parser and PostgreSQL's own `uuid_in` — swapping it in
/// would silently start rejecting rows the server would have accepted.
pub(crate) fn parse_uuid_text(text: &str) -> Option<[u8; 16]> {
    // Accept the same textual forms the SERVER's uuid input accepts:
    // optional urn:uuid: prefix, optional braces, hyphenated or bare hex.
    let text = text
        .strip_prefix("urn:uuid:")
        .or_else(|| text.strip_prefix("URN:UUID:"))
        .unwrap_or(text);
    let text = text
        .strip_prefix('{')
        .and_then(|inner| inner.strip_suffix('}'))
        .unwrap_or(text);
    let mut bytes = [0u8; 16];
    let mut index = 0;
    let mut high_nibble: Option<u8> = None;
    let mut hex_seen = 0usize;
    let mut hyphen_at: Option<usize> = None;
    for character in text.bytes() {
        if character == b'-' {
            // Hyphens only at group boundaries (8-4-4-4-12), matching the
            // server's grouping rule — and at most ONE per boundary, so
            // "550e8400--e29b-…" is rejected the way the server rejects it.
            // Without the second guard, `hex_seen` alone still reads as a
            // boundary for the repeat.
            if !matches!(hex_seen, 8 | 12 | 16 | 20) || hyphen_at == Some(hex_seen) {
                return None;
            }
            hyphen_at = Some(hex_seen);
            continue;
        }
        hex_seen += 1;
        let nibble = (character as char).to_digit(16)? as u8;
        match high_nibble {
            None => high_nibble = Some(nibble),
            Some(high) => {
                if index >= 16 {
                    return None;
                }
                bytes[index] = (high << 4) | nibble;
                index += 1;
                high_nibble = None;
            }
        }
    }
    (index == 16 && high_nibble.is_none()).then_some(bytes)
}

#[cfg(test)]
mod round_trip {
    use proptest::prelude::*;

    use super::*;
    use crate::types::binary::decode_numeric;

    #[test]
    fn numeric_edges_round_trip_through_the_wire_decoder() {
        // Zero at every scale, negatives, exact scale boundaries, and values
        // whose base-10000 alignment needs padding.
        for &(value, scale) in &[
            (0i128, 0u8),
            (0, 4),
            (0, 7),
            (1, 0),
            (-1, 0),
            (1, 4),      // 0.0001
            (-1, 4),     // -0.0001
            (12_345, 2), // 123.45
            (-12_345, 2),
            (10_000, 4), // 1.0000
            (99_999_999, 4),
            (1, 38),
            (i128::from(i64::MAX), 0),
            (i128::from(i64::MIN) + 1, 0),
            (123_456_789_012_345_678_901_234_567i128, 9),
            (-123_456_789_012_345_678_901_234_567i128, 9),
            (1_000_000_000_000i128, 12), // exactly 1.000000000000
        ] {
            let mut wire = BytesMut::new();
            append_numeric(value, scale, &mut wire);
            let decoded =
                decode_numeric(&wire, scale).unwrap_or_else(|e| panic!("({value}, {scale}): {e}"));
            assert_eq!(decoded, value, "({value}, {scale})");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2048, ..ProptestConfig::default() })]

        /// Encode → decode is the identity everywhere the DECODER can
        /// represent the padded accumulation (its own documented boundary:
        /// it accumulates wire digits into i128 before rescaling, so
        /// |v|·10^pad must fit). The encoder is verified beyond that range
        /// structurally below and against a live server in the destination
        /// conformance suite.
        #[test]
        fn numeric_round_trips(value in any::<i128>(), scale in 0u8..=38) {
            let pad = (4 - (scale as u32 % 4)) % 4;
            let limit = i128::MAX / 10i128.pow(pad);
            let value = value.clamp(-limit, limit);
            let mut wire = BytesMut::new();
            append_numeric(value, scale, &mut wire);
            prop_assert_eq!(decode_numeric(&wire, scale).unwrap(), value);
        }
    }

    /// The exact overflow shape — 39-digit magnitudes at pad-requiring
    /// scales. Verified STRUCTURALLY (group-by-group against the decimal
    /// string), since the decoder's own i128 accumulation cannot represent
    /// these.
    #[test]
    fn numeric_wire_is_exact_at_i128_extremes() {
        for &(value, scale) in &[
            (i128::MAX, 3u8),
            (i128::MIN, 3),
            (i128::MAX, 37),
            (i128::MIN + 1, 1),
        ] {
            let mut wire = BytesMut::new();
            append_numeric(value, scale, &mut wire);
            let digit_count = i16::from_be_bytes([wire[0], wire[1]]) as usize;
            let weight = i16::from_be_bytes([wire[2], wire[3]]) as i32;
            let sign_word = u16::from_be_bytes([wire[4], wire[5]]);
            assert_eq!(sign_word, if value < 0 { 0x4000 } else { 0x0000 });
            // Reconstruct the decimal digits from the groups and compare
            // against the ground-truth rendering of the input.
            let mut digits = String::new();
            for group in 0..digit_count {
                let value = u16::from_be_bytes([wire[8 + group * 2], wire[9 + group * 2]]);
                digits.push_str(&format!("{value:04}"));
            }
            // The wire value is digits × 10000^(weight − ndigits + 1);
            // rescale to `scale` and compare as strings.
            let exponent = 4 * (weight - digit_count as i32 + 1) + scale as i32;
            let mut expected = value.unsigned_abs().to_string();
            match exponent.cmp(&0) {
                std::cmp::Ordering::Greater => {
                    digits.push_str(&"0".repeat(exponent as usize));
                }
                std::cmp::Ordering::Less => {
                    expected.push_str(&"0".repeat((-exponent) as usize));
                }
                std::cmp::Ordering::Equal => {}
            }
            assert_eq!(
                digits.trim_start_matches('0'),
                expected.trim_start_matches('0'),
                "({value}, {scale})"
            );
        }
    }

    #[test]
    fn uuid_parses_the_server_forms_and_rejects_garbage() {
        let bytes = parse_uuid_text("550e8400-e29b-41d4-a716-446655440000").expect("canonical");
        assert_eq!(bytes[0], 0x55);
        assert_eq!(bytes[15], 0x00);
        // Uppercase accepted (hex digits case-insensitive).
        assert!(parse_uuid_text("550E8400-E29B-41D4-A716-446655440000").is_some());
        for bad in [
            "",
            "550e8400e29b41d4a716446655440000zz",
            "550e8400-e29b-41d4-a716-44665544000",   // short
            "550e8400-e29b-41d4-a716-4466554400000", // long
            "550e-8400e29b-41d4-a716-446655440000",  // wrong hyphens
            "not-a-uuid-at-all",
        ] {
            assert!(parse_uuid_text(bad).is_none(), "{bad}");
        }
        // Bare 32-hex, urn prefix, and braces: all server-accepted.
        assert!(parse_uuid_text("550e8400e29b41d4a716446655440000").is_some());
        assert!(parse_uuid_text("urn:uuid:550e8400-e29b-41d4-a716-446655440000").is_some());
        assert!(parse_uuid_text("{550e8400-e29b-41d4-a716-446655440000}").is_some());
        assert!(parse_uuid_text("{550e8400e29b41d4a716446655440000}").is_some());
        assert!(parse_uuid_text("urn:uuid:{nope}").is_none());
        // A REPEATED hyphen at one boundary: `hex_seen` alone still reads as
        // a legal boundary for the second one, so a consumed boundary is
        // tracked. The server rejects these; so must we.
        for repeated in [
            "550e8400--e29b-41d4-a716-446655440000",
            "550e8400-e29b--41d4-a716-446655440000",
            "550e8400-e29b-41d4-a716--446655440000",
            "550e8400---e29b-41d4-a716-446655440000",
        ] {
            assert!(parse_uuid_text(repeated).is_none(), "{repeated}");
        }
        // …while one hyphen per boundary stays accepted, including partial
        // hyphenation, which the server also accepts.
        assert!(parse_uuid_text("550e8400-e29b41d4a716446655440000").is_some());
        assert!(parse_uuid_text("550e8400e29b41d4-a716-446655440000").is_some());
        assert!(parse_uuid_text("{550e8400-e29b-41d4-a716-446655440000").is_none());
    }
}

#[cfg(test)]
mod fields {
    use super::*;
    use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};

    fn field_bytes(encoder: &Encoder<'_>, row: usize) -> Result<BytesMut, String> {
        let mut out = BytesMut::new();
        encoder.append_field(row, &mut out)?;
        Ok(out)
    }

    #[test]
    fn null_is_a_bare_minus_one_with_no_value_bytes() {
        let array = Int64Array::from(vec![None, Some(7)]);
        let encoder = Encoder::new(Wire::Int8, &array).expect("encoder");
        assert_eq!(
            &field_bytes(&encoder, 0).unwrap()[..],
            &(-1i32).to_be_bytes()
        );
        // …and a present value carries its length, so NULL and a zero-length
        // value can never be confused on the wire.
        let present = field_bytes(&encoder, 1).unwrap();
        assert_eq!(&present[..4], &8i32.to_be_bytes());
        assert_eq!(&present[4..], &7i64.to_be_bytes());
    }

    #[test]
    fn an_empty_string_is_length_zero_not_null() {
        let array = StringArray::from(vec![Some(""), None]);
        let encoder = Encoder::new(Wire::Text, &array).expect("encoder");
        assert_eq!(&field_bytes(&encoder, 0).unwrap()[..], &0i32.to_be_bytes());
        assert_eq!(
            &field_bytes(&encoder, 1).unwrap()[..],
            &(-1i32).to_be_bytes()
        );
    }

    #[test]
    fn unrepresentable_values_are_errors_naming_the_value() {
        let bad_uuid = StringArray::from(vec![Some("not-a-uuid")]);
        let encoder = Encoder::new(Wire::Uuid, &bad_uuid).expect("encoder");
        assert!(field_bytes(&encoder, 0).is_err());

        let far_future = TimestampMicrosecondArray::from(vec![Some(i64::MAX)]);
        let encoder = Encoder::new(Wire::TimestampTz, &far_future).expect("encoder");
        assert!(field_bytes(&encoder, 0).is_err());
    }

    #[test]
    fn a_column_type_mismatch_is_caught_at_construction() {
        let array = Int64Array::from(vec![Some(1)]);
        assert!(Encoder::new(Wire::Text, &array).is_err());
    }
}
