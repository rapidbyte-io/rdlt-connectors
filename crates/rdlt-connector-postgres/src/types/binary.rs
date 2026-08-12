//! The wire face: COPY BINARY bytes → values, and the incremental stream
//! decoder (THE hot path).
//!
//! The scalar decoders at the top turn one field's bytes into one value;
//! [`Decoder`] below parses the stream framing — 19-byte header, per tuple
//! an i16 field count then length-prefixed fields (NULL = −1), i16 −1
//! trailer — incrementally, because chunk boundaries never align with rows,
//! appending straight into Arrow builders. Batches cut at the byte target /
//! row maximum, only ever on row boundaries.
//!
//! Total: every failure is a typed error; no panics, no silent coercion
//! (fuzz target: `copy_decode`).

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema};

use super::Column;
use super::builder::ColumnBuilder;

/// µs from the PG epoch (2000-01-01) to the Unix epoch.
pub(super) const PG_EPOCH_MICROSECONDS: i64 = 946_684_800_000_000;
/// Days from the PG epoch to the Unix epoch.
pub(super) const PG_EPOCH_DAYS: i32 = 10_957;
const HEADER_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

#[derive(Debug, thiserror::Error)]
#[error("binary COPY decode: {0}")]
pub(crate) struct DecodeError(pub(crate) String);

fn fail<T>(detail: impl Into<String>) -> Result<T, String> {
    Err(detail.into())
}

pub(super) fn big_endian_i16(bytes: &[u8]) -> Result<i16, String> {
    match bytes.try_into() {
        Ok(array) => Ok(i16::from_be_bytes(array)),
        Err(_) => fail("short i16"),
    }
}

pub(super) fn big_endian_i32(bytes: &[u8]) -> Result<i32, String> {
    match bytes.try_into() {
        Ok(array) => Ok(i32::from_be_bytes(array)),
        Err(_) => fail("short i32"),
    }
}

pub(super) fn big_endian_i64(bytes: &[u8]) -> Result<i64, String> {
    match bytes.try_into() {
        Ok(array) => Ok(i64::from_be_bytes(array)),
        Err(_) => fail("short i64"),
    }
}

/// A fixed-width field, or a typed length error naming what was expected.
pub(super) fn exact<const N: usize>(field: &[u8], what: &str) -> Result<[u8; N], String> {
    field
        .try_into()
        .map_err(|_| format!("{what}: expected {N} bytes, got {}", field.len()))
}

/// PG binary numeric → i128 at the declared scale (NBASE-10000 digits;
/// NaN/±Inf have no Decimal representation and are typed errors).
pub(crate) fn decode_numeric(field: &[u8], scale: u8) -> Result<i128, String> {
    if field.len() < 8 {
        return fail("numeric: header short");
    }
    let digit_count = big_endian_i16(&field[0..2])? as i32;
    let weight = big_endian_i16(&field[2..4])? as i32;
    let sign_word = u16::from_be_bytes([field[4], field[5]]);
    let negative = match sign_word {
        0x0000 => false,
        0x4000 => true,
        0xC000 => return fail("numeric NaN is not representable as Decimal"),
        0xD000 | 0xF000 => return fail("numeric ±Infinity is not representable as Decimal"),
        other => return fail(format!("numeric: unknown sign word {other:#06x}")),
    };
    if !(0..=0x4000).contains(&(digit_count as i64)) || field.len() < 8 + (digit_count as usize) * 2
    {
        return fail("numeric: digit count exceeds field");
    }
    let mut accumulated: i128 = 0;
    for index in 0..digit_count as usize {
        let digit = u16::from_be_bytes([field[8 + index * 2], field[9 + index * 2]]) as i128;
        if digit >= 10_000 {
            return fail("numeric: base-10000 digit out of range");
        }
        accumulated = accumulated
            .checked_mul(10_000)
            .and_then(|value| value.checked_add(digit))
            .ok_or_else(|| "numeric: overflow while accumulating digits".to_string())?;
    }
    // The accumulated integer is value × 10000^(digit_count−1−weight);
    // rescale to the declared scale exactly or report drift.
    let exponent = 4 * (weight - digit_count + 1) + scale as i32;
    let mut value = accumulated;
    if exponent >= 0 {
        for _ in 0..exponent {
            value = value
                .checked_mul(10)
                .ok_or_else(|| "numeric: overflow while rescaling".to_string())?;
        }
    } else {
        for _ in 0..(-exponent) {
            if value % 10 != 0 {
                return fail(
                    "numeric: more fractional digits on the wire than the declared scale \
                     (schema drift)",
                );
            }
            value /= 10;
        }
    }
    Ok(if negative { -value } else { value })
}

/// i64 µs since PG epoch → Unix µs; the ±infinity sentinels saturate —
/// extremes stay visible, never NULL, never an error.
pub(super) fn rebase_timestamp(microseconds: i64) -> i64 {
    if microseconds == i64::MAX || microseconds == i64::MIN {
        return microseconds;
    }
    microseconds
        .checked_add(PG_EPOCH_MICROSECONDS)
        .unwrap_or(if microseconds > 0 { i64::MAX } else { i64::MIN })
}

/// i32 days since PG epoch → Unix days; ±infinity saturates.
pub(super) fn rebase_date(days: i32) -> i32 {
    if days == i32::MAX || days == i32::MIN {
        return days;
    }
    days.checked_add(PG_EPOCH_DAYS)
        .unwrap_or(if days > 0 { i32::MAX } else { i32::MIN })
}

/// 16 uuid wire bytes → canonical lowercase hyphenated text.
pub(super) fn uuid_text(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            text.push('-');
        }
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 0xf) as usize] as char);
    }
    text
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Header,
    Tuples,
    Done,
}

/// What one attempt to consume a tuple concluded.
enum Consumed {
    NeedMore,
    Row(usize),
    Trailer(usize),
}

/// The incremental COPY BINARY stream decoder.
#[derive(Debug)]
pub(crate) struct Decoder {
    columns: Vec<Column>,
    schema: Arc<Schema>,
    builders: Vec<ColumnBuilder>,
    state: State,
    /// Unparsed carry-over between `feed` calls (chunks split mid-tuple).
    carry: Vec<u8>,
    rows_in_batch: usize,
    bytes_in_batch: usize,
    rows_total: u64,
    batch_target_bytes: usize,
    batch_max_rows: usize,
}

impl Decoder {
    pub(crate) fn new(
        columns: Vec<Column>,
        batch_target_bytes: usize,
        batch_max_rows: usize,
    ) -> Result<Self, DecodeError> {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| Field::new(&column.name, column.kind.arrow(), !column.not_null))
                .collect::<Vec<_>>(),
        ));
        let builders = columns
            .iter()
            .map(|column| {
                ColumnBuilder::new(&column.kind)
                    .map_err(|detail| DecodeError(format!("column `{}`: {detail}", column.name)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            columns,
            schema,
            builders,
            state: State::Header,
            carry: Vec::new(),
            rows_in_batch: 0,
            bytes_in_batch: 0,
            rows_total: 0,
            batch_target_bytes: batch_target_bytes.max(1),
            batch_max_rows: batch_max_rows.max(1),
        })
    }

    pub(crate) fn rows_decoded(&self) -> u64 {
        self.rows_total
    }

    /// A zero-row batch carrying the declared schema — pushed for empty
    /// tables so destinations still materialize them with correct types.
    pub(crate) fn empty_batch(&self) -> RecordBatch {
        RecordBatch::new_empty(Arc::clone(&self.schema))
    }

    /// Feed one COPY data chunk; returns every batch completed by it.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<Vec<RecordBatch>, DecodeError> {
        if self.state == State::Done {
            if chunk.is_empty() {
                return Ok(Vec::new());
            }
            return Err(DecodeError("data after COPY trailer".into()));
        }
        self.carry.extend_from_slice(chunk);
        let mut position = 0usize;
        let mut batches = Vec::new();

        if self.state == State::Header {
            match self.try_consume_header(position)? {
                Some(next) => {
                    position = next;
                    self.state = State::Tuples;
                }
                None => return Ok(batches), // need more bytes
            }
        }

        while self.state == State::Tuples {
            match self.try_consume_tuple(position)? {
                Consumed::NeedMore => break,
                Consumed::Trailer(next) => {
                    position = next;
                    self.state = State::Done;
                    if self.carry.len() > position {
                        return Err(DecodeError("data after COPY trailer".into()));
                    }
                }
                Consumed::Row(next) => {
                    position = next;
                    if self.rows_in_batch >= self.batch_max_rows
                        || self.bytes_in_batch >= self.batch_target_bytes
                    {
                        batches.push(self.cut_batch()?);
                    }
                }
            }
        }
        self.carry.drain(..position);
        Ok(batches)
    }

    /// Stream end: the trailer must have been seen; flush the tail batch.
    pub(crate) fn finish(&mut self) -> Result<Option<RecordBatch>, DecodeError> {
        if self.state != State::Done {
            return Err(DecodeError(
                "COPY stream ended before the binary trailer (truncated stream)".into(),
            ));
        }
        if !self.carry.is_empty() {
            return Err(DecodeError("trailing bytes after COPY trailer".into()));
        }
        if self.rows_in_batch == 0 {
            return Ok(None);
        }
        Ok(Some(self.cut_batch()?))
    }

    fn cut_batch(&mut self) -> Result<RecordBatch, DecodeError> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect();
        self.rows_in_batch = 0;
        self.bytes_in_batch = 0;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|e| DecodeError(format!("assembling RecordBatch: {e}")))
    }

    /// Header: signature + flags(4) + extension length(4) + extension.
    fn try_consume_header(&self, position: usize) -> Result<Option<usize>, DecodeError> {
        let window = &self.carry[position..];
        if window.len() < 19 {
            return Ok(None);
        }
        if &window[..11] != HEADER_SIGNATURE {
            return Err(DecodeError("bad COPY binary signature".into()));
        }
        let extension_length = big_endian_i32(&window[15..19]).map_err(DecodeError)?;
        if extension_length < 0 {
            return Err(DecodeError("negative header extension length".into()));
        }
        let total = 19usize + extension_length as usize;
        if window.len() < total {
            return Ok(None);
        }
        Ok(Some(position + total))
    }

    fn try_consume_tuple(&mut self, position: usize) -> Result<Consumed, DecodeError> {
        // Split borrows: locate every field from an immutable window first,
        // then decode and append.
        let carry_length = self.carry.len();
        if carry_length - position < 2 {
            return Ok(Consumed::NeedMore);
        }
        let field_count =
            big_endian_i16(&self.carry[position..position + 2]).map_err(DecodeError)?;
        if field_count == -1 {
            return Ok(Consumed::Trailer(position + 2));
        }
        if field_count as usize != self.columns.len() {
            return Err(DecodeError(format!(
                "tuple has {field_count} fields, schema has {} (drift)",
                self.columns.len()
            )));
        }
        // First pass: locate every field; bail NeedMore on a partial tuple.
        //
        // This Vec is per ROW, and hoisting it to a reused field was tried in
        // the first-generation crate and MEASURED WORSE: +2.9% instructions
        // on the 10k-row decode bench. The allocation it removes is a
        // same-size request served from a hot allocator bin; the field
        // indirection it adds costs more, because a local's length and
        // capacity stay in registers where a field's must be reloaded
        // through `&mut self` on every push.
        let mut ranges: Vec<Option<(usize, usize)>> = Vec::with_capacity(self.columns.len());
        let mut cursor = position + 2;
        let mut tuple_bytes = 2usize;
        for _ in 0..self.columns.len() {
            if carry_length - cursor < 4 {
                return Ok(Consumed::NeedMore);
            }
            let length = big_endian_i32(&self.carry[cursor..cursor + 4]).map_err(DecodeError)?;
            cursor += 4;
            tuple_bytes += 4;
            if length == -1 {
                ranges.push(None);
                continue;
            }
            if length < 0 {
                return Err(DecodeError(format!("negative field length {length}")));
            }
            let length = length as usize;
            if carry_length - cursor < length {
                return Ok(Consumed::NeedMore);
            }
            ranges.push(Some((cursor, cursor + length)));
            cursor += length;
            tuple_bytes += length;
        }
        // Second pass: the whole tuple is buffered — decode and append.
        for (index, range) in ranges.iter().enumerate() {
            let column = &self.columns[index];
            match range {
                None => {
                    if column.not_null {
                        return Err(DecodeError(format!(
                            "NULL on NOT NULL column `{}` (schema drift)",
                            column.name
                        )));
                    }
                    self.builders[index].append_null();
                }
                Some((start, end)) => {
                    let field: &[u8] = &self.carry[*start..*end];
                    self.builders[index].append_wire(field).map_err(|detail| {
                        DecodeError(format!("column `{}`: {detail}", column.name))
                    })?;
                }
            }
        }
        self.rows_in_batch += 1;
        self.bytes_in_batch += tuple_bytes;
        self.rows_total += 1;
        Ok(Consumed::Row(cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Kind;
    use arrow_array::{Array, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::DataType;

    fn column(name: &str, kind: Kind, not_null: bool) -> Column {
        Column {
            name: name.into(),
            kind,
            not_null,
        }
    }

    fn header() -> Vec<u8> {
        let mut wire = HEADER_SIGNATURE.to_vec();
        wire.extend_from_slice(&0i32.to_be_bytes()); // flags
        wire.extend_from_slice(&0i32.to_be_bytes()); // extension length
        wire
    }

    fn tuple(fields: &[Option<Vec<u8>>]) -> Vec<u8> {
        let mut wire = (fields.len() as i16).to_be_bytes().to_vec();
        for field in fields {
            match field {
                None => wire.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(bytes) => {
                    wire.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    wire.extend_from_slice(bytes);
                }
            }
        }
        wire
    }

    fn trailer() -> Vec<u8> {
        (-1i16).to_be_bytes().to_vec()
    }

    fn numeric_wire(
        digit_count: i16,
        weight: i16,
        sign_word: u16,
        display_scale: u16,
        digits: &[u16],
    ) -> Vec<u8> {
        let mut wire = Vec::new();
        wire.extend_from_slice(&digit_count.to_be_bytes());
        wire.extend_from_slice(&weight.to_be_bytes());
        wire.extend_from_slice(&sign_word.to_be_bytes());
        wire.extend_from_slice(&display_scale.to_be_bytes());
        for digit in digits {
            wire.extend_from_slice(&digit.to_be_bytes());
        }
        wire
    }

    fn two_column_decoder() -> Decoder {
        Decoder::new(
            vec![
                column("id", Kind::Int64, true),
                column("name", Kind::Text, false),
            ],
            usize::MAX >> 1,
            usize::MAX >> 1,
        )
        .expect("two-column decoder builds")
    }

    #[test]
    fn decodes_rows_across_chunk_splits() {
        let mut wire = header();
        wire.extend(tuple(&[
            Some(7i64.to_be_bytes().to_vec()),
            Some(b"seven".to_vec()),
        ]));
        wire.extend(tuple(&[Some(8i64.to_be_bytes().to_vec()), None]));
        wire.extend(trailer());

        // Feed byte-by-byte: worst-case chunk misalignment.
        let mut decoder = two_column_decoder();
        let mut batches = Vec::new();
        for byte in &wire {
            batches.extend(decoder.feed(std::slice::from_ref(byte)).expect("feed"));
        }
        let tail = decoder.finish().expect("finish").expect("batch");
        batches.push(tail);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(decoder.rows_decoded(), 2);
        let batch = &batches[0];
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 7);
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "seven");
        assert!(names.is_null(1));
    }

    #[test]
    fn batch_cuts_on_max_rows() {
        let mut decoder = Decoder::new(
            vec![column("id", Kind::Int64, true)],
            usize::MAX >> 1,
            2, // cut every 2 rows
        )
        .expect("decoder builds");
        let mut wire = header();
        for value in 0..5i64 {
            wire.extend(tuple(&[Some(value.to_be_bytes().to_vec())]));
        }
        wire.extend(trailer());
        let mut batches = decoder.feed(&wire).expect("feed");
        if let Some(tail) = decoder.finish().expect("finish") {
            batches.push(tail);
        }
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            [2, 2, 1]
        );
    }

    #[test]
    fn numeric_decodes_at_declared_scale() {
        let mut decoder = Decoder::new(
            vec![column(
                "n",
                Kind::Decimal {
                    precision: 10,
                    scale: 2,
                },
                false,
            )],
            usize::MAX >> 1,
            usize::MAX >> 1,
        )
        .expect("decoder builds");
        let mut wire = header();
        // 1.25: digits [1, 2500], weight 0.
        wire.extend(tuple(&[Some(numeric_wire(2, 0, 0x0000, 2, &[1, 2500]))]));
        // -0.05: digits [500], weight −1, sign negative.
        wire.extend(tuple(&[Some(numeric_wire(1, -1, 0x4000, 2, &[500]))]));
        // 12345678.99: digits [1234, 5678, 9900], weight 1.
        wire.extend(tuple(&[Some(numeric_wire(
            3,
            1,
            0x0000,
            2,
            &[1234, 5678, 9900],
        ))]));
        wire.extend(trailer());
        let mut batches = decoder.feed(&wire).expect("feed");
        if let Some(tail) = decoder.finish().expect("finish") {
            batches.push(tail);
        }
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.value(0), 125); // 1.25 @ scale 2
        assert_eq!(values.value(1), -5); // -0.05
        assert_eq!(values.value(2), 1_234_567_899); // 12345678.99
    }

    #[test]
    fn numeric_nan_and_infinity_are_typed_errors() {
        for sign_word in [0xC000u16, 0xD000, 0xF000] {
            let error = decode_numeric(&numeric_wire(0, 0, sign_word, 0, &[]), 2).unwrap_err();
            assert!(
                error.contains("not representable"),
                "{sign_word:#06x}: {error}"
            );
        }
    }

    #[test]
    fn timestamp_rebase_and_infinity_saturation() {
        assert_eq!(rebase_timestamp(0), PG_EPOCH_MICROSECONDS);
        assert_eq!(rebase_timestamp(i64::MAX), i64::MAX);
        assert_eq!(rebase_timestamp(i64::MIN), i64::MIN);
        assert_eq!(rebase_date(0), PG_EPOCH_DAYS);
        assert_eq!(rebase_date(i32::MAX), i32::MAX);
        assert_eq!(rebase_date(i32::MIN), i32::MIN);
    }

    #[test]
    fn timestamps_land_as_utc_microseconds() {
        let mut decoder = Decoder::new(
            vec![column("ts", Kind::TimestampTz, false)],
            usize::MAX >> 1,
            usize::MAX >> 1,
        )
        .expect("decoder builds");
        let mut wire = header();
        wire.extend(tuple(&[Some(0i64.to_be_bytes().to_vec())])); // PG epoch
        wire.extend(trailer());
        let mut batches = decoder.feed(&wire).expect("feed");
        if let Some(tail) = decoder.finish().expect("finish") {
            batches.push(tail);
        }
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        );
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), PG_EPOCH_MICROSECONDS);
    }

    #[test]
    fn uuid_renders_canonical_lowercase() {
        let bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(uuid_text(&bytes), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn drift_and_truncation_are_typed_errors() {
        // NULL on NOT NULL column.
        let mut decoder = two_column_decoder();
        let mut wire = header();
        wire.extend(tuple(&[None, Some(b"x".to_vec())]));
        let error = decoder.feed(&wire).unwrap_err();
        assert!(error.0.contains("NOT NULL"), "{error}");

        // Wrong field count.
        let mut decoder = two_column_decoder();
        let mut wire = header();
        wire.extend(tuple(&[Some(1i64.to_be_bytes().to_vec())]));
        let error = decoder.feed(&wire).unwrap_err();
        assert!(error.0.contains("drift"), "{error}");

        // Truncated stream: no trailer before finish.
        let mut decoder = two_column_decoder();
        decoder.feed(&header()).expect("header only");
        let error = decoder.finish().unwrap_err();
        assert!(error.0.contains("truncated"), "{error}");

        // Bad signature.
        let mut decoder = two_column_decoder();
        let error = decoder.feed(&[0u8; 19]).unwrap_err();
        assert!(error.0.contains("signature"), "{error}");

        // Data after trailer.
        let mut decoder = two_column_decoder();
        let mut wire = header();
        wire.extend(trailer());
        wire.push(0);
        let error = decoder.feed(&wire).unwrap_err();
        assert!(error.0.contains("after COPY trailer"), "{error}");
    }

    #[test]
    fn jsonb_version_byte_stripped() {
        let mut decoder = Decoder::new(
            vec![column("j", Kind::Jsonb, false)],
            usize::MAX >> 1,
            usize::MAX >> 1,
        )
        .expect("decoder builds");
        let mut wire = header();
        let mut payload = vec![1u8];
        payload.extend_from_slice(br#"{"a":1}"#);
        wire.extend(tuple(&[Some(payload)]));
        wire.extend(trailer());
        let mut batches = decoder.feed(&wire).expect("feed");
        if let Some(tail) = decoder.finish().expect("finish") {
            batches.push(tail);
        }
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), r#"{"a":1}"#);
    }
}
