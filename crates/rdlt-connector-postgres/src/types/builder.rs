//! The one Arrow column builder both input faces feed: wire bytes from the
//! COPY decoder, text forms from the CDC change stream. One builder per
//! column, appended row by row, finished into an Arrow array.
//!
//! Having a single builder is what makes "one conversion vocabulary" true at
//! the output end: whichever face a value arrives through, it lands in the
//! same Arrow shape by construction.

use arrow_array::ArrayRef;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use std::sync::Arc;

use super::Kind;
use super::binary;
use super::text;

#[derive(Debug)]
enum Backing {
    Bool(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Decimal(Decimal128Builder),
    Text(StringBuilder),
    Binary(BinaryBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
}

/// One column's builder, dispatching on its [`Kind`].
#[derive(Debug)]
pub(crate) struct ColumnBuilder {
    kind: Kind,
    backing: Backing,
}

impl ColumnBuilder {
    /// Reflection and hints only ever emit a valid (precision, scale) — a
    /// rejection here is an internal invariant break, surfaced as a typed
    /// error rather than a panic.
    pub(crate) fn new(kind: &Kind) -> Result<Self, String> {
        let backing = match kind {
            Kind::Bool => Backing::Bool(BooleanBuilder::new()),
            Kind::Int16 | Kind::Int32 | Kind::Int64 => Backing::Int64(Int64Builder::new()),
            Kind::Float32 | Kind::Float64 => Backing::Float64(Float64Builder::new()),
            Kind::Decimal { precision, scale } => Backing::Decimal(
                Decimal128Builder::new()
                    .with_precision_and_scale(*precision, *scale as i8)
                    .map_err(|e| {
                        format!(
                            "internal invalid decimal shape (precision {precision}, \
                             scale {scale}): {e}"
                        )
                    })?,
            ),
            Kind::Text | Kind::Jsonb | Kind::Uuid => Backing::Text(StringBuilder::new()),
            Kind::Bytea => Backing::Binary(BinaryBuilder::new()),
            Kind::TimestampTz => {
                Backing::Timestamp(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
            }
            Kind::TimestampNaive => Backing::Timestamp(TimestampMicrosecondBuilder::new()),
            Kind::Date => Backing::Date(Date32Builder::new()),
            Kind::Time => Backing::Time(Time64MicrosecondBuilder::new()),
        };
        Ok(Self {
            kind: *kind,
            backing,
        })
    }

    pub(crate) fn append_null(&mut self) {
        match &mut self.backing {
            Backing::Bool(builder) => builder.append_null(),
            Backing::Int64(builder) => builder.append_null(),
            Backing::Float64(builder) => builder.append_null(),
            Backing::Decimal(builder) => builder.append_null(),
            Backing::Text(builder) => builder.append_null(),
            Backing::Binary(builder) => builder.append_null(),
            Backing::Timestamp(builder) => builder.append_null(),
            Backing::Date(builder) => builder.append_null(),
            Backing::Time(builder) => builder.append_null(),
        }
    }

    /// Append one COPY BINARY field. The (kind, backing) pairing is fixed at
    /// construction, so the mismatch arm is an internal error, not a user
    /// condition.
    pub(crate) fn append_wire(&mut self, field: &[u8]) -> Result<(), String> {
        match (&mut self.backing, self.kind) {
            (Backing::Bool(builder), Kind::Bool) => {
                let [value] = binary::exact::<1>(field, "bool")?;
                builder.append_value(value != 0);
            }
            (Backing::Int64(builder), Kind::Int16) => {
                builder.append_value(binary::big_endian_i16(field)? as i64);
            }
            (Backing::Int64(builder), Kind::Int32) => {
                builder.append_value(binary::big_endian_i32(field)? as i64);
            }
            (Backing::Int64(builder), Kind::Int64) => {
                builder.append_value(binary::big_endian_i64(field)?);
            }
            (Backing::Float64(builder), Kind::Float32) => {
                let bytes = binary::exact::<4>(field, "float4")?;
                builder.append_value(f32::from_be_bytes(bytes) as f64);
            }
            (Backing::Float64(builder), Kind::Float64) => {
                let bytes = binary::exact::<8>(field, "float8")?;
                builder.append_value(f64::from_be_bytes(bytes));
            }
            (Backing::Decimal(builder), Kind::Decimal { scale, .. }) => {
                builder.append_value(binary::decode_numeric(field, scale)?);
            }
            (Backing::Text(builder), Kind::Text) => {
                let value = std::str::from_utf8(field)
                    .map_err(|_| "invalid UTF-8 in text field".to_string())?;
                builder.append_value(value);
            }
            (Backing::Text(builder), Kind::Jsonb) => {
                let (&version, rest) = field
                    .split_first()
                    .ok_or_else(|| "empty jsonb field".to_string())?;
                if version != 1 {
                    return Err(format!("unknown jsonb version {version}"));
                }
                let value =
                    std::str::from_utf8(rest).map_err(|_| "invalid UTF-8 in jsonb".to_string())?;
                builder.append_value(value);
            }
            (Backing::Text(builder), Kind::Uuid) => {
                let bytes = binary::exact::<16>(field, "uuid")?;
                builder.append_value(binary::uuid_text(&bytes));
            }
            (Backing::Binary(builder), Kind::Bytea) => builder.append_value(field),
            (Backing::Timestamp(builder), Kind::TimestampTz | Kind::TimestampNaive) => {
                let microseconds = binary::big_endian_i64(field)?;
                builder.append_value(binary::rebase_timestamp(microseconds));
            }
            (Backing::Date(builder), Kind::Date) => {
                let days = binary::big_endian_i32(field)?;
                builder.append_value(binary::rebase_date(days));
            }
            (Backing::Time(builder), Kind::Time) => {
                builder.append_value(binary::big_endian_i64(field)?);
            }
            _ => return Err("builder/kind mismatch (internal)".into()),
        }
        Ok(())
    }

    /// Append one Postgres TEXT output form (the CDC face). Same target
    /// shapes as [`Self::append_wire`], by construction.
    pub(crate) fn append_text(&mut self, value: &str) -> Result<(), String> {
        match (&mut self.backing, self.kind) {
            (Backing::Bool(builder), Kind::Bool) => {
                builder.append_value(text::parse_bool(value)?);
            }
            (Backing::Int64(builder), Kind::Int16 | Kind::Int32 | Kind::Int64) => {
                builder.append_value(text::parse_int(value)?);
            }
            (Backing::Float64(builder), Kind::Float32 | Kind::Float64) => {
                builder.append_value(text::parse_float(value)?);
            }
            (Backing::Decimal(builder), Kind::Decimal { scale, .. }) => {
                builder.append_value(text::parse_decimal_scaled(value, scale)?);
            }
            // Text-family targets: jsonb and uuid ALREADY arrive in their
            // canonical text forms from the server.
            (Backing::Text(builder), Kind::Text | Kind::Jsonb | Kind::Uuid) => {
                builder.append_value(value);
            }
            (Backing::Binary(builder), Kind::Bytea) => {
                builder.append_value(text::parse_bytea_hex(value)?);
            }
            (Backing::Timestamp(builder), Kind::TimestampTz) => {
                builder.append_value(text::parse_timestamp(value, true)?);
            }
            (Backing::Timestamp(builder), Kind::TimestampNaive) => {
                builder.append_value(text::parse_timestamp(value, false)?);
            }
            (Backing::Date(builder), Kind::Date) => {
                builder.append_value(text::parse_date_days(value)?);
            }
            (Backing::Time(builder), Kind::Time) => {
                builder.append_value(text::parse_time_microseconds(value)?);
            }
            _ => return Err("builder/kind mismatch (internal)".into()),
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> ArrayRef {
        match &mut self.backing {
            Backing::Bool(builder) => Arc::new(builder.finish()),
            Backing::Int64(builder) => Arc::new(builder.finish()),
            Backing::Float64(builder) => Arc::new(builder.finish()),
            Backing::Decimal(builder) => Arc::new(builder.finish()),
            Backing::Text(builder) => Arc::new(builder.finish()),
            Backing::Binary(builder) => Arc::new(builder.finish()),
            Backing::Timestamp(builder) => Arc::new(builder.finish()),
            Backing::Date(builder) => Arc::new(builder.finish()),
            Backing::Time(builder) => Arc::new(builder.finish()),
        }
    }
}
