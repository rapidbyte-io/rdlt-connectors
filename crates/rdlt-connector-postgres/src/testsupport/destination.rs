//! Destination-side test access: the golden-SQL pin seams — DDL rendering,
//! the merge dialect, the unit-transaction literals, and stage naming — so
//! the pin suites compare emitted SQL as data without a server.

pub use crate::destination::catalog::{
    EnsureStatement, merge_ensure_statements, render_column_definition, sql_type, stage_name,
    stage_prefix, table_ddl_statements,
};
pub use crate::destination::dialect::Dialect;
pub use crate::destination::unit::{UNIT_BEGIN, UNIT_COMMIT, UNIT_ROLLBACK, UNIT_WORK_MEM};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow_schema::{Field, Schema};
use bytes::BytesMut;
use rdlt_connector_sdk::spi::core::{schema::ColumnType, types::LogicalType};
use std::sync::Arc;

use crate::types::encode;

/// One column of the pin/bench batch: name, the LOGICAL type when the
/// Arrow representation alone would not reach the wire kind (Utf8 covers
/// text, jsonb and uuid), and values chosen to include NULL plus whatever
/// the type's edges are.
struct PinColumn {
    name: &'static str,
    logical: Option<ColumnType>,
    array: Arc<dyn Array>,
}

fn scalar(logical: LogicalType) -> Option<ColumnType> {
    Some(ColumnType::Scalar { scalar: logical })
}

/// Every wire variant, in declaration order.
fn pin_columns() -> Vec<PinColumn> {
    let column = |name, logical, array| PinColumn {
        name,
        logical,
        array,
    };
    vec![
        column(
            "c_bool",
            None,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as Arc<dyn Array>,
        ),
        column(
            "c_int8",
            None,
            Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
        ),
        column(
            "c_float8",
            None,
            Arc::new(Float64Array::from(vec![
                Some(f64::MIN_POSITIVE),
                Some(-0.0),
                None,
            ])),
        ),
        column(
            "c_text",
            None,
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("héllo\u{1F600}"),
                None,
            ])),
        ),
        column(
            "c_bytea",
            None,
            Arc::new(BinaryArray::from_opt_vec(vec![
                Some(&b""[..]),
                Some(&b"\x00\xff\x7f"[..]),
                None,
            ])),
        ),
        column(
            "c_timestamptz",
            None,
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(0), Some(-1), None]).with_timezone("UTC"),
            ),
        ),
        column(
            "c_timestamp",
            None,
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(0),
                Some(-1),
                None,
            ])),
        ),
        column(
            "c_date",
            None,
            Arc::new(Date32Array::from(vec![Some(0), Some(-1), None])),
        ),
        column(
            "c_time",
            None,
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(0),
                Some(86_399_999_999),
                None,
            ])),
        ),
        column(
            "c_numeric",
            scalar(LogicalType::Decimal {
                precision: 38,
                scale: 9,
            }),
            Arc::new(
                Decimal128Array::from(vec![Some(0i128), Some(-123_456_789_012_345_678), None])
                    .with_precision_and_scale(38, 9)
                    .expect("precision/scale"),
            ),
        ),
        column(
            "c_jsonb",
            scalar(LogicalType::Json),
            Arc::new(StringArray::from(vec![
                Some("{}"),
                Some(r#"{"a":[1,null,"é"]}"#),
                None,
            ])),
        ),
        column(
            "c_uuid",
            scalar(LogicalType::Uuid),
            Arc::new(StringArray::from(vec![
                Some("00000000-0000-0000-0000-000000000000"),
                Some("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                None,
            ])),
        ),
    ]
}

/// Arrow field for a pin column: the field the destination would see.
fn field_of(column: &PinColumn) -> Field {
    Field::new(column.name, column.array.data_type().clone(), true)
}

/// The batch the pin and the bench both encode. `rows` cycles the value
/// vectors so the bench has volume; the pin uses the natural length.
pub fn bench_batch(rows: usize) -> RecordBatch {
    let columns = pin_columns();
    let schema = Arc::new(Schema::new(
        columns.iter().map(field_of).collect::<Vec<_>>(),
    ));
    let arrays: Vec<Arc<dyn Array>> = columns
        .iter()
        .map(|column| {
            let indices = UInt32Array::from(
                (0..rows)
                    .map(|row| u32::try_from(row % column.array.len()).expect("index fits"))
                    .collect::<Vec<u32>>(),
            );
            arrow_select::take::take(column.array.as_ref(), &indices, None).expect("take")
        })
        .collect();
    RecordBatch::try_new(schema, arrays).expect("bench batch")
}

/// Encode the pin batch's VALUES through the shipping path, returning
/// `(column, per-row wire bytes or NULL)`.
///
/// The encoder emits length-prefixed FIELDS, so the prefix is read back off
/// each one here: `-1` is NULL, otherwise the declared length must account
/// for exactly the bytes that follow — which pins the prefix as well as the
/// value. What is rendered stays value-bytes-only, so the fixture is a
/// stable oracle across encoder rewrites.
pub fn encode_pin_values() -> Vec<(String, Vec<Option<Vec<u8>>>)> {
    let columns = pin_columns();
    let rows = columns[0].array.len();
    columns
        .iter()
        .map(|column| {
            let wire = encode::wire_of(column.logical.as_ref(), column.array.data_type())
                .expect("supported wire");
            let encoder =
                encode::Encoder::new(wire, column.array.as_ref()).expect("column encodable");
            let cells = (0..rows)
                .map(|row| {
                    let mut buffer = BytesMut::new();
                    encoder.append_field(row, &mut buffer).expect("encodable");
                    let (prefix, value) = buffer.split_at(4);
                    let declared = i32::from_be_bytes(prefix.try_into().expect("4 bytes"));
                    if declared < 0 {
                        assert_eq!(declared, -1, "{}: NULL is spelled -1", column.name);
                        assert!(value.is_empty(), "{}: NULL carries no bytes", column.name);
                        return None;
                    }
                    assert_eq!(
                        declared as usize,
                        value.len(),
                        "{}: field length prefix disagrees with the bytes written",
                        column.name
                    );
                    Some(value.to_vec())
                })
                .collect();
            (column.name.to_string(), cells)
        })
        .collect()
}

/// The gated encoder hot path (bench body): a whole batch through the
/// production encoding path — downcast once per column, fields appended
/// into one reused buffer. Returns the byte count so the work cannot be
/// optimized away.
pub fn bench_encode(batch: &RecordBatch) -> u64 {
    let schema = batch.schema();
    let mut buffer = BytesMut::with_capacity(64 * 1024);
    let mut bytes = 0u64;
    // Wires come from the SAME logical types the pin uses. Resolving them
    // from the Arrow type alone would bench `c_jsonb` and `c_uuid` as plain
    // text — so the jsonb version byte and the uuid parser, both per-cell
    // work in production, would never appear in the instruction count and a
    // regression in either would pass the gate untouched.
    let logical = pin_columns();
    let encoders: Vec<encode::Encoder<'_>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let column = logical
                .iter()
                .find(|pin| pin.name == field.name())
                .expect("bench batch columns come from pin_columns()");
            let wire = encode::wire_of(column.logical.as_ref(), field.data_type())
                .expect("supported wire");
            encode::Encoder::new(wire, batch.column(index).as_ref()).expect("column encodable")
        })
        .collect();
    for row in 0..batch.num_rows() {
        for encoder in &encoders {
            buffer.clear();
            encoder.append_field(row, &mut buffer).expect("encodable");
            bytes += buffer.len() as u64;
        }
    }
    bytes
}
