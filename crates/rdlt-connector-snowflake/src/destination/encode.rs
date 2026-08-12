//! Batches become parquet parts; short bookkeeping strings become SQL
//! literals. Nothing else in this crate turns a value into text.
//!
//! Row data never rides statement text on this path — it travels as a
//! parquet file the service reads back — so the literal escape survives
//! only for the state document and the receipt, which carry short,
//! known strings. It stays load-bearing all the same: this is the one
//! value-to-text seam, and a bad escape would not corrupt a row, it
//! would rewrite the statement.

use arrow_array::RecordBatch;
use rdlt_connector_sdk::spi::DestinationError;
use rdlt_connector_sdk::spi::core::TableSchema;

/// Escape a string for the inside of single quotes.
///
/// Quotes double (the service's escape), and so do backslashes — the
/// service treats backslash as an escape by default, and an unhandled
/// trailing one would swallow the closing quote and turn the rest of
/// the statement into string data.
pub(super) fn sql_literal_body(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

/// A parquet part still being written, spanning as many batches as
/// `parts` allows before it is closed and uploaded.
///
/// The rows are PROJECTED to the ensured schema on the way in, not
/// written as delivered: the load statement projects columns by NAME
/// out of the file, a batch may legitimately carry more than the
/// schema ensured, and an extra column would fail the COPY for the
/// whole file. Written in the SCHEMA's column order and case — the
/// COPY's projection names exactly these.
///
/// Snappy, the workspace's parquet default: a part lives for one
/// network hop and one read, where decompression speed beats ratio.
pub(super) struct OpenPart {
    writer: parquet::arrow::ArrowWriter<Vec<u8>>,
    /// The projected schema this part was opened for. Carried because
    /// `ArrowWriter` does not expose its own, and a parquet file holds
    /// exactly one schema.
    schema: arrow_schema::SchemaRef,
    /// Rows appended so far — what the COPY's rowcount check is
    /// measured against.
    rows: u64,
    /// When the part opened, for `roll_after_seconds`.
    opened_at: std::time::Instant,
}

impl std::fmt::Debug for OpenPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenPart")
            .field("rows", &self.rows)
            .field("encoded_len", &self.encoded_len())
            .finish_non_exhaustive()
    }
}

/// Project a batch onto the ensured schema's columns, in its order.
fn project(schema: &TableSchema, batch: &RecordBatch) -> Result<RecordBatch, DestinationError> {
    let indices = schema
        .columns
        .iter()
        .map(|column| {
            // A missing column is a CONTRACT violation, not a NULL: the
            // engine guarantees batches conform to the ensured schema.
            batch.schema().index_of(&column.name).map_err(|_| {
                DestinationError::fatal(format!(
                    "snowflake: batch for `{}` is missing column `{}`",
                    schema.table, column.name
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    batch.project(&indices).map_err(|e| {
        DestinationError::fatal(format!(
            "snowflake: projecting batch for `{}`: {e}",
            schema.table
        ))
    })
}

impl OpenPart {
    /// Open a part shaped for `batch` projected onto `schema`, and
    /// write those rows into it.
    pub(super) fn begin(
        schema: &TableSchema,
        batch: &RecordBatch,
    ) -> Result<Self, DestinationError> {
        let projected = project(schema, batch)?;
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let writer =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), projected.schema(), Some(properties))
                .map_err(DestinationError::fatal)?;
        let mut part = Self {
            writer,
            schema: projected.schema(),
            rows: 0,
            opened_at: std::time::Instant::now(),
        };
        part.append(schema, batch)?;
        Ok(part)
    }

    /// Would `batch` projected onto `schema` change this part's shape?
    ///
    /// Additive evolution mid-unit is legal here — the COPY projects
    /// the latest superset and earlier files load NULL for a column
    /// they lack — but a parquet FILE still holds one schema, so the
    /// part must close rather than widen.
    pub(super) fn shape_differs(&self, schema: &TableSchema, batch: &RecordBatch) -> bool {
        match project(schema, batch) {
            // A projection failure is not a shape change: let `append`
            // raise it, so the error names the missing column.
            Err(_) => false,
            Ok(projected) => projected.schema() != self.schema,
        }
    }

    pub(super) fn append(
        &mut self,
        schema: &TableSchema,
        batch: &RecordBatch,
    ) -> Result<(), DestinationError> {
        let projected = project(schema, batch)?;
        self.rows += projected.num_rows() as u64;
        self.writer
            .write(&projected)
            .map_err(DestinationError::fatal)
    }

    /// Bytes encoded so far. Exact for flushed row groups, the
    /// library's ANTICIPATED size for pages not yet flushed — so a
    /// part closes a little under its target at small sizes and
    /// essentially at it once row groups flush.
    pub(super) fn encoded_len(&self) -> u64 {
        (self.writer.bytes_written() + self.writer.in_progress_size()) as u64
    }

    pub(super) fn rows(&self) -> u64 {
        self.rows
    }

    pub(super) fn open_for_secs(&self) -> u64 {
        self.opened_at.elapsed().as_secs()
    }

    /// Finish the file, yielding its bytes and their rowcount.
    pub(super) fn finish(self) -> Result<(Vec<u8>, u64), DestinationError> {
        let rows = self.rows;
        let bytes = self.writer.into_inner().map_err(DestinationError::fatal)?;
        Ok((bytes, rows))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use rdlt_connector_sdk::spi::core::{
        ColumnDef, ColumnType, LogicalType, Provenance, TableName,
    };

    use super::*;

    /// One batch as one complete part — the shape these tests were
    /// written against, now spelled through the open/close pair.
    fn one_part(schema: &TableSchema, batch: &RecordBatch) -> Result<Vec<u8>, DestinationError> {
        Ok(OpenPart::begin(schema, batch)?.finish()?.0)
    }

    fn utf8_schema(names: &[&str]) -> TableSchema {
        TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: names
                .iter()
                .map(|name| ColumnDef {
                    name: (*name).to_owned(),
                    column_type: ColumnType::scalar(LogicalType::Utf8),
                    nullable: true,
                    provenance: Provenance::Inferred,
                })
                .collect(),
        }
    }

    /// Both escapes, including the injection shape and the trailing
    /// backslash that would otherwise eat the closing quote.
    #[test]
    fn quotes_and_backslashes_are_both_escaped() {
        assert_eq!(sql_literal_body("o'brien"), "o''brien");
        assert_eq!(sql_literal_body("back\\slash"), "back\\\\slash");
        assert_eq!(sql_literal_body("trailing\\"), "trailing\\\\");
        assert_eq!(
            sql_literal_body("'; DROP TABLE t; --"),
            "''; DROP TABLE t; --"
        );
    }

    /// Name-resolved projection: a batch in a different column order
    /// still lands correctly, written in the SCHEMA's order.
    #[test]
    fn a_reordered_batch_is_projected_into_schema_order() {
        let arrow = Arc::new(ArrowSchema::new(vec![
            Field::new("note", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .expect("batch");
        let part = one_part(&utf8_schema(&["id", "note"]), &batch).expect("part");
        assert_eq!(&part[..4], b"PAR1", "parquet magic first");
        let footer = String::from_utf8_lossy(&part);
        let (id_at, note_at) = (
            footer.find("id").expect("id present"),
            footer.find("note").expect("note present"),
        );
        assert!(id_at < note_at, "schema order, not batch order");
    }

    /// A batch missing an ensured column refuses, naming the column.
    #[test]
    fn a_batch_missing_an_ensured_column_is_a_named_error() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(vec![1]))]).expect("batch");
        let err = one_part(&utf8_schema(&["id", "note"]), &batch).expect_err("missing is refused");
        assert!(format!("{err}").contains("note"), "{err}");
    }

    /// Non-finite floats travel — a 023-pinned CHANGE from the
    /// statement-text era, whose refusal was an artefact of the old
    /// transport, not a rule about data.
    #[test]
    fn non_finite_floats_encode_rather_than_refuse() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ]))],
        )
        .expect("batch");
        let part = one_part(&utf8_schema(&["amount"]), &batch).expect("encodes");
        assert_eq!(&part[..4], b"PAR1");
    }

    /// A part is a complete parquet file, magic at both ends.
    #[test]
    fn a_part_is_a_complete_parquet_file() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![Arc::new(Int64Array::from((0..1_000).collect::<Vec<_>>()))],
        )
        .expect("batch");
        let part = one_part(&utf8_schema(&["id"]), &batch).expect("part");
        assert_eq!(&part[..4], b"PAR1");
        assert_eq!(&part[part.len() - 4..], b"PAR1");
    }

    /// 034: a part spans MANY batches, and the rowcount it reports —
    /// what the COPY check is measured against — counts all of them.
    #[test]
    fn a_part_spans_many_batches_and_counts_every_row() {
        let schema = utf8_schema(&["id"]);
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![Arc::new(Int64Array::from((0..250).collect::<Vec<_>>()))],
        )
        .expect("batch");

        let mut part = OpenPart::begin(&schema, &batch).expect("begins");
        assert_eq!(part.rows(), 250);
        for _ in 0..3 {
            part.append(&schema, &batch).expect("appends");
        }
        assert_eq!(part.rows(), 1_000);
        assert!(part.encoded_len() > 0, "an open part reports its size");

        let (bytes, rows) = part.finish().expect("finishes");
        assert_eq!(rows, 1_000);
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    }

    /// 034: an ADDED column changes the file's shape, so the open part
    /// must close rather than widen — a parquet file holds one schema.
    /// A batch missing a column is NOT a shape change: that is an
    /// error, and it must reach `append` so the message names it.
    #[test]
    fn a_widened_batch_changes_the_shape_but_a_missing_column_does_not() {
        let narrow = utf8_schema(&["id"]);
        let arrow = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .expect("batch");

        let part = OpenPart::begin(&narrow, &batch).expect("begins");
        assert!(!part.shape_differs(&narrow, &batch), "same projection");
        let widened = utf8_schema(&["id", "note"]);
        assert!(part.shape_differs(&widened, &batch), "an added column");

        let absent = utf8_schema(&["id", "missing"]);
        assert!(
            !part.shape_differs(&absent, &batch),
            "a projection failure is not a shape change"
        );
        let err = OpenPart::begin(&absent, &batch).expect_err("refused");
        assert!(format!("{err}").contains("missing"), "{err}");
    }
}
