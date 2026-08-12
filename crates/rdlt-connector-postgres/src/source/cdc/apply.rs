//! The per-stream apply state machine: pgoutput change messages buffered by
//! transaction and flushed into change batches with commit-boundary
//! checkpoint discipline. Change batches are assembled through the SAME
//! [`crate::types::builder::ColumnBuilder`] the binary decoder feeds, so a
//! CDC value and a snapshot value land in identical Arrow shapes by
//! construction.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector_sdk::spi::SourceError;

use crate::source::config::CdcConfig;
use crate::source::errors::{self, Phase};
use crate::types::Column;
use crate::types::builder::ColumnBuilder;

use super::pgoutput::{self, Message, TupleData, TupleValue};
use super::read::StreamContext;
use super::runtime::Identity;

/// One cell of one change row: SQL NULL or the text form. (Unchanged-TOAST
/// markers are resolved BEFORE rows reach batch assembly — substitution or
/// a typed error.)
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Cell {
    Null,
    Text(String),
}

pub(super) enum Emit {
    Batch(RecordBatch),
    Checkpoint(u64),
}

/// Build one Arrow batch for `columns` + the trailing deletion-flag column.
/// `rows` are change rows (each exactly `columns.len()` cells); `deleted`
/// marks delete rows (flag TRUE; NULL otherwise). Every field is nullable:
/// delete rows carry NULL in non-key columns by design.
pub(super) fn batch_of(
    columns: &[Column],
    flag_column: &str,
    rows: &[Vec<Cell>],
    deleted: &[bool],
) -> Result<RecordBatch, String> {
    debug_assert_eq!(rows.len(), deleted.len());
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len() + 1);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len() + 1);
    for (index, column) in columns.iter().enumerate() {
        let mut builder = ColumnBuilder::new(&column.kind)
            .map_err(|detail| format!("column `{}`: {detail}", column.name))?;
        for row in rows {
            match &row[index] {
                Cell::Null => builder.append_null(),
                Cell::Text(text) => builder
                    .append_text(text)
                    .map_err(|detail| format!("column `{}`: {detail}", column.name))?,
            }
        }
        let array = builder.finish();
        fields.push(Field::new(&column.name, array.data_type().clone(), true));
        arrays.push(array);
    }
    let mut flag = arrow_array::builder::BooleanBuilder::with_capacity(rows.len());
    for &is_delete in deleted {
        if is_delete {
            flag.append_value(true);
        } else {
            flag.append_null();
        }
    }
    fields.push(Field::new(flag_column, DataType::Boolean, true));
    arrays.push(Arc::new(flag.finish()));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("assembling change batch: {e}"))
}

/// The per-stream apply state machine: relation tracking, transaction
/// buffering, commit-boundary checkpoint discipline.
pub(super) struct Apply<'a> {
    cdc: &'a CdcConfig,
    schema: &'a str,
    stream: &'a str,
    identity: &'a Identity,
    columns: &'a [Column],
    batch_max_rows: usize,
    since: u64,
    /// relation id → plan-column → relation-column index (None = not ours).
    relation_maps: HashMap<u32, Option<Vec<usize>>>,
    /// Column indices of the merge-key columns.
    key_indices: Vec<usize>,
    /// Rows of the transaction currently being decoded.
    transaction_rows: Vec<(Vec<Cell>, bool)>,
    /// Rows of committed transactions not yet pushed.
    ready_rows: Vec<(Vec<Cell>, bool)>,
    /// Commit position covering every row in `ready_rows` (and everything
    /// pushed before) — the only value checkpoints may carry.
    last_commit: Option<u64>,
    /// First unappliable record of the CURRENT transaction (unchanged TOAST
    /// without an image, TRUNCATE, keyless delete, drift). Raised at the
    /// COMMIT boundary — and only when the transaction is not already
    /// applied (commit ≤ cursor). Raising eagerly would make such records
    /// replay-fatal forever: the whole point of the fresh-snapshot recovery
    /// is that a new snapshot's cursor moves PAST them.
    transaction_error: Option<SourceError>,
}

impl<'a> Apply<'a> {
    /// Refuses a replica-identity key column that is missing from the
    /// decode plan: silently skipping one would leave `key_cells` short —
    /// or, worst case, EMPTY, where the keyless-delete guard is vacuously
    /// satisfied and a delete keyed on nothing slips through. The stream
    /// planner already enforces this; repeating it here keeps the guard
    /// non-vacuous even if a caller reaches the apply machinery directly.
    pub(super) fn new(
        context: &StreamContext<'a>,
        stream: &'a str,
        since: u64,
    ) -> Result<Self, SourceError> {
        let key_indices = context
            .identity
            .key
            .iter()
            .map(|key| {
                context
                    .columns
                    .iter()
                    .position(|column| &column.name == key)
                    .ok_or_else(|| {
                        errors::fatal(
                            Phase::Slot,
                            Some(stream),
                            format!(
                                "replica-identity key column `{key}` is missing from the                                  decode plan — every key column must survive the column                                  selection"
                            ),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            cdc: context.cdc,
            schema: context.config.schema.as_str(),
            stream,
            identity: context.identity,
            columns: context.columns,
            batch_max_rows: context.config.batch_max_rows,
            since,
            relation_maps: HashMap::new(),
            key_indices,
            transaction_rows: Vec::new(),
            ready_rows: Vec::new(),
            last_commit: None,
            transaction_error: None,
        })
    }

    fn fatal(&self, detail: impl std::fmt::Display) -> SourceError {
        errors::fatal(Phase::Decode, Some(self.stream), detail)
    }

    /// Record the current transaction's first unappliable record; decided
    /// at the commit boundary (see `transaction_error`).
    fn defer(&mut self, error: SourceError) {
        if self.transaction_error.is_none() {
            self.transaction_error = Some(error);
        }
    }

    pub(super) fn on_message(
        &mut self,
        lsn: u64,
        message: Message,
    ) -> Result<Vec<Emit>, SourceError> {
        match message {
            Message::Begin => {
                self.transaction_rows.clear();
                Ok(Vec::new())
            }
            Message::Relation(relation) => {
                let map = if relation.namespace == self.schema && relation.name == self.stream {
                    Some(self.plan_map(&relation)?)
                } else {
                    None
                };
                self.relation_maps.insert(relation.id, map);
                Ok(Vec::new())
            }
            Message::Insert { relation, new } => {
                if let Some(map) = self.our_map(relation) {
                    match self.tuple_row(&map, &new, None) {
                        Ok(row) => self.transaction_rows.push((row, false)),
                        Err(e) => self.defer(e),
                    }
                }
                Ok(Vec::new())
            }
            Message::Update { relation, old, new } => {
                if let Some(map) = self.our_map(relation) {
                    // Key-changing update: delete(old key) then insert(new),
                    // in order, same transaction.
                    let built = (|| {
                        let mut rows = Vec::new();
                        let old_key = old
                            .as_ref()
                            .map(|old_tuple| self.key_cells(&map, old_tuple))
                            .transpose()?;
                        let new_row = self.tuple_row(&map, &new, old.as_ref())?;
                        let new_key: Vec<&Cell> = self
                            .key_indices
                            .iter()
                            .map(|&index| &new_row[index])
                            .collect();
                        if let Some(old_key) = old_key {
                            let changed = old_key
                                .iter()
                                .zip(&new_key)
                                .any(|(old, new)| !matches!(old, Cell::Null) && &old != new);
                            if changed {
                                rows.push((self.delete_row(old_key), true));
                            }
                        }
                        rows.push((new_row, false));
                        Ok::<_, SourceError>(rows)
                    })();
                    match built {
                        Ok(rows) => self.transaction_rows.extend(rows),
                        Err(e) => self.defer(e),
                    }
                }
                Ok(Vec::new())
            }
            Message::Delete { relation, old } => {
                if let Some(map) = self.our_map(relation) {
                    match self.key_cells(&map, &old) {
                        Ok(key) if key.iter().any(|cell| matches!(cell, Cell::Null)) => {
                            self.defer(self.fatal(
                                "delete record carries no usable key data — the \
                                 table's replica identity was weakened \
                                 mid-stream; restore it",
                            ));
                        }
                        Ok(key) => self.transaction_rows.push((self.delete_row(key), true)),
                        Err(e) => self.defer(e),
                    }
                }
                Ok(Vec::new())
            }
            Message::Truncate { relations } => {
                if relations
                    .iter()
                    .any(|id| matches!(self.relation_maps.get(id), Some(Some(_))))
                {
                    self.defer(self.fatal(
                        "TRUNCATE arrived on this table — truncation does not \
                         replicate as row deletes; reset the stream's pipeline \
                         state AND re-initialize the destination table (a fresh \
                         snapshot starts PAST the truncation but cannot remove \
                         rows the truncation deleted), or stop truncating \
                         published tables",
                    ));
                }
                Ok(Vec::new())
            }
            Message::Commit => {
                // Already-applied transaction (commit position ≤ cursor):
                // discard rows AND any unappliable-record error — replaying
                // past an applied fault must not re-raise it. Otherwise the
                // fault (if any) surfaces HERE, at its commit position.
                let rows = std::mem::take(&mut self.transaction_rows);
                let error = self.transaction_error.take();
                if lsn <= self.since {
                    return Ok(Vec::new());
                }
                if let Some(error) = error {
                    return Err(error);
                }
                self.ready_rows.extend(rows);
                self.last_commit = Some(lsn);
                if self.ready_rows.len() >= self.batch_max_rows {
                    return self.flush(true);
                }
                Ok(Vec::new())
            }
            Message::Origin | Message::Type => Ok(Vec::new()),
        }
    }

    /// End of the peeked range: flush the remainder and checkpoint at the
    /// run target (every commit ≤ target is applied for this table). A
    /// transaction left open at range end never saw its commit — dropping
    /// it is the whole-transaction discipline (the next pass replays it).
    pub(super) fn finish(&mut self, target: u64) -> Result<Vec<Emit>, SourceError> {
        self.transaction_rows.clear();
        self.transaction_error = None;
        let mut emits = self.flush(false)?;
        emits.push(Emit::Checkpoint(target.max(self.since)));
        Ok(emits)
    }

    fn flush(&mut self, emit_checkpoint: bool) -> Result<Vec<Emit>, SourceError> {
        if self.ready_rows.is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<Vec<Cell>> = self.ready_rows.iter().map(|(row, _)| row.clone()).collect();
        let deleted: Vec<bool> = self
            .ready_rows
            .iter()
            .map(|(_, deleted)| *deleted)
            .collect();
        self.ready_rows.clear();
        let batch = batch_of(self.columns, &self.cdc.flag_column, &rows, &deleted)
            .map_err(|e| self.fatal(e))?;
        let mut emits = vec![Emit::Batch(batch)];
        if emit_checkpoint && let Some(commit) = self.last_commit {
            emits.push(Emit::Checkpoint(commit));
        }
        Ok(emits)
    }

    fn our_map(&self, relation: u32) -> Option<Vec<usize>> {
        self.relation_maps
            .get(&relation)
            .and_then(|map| map.clone())
    }

    /// plan column index → relation column index, by name. A reflected
    /// column missing from the relation = non-additive drift = typed error;
    /// EXTRA relation columns (added after this run's reflection) are
    /// deferred to the next run's reflection (additive drift applies at run
    /// boundaries).
    fn plan_map(&self, relation: &pgoutput::Relation) -> Result<Vec<usize>, SourceError> {
        self.columns
            .iter()
            .map(|column| {
                relation
                    .columns
                    .iter()
                    .position(|relation_column| relation_column.name == column.name)
                    .ok_or_else(|| {
                        self.fatal(format!(
                            "column `{}` vanished from the replicated table \
                             (non-additive schema drift)",
                            column.name
                        ))
                    })
            })
            .collect()
    }

    /// A full row from a tuple: plan-ordered cells; unchanged-TOAST markers
    /// substitute from the old image when the replica identity covers all
    /// columns, else are a typed error naming table + column.
    fn tuple_row(
        &self,
        map: &[usize],
        tuple: &TupleData,
        old: Option<&TupleData>,
    ) -> Result<Vec<Cell>, SourceError> {
        map.iter()
            .zip(self.columns)
            .map(|(&relation_index, column)| {
                let value = tuple
                    .values
                    .get(relation_index)
                    .ok_or_else(|| self.fatal(format!("tuple lacks column `{}`", column.name)))?;
                match value {
                    TupleValue::Null => Ok(Cell::Null),
                    TupleValue::Text(bytes) => self.text_cell(bytes, &column.name),
                    TupleValue::UnchangedToast => {
                        let substitute =
                            old.and_then(|old_tuple| old_tuple.values.get(relation_index));
                        match substitute {
                            Some(TupleValue::Text(bytes)) if self.identity.covers_all_columns => {
                                self.text_cell(bytes, &column.name)
                            }
                            _ => Err(self.fatal(format!(
                                "unchanged TOAST value in column `{}` and no old \
                                 image to substitute from — `ALTER TABLE {}.{} \
                                 REPLICA IDENTITY FULL` to retain TOAST values",
                                column.name, self.schema, self.stream
                            ))),
                        }
                    }
                }
            })
            .collect()
    }

    fn text_cell(&self, bytes: &[u8], column: &str) -> Result<Cell, SourceError> {
        std::str::from_utf8(bytes)
            .map(|text| Cell::Text(text.to_owned()))
            .map_err(|e| self.fatal(format!("column `{column}`: tuple text is not UTF-8: {e}")))
    }

    /// The key cells of a tuple (identity/old tuples), key-ordered.
    fn key_cells(&self, map: &[usize], tuple: &TupleData) -> Result<Vec<Cell>, SourceError> {
        self.key_indices
            .iter()
            .map(|&column_index| {
                let relation_index = map[column_index];
                match tuple.values.get(relation_index) {
                    None | Some(TupleValue::Null) => Ok(Cell::Null),
                    Some(TupleValue::Text(bytes)) => {
                        self.text_cell(bytes, &self.columns[column_index].name)
                    }
                    Some(TupleValue::UnchangedToast) => Err(self.fatal(
                        "key column arrived as an unchanged-TOAST marker — \
                         unusable key data",
                    )),
                }
            })
            .collect()
    }

    /// A delete row: key cells in place, every other column NULL, flag
    /// TRUE.
    fn delete_row(&self, key: Vec<Cell>) -> Vec<Cell> {
        let mut row = vec![Cell::Null; self.columns.len()];
        for (&column_index, cell) in self.key_indices.iter().zip(key) {
            row[column_index] = cell;
        }
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Kind;
    use arrow_array::Array;

    fn column(name: &str, kind: Kind) -> Column {
        Column {
            name: name.into(),
            kind,
            not_null: false,
        }
    }

    #[test]
    fn text_forms_assemble_into_the_decoder_shapes() {
        let columns = vec![
            column("i", Kind::Int64),
            column("f", Kind::Float64),
            column(
                "n",
                Kind::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
            column("s", Kind::Text),
            column("b", Kind::Bool),
            column("y", Kind::Bytea),
            column("ts", Kind::TimestampTz),
            column("d", Kind::Date),
            column("t", Kind::Time),
        ];
        let row = vec![
            Cell::Text("42".into()),
            Cell::Text("-Infinity".into()),
            Cell::Text("-12345.67".into()),
            Cell::Text("héllo".into()),
            Cell::Text("t".into()),
            Cell::Text("\\x0aff".into()),
            Cell::Text("2026-07-21 10:11:12.123456+00".into()),
            Cell::Text("2026-07-21".into()),
            Cell::Text("10:11:12.123456".into()),
        ];
        let nulls: Vec<Cell> = std::iter::repeat_n(Cell::Null, columns.len()).collect();
        let batch = batch_of(&columns, "_rdlt_deleted", &[row, nulls], &[false, true])
            .expect("batch assembles");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), columns.len() + 1);
        let integers = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(integers.value(0), 42);
        assert!(integers.is_null(1));
        let floats = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert_eq!(floats.value(0), f64::NEG_INFINITY);
        let numerics = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(numerics.value(0), -1_234_567);
        let binaries = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(binaries.value(0), &[0x0a, 0xff]);
        let flags = batch
            .column(columns.len())
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        assert!(flags.is_null(0), "insert/update rows carry a NULL flag");
        assert!(flags.value(1), "delete rows carry TRUE");
    }

    #[test]
    fn malformed_text_is_a_typed_error_naming_the_column() {
        let columns = vec![column("qty", Kind::Int64)];
        let error = batch_of(
            &columns,
            "_rdlt_deleted",
            &[vec![Cell::Text("not-a-number".into())]],
            &[false],
        )
        .expect_err("typed");
        assert!(error.contains("`qty`"), "{error}");
        // Excess numeric precision and NaN are refused, not rounded.
        let columns = vec![column(
            "n",
            Kind::Decimal {
                precision: 10,
                scale: 2,
            },
        )];
        for bad in ["1.234", "NaN"] {
            let error = batch_of(
                &columns,
                "_rdlt_deleted",
                &[vec![Cell::Text(bad.into())]],
                &[false],
            )
            .expect_err(bad);
            assert!(error.contains("`n`"), "{error}");
        }
    }
}
