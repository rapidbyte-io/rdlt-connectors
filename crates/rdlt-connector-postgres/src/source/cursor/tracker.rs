//! Per-stream incremental tracking: boundary dedup on resume, watermark
//! tracking for mid-stream checkpoints, and the final boundary-key run.
//! Depends on cursor-ordered streams (NULLS FIRST, direction-aligned) — the
//! SQL ORDER BY guarantees it.

use rdlt_connector_sdk::spi::error::SourceError;

use super::state::{State, scalar_at};
use crate::source::errors::{self, Phase};
use crate::types::{Kind, Scalar};

/// Everything a tracker needs, named — the construction site reads as a
/// record, not a positional list.
pub(crate) struct Spec {
    /// Index of the cursor column in the decoded batch.
    pub(crate) cursor_index: usize,
    pub(crate) kind: Kind,
    pub(crate) direction_max: bool,
    /// Stored state we resumed from (monotonic guard + dedup source).
    pub(crate) stored: Option<State>,
    /// Column indices forming the row key; None ⇒ hash every column.
    pub(crate) key_columns: Option<Vec<usize>>,
    /// `NullPolicy::Error` context: (stream, column) when configured — a
    /// NULL cursor value fails `process` with a typed error naming both.
    pub(crate) null_error: Option<(String, String)>,
    /// Stream name and cursor column, named in the typed error raised when
    /// the stream breaks its cursor ordering or a row key cannot be
    /// rendered (always present — distinct from the policy-gated
    /// `null_error`).
    pub(crate) stream: String,
    pub(crate) cursor_column: String,
}

pub(crate) struct Tracker {
    spec: Spec,
    /// True until a row strictly beyond the stored watermark is seen.
    in_boundary: bool,
    last_value: Option<Scalar>,
    /// Keys of rows sharing `last_value` (the current boundary run). Each
    /// key is a component list, not a joined string (cursor format v2, 037
    /// US4).
    run_keys: Vec<Vec<Option<String>>>,
    /// (watermark, key count) at the last emitted checkpoint — dedup of
    /// identical consecutive checkpoints.
    emitted: Option<(Scalar, usize)>,
    pub(crate) deduped_rows: u64,
}

impl Tracker {
    pub(crate) fn new(spec: Spec) -> Self {
        Self {
            spec,
            in_boundary: true,
            last_value: None,
            run_keys: Vec::new(),
            emitted: None,
            deduped_rows: 0,
        }
    }

    fn is_beyond(&self, candidate: &Scalar, reference: &Scalar) -> bool {
        if self.spec.direction_max {
            candidate > reference
        } else {
            candidate < reference
        }
    }

    /// Render this row's key as an ordered COMPONENT LIST — one entry per
    /// key column, `None` for SQL NULL — rather than a joined string (cursor
    /// format v2, 037 US4). Generation 1 joined parts with `|` and a `∅`
    /// NULL sentinel, neither escaped: a composite key whose rendered values
    /// themselves contained those characters could collide two distinct
    /// rows AT THE SAME WATERMARK and drop one as a duplicate on resume
    /// (`separator_shaped_values_no_longer_collide` below is the red-proof).
    /// A component list has no such collapsing step, so it cannot alias.
    ///
    /// The PK-less arm renders a single-element list `[Some(<blake3 hex>)]`
    /// instead of a component list: unambiguous against the PK arm because
    /// a table's `key_columns` is either `Some` or `None` for its entire
    /// cursor — the two arms never coexist on one persisted state.
    fn row_key(
        &self,
        batch: &arrow_array::RecordBatch,
        row: usize,
    ) -> Result<Vec<Option<String>>, SourceError> {
        use arrow_array::Array;
        match &self.spec.key_columns {
            Some(indices) => {
                let mut parts = Vec::with_capacity(indices.len());
                for &index in indices {
                    let column = batch.column(index);
                    if column.is_null(row) {
                        parts.push(None);
                    } else {
                        parts.push(Some(self.render_cell(column, row)?));
                    }
                }
                Ok(parts)
            }
            None => {
                // Key-less table: hash the whole row's canonical rendering.
                let mut hasher = blake3::Hasher::new();
                for column in batch.columns() {
                    if column.is_null(row) {
                        hasher.update(b"\xff\x00");
                    } else {
                        hasher.update(self.render_cell(column, row)?.as_bytes());
                        hasher.update(b"\x00");
                    }
                }
                Ok(vec![Some(hasher.finalize().to_hex().to_string())])
            }
        }
    }

    /// Render one non-NULL cell to its canonical resume-key text. A
    /// rendering failure is Fatal: defaulting it to an empty component would
    /// silently collide distinct rows and corrupt boundary dedup on resume.
    ///
    /// Zoned timestamps are re-labeled naive first: arrow's display needs a
    /// timezone database for NAMED zones (`"UTC"`), and without it every
    /// timestamptz cursor key would fail to render. The stored instant is
    /// unchanged (UTC micros), so the naive rendering is deterministic and
    /// collision-free — exactly what a resume key needs.
    fn render_cell(
        &self,
        column: &arrow_array::ArrayRef,
        row: usize,
    ) -> Result<String, SourceError> {
        use arrow_schema::DataType;
        let relabeled;
        let column = match column.data_type() {
            DataType::Timestamp(unit, Some(_)) => {
                relabeled =
                    arrow_cast::cast(column, &DataType::Timestamp(*unit, None)).map_err(|e| {
                        errors::fatal(
                            Phase::Copy,
                            Some(&self.spec.stream),
                            format!(
                                "row key for cursor column `{}`: re-labeling zoned \
                                 timestamp failed: {e}",
                                self.spec.cursor_column
                            ),
                        )
                    })?;
                &relabeled
            }
            _ => column,
        };
        // Per row, and `array_value_to_string` rebuilds an `ArrayFormatter`
        // on every call. It is NOT hoisted here, because the array being
        // formatted VARIES by row: the timestamp arm above formats a per-row
        // re-labeled temporary rather than `column`, so one formatter cannot
        // serve the loop without restructuring that normalisation first.
        // This path is incremental-cursor only and no bench cell exercises
        // it.
        arrow_cast::display::array_value_to_string(column, row).map_err(|e| {
            errors::fatal(
                Phase::Copy,
                Some(&self.spec.stream),
                format!(
                    "row key for cursor column `{}` could not be rendered to text: {e}",
                    self.spec.cursor_column
                ),
            )
        })
    }

    /// Process one decoded batch BEFORE pushing: dedup re-fetched boundary
    /// rows and update tracking. Returns the (possibly filtered) batch —
    /// `None` when every row was a duplicate — plus an intermediate
    /// checkpoint to emit AFTER the batch is pushed (rows covered by it are
    /// then all pushed).
    pub(crate) fn process(
        &mut self,
        batch: arrow_array::RecordBatch,
    ) -> Result<(Option<arrow_array::RecordBatch>, Option<State>), SourceError> {
        use arrow_array::Array;
        use arrow_array::builder::BooleanBuilder;

        let cursor_column = batch.column(self.spec.cursor_index).clone();
        // NullPolicy::Error: zero cost when clean — one null_count() check
        // per batch, typed Fatal on the first violation.
        if let Some((stream, column)) = &self.spec.null_error
            && cursor_column.null_count() > 0
        {
            return Err(errors::fatal(
                Phase::Copy,
                Some(stream),
                format!(
                    "cursor column `{column}` contains a NULL value and \
                     `nulls: error` is configured — NULL cursors are a data-contract \
                     violation under this policy (exclude/include accept them)"
                ),
            ));
        }
        let mut keep = BooleanBuilder::with_capacity(batch.num_rows());
        let mut any_dropped = false;
        let mut kept_rows: Vec<usize> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let value = scalar_at(&cursor_column, self.spec.kind, row);
            let mut drop = false;
            if self.in_boundary
                && let (Some(value), Some(stored)) = (&value, &self.spec.stored)
            {
                if self.is_beyond(value, &stored.watermark) {
                    self.in_boundary = false;
                } else if *value == stored.watermark
                    && stored.boundary_keys.contains(&self.row_key(&batch, row)?)
                {
                    drop = true;
                }
            }
            keep.append_value(!drop);
            if drop {
                self.deduped_rows += 1;
                any_dropped = true;
            } else {
                kept_rows.push(row);
            }
        }
        // Track kept rows only (dropped rows belong to the PRIOR run).
        for &row in &kept_rows {
            let Some(value) = scalar_at(&cursor_column, self.spec.kind, row) else {
                continue; // NULL cursors are re-fetched every run, never tracked
            };
            match &self.last_value {
                Some(last) if *last == value => {
                    let key = self.row_key(&batch, row)?;
                    self.run_keys.push(key);
                }
                Some(last) => {
                    if !self.is_beyond(&value, last) {
                        return Err(errors::fatal(
                            Phase::Copy,
                            Some(&self.spec.stream),
                            format!(
                                "cursor column `{}` produced an out-of-order row: the read \
                                 stream broke the ordering its resume watermark depends on",
                                self.spec.cursor_column
                            ),
                        ));
                    }
                    self.last_value = Some(value);
                    self.run_keys = vec![self.row_key(&batch, row)?];
                }
                None => {
                    self.last_value = Some(value);
                    self.run_keys = vec![self.row_key(&batch, row)?];
                }
            }
        }
        let surviving = if any_dropped {
            let mask = keep.finish();
            let filtered = arrow_select::filter::filter_record_batch(&batch, &mask)
                .expect("filter mask length matches");
            (filtered.num_rows() > 0).then_some(filtered)
        } else {
            Some(batch)
        };
        // Intermediate checkpoint after EVERY batch: watermark = last value
        // seen, keys = its boundary run. This COVERS every pushed row: an
        // engine commit at this checkpoint can resume `>= watermark` with
        // key dedup and neither lose nor double-apply the boundary run. (A
        // previous-distinct-value design under-covered pushed rows at the
        // current value — caught by the armed crash sweep as a duplicate.)
        let checkpoint = self.current_state().filter(|state| {
            let signature = (state.watermark.clone(), state.boundary_keys.len());
            if self.emitted.as_ref() == Some(&signature) {
                false
            } else {
                self.emitted = Some(signature);
                true
            }
        });
        Ok((surviving, checkpoint))
    }

    /// The current resume-safe state: watermark = last value seen, keys =
    /// its boundary run (merged with the stored keys when the watermark has
    /// not advanced past an equal stored one). `None` when nothing advanced
    /// (regressing-clock guard: the watermark NEVER moves backward).
    fn current_state(&self) -> Option<State> {
        let last = self.last_value.as_ref()?;
        let mut boundary_keys = self.run_keys.clone();
        if let Some(stored) = &self.spec.stored {
            if !self.is_beyond(last, &stored.watermark) && *last != stored.watermark {
                return None; // regressed — keep the stored watermark
            }
            if *last == stored.watermark {
                if self.run_keys.is_empty() {
                    return None; // nothing new at the boundary
                }
                // New rows at an unchanged watermark merge with its old keys
                // (those remain committed and deduped-against).
                boundary_keys.extend(stored.boundary_keys.iter().cloned());
                boundary_keys.sort();
                boundary_keys.dedup();
            }
        }
        Some(State {
            watermark: last.clone(),
            boundary_keys,
        })
    }

    /// The final checkpoint at stream end. Under `boundary: exclusive` the
    /// keys are stripped (strictly-monotonic cursors resume `>` across
    /// runs); intermediate checkpoints ALWAYS keep keys — in-run resume
    /// correctness is not a user-visible semantic choice.
    pub(crate) fn final_state(&self, keep_keys: bool) -> Option<State> {
        let mut state = self.current_state()?;
        if !keep_keys {
            state.boundary_keys = Vec::new();
        }
        Some(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(ids: &[Option<i64>], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .expect("batch builds")
    }

    fn spec(stored: Option<State>) -> Spec {
        Spec {
            cursor_index: 0,
            kind: Kind::Int64,
            direction_max: true,
            stored,
            key_columns: Some(vec![0]),
            null_error: None,
            stream: "t".into(),
            cursor_column: "id".into(),
        }
    }

    /// Render a single row's key over an all-text, all-key-column batch —
    /// the seam `separator_shaped_values_no_longer_collide` uses to compare
    /// how two different column splits of the same characters render.
    fn render_key(values: &[&str]) -> Vec<Option<String>> {
        let fields: Vec<Field> = (0..values.len())
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, false))
            .collect();
        let columns: Vec<arrow_array::ArrayRef> = values
            .iter()
            .map(|v| Arc::new(StringArray::from(vec![*v])) as arrow_array::ArrayRef)
            .collect();
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch builds");
        let mut key_spec = spec(None);
        key_spec.key_columns = Some((0..values.len()).collect());
        let tracker = Tracker::new(key_spec);
        tracker.row_key(&batch, 0).expect("row key renders")
    }

    #[test]
    fn separator_shaped_values_no_longer_collide() {
        // v1 joined ("a|b", "c") and ("a", "b|c") to the SAME "a|b|c" and
        // dropped one as a duplicate on resume. v2 keys are component
        // lists, so the two renderings differ.
        let k1 = render_key(&["a|b", "c"]);
        let k2 = render_key(&["a", "b|c"]);
        assert_ne!(k1, k2, "components are no longer flattened into one string");
    }

    #[test]
    fn boundary_rows_dedup_and_watermark_advances() {
        let stored = State {
            watermark: Scalar::Int(5),
            boundary_keys: vec![vec![Some("5".into())]],
        };
        let mut tracker = Tracker::new(spec(Some(stored)));
        // Row 5 was delivered last run (its key is stored) — dropped; 6 and
        // 7 are new.
        let (surviving, checkpoint) = tracker
            .process(batch(&[Some(5), Some(6), Some(7)], &["a", "b", "c"]))
            .expect("process");
        assert_eq!(surviving.expect("rows survive").num_rows(), 2);
        assert_eq!(tracker.deduped_rows, 1);
        let checkpoint = checkpoint.expect("checkpoint");
        assert_eq!(checkpoint.watermark, Scalar::Int(7));
        assert_eq!(checkpoint.boundary_keys, [vec![Some("7".to_string())]]);
        // Final state under a closed boundary keeps the keys.
        assert_eq!(
            tracker.final_state(true).expect("state").boundary_keys,
            [vec![Some("7".to_string())]]
        );
        // …and under an open boundary strips them.
        assert!(
            tracker
                .final_state(false)
                .expect("state")
                .boundary_keys
                .is_empty()
        );
    }

    #[test]
    fn out_of_order_stream_is_a_typed_error() {
        let mut tracker = Tracker::new(spec(None));
        let error = tracker
            .process(batch(&[Some(7), Some(5)], &["a", "b"]))
            .expect_err("out of order");
        assert!(error.to_string().contains("out-of-order"), "{error}");
    }

    #[test]
    fn regressed_watermark_never_checkpoints() {
        let stored = State {
            watermark: Scalar::Int(10),
            boundary_keys: vec![],
        };
        let mut tracker = Tracker::new(spec(Some(stored)));
        // Every row is below the stored watermark (an exclusive-boundary
        // stream re-showing old rows): no dedup keys stored, nothing beyond.
        let (_, checkpoint) = tracker
            .process(batch(&[Some(3), Some(4)], &["a", "b"]))
            .expect("process");
        assert!(checkpoint.is_none(), "watermark must never regress");
        assert!(tracker.final_state(true).is_none());
    }

    #[test]
    fn null_policy_error_raises_on_first_null() {
        let mut with_error = spec(None);
        with_error.null_error = Some(("t".into(), "id".into()));
        let mut tracker = Tracker::new(with_error);
        let error = tracker
            .process(batch(&[Some(1), None], &["a", "b"]))
            .expect_err("null under error policy");
        assert!(error.to_string().contains("data-contract"), "{error}");
    }

    #[test]
    fn identical_consecutive_checkpoints_deduplicate() {
        let mut tracker = Tracker::new(spec(None));
        let (_, first) = tracker.process(batch(&[Some(5)], &["a"])).expect("process");
        assert!(first.is_some());
        // A batch of only-NULL cursors changes nothing — no repeat
        // checkpoint.
        let mut nullable = spec(None);
        nullable.null_error = None;
        let (_, second) = tracker.process(batch(&[None], &["b"])).expect("process");
        assert!(second.is_none(), "unchanged state re-emitted");
    }
}
