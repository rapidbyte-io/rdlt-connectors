//! CSV rides the RECORD path: every row is rewritten as NDJSON and
//! flows through primary_key, dedup, merge, and drift exactly as
//! jsonl does.
//!
//! Files are whole-file units (a quoted newline makes byte offsets
//! meaningless for resume), read TWICE: a survey pass that joins each
//! column's observed value shape over the entire file, then a
//! conversion pass driven by the surveyed shapes. Declared hints beat
//! the survey, and a cell that cannot satisfy a DECLARED hint fails
//! typed with file, row, and column. A pass-2 cell that contradicts
//! pass 1 is typed too — a file that changed between the passes must
//! stop the run, never be coerced.

use std::collections::BTreeMap;

use bytes::Bytes;
use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::error::SourceError;

use super::open_decoded;
use crate::format::{Codec, SLAB_BYTES, codec_of};
use crate::source::config::{CsvOptions, HintType};
use crate::source::cursor::{FileCursor, FileProgress, FileTask};

/// What a column's values have looked like so far. `Unseen` is the
/// lattice bottom (a column with no values lands as Text); Int widens
/// to Float; Bool sits OUTSIDE the numeric chain, so mixing it with
/// anything — like mixing anything with text — joins to Text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueShape {
    Unseen,
    Bool,
    Int,
    Float,
    Text,
}

impl ValueShape {
    /// Classify one cell. Order matters: the two bool literals win
    /// first, then the integer parse, then the float parse, and
    /// everything else is text.
    fn of(cell: &str) -> Self {
        if cell == "true" || cell == "false" {
            return Self::Bool;
        }
        if cell.parse::<i64>().is_ok() {
            return Self::Int;
        }
        if cell.parse::<f64>().is_ok() {
            return Self::Float;
        }
        Self::Text
    }

    /// The lattice join — total and commutative, so the order in
    /// which rows arrive can never change the outcome.
    fn join(self, other: Self) -> Self {
        use ValueShape::*;
        match (self, other) {
            (Unseen, shape) | (shape, Unseen) => shape,
            (a, b) if a == b => a,
            (Int, Float) | (Float, Int) => Float,
            _ => Text,
        }
    }
}

/// How one column's cells become JSON, once hints have had their say.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ColumnPlan {
    Hinted(HintType),
    Shaped(ValueShape),
}

fn open_reader(
    path: &str,
    codec: Codec,
    options: &CsvOptions,
) -> Result<csv::Reader<Box<dyn std::io::Read + Send>>, SourceError> {
    Ok(csv::ReaderBuilder::new()
        .delimiter(options.delimiter as u8)
        .quote(options.quote as u8)
        .has_headers(options.header)
        .flexible(false)
        .from_reader(open_decoded(path, codec)?))
}

/// Pull the next record, typing a parser failure with the file and
/// the 1-based row (header included) it happened on. Both passes come
/// through here, so the row context cannot drift between them — its
/// absence from the conversion pass was gen 1's §9-S7 inconsistency,
/// closed structurally in this generation.
fn advance(
    reader: &mut csv::Reader<Box<dyn std::io::Read + Send>>,
    record: &mut csv::StringRecord,
    path: &str,
    rows_seen: u64,
) -> Result<bool, SourceError> {
    reader.read_record(record).map_err(|e| {
        SourceError::fatal(format!(
            "malformed CSV in `{path}` at row {}: {e}",
            rows_seen + 1
        ))
    })
}

/// The survey pass: one full read that collects header names (or
/// synthesizes `c0..cN`) and the join of every column's shapes.
fn survey(
    read_path: &str,
    codec: Codec,
    options: &CsvOptions,
    shown_path: &str,
) -> Result<(Vec<String>, Vec<ValueShape>), SourceError> {
    let mut reader = open_reader(read_path, codec, options)?;
    let header_names: Vec<String> = if options.header {
        reader
            .headers()
            .map_err(|e| SourceError::fatal(format!("reading CSV header of `{shown_path}`: {e}")))?
            .iter()
            .map(str::to_owned)
            .collect()
    } else {
        Vec::new()
    };
    // A repeated header name is refused, never merged: rows become
    // name-keyed JSON objects, so the later column would silently
    // OVERWRITE the earlier one's value — and under a shape inferred
    // per INDEX, possibly as the wrong type (030 review).
    let mut seen = std::collections::BTreeSet::new();
    for name in &header_names {
        if !seen.insert(name.as_str()) {
            return Err(SourceError::fatal(format!(
                "duplicate CSV column `{name}` in `{shown_path}` — every header name must \
                 be unique; rename the column, or drop the duplicate"
            )));
        }
    }

    let mut shapes: Vec<ValueShape> = Vec::new();
    let mut record = csv::StringRecord::new();
    let mut row = u64::from(options.header);
    while advance(&mut reader, &mut record, shown_path, row)? {
        row += 1;
        if shapes.len() < record.len() {
            shapes.resize(record.len(), ValueShape::Unseen);
        }
        for (column, cell) in record.iter().enumerate() {
            if !cell.is_empty() {
                shapes[column] = shapes[column].join(ValueShape::of(cell));
            }
        }
    }

    let names = if options.header {
        header_names
    } else {
        (0..shapes.len()).map(|i| format!("c{i}")).collect()
    };
    Ok((names, shapes))
}

/// Resolve each column's plan: a declared hint wins outright, and a
/// column the survey never saw a value for converts as text.
fn plan_columns(
    names: &[String],
    shapes: &[ValueShape],
    hints: &BTreeMap<String, HintType>,
) -> Vec<ColumnPlan> {
    names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            if let Some(hint) = hints.get(name) {
                return ColumnPlan::Hinted(*hint);
            }
            ColumnPlan::Shaped(match shapes.get(i).copied() {
                Some(ValueShape::Unseen) | None => ValueShape::Text,
                Some(shape) => shape,
            })
        })
        .collect()
}

/// Read one CSV task: survey, then convert into NDJSON slabs, with a
/// single completion checkpoint (a mid-file crash re-delivers the
/// file; the keyed merge/dedup layer absorbs the repeat). `Ok(false)`
/// means the host cancelled.
pub(crate) async fn read_task(
    task: &FileTask,
    options: &CsvOptions,
    hints: &BTreeMap<String, HintType>,
    cursor: &mut FileCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    let codec = codec_of(&task.path);
    super::verify_local_snapshot(task)?;

    let (names, shapes) = survey(read_path, codec, options, &task.path)?;
    let plans = plan_columns(&names, &shapes, hints);

    let mut reader = open_reader(read_path, codec, options)?;
    let mut record = csv::StringRecord::new();
    let mut row = u64::from(options.header);
    let mut slab: Vec<u8> = Vec::with_capacity(SLAB_BYTES);
    while advance(&mut reader, &mut record, &task.path, row)? {
        row += 1;
        encode_row(&mut slab, &names, &plans, &record, &task.path, row)?;
        if slab.len() >= SLAB_BYTES {
            let full = std::mem::replace(&mut slab, Vec::with_capacity(SLAB_BYTES));
            if feed.raw_json(Bytes::from(full)).await.is_break() {
                return Ok(false);
            }
        }
    }
    if !slab.is_empty() && feed.raw_json(Bytes::from(slab)).await.is_break() {
        return Ok(false);
    }

    cursor.record(
        &task.path,
        FileProgress {
            done_units: task.size_units,
            size_units: task.size_units,
            ended_at_record_boundary: true,
            mtime_ms: task.mtime_ms,
            etag: task.etag.clone(),
            tail_hash: None, // whole-file units resume by re-delivery, never by tail
            row_groups_hash: None,
        },
    );
    if feed.checkpoint(cursor.encode()).await.is_break() {
        return Ok(false);
    }
    Ok(true)
}

/// Append one row to the slab as an NDJSON object. Empty cells are
/// JSON null; everything else converts under its column's plan.
fn encode_row(
    slab: &mut Vec<u8>,
    names: &[String],
    plans: &[ColumnPlan],
    record: &csv::StringRecord,
    path: &str,
    row: u64,
) -> Result<(), SourceError> {
    let mut object = serde_json::Map::with_capacity(names.len());
    for (i, cell) in record.iter().enumerate() {
        let Some(name) = names.get(i) else {
            return Err(SourceError::fatal(format!(
                "malformed CSV in `{path}` at row {row}: {} fields, {} columns",
                record.len(),
                names.len()
            )));
        };
        let value = if cell.is_empty() {
            serde_json::Value::Null
        } else {
            convert(cell, plans[i], path, row, name)?
        };
        object.insert(name.clone(), value);
    }
    serde_json::to_writer(&mut *slab, &serde_json::Value::Object(object))
        .map_err(|e| SourceError::fatal(e.to_string()))?;
    slab.push(b'\n');
    Ok(())
}

fn convert(
    cell: &str,
    plan: ColumnPlan,
    path: &str,
    row: u64,
    column: &str,
) -> Result<serde_json::Value, SourceError> {
    match plan {
        ColumnPlan::Shaped(shape) => convert_shaped(cell, shape, path, row, column),
        ColumnPlan::Hinted(hint) => convert_hinted(cell, hint, path, row, column),
    }
}

/// Convert under a SURVEYED shape. A cell the survey could not have
/// classified this way means the file changed between the passes —
/// typed and fatal, because any coercion (say, everything that is not
/// the literal `true` becoming false) would load wrong values without
/// a sound.
fn convert_shaped(
    cell: &str,
    shape: ValueShape,
    path: &str,
    row: u64,
    column: &str,
) -> Result<serde_json::Value, SourceError> {
    let changed = |expected: &str| {
        SourceError::fatal(format!(
            "`{path}` row {row} column `{column}`: value no longer parses as the inferred {expected} — the file changed between the inference and conversion passes; retry the run"
        ))
    };
    Ok(match shape {
        ValueShape::Bool => match cell {
            "true" => serde_json::Value::Bool(true),
            "false" => serde_json::Value::Bool(false),
            _ => return Err(changed("bool")),
        },
        ValueShape::Int => {
            serde_json::Value::from(cell.parse::<i64>().map_err(|_| changed("int64"))?)
        }
        ValueShape::Float => {
            let number = cell.parse::<f64>().map_err(|_| changed("float64"))?;
            match serde_json::Number::from_f64(number) {
                Some(finite) => serde_json::Value::Number(finite),
                None => {
                    return Err(SourceError::fatal(format!(
                        "`{path}` row {row} column `{column}`: non-finite value `{cell}` has no JSON representation — declare a utf8 type hint to load it as a string"
                    )));
                }
            }
        }
        ValueShape::Unseen | ValueShape::Text => serde_json::Value::String(cell.to_owned()),
    })
}

/// Convert under a DECLARED hint. A cell that cannot satisfy it is a
/// typed violation naming file, row, and column.
fn convert_hinted(
    cell: &str,
    hint: HintType,
    path: &str,
    row: u64,
    column: &str,
) -> Result<serde_json::Value, SourceError> {
    let unmet = |expected: &str| {
        SourceError::fatal(format!(
            "`{path}` row {row} column `{column}`: value does not satisfy the declared {expected} hint"
        ))
    };
    Ok(match hint {
        HintType::Bool => match cell {
            "true" => serde_json::Value::Bool(true),
            "false" => serde_json::Value::Bool(false),
            _ => return Err(unmet("bool")),
        },
        HintType::Int64 => {
            serde_json::Value::from(cell.parse::<i64>().map_err(|_| unmet("int64"))?)
        }
        HintType::Float64 => cell
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .ok_or_else(|| unmet("float64"))?,
        HintType::Json => serde_json::from_str(cell).map_err(|_| unmet("json"))?,
        // Everything string-shaped stays a JSON string here — the
        // engine applies the declared type further down. Spelled out one by
        // one ON PURPOSE: a future hint variant must break this match
        // and force a decision about its CSV conversion, instead of
        // arriving silently as text.
        HintType::Utf8
        | HintType::TimestampTz
        | HintType::Date
        | HintType::Time
        | HintType::Uuid => serde_json::Value::String(cell.to_owned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole lattice: bottom, idempotence, numeric widening,
    /// bool's disjointness, and commutativity over every pair.
    #[test]
    fn the_join_lattice_is_total_and_commutative() {
        use ValueShape::*;
        for shape in [Unseen, Bool, Int, Float, Text] {
            assert_eq!(Unseen.join(shape), shape, "Unseen is bottom");
            assert_eq!(shape.join(shape), shape, "idempotent");
        }
        assert_eq!(Int.join(Float), Float, "int widens");
        assert_eq!(Bool.join(Int), Text, "bool is disjoint from numbers");
        assert_eq!(Float.join(Text), Text);
        for a in [Unseen, Bool, Int, Float, Text] {
            for b in [Unseen, Bool, Int, Float, Text] {
                assert_eq!(a.join(b), b.join(a), "{a:?} {b:?}");
            }
        }
    }

    /// Classification is exact: only the two lowercase literals are
    /// bools, the numeric parses decide Int/Float, all else is text.
    #[test]
    fn cell_kinds_come_from_the_exact_rules() {
        assert_eq!(ValueShape::of("true"), ValueShape::Bool);
        assert_eq!(ValueShape::of("false"), ValueShape::Bool);
        assert_eq!(ValueShape::of("TRUE"), ValueShape::Text, "case matters");
        assert_eq!(ValueShape::of("42"), ValueShape::Int);
        assert_eq!(ValueShape::of("4.5"), ValueShape::Float);
        assert_eq!(ValueShape::of("4e2"), ValueShape::Float);
        assert_eq!(ValueShape::of("x"), ValueShape::Text);
    }

    /// A surveyed bool that no longer parses is the TYPED two-pass
    /// failure — never a silent false.
    #[test]
    fn a_changed_file_between_passes_fails_typed() {
        for altered in ["yes", "1", "TRUE"] {
            let err = convert(
                altered,
                ColumnPlan::Shaped(ValueShape::Bool),
                "f.csv",
                7,
                "flag",
            )
            .expect_err("typed");
            assert!(
                format!("{err}").contains("changed between the inference and conversion passes"),
                "{err}"
            );
        }
    }

    /// Hint violations carry file, row, and column; a non-finite
    /// float names the utf8 escape hatch.
    #[test]
    fn hint_violations_and_non_finite_floats_are_typed() {
        let err = convert("x", ColumnPlan::Hinted(HintType::Int64), "f.csv", 3, "n")
            .expect_err("violation");
        assert!(
            format!("{err}")
                .contains("`f.csv` row 3 column `n`: value does not satisfy the declared int64"),
            "{err}"
        );
        let err = convert(
            "NaN",
            ColumnPlan::Shaped(ValueShape::Float),
            "f.csv",
            4,
            "v",
        )
        .expect_err("non-finite");
        assert!(
            format!("{err}").contains("declare a utf8 type hint to load it as a string"),
            "{err}"
        );
    }

    /// A repeated header name refuses — name-keyed rows would silently
    /// drop the earlier column (030 review).
    #[tokio::test]
    async fn duplicate_header_names_are_refused() {
        use rdlt_connector_sdk::source::Feed;
        use rdlt_connector_sdk::spi::channel::records;

        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("rows.csv");
        let bytes: &[u8] = b"id,name,id\n1,a,2\n";
        std::fs::write(&path, bytes).expect("plant");
        let (out, _hold) = records(1 << 20);
        let mut feed = Feed::new(out);
        let mut cursor = FileCursor::default();
        let task = FileTask {
            path: path.to_str().expect("utf8").to_owned(),
            read_path: None,
            start: 0,
            size_units: bytes.len() as u64,
            mtime_ms: None,
            etag: None,
            resume_check: None,
        };
        let err = read_task(
            &task,
            &CsvOptions::default(),
            &Default::default(),
            &mut cursor,
            &mut feed,
        )
        .await
        .expect_err("refused")
        .to_string();
        assert!(
            err.contains("duplicate CSV column `id`")
                && err.contains("every header name must be unique"),
            "{err}"
        );
    }
}
