//! The `json_type` capability is REAL, end to end: a Json column
//! written through the Shell lands as a native DuckDB JSON column —
//! declared JSON in the catalog, directly queryable with
//! json_extract — and merge semantics compose with it. A capability
//! declared but never exercised would let the engine plan around a
//! type that never materializes; these cells are the exercise.
//!
//! The wire shape under test: the stage leg types Json as VARCHAR
//! (the Arrow appender writes Utf8), and the publish INSERT…SELECT
//! applies DuckDB's implicit VARCHAR→JSON cast, which validates the
//! document on the way through.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector_duckdb::destination::{self, Config, Shell};
use rdlt_connector_sdk::spi::core::{
    ColumnDef, ColumnType, LoadId, LogicalType, PipelineId, Provenance, TableName, TableSchema,
    WriteMode,
};
use rdlt_connector_sdk::spi::{Destination, OpenContext};
use rdlt_testkit::commit_meta_for;

/// `docs(id: Int64, doc: Json)` — the logical schema the SPI carries.
fn docs_schema() -> TableSchema {
    TableSchema {
        table: TableName::new("docs"),
        parent: None,
        columns: vec![
            ColumnDef {
                name: "id".to_owned(),
                column_type: ColumnType::scalar(LogicalType::Int64),
                nullable: false,
                provenance: Provenance::Inferred,
            },
            ColumnDef {
                name: "doc".to_owned(),
                column_type: ColumnType::scalar(LogicalType::Json),
                nullable: true,
                provenance: Provenance::Inferred,
            },
        ],
    }
}

/// The physical batch: Json travels as Utf8 text in schema column
/// order (the stage appender is positional).
fn docs_batch(rows: &[(i64, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let payloads: Vec<&str> = rows.iter().map(|(_, doc)| *doc).collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("doc", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ],
    )
    .expect("json fixture batch")
}

/// One whole load of `rows` under `mode`, as its own session.
async fn load(
    shell: &Shell,
    pipeline: &PipelineId,
    load_id: &str,
    mode: &WriteMode,
    rows: &[(i64, &str)],
) {
    let load_id = LoadId::new(load_id);
    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load_id.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&docs_schema(), mode)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("docs"), docs_batch(rows))
        .await
        .expect("write");
    session
        .commit(commit_meta_for(pipeline, &load_id, 1))
        .await
        .expect("commit");
}

fn cell(config: &Config, sql: &str) -> String {
    destination::testhook::query_string(config, sql).expect("query")
}

/// The declared column type is JSON — not the VARCHAR the stage used —
/// and DuckDB's own json_extract works on the landed column directly.
#[tokio::test]
async fn a_json_column_lands_native_and_extracts() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::new(dir.path().join("native.duckdb"));
    let shell = Shell::new(config.clone()).expect("valid");
    load(
        &shell,
        &PipelineId::new("native"),
        "load-1",
        &WriteMode::Append,
        &[
            (1, r#"{"depth": {"value": 31}}"#),
            (2, r#"{"depth": null}"#),
        ],
    )
    .await;
    drop(shell);

    assert_eq!(
        cell(
            &config,
            "SELECT data_type FROM information_schema.columns \
             WHERE table_name = 'docs' AND column_name = 'doc'",
        ),
        "JSON",
        "the capability's whole point: native, not text"
    );
    assert_eq!(
        cell(
            &config,
            "SELECT CAST(json_extract(doc, '$.depth.value') AS VARCHAR) \
             FROM docs WHERE id = 1",
        ),
        "31",
        "json_extract straight over the landed column"
    );
}

/// Merge composes with native JSON: a redelivered key REPLACES its
/// document, leaving one row whose payload is the later delivery.
#[tokio::test]
async fn a_redelivered_key_replaces_its_json_document() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::new(dir.path().join("merge.duckdb"));
    let shell = Shell::new(config.clone()).expect("valid");
    let pipeline = PipelineId::new("compose");
    let keyed = WriteMode::Merge {
        key: vec!["id".to_owned()],
    };
    load(
        &shell,
        &pipeline,
        "load-1",
        &keyed,
        &[(1, r#"{"rev": "first"}"#), (2, r#"{"rev": "only"}"#)],
    )
    .await;
    load(
        &shell,
        &pipeline,
        "load-2",
        &keyed,
        &[(1, r#"{"rev": "second"}"#)],
    )
    .await;
    drop(shell);

    assert_eq!(
        destination::testhook::count_rows(&config, "docs").expect("count"),
        2,
        "redelivery replaced — never duplicated"
    );
    assert_eq!(
        cell(
            &config,
            "SELECT CAST(json_extract(doc, '$.rev') AS VARCHAR) FROM docs WHERE id = 1",
        ),
        "\"second\"",
        "the surviving document is the redelivered one"
    );
}
