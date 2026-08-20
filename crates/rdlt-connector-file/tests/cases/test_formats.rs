//! The other two formats and the codecs, end to end through the
//! engine: CSV with inference and options, parquet with row-group
//! resume, gzip whole-file units.

use std::io::Write;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector_file::{destination, source};
use rdlt_connector_sdk::config::Document;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;

use super::common::{local_dest, plant};

async fn run(
    src: source::Config,
    config: &destination::Config,
    pipeline: &str,
    workdir: &std::path::Path,
) -> Result<(), String> {
    let src = source::Shell::new(src).expect("valid");
    let dest = destination::Shell::new(config.clone()).expect("valid");
    Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.join("wal")),
        src,
        dest,
    )
    .run()
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

fn stream(dir: &std::path::Path, extra: serde_json::Value) -> source::Config {
    let mut stream = serde_json::json!({
        "name": "events",
        "path": dir.join("data/*").to_str().expect("utf8"),
    });
    stream
        .as_object_mut()
        .expect("object")
        .extend(extra.as_object().expect("object").clone());
    source::Config::from_value(serde_json::json!({"streams": [stream]})).expect("valid")
}

/// CSV with a custom delimiter and no header: c0..cN columns, inferred
/// types, exact totals.
#[tokio::test]
async fn csv_reads_with_options_through_the_engine() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    plant(input.path(), "data/rows.csv", b"1;alpha\n2;beta\n3;gamma\n");

    let src = stream(
        input.path(),
        serde_json::json!({"format": "csv", "csv": {"delimiter": ";", "header": false}}),
    );
    let config = local_dest(out.path());
    run(src, &config, "csv-options", workdir.path())
        .await
        .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );
}

/// A grown CSV is a WHOLE-FILE refusal — the frozen size-change
/// framing, not a resume.
#[tokio::test]
async fn a_grown_csv_refuses_as_a_whole_file() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    plant(input.path(), "data/rows.csv", b"id,name\n1,alpha\n");

    let src = || stream(input.path(), serde_json::json!({"format": "csv"}));
    let config = local_dest(out.path());
    run(src(), &config, "csv-grown", workdir.path())
        .await
        .expect("first run");
    plant(input.path(), "data/rows.csv", b"id,name\n1,alpha\n2,beta\n");
    let err = run(src(), &config, "csv-grown", workdir.path())
        .await
        .expect_err("refused");
    assert!(
        err.contains("whole-file formats (csv, compressed) never grow in place"),
        "{err}"
    );
}

fn write_parquet(path: &std::path::Path, ids: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("parents");
    let file = std::fs::File::create(path).expect("create");
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

/// Parquet reads through the engine; a second file arriving later is
/// picked up by the next run while the finished file is skipped.
#[tokio::test]
async fn parquet_reads_and_resumes_per_file() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    write_parquet(&input.path().join("data/a.parquet"), &[1, 2, 3]);

    let src = || stream(input.path(), serde_json::json!({"format": "parquet"}));
    let config = local_dest(out.path());
    run(src(), &config, "pq-resume", workdir.path())
        .await
        .expect("first run");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );

    write_parquet(&input.path().join("data/b.parquet"), &[4, 5]);
    run(src(), &config, "pq-resume", workdir.path())
        .await
        .expect("second run");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        5,
        "only the new file's rows landed"
    );
}

/// A gzip jsonl file decodes as ONE whole-file unit; a lying extension
/// (gzip suffix, plain bytes) refuses with the magic-number framing.
#[tokio::test]
async fn gzip_decodes_whole_and_a_lying_extension_refuses() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(b"{\"id\": 1}\n{\"id\": 2}\n")
        .expect("encode");
    plant(
        input.path(),
        "data/events.jsonl.gz",
        &encoder.finish().expect("finish"),
    );

    let src = || stream(input.path(), serde_json::json!({"format": "jsonl"}));
    let config = local_dest(out.path());
    run(src(), &config, "gzip-whole", workdir.path())
        .await
        .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        2
    );

    let input2 = tempfile::tempdir().expect("input2");
    let out2 = tempfile::tempdir().expect("out2");
    let workdir2 = tempfile::tempdir().expect("workdir2");
    plant(input2.path(), "data/plain.jsonl.gz", b"{\"id\": 1}\n");
    let err = run(
        stream(input2.path(), serde_json::json!({"format": "jsonl"})),
        &local_dest(out2.path()),
        "gzip-lying",
        workdir2.path(),
    )
    .await
    .expect_err("refused");
    assert!(
        err.contains("gzip") || err.contains("magic"),
        "the lying extension names the mismatch: {err}"
    );
}
