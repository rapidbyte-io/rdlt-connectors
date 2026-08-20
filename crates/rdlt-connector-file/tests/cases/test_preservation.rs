//! The WELD PROOFS, carried verbatim from generation 1: committed
//! pre-015 artifacts — a cursor document, an on-disk commit log, the
//! layout names, config spellings — must keep their exact meaning
//! through the rewrite. These bytes are the persisted-format contract.

use rdlt_connector_file::destination;
use rdlt_connector_file::source::cursor::{FileCursor, FileMeta};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::core::schema::ident_hash;
use rdlt_connector_sdk::spi::core::{
    commit::WriteMode, cursor::Cursor, id::LoadId, id::PipelineId, id::TableName,
};
use rdlt_connector_sdk::spi::{destination::Destination, destination::OpenContext};
use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

use super::common::local_dest;

/// A cursor document EXACTLY as pre-015 runs persisted it
/// (format_version 1). Parsing and plan decisions must never change
/// for these bytes.
const PRE_015_CURSOR: &str = r#"{
  "format_version": 1,
  "files": {
    "data/complete.jsonl": {"done": 30, "size": 30, "eol": true, "mtime_ms": 1750000000000},
    "data/grown.jsonl":    {"done": 10, "size": 20, "eol": true, "mtime_ms": 1750000000000},
    "data/shrunk.jsonl":   {"done": 40, "size": 40, "eol": true, "mtime_ms": 1750000000000},
    "data/rewritten.jsonl":{"done": 25, "size": 25, "eol": true, "mtime_ms": 1750000000000},
    "data/midline.jsonl":  {"done": 15, "size": 15, "eol": false, "mtime_ms": 1750000000000}
  }
}"#;

fn meta(path: &str, size: u64, mtime_ms: u64) -> FileMeta {
    FileMeta {
        path: path.into(),
        size_units: size,
        mtime_ms: Some(mtime_ms),
        etag: None,
    }
}

#[test]
fn pre_015_cursor_document_parses_and_plans_identically() {
    let cursor_value: serde_json::Value = serde_json::from_str(PRE_015_CURSOR).expect("fixture");
    let cursor = FileCursor::decode(Some(&Cursor::new(cursor_value))).expect("pre-015 doc decodes");

    // Complete + unchanged → skipped; grown (eol) → resume at done; new → 0.
    let tasks = cursor
        .plan(&[
            meta("data/complete.jsonl", 30, 1750000000000),
            meta("data/grown.jsonl", 25, 1750000000001),
            meta("data/new.jsonl", 7, 1750000000002),
        ])
        .expect("plan");
    assert_eq!(tasks.len(), 2, "complete file is skipped");
    assert_eq!(
        (tasks[0].path.as_str(), tasks[0].start),
        ("data/grown.jsonl", 10)
    );
    assert_eq!(
        (tasks[1].path.as_str(), tasks[1].start),
        ("data/new.jsonl", 0)
    );

    // Shrunk → typed, naming the file.
    let err = cursor
        .plan(&[meta("data/shrunk.jsonl", 39, 1750000000001)])
        .expect_err("shrunk is loud")
        .to_string();
    assert!(
        err.contains("data/shrunk.jsonl") && err.contains("shrank"),
        "{err}"
    );

    // Same size, moved mtime → rewritten in place, typed.
    let err = cursor
        .plan(&[meta("data/rewritten.jsonl", 25, 1750009999999)])
        .expect_err("rewrite tripwire")
        .to_string();
    assert!(
        err.contains("data/rewritten.jsonl") && err.contains("rewritten"),
        "{err}"
    );

    // Grown past a non-eol consumed range → typed (mid-record offset).
    let err = cursor
        .plan(&[meta("data/midline.jsonl", 22, 1750000000001)])
        .expect_err("mid-record offset is loud")
        .to_string();
    assert!(
        err.contains("data/midline.jsonl") && err.contains("unterminated"),
        "{err}"
    );
}

/// The on-disk artifact NAMES and shapes are the persisted-format
/// contract (LAYOUT_FORMAT_VERSION 2): part-file pattern, state and
/// commit-log file names keyed by the pipeline scope hash, commit-log
/// JSON shape.
#[tokio::test]
async fn pre_015_artifact_names_and_commit_log_shape_are_frozen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = destination::Shell::new(local_dest(dir.path())).expect("valid");
    let pipeline = PipelineId::new("pin-pipeline");
    let load = LoadId::new("load-a");
    let scope = ident_hash(pipeline.as_str(), 12);

    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1, 2]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    assert!(
        dir.path().join("events/part-load-a-1-0.parquet").is_file(),
        "part-file naming per (load, seq, per-table n) is frozen"
    );
    assert!(
        dir.path()
            .join(format!("_rdlt_state.{scope}.json"))
            .is_file()
    );
    let commits_path = dir.path().join(format!("_rdlt_commits.{scope}.json"));
    let log: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&commits_path).expect("commit log exists"))
            .expect("commit log is JSON");
    assert_eq!(
        log["receipts"][0],
        serde_json::json!(["load-a", 1]),
        "commit-log receipt shape is frozen: {log}"
    );
}

/// AMENDED by 037 US4 D1: pre-015 (gen-1) commit logs used to keep
/// driving D3 receipt dedup transparently — a replayed (load, seq)
/// would discard its staged data and republish nothing, no matter how
/// old the log. Layout v2's partition-directory encoding is a
/// greenfield format break with deliberately NO migration path, so
/// this SAME pre-015 fixture (format_version 1) is now refused typed
/// at commit instead: the old cross-version dedup guarantee this test
/// used to pin is retired by design, not a regression.
#[tokio::test]
async fn pre_015_commit_log_fixture_now_refuses_as_predating_v2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("pin-pipeline");
    let scope = ident_hash(pipeline.as_str(), 12);
    let file = format!("_rdlt_commits.{scope}.json");
    std::fs::write(
        dir.path().join(&file),
        r#"{"format_version": 1, "receipts": [["load-x", 1]]}"#,
    )
    .expect("plant fixture");

    let config = local_dest(dir.path());
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let load = LoadId::new("load-x");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1, 2, 3]))
        .await
        .expect("write");
    let err = s
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("a pre-v2 commit log is refused, never silently migrated")
        .to_string();
    assert!(
        err.contains(&format!(
            "commit log `{file}` format v1 predates this build (v2): the partition-directory \
             encoding changed; point the destination at a fresh path or delete the old output"
        )),
        "{err}"
    );
}

/// Every pre-015 source-config spelling parses with identical meaning.
#[test]
fn pre_015_source_config_spellings_parse() {
    let config = rdlt_connector_file::source::Config::from_yaml(
        r#"
streams:
  - name: events
    format: jsonl
    path: "data/*.jsonl"
    primary_key: [id]
    type_hints: {created_at: timestamp_tz}
    validate: false
  - name: metrics
    format: parquet
    path: data/metrics.parquet
"#,
    )
    .expect("pre-015 document parses");
    assert_eq!(config.streams.len(), 2);
    assert_eq!(config.streams[0].validate, Some(false));
}
