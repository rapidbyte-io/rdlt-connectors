//! The document gate and a first live round-trip through the Shell.

use rdlt_connector_duckdb::destination::{self, Config, Shell};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector_sdk::spi::{Destination, OpenContext};
use rdlt_testkit::{batch_of, commit_meta_for, schema_for};

/// The frozen bare-identifier refusal, now at the Document gate.
#[test]
fn setting_keys_and_extension_names_must_be_bare() {
    let mut config = Config::new("x.duckdb");
    config.settings = Some(
        [("threads'; DROP TABLE x; --".to_owned(), "1".to_owned())]
            .into_iter()
            .collect(),
    );
    let err = config.validate().expect_err("refused").to_string();
    assert_eq!(
        err,
        "invalid duckdb destination config: duckdb setting `threads'; DROP TABLE x; --`: \
         keys must be bare identifiers ([A-Za-z0-9_]) — refusing to interpolate"
    );
}

/// 034: `parts` is REFUSED here, not accepted and ignored.
///
/// This destination appends into tables — there is no output file for
/// a part size to describe. `deny_unknown_fields` already does the
/// refusing; the pin exists so that removing it, or adding a field
/// that shadows it, fails a test rather than silently making a
/// meaningless setting look effective.
#[test]
fn part_sizing_is_refused_because_there_are_no_files() {
    let err = Config::from_value(serde_json::json!({
        "path": "x.duckdb",
        "parts": {"target_bytes": 134217728},
    }))
    .expect_err("refused")
    .to_string();
    assert!(err.contains("parts"), "the refusal names the field: {err}");
    assert!(err.contains("unknown field"), "{err}");
}

/// A full write→commit→read-back round trip through the Shell against
/// a real database file — the Backend works before the suite grows.
#[tokio::test]
async fn a_first_load_lands_and_counts() {
    let dir = tempfile::tempdir().expect("dir");
    let config = Config::new(dir.path().join("out.duckdb"));
    let shell = Shell::new(config).expect("valid");
    let pipeline = PipelineId::new("smoke");
    let load = LoadId::new("load-1");
    let mut s = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1, 2, 3]))
        .await
        .expect("write");
    let receipt = s
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
    assert_eq!(receipt.commit_seq, 1);

    // Count through the testhook's own fresh instance over the same
    // file (proves the publish is durable, not a temp-table illusion).
    drop(s);
    let config = Config::new(dir.path().join("out.duckdb"));
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );
}

/// Both memory-limit spellings at once are refused — the later SET
/// would silently win otherwise; case-insensitive because DuckDB SET
/// keys are.
#[test]
fn memory_limit_in_settings_conflicts_with_the_field() {
    for spelling in ["memory_limit", "MEMORY_LIMIT"] {
        let mut config = Config::new("x.duckdb");
        config.memory_limit = Some("1GB".to_owned());
        config.settings = Some(
            [(spelling.to_owned(), "8GB".to_owned())]
                .into_iter()
                .collect(),
        );
        let err = config.validate().expect_err("refused").to_string();
        assert!(
            err.contains("conflicts with the `memory_limit` field"),
            "{spelling}: {err}"
        );
    }
}
