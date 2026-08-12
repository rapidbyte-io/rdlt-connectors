//! Both halves' documents: the generated schemas ACCEPT what the
//! parsers accept and REFUSE what they refuse — the schema is
//! generated from the same structs, and these cells keep that claim
//! honest through the Shell entry points every embedder uses.

use rdlt_connector_file::{destination, source};

fn compiled(schema: serde_json::Value) -> jsonschema::Validator {
    jsonschema::validator_for(&schema).expect("schema compiles")
}

/// The source schema and parser agree on a representative document —
/// and on its refusal.
#[test]
fn the_source_schema_and_parser_agree() {
    let schema = compiled(source::config_schema());
    let good = serde_json::json!({
        "streams": [
            {"name": "a", "format": "jsonl", "path": "data/*.jsonl", "validate": false},
            {"name": "b", "format": "csv", "path": "data/b.csv",
             "csv": {"delimiter": ";", "header": false},
             "type_hints": {"ts": "timestamp_tz"}},
            {"name": "c", "format": "parquet", "path": "data/c.parquet"},
        ]
    });
    assert!(schema.is_valid(&good));
    assert!(source::Shell::from_value(good).is_ok());

    let unknown_key = serde_json::json!({
        "streams": [{"name": "a", "format": "jsonl", "path": "x", "fromat": "jsonl"}]
    });
    assert!(
        !schema.is_valid(&unknown_key),
        "deny_unknown_fields reaches the schema"
    );
    assert!(source::Shell::from_value(unknown_key).is_err());
}

/// The destination schema and parser agree — including the embedded
/// SPI parquet block.
#[test]
fn the_destination_schema_and_parser_agree() {
    let schema = compiled(destination::config_schema());
    let good = serde_json::json!({
        "path": "out",
        "format": "parquet",
        "partition_by": "region",
        "parquet": {"compression": "zstd", "compression_level": 3}
    });
    assert!(schema.is_valid(&good));
    assert!(destination::Shell::from_value(good).is_ok());

    let bad = serde_json::json!({"path": "out", "format": "orc"});
    assert!(!schema.is_valid(&bad));
    assert!(destination::Shell::from_value(bad).is_err());
}

/// The Shell's four entry points all pass the same gate: a document
/// refused by validate() is refused from YAML, JSON, and value alike.
#[test]
fn every_entry_point_passes_the_same_gate() {
    let yaml = "path: ''\n";
    let json = r#"{"path": ""}"#;
    let err = destination::Shell::from_yaml(yaml)
        .expect_err("refused")
        .to_string();
    assert_eq!(
        err,
        "invalid file destination config: file destination: `path` must not be empty"
    );
    assert!(destination::Shell::from_json(json).is_err());
    assert!(destination::Shell::new(destination::Config::new("")).is_err());
}
