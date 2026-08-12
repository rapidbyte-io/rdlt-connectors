//! The config document through the PUBLIC face: the generated schema
//! validates the corpus the parser accepts and refuses what it
//! refuses; secrets never render; the Shell family is the whole entry
//! surface.

use rdlt_connector_iceberg::destination::{Config, Shell, config_schema};
use rdlt_connector_sdk::config::Document;

use super::common::minimal_doc;

/// The generated schema and the parser agree on the corpus: every
/// document the parser accepts validates against the schema.
#[test]
fn the_schema_accepts_what_the_parser_accepts() {
    let corpus = vec![
        minimal_doc(),
        {
            let mut doc = minimal_doc();
            doc["create_namespace"] = serde_json::json!(true);
            doc["storage"] = serde_json::json!({"s3": {
                "endpoint": "http://s3", "region": "r",
                "access_key": "ak", "secret_key": "sk", "path_style": false,
            }});
            doc["tables"] = serde_json::json!({"events": {
                "name": "renamed",
                "partition_by": [
                    {"column": "ts", "transform": "day"},
                    {"column": "id", "transform": {"bucket": 16}},
                ],
            }});
            doc
        },
        {
            let mut doc = minimal_doc();
            doc["catalog"]["auth"] = serde_json::json!({"oauth2_client_credentials": {
                "client_id": "c", "client_secret": "s",
                "scopes": ["a"], "token_url": "http://t",
            }});
            doc["catalog"]["props"] = serde_json::json!({"io-impl": "custom"});
            doc
        },
    ];
    let schema = jsonschema::validator_for(&config_schema()).expect("valid schema");
    for doc in corpus {
        Config::from_value(doc.clone()).expect("parser accepts");
        assert!(
            schema.validate(&doc).is_ok(),
            "schema must accept what the parser accepts: {doc}"
        );
    }
}

/// Unknown fields refuse in the parser AND fail schema validation —
/// the two gates cannot drift apart.
#[test]
fn unknown_fields_refuse_in_parser_and_schema_alike() {
    let mut doc = minimal_doc();
    doc["catalog"]["warehous"] = serde_json::json!("typo");
    Config::from_value(doc.clone()).expect_err("parser refuses");
    let schema = jsonschema::validator_for(&config_schema()).expect("valid schema");
    assert!(schema.validate(&doc).is_err(), "schema refuses too");
}

/// The whole Shell family parses and validates: yaml, json, value,
/// and an already-parsed config — no path around the gate.
#[test]
fn every_shell_entry_passes_the_document_gate() {
    let doc = minimal_doc();
    Shell::from_value(doc.clone()).expect("value");
    Shell::from_yaml(&serde_yaml::to_string(&doc).expect("yaml")).expect("yaml");
    Shell::from_json(&serde_json::to_string(&doc).expect("json")).expect("json");
    Shell::new(Config::from_value(doc).expect("config")).expect("new");

    let mut bad = minimal_doc();
    bad["namespace"] = serde_json::json!("raw..orders");
    let err = Shell::from_value(bad).expect_err("the gate refuses");
    assert!(
        format!("{err}").contains("must be non-empty dot-separated levels"),
        "{err}"
    );
}

/// Secrets never render through the public Debug surface, whichever
/// field carries them.
#[test]
fn secrets_never_render_through_the_shell() {
    let mut doc = minimal_doc();
    doc["catalog"]["auth"] = serde_json::json!({"oauth2_client_credentials": {
        "client_id": "c", "client_secret": "sekret-1",
    }});
    doc["storage"] = serde_json::json!({"s3": {"access_key": "ak-2", "secret_key": "sk-3"}});
    let config = Config::from_value(doc).expect("valid");
    let rendered = format!("{config:?}");
    for leaked in ["sekret-1", "ak-2", "sk-3"] {
        assert!(!rendered.contains(leaked), "{leaked} leaked: {rendered}");
    }
}
