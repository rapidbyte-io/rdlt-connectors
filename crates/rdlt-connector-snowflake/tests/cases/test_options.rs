//! The shared destination-option vocabulary reads identically here to
//! every other SQL destination — one YAML shape across the family — and
//! sqlcore's validation fires through this crate's document gate.

use rdlt_connector_sdk::config::Document;
use rdlt_connector_snowflake::destination::Config;
use serde_json::json;

fn minimal() -> serde_json::Value {
    json!({
        "account": "MYORG-MYACCT",
        "user": "LOADER",
        "auth": {"key_pair": {"private_key": "/k.p8"}},
        "database": "ANALYTICS",
        "schema": "RAW",
    })
}

/// The family's spellings all parse: a default strategy, per-table
/// strategies, hard delete, dedup sort, merge scope, scd2 columns.
#[test]
fn the_family_option_spellings_parse_flattened() {
    let mut doc = minimal();
    doc["merge_strategy"] = json!("delete_insert");
    doc["tables"] = json!({
        "events": {
            "merge_strategy": "upsert",
            "hard_delete": "_deleted",
            "dedup_sort": {"column": "v", "order": "desc"},
        },
        "dims": {
            "merge_strategy": "scd2",
            "scd2": {"valid_from": "vf", "valid_to": "vt"},
        },
    });
    let config = Config::from_value(doc).expect("the family vocabulary parses");
    assert!(config.options.merge_strategy.is_some());
    assert_eq!(config.options.tables.len(), 2);
    assert!(config.options.tables["events"].hard_delete.is_some());
    assert!(config.options.tables["dims"].scd2.is_some());
}

/// sqlcore's own validation reaches this document too: scd2 options
/// without the scd2 strategy are refused at parse, with the shared
/// wording (the same sentence postgres and duckdb produce).
#[test]
fn invalid_option_combinations_are_refused_through_the_gate() {
    let mut doc = minimal();
    doc["tables"] = json!({
        "events": {"scd2": {"valid_from": "vf"}},
    });
    let err = Config::from_value(doc).expect_err("scd2 options without the strategy are refused");
    let rendered = err.to_string();
    assert!(rendered.contains("events"), "names the table: {rendered}");
    assert!(rendered.contains("scd2"), "names the option: {rendered}");
}
