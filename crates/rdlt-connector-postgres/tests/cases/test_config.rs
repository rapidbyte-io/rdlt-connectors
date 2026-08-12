//! What a source document may say and what it may not: the defaults a
//! minimal document inherits, the shapes the local gate refuses, and the
//! wording each refusal reaches the user with. Nothing here touches a
//! database — every rule below is decidable from the document alone.

use rdlt_connector_postgres::source::config::*;
use rdlt_connector_sdk::config::Document;

const MINIMAL: &str = r#"
conn: "postgresql://u:p@localhost:5432/db"
"#;

#[test]
fn a_minimal_config_fills_in_documented_defaults() {
    let config = Config::from_yaml(MINIMAL).expect("minimal config");
    assert_eq!(config.schema, "public");
    assert!(!config.include_views);
    assert!(config.tables.is_none());
    assert_eq!(config.batch_target_bytes, 8 << 20);
    assert_eq!(config.batch_max_rows, 65_536);
}

#[test]
fn unknown_fields_rejected() {
    let error = Config::from_yaml("conn: host=localhost\nfrobnicate: true\n").unwrap_err();
    assert!(matches!(error, ConfigError::Yaml(_)), "{error}");
}

#[test]
fn qualified_table_name_rejected() {
    let error =
        Config::from_yaml("conn: host=localhost\ntables:\n  - name: sales.orders\n").unwrap_err();
    assert!(error.to_string().contains("schema-qualified"), "{error}");
}

#[test]
fn include_exclude_mutually_exclusive() {
    let error = Config::from_yaml(
        "conn: host=localhost\ntables:\n  - name: t\n    included_columns: [a]\n    excluded_columns: [b]\n",
    )
    .unwrap_err();
    assert!(error.to_string().contains("mutually exclusive"), "{error}");
}

/// An empty table list means "no tables" — the only way to say "deliver the
/// declared queries and nothing else". Absent, by contrast, still discovers
/// every table, so a queries-only pipeline that omits the field receives the
/// whole schema alongside its queries.
#[test]
fn empty_table_list_selects_no_tables_and_keeps_queries() {
    let config = Config::from_yaml(
        "conn: host=localhost\ntables: []\nqueries:\n  - name: q\n    sql: SELECT 1\n",
    )
    .expect("empty list alongside a query is a valid selection");
    assert_eq!(config.tables.as_deref(), Some(&[][..]));
    assert_eq!(config.queries.len(), 1);
}

/// Selecting nothing at all would move zero rows and report success, so it
/// is refused where it is knowable — at parse time, from the document alone.
#[test]
fn selecting_no_streams_at_all_is_rejected_naming_both_remedies() {
    let error = Config::from_yaml("conn: host=localhost\ntables: []\n")
        .expect_err("no tables and no queries selects nothing");
    let message = error.to_string();
    assert!(message.contains("no streams selected"), "{message}");
    assert!(
        message.contains("queries") && message.contains("omit"),
        "{message}"
    );
}

/// Change data capture reads the configured tables; configuring none would
/// leave the slot un-preflighted and never advanced, i.e. the block would
/// silently behave as if it were absent.
#[test]
fn cdc_with_an_empty_table_list_is_rejected() {
    let error = Config::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s1\n  publication: p1\ntables: []\n\
         queries:\n  - name: q\n    sql: SELECT 1\n",
    )
    .expect_err("cdc captures the configured tables; none means nothing");
    assert!(error.to_string().contains("captures nothing"), "{error}");
}

#[test]
fn empty_selections_rejected() {
    for document in [
        "conn: host=localhost\ntables: []\n",
        "conn: host=localhost\ntables:\n  - name: t\n    included_columns: []\n",
        "conn: host=localhost\ntables:\n  - name: t\n    primary_key: []\n",
        "conn: \"\"\n",
        "conn: host=localhost\nbatch_max_rows: 0\n",
    ] {
        assert!(
            Config::from_yaml(document).is_err(),
            "should reject: {document}"
        );
    }
}

#[test]
fn cdc_block_parses_with_defaults() {
    let config = Config::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s1\n  publication: p1\ntables:\n  - name: orders\n",
    )
    .expect("cdc config");
    let cdc = config.cdc.expect("cdc");
    assert_eq!(cdc.slot, "s1");
    assert_eq!(cdc.publication, "p1");
    assert!(!cdc.create_if_missing);
    assert_eq!(cdc.mode, CdcMode::Catchup);
    assert_eq!(cdc.idle_wait, Wait { seconds: 1 });
    assert_eq!(cdc.flag_column, "_rdlt_deleted");
    assert_eq!(cdc.ack, AckMode::Auto);
}

#[test]
fn cdc_validation_matrix() {
    // cursor + cdc on the same table — typed, names the table.
    let error = Config::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
         tables:\n  - name: orders\n    cursor:\n      column: id\n",
    )
    .unwrap_err();
    assert!(error.to_string().contains("`orders`"), "{error}");
    assert!(error.to_string().contains("mutually exclusive"), "{error}");
    // Required + non-empty names.
    for document in [
        "conn: host=localhost\ncdc:\n  publication: p\n",
        "conn: host=localhost\ncdc:\n  slot: s\n",
        "conn: host=localhost\ncdc:\n  slot: \"\"\n  publication: p\n",
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  flag_column: \"\"\n",
        // unknown fields fail; idle_wait rejects magnitudes.
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  drop_slot: true\n",
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  idle_wait: \"5\"\n",
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  mode: streaming\n",
    ] {
        assert!(
            Config::from_yaml(document).is_err(),
            "should reject: {document}"
        );
    }
    // Tail mode + duration idle_wait + ack off parse.
    let config = Config::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
         \x20 mode: tail\n  idle_wait: \"5m\"\n  ack: off\n",
    )
    .expect("tail config");
    let cdc = config.cdc.expect("cdc");
    assert_eq!(cdc.mode, CdcMode::Tail);
    assert_eq!(cdc.idle_wait, Wait { seconds: 300 });
    assert_eq!(cdc.ack, AckMode::Off);
}

#[test]
fn json_and_value_entry_points_share_validation() {
    let json =
        r#"{"conn": "host=localhost", "tables": [{"name": "t", "cursor": {"column": "id"}}]}"#;
    let from_json = Config::from_json(json).expect("json");
    let from_yaml = Config::from_yaml(
        "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: id\n",
    )
    .expect("yaml");
    assert_eq!(from_json, from_yaml, "one document shape, two syntaxes");
    let value: serde_json::Value = serde_json::from_str(json).expect("value");
    assert_eq!(Config::from_value(value).expect("value"), from_json);
    // Validation is shared: the parse gate fires on every entry point.
    assert!(Config::from_json(r#"{"conn": "not a conn"}"#).is_err());
    assert!(
        Config::from_value(serde_json::json!({"conn": "x", "unknown": 1})).is_err(),
        "deny_unknown_fields holds for Value too"
    );
}

#[test]
fn malformed_conn_strings_and_tls_contradictions_are_typed_at_parse() {
    // Contract rule 1: parse failure = typed CONFIG error, up front.
    let error = Config::from_yaml("conn: not-a-conn-string\n").unwrap_err();
    assert!(error.to_string().contains("does not parse"), "{error}");
    // TLS is wired — every conn-string sslmode level now passes config
    // validation (incl. the spaced keyword form).
    for connection_string in [
        "postgresql://u:p@h/db?sslmode=require",
        "host=h sslmode=require",
        "host=h sslmode = require",
        "host=h sslmode=prefer",
        "host=h sslmode=disable",
    ] {
        assert!(
            Config::from_yaml(&format!("conn: \"{connection_string}\"\n")).is_ok(),
            "{connection_string} must validate"
        );
    }
    // Contradiction rule (tls-policy.md): explicit conn sslmode reversed
    // by the block = typed config error; refinement is allowed.
    let error = Config::from_yaml("conn: \"host=h sslmode=disable\"\ntls:\n  mode: verify_full\n")
        .unwrap_err();
    assert!(error.to_string().contains("contradicts"), "{error}");
    assert!(
        Config::from_yaml("conn: \"host=h sslmode=require\"\ntls:\n  mode: verify_full\n").is_ok(),
        "require -> verify_full is refinement, not contradiction"
    );
}

#[test]
fn duplicate_tables_rejected() {
    let error =
        Config::from_yaml("conn: host=localhost\ntables:\n  - name: t\n  - name: t\n").unwrap_err();
    assert!(error.to_string().contains("listed twice"), "{error}");
}

#[test]
fn lag_with_open_boundary_dies_at_config_parse() {
    let error = Config::from_yaml(
        "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: ts\n      boundary: exclusive\n      lag: \"5m\"\n",
    )
    .unwrap_err();
    assert!(error.to_string().contains("INCLUSIVE boundary"), "{error}");
    // Closed (default) parses fine.
    Config::from_yaml(
        "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: ts\n      lag: \"5m\"\n",
    )
    .expect("closed + lag parses");
}

#[test]
fn tls_contradiction_rejected_at_config_validation() {
    // sslmode=require is ACCEPTED (TLS is wired); the config-level
    // rejection is now the contradiction rule.
    assert!(
        Config::from_yaml("conn: \"postgresql://u:p@localhost/db?sslmode=require\"\n").is_ok(),
        "require now validates — TLS is wired"
    );
    let error = Config::from_yaml(
        "conn: \"postgresql://u:p@localhost/db?sslmode=require\"\ntls:\n  mode: disable\n",
    )
    .unwrap_err();
    assert!(error.to_string().contains("contradicts"), "{error}");
}
