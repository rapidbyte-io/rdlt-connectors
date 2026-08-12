//! The published JSON Schema and the parser are two views of one set of
//! structs, so they must agree on every document: what the schema admits the
//! parser accepts, and what either rejects the other rejects too. The cases
//! below drive both layers with the same documents and compare their verdicts.

use jsonschema::validator_for;
use rdlt_connector_postgres::source::{Config, config_schema};
use rdlt_connector_sdk::config::Document;
use serde_json::json;

fn example() -> serde_json::Value {
    json!({
        "conn": "postgres://app@db.internal/app",
        "schema": "public",
        "tls": {"mode": "verify_full", "root_cert": "/etc/rdlt/ca.pem",
                "client_cert": "/etc/rdlt/client.pem", "client_key": "/etc/rdlt/client.key"},
        "tables": [
            {
                "name": "orders",
                "cursor": {"column": "updated_at", "lag": "5m",
                           "end_bound": "inclusive", "nulls": "error"},
                "type_hints": {"total": "decimal(12,4)", "created": "timestamp_tz"}
            }
        ],
        "queries": [
            {
                "name": "order_totals",
                "sql": "SELECT id, sum(x) AS total FROM t GROUP BY id",
                "primary_key": ["id"]
            }
        ]
    })
}

#[test]
fn documented_example_validates_and_parses() {
    let validator = validator_for(&config_schema()).expect("generated schema compiles");
    let example = example();
    assert!(
        validator.is_valid(&example),
        "example must validate: {:?}",
        validator.iter_errors(&example).next()
    );
    Config::from_value(example).expect("schema-valid example parses");
}

#[test]
fn unknown_fields_fail_schema_and_parser_identically() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let mut bad = example();
    bad["ssl"] = json!("on"); // plausible typo for `tls`
    assert!(
        !validator.is_valid(&bad),
        "unknown field must fail the schema"
    );
    assert!(
        Config::from_value(bad).is_err(),
        "and the parser agrees (deny_unknown_fields)"
    );
}

#[test]
fn schema_valid_corpus_parses() {
    // schema-valid ⇒ parses: the declared surface never over-promises.
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let corpus = [
        json!({"conn": "host=localhost user=a dbname=d"}),
        json!({"conn": "postgres://u@h/d", "include_views": true, "batch_max_rows": 7}),
        json!({"conn": "postgres://u@h/d", "tls": {"mode": "require"}}),
        json!({"conn": "postgres://u@h/d",
               "queries": [{"name": "q", "sql": "SELECT 1 AS x"}]}),
    ];
    for config in corpus {
        assert!(
            validator.is_valid(&config),
            "corpus entry invalid: {config}"
        );
        Config::from_value(config.clone())
            .unwrap_or_else(|e| panic!("schema-valid config failed to parse: {config}: {e}"));
    }
    // Bad hint strings are stopped by the schema's pattern, same as FromStr.
    let bad_hint = json!({"conn": "postgres://u@h/d",
        "tables": [{"name": "t", "type_hints": {"c": "decimal(banana)"}}]});
    assert!(!validator.is_valid(&bad_hint));
    assert!(Config::from_value(bad_hint).is_err());
    // Bad lag strings likewise (pattern mirrors FromStr)…
    let bad_lag = json!({"conn": "postgres://u@h/d",
        "tables": [{"name": "t", "cursor": {"column": "ts", "lag": "soon"}}]});
    assert!(!validator.is_valid(&bad_lag));
    assert!(Config::from_value(bad_lag).is_err());
    // …and the new field corpus is schema-valid AND parses.
    let new_fields = json!({"conn": "postgres://u@h/d",
        "tls": {"mode": "require", "client_cert": "/c.pem", "client_key": "/k.pem"},
        "tables": [{"name": "t",
                    "cursor": {"column": "ts", "lag": "1d",
                               "end_value": "2026-01-01T00:00:00Z",
                               "end_bound": "inclusive", "nulls": "error"}}]});
    assert!(validator.is_valid(&new_fields), "new fields validate");
    Config::from_value(new_fields).expect("new fields parse");
}

// ---- the cdc block ----

#[test]
fn cdc_block_round_trips_the_schema() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    // The documented example (quickstart shape) validates AND parses.
    let example = json!({"conn": "postgres://u@h/d",
        "cdc": {"slot": "my_slot", "publication": "my_pub",
                "create_if_missing": true, "mode": "tail",
                "idle_wait": "5s", "flag_column": "_rdlt_deleted", "ack": "auto"},
        "tables": [{"name": "orders"}]});
    assert!(
        validator.is_valid(&example),
        "cdc example must validate: {:?}",
        validator.iter_errors(&example).next()
    );
    Config::from_value(example).expect("cdc example parses");
    // Unknown fields fail BOTH layers (C4).
    let unknown = json!({"conn": "postgres://u@h/d",
        "cdc": {"slot": "s", "publication": "p", "drop_slot": true}});
    assert!(!validator.is_valid(&unknown));
    assert!(Config::from_value(unknown).is_err());
    // idle_wait keeps the duration vocabulary at the schema layer too (C4):
    // a magnitude fails the pattern, same as FromStr.
    let bad_wait = json!({"conn": "postgres://u@h/d",
        "cdc": {"slot": "s", "publication": "p", "idle_wait": "5"}});
    assert!(!validator.is_valid(&bad_wait));
    assert!(Config::from_value(bad_wait).is_err());
    // cdc + cursor exclusivity (C1) is a VALIDATION rule: schema-valid by
    // shape, rejected by the parser naming the table.
    let exclusive = json!({"conn": "postgres://u@h/d",
        "cdc": {"slot": "s", "publication": "p"},
        "tables": [{"name": "t", "cursor": {"column": "id"}}]});
    assert!(validator.is_valid(&exclusive), "shape-valid");
    let error = Config::from_value(exclusive).expect_err("C1").to_string();
    assert!(
        error.contains("`t`") && error.contains("mutually exclusive"),
        "{error}"
    );
}
