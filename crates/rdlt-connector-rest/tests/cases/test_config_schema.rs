//! T019: generated config schema — examples validate, unknown fields fail
//! schema AND parser identically.

use jsonschema::validator_for;
use rdlt_connector_rest::source::{Config, config_schema};
use rdlt_connector_sdk::config::Document;
use serde_json::json;

fn example() -> serde_json::Value {
    json!({
        "base_url": "https://api.example.com",
        "auth": {"bearer": {"token": "secret"}},
        "streams": [
            {
                "name": "issues",
                "path": "/issues",
                "pagination": {"type": "page", "start": 1},
                "cursor_field": "updated_at",
                "cursor_param": "since",
                "primary_key": ["id"],
                "type_hints": {"updated_at": "timestamp_tz"}
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
    bad["streams"][0]["cursor"] = json!("updated_at"); // typo for cursor_field
    assert!(!validator.is_valid(&bad));
    assert!(Config::from_value(bad).is_err());
}

#[test]
fn schema_valid_corpus_parses() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let corpus = [
        json!({"base_url": "https://x", "streams": [{"name": "s", "path": "/s"}]}),
        json!({"base_url": "https://x", "auth": "none",
               "streams": [{"name": "s", "path": "/s",
                            "pagination": {"type": "offset", "page_size": 50}}]}),
        // The full 014 surface in one document: api_key auth, source
        // defaults, pacing knobs, POST body, selector, link_header, the
        // incremental block, response actions, parent-child.
        json!({"base_url": "https://x",
        "auth": {"api_key": {"name": "k", "key": "v", "location": "query"}},
        "headers": {"x-a": "1"}, "params": {"b": "2"},
        "max_concurrency": 4, "min_request_interval_ms": 100,
        "retry_after_cap_secs": 60, "max_pages": 50,
        "streams": [
            {"name": "parents", "path": "/p",
             "pagination": {"type": "cursor", "cursor_path": "next",
                             "cursor_param": "c"}},
            {"name": "kids", "path": "/p/{id}/kids",
             "method": "post", "body": {"q": "{id}"},
             "records_path": "data.items[*]",
             "pagination": {"type": "link_header"},
             "incremental": {"cursor_field": "seq", "start_param": "since",
                              "end_param": "until", "end_value": "9",
                              "initial_value": "0"},
             "response_actions": [{"status": 404, "action": "end_stream"}],
             "parent": {"stream": "parents",
                         "placeholders": {"id": "id"}, "include": ["id"]}}
        ]}),
        // The remaining families and schemes.
        json!({"base_url": "https://x",
        "auth": {"oauth2_client_credentials": {
            "token_url": "https://x/token", "client_id": "c",
            "client_secret": "s", "scopes": ["read"],
            "audience": "aud", "expiry_margin_secs": 30}},
        "streams": [
            {"name": "a", "path": "/a",
             "pagination": {"type": "header_cursor", "header": "x-next",
                             "cursor_param": "c"}},
            {"name": "b", "path": "/b",
             "pagination": {"type": "next_url", "next_url_path": "next"}},
            {"name": "c", "path": "/c",
             "pagination": {"type": "page", "total_pages_path": "meta.pages"}},
            {"name": "d", "path": "/d",
             "pagination": {"type": "offset", "page_size": 10,
                             "total_count_path": "meta.total"}}
        ]}),
    ];
    for config in corpus {
        assert!(
            validator.is_valid(&config),
            "corpus entry invalid: {config}"
        );
        Config::from_value(config.clone())
            .unwrap_or_else(|e| panic!("schema-valid config failed to parse: {config}: {e}"));
    }
}

#[test]
fn empty_streams_rejected() {
    let err = Config::from_value(json!({"base_url": "https://x", "streams": []}))
        .expect_err("no streams")
        .to_string();
    assert!(err.contains("at least one stream"), "{err}");
}
