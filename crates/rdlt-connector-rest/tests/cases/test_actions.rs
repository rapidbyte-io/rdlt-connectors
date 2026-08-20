//! Feature 014 US1 (T005/T007): selectors, response actions, POST bodies.

use super::common::{read_err, read_ok, read_stream, stream_yaml};
use rdlt_connector_sdk::config::Document;
use serde_json::json;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Wildcard selectors extract nested records.
#[tokio::test]
async fn wildcard_selector_extracts_nested() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"groups": [
                {"payload": {"id": 1}},
                {"payload": {"id": 2}}
            ]}
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data.groups[*].payload",
    );
    let rows = read_ok(&yaml, "items").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
}

/// No-match selector: typed, naming the path and the response shape.
#[tokio::test]
async fn selector_no_match_is_typed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "meta": 1, "rows": []
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(&server.uri(), "items", "/items", "records_path: data.items");
    let err = read_err(&yaml, "items").await;
    assert!(err.contains("data.items") && err.contains("meta"), "{err}");
}

/// Invalid selector syntax fails AT CONFIG PARSE, naming the subset.
#[tokio::test]
async fn invalid_selector_fails_at_parse() {
    let err = rdlt_connector_rest::source::Config::from_yaml(&stream_yaml(
        "http://x",
        "a",
        "/a",
        r#"records_path: "data[x]""#,
    ))
    .expect_err("bad selector")
    .to_string();
    assert!(err.contains("records_path") && err.contains("[*]"), "{err}");
}

/// 404 → end_stream: declared action; totals = what arrived before.
#[tokio::test]
async fn action_404_end_stream() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "response_actions:\n  - {status: 404, action: end_stream}",
    );
    let rows = read_ok(&yaml, "items").await;
    assert!(rows.is_empty(), "clean end, zero rows");
}

/// Undeclared 4xx stays a typed error (allow-list posture).
#[tokio::test]
async fn undeclared_4xx_stays_typed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "response_actions:\n  - {status: 404, action: end_stream}",
    );
    let err = read_err(&yaml, "items").await;
    assert!(err.contains("403"), "{err}");
}

/// content_contains → ignore: matching page treated as empty; pagination
/// still terminates per its family.
#[tokio::test]
async fn action_content_ignore() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "warning": "quota_soft_limit", "data": [{"id": 1}]
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data\nresponse_actions:\n  - {content_contains: quota_soft_limit, action: ignore}",
    );
    let rows = read_ok(&yaml, "items").await;
    assert!(rows.is_empty(), "ignored page contributes nothing");
}

/// Unconditional actions are rejected at parse (they'd swallow everything).
#[tokio::test]
async fn unconditional_action_rejected_at_parse() {
    let err = rdlt_connector_rest::source::Config::from_yaml(&stream_yaml(
        "http://x",
        "a",
        "/a",
        "response_actions:\n  - {action: ignore}",
    ))
    .expect_err("unconditional")
    .to_string();
    assert!(err.contains("response_actions[0]"), "{err}");
}

/// POST body template + body-merged cursor pagination (the search-endpoint
/// pattern).
#[tokio::test]
async fn post_body_with_cursor_pagination() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .and(body_partial_json(json!({"query": "x", "cursor": "c2"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hits": [{"id": 2}], "next": null
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .and(body_partial_json(json!({"query": "x"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hits": [{"id": 1}], "next": "c2"
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "hits",
        "/search",
        "method: post\nbody: {query: x}\nrecords_path: hits\npagination: {type: cursor, cursor_path: next, cursor_param: cursor}",
    );
    let rows = read_ok(&yaml, "hits").await;
    assert_eq!(rows.len(), 2);
}

/// body without method: post is typed at parse.
#[tokio::test]
async fn body_requires_post() {
    let err = rdlt_connector_rest::source::Config::from_yaml(&stream_yaml(
        "http://x",
        "a",
        "/a",
        "body: {q: 1}",
    ))
    .expect_err("body on GET")
    .to_string();
    assert!(err.contains("method: post"), "{err}");
}

/// Incremental aliases and the block are mutually exclusive (typed).
#[tokio::test]
async fn incremental_block_and_aliases_are_exclusive() {
    let err = rdlt_connector_rest::source::Config::from_yaml(&stream_yaml(
        "http://x",
        "a",
        "/a",
        "cursor_field: seq\ncursor_param: since\nincremental: {cursor_field: seq}",
    ))
    .expect_err("mixed")
    .to_string();
    assert!(err.contains("not both"), "{err}");
}

/// A wildcard records_path over a terminal EMPTY page ends cleanly — the
/// standard page-family termination, never a "matched nothing" error.
#[tokio::test]
async fn wildcard_selector_terminates_on_empty_page() {
    use wiremock::matchers::query_param;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"items": [{"payload": {"id": 1}}, {"payload": {"id": 2}}]}
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {"items": []}
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data.items[*].payload\npagination: {type: page}",
    );
    let rows = read_ok(&yaml, "items").await;
    assert_eq!(rows.len(), 2, "clean end at the empty terminal page");
}

/// content_contains + `action: error` is FATAL — never a silent clean end.
#[tokio::test]
async fn action_content_error_is_fatal() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "error": "internal_error", "data": []
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data\nresponse_actions:\n  - {content_contains: internal_error, action: error}",
    );
    let err = read_err(&yaml, "items").await;
    assert!(err.contains("declared error action matched"), "{err}");
}

/// `ignore` composes with body-driven pagination: an ignored mid-chain page
/// still carries the cursor forward; an ignored final page ends the chain.
#[tokio::test]
async fn action_ignore_rides_cursor_pagination() {
    use wiremock::matchers::query_param;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("c", "c3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 3}], "next": null
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("c", "c2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "warning": "maintenance_window", "data": [{"id": 99}], "next": "c3"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}], "next": "c2"
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data\npagination: {type: cursor, cursor_path: next, cursor_param: c}\nresponse_actions:\n  - {content_contains: maintenance_window, action: ignore}",
    );
    let rows = read_ok(&yaml, "items").await;
    assert_eq!(
        rows.len(),
        2,
        "pages 1 and 3; the ignored page 2 contributes nothing"
    );
}

/// A declared status action matches the TYPED status — including 429, whose
/// classified error does not render the status text.
#[tokio::test]
async fn action_429_end_stream() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "response_actions:\n  - {status: 429, action: end_stream}",
    );
    let rows = read_ok(&yaml, "items").await;
    assert!(rows.is_empty(), "declared 429 ends cleanly");
}

/// status AND content_contains on one action: both must hold (the error
/// body is read for the match); a non-matching body keeps the typed error.
#[tokio::test]
async fn action_status_and_content_combined() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(404).set_body_string("resource gone"))
        .mount(&server)
        .await;
    let extra = "response_actions:\n  - {status: 404, content_contains: gone, action: end_stream}";
    let rows = read_ok(
        &stream_yaml(&server.uri(), "items", "/items", extra),
        "items",
    )
    .await;
    assert!(rows.is_empty(), "404 + matching body ends cleanly");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(404).set_body_string("something else"))
        .mount(&server)
        .await;
    let err = read_err(
        &stream_yaml(&server.uri(), "items", "/items", extra),
        "items",
    )
    .await;
    assert!(
        err.contains("404"),
        "non-matching body keeps the typed 404: {err}"
    );
}

/// Declared statuses must be real HTTP statuses (typo net).
#[tokio::test]
async fn action_status_out_of_range_rejected_at_parse() {
    let err = rdlt_connector_rest::source::Config::from_yaml(&stream_yaml(
        "http://x",
        "a",
        "/a",
        "response_actions:\n  - {status: 42, action: end_stream}",
    ))
    .expect_err("status 42")
    .to_string();
    assert!(err.contains("not an HTTP status"), "{err}");
}

/// The remaining validation arms, and the JSON text entry point rides the
/// same validation: alias halves set together, empty incremental
/// cursor_field, both total stops declared.
#[tokio::test]
async fn validation_matrix_covers_remaining_arms() {
    for (frag, needle) in [
        ("cursor_field: seq", "set together"),
        ("incremental: {cursor_field: \" \"}", "must not be empty"),
        (
            "pagination: {type: page, total_pages_path: meta.pages, total_count_path: meta.total}",
            "pick one stop condition",
        ),
    ] {
        let err = rdlt_connector_rest::source::Config::from_yaml(&stream_yaml(
            "http://x", "a", "/a", frag,
        ))
        .expect_err(needle)
        .to_string();
        assert!(err.contains(needle), "expected `{needle}` in: {err}");
    }
    // from_json: same document shape, same validation.
    rdlt_connector_rest::source::Config::from_json(
        r#"{"base_url": "http://x", "streams": [{"name": "a", "path": "/a"}]}"#,
    )
    .expect("valid JSON document parses");
    let err = rdlt_connector_rest::source::Config::from_json(
        r#"{"base_url": "http://x", "streams": []}"#,
    )
    .expect_err("empty streams via JSON")
    .to_string();
    assert!(err.contains("at least one stream"), "{err}");
}

/// end_param requires end_value (closed windows are explicit).
#[tokio::test]
async fn end_param_requires_end_value() {
    let err = rdlt_connector_rest::source::Config::from_yaml(&stream_yaml(
        "http://x",
        "a",
        "/a",
        "incremental: {cursor_field: seq, start_param: since, end_param: until}",
    ))
    .expect_err("end without value")
    .to_string();
    assert!(err.contains("end_value"), "{err}");
}

/// The incremental block binds start AND end params on requests.
#[tokio::test]
async fn incremental_start_and_end_params_bind() {
    use wiremock::matchers::query_param;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("since", "5"))
        .and(query_param("until", "9"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"seq": 7}])))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        r#"incremental: {cursor_field: seq, start_param: since, end_param: until, end_value: "9"}"#,
    );
    let outcome = read_stream(
        &yaml,
        "items",
        Some(rdlt_connector_sdk::spi::core::cursor::Cursor::new("5")),
    )
    .await;
    outcome.result.expect("windowed read");
    assert_eq!(outcome.rows.len(), 1);
    assert_eq!(
        outcome.checkpoints.last(),
        Some(&rdlt_connector_sdk::spi::core::cursor::Cursor::new("7"))
    );
}
