//! Feature 014 US1 (T004): the seven pagination families — correct records,
//! correct termination, loud loop guards (contract RS2).

use super::common::{read_err, read_ok, stream_yaml};
use rdlt_connector_sdk::config::Document;
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ids(rows: &[serde_json::Value]) -> Vec<i64> {
    rows.iter().map(|r| r["id"].as_i64().unwrap()).collect()
}

/// cursor: body cursor feeds the param; absent cursor terminates.
#[tokio::test]
async fn body_cursor_chains_and_terminates() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("cursor", "c2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 3}], "meta": {}
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}, {"id": 2}], "meta": {"next": "c2"}
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data\npagination: {type: cursor, cursor_path: meta.next, cursor_param: cursor}",
    );
    assert_eq!(ids(&read_ok(&yaml, "items").await), vec![1, 2, 3]);
}

/// header_cursor: response header feeds the param; absent header terminates.
#[tokio::test]
async fn header_cursor_chains_and_terminates() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("after", "tok-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 2}])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-next-cursor", "tok-2")
                .set_body_json(json!([{"id": 1}])),
        )
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "pagination: {type: header_cursor, header: x-next-cursor, cursor_param: after}",
    );
    assert_eq!(ids(&read_ok(&yaml, "items").await), vec![1, 2]);
}

/// next_url: body URL (absolute AND relative) chains; null terminates.
#[tokio::test]
async fn next_url_follows_absolute_and_relative() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": 1}],
            "next": format!("{}/items2", server.uri())
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": 2}],
            "next": "/items3"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": 3}],
            "next": null
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: results\npagination: {type: next_url, next_url_path: next}",
    );
    assert_eq!(ids(&read_ok(&yaml, "items").await), vec![1, 2, 3]);
}

/// link_header: RFC5988 rel=next chains; absent link terminates.
#[tokio::test]
async fn link_header_follows_rel_next() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("p", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 2}])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(
                    "link",
                    format!(
                        "<{}/items?p=2>; rel=\"next\", <{}/items?p=9>; rel=\"last\"",
                        server.uri(),
                        server.uri()
                    )
                    .as_str(),
                )
                .set_body_json(json!([{"id": 1}])),
        )
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "pagination: {type: link_header}",
    );
    assert_eq!(ids(&read_ok(&yaml, "items").await), vec![1, 2]);
}

/// page + total_pages_path: stops at the declared total WITHOUT an extra
/// empty-page request.
#[tokio::test]
async fn page_with_total_pages_stops_at_total() {
    let server = MockServer::start().await;
    for page in 1..=2 {
        Mock::given(method("GET"))
            .and(path("/items"))
            .and(query_param("page", page.to_string().as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": page}], "total_pages": 2
            })))
            .expect(1)
            .mount(&server)
            .await;
    }
    // NO page=3 mock: a third request would 404 and fail the read.
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data\npagination: {type: page, total_pages_path: total_pages}",
    );
    assert_eq!(ids(&read_ok(&yaml, "items").await), vec![1, 2]);
}

/// offset + total_count_path: stops at the declared record total.
#[tokio::test]
async fn offset_with_total_count_stops_at_total() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}, {"id": 2}], "total": 3
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 3}], "total": 3
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data\npagination: {type: offset, page_size: 2, total_count_path: total}",
    );
    // Page 2 is short (1 < 2) so termination also holds without the total;
    // the total makes it explicit.
    assert_eq!(ids(&read_ok(&yaml, "items").await), vec![1, 2, 3]);
}

/// RS2: a cursor that never advances is a typed error, not an infinite loop.
#[tokio::test]
async fn same_request_loop_guard_fires_typed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}], "next": "stuck"
        })))
        .mount(&server)
        .await;
    let yaml = stream_yaml(
        &server.uri(),
        "items",
        "/items",
        "records_path: data\npagination: {type: cursor, cursor_path: next, cursor_param: cursor}",
    );
    let err = read_err(&yaml, "items").await;
    assert!(
        err.contains("SAME request twice") && err.contains("items"),
        "{err}"
    );
}

/// RS2: max_pages bounds a pathological chain.
#[tokio::test]
async fn max_pages_guard_fires_typed() {
    let server = MockServer::start().await;
    // Every page returns a fresh cursor — endless chain.
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(|req: &wiremock::Request| {
            let n: u64 = req
                .url
                .query_pairs()
                .find(|(k, _)| k == "cursor")
                .map(|(_, v)| v.parse().unwrap_or(0))
                .unwrap_or(0);
            ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": n}], "next": (n + 1).to_string()
            }))
        })
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
max_pages: 5
streams:
  - name: items
    path: /items
    records_path: data
    pagination: {{type: cursor, cursor_path: next, cursor_param: cursor}}
"#,
        server.uri()
    );
    let err = read_err(&yaml, "items").await;
    assert!(err.contains("max_pages (5)"), "{err}");
}

/// Frozen spellings: the pre-014 page/offset documents parse identically.
#[tokio::test]
async fn pre_014_pagination_spellings_parse() {
    for yaml in [
        "base_url: http://x\nstreams:\n  - name: a\n    path: /a\n    pagination: {type: page}\n",
        "base_url: http://x\nstreams:\n  - name: a\n    path: /a\n    pagination: {type: offset, page_size: 10}\n",
        "base_url: http://x\nstreams:\n  - name: a\n    path: /a\n",
    ] {
        rdlt_connector_rest::source::Config::from_yaml(yaml).expect("pre-014 spelling parses");
    }
}

/// Auth still attaches on every paged request (regression net for the
/// client extraction) — bearer header asserted by the mock matcher.
#[tokio::test]
async fn auth_rides_every_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Bearer tok-9"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Bearer tok-9"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
auth: {{bearer: {{token: tok-9}}}}
streams:
  - name: items
    path: /items
    pagination: {{type: page}}
"#,
        server.uri()
    );
    assert_eq!(ids(&read_ok(&yaml, "items").await), vec![1]);
}
