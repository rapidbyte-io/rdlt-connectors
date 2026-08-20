//! T055: REST source against a mock API — pagination, resume, error classification —
//! plus the public source conformance suite (the certification gate).

use rdlt_connector_rest::source::Shell;
use rdlt_connector_sdk::spi::{
    channel::records, core::cursor::Cursor, error::SourceError, source::ReadRequest, source::Source,
};
use rdlt_testkit::conformance::assert_conformant;
use rdlt_testkit::conformance::source::verify as verify_source;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config_yaml(base_url: &str) -> String {
    format!(
        r#"
base_url: "{base_url}"
streams:
  - name: events
    path: /events
    pagination:
      type: page
    cursor_field: seq
    cursor_param: since
    primary_key: [seq]
"#
    )
}

/// Mock a cursor-aware paginated API: rows 1..=3 across two pages; `since` filters
/// server-side (what makes the resume law hold for a real API).
async fn mock_api() -> MockServer {
    let server = MockServer::start().await;
    // Resume mocks FIRST: wiremock is first-match-wins, and these are more specific
    // (the API filters server-side by `since` — what makes the resume law hold).
    for (since, rows) in [("2", json!([{"seq": 3, "v": "c"}])), ("3", json!([]))] {
        Mock::given(method("GET"))
            .and(path("/events"))
            .and(query_param("since", since))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(rows))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/events"))
            .and(query_param("since", since))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .mount(&server)
            .await;
    }
    // Full reads (no `since`).
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"seq": 1, "v": "a"}, {"seq": 2, "v": "b"}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"seq": 3, "v": "c"}])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("page", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn paginates_and_checkpoints_max_cursor() {
    let server = mock_api().await;
    let source = Shell::from_yaml(&config_yaml(&server.uri())).expect("config");
    let (out, mut input) = records(1 << 20);
    let spec = source.streams().await.expect("streams")[0].clone();
    let read = tokio::spawn(async move { source.read(ReadRequest::new(spec, None, out)).await });

    let mut rows = 0usize;
    let mut checkpoints = Vec::new();
    while let Some(push) = input.recv().await {
        match push.payload {
            rdlt_connector_sdk::spi::channel::PushPayload::RawJson(bytes) => {
                let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
                rows += v.as_array().expect("array").len();
            }
            rdlt_connector_sdk::spi::channel::PushPayload::Checkpoint(c) => checkpoints.push(c),
            other => panic!("unexpected push {other:?}"),
        }
    }
    read.await.expect("join").expect("read ok");
    assert_eq!(rows, 3, "both pages consumed");
    assert_eq!(
        checkpoints.last(),
        Some(&Cursor::new("3")),
        "max cursor checkpointed"
    );
}

#[tokio::test]
async fn resume_sends_cursor_param_and_skips_completed_ranges() {
    let server = mock_api().await;
    let source = Shell::from_yaml(&config_yaml(&server.uri())).expect("config");
    let (out, mut input) = records(1 << 20);
    let spec = source.streams().await.expect("streams")[0].clone();
    let read = tokio::spawn(async move {
        source
            .read(ReadRequest::new(spec, Some(Cursor::new("2")), out))
            .await
    });
    let mut rows = Vec::new();
    while let Some(push) = input.recv().await {
        if let rdlt_connector_sdk::spi::channel::PushPayload::RawJson(bytes) = push.payload {
            let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
            rows.extend(v.as_array().expect("array").clone());
        }
    }
    read.await.expect("join").expect("read ok");
    assert_eq!(
        rows,
        vec![json!({"seq": 3, "v": "c"})],
        "only rows after the cursor"
    );
}

#[tokio::test]
async fn error_classification_matches_the_contract() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/limited"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "7"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/broken"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/forbidden"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    for (endpoint, want) in [
        ("limited", "rate"),
        ("broken", "transient"),
        ("forbidden", "fatal"),
    ] {
        let yaml = format!(
            "base_url: \"{}\"\nstreams:\n  - name: s\n    path: /{endpoint}\n",
            server.uri()
        );
        let source = Shell::from_yaml(&yaml).expect("config");
        let (out, _input) = records(1 << 20);
        let spec = source.streams().await.expect("streams")[0].clone();
        let err = source
            .read(ReadRequest::new(spec, None, out))
            .await
            .expect_err("must fail");
        match (want, &err) {
            ("rate", SourceError::RateLimited { retry_after, .. }) => {
                assert_eq!(*retry_after, Some(std::time::Duration::from_secs(7)));
            }
            ("transient", SourceError::Transient(_)) => {}
            ("fatal", SourceError::Fatal(_)) => {}
            (want, got) => panic!("{endpoint}: wanted {want}, got {got:?}"),
        }
    }
}

/// The certification gate: the REST source passes the public conformance suite
/// against a cursor-honoring API.
#[tokio::test]
async fn rest_source_is_conformant() {
    let server = mock_api().await;
    let source = Shell::from_yaml(&config_yaml(&server.uri())).expect("config");
    assert_conformant(verify_source(&source).await.expecting_no_skips());
}

/// Feature 014 US2 (T009): a 429 with Retry-After within the cap waits
/// in-source and then SUCCEEDS — the wait is real (elapsed proves it).
#[tokio::test]
async fn retry_after_within_cap_waits_and_succeeds() {
    let server = MockServer::start().await;
    // First call 429 (retry-after 1s), afterwards 200.
    Mock::given(method("GET"))
        .and(path("/flaky"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "1"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/flaky"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    let yaml = format!(
        "base_url: \"{}\"\nstreams:\n  - name: flaky\n    path: /flaky\n",
        server.uri()
    );
    let source = Shell::from_yaml(&yaml).expect("config");
    let (out, mut input) = records(1 << 20);
    let spec = source.streams().await.expect("streams")[0].clone();
    let started = std::time::Instant::now();
    let read = tokio::spawn(async move { source.read(ReadRequest::new(spec, None, out)).await });
    let mut rows = 0usize;
    while let Some(push) = input.recv().await {
        if let rdlt_connector_sdk::spi::channel::PushPayload::RawJson(bytes) = push.payload {
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            rows += v.as_array().unwrap().len();
        }
    }
    read.await.unwrap().expect("read succeeds after the wait");
    assert_eq!(rows, 1);
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(1),
        "the Retry-After wait was honored"
    );
}

/// T009: pacing — requests respect min_request_interval_ms (observed via
/// total elapsed across a 3-request page chain).
#[tokio::test]
async fn pacing_floor_is_observed() {
    let server = MockServer::start().await;
    for (page, rows) in [
        ("1", json!([{"id": 1}])),
        ("2", json!([{"id": 2}])),
        ("3", json!([])),
    ] {
        Mock::given(method("GET"))
            .and(path("/paced"))
            .and(query_param("page", page))
            .respond_with(ResponseTemplate::new(200).set_body_json(rows))
            .mount(&server)
            .await;
    }
    let yaml = format!(
        "base_url: \"{}\"\nmin_request_interval_ms: 200\nstreams:\n  - name: paced\n    path: /paced\n    pagination: {{type: page}}\n",
        server.uri()
    );
    let source = Shell::from_yaml(&yaml).expect("config");
    let (out, mut input) = records(1 << 20);
    let spec = source.streams().await.expect("streams")[0].clone();
    let started = std::time::Instant::now();
    let read = tokio::spawn(async move { source.read(ReadRequest::new(spec, None, out)).await });
    while input.recv().await.is_some() {}
    read.await.unwrap().expect("read ok");
    // 3 requests with a 200ms floor: >= 400ms between first and last sends.
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(400),
        "pacing floor not observed: {:?}",
        started.elapsed()
    );
}
