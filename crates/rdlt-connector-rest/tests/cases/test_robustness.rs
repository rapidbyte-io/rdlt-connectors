//! A stalled server must not hang the pipeline, and a pagination configuration
//! that cannot advance must be refused before any request is sent.
//!
//! An unbounded wait is the worst failure a scheduled pipeline can have: it
//! consumes the slot and produces no signal at all — no error to classify, no
//! retry budget engaged, nothing for an operator to act on.

use std::time::Duration;

use rdlt_connector_rest::source::Config;
use rdlt_connector_rest::source::Shell;
use rdlt_connector_sdk::spi::{
    channel::records, error::SourceError, source::ReadRequest, source::Source,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A server that accepts the connection and then never answers.
async fn stalling_server(delay: Duration) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(delay)
                .set_body_json(json!([])),
        )
        .mount(&server)
        .await;
    server
}

async fn read_to_end(source: Shell) -> Result<(), SourceError> {
    let (out, mut input) = records(1 << 20);
    let spec = source.streams().await?[0].clone();
    let read = tokio::spawn(async move { source.read(ReadRequest::new(spec, None, out)).await });
    while input.recv().await.is_some() {}
    read.await.expect("join")
}

/// The read fails with a typed error the engine can classify, rather than
/// waiting forever. The test's own bound is deliberately far longer than the
/// configured timeout: a red run must terminate, not hang the suite.
#[tokio::test]
async fn a_stalled_server_fails_typed_instead_of_hanging() {
    let server = stalling_server(Duration::from_secs(60)).await;
    let source = Shell::from_yaml(&format!(
        "base_url: \"{}\"\nrequest_timeout_secs: 1\nstreams:\n  - name: events\n    path: /events\n",
        server.uri()
    ))
    .expect("config");

    let outcome = tokio::time::timeout(Duration::from_secs(20), read_to_end(source))
        .await
        .expect("the read must return on its own, not be cut off by the test");

    let error = outcome.expect_err("a stalled server is a failure, not a silent empty read");
    assert!(
        matches!(error, SourceError::Transient { .. }),
        "a timeout is transient — the engine's retry budget should get a turn: {error:?}"
    );
}

/// Zero must not be a way to spell "no timeout": the guarantee is that NO
/// configuration produces an unbounded wait.
#[test]
fn a_zero_request_timeout_is_refused() {
    // Asserted through the CONFIG TYPE, not a rendered message. Matching the
    // message made this pass even with the whole field deleted, because
    // `deny_unknown_fields` echoes an unknown key straight back into its error.
    let accepted: Config = serde_yaml_ng::from_str(
        "base_url: \"http://127.0.0.1:1\"\nrequest_timeout_secs: 30\nstreams:\n  - name: events\n    path: /events\n",
    )
    .expect("a positive deadline parses");
    assert_eq!(
        accepted.request_timeout_secs, 30,
        "the field must exist and carry the configured value"
    );

    let defaulted: Config = serde_yaml_ng::from_str(
        "base_url: \"http://127.0.0.1:1\"\nstreams:\n  - name: events\n    path: /events\n",
    )
    .expect("the field defaults");
    assert!(
        defaulted.request_timeout_secs > 0,
        "an unconfigured source still gets a deadline"
    );

    Shell::from_yaml(
        "base_url: \"http://127.0.0.1:1\"\nrequest_timeout_secs: 0\nstreams:\n  - name: events\n    path: /events\n",
    )
    .expect_err("0 must not disable the bound");
}

/// Page parameters reach a POST request through its body, and only a keyed
/// document has somewhere to put them. With any other body every page sends the
/// byte-identical request: the duplicate-request guard cannot see it (it hashes
/// the page parameters, which do change), so the run ingests the first page
/// `max_pages` times and then fails citing a page limit that was never the
/// problem. Refused at configuration time, before a single request.
#[test]
fn post_pagination_with_a_non_object_body_is_refused_at_validation() {
    for body in ["[1, 2, 3]", "\"a string\"", "42"] {
        let error = Shell::from_yaml(&format!(
            "base_url: \"http://127.0.0.1:1\"\nstreams:\n  - name: events\n    path: /events\n    \
             method: post\n    body: {body}\n    pagination:\n      type: page\n"
        ))
        .expect_err("page params cannot reach a non-object body");
        let rendered = error.to_string();
        assert!(
            rendered.contains("events"),
            "the refusal names the stream: {rendered}"
        );
    }
}

/// The same body shapes are fine WITHOUT pagination — nothing needs to reach
/// them — so the refusal must be about the combination, not the body.
#[test]
fn a_non_object_body_without_pagination_is_accepted() {
    Shell::from_yaml(
        "base_url: \"http://127.0.0.1:1\"\nstreams:\n  - name: events\n    path: /events\n    \
         method: post\n    body: [1, 2, 3]\n",
    )
    .expect("a non-object body is only a problem when page params must reach it");
}

/// Server pacing arrives in either form RFC 9110 allows. Reading only
/// delta-seconds silently discards a date-form instruction, and the source then
/// paces itself worse than the server asked for.
#[tokio::test]
async fn a_retry_after_http_date_is_honoured_like_delta_seconds() {
    let server = MockServer::start().await;
    // `fmt_http_date` truncates to whole seconds, so `now + 1s` leaves a margin
    // uniformly distributed over (0s, 1s] — near zero the header is already in
    // the past by the time the request goes out, and the test blames the source
    // for a header that had expired. A wide margin removes the race; the wait is
    // observed rather than waited out.
    let at = httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(30));
    Mock::given(method("GET"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", at.as_str()))
        .mount(&server)
        .await;

    let source = Shell::from_yaml(&format!(
        "base_url: \"{}\"\nretry_after_cap_secs: 1\nstreams:\n  - name: events\n    path: /events\n",
        server.uri()
    ))
    .expect("config");

    // The date-form wait (30s) exceeds the in-source cap (1s), so the source
    // declines to wait and surfaces the instruction to the engine — carrying the
    // SERVER's window, not the cap. Reading only delta-seconds would surface
    // `None` here and the engine would fall back to its own short backoff.
    let error = read_to_end(source).await.expect_err("429 surfaces");
    match error {
        SourceError::RateLimited { retry_after, .. } => {
            let wait = retry_after.expect("the date form must be read, not discarded");
            assert!(
                wait > Duration::from_secs(20),
                "the server's own window is reported, unclamped: {wait:?}"
            );
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
}

/// A credential belongs under `auth:` (Secret-wrapped and masked), never in a
/// plain `headers:` map that renders verbatim in Debug output.
#[test]
fn credential_bearing_headers_are_refused() {
    for name in [
        "Authorization",
        "proxy-authorization",
        "Cookie",
        "X-Api-Key",
        "apikey",
        "x-amz-security-token",
        "private-token",
    ] {
        let error = Shell::from_yaml(&format!(
            "base_url: \"http://127.0.0.1:1\"\nheaders:\n  {name}: \"secret\"\nstreams:\n  - name: events\n    path: /events\n"
        ))
        .unwrap_err();
        assert!(
            error.to_string().contains(name),
            "`{name}` must be refused by name: {error}"
        );
    }
    // A header that merely LOOKS similar is not a credential and must be allowed —
    // a guard that fires on innocent configuration gets disabled, not heeded.
    Shell::from_yaml(
        "base_url: \"http://127.0.0.1:1\"\nheaders:\n  x-request-token-count: \"5\"\nstreams:\n  - name: events\n    path: /events\n",
    )
    .expect("an exact-name rule must not reject look-alikes");
}

/// The encoding must be applied AT THE CALL SITE, not merely available. The
/// unit test on the encoder passes whether or not the driver calls it, so this
/// drives the real child-request path and asserts on the URL the server actually
/// received.
#[tokio::test]
async fn a_parent_value_reaches_the_server_as_a_single_path_segment() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"name": "../admin"}])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;

    let source = Shell::from_yaml(&format!(
        r#"
base_url: "{}"
streams:
  - name: repos
    path: /repos
  - name: issues
    path: /repos/{{{{repo}}}}/issues
    parent:
      stream: repos
      placeholders: {{repo: name}}
"#,
        server.uri()
    ))
    .expect("config");

    let specs = source.streams().await.expect("streams");
    let issues = specs
        .iter()
        .find(|s| s.name.as_str() == "issues")
        .expect("child stream")
        .clone();
    let (out, mut input) = records(1 << 20);
    let read = tokio::spawn(async move { source.read(ReadRequest::new(issues, None, out)).await });
    while input.recv().await.is_some() {}
    read.await.expect("join").expect("child read");

    let child = server
        .received_requests()
        .await
        .expect("recorded")
        .into_iter()
        .map(|r| r.url.path().to_owned())
        .find(|p| p != "/repos")
        .expect("the child request was sent");
    assert!(
        !child.starts_with("/admin"),
        "a parent value must not walk the request up a path segment: {child}"
    );
    assert!(
        child.contains("%2F") || child.contains("..%2"),
        "the value is encoded into ONE segment: {child}"
    );
}
