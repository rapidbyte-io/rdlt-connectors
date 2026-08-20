//! Feature 014 US1 (T006): auth schemes — attachment, the OAuth2 token
//! lifecycle, and the secret grep-proof (contracts RS3/RS4).

use super::common::{read_ok, read_stream};
use rdlt_connector_sdk::config::Document;
use serde_json::json;
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn one_item_stream(server: &MockServer, extra: &str) -> String {
    format!(
        r#"
base_url: "{}"
{extra}
streams:
  - name: items
    path: /items
"#,
        server.uri()
    )
}

/// api_key in a header and in a query param.
#[tokio::test]
async fn api_key_attaches_header_and_query() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("x-api-key", "k-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    let yaml = one_item_stream(&server, "auth: {api_key: {name: x-api-key, key: k-123}}");
    assert_eq!(read_ok(&yaml, "items").await.len(), 1);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(query_param("api_key", "k-456"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    let yaml = one_item_stream(
        &server,
        "auth: {api_key: {name: api_key, key: k-456, location: query}}",
    );
    assert_eq!(read_ok(&yaml, "items").await.len(), 1);
}

/// RS6: the pre-014 YAML tagged spelling (`auth: !bearer` — the only YAML
/// form the pre-014 crate accepted) still parses, and still attaches.
#[tokio::test]
async fn pre_014_tagged_auth_spelling_parses_and_attaches() {
    for yaml in [
        "base_url: http://x\nauth: !bearer\n  token: t\nstreams:\n  - name: a\n    path: /a\n",
        "base_url: http://x\nauth: !basic\n  username: u\n  password: p\nstreams:\n  - name: a\n    path: /a\n",
        "base_url: http://x\nauth: !header\n  name: x-k\n  value: v\nstreams:\n  - name: a\n    path: /a\n",
    ] {
        rdlt_connector_rest::source::Config::from_yaml(yaml)
            .unwrap_or_else(|e| panic!("pre-014 tagged spelling must parse: {e}\n{yaml}"));
    }
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Bearer tok-old"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    let yaml = format!(
        "base_url: \"{}\"\nauth: !bearer\n  token: tok-old\nstreams:\n  - name: items\n    path: /items\n",
        server.uri()
    );
    assert_eq!(read_ok(&yaml, "items").await.len(), 1);
}

/// basic and arbitrary-header schemes attach their exact values.
#[tokio::test]
async fn basic_and_header_auth_attach() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Basic dTpwdw==")) // "u:pw"
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    let yaml = one_item_stream(&server, "auth: {basic: {username: u, password: pw}}");
    assert_eq!(read_ok(&yaml, "items").await.len(), 1);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("x-auth", "v-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    let yaml = one_item_stream(&server, "auth: {header: {name: x-auth, value: v-1}}");
    assert_eq!(read_ok(&yaml, "items").await.len(), 1);
}

/// OAuth2: lazy fetch, cache across pages, bearer attachment.
#[tokio::test]
async fn oauth2_fetches_once_and_caches() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=client_credentials"))
        .and(body_string_contains("client_id=cid"))
        .and(body_string_contains("scope=read+write"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "tok-oauth", "expires_in": 3600
        })))
        .expect(1) // cache proof: ONE fetch for two data requests
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Bearer tok-oauth"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Bearer tok-oauth"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{0}"
auth:
  oauth2_client_credentials:
    token_url: "{0}/token"
    client_id: cid
    client_secret: shh-secret
    scopes: [read, write]
streams:
  - name: items
    path: /items
    pagination: {{type: page}}
"#,
        server.uri()
    );
    assert_eq!(read_ok(&yaml, "items").await.len(), 1);
}

/// OAuth2: a 401 on the data request drops the cache, re-fetches ONCE, and a
/// second 401 is fatal (never a loop).
#[tokio::test]
async fn oauth2_refreshes_once_on_401_then_fatal() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "tok", "expires_in": 3600
        })))
        .expect(2) // initial + the one 401-triggered re-fetch
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{0}"
auth:
  oauth2_client_credentials:
    token_url: "{0}/token"
    client_id: cid
    client_secret: shh
streams:
  - name: items
    path: /items
"#,
        server.uri()
    );
    let outcome = read_stream(&yaml, "items", None).await;
    let err = outcome.result.expect_err("second 401 is fatal").to_string();
    assert!(err.contains("401"), "{err}");
}

/// OAuth2: a 5xx from the token endpoint is TRANSIENT (engine budget).
#[tokio::test]
async fn oauth2_token_endpoint_5xx_is_transient() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{0}"
auth:
  oauth2_client_credentials:
    token_url: "{0}/token"
    client_id: cid
    client_secret: shh
streams:
  - name: items
    path: /items
"#,
        server.uri()
    );
    let outcome = read_stream(&yaml, "items", None).await;
    match outcome.result {
        Err(rdlt_connector_sdk::spi::error::SourceError::Transient { .. }) => {}
        other => panic!("expected Transient, got {other:?}"),
    }
}

/// A rate-limited token endpoint classifies exactly like a rate-limited
/// data request: `RateLimited` carrying the server's Retry-After, for the
/// engine's backoff — NEVER fatal, which would abort the run over an
/// ordinary shared-issuer limit.
#[tokio::test]
async fn oauth2_token_endpoint_429_is_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "7"))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{0}"
auth:
  oauth2_client_credentials:
    token_url: "{0}/token"
    client_id: cid
    client_secret: shh
streams:
  - name: items
    path: /items
"#,
        server.uri()
    );
    let outcome = read_stream(&yaml, "items", None).await;
    match outcome.result {
        Err(rdlt_connector_sdk::spi::error::SourceError::RateLimited { retry_after, .. }) => {
            assert_eq!(
                retry_after,
                Some(std::time::Duration::from_secs(7)),
                "the issuer's Retry-After reaches the engine"
            );
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
}

/// Source-level headers and params ride every request, merged UNDER the
/// stream's own (same name → the stream's value wins, exactly once).
#[tokio::test]
async fn headers_and_params_merge_stream_over_source() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("x-source", "from-source"))
        .and(header("x-stream", "from-stream"))
        .and(header("x-shared", "stream-wins"))
        .and(query_param("api_version", "2024"))
        .and(query_param("scope", "narrow"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id": 1}])))
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
base_url: "{}"
headers: {{x-source: from-source, x-shared: source-loses}}
params: {{api_version: "2024", scope: broad}}
streams:
  - name: items
    path: /items
    headers: {{x-stream: from-stream, x-shared: stream-wins}}
    params: {{scope: narrow}}
"#,
        server.uri()
    );
    assert_eq!(read_ok(&yaml, "items").await.len(), 1);
}

/// RS4 grep-proof: secrets never render — config Debug, source Debug, and
/// every error path from a failing authenticated read.
#[tokio::test]
async fn secrets_never_render_anywhere() {
    const SECRETS: &[&str] = &[
        "hunter2-token",
        "hunter2-pass",
        "hunter2-key",
        "hunter2-client",
    ];
    let server = MockServer::start().await;
    // Everything 500s: error paths render, and must not leak.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    for auth in [
        "auth: {bearer: {token: hunter2-token}}",
        "auth: {basic: {username: u, password: hunter2-pass}}",
        "auth: {header: {name: x-auth, value: hunter2-key}}",
        "auth: {api_key: {name: k, key: hunter2-key}}",
        &format!(
            "auth: {{oauth2_client_credentials: {{token_url: \"{}/token\", client_id: cid, client_secret: hunter2-client}}}}",
            server.uri()
        ),
    ] {
        let yaml = one_item_stream(&server, auth);
        let config = rdlt_connector_rest::source::Config::from_yaml(&yaml).expect("parses");
        let rendered_config = format!("{config:?}");
        let source = rdlt_connector_rest::source::Shell::new(config).expect("valid config");
        let rendered_source = format!("{source:?}");
        let outcome = read_stream(&yaml, "items", None).await;
        let rendered_err = outcome.result.expect_err("500s fail").to_string();
        for secret in SECRETS {
            for (what, rendering) in [
                ("config Debug", &rendered_config),
                ("source Debug", &rendered_source),
                ("error", &rendered_err),
            ] {
                assert!(
                    !rendering.contains(secret),
                    "{what} leaked `{secret}` under {auth}: {rendering}"
                );
            }
        }
    }
}
