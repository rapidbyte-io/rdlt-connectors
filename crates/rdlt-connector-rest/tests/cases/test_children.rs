//! Feature 014 US3 (T011/T012): parent-child composition + the composed
//! example connector — engine-driven (the real wiring, not just the source).

use super::common::read_err;
use rdlt_connector_duckdb::destination as duck;
use rdlt_connector_rest::source::Shell;
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::error::SourceError;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A nested mock API: /repos (2 repos, paginated) and per-repo /issues.
async fn nested_api() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"owner": "acme", "name": "one", "stars": 5},
            {"owner": "acme", "name": "two", "stars": 9}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;
    for (repo, issues) in [
        (
            "one",
            json!([{"id": 11, "title": "a"}, {"id": 12, "title": "b"}]),
        ),
        ("two", json!([{"id": 21, "title": "c"}])),
    ] {
        Mock::given(method("GET"))
            .and(path(format!("/repos/acme/{repo}/issues")))
            .respond_with(ResponseTemplate::new(200).set_body_json(issues))
            .mount(&server)
            .await;
    }
    server
}

fn nested_yaml(base: &str) -> String {
    format!(
        r#"
base_url: "{base}"
streams:
  - name: repos
    path: /repos
    pagination: {{type: page}}
  - name: issues
    path: /repos/{{owner}}/{{repo}}/issues
    parent:
      stream: repos
      placeholders: {{owner: owner, repo: name}}
      include: [name, stars]
"#
    )
}

/// The full engine path: parent fan-out, resolved paths, parent-field
/// embedding, exact totals in a real destination.
#[tokio::test(flavor = "multi_thread")]
async fn parent_child_reads_through_the_engine() {
    let server = nested_api().await;
    let source = Shell::from_yaml(&nested_yaml(&server.uri())).expect("config");
    let dir = tempfile::tempdir().unwrap();
    let dest_config = duck::Config::new(dir.path().join("nested.duckdb"));
    let dest = duck::Shell::new(dest_config.clone()).unwrap();
    let mut config = EngineConfig::new("nested");
    config = config.with_workdir(dir.path().join("work"));
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");

    assert_eq!(
        duck::testhook::count_rows(&dest_config, "repos").unwrap(),
        2
    );
    assert_eq!(
        duck::testhook::count_rows(&dest_config, "issues").unwrap(),
        3,
        "2 + 1 across parents"
    );
    // Parent fields embedded.
    assert_eq!(
        duck::testhook::query_string(
            &dest_config,
            "SELECT _parent_name FROM issues WHERE id = 21"
        )
        .unwrap(),
        "two"
    );
    assert_eq!(
        duck::testhook::query_string(
            &dest_config,
            "SELECT count(*)::VARCHAR FROM issues WHERE _parent_stars = 5"
        )
        .unwrap(),
        "2",
        "both issues of repo `one` carry its stars"
    );
}

/// Child failure names the parent's resolved values (RS7).
#[tokio::test]
async fn child_failure_names_resolved_values() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"owner": "acme", "name": "ghost"}
        ])))
        .mount(&server)
        .await;
    // No /repos/acme/ghost/issues mock → 404.
    let yaml = format!(
        r#"
base_url: "{}"
streams:
  - name: repos
    path: /repos
  - name: issues
    path: /repos/{{owner}}/{{repo}}/issues
    parent:
      stream: repos
      placeholders: {{owner: owner, repo: name}}
"#,
        server.uri()
    );
    let err = read_err(&yaml, "issues").await;
    assert!(
        err.contains("owner=acme") && err.contains("repo=ghost"),
        "{err}"
    );
}

/// A retryable child failure keeps its classification through the parent
/// fan-out: a 500 stays Transient and a 429 stays RateLimited, so the run
/// consumes its retry budget instead of aborting. The parent context is
/// still attached to the surfaced error.
#[tokio::test]
async fn child_retryable_failure_keeps_classification() {
    for (status, expect_rate_limited) in [(500u16, false), (429u16, true)] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"owner": "acme", "name": "ghost"}
            ])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/ghost/issues"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let yaml = format!(
            r#"
base_url: "{}"
streams:
  - name: repos
    path: /repos
  - name: issues
    path: /repos/{{owner}}/{{repo}}/issues
    parent:
      stream: repos
      placeholders: {{owner: owner, repo: name}}
"#,
            server.uri()
        );
        let outcome = super::common::read_stream(&yaml, "issues", None).await;
        let err = outcome.result.expect_err("child failure surfaces typed");
        match &err {
            SourceError::RateLimited { .. } => {
                assert!(expect_rate_limited, "HTTP {status} → RateLimited: {err:?}")
            }
            SourceError::Transient(_) => {
                assert!(!expect_rate_limited, "HTTP {status} → Transient: {err:?}")
            }
            other => panic!("HTTP {status} child failure must stay retryable, got {other:?}"),
        }
        let chain = error_chain(&err);
        assert!(
            chain.contains("owner=acme") && chain.contains("repo=ghost"),
            "parent context lost: {chain}"
        );
    }
}

/// Render an error and its full `source` chain (the `RateLimited` variant
/// keeps its detail in the source, not the top-level Display).
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut rendered = err.to_string();
    let mut source = err.source();
    while let Some(next) = source {
        rendered.push_str(": ");
        rendered.push_str(&next.to_string());
        source = next.source();
    }
    rendered
}

/// Parent validation matrix: undeclared parent, self-parent, nested child,
/// unused placeholder, placeholder without parent.
#[tokio::test]
async fn parent_validation_matrix() {
    for (yaml_frag, needle) in [
        (
            "  - name: c\n    path: /c/{x}\n    parent: {stream: ghost, placeholders: {x: id}}\n",
            "not a declared stream",
        ),
        (
            "  - name: c\n    path: /c/{x}\n    parent: {stream: c, placeholders: {x: id}}\n",
            "parent.stream is itself",
        ),
        (
            "  - name: c\n    path: /c/{x}\n    parent: {stream: a, placeholders: {y: id}}\n",
            "never used",
        ),
        ("  - name: c\n    path: /c/{x}\n", "no `parent` block"),
        (
            "  - name: b\n    path: /b/{y}\n    parent: {stream: a, placeholders: {y: id}}\n  - name: c\n    path: /c/{x}\n    parent: {stream: b, placeholders: {x: id}}\n",
            "itself a child",
        ),
        (
            "  - name: c\n    path: /c\n    parent: {stream: a, placeholders: {}}\n",
            "placeholders must not be empty",
        ),
    ] {
        let yaml = format!("base_url: http://x\nstreams:\n  - name: a\n    path: /a\n{yaml_frag}");
        let err = rdlt_connector_rest::source::Config::from_yaml(&yaml)
            .expect_err(needle)
            .to_string();
        assert!(err.contains(needle), "expected `{needle}` in: {err}");
    }
}

/// T012: the composed example connector (public pieces only) runs against
/// the same nested API through the engine — the US3 seam proof.
#[tokio::test(flavor = "multi_thread")]
async fn composed_example_runs_through_the_engine() {
    let server = nested_api().await;
    let source = composed_api::build(&server.uri()).expect("composed connector builds");
    let dir = tempfile::tempdir().unwrap();
    let dest_config = duck::Config::new(dir.path().join("composed.duckdb"));
    let dest = duck::Shell::new(dest_config.clone()).unwrap();
    let mut config = EngineConfig::new("composed");
    config = config.with_workdir(dir.path().join("work"));
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(
        duck::testhook::count_rows(&dest_config, "repos").unwrap(),
        2
    );
    assert_eq!(
        duck::testhook::count_rows(&dest_config, "issues").unwrap(),
        3
    );
}

/// `max_concurrency` observably overlaps child sequences (R5): three
/// children whose responses each take 400ms complete well under the
/// sequential floor of 1200ms — with exact totals and embedding intact.
#[tokio::test(flavor = "multi_thread")]
async fn children_fan_out_concurrently_within_the_limit() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"name": "one"}, {"name": "two"}, {"name": "three"}
        ])))
        .mount(&server)
        .await;
    for repo in ["one", "two", "three"] {
        Mock::given(method("GET"))
            .and(path(format!("/repos/{repo}/issues")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!([{"id": repo}]))
                    .set_delay(std::time::Duration::from_millis(400)),
            )
            .mount(&server)
            .await;
    }
    let yaml = format!(
        r#"
base_url: "{}"
max_concurrency: 3
streams:
  - name: repos
    path: /repos
  - name: issues
    path: /repos/{{repo}}/issues
    parent:
      stream: repos
      placeholders: {{repo: name}}
"#,
        server.uri()
    );
    let started = std::time::Instant::now();
    let rows = super::common::read_ok(&yaml, "issues").await;
    let elapsed = started.elapsed();
    assert_eq!(rows.len(), 3);
    assert!(
        elapsed < std::time::Duration::from_millis(1100),
        "three 400ms children under max_concurrency=3 must overlap \
         (sequential floor is 1200ms), took {elapsed:?}"
    );
}

/// `max_concurrency: 0` is rejected at parse.
#[tokio::test]
async fn zero_max_concurrency_rejected_at_parse() {
    let err = rdlt_connector_rest::source::Config::from_yaml(
        "base_url: http://x\nmax_concurrency: 0\nstreams:\n  - name: a\n    path: /a\n",
    )
    .expect_err("zero concurrency")
    .to_string();
    assert!(err.contains("max_concurrency"), "{err}");
}

/// The example "named API connector": a config GENERATOR over the public
/// surface — the shape a Google-Search-Console-style wrapper takes. No raw
/// HTTP anywhere; auth/pagination/extraction/engine wiring all inherited.
mod composed_api {
    use rdlt_connector_rest::source::{Config, Shell};
    use rdlt_connector_sdk::config::Document;

    /// "MiniHub": repos + their issues, as a one-call constructor.
    pub fn build(base_url: &str) -> Result<Shell, Box<dyn std::error::Error>> {
        let config = Config::from_value(serde_json::json!({
            "base_url": base_url,
            "streams": [
                {"name": "repos", "path": "/repos", "pagination": {"type": "page"}},
                {"name": "issues", "path": "/repos/{owner}/{repo}/issues",
                 "parent": {"stream": "repos",
                             "placeholders": {"owner": "owner", "repo": "name"},
                             "include": ["name"]}}
            ]
        }))?;
        Ok(Shell::new(config)?)
    }
}
