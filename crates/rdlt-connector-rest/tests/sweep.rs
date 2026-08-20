//! Feature 014 US2 (T010): crash sweep over the REST read path — every fail
//! point × action, armed-fire pinned, crash/rerun exactly-once through the
//! engine (contract RS7). One test fn: fail points are process-global.
//!
//! Run with `--features failpoints` (the `make test TARGET=sweep` gate).

#![cfg(feature = "failpoints")]

use std::path::Path;

use rdlt_connector_duckdb::destination as duck;
use rdlt_connector_rest::source::Shell;
use rdlt_connector_sdk::spi::core::failpoint::fail;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use serde_json::json;
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOTAL_ROWS: u64 = 4;

/// A cursor-honoring paged API: rows seq 1..=4, page size 2, server-side
/// `since` filtering — what makes the engine's resume law hold.
async fn mock_api() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/events"))
        .respond_with(|req: &wiremock::Request| {
            let get = |k: &str| {
                req.url
                    .query_pairs()
                    .find(|(key, _)| key == k)
                    .map(|(_, v)| v.to_string())
            };
            let since: i64 = get("since").and_then(|v| v.parse().ok()).unwrap_or(0);
            let page: usize = get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
            let remaining: Vec<i64> = (1..=TOTAL_ROWS as i64).filter(|s| *s > since).collect();
            let start = (page - 1) * 2;
            let slice: Vec<serde_json::Value> = remaining
                .iter()
                .skip(start)
                .take(2)
                .map(|seq| json!({"seq": seq, "v": format!("row-{seq}")}))
                .collect();
            ResponseTemplate::new(200).set_body_json(slice)
        })
        .mount(&server)
        .await;
    server
}

fn yaml(base: &str) -> String {
    format!(
        r#"
base_url: "{base}"
streams:
  - name: events
    path: /events
    pagination: {{type: page}}
    incremental: {{cursor_field: seq, start_param: since}}
    primary_key: [seq]
"#
    )
}

async fn attempt(workdir: &Path, db: &Path, base: &str) -> Result<(), String> {
    let source = Shell::from_yaml(&yaml(base)).expect("config");
    let dest = duck::Shell::new(duck::Config::new(db)).map_err(|e| e.to_string())?;
    let mut config = EngineConfig::new("rest-sweep");
    config = config.with_workdir(workdir.to_path_buf());
    let engine = Engine::new(config, source, dest);
    match tokio::spawn(engine.run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rest_read_path_survives_crash_sweep() {
    let server = mock_api().await;
    let base = server.uri();

    let mut fired: std::collections::BTreeSet<(&str, &str)> = std::collections::BTreeSet::new();
    for &point in rdlt_connector_rest::source::FAIL_POINTS {
        for action in ["return", "panic", "1*off->return"] {
            let dir = tempfile::tempdir().expect("tempdir");
            let workdir = dir.path().join("wal");
            let db = dir.path().join("sweep.duckdb");

            fail::cfg(point, action).expect("configure fail point");
            let armed1 = attempt(&workdir, &db, &base).await;
            // Second run still armed: a crash during recovery itself.
            let armed2 = attempt(&workdir, &db, &base).await;
            fail::remove(point);
            if armed1.is_err() || armed2.is_err() {
                fired.insert((point, action));
            }

            let recovered = attempt(&workdir, &db, &base).await;
            assert!(
                recovered.is_ok(),
                "[{point} / {action}] recovery failed: {recovered:?}"
            );
            assert_eq!(
                duck::testhook::count_rows(&duck::Config::new(&db), "events").unwrap(),
                TOTAL_ROWS,
                "[{point} / {action}] exactly-once violated"
            );
        }
    }

    // Armed-fire pin: every (point, action) must actually fire — a missing
    // entry means a crash_point! site went dead (vacuous sweep).
    let mut expected: std::collections::BTreeSet<(&str, &str)> = std::collections::BTreeSet::new();
    for &point in rdlt_connector_rest::source::FAIL_POINTS {
        for action in ["return", "panic", "1*off->return"] {
            expected.insert((point, action));
        }
    }
    assert_eq!(fired, expected, "armed-fire pin diverged");
}

/// The registry names exactly the crash points armed in this crate's sources.
///
/// The sweep's own `fired == registry` check cannot establish this: it compares
/// the registry against itself, so deleting a point from BOTH the code and the
/// list leaves it true while the matrix quietly shrinks. This reads the sources
/// instead, which is the only way a dropped point becomes visible.
///
/// One registry here, so the union is itself.
#[test]
fn the_registry_matches_the_sources() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::scanner::assert_registry_matches_sources(
        &src,
        &[rdlt_connector_rest::source::FAIL_POINTS],
    );
}
