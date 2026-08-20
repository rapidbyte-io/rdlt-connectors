//! Credential provisioning, live: vending is the default path, an
//! explicit S3 override works, a WRONG override FAILS (never silently
//! ignored), and bearer auth talks to the catalog.

use rdlt_connector_iceberg::destination::Shell;
use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory::{Batch as MemoryBatch, Source as MemorySource, Stream as MemoryStream};
use serde_json::json;

use super::common::{CatalogFixture, S3_KEY, S3_SECRET};

fn one_row() -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1)],
    )])
}

async fn run(doc: serde_json::Value, pipeline: &str) -> Result<(), String> {
    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
        one_row(),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .map(|_| ())
    .map_err(|e| format!("{e}"))
}

/// The vended path (no storage block) is the default and works —
/// already proven by every other cell; here the OVERRIDE matrix.
#[tokio::test]
async fn the_override_matrix_works_and_wrong_credentials_fail() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    // Correct explicit override.
    let mut doc = fixture.doc("prov_ok_v2");
    doc["storage"] = json!({"s3": {
        "endpoint": fixture.s3_endpoint,
        "region": "us-east-1",
        "access_key": S3_KEY,
        "secret_key": S3_SECRET,
    }});
    run(doc, "ice-prov-ok")
        .await
        .expect("explicit override loads");

    // WRONG override: must FAIL — a destination that silently ignored
    // the override would be writing with credentials the operator
    // thinks are unused.
    let mut doc = fixture.doc("prov_bad_v2");
    doc["storage"] = json!({"s3": {
        "endpoint": fixture.s3_endpoint,
        "region": "us-east-1",
        "access_key": "wrong-key",
        "secret_key": "wrong-secret",
    }});
    let err = run(doc, "ice-prov-bad")
        .await
        .expect_err("wrong credentials must fail, never be ignored");
    assert!(
        err.contains("table `events`"),
        "the failure carries the table context, not a fixture accident: {err}"
    );
}

/// Bearer auth (the fixture's admin token) drives a load end to end.
#[tokio::test]
async fn bearer_auth_loads_live() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let mut doc = fixture.doc("bearer_v2");
    doc["catalog"]["auth"] = json!({"bearer": {"token": fixture.admin_token}});
    run(doc, "ice-bearer").await.expect("bearer loads");
}

/// A rejected TOKEN on the data path classifies FATAL with the
/// credential advice — the live 401 end to end.
#[tokio::test]
async fn a_rejected_token_is_fatal_with_advice() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let mut doc = fixture.doc("authfail_v2");
    doc["catalog"]["auth"] = json!({"bearer": {"token": "not-a-real-token"}});
    let err = run(doc, "ice-authfail").await.expect_err("must refuse");
    assert!(
        err.contains("fix the credential or its grants"),
        "the 401 carries the credential advice: {err}"
    );
}

/// Open failures carry the context frame, end to end: a wrong
/// warehouse names itself (the catalog handshake is lazy — the
/// refusal surfaces at the first namespace operation inside connect),
/// and an unreachable catalog is typed.
#[tokio::test]
async fn open_failures_carry_the_catalog_context() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let mut doc = fixture.doc("openfail_v2");
    doc["catalog"]["warehouse"] = json!("no-such-warehouse");
    let err = run(doc, "ice-openfail")
        .await
        .expect_err("a wrong warehouse refuses");
    assert!(
        err.contains("no-such-warehouse"),
        "the failure names the warehouse: {err}"
    );

    let mut doc = fixture.doc("unreachable_v2");
    doc["catalog"]["uri"] = json!("http://127.0.0.1:9");
    let err = run(doc, "ice-unreach")
        .await
        .expect_err("an unreachable catalog refuses");
    assert!(
        err.contains("namespace `unreachable_v2`") || err.contains("catalog `http://127.0.0.1:9`"),
        "typed with a context frame: {err}"
    );
}
