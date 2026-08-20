//! Interop: an INDEPENDENT implementation (pyiceberg) reads back a
//! PARTITIONED load this crate wrote — count, partition field, and
//! the identity properties (the recorded consolidation of the gen-1
//! trio). Skip-not-fail without the venv (tools/interop;
//! RDLT_INTEROP_PYTHON overrides).

use rdlt_connector_iceberg::destination::Shell;
use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory::{Batch as MemoryBatch, Source as MemorySource, Stream as MemoryStream};
use serde_json::json;

use super::common::{CLIENT_ID, CLIENT_SECRET, CatalogFixture, WAREHOUSE};

fn interop_python() -> Option<std::path::PathBuf> {
    if let Ok(path) = std::env::var("RDLT_INTEROP_PYTHON") {
        return Some(path.into());
    }
    let venv = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/interop/.venv/bin/python");
    if venv.exists() {
        Some(venv)
    } else {
        eprintln!("SKIP: tools/interop venv absent — pyiceberg read-back not run");
        None
    }
}

/// pyiceberg reads the crate's output: count, columns, and the
/// snapshot identity properties all agree with what was written.
#[tokio::test]
async fn pyiceberg_reads_back_a_partitioned_load() {
    let Some(python) = interop_python() else {
        return;
    };
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "interop_v2";
    let mut doc = fixture.doc(namespace);
    doc["tables"] = json!({"events": {"partition_by": [
        {"column": "id", "transform": {"bucket": 4}},
    ]}});

    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("ice-interop-v2").with_workdir(workdir.path().join("wal")),
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(vec![json!({"id": 1}), json!({"id": 2})]).with_checkpoint(1)],
        )]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");

    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/interop/pyiceberg_readback.py");
    let output = std::process::Command::new(python)
        .arg(script)
        .args(["--uri", &fixture.catalog_uri])
        .args(["--warehouse", WAREHOUSE])
        .args(["--credential", &format!("{CLIENT_ID}:{CLIENT_SECRET}")])
        .args(["--namespace", namespace])
        .args(["--table", "events"])
        .output()
        .expect("pyiceberg readback runs");
    assert!(
        output.status.success(),
        "readback failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("readback json");
    assert_eq!(report["count"], 2, "{report}");
    assert_eq!(report["partition_fields"][0], "id_bucket", "{report}");
    assert!(
        report["snapshot_props"]["rdlt.commit-seq"].is_string(),
        "the identity is readable by an independent implementation: {report}"
    );
}
