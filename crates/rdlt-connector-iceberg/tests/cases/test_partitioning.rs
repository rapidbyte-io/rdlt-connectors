//! Partitioning, live: the spec lands in the catalog's raw metadata
//! with the convention names, transformed writes fan out, and a
//! config that disagrees with the live spec refuses typed.

use rdlt_connector_iceberg::destination::Shell;
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::CatalogFixture;

fn source_of(rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(rows).with_checkpoint(1)],
    )])
}

/// A bucket-partitioned load: the spec is visible in raw metadata
/// (field ids from 1000, the `_bucket` name), and rows land.
#[tokio::test]
async fn a_bucketed_table_carries_its_spec_and_loads() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "part_v2";
    let mut doc = fixture.doc(namespace);
    doc["tables"] = json!({"events": {"partition_by": [
        {"column": "id", "transform": {"bucket": 4}},
    ]}});

    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("ice-part-v2").with_workdir(workdir.path().join("wal")),
        source_of(vec![
            json!({"id": 1}),
            json!({"id": 2}),
            json!({"id": 3}),
            json!({"id": 4}),
        ]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");

    let metadata = fixture.table_metadata(namespace, "events").await;
    let spec = &metadata["metadata"]["partition-specs"][0]["fields"][0];
    assert_eq!(spec["name"], "id_bucket", "{metadata}");
    assert_eq!(spec["transform"], "bucket[4]", "{metadata}");
    assert_eq!(spec["field-id"], 1000, "{metadata}");

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(total, 4, "{summaries:?}");
}

/// A config that disagrees with the live spec refuses with the frozen
/// drop-or-align advice — partition specs are fixed at creation.
#[tokio::test]
async fn a_spec_mismatch_refuses_typed() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "specdrift_v2";
    let mut doc = fixture.doc(namespace);
    doc["tables"] = json!({"events": {"partition_by": [
        {"column": "id", "transform": {"bucket": 4}},
    ]}});
    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("ice-drift-1").with_workdir(workdir.path().join("wal1")),
        source_of(vec![json!({"id": 1})]),
        Shell::from_value(doc.clone()).expect("valid"),
    )
    .run()
    .await
    .expect("run 1 creates the table");

    doc["tables"] = json!({"events": {"partition_by": [
        {"column": "id", "transform": {"bucket": 8}},
    ]}});
    let err = Engine::new(
        EngineConfig::new("ice-drift-2").with_workdir(workdir.path().join("wal2")),
        source_of(vec![json!({"id": 2})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("the changed spec refuses");
    let text = format!("{err}");
    assert!(
        text.contains("does not match the live table's partition spec")
            && text.contains("drop the table or align the config"),
        "{text}"
    );
}
