//! Schema evolution, live: a column added MID-WINDOW retires the
//! in-flight writer and keeps every row; a column added ACROSS loads
//! lands as an additive ALTER; the reserved-name and closed-namespace
//! refusals speak their frozen spellings; and two streams may not
//! share one table.

use rdlt_connector_iceberg::destination::{Shell, testhook};
use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory::{Batch as MemoryBatch, Source as MemorySource, Stream as MemoryStream};
use serde_json::json;

use super::common::CatalogFixture;

/// A column appearing in the SECOND batch of one commit window: the
/// mid-window ensure retires the writer, its closed files join the
/// window, and every row lands exactly once.
#[tokio::test]
async fn a_column_added_mid_window_keeps_every_row() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "midwin_v2";
    let doc = fixture.doc(namespace);

    // Two batches, ONE checkpoint: both land in the same commit
    // window, and the second carries a column the first did not.
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1})]),
            MemoryBatch::new(vec![json!({"id": 2, "extra": "kept"})]).with_checkpoint(1),
        ],
    )]);
    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("ice-midwin-v2").with_workdir(workdir.path().join("wal")),
        source,
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles through the mid-window evolution");

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(total, 2, "both files joined the window: {summaries:?}");
    // The retired writer's file and the evolved writer's file are
    // distinct data files in ONE snapshot.
    assert_eq!(summaries.len(), 1, "one commit window: {summaries:?}");
    assert_eq!(
        summaries[0].get("added-data-files").map(String::as_str),
        Some("2"),
        "the retired file AND the evolved file: {summaries:?}"
    );
    let metadata = fixture.table_metadata(namespace, "events").await;
    let fields = metadata["metadata"]["schemas"]
        .as_array()
        .and_then(|s| s.last())
        .map(|s| s["fields"].to_string())
        .unwrap_or_default();
    assert!(fields.contains("extra"), "the column evolved: {metadata}");
}

/// A column added ACROSS loads is an additive ALTER: load 2 widens
/// the live table and both loads' rows stay.
#[tokio::test]
async fn a_column_added_across_loads_alters_additively() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "widen_v2";
    let doc = fixture.doc(namespace);
    let run = |pipeline: &str, rows: Vec<serde_json::Value>| {
        let doc = doc.clone();
        let pipeline = pipeline.to_owned();
        async move {
            let workdir = tempfile::tempdir().expect("workdir");
            Engine::new(
                EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
                MemorySource::new(vec![MemoryStream::new(
                    StreamSpec::new("events"),
                    vec![MemoryBatch::new(rows).with_checkpoint(1)],
                )]),
                Shell::from_value(doc).expect("valid"),
            )
            .run()
            .await
            .expect("the load settles")
        }
    };
    run("ice-widen-1", vec![json!({"id": 1})]).await;
    run("ice-widen-2", vec![json!({"id": 2, "note": "new"})]).await;

    let metadata = fixture.table_metadata(namespace, "events").await;
    let fields = metadata["metadata"]["schemas"]
        .as_array()
        .and_then(|s| s.last())
        .map(|s| s["fields"].to_string())
        .unwrap_or_default();
    assert!(fields.contains("note"), "the ALTER landed: {metadata}");
    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(total, 2, "{summaries:?}");
}

/// The reserved marker-table name refuses with the frozen spelling.
#[tokio::test]
async fn the_reserved_state_table_name_is_refused() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let mut doc = fixture.doc("reserved_v2");
    doc["tables"] = json!({"events": {"name": testhook::STATE_TABLE}});
    let workdir = tempfile::tempdir().expect("workdir");
    let err = Engine::new(
        EngineConfig::new("ice-reserved-v2").with_workdir(workdir.path().join("wal")),
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1)],
        )]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("the reserved name refuses");
    assert!(
        format!("{err}").contains("is reserved for the rdlt state marker table"),
        "{err}"
    );
}

/// A missing namespace with create_namespace false refuses with the
/// frozen advice.
#[tokio::test]
async fn a_missing_namespace_refuses_when_creation_is_off() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let mut doc = fixture.doc("absent_ns_v2");
    doc["create_namespace"] = json!(false);
    let workdir = tempfile::tempdir().expect("workdir");
    let err = Engine::new(
        EngineConfig::new("ice-nons-v2").with_workdir(workdir.path().join("wal")),
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1)],
        )]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("must refuse");
    assert!(
        format!("{err}")
            .contains("does not exist and create_namespace is false — create it (or set"),
        "{err}"
    );
}

/// Two streams renamed onto ONE physical table refuse at ensure — the
/// collision no config-level check can see.
#[tokio::test]
async fn two_streams_sharing_one_table_are_refused() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let mut doc = fixture.doc("shared_v2");
    // Stream `b` renamed onto stream `a`'s DEFAULT name.
    doc["tables"] = json!({"b": {"name": "a"}});
    let workdir = tempfile::tempdir().expect("workdir");
    let err = Engine::new(
        EngineConfig::new("ice-shared-v2").with_workdir(workdir.path().join("wal")),
        MemorySource::new(vec![
            MemoryStream::new(
                StreamSpec::new("a"),
                vec![MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1)],
            ),
            MemoryStream::new(
                StreamSpec::new("b"),
                vec![MemoryBatch::new(vec![json!({"id": 2})]).with_checkpoint(1)],
            ),
        ]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("sharing one table must refuse");
    assert!(
        format!("{err}").contains("may not share one table"),
        "{err}"
    );
}

/// The `tables` map is keyed by the ENGINE'S NORMALIZED root-table
/// name, not the raw stream name — the frozen generation-1 lookup
/// semantics, pinned live: a stream named `Order-Items` reads its
/// options under `order_items`.
#[tokio::test]
async fn table_options_key_on_the_normalized_name() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "normkey_v2";
    let mut doc = fixture.doc(namespace);
    doc["tables"] = json!({"order_items": {"name": "renamed_items"}});

    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("ice-normkey-v2").with_workdir(workdir.path().join("wal")),
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("Order-Items"),
            vec![MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1)],
        )]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");

    let metadata = fixture.table_metadata(namespace, "renamed_items").await;
    assert!(
        metadata["metadata"]["table-uuid"].is_string(),
        "the options applied under the normalized key: {metadata}"
    );
}
