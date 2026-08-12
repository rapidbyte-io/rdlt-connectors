//! One real load through the ENGINE against the live fixture: exact
//! totals, one identity-stamped snapshot per commit, and no snapshot
//! at all for an empty window.

use rdlt_connector_iceberg::destination::{Shell, testhook};
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::CatalogFixture;

#[tokio::test]
async fn an_append_load_lands_exact_totals_with_one_snapshot_per_commit() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "ing_v2";
    let doc = fixture.doc(namespace);

    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![
                json!({"id": 1, "note": "a"}),
                json!({"id": 2, "note": "b"}),
            ])
            .with_checkpoint(1),
            MemoryBatch::new(vec![
                json!({"id": 3, "note": "c"}),
                json!({"id": 4, "note": "d"}),
            ])
            .with_checkpoint(2),
        ],
    )]);

    let workdir = tempfile::tempdir().expect("workdir");
    let pipeline = "ice-ing-v2";
    let report = Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
        source,
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");
    assert!(!report.tables.is_empty());

    // The receipt oracle: raw snapshot summaries off the catalog.
    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(summaries.len(), 2, "one snapshot per commit: {summaries:?}");
    let scope = testhook::scope_of(pipeline);
    let mut total_rows = 0u64;
    for (index, summary) in summaries.iter().enumerate() {
        assert_eq!(summary.get("rdlt.pipeline"), Some(&scope));
        assert_eq!(
            summary.get("rdlt.load-id"),
            Some(&report.load_id.as_str().to_owned())
        );
        assert_eq!(
            summary.get("rdlt.commit-seq"),
            Some(&(index as u64 + 1).to_string())
        );
        total_rows += summary
            .get("added-records")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
    }
    assert_eq!(total_rows, 4, "exact totals, no duplicates: {summaries:?}");
}

/// An empty commit window publishes NO snapshot — a zero-row snapshot
/// would be noise every catalog reader pays for.
#[tokio::test]
async fn an_empty_commit_publishes_no_snapshot() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "empty_v2";
    let doc = fixture.doc(namespace);

    // One batch with rows and a checkpoint, then a checkpoint-only
    // batch: the second commit window is empty.
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1),
            MemoryBatch::new(vec![]).with_checkpoint(2),
        ],
    )]);

    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("ice-empty-v2").with_workdir(workdir.path().join("wal")),
        source,
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(
        summaries.len(),
        1,
        "the empty window published nothing: {summaries:?}"
    );
}
