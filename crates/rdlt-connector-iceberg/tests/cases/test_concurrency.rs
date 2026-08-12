//! Concurrency and drift, live: two writers race their commits and no
//! snapshot is lost; a nested stream loads twice without phantom
//! drift; the sdk conformance kit certifies the shell.

use rdlt_connector_iceberg::destination::Shell;
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::CatalogFixture;

fn source_of(rows: Vec<serde_json::Value>, checkpoints: u64) -> MemorySource {
    let per = (rows.len() as u64 / checkpoints.max(1)).max(1) as usize;
    let batches: Vec<MemoryBatch> = rows
        .chunks(per)
        .enumerate()
        .map(|(i, chunk)| MemoryBatch::new(chunk.to_vec()).with_checkpoint(i as u64 + 1))
        .collect();
    MemorySource::new(vec![MemoryStream::new(StreamSpec::new("events"), batches)])
}

/// Two pipelines write ONE table concurrently, four commits each — the
/// bounded retry absorbs the races and no snapshot is lost.
#[tokio::test]
async fn two_live_writers_lose_no_snapshot() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "race_v2";
    let doc = fixture.doc(namespace);

    let run = |pipeline: String, base: i64| {
        let doc = doc.clone();
        async move {
            let workdir = tempfile::tempdir().expect("workdir");
            Engine::new(
                EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
                source_of((0..8).map(|i| json!({"id": base + i})).collect(), 4),
                Shell::from_value(doc).expect("valid"),
            )
            .run()
            .await
            .expect("the load settles")
        }
    };
    let (a, b) = tokio::join!(run("ice-race-a".into(), 0), run("ice-race-b".into(), 100));
    drop((a, b));

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(total, 16, "no snapshot lost to the race: {summaries:?}");
    assert_eq!(summaries.len(), 8, "four commits each: {summaries:?}");
}

/// A struct-bearing stream loads TWICE without reporting drift — the
/// catalog renumbers field ids level-order against our depth-first
/// assignment, and only the id-ignoring comparison keeps the second
/// load green (the pinned generation-1 defect class).
#[tokio::test]
async fn a_nested_stream_loads_twice_without_phantom_drift() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "nested_v2";
    let doc = fixture.doc(namespace);
    let rows = vec![json!({"id": 1, "meta": {"kind": "a", "score": 2}, "tags": ["x", "y"]})];

    for run in 1..=2 {
        let workdir = tempfile::tempdir().expect("workdir");
        Engine::new(
            EngineConfig::new(format!("ice-nested-{run}")).with_workdir(workdir.path().join("wal")),
            source_of(rows.clone(), 1),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("both loads settle — no phantom drift");
    }

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(summaries.len(), 2, "{summaries:?}");
}
