//! Output part sizing (034) against the live catalog: the configured
//! target reaches the library's rolling writer, and the file count in
//! the snapshot summary is the independent oracle for it.
//!
//! `added-data-files` is read off the catalog's own snapshot summary,
//! not off anything this crate computed — which is what makes it
//! evidence rather than a restatement of the code under test.

use rdlt_connector_iceberg::destination::Shell;
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::CatalogFixture;

/// Rows with an incompressible payload, so file size tracks row count
/// rather than how well a repeated fixture dictionary-encodes.
fn rows(offset: usize, count: usize) -> Vec<serde_json::Value> {
    (offset..offset + count)
        .map(|i| {
            let payload: String = (0..96)
                .map(|k| char::from(b'a' + ((i * 7 + k) % 26) as u8))
                .collect();
            json!({"id": i as i64, "payload": format!("{i}-{payload}")})
        })
        .collect()
}

fn source_of(batches: usize, per_batch: usize) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        (0..batches)
            .map(|b| MemoryBatch::new(rows(b * per_batch, per_batch)))
            .collect(),
    )])
}

/// A small target rolls the window into MANY data files; the default
/// leaves the same rows in ONE. Same data, same single commit — only
/// `parts.target_bytes` differs, so the file count is attributable to
/// it and nothing else.
///
/// MEASURED: 16,000 rows gave **8 data files at a 64 KiB target and 1
/// at the 128 MiB default**.
#[tokio::test]
async fn the_configured_target_rolls_the_window_into_several_files() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };

    let mut counts = Vec::new();
    for (namespace, target) in [("parts_rolled", Some(64 * 1024)), ("parts_default", None)] {
        let mut doc = fixture.doc(namespace);
        if let Some(bytes) = target {
            doc["parts"] = json!({"target_bytes": bytes});
        }
        let workdir = tempfile::tempdir().expect("workdir");
        Engine::new(
            EngineConfig::new(format!("ice-parts-{namespace}"))
                .with_workdir(workdir.path().join("wal")),
            source_of(8, 2_000),
            Shell::from_value(doc).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");

        let summaries = fixture.snapshot_summaries(namespace, "events").await;
        let files: u64 = summaries
            .iter()
            .filter_map(|s| s.get("added-data-files")?.parse::<u64>().ok())
            .sum();
        let records: u64 = summaries
            .iter()
            .filter_map(|s| s.get("added-records")?.parse::<u64>().ok())
            .sum();
        assert_eq!(records, 16_000, "{namespace}: every row landed once");
        counts.push((namespace, files));
    }

    let rolled = counts[0].1;
    let default = counts[1].1;
    assert!(
        rolled > default,
        "a 64 KiB target must produce more files than the 128 MiB default: {counts:?}"
    );
    assert_eq!(
        default, 1,
        "16,000 small rows are nowhere near 128 MiB, so the default writes one file: {counts:?}"
    );
}
