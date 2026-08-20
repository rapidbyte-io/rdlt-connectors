//! Real loads through the ENGINE: memory → file, and file → file (the
//! whole family round-trips through one pipeline).

use rdlt_connector_file::{destination, source};
use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory::{Batch as MemoryBatch, Source as MemorySource, Stream as MemoryStream};
use serde_json::json;

use super::common::{jsonl_source, local_dest, plant};

/// Memory source → parquet destination: exact totals through the
/// engine, plus the on-disk artifacts.
#[tokio::test]
async fn a_memory_load_lands_exact_totals_in_parquet() {
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1}), json!({"id": 2})]).with_checkpoint(1),
            MemoryBatch::new(vec![json!({"id": 3})]).with_checkpoint(2),
        ],
    )]);
    let config = local_dest(out.path());
    let dest = destination::Shell::new(config.clone()).expect("valid");
    Engine::new(
        EngineConfig::new("mem-to-parquet").with_workdir(workdir.path().join("wal")),
        source,
        dest,
    )
    .run()
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );
}

/// File source → file destination: planted jsonl through the engine
/// into parquet, totals exact.
#[tokio::test]
async fn a_file_to_file_load_round_trips() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    plant(input.path(), "data/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n");
    plant(input.path(), "data/b.jsonl", b"{\"id\": 3}\n");

    let src = source::Shell::new(jsonl_source(input.path(), "data/*.jsonl")).expect("valid");
    let config = local_dest(out.path());
    let dest = destination::Shell::new(config.clone()).expect("valid");
    Engine::new(
        EngineConfig::new("file-to-file").with_workdir(workdir.path().join("wal")),
        src,
        dest,
    )
    .run()
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );
}
